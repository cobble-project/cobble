use crate::cache::{BlockCache, new_block_cache_with_config};
use crate::config::VolumeUsageKind;
use crate::coordinator::GlobalSnapshotManifest;
use crate::db_state::{bucket_range_fits_total, bucket_range_last, bucket_slots_for_total};
use crate::error::{Error, Result};
use crate::file::{FileSystem, FileSystemRegistry, MetadataReader};
use crate::lru::LruCache;
use crate::merge_operator::MergeOperatorResolver;
use crate::metrics_manager::MetricsManager;
use crate::paths::{
    SNAPSHOT_DIR, global_snapshot_current_path, global_snapshot_manifest_path_by_pointer,
    snapshot_manifest_name,
};
use crate::util::{build_commit_short_id, build_version_string};
use crate::{Config, DbIterator, ReadOnlyDb, ReadOptions, ScanOptions, VolumeDescriptor};
use bytes::Bytes;
use log::info;
use serde_json::Error as SerdeError;
use size::Size;
use std::collections::HashMap;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct ReaderConfig {
    pub volumes: Vec<VolumeDescriptor>,
    pub total_buckets: u32,
    pub pin_partition_in_memory_count: usize,
    pub block_cache_size: Size,
    pub block_cache_hybrid_enabled: bool,
    pub block_cache_hybrid_disk_size: Option<Size>,
    pub reload_tolerance: Duration,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        let default_config = Config::default();
        Self {
            volumes: VolumeDescriptor::single_volume("file:///tmp/"),
            total_buckets: default_config.total_buckets,
            pin_partition_in_memory_count: 1,
            block_cache_size: Size::from_mib(512),
            block_cache_hybrid_enabled: default_config.block_cache_hybrid_enabled,
            block_cache_hybrid_disk_size: default_config.block_cache_hybrid_disk_size,
            reload_tolerance: Duration::from_secs(10),
        }
    }
}

impl ReaderConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            volumes: config.volumes.clone(),
            total_buckets: config.total_buckets,
            pin_partition_in_memory_count: config.reader.pin_partition_in_memory_count,
            block_cache_size: config.reader.block_cache_size,
            block_cache_hybrid_enabled: config.block_cache_hybrid_enabled,
            block_cache_hybrid_disk_size: config.block_cache_hybrid_disk_size,
            reload_tolerance: Duration::from_secs(config.reader.reload_tolerance_seconds),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BucketSnapshotKey {
    db_id: String,
    snapshot_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GlobalSnapshotSummary {
    pub id: u64,
    pub total_buckets: u32,
    pub shard_snapshot_count: usize,
    pub is_current: bool,
}

/// Read proxy that routes reads to bucket snapshots and caches them with LRU eviction.
pub struct Reader {
    config: Config,
    global_snapshot: GlobalSnapshotManifest,
    bucket_map: Vec<Option<Arc<BucketSnapshotKey>>>,
    cache: LruCache<Arc<BucketSnapshotKey>, Arc<ReadOnlyDb>>,
    block_cache: Option<BlockCache>,
    fs: Arc<dyn FileSystem>,
    db_id: String,
    metrics_manager: Arc<MetricsManager>,
    last_pointer: Option<String>,
    last_pointer_modified: Option<u64>,
    auto_refresh: bool,
    fixed_snapshot_id: Option<u64>,
    reload_tolerance: Duration,
    last_refresh_at: Option<Instant>,
    resolver: Option<Arc<dyn MergeOperatorResolver>>,
}

impl Reader {
    pub fn open(read_config: ReaderConfig, global_snapshot_id: u64) -> Result<Self> {
        Self::open_with_resolver(read_config, global_snapshot_id, None)
    }

    pub fn open_with_resolver(
        read_config: ReaderConfig,
        global_snapshot_id: u64,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        let block_cache_size =
            crate::util::size_to_usize("reader.block_cache_size", read_config.block_cache_size)
                .map_err(Error::ConfigError)?;
        let config = Config {
            volumes: read_config.volumes.clone(),
            total_buckets: read_config.total_buckets,
            block_cache_size: read_config.block_cache_size,
            block_cache_hybrid_enabled: read_config.block_cache_hybrid_enabled,
            block_cache_hybrid_disk_size: read_config.block_cache_hybrid_disk_size,
            ..Config::default()
        }
        .normalize_volume_paths()?;
        info!(
            "Cobble reader ({}, Rev:{}) start.",
            build_version_string(),
            build_commit_short_id()
        );
        let registry = FileSystemRegistry::new();
        let volumes = if config.volumes.is_empty() {
            return Err(Error::ConfigError("No volumes configured".to_string()));
        } else {
            config.volumes.clone()
        };
        let meta_volume = volumes
            .iter()
            .find(|volume| volume.supports(VolumeUsageKind::Meta))
            .unwrap_or_else(|| volumes.first().expect("No meta volume exists"));
        let fs = registry.get_or_register_volume(meta_volume)?;
        let manifest_name = snapshot_manifest_name(global_snapshot_id);
        let global_snapshot = load_global_snapshot_by_name(&fs, &manifest_name)?;
        let bucket_map = build_bucket_map(&global_snapshot)?;
        let db_id = Uuid::new_v4().to_string();
        let block_cache = if block_cache_size > 0 {
            Some(new_block_cache_with_config(
                &config,
                &db_id,
                block_cache_size,
                None,
            )?)
        } else {
            None
        };
        let metrics_manager = Arc::new(MetricsManager::new(&db_id));
        Ok(Self {
            config,
            global_snapshot,
            bucket_map,
            cache: LruCache::new(read_config.pin_partition_in_memory_count),
            block_cache,
            fs,
            db_id,
            metrics_manager,
            last_pointer: Some(manifest_name),
            last_pointer_modified: None,
            auto_refresh: false,
            fixed_snapshot_id: Some(global_snapshot_id),
            reload_tolerance: read_config.reload_tolerance,
            last_refresh_at: None,
            resolver,
        })
    }

    pub fn open_current(read_config: ReaderConfig) -> Result<Self> {
        Self::open_current_with_resolver(read_config, None)
    }

    pub fn open_current_with_resolver(
        read_config: ReaderConfig,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        let block_cache_size =
            crate::util::size_to_usize("reader.block_cache_size", read_config.block_cache_size)
                .map_err(Error::ConfigError)?;
        let config = Config {
            volumes: read_config.volumes.clone(),
            total_buckets: read_config.total_buckets,
            block_cache_size: read_config.block_cache_size,
            block_cache_hybrid_enabled: read_config.block_cache_hybrid_enabled,
            block_cache_hybrid_disk_size: read_config.block_cache_hybrid_disk_size,
            ..Config::default()
        }
        .normalize_volume_paths()?;
        info!(
            "cobble=reader runtime start version={} build_commit={}",
            build_version_string(),
            build_commit_short_id()
        );
        let registry = FileSystemRegistry::new();
        let volumes = if config.volumes.is_empty() {
            return Err(Error::ConfigError("No volumes configured".to_string()));
        } else {
            config.volumes.clone()
        };
        let meta_volume = volumes
            .iter()
            .find(|volume| volume.supports(VolumeUsageKind::Meta))
            .unwrap_or_else(|| volumes.first().expect("default volume exists"));
        let fs = registry.get_or_register_volume(meta_volume)?;
        let (pointer, modified) = read_manifest_pointer(&fs, None)?
            .ok_or_else(|| Error::IoError("Global snapshot pointer missing".to_string()))?;
        let global_snapshot = load_global_snapshot_by_name(&fs, &pointer)?;
        let bucket_map = build_bucket_map(&global_snapshot)?;
        let db_id = Uuid::new_v4().to_string();
        let block_cache = if block_cache_size > 0 {
            Some(new_block_cache_with_config(
                &config,
                &db_id,
                block_cache_size,
                None,
            )?)
        } else {
            None
        };
        let metrics_manager = Arc::new(MetricsManager::new(&db_id));
        Ok(Self {
            config,
            global_snapshot,
            bucket_map,
            cache: LruCache::new(read_config.pin_partition_in_memory_count),
            block_cache,
            fs,
            db_id,
            metrics_manager,
            last_pointer: Some(pointer),
            last_pointer_modified: modified,
            auto_refresh: true,
            fixed_snapshot_id: None,
            reload_tolerance: read_config.reload_tolerance,
            last_refresh_at: Some(Instant::now()),
            resolver,
        })
    }

    pub fn refresh(&mut self) -> Result<()> {
        let (pointer, modified) = read_manifest_pointer(&self.fs, None)?
            .ok_or_else(|| Error::IoError("Global snapshot pointer missing".to_string()))?;
        self.reload_if_changed(pointer, modified, true)
    }

    pub fn get(&mut self, bucket_id: u16, key: &[u8]) -> Result<Option<Vec<Option<Bytes>>>> {
        self.get_with_options(bucket_id, key, &ReadOptions::default())
    }

    pub fn multi_get<K: AsRef<[u8]>>(
        &mut self,
        keys: &[(u16, K)],
    ) -> Result<Vec<Option<Vec<Option<Bytes>>>>> {
        self.multi_get_with_options(keys, &ReadOptions::default())
    }

    pub fn multi_get_with_options<K: AsRef<[u8]>>(
        &mut self,
        keys: &[(u16, K)],
        options: &ReadOptions,
    ) -> Result<Vec<Option<Vec<Option<Bytes>>>>> {
        if self.auto_refresh {
            self.refresh_if_changed(false)?;
        }
        let mut by_snapshot = HashMap::<Arc<BucketSnapshotKey>, Vec<(usize, u16, &[u8])>>::new();
        for (index, (bucket, key)) in keys.iter().enumerate() {
            let snapshot_key = self.snapshot_key_for_bucket(*bucket)?;
            by_snapshot
                .entry(snapshot_key)
                .or_default()
                .push((index, *bucket, key.as_ref()));
        }
        let mut results = vec![None; keys.len()];
        for (snapshot_key, requests) in by_snapshot {
            let db = self.load_snapshot(&snapshot_key)?;
            let batch_keys = requests
                .iter()
                .map(|(_, bucket, key)| (*bucket, *key))
                .collect::<Vec<_>>();
            let batch_results = db.multi_get_with_options(&batch_keys, options)?;
            for ((index, _, _), result) in requests.into_iter().zip(batch_results) {
                results[index] = result;
            }
        }
        Ok(results)
    }

    pub fn get_with_options(
        &mut self,
        bucket_id: u16,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<Bytes>>>> {
        if self.auto_refresh {
            self.refresh_if_changed(false)?;
        }
        let snapshot_key = self.snapshot_key_for_bucket(bucket_id)?;
        let db = self.load_snapshot(&snapshot_key)?;
        db.get_with_options(bucket_id, key, options)
    }

    pub fn scan(&mut self, bucket_id: u16, range: Range<&[u8]>) -> Result<DbIterator<'static>> {
        self.scan_with_options(bucket_id, range, &ScanOptions::default())
    }

    pub fn scan_with_options(
        &mut self,
        bucket_id: u16,
        range: Range<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator<'static>> {
        if self.auto_refresh {
            self.refresh_if_changed(false)?;
        }
        let snapshot_key = self.snapshot_key_for_bucket(bucket_id)?;
        let db = self.load_snapshot(&snapshot_key)?;
        db.scan_with_options(bucket_id, range, options)
    }

    pub fn read_mode(&self) -> &'static str {
        if self.fixed_snapshot_id.is_some() {
            "snapshot"
        } else {
            "current"
        }
    }

    pub fn configured_snapshot_id(&self) -> Option<u64> {
        self.fixed_snapshot_id
    }

    pub fn current_global_snapshot(&self) -> &GlobalSnapshotManifest {
        &self.global_snapshot
    }

    pub fn list_global_snapshots(&self) -> Result<Vec<GlobalSnapshotSummary>> {
        let current_snapshot_id = read_manifest_pointer(&self.fs, None)?
            .and_then(|(pointer, _)| parse_snapshot_id(&pointer));
        let mut snapshots = Vec::new();
        for entry in self.fs.list(SNAPSHOT_DIR)? {
            let manifest_name = entry.rsplit('/').next().unwrap_or(entry.as_str()).trim();
            let Some(snapshot_id) = parse_snapshot_id(manifest_name) else {
                continue;
            };
            let manifest = match load_global_snapshot_by_name(&self.fs, manifest_name) {
                Ok(manifest) => manifest,
                Err(Error::ChecksumMismatch(_)) => continue,
                Err(err) => return Err(err),
            };
            snapshots.push(GlobalSnapshotSummary {
                id: manifest.id,
                total_buckets: manifest.total_buckets,
                shard_snapshot_count: manifest.shard_snapshots.len(),
                is_current: current_snapshot_id == Some(snapshot_id),
            });
        }
        snapshots.sort_by_key(|snapshot| snapshot.id);
        Ok(snapshots)
    }

    pub fn list_global_snapshot_manifests(&self) -> Result<Vec<GlobalSnapshotManifest>> {
        let mut manifests = Vec::new();
        for entry in self.fs.list(SNAPSHOT_DIR)? {
            let manifest_name = entry.rsplit('/').next().unwrap_or(entry.as_str()).trim();
            if parse_snapshot_id(manifest_name).is_none() {
                continue;
            }
            match load_global_snapshot_by_name(&self.fs, manifest_name) {
                Ok(manifest) => manifests.push(manifest),
                Err(Error::ChecksumMismatch(_)) => {}
                Err(err) => return Err(err),
            }
        }
        manifests.sort_by_key(|snapshot| snapshot.id);
        Ok(manifests)
    }

    fn load_snapshot(&mut self, key: &Arc<BucketSnapshotKey>) -> Result<Arc<ReadOnlyDb>> {
        if let Some(db) = self.cache.get(key) {
            return Ok(Arc::clone(db));
        }
        let shard_metrics_manager = Arc::new(MetricsManager::new(format!(
            "{}-{}",
            key.db_id, key.snapshot_id
        )));
        let db = Arc::new(
            ReadOnlyDb::open_with_db_id_and_cache_with_metrics_and_resolver(
                self.config.clone(),
                key.snapshot_id,
                key.db_id.clone(),
                self.block_cache.clone(),
                shard_metrics_manager,
                self.resolver.clone(),
            )?,
        );
        self.cache.insert(Arc::clone(key), Arc::clone(&db));
        Ok(db)
    }

    fn snapshot_key_for_bucket(&self, bucket_id: u16) -> Result<Arc<BucketSnapshotKey>> {
        if bucket_id as usize >= bucket_slots_for_total(self.global_snapshot.total_buckets) {
            return Err(Error::IoError(format!(
                "Bucket {} outside total buckets {}",
                bucket_id, self.global_snapshot.total_buckets
            )));
        }
        self.bucket_map
            .get(bucket_id as usize)
            .and_then(|entry| entry.as_ref())
            .cloned()
            .ok_or_else(|| Error::IoError(format!("No bucket snapshot for bucket {}", bucket_id)))
    }

    fn refresh_if_changed(&mut self, force: bool) -> Result<()> {
        if !force
            && let Some(last) = self.last_refresh_at
            && last.elapsed() < self.reload_tolerance
        {
            return Ok(());
        }
        let Some((pointer, modified)) =
            read_manifest_pointer(&self.fs, self.last_pointer_modified)?
        else {
            self.last_refresh_at = Some(Instant::now());
            return Ok(());
        };
        self.reload_if_changed(pointer, modified, force)
    }

    fn reload_if_changed(
        &mut self,
        pointer: String,
        modified: Option<u64>,
        force: bool,
    ) -> Result<()> {
        if self.last_pointer.as_deref() == Some(pointer.as_str())
            && self.last_pointer_modified == modified
        {
            if force {
                self.last_refresh_at = Some(Instant::now());
            }
            return Ok(());
        }
        let global_snapshot = load_global_snapshot_by_name(&self.fs, &pointer)?;
        let bucket_map = build_bucket_map(&global_snapshot)?;
        self.global_snapshot = global_snapshot;
        self.bucket_map = bucket_map;
        self.last_pointer = Some(pointer);
        self.last_pointer_modified = modified;
        self.cache.clear();
        self.last_refresh_at = Some(Instant::now());
        Ok(())
    }
}

fn decode_global_snapshot(bytes: &[u8]) -> Result<GlobalSnapshotManifest> {
    let manifest: GlobalSnapshotManifest =
        serde_json::from_slice(bytes).map_err(|err: SerdeError| {
            Error::IoError(format!("Failed to decode global manifest: {}", err))
        })?;
    manifest.validate_version()?;
    Ok(manifest)
}

fn parse_snapshot_id(manifest_name: &str) -> Option<u64> {
    let trimmed = manifest_name.trim();
    let id = trimmed.strip_prefix("SNAPSHOT-")?;
    id.parse::<u64>().ok()
}

fn load_global_snapshot_by_name(
    fs: &Arc<dyn FileSystem>,
    manifest_name: &str,
) -> Result<GlobalSnapshotManifest> {
    let manifest_path = global_snapshot_manifest_path_by_pointer(manifest_name);
    let reader = fs.open_read(&manifest_path)?;
    let payload = MetadataReader::new(reader).read_all()?;
    decode_global_snapshot(payload.as_ref())
}

fn read_manifest_pointer(
    fs: &Arc<dyn FileSystem>,
    last_modified: Option<u64>,
) -> Result<Option<(String, Option<u64>)>> {
    let pointer_path = global_snapshot_current_path();
    let mut last_err = None;
    for _ in 0..5 {
        if !fs.exists(&pointer_path)? {
            std::thread::sleep(Duration::from_millis(10));
            continue;
        }
        let modified = fs.last_modified(&pointer_path)?;
        if let (Some(previous), Some(current)) = (last_modified, modified)
            && previous == current
        {
            return Ok(None);
        }
        let reader = match fs.open_read(&pointer_path) {
            Ok(reader) => reader,
            Err(err) => {
                last_err = Some(err);
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }
        };
        let payload = match MetadataReader::new(reader).read_all() {
            Ok(payload) => payload,
            Err(Error::ChecksumMismatch(_)) => return Ok(None),
            Err(err) => return Err(err),
        };
        let pointer = String::from_utf8(payload.to_vec())
            .map_err(|err| Error::IoError(format!("Invalid manifest pointer: {}", err)))?;
        let pointer = pointer.trim().to_string();
        if pointer.is_empty() {
            return Ok(None);
        }
        return Ok(Some((pointer, modified)));
    }
    if let Some(err) = last_err {
        return Err(err);
    }
    Ok(None)
}

fn build_bucket_map(
    manifest: &GlobalSnapshotManifest,
) -> Result<Vec<Option<Arc<BucketSnapshotKey>>>> {
    let mut mapping = vec![None; bucket_slots_for_total(manifest.total_buckets)];
    for snapshot in &manifest.shard_snapshots {
        let key = Arc::new(BucketSnapshotKey {
            db_id: snapshot.db_id.clone(),
            snapshot_id: snapshot.snapshot_id,
        });
        for range in &snapshot.ranges {
            validate_range(range, manifest.total_buckets)?;
            let Some(last_bucket) = bucket_range_last(range) else {
                continue;
            };
            let mut bucket_id = *range.start();
            loop {
                let idx = bucket_id as usize;
                if mapping[idx].is_some() {
                    return Err(Error::IoError(format!(
                        "Bucket {} mapped to multiple snapshots",
                        bucket_id
                    )));
                }
                mapping[idx] = Some(Arc::clone(&key));
                if bucket_id == last_bucket {
                    break;
                }
                bucket_id = bucket_id.saturating_add(1);
            }
        }
    }
    Ok(mapping)
}

fn validate_range(range: &RangeInclusive<u16>, total_buckets: u32) -> Result<()> {
    if !bucket_range_fits_total(range, total_buckets) {
        return Err(Error::IoError(format!(
            "Invalid range {:?} for total buckets {}",
            range, total_buckets
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/reader.rs"]
mod tests;
