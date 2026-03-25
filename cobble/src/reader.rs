use crate::block_cache::{BlockCache, new_block_cache_with_config};
use crate::config::VolumeUsageKind;
use crate::coordinator::GlobalSnapshotManifest;
use crate::db_state::{bucket_range_fits_total, bucket_range_last, bucket_slots_for_total};
use crate::error::{Error, Result};
use crate::file::{File, FileSystem, FileSystemRegistry};
use crate::lru::LruCache;
use crate::metrics_manager::MetricsManager;
use crate::paths::{
    SNAPSHOT_DIR, global_snapshot_current_path, global_snapshot_manifest_path_by_pointer,
    snapshot_manifest_name,
};
#[cfg(test)]
use crate::paths::{bucket_snapshot_dir, bucket_snapshot_manifest_path};
use crate::util::{build_commit_short_id, build_version_string};
use crate::{Config, DbIterator, ReadOnlyDb, ReadOptions, ScanOptions, VolumeDescriptor};
use bytes::Bytes;
use log::info;
use serde_json::Error as SerdeError;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct ReaderConfig {
    pub volumes: Vec<VolumeDescriptor>,
    pub total_buckets: u32,
    pub pin_partition_in_memory_count: usize,
    pub block_cache_size: usize,
    pub block_cache_hybrid_enabled: bool,
    pub block_cache_hybrid_disk_size: Option<usize>,
    pub reload_tolerance: Duration,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        let default_config = Config::default();
        Self {
            volumes: VolumeDescriptor::single_volume("file:///tmp/".to_string()),
            total_buckets: default_config.total_buckets,
            pin_partition_in_memory_count: 1,
            block_cache_size: 512 * 1024 * 1024,
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
}

impl Reader {
    pub fn open(read_config: ReaderConfig, global_snapshot_id: u64) -> Result<Self> {
        let config = Config {
            volumes: read_config.volumes.clone(),
            total_buckets: read_config.total_buckets,
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
        let block_cache = if read_config.block_cache_size > 0 {
            Some(new_block_cache_with_config(
                &config,
                &db_id,
                read_config.block_cache_size,
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
        })
    }

    pub fn open_current(read_config: ReaderConfig) -> Result<Self> {
        let config = Config {
            volumes: read_config.volumes.clone(),
            total_buckets: read_config.total_buckets,
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
        let block_cache = if read_config.block_cache_size > 0 {
            Some(new_block_cache_with_config(
                &config,
                &db_id,
                read_config.block_cache_size,
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
        })
    }

    pub fn refresh(&mut self) -> Result<()> {
        let (pointer, modified) = read_manifest_pointer(&self.fs, None)?
            .ok_or_else(|| Error::IoError("Global snapshot pointer missing".to_string()))?;
        self.reload_if_changed(pointer, modified, true)
    }

    pub fn get(
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
        db.get(bucket_id, key, options)
    }

    pub fn scan(
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
        db.scan(bucket_id, range, options)
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
            let manifest = load_global_snapshot_by_name(&self.fs, manifest_name)?;
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

    fn load_snapshot(&mut self, key: &Arc<BucketSnapshotKey>) -> Result<Arc<ReadOnlyDb>> {
        if let Some(db) = self.cache.get(key) {
            return Ok(Arc::clone(db));
        }
        let db = Arc::new(ReadOnlyDb::open_with_db_id_and_cache_with_metrics(
            self.config.clone(),
            key.snapshot_id,
            key.db_id.clone(),
            self.block_cache.clone(),
            Arc::clone(&self.metrics_manager),
        )?);
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
    serde_json::from_slice(bytes).map_err(|err: SerdeError| {
        Error::IoError(format!("Failed to decode global manifest: {}", err))
    })
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
    let bytes = reader.read_at(0, reader.size())?;
    decode_global_snapshot(bytes.as_ref())
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
        let bytes = reader.read_at(0, reader.size())?;
        let pointer = String::from_utf8(bytes.to_vec())
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
mod tests {
    use super::*;
    use crate::VolumeDescriptor;
    use crate::coordinator::{CoordinatorConfig, DbCoordinator, ShardSnapshotInput};
    use std::path::Path;

    fn cleanup_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn wait_for_manifest_in_db(root: &str, db_id: &str, snapshot_id: u64) -> String {
        let full_path = format!(
            "{}/{}",
            root,
            bucket_snapshot_manifest_path(db_id, snapshot_id)
        );
        for _ in 0..50 {
            if Path::new(&full_path).exists() {
                return full_path;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(
            Path::new(&full_path).exists(),
            "manifest missing at {}",
            full_path
        );
        format!("file://{}", full_path)
    }

    fn create_bucket_manifest(
        fs: Arc<dyn crate::file::FileSystem>,
        root: &str,
        db_id: &str,
        snapshot_id: u64,
    ) -> String {
        let snapshot_dir = bucket_snapshot_dir(db_id);
        let manifest_path = bucket_snapshot_manifest_path(db_id, snapshot_id);
        let schema_dir = format!("{}/schema", db_id);
        let schema_path = format!("{}/schema/schema-0", db_id);
        let _ = fs.create_dir(db_id);
        let _ = fs.create_dir(&snapshot_dir);
        let _ = fs.create_dir(&schema_dir);
        let mut schema_writer = fs.open_write(&schema_path).unwrap();
        schema_writer
            .write(br#"{"id":0,"num_columns":1,"merge_operator_ids":[]}"#)
            .unwrap();
        schema_writer.close().unwrap();
        let mut writer = fs.open_write(&manifest_path).unwrap();
        let manifest = format!(
            "{{\"id\":{},\"seq_id\":0,\"latest_schema_id\":0,\"bucket_ranges\":[{{\"start\":0,\"end\":1}}],\"lsm_tree_bucket_ranges\":[{{\"start\":0,\"end\":1}}],\"tree_levels\":[[]],\"vlog_files\":[],\"active_memtable_data\":[]}}",
            snapshot_id
        );
        writer.write(manifest.as_bytes()).unwrap();
        writer.close().unwrap();
        wait_for_manifest_in_db(root, db_id, snapshot_id)
    }

    fn wait_for_pointer(root: &str, snapshot_id: u64) {
        let path = format!("{}/{}", root, global_snapshot_current_path());
        let manifest = snapshot_manifest_name(snapshot_id);
        for _ in 0..50 {
            if let Ok(contents) = std::fs::read_to_string(&path) {
                if contents.trim() == manifest {
                    return;
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let contents = std::fs::read_to_string(&path).expect("read pointer");
        assert_eq!(contents.trim(), manifest);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_read_proxy_routes_and_evicts() {
        let root = "/tmp/reader";
        cleanup_root(root);
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", root))
            .unwrap();
        let db_a = "db-a".to_string();
        let db_b = "db-b".to_string();
        let snap_a = 1;
        let snap_b = 2;
        let path_a = create_bucket_manifest(Arc::clone(&fs), root, &db_a, snap_a);
        let path_b = create_bucket_manifest(Arc::clone(&fs), root, &db_b, snap_b);

        let coordinator = DbCoordinator::open(CoordinatorConfig {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        })
        .unwrap();
        let global = coordinator
            .take_global_snapshot(
                4,
                vec![
                    ShardSnapshotInput {
                        ranges: vec![0u16..=1u16],
                        db_id: db_a.clone(),
                        snapshot_id: snap_a,
                        manifest_path: path_a,
                    },
                    ShardSnapshotInput {
                        ranges: vec![2u16..=3u16],
                        db_id: db_b.clone(),
                        snapshot_id: snap_b,
                        manifest_path: path_b,
                    },
                ],
            )
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();
        wait_for_pointer(root, global.id);

        let mut proxy = Reader::open_current(ReaderConfig {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            total_buckets: 4,
            ..ReaderConfig::default()
        })
        .unwrap();
        let value_a = proxy.get(0, b"key-a", &ReadOptions::default()).unwrap();
        assert!(value_a.is_none());
        assert_eq!(proxy.cache.len(), 1);
        assert!(proxy.cache.contains_key(&Arc::new(BucketSnapshotKey {
            db_id: db_a.clone(),
            snapshot_id: snap_a,
        })));

        proxy.reload_tolerance = Duration::from_millis(0);
        let value_b = proxy.get(3, b"key-b", &ReadOptions::default()).unwrap();
        assert!(value_b.is_none());
        assert_eq!(proxy.cache.len(), 1);
        assert!(!proxy.cache.contains_key(&Arc::new(BucketSnapshotKey {
            db_id: db_a,
            snapshot_id: snap_a,
        })));
        assert!(proxy.cache.contains_key(&Arc::new(BucketSnapshotKey {
            db_id: db_b,
            snapshot_id: snap_b,
        })));

        cleanup_root(root);
    }

    #[test]
    // #[serial_test::serial(file)]
    fn test_read_proxy_refreshes_on_pointer_change() {
        let root = "/tmp/read_proxy_refresh";
        cleanup_root(root);
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", root))
            .unwrap();
        let db_a = "db-a".to_string();
        let db_b = "db-b".to_string();
        let snap_a = 10;
        let snap_b = 20;
        let path_a = create_bucket_manifest(Arc::clone(&fs), root, &db_a, snap_a);
        let path_b = create_bucket_manifest(Arc::clone(&fs), root, &db_b, snap_b);

        let coordinator = DbCoordinator::open(CoordinatorConfig {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        })
        .unwrap();
        let global_a = coordinator
            .take_global_snapshot(
                4,
                vec![ShardSnapshotInput {
                    ranges: vec![0u16..=3u16],
                    db_id: db_a.clone(),
                    snapshot_id: snap_a,
                    manifest_path: path_a,
                }],
            )
            .unwrap();
        coordinator.materialize_global_snapshot(&global_a).unwrap();
        wait_for_pointer(root, global_a.id);

        let mut proxy = Reader::open_current(ReaderConfig {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            total_buckets: 4,
            ..ReaderConfig::default()
        })
        .unwrap();
        proxy.reload_tolerance = Duration::from_millis(0);
        let _ = proxy.get(0, b"key", &ReadOptions::default()).unwrap();
        assert!(proxy.cache.contains_key(&Arc::new(BucketSnapshotKey {
            db_id: db_a.clone(),
            snapshot_id: snap_a,
        })));

        let global_b = coordinator
            .take_global_snapshot(
                4,
                vec![ShardSnapshotInput {
                    ranges: vec![0u16..=3u16],
                    db_id: db_b.clone(),
                    snapshot_id: snap_b,
                    manifest_path: path_b,
                }],
            )
            .unwrap();
        coordinator.materialize_global_snapshot(&global_b).unwrap();
        wait_for_pointer(root, global_b.id);

        proxy.refresh().unwrap();
        let _ = proxy.get(0, b"key", &ReadOptions::default()).unwrap();
        assert!(proxy.cache.contains_key(&Arc::new(BucketSnapshotKey {
            db_id: db_b,
            snapshot_id: snap_b,
        })));
        assert!(!proxy.cache.contains_key(&Arc::new(BucketSnapshotKey {
            db_id: db_a,
            snapshot_id: snap_a,
        })));

        cleanup_root(root);
    }
}
