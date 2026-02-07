use crate::config::VolumeUsageKind;
use crate::error::{Error, Result};
use crate::file::{File, FileSystem, FileSystemRegistry};
use crate::maintainer::GlobalSnapshotManifest;
#[cfg(test)]
use crate::paths::{bucket_snapshot_dir, bucket_snapshot_manifest_path};
use crate::paths::{
    global_snapshot_current_path, global_snapshot_manifest_path_by_pointer, snapshot_manifest_name,
};
use crate::sst::block_cache::{BlockCache, new_block_cache};
use crate::{Config, ReadOnlyDb, VolumeDescriptor};
use bytes::Bytes;
use serde_json::Error as SerdeError;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_RELOAD_TOLERANCE: Duration = Duration::from_secs(10);

#[derive(Clone, Debug)]
pub struct ReadProxyConfig {
    pub volumes: Vec<VolumeDescriptor>,
    pub pin_partition_in_memory_count: usize,
    pub block_cache_size: usize,
    pub reload_tolerance: Duration,
}

impl Default for ReadProxyConfig {
    fn default() -> Self {
        Self {
            volumes: VolumeDescriptor::single_volume("file:///tmp/".to_string()),
            pin_partition_in_memory_count: 1,
            block_cache_size: 512 * 1024 * 1024,
            reload_tolerance: DEFAULT_RELOAD_TOLERANCE,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BucketSnapshotKey {
    db_id: String,
    snapshot_id: u64,
}

struct LruCache<K, V> {
    capacity: usize,
    map: HashMap<K, V>,
    order: VecDeque<K>,
}

impl<K, V> LruCache<K, V>
where
    K: Eq + Hash + Clone,
{
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        if self.map.contains_key(key) {
            self.touch(key);
        }
        self.map.get(key)
    }

    fn insert(&mut self, key: K, value: V) {
        if self.capacity == 0 {
            return;
        }
        if self.map.contains_key(&key) {
            self.map.insert(key.clone(), value);
            self.touch(&key);
            return;
        }
        if self.map.len() == self.capacity
            && let Some(old_key) = self.order.pop_back()
        {
            self.map.remove(&old_key);
        }
        self.order.push_front(key.clone());
        self.map.insert(key, value);
    }

    fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
    }

    fn touch(&mut self, key: &K) {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
        }
        self.order.push_front(key.clone());
    }
}

/// Read proxy that routes reads to bucket snapshots and caches them with LRU eviction.
pub struct ReadProxy {
    config: Config,
    global_snapshot: GlobalSnapshotManifest,
    bucket_map: Vec<Option<Arc<BucketSnapshotKey>>>,
    cache: LruCache<Arc<BucketSnapshotKey>, Arc<ReadOnlyDb>>,
    block_cache: Option<BlockCache>,
    fs: Arc<dyn FileSystem>,
    last_pointer: Option<String>,
    last_pointer_modified: Option<u64>,
    auto_refresh: bool,
    reload_tolerance: Duration,
    last_refresh_at: Option<Instant>,
}

impl ReadProxy {
    pub fn open(read_config: ReadProxyConfig, global_snapshot_id: u64) -> Result<Self> {
        let config = Config {
            volumes: read_config.volumes.clone(),
            ..Config::default()
        };
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
        let block_cache = if read_config.block_cache_size > 0 {
            Some(new_block_cache(read_config.block_cache_size))
        } else {
            None
        };
        Ok(Self {
            config,
            global_snapshot,
            bucket_map,
            cache: LruCache::new(read_config.pin_partition_in_memory_count),
            block_cache,
            fs,
            last_pointer: Some(manifest_name),
            last_pointer_modified: None,
            auto_refresh: false,
            reload_tolerance: read_config.reload_tolerance,
            last_refresh_at: None,
        })
    }

    pub fn open_current(read_config: ReadProxyConfig) -> Result<Self> {
        let config = Config {
            volumes: read_config.volumes.clone(),
            ..Config::default()
        };
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
        let block_cache = if read_config.block_cache_size > 0 {
            Some(new_block_cache(read_config.block_cache_size))
        } else {
            None
        };
        Ok(Self {
            config,
            global_snapshot,
            bucket_map,
            cache: LruCache::new(read_config.pin_partition_in_memory_count),
            block_cache,
            fs,
            last_pointer: Some(pointer),
            last_pointer_modified: modified,
            auto_refresh: true,
            reload_tolerance: read_config.reload_tolerance,
            last_refresh_at: Some(Instant::now()),
        })
    }

    pub fn refresh(&mut self) -> Result<()> {
        let (pointer, modified) = read_manifest_pointer(&self.fs, None)?
            .ok_or_else(|| Error::IoError("Global snapshot pointer missing".to_string()))?;
        self.reload_if_changed(pointer, modified, true)
    }

    pub fn get(&mut self, bucket_id: u16, key: &[u8]) -> Result<Option<Vec<Option<Bytes>>>> {
        if self.auto_refresh {
            self.refresh_if_changed(false)?;
        }
        if bucket_id >= self.global_snapshot.total_buckets {
            return Err(Error::IoError(format!(
                "Bucket {} outside total buckets {}",
                bucket_id, self.global_snapshot.total_buckets
            )));
        }
        let snapshot_key = self
            .bucket_map
            .get(bucket_id as usize)
            .and_then(|entry| entry.as_ref())
            .cloned()
            .ok_or_else(|| {
                Error::IoError(format!("No bucket snapshot for bucket {}", bucket_id))
            })?;
        let db = self.load_snapshot(&snapshot_key)?;
        db.get(key)
    }

    fn load_snapshot(&mut self, key: &Arc<BucketSnapshotKey>) -> Result<Arc<ReadOnlyDb>> {
        if let Some(db) = self.cache.get(key) {
            return Ok(Arc::clone(db));
        }
        let db = Arc::new(ReadOnlyDb::open_with_db_id_and_cache(
            self.config.clone(),
            key.snapshot_id,
            key.db_id.clone(),
            self.block_cache.clone(),
        )?);
        self.cache.insert(Arc::clone(key), Arc::clone(&db));
        Ok(db)
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
    let mut mapping = vec![None; manifest.total_buckets as usize];
    for snapshot in &manifest.bucket_snapshots {
        let key = Arc::new(BucketSnapshotKey {
            db_id: snapshot.db_id.clone(),
            snapshot_id: snapshot.snapshot_id,
        });
        for range in &snapshot.ranges {
            validate_range(range, manifest.total_buckets)?;
            for bucket_id in range.start..range.end {
                let idx = bucket_id as usize;
                if mapping[idx].is_some() {
                    return Err(Error::IoError(format!(
                        "Bucket {} mapped to multiple snapshots",
                        bucket_id
                    )));
                }
                mapping[idx] = Some(Arc::clone(&key));
            }
        }
    }
    Ok(mapping)
}

fn validate_range(range: &Range<u16>, total_buckets: u16) -> Result<()> {
    if range.start >= range.end || range.end > total_buckets {
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
    use crate::maintainer::{BucketSnapshotInput, MaintainerConfig, MaintainerNode};
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
        let _ = fs.create_dir(db_id);
        let _ = fs.create_dir(&snapshot_dir);
        let mut writer = fs.open_write(&manifest_path).unwrap();
        let manifest = format!("{{\"id\":{},\"seq_id\":0,\"levels\":[]}}", snapshot_id);
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
        let root = "/tmp/read_proxy";
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

        let maintainer = MaintainerNode::open(MaintainerConfig {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        })
        .unwrap();
        let global = maintainer
            .take_global_snapshot(
                4,
                vec![
                    BucketSnapshotInput {
                        ranges: vec![0u16..2u16],
                        db_id: db_a.clone(),
                        snapshot_id: snap_a,
                        manifest_path: path_a,
                    },
                    BucketSnapshotInput {
                        ranges: vec![2u16..4u16],
                        db_id: db_b.clone(),
                        snapshot_id: snap_b,
                        manifest_path: path_b,
                    },
                ],
            )
            .unwrap();
        maintainer.materialize_global_snapshot(&global).unwrap();
        wait_for_pointer(root, global.id);

        let mut proxy = ReadProxy::open_current(ReadProxyConfig {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..ReadProxyConfig::default()
        })
        .unwrap();
        let value_a = proxy.get(0, b"key-a").unwrap();
        assert!(value_a.is_none());
        assert_eq!(proxy.cache.len(), 1);
        assert!(proxy.cache.contains_key(&Arc::new(BucketSnapshotKey {
            db_id: db_a.clone(),
            snapshot_id: snap_a,
        })));

        proxy.reload_tolerance = Duration::from_millis(0);
        let value_b = proxy.get(3, b"key-b").unwrap();
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
    #[serial_test::serial(file)]
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

        let maintainer = MaintainerNode::open(MaintainerConfig {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        })
        .unwrap();
        let global_a = maintainer
            .take_global_snapshot(
                4,
                vec![BucketSnapshotInput {
                    ranges: vec![0u16..4u16],
                    db_id: db_a.clone(),
                    snapshot_id: snap_a,
                    manifest_path: path_a,
                }],
            )
            .unwrap();
        maintainer.materialize_global_snapshot(&global_a).unwrap();
        wait_for_pointer(root, global_a.id);

        let mut proxy = ReadProxy::open_current(ReadProxyConfig {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..ReadProxyConfig::default()
        })
        .unwrap();
        proxy.reload_tolerance = Duration::from_millis(0);
        let _ = proxy.get(0, b"key").unwrap();
        assert!(proxy.cache.contains_key(&Arc::new(BucketSnapshotKey {
            db_id: db_a.clone(),
            snapshot_id: snap_a,
        })));

        let global_b = maintainer
            .take_global_snapshot(
                4,
                vec![BucketSnapshotInput {
                    ranges: vec![0u16..4u16],
                    db_id: db_b.clone(),
                    snapshot_id: snap_b,
                    manifest_path: path_b,
                }],
            )
            .unwrap();
        maintainer.materialize_global_snapshot(&global_b).unwrap();
        wait_for_pointer(root, global_b.id);

        proxy.refresh().unwrap();
        let _ = proxy.get(0, b"key").unwrap();
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
