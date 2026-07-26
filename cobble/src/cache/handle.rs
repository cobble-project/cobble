use crate::error::{Error, Result};
use foyer::{
    BlockEngineConfig, Cache, CacheBuilder, DeviceBuilder, EventListener, FsDeviceBuilder,
    HybridCache, HybridCacheBuilder, PsyncIoEngineConfig,
};
use log::warn;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub trait CacheHandle<K, V>: Send + Sync {
    fn get(&self, key: &K) -> Option<V>;
    fn insert(&self, key: K, value: V);
    fn remove(&self, key: &K);
    fn clear(&self);
}

struct HybridCacheBackend<K, V>
where
    K: foyer::StorageKey + Clone,
    V: foyer::StorageValue + Clone,
{
    inner: HybridCache<K, V>,
    runtime: Arc<tokio::runtime::Runtime>,
    cache_root: std::path::PathBuf,
}

impl<K, V> Drop for HybridCacheBackend<K, V>
where
    K: foyer::StorageKey + Clone,
    V: foyer::StorageValue + Clone,
{
    fn drop(&mut self) {
        if let Err(err) = self.runtime.block_on(self.inner.close()) {
            warn!("failed to close hybrid block cache: {}", err);
        }
        if let Err(err) = std::fs::remove_dir_all(&self.cache_root)
            && err.kind() != ErrorKind::NotFound
        {
            warn!(
                "failed to remove hybrid cache directory {}: {}",
                self.cache_root.display(),
                err
            );
        }
    }
}

#[derive(Clone)]
enum FoyerCacheBackend<K, V>
where
    K: foyer::StorageKey + Clone,
    V: foyer::StorageValue + Clone,
{
    Memory(Cache<K, V>),
    Hybrid(Arc<HybridCacheBackend<K, V>>),
}

#[derive(Clone)]
pub struct FoyerCache<K, V>
where
    K: foyer::StorageKey + Clone,
    V: foyer::StorageValue + Clone,
{
    backend: FoyerCacheBackend<K, V>,
}

impl<K, V> FoyerCache<K, V>
where
    K: foyer::StorageKey + Clone,
    V: foyer::StorageValue + Clone,
{
    pub fn new(
        capacity: usize,
        weighter: impl Fn(&K, &V) -> usize + Send + Sync + 'static,
    ) -> Self {
        Self::new_with_event_listener(capacity, weighter, None)
    }

    pub fn new_with_event_listener(
        capacity: usize,
        weighter: impl Fn(&K, &V) -> usize + Send + Sync + 'static,
        event_listener: Option<Arc<dyn EventListener<Key = K, Value = V>>>,
    ) -> Self {
        let mut builder = CacheBuilder::new(capacity).with_weighter(weighter);
        if let Some(event_listener) = event_listener {
            builder = builder.with_event_listener(event_listener);
        }
        Self {
            backend: FoyerCacheBackend::Memory(builder.build()),
        }
    }

    pub fn new_hybrid(
        memory_capacity: usize,
        disk_capacity: usize,
        disk_path: impl AsRef<Path>,
        weighter: impl Fn(&K, &V) -> usize + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::new_hybrid_with_event_listener(
            memory_capacity,
            disk_capacity,
            disk_path,
            weighter,
            None,
        )
    }

    pub fn new_hybrid_with_event_listener(
        memory_capacity: usize,
        disk_capacity: usize,
        disk_path: impl AsRef<Path>,
        weighter: impl Fn(&K, &V) -> usize + Send + Sync + 'static,
        event_listener: Option<Arc<dyn EventListener<Key = K, Value = V>>>,
    ) -> Result<Self> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .thread_name("cobble-hybrid-cache")
                .build()
                .map_err(|err| {
                    Error::ConfigError(format!("Failed to build hybrid cache runtime: {err}"))
                })?,
        );
        let disk_path_buf = disk_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&disk_path_buf).map_err(|err| {
            Error::ConfigError(format!(
                "Failed to create hybrid cache directory {}: {}",
                disk_path_buf.display(),
                err
            ))
        })?;
        let handle = runtime.handle().clone();
        let hybrid = runtime
            .block_on(async move {
                let device = FsDeviceBuilder::new(disk_path_buf)
                    .with_capacity(disk_capacity)
                    .build()
                    .map_err(|err| {
                        Error::ConfigError(format!("Failed to build hybrid cache device: {}", err))
                    })?;
                let mut builder = HybridCacheBuilder::new().with_name("cobble-block-cache");
                if let Some(event_listener) = event_listener {
                    builder = builder.with_event_listener(event_listener);
                }
                let cache = builder
                    .memory(memory_capacity)
                    .with_weighter(weighter)
                    .storage()
                    .with_io_engine_config(PsyncIoEngineConfig::new())
                    .with_engine_config(BlockEngineConfig::new(device))
                    .with_spawner(handle.into())
                    .build()
                    .await
                    .map_err(|err| {
                        Error::ConfigError(format!("Failed to build hybrid block cache: {err}"))
                    })?;
                Ok::<HybridCache<K, V>, Error>(cache)
            })
            .map_err(|err| {
                Error::ConfigError(format!("Failed to initialize hybrid cache: {err}"))
            })?;
        Ok(Self {
            backend: FoyerCacheBackend::Hybrid(Arc::new(HybridCacheBackend {
                inner: hybrid,
                runtime,
                cache_root: disk_path.as_ref().to_path_buf(),
            })),
        })
    }
}

impl<K, V> CacheHandle<K, V> for FoyerCache<K, V>
where
    K: foyer::StorageKey + Clone,
    V: foyer::StorageValue + Clone,
{
    fn get(&self, key: &K) -> Option<V> {
        match &self.backend {
            FoyerCacheBackend::Memory(cache) => cache.get(key).map(|entry| entry.value().clone()),
            FoyerCacheBackend::Hybrid(cache) => {
                if let Some(entry) = cache.inner.memory().get(key) {
                    return Some(entry.value().clone());
                }

                cache
                    .runtime
                    .block_on(cache.inner.get(key))
                    .ok()
                    .flatten()
                    .map(|entry| entry.value().clone())
            }
        }
    }

    fn insert(&self, key: K, value: V) {
        match &self.backend {
            FoyerCacheBackend::Memory(cache) => {
                cache.insert(key, value);
            }
            FoyerCacheBackend::Hybrid(cache) => {
                cache.inner.insert(key, value);
            }
        }
    }

    fn remove(&self, key: &K) {
        match &self.backend {
            FoyerCacheBackend::Memory(cache) => {
                cache.remove(key);
            }
            FoyerCacheBackend::Hybrid(cache) => {
                cache.inner.remove(key);
            }
        }
    }

    fn clear(&self) {
        match &self.backend {
            FoyerCacheBackend::Memory(cache) => cache.clear(),
            FoyerCacheBackend::Hybrid(cache) => {
                if let Err(err) = cache.runtime.block_on(cache.inner.clear()) {
                    warn!("failed to clear hybrid block cache: {}", err);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct MockCache<K, V> {
    values: Arc<Mutex<HashMap<K, V>>>,
    get_count: Arc<AtomicUsize>,
    insert_count: Arc<AtomicUsize>,
}

impl<K, V> MockCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            values: Arc::new(Mutex::new(HashMap::new())),
            get_count: Arc::new(AtomicUsize::new(0)),
            insert_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_count(&self) -> usize {
        self.get_count.load(Ordering::Relaxed)
    }

    pub fn insert_count(&self) -> usize {
        self.insert_count.load(Ordering::Relaxed)
    }
}

impl<K, V> CacheHandle<K, V> for MockCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn get(&self, key: &K) -> Option<V> {
        self.get_count.fetch_add(1, Ordering::Relaxed);
        self.values.lock().unwrap().get(key).cloned()
    }

    fn insert(&self, key: K, value: V) {
        self.insert_count.fetch_add(1, Ordering::Relaxed);
        self.values.lock().unwrap().insert(key, value);
    }

    fn remove(&self, key: &K) {
        self.values.lock().unwrap().remove(key);
    }

    fn clear(&self) {
        self.values.lock().unwrap().clear();
    }
}

impl<K, V> Default for MockCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{CacheHandle, FoyerCache, FoyerCacheBackend};

    #[test]
    fn hybrid_get_returns_memory_hit() {
        let directory = tempfile::tempdir().unwrap();
        let cache = FoyerCache::new_hybrid(
            4 * 1024 * 1024,
            16 * 1024 * 1024,
            directory.path(),
            |_, value: &Vec<u8>| value.len(),
        )
        .unwrap();
        let key = 7_u64;
        let value = vec![b'm'; 7 * 1024];

        cache.insert(key, value.clone());

        let FoyerCacheBackend::Hybrid(backend) = &cache.backend else {
            panic!("expected hybrid cache backend");
        };
        assert_eq!(
            backend
                .inner
                .memory()
                .get(&key)
                .map(|entry| entry.value().clone()),
            Some(value.clone())
        );
        assert_eq!(cache.get(&key), Some(value));
    }

    #[test]
    fn hybrid_get_falls_back_to_disk_after_memory_eviction() {
        let directory = tempfile::tempdir().unwrap();
        let cache = FoyerCache::new_hybrid(
            4 * 1024 * 1024,
            16 * 1024 * 1024,
            directory.path(),
            |_, value: &Vec<u8>| value.len(),
        )
        .unwrap();
        let key = 8_u64;
        let value = vec![b'd'; 7 * 1024];

        cache.insert(key, value.clone());

        let FoyerCacheBackend::Hybrid(backend) = &cache.backend else {
            panic!("expected hybrid cache backend");
        };
        backend.inner.memory().evict_all();
        backend.runtime.block_on(backend.inner.storage().wait());
        assert!(backend.inner.memory().get(&key).is_none());

        assert_eq!(cache.get(&key), Some(value));
    }
}
