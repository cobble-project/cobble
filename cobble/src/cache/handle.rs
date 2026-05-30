use crate::error::{Error, Result};
use foyer::{
    BlockEngineConfig, Cache, CacheBuilder, DeviceBuilder, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder, PsyncIoEngineConfig,
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
        Self {
            backend: FoyerCacheBackend::Memory(
                CacheBuilder::new(capacity).with_weighter(weighter).build(),
            ),
        }
    }

    pub fn new_hybrid(
        memory_capacity: usize,
        disk_capacity: usize,
        disk_path: impl AsRef<Path>,
        weighter: impl Fn(&K, &V) -> usize + Send + Sync + 'static,
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
                let cache = HybridCacheBuilder::new()
                    .with_name("cobble-block-cache")
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
            FoyerCacheBackend::Hybrid(cache) => cache
                .runtime
                .block_on(cache.inner.get(key))
                .ok()
                .flatten()
                .map(|entry| entry.value().clone()),
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
