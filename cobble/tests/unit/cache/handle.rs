use super::{CacheHandle, FoyerCache, FoyerCacheBackend};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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
