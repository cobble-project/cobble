use foyer::{Cache, CacheBuilder};
use std::hash::Hash;

pub trait CacheHandle<K, V>: Send + Sync {
    fn get(&self, key: &K) -> Option<V>;
    fn insert(&self, key: K, value: V);
}

#[derive(Clone)]
pub struct FoyerCache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    inner: Cache<K, V>,
}

impl<K, V> FoyerCache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(
        capacity: usize,
        weighter: impl Fn(&K, &V) -> usize + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: CacheBuilder::new(capacity).with_weighter(weighter).build(),
        }
    }
}

impl<K, V> CacheHandle<K, V> for FoyerCache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn get(&self, key: &K) -> Option<V> {
        self.inner.get(key).map(|entry| entry.value().clone())
    }

    fn insert(&self, key: K, value: V) {
        self.inner.insert(key, value);
    }
}

#[derive(Clone)]
pub struct MockCache<K, V> {
    values: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<K, V>>>,
    get_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    insert_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl<K, V> MockCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            values: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            get_count: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            insert_count: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    pub fn get_count(&self) -> usize {
        self.get_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn insert_count(&self) -> usize {
        self.insert_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl<K, V> CacheHandle<K, V> for MockCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn get(&self, key: &K) -> Option<V> {
        self.get_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.values.lock().unwrap().get(key).cloned()
    }

    fn insert(&self, key: K, value: V) {
        self.insert_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
