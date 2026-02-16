use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

pub(crate) struct LruCache<K, V> {
    capacity: usize,
    map: HashMap<K, V>,
    order: VecDeque<K>,
}

impl<K, V> LruCache<K, V>
where
    K: Eq + Hash + Clone,
{
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub(crate) fn get(&mut self, key: &K) -> Option<&V> {
        if self.map.contains_key(key) {
            self.touch(key);
        }
        self.map.get(key)
    }

    pub(crate) fn insert(&mut self, key: K, value: V) {
        if self.capacity == 0 {
            return;
        }
        if let std::collections::hash_map::Entry::Occupied(mut entry) = self.map.entry(key.clone())
        {
            entry.insert(value);
            self.touch(&key);
            return;
        }
        if self.map.len() == self.capacity
            && let Some(evicted) = self.order.pop_back()
        {
            self.map.remove(&evicted);
        }
        self.order.push_front(key.clone());
        self.map.insert(key, value);
    }

    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let value = self.map.remove(key);
        if let Some(pos) = self.order.iter().position(|entry| entry == key) {
            self.order.remove(pos);
        }
        value
    }

    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
    }

    fn touch(&mut self, key: &K) {
        if let Some(pos) = self.order.iter().position(|entry| entry == key) {
            self.order.remove(pos);
        }
        self.order.push_front(key.clone());
    }
}
