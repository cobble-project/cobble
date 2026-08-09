use super::*;

#[test]
fn test_lru_basic_insert_get() {
    let mut cache = LruCache::new(2);
    cache.insert("a", 1);
    cache.insert("b", 2);
    assert_eq!(cache.get(&"a"), Some(&1));
    assert_eq!(cache.get(&"b"), Some(&2));
    assert_eq!(cache.len(), 2);
}

#[test]
fn test_lru_eviction() {
    let mut cache = LruCache::new(2);
    cache.insert("a", 1);
    cache.insert("b", 2);
    cache.insert("c", 3); // evicts "a"
    assert_eq!(cache.get(&"a"), None);
    assert_eq!(cache.get(&"b"), Some(&2));
    assert_eq!(cache.get(&"c"), Some(&3));
}

#[test]
fn test_lru_touch_prevents_eviction() {
    let mut cache = LruCache::new(2);
    cache.insert("a", 1);
    cache.insert("b", 2);
    cache.get(&"a"); // touch "a", making "b" the LRU
    cache.insert("c", 3); // evicts "b"
    assert_eq!(cache.get(&"a"), Some(&1));
    assert_eq!(cache.get(&"b"), None);
    assert_eq!(cache.get(&"c"), Some(&3));
}

#[test]
fn test_lru_update_existing() {
    let mut cache = LruCache::new(2);
    cache.insert("a", 1);
    cache.insert("a", 10);
    assert_eq!(cache.get(&"a"), Some(&10));
    assert_eq!(cache.len(), 1);
}

#[test]
fn test_lru_remove() {
    let mut cache = LruCache::new(3);
    cache.insert("a", 1);
    cache.insert("b", 2);
    cache.insert("c", 3);
    assert_eq!(cache.remove(&"b"), Some(2));
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.get(&"b"), None);
    // Remaining entries still accessible
    assert_eq!(cache.get(&"a"), Some(&1));
    assert_eq!(cache.get(&"c"), Some(&3));
}

#[test]
fn test_lru_zero_capacity() {
    let mut cache: LruCache<&str, i32> = LruCache::new(0);
    cache.insert("a", 1);
    assert_eq!(cache.get(&"a"), None);
    assert_eq!(cache.len(), 0);
}

#[test]
fn test_lru_clear() {
    let mut cache = LruCache::new(3);
    cache.insert("a", 1);
    cache.insert("b", 2);
    cache.clear();
    assert_eq!(cache.len(), 0);
    assert_eq!(cache.get(&"a"), None);
}

#[test]
fn test_lru_contains_key() {
    let mut cache = LruCache::new(2);
    cache.insert("a", 1);
    assert!(cache.contains_key(&"a"));
    assert!(!cache.contains_key(&"b"));
}

#[test]
fn test_lru_slot_reuse_after_remove() {
    let mut cache = LruCache::new(2);
    cache.insert("a", 1);
    cache.insert("b", 2);
    cache.remove(&"a");
    cache.insert("c", 3); // should reuse the freed slot
    assert_eq!(cache.get(&"c"), Some(&3));
    assert_eq!(cache.get(&"b"), Some(&2));
    assert_eq!(cache.len(), 2);
}

#[test]
fn test_lru_eviction_order_after_multiple_touches() {
    let mut cache = LruCache::new(3);
    cache.insert("a", 1);
    cache.insert("b", 2);
    cache.insert("c", 3);
    // Touch order: c(most recent) -> b -> a(least recent)
    cache.get(&"a"); // now a is most recent
    cache.get(&"b"); // now b is most recent
    // Order: b -> a -> c
    cache.insert("d", 4); // evicts "c"
    assert_eq!(cache.get(&"c"), None);
    assert_eq!(cache.get(&"a"), Some(&1));
    assert_eq!(cache.get(&"b"), Some(&2));
    assert_eq!(cache.get(&"d"), Some(&4));
}
