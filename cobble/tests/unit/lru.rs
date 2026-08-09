use super::*;

#[test]
fn lru_insert_get_update_and_contains() {
    {
        let mut cache = LruCache::new(2);
        cache.insert("a", 1);
        cache.insert("b", 2);
        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.len(), 2);
    }

    {
        let mut cache = LruCache::new(2);
        cache.insert("a", 1);
        cache.insert("a", 10);
        assert_eq!(cache.get(&"a"), Some(&10));
        assert_eq!(cache.len(), 1);
    }

    {
        let mut cache = LruCache::new(2);
        cache.insert("a", 1);
        assert!(cache.contains_key(&"a"));
        assert!(!cache.contains_key(&"b"));
    }
}

#[test]
fn lru_eviction_respects_insertion_and_touch_order() {
    {
        let mut cache = LruCache::new(2);
        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("c", 3); // evicts "a"
        assert_eq!(cache.get(&"a"), None);
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.get(&"c"), Some(&3));
    }

    {
        let mut cache = LruCache::new(2);
        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.get(&"a"); // touch "a", making "b" the LRU
        cache.insert("c", 3); // evicts "b"
        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"c"), Some(&3));
    }

    {
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
}

#[test]
fn lru_removal_clear_and_slot_reuse_preserve_remaining_entries() {
    {
        let mut cache = LruCache::new(3);
        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("c", 3);
        assert_eq!(cache.remove(&"b"), Some(2));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"a"), Some(&1));
        assert_eq!(cache.get(&"c"), Some(&3));
    }

    {
        let mut cache = LruCache::new(3);
        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.get(&"a"), None);
    }

    {
        let mut cache = LruCache::new(2);
        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.remove(&"a");
        cache.insert("c", 3); // reuses the freed slot
        assert_eq!(cache.get(&"c"), Some(&3));
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.len(), 2);
    }
}

#[test]
fn lru_zero_capacity_never_retains_entries() {
    let mut cache: LruCache<&str, i32> = LruCache::new(0);
    cache.insert("a", 1);
    assert_eq!(cache.get(&"a"), None);
    assert_eq!(cache.len(), 0);
}
