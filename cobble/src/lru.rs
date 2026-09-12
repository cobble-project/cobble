use std::collections::HashMap;
use std::hash::Hash;

const SENTINEL: usize = usize::MAX;

/// Node in a doubly-linked list stored in a flat Vec-based slab.
/// Value is wrapped in Option so we can move it out without requiring Default.
struct Node<K, V> {
    key: K,
    value: Option<V>,
    prev: usize,
    next: usize,
}

/// O(1) LRU cache using a HashMap for key lookup and a Vec-based
/// doubly-linked list for recency ordering. All get/insert/remove/touch
/// operations are O(1) amortized.
pub(crate) struct LruCache<K, V> {
    capacity: usize,
    /// Maps key → slab index for O(1) lookup.
    index: HashMap<K, usize>,
    /// Flat slab storing linked-list nodes.
    nodes: Vec<Node<K, V>>,
    /// Free-list of recycled slab slots.
    free: Vec<usize>,
    /// Index of the most-recently-used node (list head), or SENTINEL if empty.
    head: usize,
    /// Index of the least-recently-used node (list tail), or SENTINEL if empty.
    tail: usize,
}

impl<K, V> LruCache<K, V>
where
    K: Eq + Hash + Clone,
{
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            index: HashMap::with_capacity(capacity),
            nodes: Vec::with_capacity(capacity),
            free: Vec::new(),
            head: SENTINEL,
            tail: SENTINEL,
        }
    }

    pub(crate) fn get(&mut self, key: &K) -> Option<&V> {
        let idx = *self.index.get(key)?;
        self.move_to_head(idx);
        self.nodes[idx].value.as_ref()
    }

    pub(crate) fn insert(&mut self, key: K, value: V) {
        if self.capacity == 0 {
            return;
        }
        if let Some(&idx) = self.index.get(&key) {
            self.nodes[idx].value = Some(value);
            self.move_to_head(idx);
            return;
        }
        if self.index.len() == self.capacity {
            self.evict_tail();
        }
        let idx = self.alloc_node(key.clone(), value);
        self.push_head(idx);
        self.index.insert(key, idx);
    }

    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let idx = self.index.remove(key)?;
        self.unlink(idx);
        let value = self.nodes[idx].value.take();
        self.free.push(idx);
        value
    }

    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.index.contains_key(key)
    }

    pub(crate) fn len(&self) -> usize {
        self.index.len()
    }

    pub(crate) fn clear(&mut self) {
        self.index.clear();
        self.nodes.clear();
        self.free.clear();
        self.head = SENTINEL;
        self.tail = SENTINEL;
    }

    /// Allocate a slab slot, reusing a free slot if available.
    fn alloc_node(&mut self, key: K, value: V) -> usize {
        if let Some(idx) = self.free.pop() {
            self.nodes[idx] = Node {
                key,
                value: Some(value),
                prev: SENTINEL,
                next: SENTINEL,
            };
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(Node {
                key,
                value: Some(value),
                prev: SENTINEL,
                next: SENTINEL,
            });
            idx
        }
    }

    /// Insert node at the head of the linked list (most recently used).
    fn push_head(&mut self, idx: usize) {
        self.nodes[idx].prev = SENTINEL;
        self.nodes[idx].next = self.head;
        if self.head != SENTINEL {
            self.nodes[self.head].prev = idx;
        }
        self.head = idx;
        if self.tail == SENTINEL {
            self.tail = idx;
        }
    }

    /// Remove a node from wherever it sits in the linked list.
    fn unlink(&mut self, idx: usize) {
        let prev = self.nodes[idx].prev;
        let next = self.nodes[idx].next;
        if prev != SENTINEL {
            self.nodes[prev].next = next;
        } else {
            self.head = next;
        }
        if next != SENTINEL {
            self.nodes[next].prev = prev;
        } else {
            self.tail = prev;
        }
        self.nodes[idx].prev = SENTINEL;
        self.nodes[idx].next = SENTINEL;
    }

    /// Move an existing node to the head (most recently used).
    fn move_to_head(&mut self, idx: usize) {
        if self.head == idx {
            return;
        }
        self.unlink(idx);
        self.push_head(idx);
    }

    /// Evict the least-recently-used entry (tail).
    fn evict_tail(&mut self) {
        if self.tail == SENTINEL {
            return;
        }
        let idx = self.tail;
        self.unlink(idx);
        self.index.remove(&self.nodes[idx].key);
        self.nodes[idx].value = None;
        self.free.push(idx);
    }
}

#[cfg(test)]
#[path = "../tests/unit/lru.rs"]
mod tests;
