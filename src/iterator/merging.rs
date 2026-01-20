use crate::error::Result;
use crate::iterator::KvIterator;
use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// A wrapper for iterator entries in the heap.
/// This stores the current key and the index of the iterator.
struct HeapEntry {
    /// The current key from the iterator.
    key: Bytes,
    /// The index of the iterator in the iterators vector.
    iter_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.iter_idx == other.iter_idx
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior
        // First compare by key (smaller key has higher priority)
        // Then by iterator index (smaller index has higher priority for stability)
        match other.key.cmp(&self.key) {
            Ordering::Equal => other.iter_idx.cmp(&self.iter_idx),
            ord => ord,
        }
    }
}

/// A merging iterator that combines multiple sorted iterators into a single
/// globally ordered iterator.
///
/// This is commonly used in LSM-tree implementations to merge data from
/// multiple levels or runs during reads or compaction.
pub struct MergingIterator<I: KvIterator> {
    /// The child iterators being merged.
    iterators: Vec<I>,
    /// The min-heap for efficient minimum key selection.
    heap: BinaryHeap<HeapEntry>,
    /// The index of the current (smallest) iterator.
    current_idx: Option<usize>,
}

impl<I: KvIterator> MergingIterator<I> {
    /// Create a new MergingIterator from a list of child iterators.
    pub fn new(iterators: Vec<I>) -> Self {
        Self {
            iterators,
            heap: BinaryHeap::new(),
            current_idx: None,
        }
    }

    /// Rebuild the heap with all valid iterators.
    fn rebuild_heap(&mut self) -> Result<()> {
        self.heap.clear();

        for (idx, iter) in self.iterators.iter().enumerate() {
            if iter.valid()
                && let Some(key) = iter.key()?
            {
                self.heap.push(HeapEntry { key, iter_idx: idx });
            }
        }

        self.current_idx = self.heap.peek().map(|e| e.iter_idx);
        Ok(())
    }
}

impl<I: KvIterator> KvIterator for MergingIterator<I> {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        // Seek all iterators to the target
        for iter in &mut self.iterators {
            iter.seek(target)?;
        }

        // Rebuild the heap
        self.rebuild_heap()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        // Seek all iterators to first
        for iter in &mut self.iterators {
            iter.seek_to_first()?;
        }

        // Rebuild the heap
        self.rebuild_heap()
    }

    fn next(&mut self) -> Result<bool> {
        // Pop the current minimum from the heap
        let Some(entry) = self.heap.pop() else {
            self.current_idx = None;
            return Ok(false);
        };

        let iter_idx = entry.iter_idx;

        // Advance the iterator that had the minimum
        if let Some(iter) = self.iterators.get_mut(iter_idx) {
            iter.next()?;

            // Re-add to heap if still valid
            if iter.valid()
                && let Some(key) = iter.key()?
            {
                self.heap.push(HeapEntry { key, iter_idx });
            }
        }

        // Update current_idx to the new minimum
        self.current_idx = self.heap.peek().map(|e| e.iter_idx);

        Ok(self.current_idx.is_some())
    }

    fn valid(&self) -> bool {
        self.current_idx.is_some()
    }

    fn key(&self) -> Result<Option<Bytes>> {
        if let Some(idx) = self.current_idx {
            self.iterators[idx].key()
        } else {
            Ok(None)
        }
    }

    fn value(&self) -> Result<Option<Bytes>> {
        if let Some(idx) = self.current_idx {
            self.iterators[idx].value()
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A simple mock iterator for testing
    struct MockIterator {
        entries: Vec<(Bytes, Bytes)>,
        index: usize,
    }

    impl MockIterator {
        fn new<K: AsRef<[u8]>, V: AsRef<[u8]>>(entries: Vec<(K, V)>) -> Self {
            Self {
                entries: entries
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            Bytes::copy_from_slice(k.as_ref()),
                            Bytes::copy_from_slice(v.as_ref()),
                        )
                    })
                    .collect(),
                index: usize::MAX, // Invalid until seek
            }
        }
    }

    impl KvIterator for MockIterator {
        fn seek(&mut self, target: &[u8]) -> Result<()> {
            self.index = self
                .entries
                .iter()
                .position(|(k, _)| k.as_ref() >= target)
                .unwrap_or(self.entries.len());
            Ok(())
        }

        fn seek_to_first(&mut self) -> Result<()> {
            self.index = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<bool> {
            if self.index < self.entries.len() {
                self.index += 1;
                Ok(self.index < self.entries.len())
            } else {
                Ok(false)
            }
        }

        fn valid(&self) -> bool {
            self.index < self.entries.len()
        }

        fn key(&self) -> Result<Option<Bytes>> {
            if self.valid() {
                Ok(Some(self.entries[self.index].0.clone()))
            } else {
                Ok(None)
            }
        }

        fn value(&self) -> Result<Option<Bytes>> {
            if self.valid() {
                Ok(Some(self.entries[self.index].1.clone()))
            } else {
                Ok(None)
            }
        }
    }

    #[test]
    fn test_merging_iterator_basic() {
        let iter1 = MockIterator::new(vec![(b"a".as_slice(), b"1"), (b"c", b"3"), (b"e", b"5")]);
        let iter2 = MockIterator::new(vec![(b"b".as_slice(), b"2"), (b"d", b"4"), (b"f", b"6")]);

        let mut merger = MergingIterator::new(vec![iter1, iter2]);
        merger.seek_to_first().unwrap();

        let mut results = vec![];
        while merger.valid() {
            let (k, v) = merger.current().unwrap().unwrap();
            results.push((k, v));
            merger.next().unwrap();
        }

        assert_eq!(results.len(), 6);
        assert_eq!(results[0].0.as_ref(), b"a");
        assert_eq!(results[1].0.as_ref(), b"b");
        assert_eq!(results[2].0.as_ref(), b"c");
        assert_eq!(results[3].0.as_ref(), b"d");
        assert_eq!(results[4].0.as_ref(), b"e");
        assert_eq!(results[5].0.as_ref(), b"f");
    }

    #[test]
    fn test_merging_iterator_overlapping_keys() {
        // Test with duplicate keys across iterators
        let iter1 = MockIterator::new(vec![
            (b"a".as_slice(), b"v1-a"),
            (b"b", b"v1-b"),
            (b"c", b"v1-c"),
        ]);
        let iter2 = MockIterator::new(vec![
            (b"a".as_slice(), b"v2-a"),
            (b"b", b"v2-b"),
            (b"d", b"v2-d"),
        ]);

        let mut merger = MergingIterator::new(vec![iter1, iter2]);
        merger.seek_to_first().unwrap();

        let mut results = vec![];
        while merger.valid() {
            let (k, v) = merger.current().unwrap().unwrap();
            results.push((k, v));
            merger.next().unwrap();
        }

        // All entries should be present, including duplicates
        assert_eq!(results.len(), 6);
        assert_eq!(results[0].0.as_ref(), b"a");
        assert_eq!(results[0].1.as_ref(), b"v1-a"); // First iterator wins for same key
        assert_eq!(results[1].0.as_ref(), b"a");
        assert_eq!(results[1].1.as_ref(), b"v2-a");
        assert_eq!(results[2].0.as_ref(), b"b");
        assert_eq!(results[3].0.as_ref(), b"b");
        assert_eq!(results[4].0.as_ref(), b"c");
        assert_eq!(results[5].0.as_ref(), b"d");
    }

    #[test]
    fn test_merging_iterator_seek() {
        let iter1 = MockIterator::new(vec![(b"a".as_slice(), b"1"), (b"c", b"3"), (b"e", b"5")]);
        let iter2 = MockIterator::new(vec![(b"b".as_slice(), b"2"), (b"d", b"4"), (b"f", b"6")]);

        let mut merger = MergingIterator::new(vec![iter1, iter2]);

        // Seek to "c"
        merger.seek(b"c").unwrap();
        assert!(merger.valid());
        assert_eq!(merger.key().unwrap().unwrap().as_ref(), b"c");

        // Seek to "d"
        merger.seek(b"d").unwrap();
        assert!(merger.valid());
        assert_eq!(merger.key().unwrap().unwrap().as_ref(), b"d");

        // Seek to non-existent key between entries
        merger.seek(b"ca").unwrap();
        assert!(merger.valid());
        assert_eq!(merger.key().unwrap().unwrap().as_ref(), b"d");
    }

    #[test]
    fn test_merging_iterator_empty() {
        let iter1 = MockIterator::new(Vec::<(&[u8], &[u8])>::new());
        let iter2 = MockIterator::new(Vec::<(&[u8], &[u8])>::new());

        let mut merger = MergingIterator::new(vec![iter1, iter2]);
        merger.seek_to_first().unwrap();

        assert!(!merger.valid());
        assert!(merger.current().unwrap().is_none());
    }

    #[test]
    fn test_merging_iterator_single() {
        let iter1 = MockIterator::new(vec![(b"a".as_slice(), b"1"), (b"b", b"2"), (b"c", b"3")]);

        let mut merger = MergingIterator::new(vec![iter1]);
        merger.seek_to_first().unwrap();

        let mut count = 0;
        while merger.valid() {
            count += 1;
            merger.next().unwrap();
        }

        assert_eq!(count, 3);
    }

    #[test]
    fn test_merging_iterator_many() {
        let iter1 = MockIterator::new(vec![(b"a".as_slice(), b"1"), (b"d", b"4")]);
        let iter2 = MockIterator::new(vec![(b"b".as_slice(), b"2"), (b"e", b"5")]);
        let iter3 = MockIterator::new(vec![(b"c".as_slice(), b"3"), (b"f", b"6")]);

        let mut merger = MergingIterator::new(vec![iter1, iter2, iter3]);
        merger.seek_to_first().unwrap();

        let mut results = vec![];
        while merger.valid() {
            let (k, _) = merger.current().unwrap().unwrap();
            results.push(k);
            merger.next().unwrap();
        }

        assert_eq!(results.len(), 6);
        assert_eq!(results[0].as_ref(), b"a");
        assert_eq!(results[1].as_ref(), b"b");
        assert_eq!(results[2].as_ref(), b"c");
        assert_eq!(results[3].as_ref(), b"d");
        assert_eq!(results[4].as_ref(), b"e");
        assert_eq!(results[5].as_ref(), b"f");
    }

    #[test]
    fn test_merging_iterator_one_empty() {
        let iter1 = MockIterator::new(vec![(b"a".as_slice(), b"1".as_slice()), (b"c", b"3")]);
        let iter2 = MockIterator::new(Vec::<(&[u8], &[u8])>::new());
        let iter3 = MockIterator::new(vec![(b"b".as_slice(), b"2".as_slice()), (b"d", b"4")]);

        let mut merger = MergingIterator::new(vec![iter1, iter2, iter3]);
        merger.seek_to_first().unwrap();

        let mut results = vec![];
        while merger.valid() {
            let (k, _) = merger.current().unwrap().unwrap();
            results.push(k);
            merger.next().unwrap();
        }

        assert_eq!(results.len(), 4);
        assert_eq!(results[0].as_ref(), b"a");
        assert_eq!(results[1].as_ref(), b"b");
        assert_eq!(results[2].as_ref(), b"c");
        assert_eq!(results[3].as_ref(), b"d");
    }
}
