use crate::error::Result;
use crate::iterator::KvIterator;
use bytes::Bytes;
use std::cmp::Ordering;

/// A merging iterator that combines multiple sorted iterators into a single
/// globally ordered iterator.
///
/// This is commonly used in LSM-tree implementations to merge data from
/// multiple levels or runs during reads or compaction.
pub struct MergingIterator<I> {
    /// The child iterators being merged.
    iterators: Vec<I>,
    /// The min-heap of iterator indices for efficient minimum key selection.
    heap: Vec<usize>,
    /// The index of the current (smallest) iterator.
    current_idx: Option<usize>,
}

impl<I> MergingIterator<I> {
    /// Create a new MergingIterator from a list of child iterators.
    pub fn new(iterators: Vec<I>) -> Self {
        Self {
            iterators,
            heap: Vec::new(),
            current_idx: None,
        }
    }

    /// Rebuild the heap with all valid iterators.
    fn rebuild_heap<'a>(&mut self) -> Result<()>
    where
        I: KvIterator<'a>,
    {
        let mut indices = Vec::new();
        for (idx, iter) in self.iterators.iter().enumerate() {
            if iter.valid() && iter.key_slice()?.is_some() {
                indices.push(idx);
            }
        }

        self.heap.clear();
        for idx in indices {
            self.push_heap(idx)?;
        }

        self.current_idx = self.heap.first().copied();
        Ok(())
    }

    fn compare_iters<'a>(&self, left_idx: usize, right_idx: usize) -> Result<Ordering>
    where
        I: KvIterator<'a>,
    {
        let left = self.iterators[left_idx].key_slice()?;
        let right = self.iterators[right_idx].key_slice()?;
        let ord = match (left, right) {
            (Some(left), Some(right)) => left.cmp(right),
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (None, None) => Ordering::Equal,
        };
        if ord == Ordering::Equal {
            Ok(left_idx.cmp(&right_idx))
        } else {
            Ok(ord)
        }
    }

    fn push_heap<'a>(&mut self, idx: usize) -> Result<()>
    where
        I: KvIterator<'a>,
    {
        self.heap.push(idx);
        self.sift_up(self.heap.len().saturating_sub(1))
    }

    fn pop_heap<'a>(&mut self) -> Result<Option<usize>>
    where
        I: KvIterator<'a>,
    {
        let Some(last) = self.heap.pop() else {
            return Ok(None);
        };
        if self.heap.is_empty() {
            return Ok(Some(last));
        }
        let min = self.heap[0];
        self.heap[0] = last;
        self.sift_down(0)?;
        Ok(Some(min))
    }

    fn sift_up<'a>(&mut self, mut idx: usize) -> Result<()>
    where
        I: KvIterator<'a>,
    {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if self.compare_iters(self.heap[idx], self.heap[parent])? == Ordering::Less {
                self.heap.swap(idx, parent);
                idx = parent;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn sift_down<'a>(&mut self, mut idx: usize) -> Result<()>
    where
        I: KvIterator<'a>,
    {
        let len = self.heap.len();
        loop {
            let left = idx * 2 + 1;
            let right = left + 1;
            if left >= len {
                break;
            }
            let mut smallest = left;
            if right < len
                && self.compare_iters(self.heap[right], self.heap[left])? == Ordering::Less
            {
                smallest = right;
            }
            if self.compare_iters(self.heap[smallest], self.heap[idx])? == Ordering::Less {
                self.heap.swap(idx, smallest);
                idx = smallest;
            } else {
                break;
            }
        }
        Ok(())
    }
}

impl<'a, I> KvIterator<'a> for MergingIterator<I>
where
    I: KvIterator<'a>,
{
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
        let Some(iter_idx) = self.pop_heap()? else {
            self.current_idx = None;
            return Ok(false);
        };

        // Advance the iterator that had the minimum
        if let Some(iter) = self.iterators.get_mut(iter_idx) {
            iter.next()?;

            // Re-add to heap if still valid
            if iter.valid() && iter.key_slice()?.is_some() {
                self.push_heap(iter_idx)?;
            }
        }

        // Update current_idx to the new minimum
        self.current_idx = self.heap.first().copied();

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

    fn key_slice(&self) -> Result<Option<&[u8]>> {
        if let Some(idx) = self.current_idx {
            self.iterators[idx].key_slice()
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

    fn value_slice(&self) -> Result<Option<&[u8]>> {
        if let Some(idx) = self.current_idx {
            self.iterators[idx].value_slice()
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iterator::mock_iterator::MockIterator;

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
