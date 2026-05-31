use crate::error::Result;
use crate::iterator::KvIterator;
use crate::r#type::KvValue;
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
    /// Child iterators that temporarily returned `false` because they hit a
    /// boundary stop. They are not eligible for the heap until callers clear
    /// the stop and we explicitly resume them.
    paused_iterators: Vec<usize>,
    /// Whether the merge as a whole has already surfaced a boundary stop to the
    /// caller. While this remains true, `next()` must keep returning `false`.
    stopped_at_block_boundary: bool,
}

impl<I> MergingIterator<I> {
    /// Create a new MergingIterator from a list of child iterators.
    pub fn new(iterators: Vec<I>) -> Self {
        Self {
            iterators,
            heap: Vec::new(),
            current_idx: None,
            paused_iterators: Vec::new(),
            stopped_at_block_boundary: false,
        }
    }

    /// Rebuild the heap with all valid iterators.
    fn rebuild_heap<'a>(&mut self) -> Result<()>
    where
        I: KvIterator<'a>,
    {
        let mut indices = Vec::new();
        for (idx, iter) in self.iterators.iter().enumerate() {
            if iter.valid() && iter.key()?.is_some() {
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
        let left = self.iterators[left_idx].key()?;
        let right = self.iterators[right_idx].key()?;
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

    fn resume_paused_iterators<'a>(&mut self) -> Result<()>
    where
        I: KvIterator<'a>,
    {
        let paused_iterators = std::mem::take(&mut self.paused_iterators);
        for idx in paused_iterators {
            let iter = &mut self.iterators[idx];
            let advanced = iter.next()?;
            if !advanced {
                if iter.stopped_at_block_boundary() {
                    self.paused_iterators.push(idx);
                }
                continue;
            }
            if iter.valid() && iter.key()?.is_some() {
                self.push_heap(idx)?;
            }
        }
        self.current_idx = self.heap.first().copied();
        Ok(())
    }
}

impl<'a, I> KvIterator<'a> for MergingIterator<I>
where
    I: KvIterator<'a>,
{
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.stopped_at_block_boundary = false;
        self.paused_iterators.clear();
        // Seek all iterators to the target
        for iter in &mut self.iterators {
            iter.seek(target)?;
        }

        // Rebuild the heap
        self.rebuild_heap()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.stopped_at_block_boundary = false;
        self.paused_iterators.clear();
        // Seek all iterators to first
        for iter in &mut self.iterators {
            iter.seek_to_first()?;
        }

        // Rebuild the heap
        self.rebuild_heap()
    }

    fn next(&mut self) -> Result<bool> {
        if self.stopped_at_block_boundary {
            return Ok(false);
        }
        if self.current_idx.is_none() {
            if self.paused_iterators.is_empty() {
                return Ok(false);
            }
            self.resume_paused_iterators()?;
            if self.current_idx.is_none() && !self.paused_iterators.is_empty() {
                self.stopped_at_block_boundary = true;
                return Ok(false);
            }
            return Ok(self.current_idx.is_some());
        }
        // Pop the current minimum from the heap
        let Some(iter_idx) = self.pop_heap()? else {
            self.current_idx = None;
            return Ok(false);
        };

        // Advance the iterator that had the minimum
        if let Some(iter) = self.iterators.get_mut(iter_idx) {
            let advanced = iter.next()?;
            if !advanced {
                if iter.stopped_at_block_boundary() {
                    self.paused_iterators.push(iter_idx);
                }
            } else if iter.valid() && iter.key()?.is_some() {
                self.push_heap(iter_idx)?;
            }
        }

        // Update current_idx to the new minimum
        self.current_idx = self.heap.first().copied();

        if self.current_idx.is_none() && !self.paused_iterators.is_empty() {
            self.stopped_at_block_boundary = true;
            return Ok(false);
        }

        Ok(self.current_idx.is_some())
    }

    fn valid(&self) -> bool {
        self.current_idx.is_some()
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        if let Some(idx) = self.current_idx {
            self.iterators[idx].key()
        } else {
            Ok(None)
        }
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        if let Some(idx) = self.current_idx {
            self.iterators[idx].take_key()
        } else {
            Ok(None)
        }
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        if let Some(idx) = self.current_idx {
            self.iterators[idx].take_value()
        } else {
            Ok(None)
        }
    }

    fn set_stop_at_block_boundary(&mut self, enabled: bool) {
        self.paused_iterators.clear();
        self.stopped_at_block_boundary = false;
        for iter in &mut self.iterators {
            iter.set_stop_at_block_boundary(enabled);
        }
    }

    fn clear_stop_at_block_boundary(&mut self) {
        self.stopped_at_block_boundary = false;
        for iter in &mut self.iterators {
            iter.clear_stop_at_block_boundary();
        }
    }

    fn stopped_at_block_boundary(&self) -> bool {
        self.stopped_at_block_boundary
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iterator::mock_iterator::MockIterator;
    use bytes::Bytes;

    struct BoundaryMockIterator {
        entries: Vec<(Bytes, Bytes)>,
        index: usize,
        pause_after_index: Option<usize>,
        should_stop_at_block_boundary: bool,
        pending_resume: bool,
        stopped_at_block_boundary: bool,
    }

    impl BoundaryMockIterator {
        fn new<K: AsRef<[u8]>, V: AsRef<[u8]>>(
            entries: Vec<(K, V)>,
            pause_after_index: Option<usize>,
        ) -> Self {
            Self {
                entries: entries
                    .into_iter()
                    .map(|(key, value)| {
                        (
                            Bytes::copy_from_slice(key.as_ref()),
                            Bytes::copy_from_slice(value.as_ref()),
                        )
                    })
                    .collect(),
                index: usize::MAX,
                pause_after_index,
                should_stop_at_block_boundary: false,
                pending_resume: false,
                stopped_at_block_boundary: false,
            }
        }
    }

    impl<'a> KvIterator<'a> for BoundaryMockIterator {
        fn seek(&mut self, target: &[u8]) -> Result<()> {
            self.pending_resume = false;
            self.stopped_at_block_boundary = false;
            self.index = self
                .entries
                .iter()
                .position(|(key, _)| key.as_ref() >= target)
                .unwrap_or(self.entries.len());
            Ok(())
        }

        fn seek_to_first(&mut self) -> Result<()> {
            self.pending_resume = false;
            self.stopped_at_block_boundary = false;
            self.index = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<bool> {
            if self.stopped_at_block_boundary {
                return Ok(false);
            }
            self.stopped_at_block_boundary = false;
            if self.pending_resume {
                self.pending_resume = false;
                self.index += 1;
                return Ok(self.index < self.entries.len());
            }
            if self.index >= self.entries.len() {
                return Ok(false);
            }
            if self.should_stop_at_block_boundary && self.pause_after_index == Some(self.index) {
                self.pending_resume = true;
                self.stopped_at_block_boundary = true;
                return Ok(false);
            }
            self.index += 1;
            Ok(self.index < self.entries.len())
        }

        fn valid(&self) -> bool {
            self.index < self.entries.len()
        }

        fn key(&self) -> Result<Option<&[u8]>> {
            Ok(self.entries.get(self.index).map(|(key, _)| key.as_ref()))
        }

        fn take_key(&mut self) -> Result<Option<Bytes>> {
            Ok(self.entries.get(self.index).map(|(key, _)| key.clone()))
        }

        fn take_value(&mut self) -> Result<Option<KvValue>> {
            Ok(self
                .entries
                .get(self.index)
                .map(|(_, value)| KvValue::Encoded(value.clone())))
        }

        fn set_stop_at_block_boundary(&mut self, enabled: bool) {
            self.should_stop_at_block_boundary = enabled;
            self.pending_resume = false;
            self.stopped_at_block_boundary = false;
        }

        fn clear_stop_at_block_boundary(&mut self) {
            self.stopped_at_block_boundary = false;
        }

        fn stopped_at_block_boundary(&self) -> bool {
            self.stopped_at_block_boundary
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
            let (k, kv) = merger.take_current().unwrap().unwrap();
            let v = kv.unwrap_encoded();
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
            let (k, kv) = merger.take_current().unwrap().unwrap();
            let v = kv.unwrap_encoded();
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
        assert_eq!(merger.key().unwrap().unwrap(), b"c");

        // Seek to "d"
        merger.seek(b"d").unwrap();
        assert!(merger.valid());
        assert_eq!(merger.key().unwrap().unwrap(), b"d");

        // Seek to non-existent key between entries
        merger.seek(b"ca").unwrap();
        assert!(merger.valid());
        assert_eq!(merger.key().unwrap().unwrap(), b"d");
    }

    #[test]
    fn test_merging_iterator_empty() {
        let iter1 = MockIterator::new(Vec::<(&[u8], &[u8])>::new());
        let iter2 = MockIterator::new(Vec::<(&[u8], &[u8])>::new());

        let mut merger = MergingIterator::new(vec![iter1, iter2]);
        merger.seek_to_first().unwrap();

        assert!(!merger.valid());
        assert!(merger.take_current().unwrap().is_none());
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
            let (k, _) = merger.take_current().unwrap().unwrap();
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
    fn test_merging_iterator_resumes_after_child_boundary_stop() {
        let iter1 = BoundaryMockIterator::new(vec![(b"a".as_slice(), b"1"), (b"e", b"5")], Some(0));
        let iter2 = BoundaryMockIterator::new(vec![(b"b".as_slice(), b"2"), (b"c", b"3")], None);
        let mut merger = MergingIterator::new(vec![iter1, iter2]);
        merger.set_stop_at_block_boundary(true);
        merger.seek_to_first().unwrap();

        let mut keys: Vec<Bytes> = vec![merger.take_current().unwrap().unwrap().0];
        loop {
            if merger.next().unwrap() {
                keys.push(merger.take_current().unwrap().unwrap().0);
                continue;
            }
            if merger.stopped_at_block_boundary() {
                assert_eq!(keys.last().unwrap().as_ref(), b"c");
                merger.clear_stop_at_block_boundary();
                continue;
            }
            break;
        }

        assert_eq!(
            keys,
            vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
                Bytes::from_static(b"e"),
            ]
        );
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
            let (k, _) = merger.take_current().unwrap().unwrap();
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
