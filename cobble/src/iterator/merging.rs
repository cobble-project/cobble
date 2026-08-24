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
        // The paused child was removed by pop_heap(), while every other child
        // stayed in a valid heap. Incrementally reinsert each resumed child;
        // only expose the root after every child has a comparable next key.
        if self.paused_iterators.is_empty() {
            self.current_idx = self.heap.first().copied();
        }
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
            if !self.paused_iterators.is_empty() {
                // A child can encounter another physical boundary while
                // resuming (for example, a data-block boundary followed by a
                // file boundary). Its next key is still unknown, so rows from
                // the parked heap cannot yet be returned safely.
                self.current_idx = None;
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
        let mut stopped_at_child_boundary = false;
        if let Some(iter) = self.iterators.get_mut(iter_idx) {
            let advanced = iter.next()?;
            if !advanced {
                if iter.stopped_at_block_boundary() {
                    self.paused_iterators.push(iter_idx);
                    stopped_at_child_boundary = true;
                }
            } else if iter.valid() && iter.key()?.is_some() {
                self.push_heap(iter_idx)?;
            }
        }

        // Once any child pauses, its next key is unknown until that child is
        // resumed. Do not expose rows from the remaining heap in the meantime:
        // they may sort after the paused child's next block. Keep the heap
        // intact and rebuild the globally ordered position after clear/resume.
        if stopped_at_child_boundary {
            self.current_idx = None;
            self.stopped_at_block_boundary = true;
            return Ok(false);
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
#[path = "../../tests/unit/iterator/merging.rs"]
mod tests;
