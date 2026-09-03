use crate::data_file::DataFile;
use crate::error::Result;
use crate::iterator::KvIterator;
use crate::r#type::KvValue;
use bytes::Bytes;
use std::sync::Arc;

/// A sorted run is a sequence of data files where key ranges are non-overlapping
/// and sorted. This is common in LSM-tree structures where each level (except L0)
/// contains files with non-overlapping key ranges.
pub struct SortedRun {
    /// The data files in this sorted run, ordered by their key ranges.
    files: Vec<Arc<DataFile>>,
    level: u8,
}

impl SortedRun {
    /// Create a new SortedRun from a list of data files.
    /// The files should already be sorted by their key ranges.
    pub fn new(level: u8, files: Vec<Arc<DataFile>>) -> Self {
        Self { files, level }
    }

    /// Get the number of files in this sorted run.
    pub fn len(&self) -> usize {
        self.files.len()
    }

    /// Check if this sorted run is empty.
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Get the files in this sorted run.
    pub fn files(&self) -> &[Arc<DataFile>] {
        &self.files
    }

    pub fn level(&self) -> u8 {
        self.level
    }

    /// Get the start key of this sorted run (the smallest key).
    pub fn start_key(&self) -> Option<&[u8]> {
        self.files.first().map(|f| f.start_key.as_slice())
    }

    /// Get the end key of this sorted run (the largest key).
    pub fn end_key(&self) -> Option<&[u8]> {
        self.files.last().map(|f| f.end_key.as_slice())
    }

    /// Find the index of the first file whose key range could contain the target key.
    ///
    /// This performs a binary search to find the first file whose end_key >= target.
    /// The returned file's start_key might be greater than the target (if the target
    /// falls between files), but no earlier file could contain the target.
    ///
    /// Returns `None` if the target is beyond all files' key ranges.
    pub fn find_file(&self, target: &[u8]) -> Option<usize> {
        if self.files.is_empty() {
            return None;
        }

        // Binary search for the first file whose end_key >= target
        let mut left = 0;
        let mut right = self.files.len();

        while left < right {
            let mid = (left + right) / 2;
            if self.files[mid].end_key.as_slice() < target {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left < self.files.len() {
            Some(left)
        } else {
            None
        }
    }

    /// Create an iterator over this sorted run.
    /// The `create_iterator` function is used to create an iterator for each file.
    pub fn iter<I, F>(&self, create_iterator: F) -> SortedRunIterator<I, F>
    where
        I: for<'a> KvIterator<'a>,
        F: Fn(&DataFile) -> Result<I>,
    {
        SortedRunIterator::new(self.files.clone(), create_iterator)
    }
}

/// An iterator over a sorted run.
/// This iterator traverses all files in the sorted run in order.
pub struct SortedRunIterator<I, F>
where
    I: for<'a> KvIterator<'a>,
    F: Fn(&DataFile) -> Result<I>,
{
    /// The data files in this sorted run.
    files: Vec<Arc<DataFile>>,
    /// The current file index.
    current_file_idx: usize,
    /// The current file iterator.
    current_iter: Option<I>,
    /// Function to create an iterator for a file.
    create_iterator: F,
    /// Runtime configuration that decides whether crossing a file boundary or
    /// an inner physical boundary should be surfaced as a stop.
    should_stop_at_block_boundary: bool,
    /// The next file to open after callers clear a surfaced file-boundary stop.
    pending_file_boundary: Option<usize>,
    /// Whether the current child iterator already surfaced a stop and needs one
    /// resume `next()` after callers clear it.
    pending_inner_boundary_resume: bool,
    /// Whether this sorted-run wrapper has already surfaced a stop to callers.
    stopped_at_block_boundary: bool,
}

impl<I, F> SortedRunIterator<I, F>
where
    I: for<'a> KvIterator<'a>,
    F: Fn(&DataFile) -> Result<I>,
{
    fn new(files: Vec<Arc<DataFile>>, create_iterator: F) -> Self {
        Self {
            files,
            current_file_idx: 0,
            current_iter: None,
            create_iterator,
            should_stop_at_block_boundary: false,
            pending_file_boundary: None,
            pending_inner_boundary_resume: false,
            stopped_at_block_boundary: false,
        }
    }

    fn load_file(&mut self, idx: usize, target: Option<&[u8]>) -> Result<bool> {
        if idx >= self.files.len() {
            self.current_iter = None;
            return Ok(false);
        }

        let mut iter = (self.create_iterator)(&self.files[idx])?;
        iter.set_stop_at_block_boundary(self.should_stop_at_block_boundary);
        match target {
            Some(target) => iter.seek(target)?,
            None => iter.seek_to_first()?,
        }
        self.current_file_idx = idx;
        self.current_iter = Some(iter);
        Ok(true)
    }
}

impl<'a, I, F> KvIterator<'a> for SortedRunIterator<I, F>
where
    I: for<'b> KvIterator<'b>,
    F: Fn(&DataFile) -> Result<I> + 'a,
{
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.stopped_at_block_boundary = false;
        self.pending_file_boundary = None;
        self.pending_inner_boundary_resume = false;
        // Binary search for the first file whose end_key >= target
        let mut left = 0;
        let mut right = self.files.len();

        while left < right {
            let mid = (left + right) / 2;
            if self.files[mid].end_key.as_slice() < target {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        let file_idx = left;

        if file_idx >= self.files.len() {
            self.current_iter = None;
            return Ok(());
        }

        self.load_file(file_idx, Some(target))?;

        if let Some(iter) = &mut self.current_iter {
            // If the current iterator is not valid after seek,
            // the target might be between files, try the next file
            if !iter.valid() && file_idx + 1 < self.files.len() {
                self.load_file(file_idx + 1, None)?;
            }
        }

        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.stopped_at_block_boundary = false;
        self.pending_file_boundary = None;
        self.pending_inner_boundary_resume = false;
        if self.files.is_empty() {
            self.current_iter = None;
            return Ok(());
        }
        self.load_file(0, None)?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool> {
        if self.stopped_at_block_boundary {
            return Ok(false);
        }
        if self.pending_inner_boundary_resume {
            self.pending_inner_boundary_resume = false;
            if let Some(iter) = &mut self.current_iter {
                if iter.next()? {
                    return Ok(true);
                }
                if iter.stopped_at_block_boundary() {
                    self.pending_inner_boundary_resume = true;
                    self.stopped_at_block_boundary = true;
                    return Ok(false);
                }
            }
        }
        if let Some(next_idx) = self.pending_file_boundary.take() {
            self.load_file(next_idx, None)?;
            return Ok(self.current_iter.as_ref().is_some_and(|i| i.valid()));
        }
        if let Some(iter) = &mut self.current_iter {
            if iter.next()? {
                return Ok(true);
            }
            if iter.stopped_at_block_boundary() {
                self.pending_inner_boundary_resume = true;
                self.stopped_at_block_boundary = true;
                return Ok(false);
            }

            // Current file exhausted, move to next file
            let next_idx = self.current_file_idx + 1;
            if next_idx < self.files.len() {
                if self.should_stop_at_block_boundary {
                    self.current_iter = None;
                    self.pending_file_boundary = Some(next_idx);
                    self.stopped_at_block_boundary = true;
                    return Ok(false);
                }
                self.load_file(next_idx, None)?;
                return Ok(self.current_iter.as_ref().is_some_and(|i| i.valid()));
            } else {
                self.current_iter = None;
                return Ok(false);
            }
        }
        Ok(false)
    }

    fn valid(&self) -> bool {
        self.current_iter.as_ref().is_some_and(|i| i.valid())
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        if let Some(iter) = &self.current_iter {
            iter.key()
        } else {
            Ok(None)
        }
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        if let Some(iter) = &mut self.current_iter {
            iter.take_key()
        } else {
            Ok(None)
        }
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        if let Some(iter) = &mut self.current_iter {
            iter.take_value()
        } else {
            Ok(None)
        }
    }

    fn set_stop_at_block_boundary(&mut self, enabled: bool) {
        self.should_stop_at_block_boundary = enabled;
        self.pending_file_boundary = None;
        self.pending_inner_boundary_resume = false;
        self.stopped_at_block_boundary = false;
        if let Some(iter) = &mut self.current_iter {
            iter.set_stop_at_block_boundary(enabled);
        }
    }

    fn clear_stop_at_block_boundary(&mut self) {
        self.stopped_at_block_boundary = false;
        if let Some(iter) = &mut self.current_iter {
            iter.clear_stop_at_block_boundary();
        }
    }

    fn stopped_at_block_boundary(&self) -> bool {
        self.stopped_at_block_boundary
    }

    fn current_schema_id(&self) -> Option<u64> {
        self.current_iter
            .as_ref()
            .and_then(KvIterator::current_schema_id)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/iterator/sorted_run.rs"]
mod tests;
