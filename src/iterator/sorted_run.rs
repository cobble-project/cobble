use crate::data_file::DataFile;
use crate::error::Result;
use crate::iterator::KvIterator;
use bytes::Bytes;
use std::sync::Arc;

/// A sorted run is a sequence of data files where key ranges are non-overlapping
/// and sorted. This is common in LSM-tree structures where each level (except L0)
/// contains files with non-overlapping key ranges.
pub struct SortedRun {
    /// The data files in this sorted run, ordered by their key ranges.
    files: Vec<Arc<DataFile>>,
}

impl SortedRun {
    /// Create a new SortedRun from a list of data files.
    /// The files should already be sorted by their key ranges.
    pub fn new(files: Vec<Arc<DataFile>>) -> Self {
        Self { files }
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
        I: KvIterator,
        F: Fn(&DataFile) -> Result<I>,
    {
        SortedRunIterator::new(self.files.clone(), create_iterator)
    }
}

/// An iterator over a sorted run.
/// This iterator traverses all files in the sorted run in order.
pub struct SortedRunIterator<I, F>
where
    I: KvIterator,
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
}

impl<I, F> SortedRunIterator<I, F>
where
    I: KvIterator,
    F: Fn(&DataFile) -> Result<I>,
{
    fn new(files: Vec<Arc<DataFile>>, create_iterator: F) -> Self {
        Self {
            files,
            current_file_idx: 0,
            current_iter: None,
            create_iterator,
        }
    }

    fn load_file(&mut self, idx: usize) -> Result<bool> {
        if idx >= self.files.len() {
            self.current_iter = None;
            return Ok(false);
        }

        let mut iter = (self.create_iterator)(&self.files[idx])?;
        iter.seek_to_first()?;
        self.current_file_idx = idx;
        self.current_iter = Some(iter);
        Ok(true)
    }
}

impl<I, F> KvIterator for SortedRunIterator<I, F>
where
    I: KvIterator,
    F: Fn(&DataFile) -> Result<I>,
{
    fn seek(&mut self, target: &[u8]) -> Result<()> {
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

        self.load_file(file_idx)?;

        if let Some(iter) = &mut self.current_iter {
            iter.seek(target)?;
            // If the current iterator is not valid after seek,
            // the target might be between files, try the next file
            if !iter.valid() && file_idx + 1 < self.files.len() {
                self.load_file(file_idx + 1)?;
            }
        }

        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<()> {
        if self.files.is_empty() {
            self.current_iter = None;
            return Ok(());
        }
        self.load_file(0)?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool> {
        if let Some(iter) = &mut self.current_iter {
            if iter.next()? {
                return Ok(true);
            }

            // Current file exhausted, move to next file
            let next_idx = self.current_file_idx + 1;
            if next_idx < self.files.len() {
                self.load_file(next_idx)?;
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

    fn key(&self) -> Result<Option<Bytes>> {
        if let Some(iter) = &self.current_iter {
            iter.key()
        } else {
            Ok(None)
        }
    }

    fn value(&self) -> Result<Option<Bytes>> {
        if let Some(iter) = &self.current_iter {
            iter.value()
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_file::DataFileType;
    use crate::file::FileHandle;
    use crate::iterator::mock_iterator::MockIterator;

    fn create_data_file(id: u64, start: &[u8], end: &[u8]) -> Arc<DataFile> {
        Arc::new(DataFile {
            file_handle: FileHandle { id, size: 0 },
            file_type: DataFileType::SSTable,
            start_key: start.to_vec(),
            end_key: end.to_vec(),
            path: format!("test_{}.sst", id),
        })
    }

    #[test]
    fn test_sorted_run_basic() {
        let files = vec![
            create_data_file(1, b"a", b"c"),
            create_data_file(2, b"d", b"f"),
            create_data_file(3, b"g", b"i"),
        ];

        let run = SortedRun::new(files);
        assert_eq!(run.len(), 3);
        assert!(!run.is_empty());
        assert_eq!(run.start_key(), Some(b"a".as_slice()));
        assert_eq!(run.end_key(), Some(b"i".as_slice()));
    }

    #[test]
    fn test_sorted_run_empty() {
        let run = SortedRun::new(vec![]);
        assert_eq!(run.len(), 0);
        assert!(run.is_empty());
        assert_eq!(run.start_key(), None);
        assert_eq!(run.end_key(), None);
    }

    #[test]
    fn test_find_file() {
        let files = vec![
            create_data_file(1, b"a", b"c"),
            create_data_file(2, b"d", b"f"),
            create_data_file(3, b"g", b"i"),
        ];

        let run = SortedRun::new(files);

        // Target in first file
        assert_eq!(run.find_file(b"b"), Some(0));

        // Target in second file
        assert_eq!(run.find_file(b"e"), Some(1));

        // Target in third file
        assert_eq!(run.find_file(b"h"), Some(2));

        // Target before all files
        assert_eq!(run.find_file(b"0"), Some(0));

        // Target at boundary
        assert_eq!(run.find_file(b"c"), Some(0));
        assert_eq!(run.find_file(b"d"), Some(1));

        // Target after all files
        assert_eq!(run.find_file(b"z"), None);
    }

    #[test]
    fn test_sorted_run_iterator() {
        let files = vec![
            create_data_file(1, b"a", b"c"),
            create_data_file(2, b"d", b"f"),
        ];

        let run = SortedRun::new(files);

        // Create a mock iterator factory
        let create_iter = |file: &DataFile| -> Result<MockIterator> {
            let entries = match file.file_handle.id {
                1 => vec![(b"a".as_slice(), b"v1"), (b"b", b"v2"), (b"c", b"v3")],
                2 => vec![(b"d".as_slice(), b"v4"), (b"e", b"v5"), (b"f", b"v6")],
                _ => vec![],
            };
            Ok(MockIterator::new(entries))
        };

        let mut iter = run.iter(create_iter);
        iter.seek_to_first().unwrap();

        let mut results = vec![];
        while iter.valid() {
            let (k, v) = iter.current().unwrap().unwrap();
            results.push((k, v));
            iter.next().unwrap();
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
    fn test_sorted_run_iterator_seek() {
        let files = vec![
            create_data_file(1, b"a", b"c"),
            create_data_file(2, b"d", b"f"),
        ];

        let run = SortedRun::new(files);

        let create_iter = |file: &DataFile| -> Result<MockIterator> {
            let entries = match file.file_handle.id {
                1 => vec![(b"a".as_slice(), b"v1"), (b"b", b"v2"), (b"c", b"v3")],
                2 => vec![(b"d".as_slice(), b"v4"), (b"e", b"v5"), (b"f", b"v6")],
                _ => vec![],
            };
            Ok(MockIterator::new(entries))
        };

        let mut iter = run.iter(create_iter);

        // Seek to middle of first file
        iter.seek(b"b").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().unwrap().as_ref(), b"b");

        // Seek to second file
        iter.seek(b"e").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().unwrap().as_ref(), b"e");

        // Seek to exact boundary
        iter.seek(b"d").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().unwrap().as_ref(), b"d");
    }
}
