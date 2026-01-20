mod merging;
mod sorted_run;

#[allow(unused_imports)]
pub(crate) use merging::MergingIterator;
#[allow(unused_imports)]
pub(crate) use sorted_run::SortedRun;

use crate::error::Result;
use bytes::Bytes;

/// A trait for key-value iterators.
/// This provides a common interface for iterating over sorted key-value pairs
/// from various sources (e.g., SST files, memtables).
pub(crate) trait KvIterator {
    /// Seek to the first key >= target.
    fn seek(&mut self, target: &[u8]) -> Result<()>;

    /// Move to the first entry.
    fn seek_to_first(&mut self) -> Result<()>;

    /// Move to the next entry.
    /// Returns `true` if there is a next entry, `false` otherwise.
    fn next(&mut self) -> Result<bool>;

    /// Check if the iterator is valid (has a current entry).
    fn valid(&self) -> bool;

    /// Get the current key.
    /// Returns `None` if the iterator is not valid.
    fn key(&self) -> Result<Option<Bytes>>;

    /// Get the current value.
    /// Returns `None` if the iterator is not valid.
    fn value(&self) -> Result<Option<Bytes>>;

    /// Get the current key-value pair.
    /// Returns `None` if the iterator is not valid.
    fn current(&self) -> Result<Option<(Bytes, Bytes)>> {
        if !self.valid() {
            return Ok(None);
        }
        let key = self.key()?;
        let value = self.value()?;
        match (key, value) {
            (Some(k), Some(v)) => Ok(Some((k, v))),
            _ => Ok(None),
        }
    }
}
