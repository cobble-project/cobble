//! Iterator module for key-value iteration across data files.
//!
//! This module provides generic iterator abstractions that are independent of
//! the underlying data file format (SST, etc.), allowing for future extensibility.

mod deduplicating;
mod factory;
mod merging;
pub(crate) mod mock_iterator;
mod sorted_run;

// Public API exports for the iterator module.
// These are currently unused within this crate but are exported for external usage.
#[allow(unused_imports)]
pub(crate) use deduplicating::DeduplicatingIterator;
#[allow(unused_imports)]
pub(crate) use factory::{IteratorFactoryOptions, create_iterator, make_iterator_factory};
#[allow(unused_imports)]
pub(crate) use merging::MergingIterator;
#[allow(unused_imports)]
pub(crate) use sorted_run::SortedRun;

use crate::error::Result;
use bytes::Bytes;

/// A trait for key-value iterators.
/// This provides a common interface for iterating over sorted key-value pairs
/// from various sources (e.g., SST files, memtables).
pub(crate) trait KvIterator<'a>: 'a {
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

/// Implement KvIterator for Box<dyn for<'a> KvIterator<'a>> to support dynamic dispatch.
impl<'a> KvIterator<'a> for Box<dyn for<'b> KvIterator<'b>> {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        (**self).seek(target)
    }

    fn seek_to_first(&mut self) -> Result<()> {
        (**self).seek_to_first()
    }

    fn next(&mut self) -> Result<bool> {
        (**self).next()
    }

    fn valid(&self) -> bool {
        (**self).valid()
    }

    fn key(&self) -> Result<Option<Bytes>> {
        (**self).key()
    }

    fn value(&self) -> Result<Option<Bytes>> {
        (**self).value()
    }
}
