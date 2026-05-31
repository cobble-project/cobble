//! Iterator module for key-value iteration across data files.
//!
//! This module provides generic iterator abstractions that are independent of
//! the underlying data file format (SST, etc.), allowing for future extensibility.

mod bucket_filter;
mod column_masking;
mod deduplicating;
mod factory;
mod merging;
pub(crate) mod mock_iterator;
mod schema_evolving;
mod sorted_run;
mod truncation_filter;
mod vlog_seq_offset;

// Public API exports for the iterator module.
// These are currently unused within this crate but are exported for external usage.
#[allow(unused_imports)]
pub(crate) use bucket_filter::BucketFilterIterator;
#[allow(unused_imports)]
pub(crate) use column_masking::ColumnMaskingIterator;
#[allow(unused_imports)]
pub(crate) use deduplicating::DeduplicatingIterator;
#[allow(unused_imports)]
pub(crate) use factory::{IteratorFactoryOptions, create_iterator, make_iterator_factory};
#[allow(unused_imports)]
pub(crate) use merging::MergingIterator;
#[allow(unused_imports)]
pub(crate) use schema_evolving::SchemaEvolvingIterator;
#[allow(unused_imports)]
pub(crate) use sorted_run::SortedRun;
#[allow(unused_imports)]
pub(crate) use truncation_filter::TruncationFilterIterator;
#[allow(unused_imports)]
pub(crate) use vlog_seq_offset::VlogSeqOffsetIterator;

use crate::error::Result;
use crate::r#type::KvValue;
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

    /// Borrow the current key as a slice.
    /// Can be called multiple times per position.
    /// Returns `None` if the iterator is not valid.
    fn key(&self) -> Result<Option<&[u8]>>;

    /// Take ownership of the current key.
    /// Consumes from internal cache; should be called at most once per position.
    fn take_key(&mut self) -> Result<Option<Bytes>>;

    /// Take ownership of the current value as a KvValue.
    /// Consumes from internal cache; should be called at most once per position.
    fn take_value(&mut self) -> Result<Option<KvValue>>;

    /// Enable or disable boundary-aware stopping for this iterator.
    ///
    /// When enabled, leaf iterators stop after crossing the next physical read
    /// boundary they expose, such as an SST data block or Parquet row group.
    /// Wrapper iterators surface the same stop upward so callers can return a
    /// partial batch without forcing every child to stop in lockstep.
    fn set_stop_at_block_boundary(&mut self, _enabled: bool) {}

    /// Clear a previously reported boundary stop so iteration can continue.
    ///
    /// Callers should invoke this after observing `stopped_at_block_boundary()`
    /// and consuming any rows already produced for the current batch.
    fn clear_stop_at_block_boundary(&mut self) {}

    /// Returns `true` when the iterator paused at a physical block boundary.
    ///
    /// This is a sticky signal until `clear_stop_at_block_boundary()` is
    /// called, which lets upper layers return a batch first and then resume
    /// from the same iterator state on the next poll.
    fn stopped_at_block_boundary(&self) -> bool {
        false
    }

    /// Take ownership of both key and value.
    /// Consumes from internal cache; should be called at most once per position.
    fn take_current(&mut self) -> Result<Option<(Bytes, KvValue)>> {
        if !self.valid() {
            return Ok(None);
        }
        let key = self.take_key()?;
        let value = self.take_value()?;
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

    fn key(&self) -> Result<Option<&[u8]>> {
        (**self).key()
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        (**self).take_key()
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        (**self).take_value()
    }

    fn set_stop_at_block_boundary(&mut self, enabled: bool) {
        (**self).set_stop_at_block_boundary(enabled);
    }

    fn clear_stop_at_block_boundary(&mut self) {
        (**self).clear_stop_at_block_boundary();
    }

    fn stopped_at_block_boundary(&self) -> bool {
        (**self).stopped_at_block_boundary()
    }
}
