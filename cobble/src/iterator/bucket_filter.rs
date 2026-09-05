//! A `KvIterator` wrapper that filters keys based on their bucket.
use crate::error::Result;
use crate::iterator::KvIterator;
use crate::r#type::{ENCODED_KEY_BUCKET_BYTES, KvValue, encode_bucket_prefix};
use bytes::Bytes;
use std::ops::RangeInclusive;

/// Restricts a storage iterator to one contiguous numeric bucket range.
///
/// Encoded bucket prefixes are big-endian, so the numeric range is also one lexicographic key
/// interval. Seeking to its lower bound and stopping at its exclusive upper bound avoids reading
/// data outside a rescaled file's effective range.
pub(crate) struct BucketFilterIterator<I>
where
    I: for<'a> KvIterator<'a>,
{
    inner: I,
    start_key: [u8; ENCODED_KEY_BUCKET_BYTES],
    end_key_exclusive: Option<[u8; ENCODED_KEY_BUCKET_BYTES]>,
    exhausted: bool,
}

impl<I> BucketFilterIterator<I>
where
    I: for<'a> KvIterator<'a>,
{
    pub(crate) fn new(inner: I, range: RangeInclusive<u16>) -> Self {
        let start_key = encode_bucket_prefix(*range.start());
        let end_key_exclusive = range.end().checked_add(1).map(encode_bucket_prefix);
        Self {
            inner,
            start_key,
            end_key_exclusive,
            exhausted: false,
        }
    }

    fn update_exhausted(&mut self) -> Result<()> {
        if let (Some(end_key), Some(key)) = (self.end_key_exclusive.as_ref(), self.inner.key()?)
            && key >= end_key.as_slice()
        {
            self.exhausted = true;
        }
        Ok(())
    }
}

impl<'a, I> KvIterator<'a> for BucketFilterIterator<I>
where
    I: for<'b> KvIterator<'b>,
{
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.exhausted = self
            .end_key_exclusive
            .as_ref()
            .is_some_and(|end_key| target >= end_key.as_slice());
        if self.exhausted {
            return Ok(());
        }
        self.inner.seek(target.max(self.start_key.as_slice()))?;
        self.update_exhausted()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.exhausted = false;
        self.inner.seek(&self.start_key)?;
        self.update_exhausted()
    }

    fn next(&mut self) -> Result<bool> {
        if self.exhausted {
            return Ok(false);
        }
        if !self.inner.next()? {
            return Ok(false);
        }
        self.update_exhausted()?;
        Ok(self.valid())
    }

    fn valid(&self) -> bool {
        !self.exhausted && self.inner.valid()
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        if self.exhausted {
            Ok(None)
        } else {
            self.inner.key()
        }
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        if self.exhausted {
            Ok(None)
        } else {
            self.inner.take_key()
        }
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        if self.exhausted {
            Ok(None)
        } else {
            self.inner.take_value()
        }
    }

    fn set_stop_at_block_boundary(&mut self, enabled: bool) {
        self.inner.set_stop_at_block_boundary(enabled);
    }

    fn clear_stop_at_block_boundary(&mut self) {
        self.inner.clear_stop_at_block_boundary();
    }

    fn stopped_at_block_boundary(&self) -> bool {
        self.inner.stopped_at_block_boundary()
    }

    fn current_schema_id(&self) -> Option<u64> {
        self.inner.current_schema_id()
    }
}

#[cfg(test)]
#[path = "../../tests/unit/iterator/bucket_filter.rs"]
mod tests;
