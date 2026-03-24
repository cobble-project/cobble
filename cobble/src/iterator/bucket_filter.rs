//! A `KvIterator` wrapper that filters keys based on their bucket.
use crate::error::Result;
use crate::iterator::KvIterator;
use crate::r#type::{KvValue, key_bucket};
use bytes::Bytes;
use std::ops::RangeInclusive;

pub(crate) struct BucketFilterIterator<I>
where
    I: for<'a> KvIterator<'a>,
{
    inner: I,
    range: RangeInclusive<u16>,
}

impl<I> BucketFilterIterator<I>
where
    I: for<'a> KvIterator<'a>,
{
    pub(crate) fn new(inner: I, range: RangeInclusive<u16>) -> Self {
        Self { inner, range }
    }

    fn key_in_range(&self, key: &[u8]) -> bool {
        key_bucket(key).is_none_or(|bucket| self.range.contains(&bucket))
    }

    fn advance_to_in_range(&mut self) -> Result<()> {
        while self.inner.valid() {
            let Some(key) = self.inner.key()? else {
                break;
            };
            if self.key_in_range(key) {
                break;
            }
            if !self.inner.next()? {
                break;
            }
        }
        Ok(())
    }
}

impl<'a, I> KvIterator<'a> for BucketFilterIterator<I>
where
    I: for<'b> KvIterator<'b>,
{
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.inner.seek(target)?;
        self.advance_to_in_range()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.inner.seek_to_first()?;
        self.advance_to_in_range()
    }

    fn next(&mut self) -> Result<bool> {
        if !self.inner.next()? {
            return Ok(false);
        }
        self.advance_to_in_range()?;
        Ok(self.inner.valid())
    }

    fn valid(&self) -> bool {
        self.inner.valid()
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        self.inner.key()
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        self.inner.take_key()
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        self.inner.take_value()
    }
}
