//! A `KvIterator` wrapper that filters keys based on their bucket.
use crate::error::Result;
use crate::iterator::KvIterator;
use crate::r#type::key_bucket;
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
            let Some(key) = self.inner.key_slice()? else {
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

    fn key(&self) -> Result<Option<Bytes>> {
        self.inner.key()
    }

    fn key_slice(&self) -> Result<Option<&[u8]>> {
        self.inner.key_slice()
    }

    fn value(&self) -> Result<Option<Bytes>> {
        self.inner.value()
    }

    fn value_slice(&self) -> Result<Option<&[u8]>> {
        self.inner.value_slice()
    }
}
