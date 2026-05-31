use crate::error::Result;
use crate::iterator::KvIterator;
use crate::r#type::KvValue;
use bytes::Bytes;

pub(crate) struct TruncationFilterIterator<I>
where
    I: for<'a> KvIterator<'a>,
{
    inner: I,
    lower_bound_exclusive: Option<Bytes>,
}

impl<I> TruncationFilterIterator<I>
where
    I: for<'a> KvIterator<'a>,
{
    pub(crate) fn new(inner: I, lower_bound_exclusive: Option<Bytes>) -> Self {
        Self {
            inner,
            lower_bound_exclusive,
        }
    }

    fn key_is_visible(&self, key: &[u8]) -> bool {
        self.lower_bound_exclusive
            .as_deref()
            .is_none_or(|lower_bound| key > lower_bound)
    }

    fn advance_to_visible(&mut self) -> Result<()> {
        while self.inner.valid() {
            let Some(key) = self.inner.key()? else {
                break;
            };
            if self.key_is_visible(key) {
                break;
            }
            if !self.inner.next()? {
                break;
            }
        }
        Ok(())
    }
}

impl<'a, I> KvIterator<'a> for TruncationFilterIterator<I>
where
    I: for<'b> KvIterator<'b>,
{
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.inner.seek(target)?;
        self.advance_to_visible()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.inner.seek_to_first()?;
        self.advance_to_visible()
    }

    fn next(&mut self) -> Result<bool> {
        if !self.inner.next()? {
            return Ok(false);
        }
        self.advance_to_visible()?;
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

    fn set_stop_at_block_boundary(&mut self, enabled: bool) {
        self.inner.set_stop_at_block_boundary(enabled);
    }

    fn clear_stop_at_block_boundary(&mut self) {
        self.inner.clear_stop_at_block_boundary();
    }

    fn stopped_at_block_boundary(&self) -> bool {
        self.inner.stopped_at_block_boundary()
    }
}
