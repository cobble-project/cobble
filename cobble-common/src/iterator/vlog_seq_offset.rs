use crate::error::Result;
use crate::iterator::KvIterator;
use crate::sst::row_codec::{decode_value, encode_value};
use crate::util::unsafe_bytes;
use crate::vlog::apply_vlog_offset_to_value;
use bytes::Bytes;
use std::cell::RefCell;

pub(crate) struct VlogSeqOffsetIterator<I> {
    inner: I,
    num_columns: usize,
    file_seq_offset: u32,
    value_cache: RefCell<Option<Bytes>>,
}

impl<I> VlogSeqOffsetIterator<I> {
    pub(crate) fn new(inner: I, num_columns: usize, file_seq_offset: u32) -> Self {
        Self {
            inner,
            num_columns,
            file_seq_offset,
            value_cache: RefCell::new(None),
        }
    }

    fn clear_cache(&self) {
        self.value_cache.borrow_mut().take();
    }

    fn ensure_value_cached<'a>(&self) -> Result<Option<()>>
    where
        I: KvIterator<'a>,
    {
        if self.value_cache.borrow().is_some() {
            return Ok(Some(()));
        }
        let Some(value_slice) = self.inner.value_slice()? else {
            self.clear_cache();
            return Ok(None);
        };
        if self.file_seq_offset == 0 {
            *self.value_cache.borrow_mut() = Some(unsafe_bytes(value_slice));
            return Ok(Some(()));
        }
        let mut encoded = unsafe_bytes(value_slice);
        let value = decode_value(&mut encoded, self.num_columns)?;
        let shifted = apply_vlog_offset_to_value(value, self.file_seq_offset)?;
        *self.value_cache.borrow_mut() = Some(encode_value(&shifted, self.num_columns));
        Ok(Some(()))
    }
}

impl<'a, I> KvIterator<'a> for VlogSeqOffsetIterator<I>
where
    I: KvIterator<'a>,
{
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.clear_cache();
        self.inner.seek(target)
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.clear_cache();
        self.inner.seek_to_first()
    }

    fn next(&mut self) -> Result<bool> {
        self.clear_cache();
        self.inner.next()
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
        let Some(()) = self.ensure_value_cached()? else {
            return Ok(None);
        };
        Ok(self.value_cache.borrow().as_ref().cloned())
    }

    fn value_slice(&self) -> Result<Option<&[u8]>> {
        let Some(()) = self.ensure_value_cached()? else {
            return Ok(None);
        };
        let cached = self.value_cache.borrow();
        if let Some(bytes) = cached.as_ref() {
            let ptr = bytes.as_ptr();
            let len = bytes.len();
            drop(cached);
            // SAFETY: cached value bytes remain valid until iterator position changes.
            return Ok(Some(unsafe { std::slice::from_raw_parts(ptr, len) }));
        }
        Ok(None)
    }
}
