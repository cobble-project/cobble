use crate::error::Result;
use crate::iterator::KvIterator;
use crate::r#type::KvValue;
use crate::vlog::apply_vlog_offset_to_value;
use bytes::Bytes;

pub(crate) struct VlogSeqOffsetIterator<I> {
    inner: I,
    num_columns: usize,
    file_seq_offset: u32,
}

impl<I> VlogSeqOffsetIterator<I> {
    pub(crate) fn new(inner: I, num_columns: usize, file_seq_offset: u32) -> Self {
        Self {
            inner,
            num_columns,
            file_seq_offset,
        }
    }

    fn apply_offset(&self, kv_value: KvValue) -> Result<KvValue> {
        if self.file_seq_offset == 0 {
            return Ok(kv_value);
        }
        let value = kv_value.into_decoded(self.num_columns)?;
        let shifted = apply_vlog_offset_to_value(value, self.file_seq_offset)?;
        Ok(KvValue::Decoded(shifted))
    }
}

impl<'a, I> KvIterator<'a> for VlogSeqOffsetIterator<I>
where
    I: KvIterator<'a>,
{
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.inner.seek(target)
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.inner.seek_to_first()
    }

    fn next(&mut self) -> Result<bool> {
        self.inner.next()
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
        let Some(value) = self.inner.take_value()? else {
            return Ok(None);
        };
        Ok(Some(self.apply_offset(value)?))
    }
}
