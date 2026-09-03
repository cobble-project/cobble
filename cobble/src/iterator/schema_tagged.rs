use crate::error::Result;
use crate::iterator::KvIterator;
use crate::r#type::KvValue;
use bytes::Bytes;

/// Associates every entry from one physical input file with its encoding schema.
pub(crate) struct SchemaTaggedIterator<I> {
    inner: I,
    schema_id: u64,
}

impl<I> SchemaTaggedIterator<I> {
    pub(crate) fn new(inner: I, schema_id: u64) -> Self {
        Self { inner, schema_id }
    }
}

impl<'a, I> KvIterator<'a> for SchemaTaggedIterator<I>
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

    fn current_schema_id(&self) -> Option<u64> {
        self.inner.valid().then_some(self.schema_id)
    }
}
