use crate::error::{Error, Result};
use crate::iterator::KvIterator;
use crate::schema::{Schema, SchemaManager};
use crate::sst::row_codec::encode_value;
use crate::r#type::KvValue;
use bytes::Bytes;
use std::sync::Arc;

/// An iterator wrapper that evolves values from a source schema to a target schema on-the-fly.
pub(crate) struct SchemaEvolvingIterator<I> {
    inner: I,
    source_schema: Arc<Schema>,
    target_schema: Arc<Schema>,
    schema_manager: Arc<SchemaManager>,
}

impl<I> SchemaEvolvingIterator<I> {
    pub(crate) fn new(
        inner: I,
        source_schema: Arc<Schema>,
        target_schema: Arc<Schema>,
        schema_manager: Arc<SchemaManager>,
    ) -> Self {
        Self {
            inner,
            source_schema,
            target_schema,
            schema_manager,
        }
    }

    fn should_evolve(&self) -> Result<bool> {
        let source_schema_id = self.source_schema.version();
        let target_schema_id = self.target_schema.version();
        if source_schema_id == target_schema_id {
            return Ok(false);
        }
        if source_schema_id > target_schema_id {
            return Err(Error::InvalidState(format!(
                "Source schema {} is newer than target schema {}",
                source_schema_id, target_schema_id
            )));
        }
        Ok(true)
    }

    fn evolve_value(&self, kv_value: KvValue) -> Result<KvValue> {
        let value = kv_value.into_decoded(self.source_schema.num_columns())?;
        let evolved = self.schema_manager.evolve_value(
            value,
            self.source_schema.version(),
            self.target_schema.version(),
        )?;
        Ok(KvValue::Encoded(encode_value(
            &evolved,
            self.target_schema.num_columns(),
        )))
    }
}

impl<'a, I> KvIterator<'a> for SchemaEvolvingIterator<I>
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
        if !self.should_evolve()? {
            return Ok(Some(value));
        }
        Ok(Some(self.evolve_value(value)?))
    }
}
