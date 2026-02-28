use crate::error::{Error, Result};
use crate::iterator::KvIterator;
use crate::schema::{Schema, SchemaManager};
use crate::sst::row_codec::{decode_value, encode_value};
use crate::util::unsafe_bytes;
use bytes::Bytes;
use std::cell::RefCell;
use std::sync::Arc;

/// An iterator wrapper that evolves values from a source schema to a target schema on-the-fly.
pub(crate) struct SchemaEvolvingIterator<I> {
    inner: I,
    source_schema_id: u64,
    source_num_columns: usize,
    target_schema: Arc<Schema>,
    schema_manager: Arc<SchemaManager>,
    evolved_value_cache: RefCell<Option<Bytes>>,
}

impl<I> SchemaEvolvingIterator<I> {
    pub(crate) fn new(
        inner: I,
        source_schema_id: u64,
        source_num_columns: usize,
        target_schema: Arc<Schema>,
        schema_manager: Arc<SchemaManager>,
    ) -> Self {
        Self {
            inner,
            source_schema_id,
            source_num_columns,
            target_schema,
            schema_manager,
            evolved_value_cache: RefCell::new(None),
        }
    }

    fn evolve_value_if_needed(&self, mut encoded: Bytes) -> Result<Bytes> {
        let target_schema_id = self.target_schema.version();
        if self.source_schema_id == target_schema_id {
            return Ok(encoded);
        }
        if self.source_schema_id > target_schema_id {
            return Err(Error::InvalidState(format!(
                "Source schema {} is newer than target schema {}",
                self.source_schema_id, target_schema_id
            )));
        }
        let value = decode_value(&mut encoded, self.source_num_columns)?;
        let evolved = self.schema_manager.evolve_value(
            value,
            self.source_schema_id,
            self.target_schema.version(),
        )?;
        Ok(encode_value(&evolved, self.target_schema.num_columns()))
    }

    fn should_evolve(&self) -> Result<bool> {
        let target_schema_id = self.target_schema.version();
        if self.source_schema_id == target_schema_id {
            return Ok(false);
        }
        if self.source_schema_id > target_schema_id {
            return Err(Error::InvalidState(format!(
                "Source schema {} is newer than target schema {}",
                self.source_schema_id, target_schema_id
            )));
        }
        Ok(true)
    }

    fn clear_cache(&self) {
        self.evolved_value_cache.borrow_mut().take();
    }

    fn ensure_evolved_value_cached<'a>(&self) -> Result<Option<()>>
    where
        I: KvIterator<'a>,
    {
        if self.evolved_value_cache.borrow().is_some() {
            return Ok(Some(()));
        }
        let Some(value_slice) = self.inner.value_slice()? else {
            self.clear_cache();
            return Ok(None);
        };
        let encoded = unsafe_bytes(value_slice);
        let evolved = self.evolve_value_if_needed(encoded)?;
        *self.evolved_value_cache.borrow_mut() = Some(evolved);
        Ok(Some(()))
    }
}

impl<'a, I> KvIterator<'a> for SchemaEvolvingIterator<I>
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
        if !self.should_evolve()? {
            self.clear_cache();
            return self.inner.value();
        }
        let Some(()) = self.ensure_evolved_value_cached()? else {
            return Ok(None);
        };
        Ok(self.evolved_value_cache.borrow().as_ref().cloned())
    }

    fn value_slice(&self) -> Result<Option<&[u8]>> {
        if !self.should_evolve()? {
            self.clear_cache();
            return self.inner.value_slice();
        }
        let Some(()) = self.ensure_evolved_value_cached()? else {
            return Ok(None);
        };
        let cached = self.evolved_value_cache.borrow();
        if let Some(bytes) = cached.as_ref() {
            let ptr = bytes.as_ptr();
            let len = bytes.len();
            drop(cached);
            // SAFETY: cached evolved bytes remain valid until iterator position changes.
            return Ok(Some(unsafe { std::slice::from_raw_parts(ptr, len) }));
        }
        Ok(None)
    }
}
