//! This module defines the `DbIterator` struct, which provides an iterator over key-value pairs in the database.
use crate::db::value_to_vec_of_columns_with_vlog;
use crate::db_state::DbState;
use crate::error::Result;
use crate::iterator::KvIterator;
use crate::iterator::{DeduplicatingIterator, MergingIterator};
use crate::lsm::DynKvIterator;
use crate::memtable::MemtableManager;
use crate::schema::{Schema, SchemaManager};
use crate::sst::row_codec::{decode_key, decode_value};
use crate::ttl::TTLProvider;
use crate::vlog::VlogStore;
use bytes::Bytes;
use std::sync::Arc;

pub(crate) struct DbIteratorOptions<'a> {
    pub(crate) end_bound: Option<(Bytes, bool)>,
    pub(crate) snapshot: Arc<DbState>,
    pub(crate) memtable_manager: Option<&'a MemtableManager>,
    pub(crate) vlog_store: Arc<VlogStore>,
    pub(crate) ttl_provider: Arc<TTLProvider>,
    pub(crate) schema_manager: Arc<SchemaManager>,
}

pub struct DbIterator<'a> {
    inner: DeduplicatingIterator<MergingIterator<DynKvIterator>>,
    end_bound: Option<(Bytes, bool)>,
    snapshot: Arc<DbState>,
    memtable_manager: Option<&'a MemtableManager>,
    vlog_store: Arc<VlogStore>,
    ttl_provider: Arc<TTLProvider>,
    schema: Arc<Schema>,
    num_columns: usize,
}

impl<'a> DbIterator<'a> {
    pub(crate) fn new(
        mut memtable_iters: Vec<DynKvIterator>,
        mut lsm_iters: Vec<DynKvIterator>,
        options: DbIteratorOptions<'a>,
    ) -> Self {
        let schema = options.schema_manager.latest_schema();
        let num_columns = schema.num_columns();
        memtable_iters.append(&mut lsm_iters);
        let inner = DeduplicatingIterator::new(
            MergingIterator::new(memtable_iters),
            num_columns,
            Arc::clone(&options.ttl_provider),
            None,
            schema.clone(),
        );
        Self {
            inner,
            end_bound: options.end_bound,
            snapshot: options.snapshot,
            memtable_manager: options.memtable_manager,
            vlog_store: options.vlog_store,
            ttl_provider: options.ttl_provider,
            schema,
            num_columns,
        }
    }

    pub(crate) fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.inner.seek(target)
    }

    fn is_past_end(&self, encoded_key: &[u8]) -> bool {
        if let Some((end_key, inclusive)) = &self.end_bound {
            encoded_key > end_key.as_ref() || (!inclusive && encoded_key == end_key.as_ref())
        } else {
            false
        }
    }

    fn next_row(&mut self) -> Result<Option<(Bytes, Vec<Option<Bytes>>)>> {
        while self.inner.valid() {
            let Some((mut encoded_key, mut encoded_value)) = self.inner.current()? else {
                self.inner.next()?;
                continue;
            };
            self.inner.next()?;
            if self.is_past_end(encoded_key.as_ref()) {
                return Ok(None);
            }
            let key = decode_key(&mut encoded_key)?;
            let value = decode_value(&mut encoded_value, self.num_columns)?;
            let columns = value_to_vec_of_columns_with_vlog(
                value,
                |pointer| match self
                    .vlog_store
                    .read_pointer(&self.snapshot.vlog_version, pointer)
                {
                    Ok(value) => Ok(value),
                    Err(vlog_err) => {
                        if let Some(memtable_manager) = self.memtable_manager {
                            memtable_manager
                                .read_vlog_pointer_with_snapshot(
                                    Arc::clone(&self.snapshot),
                                    pointer,
                                )?
                                .ok_or(vlog_err)
                        } else {
                            Err(vlog_err)
                        }
                    }
                },
                &self.schema,
                Some(self.ttl_provider.time_provider()),
            )?;
            if let Some(columns) = columns {
                return Ok(Some((Bytes::copy_from_slice(key.data()), columns)));
            }
        }
        Ok(None)
    }
}

impl Iterator for DbIterator<'_> {
    type Item = Result<(Bytes, Vec<Option<Bytes>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_row().transpose()
    }
}
