//! This module defines the `DbIterator` struct, which provides an iterator over key-value pairs in the database.
use crate::db::value_to_vec_of_columns_with_vlog;
use crate::db_state::DbState;
use crate::db_status::DbAccessGuard;
use crate::error::Result;
use crate::iterator::KvIterator;
use crate::iterator::{DeduplicatingIterator, MergingIterator};
use crate::lsm::DynKvIterator;
use crate::memtable::MemtableManager;
use crate::schema::Schema;
use crate::sst::row_codec::decode_key;
use crate::ttl::TTLProvider;
use crate::vlog::VlogStore;
use bytes::Bytes;
use std::sync::Arc;

pub(crate) struct DbIteratorOptions<'a> {
    pub(crate) end_bound: Option<(Bytes, bool)>,
    pub(crate) snapshot: Arc<DbState>,
    pub(crate) memtable_manager: Option<&'a MemtableManager>,
    pub(crate) access_guard: Option<DbAccessGuard<'a>>,
    pub(crate) vlog_store: Arc<VlogStore>,
    pub(crate) ttl_provider: Arc<TTLProvider>,
    /// Effective schema for merge and value resolution.
    /// When column projection is active, this is a projected schema
    /// with remapped operators matching the selected column indices.
    pub(crate) schema: Arc<Schema>,
    pub(crate) column_family_id: u8,
}

pub struct DbIterator<'a> {
    inner: DeduplicatingIterator<MergingIterator<DynKvIterator>>,
    end_bound: Option<(Bytes, bool)>,
    snapshot: Arc<DbState>,
    memtable_manager: Option<&'a MemtableManager>,
    _access_guard: Option<DbAccessGuard<'a>>,
    vlog_store: Arc<VlogStore>,
    ttl_provider: Arc<TTLProvider>,
    schema: Arc<Schema>,
    num_columns: usize,
    column_family_id: u8,
}

impl<'a> DbIterator<'a> {
    pub(crate) fn new(
        mut memtable_iters: Vec<DynKvIterator>,
        mut lsm_iters: Vec<DynKvIterator>,
        options: DbIteratorOptions<'a>,
    ) -> Self {
        let schema = options.schema;
        let num_columns = schema
            .num_columns_in_family(options.column_family_id)
            .unwrap_or_else(|| schema.num_columns());
        memtable_iters.append(&mut lsm_iters);
        let inner = DeduplicatingIterator::new(
            MergingIterator::new(memtable_iters),
            Some(num_columns),
            Arc::clone(&options.ttl_provider),
            None,
            schema.clone(),
        );
        Self {
            inner,
            end_bound: options.end_bound,
            snapshot: options.snapshot,
            memtable_manager: options.memtable_manager,
            _access_guard: options.access_guard,
            vlog_store: options.vlog_store,
            ttl_provider: options.ttl_provider,
            schema,
            num_columns,
            column_family_id: options.column_family_id,
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
            let Some((mut encoded_key, kv_value)) = self.inner.take_current()? else {
                self.inner.next()?;
                continue;
            };
            self.inner.next()?;
            if self.is_past_end(encoded_key.as_ref()) {
                return Ok(None);
            }
            let key = decode_key(&mut encoded_key)?;
            let value = kv_value.into_decoded(self.num_columns)?;
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
                self.column_family_id,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_state::MultiLSMTreeVersion;
    use crate::file::{FileManager, FileSystemRegistry};
    use crate::lsm::LSMTreeVersion;
    use crate::metrics_manager::MetricsManager;
    use crate::schema::SchemaManager;
    use serial_test::serial;
    use std::collections::VecDeque;

    #[test]
    #[serial(file)]
    fn test_db_iterator_uses_projected_family_schema_width() {
        let root = "/tmp/db_iterator_projected_family_schema_width";
        let _ = std::fs::remove_dir_all(root);
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", root))
            .expect("register file fs");
        let metrics_manager = Arc::new(MetricsManager::new("db-iterator-test"));
        let file_manager = Arc::new(
            FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager))
                .expect("file manager"),
        );
        let vlog_store = Arc::new(VlogStore::new(file_manager, 4096, usize::MAX));

        let schema_manager = Arc::new(SchemaManager::new(2));
        let mut builder = schema_manager.builder();
        builder
            .add_column(0, None, None, Some("metrics".to_string()))
            .unwrap();
        builder
            .add_column(1, None, None, Some("metrics".to_string()))
            .unwrap();
        let schema = builder.commit();
        let projected_schema = schema.project_in_family(1, &[1]);

        let snapshot = Arc::new(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version: MultiLSMTreeVersion::new(LSMTreeVersion { levels: Vec::new() }),
            vlog_version: crate::vlog::VlogVersion::new(),
            active: None,
            immutables: VecDeque::new(),
            suggested_base_snapshot_id: None,
        });

        let iter = DbIterator::new(
            Vec::new(),
            Vec::new(),
            DbIteratorOptions {
                end_bound: None,
                snapshot,
                memtable_manager: None,
                access_guard: None,
                vlog_store,
                ttl_provider: Arc::new(TTLProvider::disabled()),
                schema: projected_schema,
                column_family_id: 1,
            },
        );

        assert_eq!(iter.num_columns, 1);
        let _ = std::fs::remove_dir_all(root);
    }
}
