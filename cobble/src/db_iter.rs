//! This module defines the `DbIterator` struct, which provides an iterator over key-value pairs in the database.
use crate::db::value_to_vec_of_columns_with_vlog;
use crate::db_state::DbState;
use crate::db_status::DbAccessGuard;
use crate::error::Result;
use crate::iterator::KvIterator;
use crate::iterator::{DeduplicatingIterator, MergingIterator, TruncationFilterIterator};
use crate::lsm::DynKvIterator;
use crate::memtable::MemtableManager;
use crate::schema::Schema;
use crate::sst::row_codec::decode_key;
use crate::ttl::TTLProvider;
use crate::r#type::Key;
use crate::vlog::VlogStore;
use bytes::Bytes;
use std::sync::Arc;

pub(crate) type BucketedRow = (u16, Bytes, Vec<Option<Bytes>>);
type DecodedRow = (Key, Vec<Option<Bytes>>);

pub(crate) struct DbIteratorOptions<'a> {
    pub(crate) end_bound: Option<(Bytes, bool)>,
    pub(crate) lower_bound_exclusive: Option<Bytes>,
    pub(crate) max_rows: Option<usize>,
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
    pub(crate) should_stop_at_block_boundary: bool,
}

pub struct DbIterator<'a> {
    inner: DeduplicatingIterator<TruncationFilterIterator<MergingIterator<DynKvIterator>>>,
    end_bound: Option<(Bytes, bool)>,
    snapshot: Arc<DbState>,
    memtable_manager: Option<&'a MemtableManager>,
    _access_guard: Option<DbAccessGuard<'a>>,
    vlog_store: Arc<VlogStore>,
    ttl_provider: Arc<TTLProvider>,
    schema: Arc<Schema>,
    num_columns: usize,
    column_family_id: u8,
    remaining_rows: Option<usize>,
    /// Whether block-boundary stops are enabled for the iterator chain that
    /// feeds this row decoder.
    should_stop_at_block_boundary: bool,
    /// Whether the row-level iterator has already surfaced a stop to callers.
    /// After `clear_stop_at_block_boundary()`, the next decode attempt will ask
    /// the inner iterator chain to resume from the boundary.
    stopped_at_block_boundary: bool,
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
        let merged = MergingIterator::new(memtable_iters);
        let mut inner = DeduplicatingIterator::new(
            TruncationFilterIterator::new(merged, options.lower_bound_exclusive),
            Some(num_columns),
            Arc::clone(&options.ttl_provider),
            None,
            schema.clone(),
        );
        inner.set_stop_at_block_boundary(options.should_stop_at_block_boundary);
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
            remaining_rows: options.max_rows,
            should_stop_at_block_boundary: options.should_stop_at_block_boundary,
            stopped_at_block_boundary: false,
        }
    }

    pub(crate) fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.stopped_at_block_boundary = false;
        self.inner.seek(target)
    }

    /// Clear a previously surfaced block-boundary stop and allow scanning to
    /// resume from the same iterator position.
    pub fn clear_stop_at_block_boundary(&mut self) {
        self.stopped_at_block_boundary = false;
        if self.should_stop_at_block_boundary {
            self.inner.clear_stop_at_block_boundary();
        }
    }

    /// Returns `true` when the last scan attempt paused at a physical block
    /// boundary instead of exhausting the iterator.
    pub fn stopped_at_block_boundary(&self) -> bool {
        self.stopped_at_block_boundary
    }

    fn is_past_end(&self, encoded_key: &[u8]) -> bool {
        if let Some((end_key, inclusive)) = &self.end_bound {
            encoded_key > end_key.as_ref() || (!inclusive && encoded_key == end_key.as_ref())
        } else {
            false
        }
    }

    fn decode_next_row(&mut self) -> Result<Option<DecodedRow>> {
        if self.remaining_rows == Some(0) {
            return Ok(None);
        }
        if self.stopped_at_block_boundary {
            return Ok(None);
        }
        loop {
            if !self.inner.valid() {
                if !self.inner.next()? {
                    if self.inner.stopped_at_block_boundary() {
                        self.stopped_at_block_boundary = true;
                    }
                    return Ok(None);
                }
                continue;
            }
            let Some((mut encoded_key, kv_value)) = self.inner.take_current()? else {
                if !self.inner.next()? {
                    if self.inner.stopped_at_block_boundary() {
                        self.stopped_at_block_boundary = true;
                    }
                    return Ok(None);
                }
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
                |pointer| {
                    self.vlog_store
                        .read_pointer(&self.snapshot.vlog_version, pointer)
                },
                &self.schema,
                self.column_family_id,
                Some(self.ttl_provider.time_provider()),
            )?;
            if let Some(columns) = columns {
                return Ok(Some((key, columns)));
            }
        }
    }

    pub fn consume_next_row<T, F>(&mut self, mut consumer: F) -> Result<Option<T>>
    where
        F: FnMut(&Bytes, &[Option<Bytes>]) -> Result<T>,
    {
        let Some((key, columns)) = self.decode_next_row()? else {
            return Ok(None);
        };
        let consumed = consumer(key.data(), &columns)?;
        if let Some(remaining_rows) = self.remaining_rows.as_mut() {
            *remaining_rows -= 1;
        }
        Ok(Some(consumed))
    }

    pub fn consume_next_row_with_bucket<T, F>(&mut self, mut consumer: F) -> Result<Option<T>>
    where
        F: FnMut(u16, &Bytes, &[Option<Bytes>]) -> Result<T>,
    {
        let Some((bucket, key, columns)) = self.next_row_with_bucket()? else {
            return Ok(None);
        };
        let consumed = consumer(bucket, &key, &columns)?;
        Ok(Some(consumed))
    }

    pub(crate) fn next_row_with_bucket(&mut self) -> Result<Option<BucketedRow>> {
        let Some((mut key, columns)) = self.decode_next_row()? else {
            return Ok(None);
        };
        let bucket = key.bucket();
        if let Some(remaining_rows) = self.remaining_rows.as_mut() {
            *remaining_rows -= 1;
        }
        Ok(Some((bucket, key.take_data(), columns)))
    }

    fn next_row(&mut self) -> Result<Option<(Bytes, Vec<Option<Bytes>>)>> {
        let Some((_, key, columns)) = self.next_row_with_bucket()? else {
            return Ok(None);
        };
        Ok(Some((key, columns)))
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
    use crate::config::VolumeDescriptor;
    use crate::db_state::MultiLSMTreeVersion;
    use crate::file::{FileManager, FileSystemRegistry};
    use crate::lsm::LSMTreeVersion;
    use crate::metrics_manager::MetricsManager;
    use crate::schema::SchemaManager;
    use crate::{Config, Db, WriteBatch};
    use serial_test::serial;
    use std::collections::VecDeque;

    fn cleanup_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

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
            truncation_cursors: crate::db_state::new_truncation_cursors(),
            suggested_base_snapshot_id: None,
        });

        let iter = DbIterator::new(
            Vec::new(),
            Vec::new(),
            DbIteratorOptions {
                end_bound: None,
                lower_bound_exclusive: None,
                max_rows: None,
                snapshot,
                memtable_manager: None,
                access_guard: None,
                vlog_store,
                ttl_provider: Arc::new(TTLProvider::disabled()),
                schema: projected_schema,
                column_family_id: 1,
                should_stop_at_block_boundary: false,
            },
        );

        assert_eq!(iter.num_columns, 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_iterator_consume_next_row_passes_bytes_key() {
        let root = "/tmp/db_iterator_consume_next_row";
        cleanup_root(root);

        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
            num_columns: 2,
            total_buckets: 4,
            ..Config::default()
        };
        let db = Db::open(config, vec![0u16..=3u16]).unwrap();
        let mut batch = WriteBatch::new();
        batch.put(0, b"key1", 0, b"a0");
        batch.put(0, b"key1", 1, b"a1");
        batch.put(0, b"key2", 0, b"b0");
        db.write_batch(batch).unwrap();

        let mut iter = db.scan(0, b"key1"..b"key9").unwrap();
        let mut rows = Vec::new();
        while let Some(row) = iter
            .consume_next_row(|key, columns| Ok((key.clone(), columns.to_vec())))
            .unwrap()
        {
            rows.push(row);
        }

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0.as_ref(), b"key1");
        assert_eq!(rows[0].1.len(), 2);
        assert_eq!(rows[0].1[0].as_deref(), Some(b"a0".as_slice()));
        assert_eq!(rows[0].1[1].as_deref(), Some(b"a1".as_slice()));
        assert_eq!(rows[1].0.as_ref(), b"key2");
        assert_eq!(rows[1].1[0].as_deref(), Some(b"b0".as_slice()));
        assert_eq!(rows[1].1[1].as_deref(), None);

        cleanup_root(root);
    }
}
