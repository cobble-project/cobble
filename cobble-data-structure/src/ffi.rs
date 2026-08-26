//! Feature-gated bridge for language bindings.
//!
//! Only connector-specific raw or pre-encoded operations belong here. The
//! structured semantic API remains available from the crate root.

use crate::priority_queue::{DetachedPriorityQueueDescriptor, PriorityQueueBackend};
use crate::{
    StructuredDb, StructuredDbIterator, StructuredScanOptions, StructuredSingleDb,
    StructuredWriteBatch, StructuredWriteOptions,
};
use bytes::Bytes;
use cobble::Result;

/// Backend-independent priority-queue metadata for language bindings.
///
/// The descriptor owns normalized schema metadata and cached options, but no
/// database borrow. Operations receive their concrete database synchronously.
pub struct PriorityQueueDescriptor {
    inner: DetachedPriorityQueueDescriptor,
}

impl PriorityQueueDescriptor {
    #[inline]
    pub fn column_family(&self) -> &str {
        self.inner.column_family()
    }
}

#[inline]
pub fn db_new_priority_queue_descriptor(
    db: &mut StructuredDb,
    name: String,
) -> Result<PriorityQueueDescriptor> {
    Ok(PriorityQueueDescriptor {
        inner: db.new_priority_queue(name)?.detached_descriptor(),
    })
}

#[inline]
pub fn db_get_priority_queue_descriptor(
    db: &StructuredDb,
    name: String,
) -> Result<PriorityQueueDescriptor> {
    Ok(PriorityQueueDescriptor {
        inner: db.get_priority_queue(name)?.detached_descriptor(),
    })
}

#[inline]
pub fn db_get_or_new_priority_queue_descriptor(
    db: &mut StructuredDb,
    name: String,
) -> Result<PriorityQueueDescriptor> {
    Ok(PriorityQueueDescriptor {
        inner: db.get_or_new_priority_queue(name)?.detached_descriptor(),
    })
}

#[inline]
pub fn single_db_new_priority_queue_descriptor(
    db: &mut StructuredSingleDb,
    name: String,
) -> Result<PriorityQueueDescriptor> {
    Ok(PriorityQueueDescriptor {
        inner: db.new_priority_queue(name)?.detached_descriptor(),
    })
}

#[inline]
pub fn single_db_get_priority_queue_descriptor(
    db: &StructuredSingleDb,
    name: String,
) -> Result<PriorityQueueDescriptor> {
    Ok(PriorityQueueDescriptor {
        inner: db.get_priority_queue(name)?.detached_descriptor(),
    })
}

#[inline]
pub fn single_db_get_or_new_priority_queue_descriptor(
    db: &mut StructuredSingleDb,
    name: String,
) -> Result<PriorityQueueDescriptor> {
    Ok(PriorityQueueDescriptor {
        inner: db.get_or_new_priority_queue(name)?.detached_descriptor(),
    })
}

macro_rules! priority_queue_backend_functions {
    ($offer:ident, $delete:ident, $peek_batch:ident, $advance:ident, $cursor:ident,
     $db:ty, $variant:ident) => {
        #[inline]
        pub fn $offer(
            db: &$db,
            descriptor: &PriorityQueueDescriptor,
            bucket: u16,
            key: &[u8],
            value: &[u8],
        ) -> Result<()> {
            descriptor
                .inner
                .offer(PriorityQueueBackend::$variant(db), bucket, key, value)
        }

        #[inline]
        pub fn $delete(
            db: &$db,
            descriptor: &PriorityQueueDescriptor,
            bucket: u16,
            key: &[u8],
        ) -> Result<()> {
            descriptor
                .inner
                .delete(PriorityQueueBackend::$variant(db), bucket, key)
        }

        #[inline]
        pub fn $peek_batch(
            db: &$db,
            descriptor: &PriorityQueueDescriptor,
            bucket: u16,
            limit: Option<usize>,
        ) -> Result<Vec<(Bytes, Bytes)>> {
            descriptor
                .inner
                .scan_batch(PriorityQueueBackend::$variant(db), bucket, limit, false)
        }

        #[inline]
        pub fn $advance(
            db: &$db,
            descriptor: &PriorityQueueDescriptor,
            bucket: u16,
            key: &[u8],
        ) -> Result<()> {
            descriptor
                .inner
                .advance_cursor(PriorityQueueBackend::$variant(db), bucket, key)
        }

        #[inline]
        pub fn $cursor(
            db: &$db,
            descriptor: &PriorityQueueDescriptor,
            bucket: u16,
        ) -> Result<Option<Vec<u8>>> {
            descriptor
                .inner
                .cursor(PriorityQueueBackend::$variant(db), bucket)
        }
    };
}

priority_queue_backend_functions!(
    db_priority_queue_offer,
    db_priority_queue_delete,
    db_priority_queue_peek_batch,
    db_priority_queue_advance,
    db_priority_queue_cursor,
    StructuredDb,
    StructuredDb
);

priority_queue_backend_functions!(
    single_db_priority_queue_offer,
    single_db_priority_queue_delete,
    single_db_priority_queue_peek_batch,
    single_db_priority_queue_advance,
    single_db_priority_queue_cursor,
    StructuredSingleDb,
    StructuredSingleDb
);

#[inline]
pub fn db_direct_buffer_pool_config(db: &StructuredDb) -> Result<(usize, usize)> {
    db.jni_direct_buffer_pool_config()
}

#[inline]
pub fn single_db_direct_buffer_pool_config(db: &StructuredSingleDb) -> Result<(usize, usize)> {
    db.jni_direct_buffer_pool_config()
}

#[inline]
pub fn db_put_bytes_batch_with_options<'a, I>(
    db: &StructuredDb,
    bucket: u16,
    column: u16,
    entries: I,
    options: &StructuredWriteOptions,
) -> Result<()>
where
    I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
{
    db.put_bytes_batch_with_options(bucket, column, entries, options)
}

#[inline]
pub fn db_put_borrowed_bytes_with_options<K>(
    db: &StructuredDb,
    bucket: u16,
    key: K,
    column: u16,
    value: &[u8],
    options: &StructuredWriteOptions,
) -> Result<()>
where
    K: AsRef<[u8]>,
{
    db.put_borrowed_bytes_with_options(bucket, key, column, value, options)
}

#[inline]
pub fn db_merge_borrowed_bytes_with_options<K>(
    db: &StructuredDb,
    bucket: u16,
    key: K,
    column: u16,
    value: &[u8],
    options: &StructuredWriteOptions,
) -> Result<()>
where
    K: AsRef<[u8]>,
{
    db.merge_borrowed_bytes_with_options(bucket, key, column, value, options)
}

#[inline]
pub fn single_db_put_borrowed_bytes_with_options<K>(
    db: &StructuredSingleDb,
    bucket: u16,
    key: K,
    column: u16,
    value: &[u8],
    options: &StructuredWriteOptions,
) -> Result<()>
where
    K: AsRef<[u8]>,
{
    db.put_borrowed_bytes_with_options(bucket, key, column, value, options)
}

#[inline]
pub fn single_db_merge_borrowed_bytes_with_options<K>(
    db: &StructuredSingleDb,
    bucket: u16,
    key: K,
    column: u16,
    value: &[u8],
    options: &StructuredWriteOptions,
) -> Result<()>
where
    K: AsRef<[u8]>,
{
    db.merge_borrowed_bytes_with_options(bucket, key, column, value, options)
}

#[inline]
pub fn write_batch_put_borrowed_bytes_with_options<K>(
    batch: &mut StructuredWriteBatch,
    bucket: u16,
    key: K,
    column: u16,
    value: &[u8],
    options: &StructuredWriteOptions,
) -> Result<()>
where
    K: AsRef<[u8]>,
{
    batch.put_borrowed_bytes_with_options(bucket, key, column, value, options)
}

#[inline]
pub fn write_batch_merge_borrowed_bytes_with_options<K>(
    batch: &mut StructuredWriteBatch,
    bucket: u16,
    key: K,
    column: u16,
    value: &[u8],
    options: &StructuredWriteOptions,
) -> Result<()>
where
    K: AsRef<[u8]>,
{
    batch.merge_borrowed_bytes_with_options(bucket, key, column, value, options)
}

#[inline]
pub fn write_batch_put_borrowed_list_with_options<K>(
    batch: &mut StructuredWriteBatch,
    bucket: u16,
    key: K,
    column: u16,
    elements: &[&[u8]],
    options: &StructuredWriteOptions,
) -> Result<()>
where
    K: AsRef<[u8]>,
{
    batch.put_borrowed_list_with_options(bucket, key, column, elements, options)
}

#[inline]
pub fn write_batch_merge_borrowed_list_with_options<K>(
    batch: &mut StructuredWriteBatch,
    bucket: u16,
    key: K,
    column: u16,
    elements: &[&[u8]],
    options: &StructuredWriteOptions,
) -> Result<()>
where
    K: AsRef<[u8]>,
{
    batch.merge_borrowed_list_with_options(bucket, key, column, elements, options)
}

#[inline]
pub fn single_db_scan_with_options_bounds(
    db: &StructuredSingleDb,
    bucket: u16,
    start_key_inclusive: Option<&[u8]>,
    end_key_exclusive: Option<&[u8]>,
    options: &StructuredScanOptions,
) -> Result<StructuredDbIterator> {
    db.scan_with_options_bounds_for_ffi(bucket, start_key_inclusive, end_key_exclusive, options)
}

#[inline]
pub fn iterator_stopped_at_block_boundary(iterator: &StructuredDbIterator) -> bool {
    iterator.stopped_at_block_boundary()
}

#[inline]
pub fn iterator_clear_stop_at_block_boundary(iterator: &mut StructuredDbIterator) {
    iterator.clear_stop_at_block_boundary();
}

#[inline]
pub fn db_put_encoded_list<K, B>(
    db: &StructuredDb,
    bucket: u16,
    key: K,
    column: u16,
    encoded: B,
) -> Result<()>
where
    K: AsRef<[u8]>,
    B: Into<Bytes>,
{
    db.put_encoded_list(bucket, key, column, encoded)
}

#[inline]
pub fn db_put_encoded_list_with_options<K, B>(
    db: &StructuredDb,
    bucket: u16,
    key: K,
    column: u16,
    encoded: B,
    options: &StructuredWriteOptions,
) -> Result<()>
where
    K: AsRef<[u8]>,
    B: Into<Bytes>,
{
    db.put_encoded_list_with_options(bucket, key, column, encoded, options)
}

#[inline]
pub fn db_merge_encoded_list<K, B>(
    db: &StructuredDb,
    bucket: u16,
    key: K,
    column: u16,
    encoded: B,
) -> Result<()>
where
    K: AsRef<[u8]>,
    B: Into<Bytes>,
{
    db.merge_encoded_list(bucket, key, column, encoded)
}

#[inline]
pub fn db_merge_encoded_list_with_options<K, B>(
    db: &StructuredDb,
    bucket: u16,
    key: K,
    column: u16,
    encoded: B,
    options: &StructuredWriteOptions,
) -> Result<()>
where
    K: AsRef<[u8]>,
    B: Into<Bytes>,
{
    db.merge_encoded_list_with_options(bucket, key, column, encoded, options)
}

#[inline]
pub fn db_put_borrowed_list_with_options<K>(
    db: &StructuredDb,
    bucket: u16,
    key: K,
    column: u16,
    elements: &[&[u8]],
    options: &StructuredWriteOptions,
) -> Result<()>
where
    K: AsRef<[u8]>,
{
    db.put_borrowed_list_with_options(bucket, key, column, elements, options)
}

#[inline]
pub fn db_merge_borrowed_list_with_options<K>(
    db: &StructuredDb,
    bucket: u16,
    key: K,
    column: u16,
    elements: &[&[u8]],
    options: &StructuredWriteOptions,
) -> Result<()>
where
    K: AsRef<[u8]>,
{
    db.merge_borrowed_list_with_options(bucket, key, column, elements, options)
}

#[inline]
pub fn single_db_put_borrowed_list_with_options<K>(
    db: &StructuredSingleDb,
    bucket: u16,
    key: K,
    column: u16,
    elements: &[&[u8]],
    options: &StructuredWriteOptions,
) -> Result<()>
where
    K: AsRef<[u8]>,
{
    db.put_borrowed_list_with_options(bucket, key, column, elements, options)
}

#[inline]
pub fn single_db_merge_borrowed_list_with_options<K>(
    db: &StructuredSingleDb,
    bucket: u16,
    key: K,
    column: u16,
    elements: &[&[u8]],
    options: &StructuredWriteOptions,
) -> Result<()>
where
    K: AsRef<[u8]>,
{
    db.merge_borrowed_list_with_options(bucket, key, column, elements, options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ListConfig, StructuredColumnValue};
    use cobble::{Config, Error, VolumeDescriptor};
    use uuid::Uuid;

    fn config(prefix: &str) -> (String, Config) {
        let root = format!("/tmp/{prefix}_{}", Uuid::new_v4());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
            num_columns: 2,
            total_buckets: 1,
            ..Config::default()
        };
        (root, config)
    }

    fn assert_bytes_type_error(result: Result<()>) {
        assert!(matches!(
            result,
            Err(Error::InputError(message)) if message.contains("not a BYTES column")
        ));
    }

    #[test]
    fn borrowed_bytes_helpers_validate_types_and_preserve_put_merge_semantics() {
        let options = StructuredWriteOptions::default();

        let (db_root, db_config) = config("ds_ffi_borrowed_bytes_db");
        let mut db = StructuredDb::open(db_config, vec![0..=0]).unwrap();
        db.update_schema()
            .add_list_column(None, 1, ListConfig::default())
            .commit()
            .unwrap();
        db_put_borrowed_bytes_with_options(&db, 0, b"key", 0, b"left", &options).unwrap();
        db_merge_borrowed_bytes_with_options(&db, 0, b"key", 0, b"-right", &options).unwrap();
        assert_bytes_type_error(db_put_borrowed_bytes_with_options(
            &db, 0, b"key", 1, b"wrong", &options,
        ));
        let row = db.get(0, b"key").unwrap().unwrap();
        assert_eq!(
            row[0],
            Some(StructuredColumnValue::Bytes(Bytes::from_static(
                b"left-right"
            )))
        );
        db.close().unwrap();
        drop(db);
        let _ = std::fs::remove_dir_all(db_root);

        let (single_root, single_config) = config("ds_ffi_borrowed_bytes_single");
        let mut single = StructuredSingleDb::open(single_config).unwrap();
        single
            .update_schema()
            .add_list_column(None, 1, ListConfig::default())
            .commit()
            .unwrap();
        single_db_put_borrowed_bytes_with_options(&single, 0, b"key", 0, b"left", &options)
            .unwrap();
        single_db_merge_borrowed_bytes_with_options(&single, 0, b"key", 0, b"-right", &options)
            .unwrap();
        assert_bytes_type_error(single_db_merge_borrowed_bytes_with_options(
            &single, 0, b"key", 1, b"wrong", &options,
        ));
        let row = single.get(0, b"key").unwrap().unwrap();
        assert_eq!(
            row[0],
            Some(StructuredColumnValue::Bytes(Bytes::from_static(
                b"left-right"
            )))
        );
        single.close().unwrap();
        drop(single);
        let _ = std::fs::remove_dir_all(single_root);
    }

    #[test]
    fn detached_priority_queue_descriptor_reuses_validated_metadata() {
        let (root, config) = config("ds_ffi_priority_queue_descriptor");
        let mut db = StructuredSingleDb::open(config).unwrap();
        let descriptor =
            single_db_new_priority_queue_descriptor(&mut db, " jobs ".to_string()).unwrap();
        assert_eq!(descriptor.column_family(), "jobs");

        single_db_priority_queue_offer(&db, &descriptor, 0, b"k", b"left").unwrap();
        single_db_priority_queue_offer(&db, &descriptor, 0, b"k", b"-right").unwrap();
        let first = single_db_priority_queue_peek_batch(&db, &descriptor, 0, Some(1)).unwrap();
        let second = single_db_priority_queue_peek_batch(&db, &descriptor, 0, Some(1)).unwrap();
        assert_eq!(first, second);
        assert_eq!(first[0].0.as_ref(), b"k");
        assert_eq!(first[0].1.as_ref(), b"left-right");

        db.update_schema()
            .add_bytes_column(Some("plain".to_string()), 0)
            .commit()
            .unwrap();
        assert!(single_db_get_priority_queue_descriptor(&db, "plain".to_string()).is_err());
        db.close().unwrap();
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }
}
