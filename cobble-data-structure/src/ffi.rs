//! Feature-gated bridge for language bindings.
//!
//! Only connector-specific raw or pre-encoded operations belong here. The
//! structured semantic API remains available from the crate root.

use crate::{StructuredDb, StructuredSingleDb, StructuredWriteOptions};
use bytes::Bytes;
use cobble::Result;

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
}
