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
