use bytes::Bytes;
use std::collections::BTreeMap;

use crate::WriteOptions;

#[derive(PartialEq, Clone)]
pub(crate) enum WriteOp {
    Put(Bytes, Bytes, Option<u32>),
    Delete(Bytes),
    Merge(Bytes, Bytes, Option<u32>),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
pub(crate) struct KeyAndSeq {
    pub bucket: u16,
    pub key: Bytes,
    pub column: u16,
    pub seq: u64,
}

/// A write operation in a batch.
#[derive(Clone)]
pub struct WriteBatch {
    pub(crate) ops: BTreeMap<KeyAndSeq, WriteOp>,
    pub(crate) current_seq: u64,
}

impl WriteBatch {
    /// Creates a new empty `WriteBatch`.
    pub fn new() -> Self {
        Self {
            ops: BTreeMap::new(),
            current_seq: 0,
        }
    }

    fn insert_op(&mut self, bucket: u16, key: Bytes, column: u16, op: WriteOp) {
        let key_and_seq = KeyAndSeq {
            bucket,
            key: key.clone(),
            column,
            seq: self.current_seq,
        };
        self.ops.insert(key_and_seq, op);
        self.current_seq += 1;
    }

    pub fn put<K, V>(&mut self, bucket: u16, key: K, column: u16, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_with_options(bucket, key, column, value, &WriteOptions::default());
    }

    pub fn put_with_options<K, V>(
        &mut self,
        bucket: u16,
        key: K,
        column: u16,
        value: V,
        options: &WriteOptions,
    ) where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key_bytes = Bytes::copy_from_slice(key.as_ref());
        let value = Bytes::copy_from_slice(value.as_ref());
        self.insert_op(
            bucket,
            key_bytes.clone(),
            column,
            WriteOp::Put(key_bytes, value, options.ttl_seconds),
        );
    }

    pub fn delete<K>(&mut self, bucket: u16, key: K, column: u16)
    where
        K: AsRef<[u8]>,
    {
        let key_bytes = Bytes::copy_from_slice(key.as_ref());
        self.insert_op(
            bucket,
            key_bytes.clone(),
            column,
            WriteOp::Delete(key_bytes),
        );
    }

    pub fn merge<K, V>(&mut self, bucket: u16, key: K, column: u16, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.merge_with_options(bucket, key, column, value, &WriteOptions::default());
    }

    pub fn merge_with_options<K, V>(
        &mut self,
        bucket: u16,
        key: K,
        column: u16,
        value: V,
        options: &WriteOptions,
    ) where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key_bytes = Bytes::copy_from_slice(key.as_ref());
        let value = Bytes::copy_from_slice(value.as_ref());
        self.insert_op(
            bucket,
            key_bytes.clone(),
            column,
            WriteOp::Merge(key_bytes, value, options.ttl_seconds),
        );
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}
