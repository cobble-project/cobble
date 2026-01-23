use bytes::Bytes;
use std::collections::BTreeMap;

#[derive(PartialEq, Clone)]
pub(crate) enum WriteOp {
    Put(Bytes, Bytes),
    Delete(Bytes),
    Merge(Bytes, Bytes),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
pub(crate) struct KeyAndSeq {
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

    pub fn put<K, V>(&mut self, key: K, column: u16, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key_and_seq = KeyAndSeq {
            key: Bytes::copy_from_slice(key.as_ref()),
            column,
            seq: self.current_seq,
        };
        let write_op = WriteOp::Put(
            key_and_seq.key.clone(),
            Bytes::copy_from_slice(value.as_ref()),
        );
        self.ops.insert(key_and_seq, write_op);
        self.current_seq += 1;
    }

    pub fn delete<K>(&mut self, key: K, column: u16)
    where
        K: AsRef<[u8]>,
    {
        let key_and_seq = KeyAndSeq {
            key: Bytes::copy_from_slice(key.as_ref()),
            column,
            seq: self.current_seq,
        };
        let write_op = WriteOp::Delete(key_and_seq.key.clone());
        self.ops.insert(key_and_seq, write_op);
        self.current_seq += 1;
    }

    pub fn merge<K, V>(&mut self, key: K, column: u16, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key_and_seq = KeyAndSeq {
            key: Bytes::copy_from_slice(key.as_ref()),
            column,
            seq: self.current_seq,
        };
        let write_op = WriteOp::Merge(
            key_and_seq.key.clone(),
            Bytes::copy_from_slice(value.as_ref()),
        );
        self.ops.insert(key_and_seq, write_op);
        self.current_seq += 1;
    }
}
