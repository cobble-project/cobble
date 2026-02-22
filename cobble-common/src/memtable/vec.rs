//! A simple in-memory memtable implementation backed by bytes-owned KV pairs.
use crate::error::{Error, Result};
use crate::memtable::iter::OrderedMemtableKvIterator;
use crate::memtable::vlog::{RewrittenValuePlan, encode_rewritten_value};
use crate::memtable::{Memtable, MemtableReclaimer};
use crate::sst::row_codec::{encode_key_ref_into, encode_value_ref_into};
use crate::r#type::{RefKey, RefValue, ValueType};
use crate::vlog::VlogStore;
use bytes::{Bytes, BytesMut};
use std::collections::BTreeMap;

pub(crate) type MemtableKvIterator<'a> = OrderedMemtableKvIterator<'a>;

pub(crate) struct VecMemtable {
    entries: Vec<(Bytes, Bytes)>,
    blobs: BytesMut,
    capacity: usize,
    used_bytes: usize,
    reclaimer: Option<MemtableReclaimer>,
}

pub(crate) struct MemtableValueIter<'a> {
    mem: &'a VecMemtable,
    key: Vec<u8>,
    next_idx: usize,
}

impl VecMemtable {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::new(),
            blobs: BytesMut::new(),
            capacity,
            used_bytes: 0,
            reclaimer: None,
        }
    }

    pub(crate) fn with_capacity_and_reclaimer(
        capacity: usize,
        reclaimer: MemtableReclaimer,
    ) -> Self {
        let mut memtable = Self::with_capacity(capacity);
        memtable.reclaimer = Some(reclaimer);
        memtable
    }

    fn entry_size(key_len: usize, value_len: usize) -> usize {
        4 + 4 + key_len + value_len
    }

    pub(crate) fn estimate_capacity_for_ref(
        key: &RefKey<'_>,
        value: &RefValue<'_>,
        num_columns: usize,
        vlog_store: &VlogStore,
    ) -> usize {
        let key_len = key.encoded_len();
        let value_len = value.encoded_len(num_columns);
        let mut blob_bytes = 0usize;
        for column in value.columns().iter().take(num_columns).flatten() {
            if !matches!(column.value_type, ValueType::Put | ValueType::Merge) {
                continue;
            }
            if vlog_store.should_separate(column.data().len()) {
                blob_bytes = blob_bytes.saturating_add(4 + column.data().len());
            }
        }
        Self::entry_size(key_len, value_len).saturating_add(blob_bytes)
    }

    fn has_space(&self, needed: usize) -> Result<()> {
        if self.used_bytes + needed > self.capacity {
            return Err(Error::MemtableFull {
                needed,
                remaining: self.capacity.saturating_sub(self.used_bytes),
            });
        }
        Ok(())
    }
}

impl Memtable for VecMemtable {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let needed = Self::entry_size(key.len(), value.len());
        self.has_space(needed)?;
        self.entries
            .push((Bytes::copy_from_slice(key), Bytes::copy_from_slice(value)));
        self.used_bytes += needed;
        Ok(())
    }

    fn put_ref(
        &mut self,
        key: &RefKey<'_>,
        value: &RefValue<'_>,
        num_columns: usize,
    ) -> Result<()> {
        let key_len = key.encoded_len();
        let value_len = value.encoded_len(num_columns);
        let needed = Self::entry_size(key_len, value_len);
        self.has_space(needed)?;
        let mut encoded_key = vec![0u8; key_len];
        let mut encoded_value = vec![0u8; value_len];
        let mut key_slice = encoded_key.as_mut_slice();
        let mut value_slice = encoded_value.as_mut_slice();
        encode_key_ref_into(key, &mut key_slice);
        encode_value_ref_into(value, num_columns, &mut value_slice);
        self.entries
            .push((Bytes::from(encoded_key), Bytes::from(encoded_value)));
        self.used_bytes += needed;
        Ok(())
    }

    fn put_ref_rewritten(
        &mut self,
        key: &RefKey<'_>,
        plan: &RewrittenValuePlan<'_>,
        num_columns: usize,
    ) -> Result<()> {
        let key_len = key.encoded_len();
        let value_len = plan.encoded_len(num_columns);
        let needed = Self::entry_size(key_len, value_len);
        self.has_space(needed)?;
        let mut encoded_key = vec![0u8; key_len];
        let mut encoded_value = vec![0u8; value_len];
        let mut key_slice = encoded_key.as_mut_slice();
        encode_key_ref_into(key, &mut key_slice);
        encode_rewritten_value(plan, num_columns, &mut encoded_value[..]);
        self.entries
            .push((Bytes::from(encoded_key), Bytes::from(encoded_value)));
        self.used_bytes += needed;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.entries
            .iter()
            .rev()
            .find(|(entry_key, _)| entry_key.as_ref() == key)
            .map(|(_, value)| value.as_ref())
    }

    fn get_all(&self, key: &[u8]) -> Self::ValueIter<'_> {
        MemtableValueIter {
            mem: self,
            key: key.to_vec(),
            next_idx: self.entries.len(),
        }
    }

    fn remaining_capacity(&self) -> usize {
        self.capacity.saturating_sub(self.used_bytes)
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn append_blob(&mut self, data: &[u8]) -> Result<usize> {
        let encoded_len = 4usize
            .checked_add(data.len())
            .ok_or_else(|| Error::IoError("VLOG encoded value length overflow".to_string()))?;
        self.has_space(encoded_len)?;
        let offset = self.blobs.len();
        self.blobs.extend_from_slice(
            &(u32::try_from(data.len()).map_err(|_| {
                Error::IoError(format!("VLOG value too large: {} bytes", data.len()))
            })?)
            .to_le_bytes(),
        );
        self.blobs.extend_from_slice(data);
        self.used_bytes += encoded_len;
        Ok(offset + 4)
    }

    fn read_blob(&self, offset: usize, len: usize) -> Option<&[u8]> {
        let end = offset.checked_add(len)?;
        if end > self.blobs.len() {
            return None;
        }
        Some(&self.blobs[offset..end])
    }

    fn flush_blobs_to_vlog_writer(
        &self,
        _entries: &BTreeMap<u32, (usize, usize)>,
        writer: &mut crate::vlog::VlogWriter<Box<dyn crate::file::SequentialWriteFile>>,
    ) -> Result<()> {
        writer.write_encoded_records(&self.blobs)
    }

    fn blob_cursor_checkpoint(&self) -> usize {
        self.blobs.len()
    }

    fn rollback_blob_cursor(&mut self, checkpoint: usize) {
        if checkpoint <= self.blobs.len() {
            let reclaimed = self.blobs.len() - checkpoint;
            self.blobs.truncate(checkpoint);
            self.used_bytes = self.used_bytes.saturating_sub(reclaimed);
        }
    }

    fn iter(&self) -> Self::KvIter<'_> {
        let entries = self
            .entries
            .iter()
            .enumerate()
            .map(|(idx, (key, value))| (key.as_ref(), value.as_ref(), idx))
            .collect();
        MemtableKvIterator::new(entries)
    }

    type ValueIter<'a>
        = MemtableValueIter<'a>
    where
        Self: 'a;
    type KvIter<'a>
        = MemtableKvIterator<'a>
    where
        Self: 'a;
}

impl<'a> Iterator for MemtableValueIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        while self.next_idx > 0 {
            self.next_idx -= 1;
            let (entry_key, entry_value) = &self.mem.entries[self.next_idx];
            if entry_key.as_ref() == self.key.as_slice() {
                return Some(entry_value.as_ref());
            }
        }
        None
    }
}

impl Drop for VecMemtable {
    fn drop(&mut self) {
        if let Some(reclaimer) = &self.reclaimer {
            reclaimer(self.capacity as u64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iterator::KvIterator;
    use bytes::Bytes;

    #[test]
    fn put_and_get() {
        let mut mem = VecMemtable::with_capacity(1024);
        mem.put(b"key1", b"value1").unwrap();
        mem.put(b"key2", b"value2").unwrap();

        assert_eq!(mem.get(b"key1").unwrap(), b"value1");
        assert_eq!(mem.get(b"key2").unwrap(), b"value2");
        assert!(mem.get(b"missing").is_none());
    }

    #[test]
    fn overwrite_updates_value() {
        let mut mem = VecMemtable::with_capacity(1024);
        mem.put(b"key", b"old").unwrap();
        mem.put(b"key", b"new").unwrap();
        assert_eq!(mem.get(b"key").unwrap(), b"new");
    }

    #[test]
    fn capacity_enforced() {
        let mut mem = VecMemtable::with_capacity(24);
        mem.put(b"k1", b"v1").unwrap();
        let err = mem.put(b"k2", b"value_too_long").unwrap_err();
        match err {
            Error::MemtableFull { .. } => {}
            _ => panic!("unexpected error type"),
        }
    }

    #[test]
    fn get_all_returns_latest_first() {
        let mut mem = VecMemtable::with_capacity(512);
        mem.put(b"key", b"v1").unwrap();
        mem.put(b"key", b"v2").unwrap();
        mem.put(b"key", b"v3").unwrap();

        let mut iter = mem.get_all(b"key");
        assert_eq!(iter.next().unwrap(), b"v3");
        assert_eq!(iter.next().unwrap(), b"v2");
        assert_eq!(iter.next().unwrap(), b"v1");
        assert!(iter.next().is_none());
    }

    #[test]
    fn kv_iterator_orders_keys_and_values() {
        let mut mem = VecMemtable::with_capacity(1024);
        mem.put(b"b", b"v1").unwrap();
        mem.put(b"a", b"x1").unwrap();
        mem.put(b"a", b"x2").unwrap();
        mem.put(b"c", b"z1").unwrap();

        let mut iter = mem.iter();
        iter.seek_to_first().unwrap();
        let mut collected = Vec::new();
        while iter.next().unwrap() {
            let (k, v) = (iter.key().unwrap().unwrap(), iter.value().unwrap().unwrap());
            collected.push((k, v));
        }
        let expected = vec![
            (Bytes::from("a"), Bytes::from("x2")),
            (Bytes::from("a"), Bytes::from("x1")),
            (Bytes::from("b"), Bytes::from("v1")),
            (Bytes::from("c"), Bytes::from("z1")),
        ];
        assert_eq!(collected, expected);
    }

    #[test]
    fn blob_storage_round_trip() {
        let mut mem = VecMemtable::with_capacity(128);
        let offset = mem.append_blob(b"blob").unwrap();
        assert_eq!(mem.read_blob(offset, 4).unwrap(), b"blob");
        let checkpoint = mem.blob_cursor_checkpoint();
        let _ = mem.append_blob(b"-extra").unwrap();
        mem.rollback_blob_cursor(checkpoint);
        assert_eq!(mem.read_blob(offset, 4).unwrap(), b"blob");
    }
}
