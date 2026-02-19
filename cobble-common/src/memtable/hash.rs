use bytes::{Buf, BufMut, Bytes};
use std::cmp::Ordering;

use crate::error::{Error, Result};
use crate::iterator::KvIterator;
use crate::memtable::Memtable;
use crate::memtable::vlog::{RewrittenValuePlan, encode_rewritten_value};
use crate::sst::row_codec::{encode_key_ref_into, encode_value_ref_into};
use crate::r#type::{RefKey, RefValue};
use std::sync::Arc;

/// Type alias for memtable reclaimer function.
pub(crate) type MemtableReclaimer = Arc<dyn Fn(u64) + Send + Sync>;

/// Hash-indexed memtable storing entries, blobs, index nodes and bucket table in one buffer.
///
/// Layout:
/// - `[0, data_end)`: encoded KV entries, appended left-to-right.
/// - `[index_cursor, bucket_base)`: scratch area grown right-to-left by blob payloads and hash
///   index nodes.
/// - `[bucket_base, capacity)`: fixed bucket head table (`u32` offsets).
pub(crate) struct HashMemtable {
    buffer: Vec<u8>,
    data_end: usize,
    index_cursor: usize,
    bucket_base: usize,
    bucket_count: usize,
    reclaimer: Option<MemtableReclaimer>,
}

pub(crate) struct MemtableValueIter<'a> {
    mem: &'a HashMemtable,
    key: Vec<u8>,
    next_node: u32,
    bucket: usize,
}

#[derive(PartialEq, Eq)]
struct KVPair<'a> {
    key: &'a [u8],
    value: &'a [u8],
    offset: usize,
}

impl Ord for KVPair<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = self.key.cmp(other.key);
        if ord == Ordering::Equal {
            other.offset.cmp(&self.offset)
        } else {
            ord
        }
    }
}

impl PartialOrd for KVPair<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) struct MemtableKvIterator<'a> {
    mem: &'a HashMemtable,
    key_values: Vec<KVPair<'a>>,
    key_idx: usize,
    current_key: Option<&'a [u8]>,
    current_value: Option<&'a [u8]>,
}

impl HashMemtable {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        let bucket_count = Self::default_bucket_count(capacity);
        Self::with_capacity_and_buckets(capacity, bucket_count)
    }

    pub(crate) fn with_buffer(mut buffer: Vec<u8>) -> Self {
        let capacity = buffer.len();
        let bucket_count = Self::default_bucket_count(capacity);
        let bucket_count = bucket_count.max(1);
        let bucket_table_bytes = bucket_count * 4;
        assert!(
            capacity > bucket_table_bytes,
            "capacity must exceed bucket table bytes"
        );
        let bucket_base = capacity - bucket_table_bytes;
        Self::init_bucket_table(&mut buffer, bucket_base);
        Self {
            buffer,
            data_end: 0,
            index_cursor: bucket_base,
            bucket_base,
            bucket_count,
            reclaimer: None,
        }
    }

    pub(crate) fn with_buffer_and_reclaimer(buffer: Vec<u8>, reclaimer: MemtableReclaimer) -> Self {
        let mut memtable = Self::with_buffer(buffer);
        memtable.reclaimer = Some(reclaimer);
        memtable
    }

    fn with_capacity_and_buckets(capacity: usize, bucket_count: usize) -> Self {
        let bucket_count = bucket_count.max(1);
        let bucket_table_bytes = bucket_count * 4;
        assert!(
            capacity > bucket_table_bytes,
            "capacity must exceed bucket table bytes"
        );
        let mut buffer = vec![0u8; capacity];
        let bucket_base = capacity - bucket_table_bytes;
        Self::init_bucket_table(&mut buffer, bucket_base);
        Self {
            buffer,
            data_end: 0,
            index_cursor: bucket_base,
            bucket_base,
            bucket_count,
            reclaimer: None,
        }
    }

    fn default_bucket_count(capacity: usize) -> usize {
        let target = capacity / 128;
        target.clamp(4, 1024)
    }

    fn init_bucket_table(buffer: &mut [u8], bucket_base: usize) {
        for chunk in buffer[bucket_base..].chunks_mut(4) {
            chunk.copy_from_slice(&u32::MAX.to_le_bytes());
        }
    }

    fn hash_key(key: &[u8]) -> u64 {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;
        let mut hash = FNV_OFFSET;
        for &b in key {
            hash ^= b as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    fn entry_size(key_len: usize, value_len: usize) -> usize {
        4 + 4 + key_len + value_len
    }

    fn index_entry_size() -> usize {
        8 + 4 + 4
    }

    fn has_space(&self, data_len: usize) -> Result<()> {
        let need = data_len + Self::index_entry_size();
        if self.data_end + need > self.index_cursor {
            return Err(Error::MemtableFull {
                needed: need,
                remaining: self.index_cursor.saturating_sub(self.data_end),
            });
        }
        Ok(())
    }

    pub(crate) fn append_blob(&mut self, data: &[u8]) -> Result<usize> {
        if self.data_end + data.len() > self.index_cursor {
            return Err(Error::MemtableFull {
                needed: data.len(),
                remaining: self.index_cursor.saturating_sub(self.data_end),
            });
        }
        let start = self.index_cursor - data.len();
        self.buffer[start..self.index_cursor].copy_from_slice(data);
        self.index_cursor = start;
        Ok(start)
    }

    pub(crate) fn read_blob(&self, offset: usize, len: usize) -> Option<&[u8]> {
        let end = offset.checked_add(len)?;
        if offset < self.index_cursor || end > self.bucket_base {
            return None;
        }
        Some(&self.buffer[offset..end])
    }

    pub(crate) fn blob_cursor_checkpoint(&self) -> usize {
        self.index_cursor
    }

    pub(crate) fn rollback_blob_cursor(&mut self, checkpoint: usize) {
        self.index_cursor = checkpoint;
    }

    fn write_data(&mut self, key: &[u8], value: &[u8]) -> usize {
        let key_len = key.len() as u32;
        let value_len = value.len() as u32;
        let start = self.data_end;
        let end = start + Self::entry_size(key.len(), value.len());
        let mut slice = &mut self.buffer[start..end];
        slice.put_u32(key_len);
        slice.put_u32(value_len);
        slice.put_slice(key);
        slice.put_slice(value);
        self.data_end = end;
        start
    }

    fn write_data_ref(
        &mut self,
        key: &RefKey<'_>,
        value: &RefValue<'_>,
        num_columns: usize,
        key_len: usize,
        value_len: usize,
    ) -> (usize, usize) {
        let start = self.data_end;
        let end = start + Self::entry_size(key_len, value_len);
        let mut slice = &mut self.buffer[start..end];
        slice.put_u32(key_len as u32);
        slice.put_u32(value_len as u32);
        encode_key_ref_into(key, &mut slice);
        encode_value_ref_into(value, num_columns, &mut slice);
        self.data_end = end;
        (start, start + 8)
    }

    pub(crate) fn put_ref(
        &mut self,
        key: &RefKey<'_>,
        value: &RefValue<'_>,
        num_columns: usize,
    ) -> Result<()> {
        let key_len = key.encoded_len();
        let value_len = value.encoded_len(num_columns);
        let data_len = Self::entry_size(key_len, value_len);
        self.has_space(data_len)?;
        let (data_offset, key_offset) =
            self.write_data_ref(key, value, num_columns, key_len, value_len);
        let hash = Self::hash_key(&self.buffer[key_offset..key_offset + key_len]);
        let bucket = self.bucket_index_from_hash(hash);
        let node_off = self.write_index(bucket, hash, data_offset as u32);
        self.set_bucket_head(bucket, node_off);
        Ok(())
    }

    pub(crate) fn put_ref_rewritten(
        &mut self,
        key: &RefKey<'_>,
        plan: &RewrittenValuePlan<'_>,
        num_columns: usize,
    ) -> Result<()> {
        let value_len = plan.encoded_len(num_columns);
        let key_len = key.encoded_len();
        let data_len = Self::entry_size(key_len, value_len);
        self.has_space(data_len)?;
        let start = self.data_end;
        let end = start + data_len;
        let mut slice = &mut self.buffer[start..end];
        slice.put_u32(key_len as u32);
        slice.put_u32(value_len as u32);
        encode_key_ref_into(key, &mut slice);
        encode_rewritten_value(plan, num_columns, &mut slice[..value_len]);
        self.data_end = end;
        let key_offset = start + 8;
        let hash = Self::hash_key(&self.buffer[key_offset..key_offset + key_len]);
        let bucket = self.bucket_index_from_hash(hash);
        let node_off = self.write_index(bucket, hash, start as u32);
        self.set_bucket_head(bucket, node_off);
        Ok(())
    }

    fn bucket_head(&self, bucket: usize) -> u32 {
        let pos = self.bucket_base + bucket * 4;
        let mut slice = &self.buffer[pos..pos + 4];
        slice.get_u32_le()
    }

    fn set_bucket_head(&mut self, bucket: usize, head: u32) {
        let pos = self.bucket_base + bucket * 4;
        let mut slice = &mut self.buffer[pos..pos + 4];
        slice.put_u32_le(head);
    }

    fn write_index(&mut self, bucket: usize, hash: u64, key_offset: u32) -> u32 {
        let entry_size = Self::index_entry_size();
        let start = self.index_cursor - entry_size;
        let head = self.bucket_head(bucket);
        {
            let mut slice = &mut self.buffer[start..self.index_cursor];
            slice.put_u64(hash);
            slice.put_u32(key_offset);
            slice.put_u32(head);
        }
        self.index_cursor = start;
        start as u32
    }

    fn bucket_index_from_hash(&self, hash: u64) -> usize {
        (hash as usize) % self.bucket_count
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data_end == 0
    }
}

impl Memtable for HashMemtable {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let data_len = Self::entry_size(key.len(), value.len());
        self.has_space(data_len)?;
        let data_offset = self.write_data(key, value);
        let hash = Self::hash_key(key);
        let bucket = self.bucket_index_from_hash(hash);
        let node_off = self.write_index(bucket, hash, data_offset as u32);
        self.set_bucket_head(bucket, node_off);
        Ok(())
    }

    fn put_ref(
        &mut self,
        key: &RefKey<'_>,
        value: &RefValue<'_>,
        num_columns: usize,
    ) -> Result<()> {
        HashMemtable::put_ref(self, key, value, num_columns)
    }

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let hash = Self::hash_key(key);
        let bucket = self.bucket_index_from_hash(hash);
        let mut node_off = self.bucket_head(bucket);
        while node_off != u32::MAX {
            let start = node_off as usize;
            if start + Self::index_entry_size() > self.buffer.len() {
                break;
            }
            let mut node_slice = &self.buffer[start..start + Self::index_entry_size()];
            let h = node_slice.get_u64();
            let key_off = node_slice.get_u32() as usize;
            let next = node_slice.get_u32();
            if h == hash && key_off + 8 <= self.data_end {
                let mut slice = &self.buffer[key_off..self.data_end];
                let key_len = slice.get_u32() as usize;
                let value_len = slice.get_u32() as usize;
                if key_len + value_len <= slice.remaining() && slice[..key_len] == *key {
                    let value_start = key_len;
                    let value_end = value_start + value_len;
                    return Some(&slice[value_start..value_end]);
                }
            }
            node_off = next;
        }
        None
    }

    fn get_all(&self, key: &[u8]) -> MemtableValueIter<'_> {
        let bucket = self.bucket_index_from_hash(Self::hash_key(key));
        let head = self.bucket_head(bucket);
        MemtableValueIter {
            mem: self,
            key: key.to_vec(),
            next_node: head,
            bucket,
        }
    }

    fn remaining_capacity(&self) -> usize {
        self.index_cursor.saturating_sub(self.data_end)
    }

    /// Returns an iterator over all key-value pairs ordered by key bytes ascending.
    /// For duplicate keys, values are yielded in reverse insertion order (latest first).
    fn iter(&self) -> MemtableKvIterator<'_> {
        let mut key_values: Vec<KVPair> = Vec::new();
        let mut offset = 0;
        while offset < self.data_end {
            if offset + 8 > self.data_end {
                break;
            }
            let mut slice = &self.buffer[offset..self.data_end];
            let key_len = slice.get_u32() as usize;
            let value_len = slice.get_u32() as usize;
            if key_len + value_len > slice.remaining() {
                break;
            }
            key_values.push(KVPair {
                key: &slice[..key_len],
                value: &slice[key_len..key_len + value_len],
                offset,
            });
            offset += Self::entry_size(key_len, value_len);
        }
        key_values.sort();
        MemtableKvIterator {
            mem: self,
            key_values,
            key_idx: 0,
            current_key: None,
            current_value: None,
        }
    }

    type ValueIter<'a>
        = MemtableValueIter<'a>
    where
        Self: 'a;

    type KvIter<'a> = MemtableKvIterator<'a>;
}

impl<'a> Iterator for MemtableValueIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        while self.next_node != u32::MAX {
            let start = self.next_node as usize;
            if start + HashMemtable::index_entry_size() > self.mem.buffer.len() {
                self.next_node = u32::MAX;
                return None;
            }
            let mut node_slice = &self.mem.buffer[start..start + HashMemtable::index_entry_size()];
            let h = node_slice.get_u64();
            let key_off = node_slice.get_u32() as usize;
            let next = node_slice.get_u32();
            self.next_node = next;
            if h == HashMemtable::hash_key(&self.key) && key_off + 8 <= self.mem.data_end {
                let mut slice = &self.mem.buffer[key_off..self.mem.data_end];
                let key_len = slice.get_u32() as usize;
                let value_len = slice.get_u32() as usize;
                if key_len + value_len <= slice.remaining() && slice[..key_len] == self.key {
                    let value_start = key_len;
                    let value_end = value_start + value_len;
                    return Some(&slice[value_start..value_end]);
                }
            }
        }
        None
    }
}

impl Drop for HashMemtable {
    fn drop(&mut self) {
        if let Some(reclaimer) = &self.reclaimer {
            reclaimer(self.buffer.len() as u64);
        }
    }
}

impl<'a> KvIterator<'a> for MemtableKvIterator<'a> {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        match self.key_values.binary_search_by(|k| k.key.cmp(target)) {
            Ok(idx) => self.key_idx = idx,
            Err(idx) => self.key_idx = idx,
        }
        self.current_key = None;
        self.current_value = None;
        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.key_idx = 0;
        self.current_key = None;
        self.current_value = None;
        Ok(())
    }

    fn next(&mut self) -> Result<bool> {
        if self.key_idx >= self.key_values.len() {
            self.current_key = None;
            self.current_value = None;
            return Ok(false);
        }
        let key_value = &self.key_values[self.key_idx];
        self.key_idx += 1;
        self.current_key = Some(key_value.key);
        self.current_value = Some(key_value.value);
        Ok(true)
    }

    fn valid(&self) -> bool {
        self.current_key.is_some() && self.current_value.is_some()
    }

    fn key(&self) -> Result<Option<Bytes>> {
        Ok(self.current_key.map(Bytes::copy_from_slice))
    }

    fn key_slice(&self) -> Result<Option<&[u8]>> {
        Ok(self.current_key)
    }

    fn value(&self) -> Result<Option<Bytes>> {
        Ok(self.current_value.map(Bytes::copy_from_slice))
    }

    fn value_slice(&self) -> Result<Option<&[u8]>> {
        Ok(self.current_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_get() {
        let mut mem = HashMemtable::with_capacity(1024);
        mem.put(b"key1", b"value1").unwrap();
        mem.put(b"key2", b"value2").unwrap();

        assert_eq!(mem.get(b"key1").unwrap(), b"value1");
        assert_eq!(mem.get(b"key2").unwrap(), b"value2");
        assert!(mem.get(b"missing").is_none());
    }

    #[test]
    fn overwrite_updates_value() {
        let mut mem = HashMemtable::with_capacity(1024);
        mem.put(b"key", b"old").unwrap();
        mem.put(b"key", b"new").unwrap();
        assert_eq!(mem.get(b"key").unwrap(), b"new");
    }

    #[test]
    fn capacity_enforced() {
        let mut mem = HashMemtable::with_capacity(64);
        mem.put(b"k1", b"v1").unwrap();
        let err = mem.put(b"k2", b"value_too_long").unwrap_err();
        match err {
            Error::MemtableFull { .. } => {}
            _ => panic!("unexpected error type"),
        }
    }

    #[test]
    fn remaining_capacity_updates() {
        let mut mem = HashMemtable::with_capacity(100);
        let before = mem.remaining_capacity();
        mem.put(b"k", b"v").unwrap();
        assert!(mem.remaining_capacity() < before);
    }

    #[test]
    fn bucket_distribution_and_lookup() {
        // Use small bucket count to force chaining.
        let mut mem = HashMemtable::with_capacity_and_buckets(256, 4);
        mem.put(b"key1", b"v1").unwrap();
        mem.put(b"key2", b"v2").unwrap();
        mem.put(b"key3", b"v3").unwrap();

        assert_eq!(mem.get(b"key1").unwrap(), b"v1");
        assert_eq!(mem.get(b"key2").unwrap(), b"v2");
        assert_eq!(mem.get(b"key3").unwrap(), b"v3");
    }

    #[test]
    fn get_all_returns_latest_first() {
        let mut mem = HashMemtable::with_capacity(512);
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
        let mut mem = HashMemtable::with_capacity(1024);
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
    fn blob_storage_does_not_affect_kv_iteration() {
        let mut mem = HashMemtable::with_capacity(512);
        mem.put(b"k1", b"v1").unwrap();
        let blob_offset = mem.append_blob(b"blob-payload").unwrap();
        mem.put(b"k2", b"v2").unwrap();

        assert_eq!(
            mem.read_blob(blob_offset, "blob-payload".len()).unwrap(),
            b"blob-payload"
        );

        let mut iter = mem.iter();
        iter.seek_to_first().unwrap();
        let mut entries = Vec::new();
        while iter.next().unwrap() {
            let key = iter.key().unwrap().unwrap();
            let value = iter.value().unwrap().unwrap();
            entries.push((key, value));
        }
        assert_eq!(
            entries,
            vec![
                (Bytes::from("k1"), Bytes::from("v1")),
                (Bytes::from("k2"), Bytes::from("v2"))
            ]
        );
    }
}
