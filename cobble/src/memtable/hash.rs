//! Hash-indexed memtable implementation.
//! This memtable organizes entries in a single buffer with a hash index for efficient lookups.
use bytes::{Buf, BufMut};

use crate::error::{Error, Result};
use crate::memtable::iter::OrderedMemtableKvIterator;
use crate::memtable::{Memtable, MemtableReclaimer};
use crate::sst::row_codec::{encode_key_ref_into, encode_value_ref_into};
use crate::r#type::{RefKey, RefValue};

pub(crate) type MemtableKvIterator<'a> = OrderedMemtableKvIterator<'a>;

/// Hash-indexed memtable storing entries and index nodes in one buffer.
///
/// Layout:
/// - `[0, data_end)`: encoded KV entries, appended left-to-right.
/// - `[index_cursor, bucket_base)`: scratch area grown right-to-left by hash index nodes.
/// - `[bucket_base, capacity)`: fixed bucket head table (`u32` offsets).
pub(crate) struct HashMemtable {
    buffer: Vec<u8>,
    data_end: usize,
    index_cursor: usize,
    bucket_base: usize,
    bucket_count: usize,
    entry_count: usize,
    reclaimer: Option<MemtableReclaimer>,
}

pub(crate) struct MemtableValueIter<'a> {
    mem: &'a HashMemtable,
    key: Vec<u8>,
    hash: u64,
    next_node: u32,
}

impl HashMemtable {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        let bucket_count = Self::default_bucket_count(capacity);
        Self::with_capacity_and_buckets(capacity, bucket_count)
    }

    pub(crate) fn with_buffer_and_bucket_count_and_reclaimer(
        buffer: Vec<u8>,
        bucket_count: usize,
        reclaimer: MemtableReclaimer,
    ) -> Self {
        let mut memtable = Self::with_buffer_and_bucket_count(buffer, bucket_count);
        memtable.reclaimer = Some(reclaimer);
        memtable
    }

    fn with_buffer_and_bucket_count(mut buffer: Vec<u8>, bucket_count: usize) -> Self {
        let capacity = buffer.len();
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
            entry_count: 0,
            reclaimer: None,
        }
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
            entry_count: 0,
            reclaimer: None,
        }
    }

    pub(crate) fn default_bucket_count(capacity: usize) -> usize {
        // Keep the average chain short as the memtable grows. A fixed upper bound makes point
        // lookups effectively linear once a large memtable contains substantially more entries
        // than buckets. The bucket table remains bounded to roughly 3% of the configured capacity.
        Self::clamp_bucket_count(capacity, (capacity / 128).max(4))
    }

    pub(crate) fn clamp_bucket_count(capacity: usize, bucket_count: usize) -> usize {
        let (min_bucket_count, max_bucket_count) = Self::bucket_count_bounds(capacity);
        bucket_count.clamp(min_bucket_count, max_bucket_count)
    }

    fn bucket_count_bounds(capacity: usize) -> (usize, usize) {
        assert!(capacity > 4, "capacity must leave room for a bucket table");
        (1, (capacity - 1) / 4)
    }

    pub(crate) fn bucket_count(&self) -> usize {
        self.bucket_count
    }

    pub(crate) fn entry_count(&self) -> usize {
        self.entry_count
    }

    pub(crate) fn used_entry_bytes(&self) -> usize {
        self.data_end + (self.bucket_base - self.index_cursor)
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

    fn latest_entry_offset(&self, key: &[u8]) -> Option<usize> {
        let hash = Self::hash_key(key);
        let bucket = self.bucket_index_from_hash(hash);
        let mut node_off = self.bucket_head(bucket);
        while node_off != u32::MAX {
            let start = node_off as usize;
            if start + Self::index_entry_size() > self.buffer.len() {
                return None;
            }
            let mut node_slice = &self.buffer[start..start + Self::index_entry_size()];
            let node_hash = node_slice.get_u64();
            let entry_offset = node_slice.get_u32() as usize;
            let next = node_slice.get_u32();
            if node_hash == hash && entry_offset + 8 <= self.data_end {
                let mut header = &self.buffer[entry_offset..entry_offset + 8];
                let key_len = header.get_u32() as usize;
                let value_len = header.get_u32() as usize;
                let entry_end = entry_offset.checked_add(Self::entry_size(key_len, value_len))?;
                if entry_end <= self.data_end
                    && self.buffer[entry_offset + 8..entry_offset + 8 + key_len] == *key
                {
                    return Some(entry_offset);
                }
            }
            node_off = next;
        }
        None
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

    fn install_index(&mut self, bucket: usize, hash: u64, key_offset: u32) {
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
        self.set_bucket_head(bucket, start as u32);
        self.entry_count += 1;
    }

    fn bucket_index_from_hash(&self, hash: u64) -> usize {
        (hash as usize) % self.bucket_count
    }

    pub(crate) fn iter_with_bounds(
        &self,
        start_inclusive: Option<&[u8]>,
        end_exclusive: Option<&[u8]>,
    ) -> MemtableKvIterator<'_> {
        let mut entries: Vec<(&[u8], &[u8], usize)> = Vec::new();
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
            let key = &slice[..key_len];
            if let Some(start) = start_inclusive
                && key < start
            {
                offset += Self::entry_size(key_len, value_len);
                continue;
            }
            if let Some(end) = end_exclusive
                && key >= end
            {
                offset += Self::entry_size(key_len, value_len);
                continue;
            }
            entries.push((key, &slice[key_len..key_len + value_len], offset));
            offset += Self::entry_size(key_len, value_len);
        }
        MemtableKvIterator::new(entries)
    }
}

impl Memtable for HashMemtable {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let data_len = Self::entry_size(key.len(), value.len());
        self.has_space(data_len)?;
        let data_offset = self.write_data(key, value);
        let hash = Self::hash_key(key);
        let bucket = self.bucket_index_from_hash(hash);
        self.install_index(bucket, hash, data_offset as u32);
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
        let data_len = Self::entry_size(key_len, value_len);
        self.has_space(data_len)?;
        let (data_offset, key_offset) =
            self.write_data_ref(key, value, num_columns, key_len, value_len);
        let hash = Self::hash_key(&self.buffer[key_offset..key_offset + key_len]);
        let bucket = self.bucket_index_from_hash(hash);
        self.install_index(bucket, hash, data_offset as u32);
        Ok(())
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
        let hash = Self::hash_key(key);
        let hash_bucket = self.bucket_index_from_hash(hash);
        let head = self.bucket_head(hash_bucket);
        MemtableValueIter {
            mem: self,
            key: key.to_vec(),
            hash,
            next_node: head,
        }
    }

    fn remaining_capacity(&self) -> usize {
        self.index_cursor.saturating_sub(self.data_end)
    }

    fn is_empty(&self) -> bool {
        self.data_end == 0
    }

    fn data_offset(&self) -> usize {
        self.data_end
    }

    fn write_data_range(
        &self,
        start_offset: usize,
        end_offset: usize,
        writer: &mut dyn crate::file::SequentialWriteFile,
    ) -> Result<usize> {
        if start_offset > end_offset || end_offset > self.data_end {
            return Err(Error::InvalidState(format!(
                "invalid hash memtable data range [{}, {}) with end {}",
                start_offset, end_offset, self.data_end
            )));
        }
        let bytes = &self.buffer[start_offset..end_offset];
        writer.write(bytes)?;
        Ok(bytes.len())
    }

    fn try_replace_latest_ref(
        &mut self,
        key: &RefKey<'_>,
        value: &RefValue<'_>,
        num_columns: usize,
        sealed_data_end: usize,
    ) -> Result<bool> {
        let key_len = key.encoded_len();
        let mut encoded_key = vec![0; key_len];
        encode_key_ref_into(key, &mut encoded_key.as_mut_slice());
        let Some(entry_offset) = self.latest_entry_offset(&encoded_key) else {
            return Ok(false);
        };
        if entry_offset < sealed_data_end {
            return Ok(false);
        }
        let mut header = &self.buffer[entry_offset..entry_offset + 8];
        let stored_key_len = header.get_u32() as usize;
        let stored_value_len = header.get_u32() as usize;
        let value_len = value.encoded_len(num_columns);
        if stored_key_len != key_len || stored_value_len != value_len {
            return Ok(false);
        }
        let value_start = entry_offset + 8 + stored_key_len;
        let mut value_buf = &mut self.buffer[value_start..value_start + stored_value_len];
        encode_value_ref_into(value, num_columns, &mut value_buf);
        Ok(true)
    }

    /// Returns an iterator over all key-value pairs ordered by key bytes ascending.
    /// For duplicate keys, values are yielded in reverse insertion order (latest first).
    fn iter(&self) -> MemtableKvIterator<'_> {
        self.iter_with_bounds(None, None)
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
            if h == self.hash && key_off + 8 <= self.mem.data_end {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iterator::KvIterator;
    use crate::r#type::{RefColumn, ValueType};

    #[test]
    fn inplace_replace_latest_requires_equal_length_and_unsealed_entry() {
        let mut mem = HashMemtable::with_capacity(1024);
        let key = RefKey::new(0, b"key");
        let old = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"old"))]);
        let new = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"new"))]);
        let shorter = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"x"))]);
        mem.put_ref(&key, &old, 1).unwrap();
        let offset = mem.data_offset();
        assert!(mem.try_replace_latest_ref(&key, &new, 1, 0).unwrap());
        assert_eq!(mem.data_offset(), offset);
        let mut encoded_key = vec![0; key.encoded_len()];
        encode_key_ref_into(&key, &mut encoded_key.as_mut_slice());
        let mut expected_value = vec![0; new.encoded_len(1)];
        encode_value_ref_into(&new, 1, &mut expected_value.as_mut_slice());
        assert_eq!(mem.get(&encoded_key), Some(expected_value.as_slice()));
        assert!(!mem.try_replace_latest_ref(&key, &shorter, 1, 0).unwrap());
        assert_eq!(mem.data_offset(), offset);
        assert!(!mem.try_replace_latest_ref(&key, &new, 1, offset).unwrap());
    }

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
    fn entry_stats_count_successful_writes_including_duplicate_keys() {
        let mut mem = HashMemtable::with_capacity_and_buckets(128, 1);
        assert_eq!(mem.entry_count(), 0);
        assert_eq!(mem.used_entry_bytes(), 0);

        mem.put(b"key", b"value").unwrap();
        let entry_bytes = HashMemtable::entry_size(3, 5) + HashMemtable::index_entry_size();
        assert_eq!(mem.entry_count(), 1);
        assert_eq!(mem.used_entry_bytes(), entry_bytes);

        mem.put(b"key", b"value").unwrap();
        assert_eq!(mem.entry_count(), 2);
        assert_eq!(mem.used_entry_bytes(), entry_bytes * 2);

        let mut full_mem = HashMemtable::with_capacity_and_buckets(64, 1);
        full_mem.put(b"k", b"v").unwrap();
        let entry_count = full_mem.entry_count();
        let used_entry_bytes = full_mem.used_entry_bytes();
        assert!(matches!(
            full_mem.put(b"k", &[b'x'; 32]),
            Err(Error::MemtableFull { .. })
        ));
        assert_eq!(full_mem.entry_count(), entry_count);
        assert_eq!(full_mem.used_entry_bytes(), used_entry_bytes);
    }

    #[test]
    fn bucket_count_scales_with_memtable_capacity() {
        assert_eq!(
            HashMemtable::default_bucket_count(128 * 1024 * 1024),
            1024 * 1024
        );
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
            let k = iter.take_key().unwrap().unwrap();
            let v = iter.take_value().unwrap().unwrap().unwrap_encoded();
            collected.push((k, v));
        }
        let expected: Vec<(&[u8], &[u8])> =
            vec![(b"a", b"x2"), (b"a", b"x1"), (b"b", b"v1"), (b"c", b"z1")];
        assert_eq!(collected.len(), expected.len());
        for (got, exp) in collected.iter().zip(expected.iter()) {
            assert_eq!(got.0.as_ref(), exp.0);
            assert_eq!(got.1.as_ref(), exp.1);
        }
    }
}
