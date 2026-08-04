//! Skiplist-indexed memtable implementation with a single preallocated buffer.
//! The buffer stores encoded KV entries (left-to-right) and arena allocations
//! for skiplist nodes (right-to-left).
use bytes::BufMut;

use crate::error::{Error, Result};
use crate::iterator::KvIterator;
use crate::memtable::{Memtable, MemtableReclaimer};
use crate::sst::row_codec::{encode_key_ref_into, encode_value_ref_into};
use crate::r#type::KvValue;
use crate::r#type::{RefKey, RefValue};
use crate::util::unsafe_bytes;
use bytes::Bytes;
use std::cmp::Ordering;
use std::ops::Range;

const NULL_OFFSET: u32 = u32::MAX;
const MAX_HEIGHT: usize = 12;
const NODE_ENTRY_OFFSET_POS: usize = 0;
const NODE_KEY_START_POS: usize = 4;
const NODE_KEY_LEN_POS: usize = 8;
const NODE_HEIGHT_POS: usize = 12;
const NODE_KEY_PREFIX_POS: usize = 16;
const NODE_KEY_PREFIX_SIZE: usize = 16;
const U64_PREFIX_SIZE: usize = std::mem::size_of::<u64>();
const NODE_HEADER_SIZE: usize = 32; // entry_offset(u32) + key_start(u32) + key_len(u32) + height(u8) + reserved(3) + key_prefix([u8; 16])

pub(crate) struct SkiplistMemtableIter<'a> {
    mem: &'a SkiplistMemtable,
    next_node: u32,
    current_key: Option<&'a [u8]>,
    current_value: Option<&'a [u8]>,
}

pub(crate) type MemtableKvIterator<'a> = SkiplistMemtableIter<'a>;

pub(crate) struct SkiplistMemtable {
    buffer: Vec<u8>,
    data_end: usize,
    arena_cursor: usize,
    heads: [u32; MAX_HEIGHT],
    max_height: usize,
    next_ordinal: u32,
    reclaimer: Option<MemtableReclaimer>,
}

pub(crate) struct MemtableValueIter<'a> {
    mem: &'a SkiplistMemtable,
    key: Vec<u8>,
    next_node: u32,
}

impl<'a> SkiplistMemtableIter<'a> {
    fn new(mem: &'a SkiplistMemtable) -> Self {
        Self {
            mem,
            next_node: NULL_OFFSET,
            current_key: None,
            current_value: None,
        }
    }
}

impl<'a> KvIterator<'a> for SkiplistMemtableIter<'a> {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.next_node = self.mem.lower_bound_node(target);
        self.current_key = None;
        self.current_value = None;
        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.next_node = self.mem.heads[0];
        self.current_key = None;
        self.current_value = None;
        Ok(())
    }

    fn next(&mut self) -> Result<bool> {
        if self.next_node == NULL_OFFSET {
            self.current_key = None;
            self.current_value = None;
            return Ok(false);
        }
        let node = self.next_node;
        self.next_node = self.mem.node_next(node, 0).unwrap_or(NULL_OFFSET);
        let Some((key, value)) = self.mem.node_key_value(node) else {
            self.current_key = None;
            self.current_value = None;
            return Ok(false);
        };
        self.current_key = Some(key);
        self.current_value = Some(value);
        Ok(true)
    }

    fn valid(&self) -> bool {
        self.current_key.is_some() && self.current_value.is_some()
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        Ok(self.current_key)
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        Ok(self.current_key.map(unsafe_bytes))
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        Ok(self
            .current_value
            .map(|v| KvValue::Encoded(unsafe_bytes(v))))
    }
}

impl SkiplistMemtable {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self::with_buffer(vec![0u8; capacity])
    }

    pub(crate) fn with_buffer(buffer: Vec<u8>) -> Self {
        let capacity = buffer.len();
        assert!(
            capacity > NODE_HEADER_SIZE,
            "capacity must be greater than 12"
        );
        Self {
            buffer,
            data_end: 0,
            arena_cursor: capacity,
            heads: [NULL_OFFSET; MAX_HEIGHT],
            max_height: 1,
            next_ordinal: 0,
            reclaimer: None,
        }
    }

    pub(crate) fn with_buffer_and_reclaimer(buffer: Vec<u8>, reclaimer: MemtableReclaimer) -> Self {
        let mut memtable = Self::with_buffer(buffer);
        memtable.reclaimer = Some(reclaimer);
        memtable
    }

    fn entry_size(key_len: usize, value_len: usize) -> usize {
        4 + 4 + key_len + value_len
    }

    fn node_size(height: usize) -> usize {
        NODE_HEADER_SIZE + height * 4
    }

    fn has_space(&self, needed: usize) -> Result<()> {
        if self.data_end + needed > self.arena_cursor {
            return Err(Error::MemtableFull {
                needed,
                remaining: self.arena_cursor.saturating_sub(self.data_end),
            });
        }
        Ok(())
    }

    fn alloc_arena(&mut self, len: usize) -> Result<usize> {
        self.has_space(len)?;
        let start = self.arena_cursor - len;
        self.arena_cursor = start;
        Ok(start)
    }

    fn choose_height(ordinal: u32) -> usize {
        // SplitMix64-style mix on insertion ordinal to avoid key/order correlation.
        let mut bits = (ordinal as u64).wrapping_add(0x9e3779b97f4a7c15);
        bits = (bits ^ (bits >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        bits = (bits ^ (bits >> 27)).wrapping_mul(0x94d049bb133111eb);
        bits ^= bits >> 31;
        let mut height = 1usize;
        while height < MAX_HEIGHT && (bits & 0x3) == 0 {
            height += 1;
            bits >>= 2;
        }
        height
    }

    fn read_u32_le(&self, offset: usize) -> Option<u32> {
        let end = offset.checked_add(4)?;
        if end > self.buffer.len() {
            return None;
        }
        Some(u32::from_le_bytes(
            self.buffer[offset..end].try_into().ok()?,
        ))
    }

    fn write_u32_le(&mut self, offset: usize, value: u32) -> Result<()> {
        let end = offset
            .checked_add(4)
            .ok_or_else(|| Error::InvalidState("offset overflow".to_string()))?;
        if end > self.buffer.len() {
            return Err(Error::InvalidState("offset out of range".to_string()));
        }
        self.buffer[offset..end].copy_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn node_offset(node: u32) -> usize {
        node as usize
    }

    fn node_height(&self, node: u32) -> Option<usize> {
        let off = Self::node_offset(node);
        let height_pos = off.checked_add(NODE_HEIGHT_POS)?;
        let header_end = off.checked_add(NODE_HEADER_SIZE)?;
        if header_end > self.buffer.len() {
            return None;
        }
        Some(self.buffer[height_pos] as usize)
    }

    fn node_entry_offset(&self, node: u32) -> Option<usize> {
        let off = Self::node_offset(node);
        Some(self.read_u32_le(off.checked_add(NODE_ENTRY_OFFSET_POS)?)? as usize)
    }

    fn node_key_start(&self, node: u32) -> Option<usize> {
        let off = Self::node_offset(node);
        Some(self.read_u32_le(off.checked_add(NODE_KEY_START_POS)?)? as usize)
    }

    fn node_key_len(&self, node: u32) -> Option<usize> {
        let off = Self::node_offset(node);
        Some(self.read_u32_le(off.checked_add(NODE_KEY_LEN_POS)?)? as usize)
    }

    fn node_key_prefix(&self, node: u32) -> Option<&[u8]> {
        let off = Self::node_offset(node);
        let start = off.checked_add(NODE_KEY_PREFIX_POS)?;
        let end = start.checked_add(NODE_KEY_PREFIX_SIZE)?;
        if end > self.buffer.len() {
            return None;
        }
        Some(&self.buffer[start..end])
    }

    fn node_next(&self, node: u32, level: usize) -> Option<u32> {
        let height = self.node_height(node)?;
        if level >= height {
            return None;
        }
        let pos = Self::node_offset(node)
            .checked_add(NODE_HEADER_SIZE)?
            .checked_add(level * 4)?;
        self.read_u32_le(pos)
    }

    pub(crate) fn first_node_offset(&self) -> Option<u32> {
        let node = self.heads[0];
        if node == NULL_OFFSET {
            None
        } else {
            Some(node)
        }
    }

    pub(crate) fn lower_bound_node_offset(&self, target: &[u8]) -> Option<u32> {
        let node = self.lower_bound_node(target);
        if node == NULL_OFFSET {
            None
        } else {
            Some(node)
        }
    }

    pub(crate) fn next_node_offset(&self, node: u32) -> Option<u32> {
        let next = self.node_next(node, 0)?;
        if next == NULL_OFFSET {
            None
        } else {
            Some(next)
        }
    }

    pub(crate) fn node_entry(&self, node: u32) -> Option<(&[u8], &[u8])> {
        self.node_key_value(node)
    }

    /// Returns the contiguous key-value payload range and key length for a node.
    pub(crate) fn node_entry_range(&self, node: u32) -> Option<(Range<usize>, usize)> {
        let entry_offset = self.node_entry_offset(node)?;
        let key_start = self.node_key_start(node)?;
        let key_len = self.node_key_len(node)?;
        if entry_offset.checked_add(8)? != key_start {
            return None;
        }
        let value_len = self.read_u32_le(entry_offset.checked_add(4)?)? as usize;
        let value_end = key_start.checked_add(key_len)?.checked_add(value_len)?;
        if value_end > self.data_end {
            return None;
        }
        Some((key_start..value_end, key_len))
    }

    pub(crate) fn entry_bytes(&self, range: &Range<usize>) -> Option<&[u8]> {
        if range.start > range.end || range.end > self.data_end {
            return None;
        }
        Some(&self.buffer[range.clone()])
    }

    fn set_node_next(&mut self, node: u32, level: usize, next: u32) -> Result<()> {
        let height = self
            .node_height(node)
            .ok_or_else(|| Error::InvalidState("invalid node".to_string()))?;
        if level >= height {
            return Err(Error::InvalidState(format!(
                "invalid node level {} for height {}",
                level, height
            )));
        }
        let pos = Self::node_offset(node)
            .checked_add(NODE_HEADER_SIZE)
            .and_then(|v| v.checked_add(level * 4))
            .ok_or_else(|| Error::InvalidState("offset overflow".to_string()))?;
        self.write_u32_le(pos, next)
    }

    fn level_next(&self, node: u32, level: usize) -> u32 {
        if node == NULL_OFFSET {
            self.heads[level]
        } else {
            self.node_next(node, level).unwrap_or(NULL_OFFSET)
        }
    }

    fn node_key(&self, node: u32) -> Option<&[u8]> {
        let key_start = self.node_key_start(node)?;
        let key_len = self.node_key_len(node)?;
        let key_end = key_start.checked_add(key_len)?;
        if key_end > self.data_end {
            return None;
        }
        Some(&self.buffer[key_start..key_end])
    }

    fn node_key_value(&self, node: u32) -> Option<(&[u8], &[u8])> {
        let entry_offset = self.node_entry_offset(node)?;
        let key = self.node_key(node)?;
        let value_len = self.read_u32_le(entry_offset.checked_add(4)?)? as usize;
        let value_start = self
            .node_key_start(node)?
            .checked_add(self.node_key_len(node)?)?;
        let value_end = value_start.checked_add(value_len)?;
        if value_end > self.data_end {
            return None;
        }
        Some((key, &self.buffer[value_start..value_end]))
    }

    fn compare_u64_prefixes(left: &[u8], right: &[u8]) -> Option<Ordering> {
        debug_assert_eq!(left.len(), right.len());
        debug_assert!(left.len() == U64_PREFIX_SIZE || left.len() == NODE_KEY_PREFIX_SIZE);

        let left_first = u64::from_be_bytes(left[..U64_PREFIX_SIZE].try_into().ok()?);
        let right_first = u64::from_be_bytes(right[..U64_PREFIX_SIZE].try_into().ok()?);
        let first_cmp = left_first.cmp(&right_first);
        if first_cmp != Ordering::Equal || left.len() == U64_PREFIX_SIZE {
            return Some(first_cmp);
        }

        let left_second = u64::from_be_bytes(
            left[U64_PREFIX_SIZE..NODE_KEY_PREFIX_SIZE]
                .try_into()
                .ok()?,
        );
        let right_second = u64::from_be_bytes(
            right[U64_PREFIX_SIZE..NODE_KEY_PREFIX_SIZE]
                .try_into()
                .ok()?,
        );
        Some(left_second.cmp(&right_second))
    }

    fn compare_node_key(&self, node: u32, key: &[u8]) -> Option<Ordering> {
        let node_key_len = self.node_key_len(node)?;
        let prefix_len = NODE_KEY_PREFIX_SIZE.min(node_key_len).min(key.len());
        let node_prefix = self.node_key_prefix(node)?;
        let prefix_cmp = if prefix_len == U64_PREFIX_SIZE || prefix_len == NODE_KEY_PREFIX_SIZE {
            Self::compare_u64_prefixes(&node_prefix[..prefix_len], &key[..prefix_len])?
        } else {
            node_prefix[..prefix_len].cmp(&key[..prefix_len])
        };
        if prefix_cmp != Ordering::Equal {
            return Some(prefix_cmp);
        }
        if node_key_len <= prefix_len && key.len() <= prefix_len {
            return Some(node_key_len.cmp(&key.len()));
        }
        let node_key = self.node_key(node)?;
        Some(node_key[prefix_len..].cmp(&key[prefix_len..]))
    }

    fn find_greater_or_equal_node(&self, key: &[u8]) -> u32 {
        let mut current = NULL_OFFSET;
        let mut last_bigger = NULL_OFFSET;
        for level in (0..self.max_height).rev() {
            loop {
                let next = self.level_next(current, level);
                if next == NULL_OFFSET || next == last_bigger {
                    break;
                }
                match self.compare_node_key(next, key) {
                    Some(Ordering::Less) => current = next,
                    Some(Ordering::Equal | Ordering::Greater) | None => {
                        last_bigger = next;
                        break;
                    }
                }
            }
        }
        if last_bigger != NULL_OFFSET {
            last_bigger
        } else {
            self.level_next(current, 0)
        }
    }

    fn find_predecessors_for_key(&self, key: &[u8]) -> [u32; MAX_HEIGHT] {
        let mut update = [NULL_OFFSET; MAX_HEIGHT];
        let mut current = NULL_OFFSET;
        let mut last_not_after = NULL_OFFSET;
        for level in (0..self.max_height).rev() {
            loop {
                let next = self.level_next(current, level);
                if next == NULL_OFFSET || next == last_not_after {
                    break;
                }
                match self.compare_node_key(next, key) {
                    Some(Ordering::Less) => current = next,
                    Some(Ordering::Equal | Ordering::Greater) | None => {
                        last_not_after = next;
                        break;
                    }
                }
            }
            update[level] = current;
        }
        update
    }

    fn lower_bound_node(&self, key: &[u8]) -> u32 {
        self.find_greater_or_equal_node(key)
    }

    fn alloc_node(
        &mut self,
        entry_offset: usize,
        key_start: usize,
        key_len: usize,
        key: &[u8],
        height: usize,
    ) -> Result<u32> {
        let node_size = Self::node_size(height);
        let node_start = self.alloc_arena(node_size)?;
        let node = u32::try_from(node_start)
            .map_err(|_| Error::InvalidState("memtable node offset exceeds u32".to_string()))?;
        let mut slice = &mut self.buffer[node_start..node_start + node_size];
        slice.put_u32_le(
            u32::try_from(entry_offset)
                .map_err(|_| Error::InvalidState("entry offset exceeds u32".to_string()))?,
        );
        slice.put_u32_le(
            u32::try_from(key_start)
                .map_err(|_| Error::InvalidState("key start exceeds u32".to_string()))?,
        );
        slice.put_u32_le(
            u32::try_from(key_len)
                .map_err(|_| Error::InvalidState("key length exceeds u32".to_string()))?,
        );
        slice.put_u8(height as u8);
        slice.put_slice(&[0u8; 3]);
        let mut key_prefix = [0u8; NODE_KEY_PREFIX_SIZE];
        let prefix_len = key.len().min(NODE_KEY_PREFIX_SIZE);
        key_prefix[..prefix_len].copy_from_slice(&key[..prefix_len]);
        slice.put_slice(&key_prefix);
        for _ in 0..height {
            slice.put_u32_le(NULL_OFFSET);
        }
        Ok(node)
    }

    fn link_node(&mut self, node: u32, height: usize, update: &[u32; MAX_HEIGHT]) -> Result<()> {
        for (level, pred) in update.iter().copied().enumerate().take(height) {
            let next = if pred == NULL_OFFSET {
                self.heads[level]
            } else {
                self.node_next(pred, level)
                    .ok_or_else(|| Error::InvalidState("invalid predecessor node".to_string()))?
            };
            self.set_node_next(node, level, next)?;
            if pred == NULL_OFFSET {
                self.heads[level] = node;
            } else {
                self.set_node_next(pred, level, node)?;
            }
        }
        self.max_height = self.max_height.max(height);
        Ok(())
    }

    fn write_data(&mut self, key: &[u8], value: &[u8]) -> usize {
        let start = self.data_end;
        let end = start + Self::entry_size(key.len(), value.len());
        let mut slice = &mut self.buffer[start..end];
        slice.put_u32_le(key.len() as u32);
        slice.put_u32_le(value.len() as u32);
        slice.put_slice(key);
        slice.put_slice(value);
        self.data_end = end;
        start
    }
}

impl Memtable for SkiplistMemtable {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let ordinal = self.next_ordinal;
        let height = Self::choose_height(ordinal);
        let data_len = Self::entry_size(key.len(), value.len());
        let node_len = Self::node_size(height);
        self.has_space(data_len + node_len)?;
        let update = self.find_predecessors_for_key(key);
        let entry_offset = self.write_data(key, value);
        let key_start = entry_offset
            .checked_add(8)
            .ok_or_else(|| Error::InvalidState("key start overflow".to_string()))?;
        let node = self.alloc_node(entry_offset, key_start, key.len(), key, height)?;
        self.link_node(node, height, &update)?;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| Error::InvalidState("memtable ordinal overflow".to_string()))?;
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
        let mut encoded_key = vec![0u8; key_len];
        let mut key_slice = encoded_key.as_mut_slice();
        encode_key_ref_into(key, &mut key_slice);
        let ordinal = self.next_ordinal;
        let height = Self::choose_height(ordinal);
        let data_len = Self::entry_size(key_len, value_len);
        let node_len = Self::node_size(height);
        self.has_space(data_len + node_len)?;
        let update = self.find_predecessors_for_key(&encoded_key);
        let start = self.data_end;
        let end = start + data_len;
        let mut slice = &mut self.buffer[start..end];
        slice.put_u32_le(key_len as u32);
        slice.put_u32_le(value_len as u32);
        slice.put_slice(&encoded_key);
        encode_value_ref_into(value, num_columns, &mut slice);
        self.data_end = end;
        let key_start = start
            .checked_add(8)
            .ok_or_else(|| Error::InvalidState("key start overflow".to_string()))?;
        let node = self.alloc_node(start, key_start, key_len, &encoded_key, height)?;
        self.link_node(node, height, &update)?;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| Error::InvalidState("memtable ordinal overflow".to_string()))?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let node = self.lower_bound_node(key);
        if node == NULL_OFFSET {
            return None;
        }
        let (entry_key, value) = self.node_key_value(node)?;
        if entry_key == key { Some(value) } else { None }
    }

    fn get_all(&self, key: &[u8]) -> Self::ValueIter<'_> {
        MemtableValueIter {
            mem: self,
            key: key.to_vec(),
            next_node: self.lower_bound_node(key),
        }
    }

    fn remaining_capacity(&self) -> usize {
        self.arena_cursor.saturating_sub(self.data_end)
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
                "invalid skiplist memtable data range [{}, {}) with end {}",
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
        let node = self.lower_bound_node(&encoded_key);
        if node == NULL_OFFSET || self.node_key(node) != Some(encoded_key.as_slice()) {
            return Ok(false);
        }
        let Some(entry_offset) = self.node_entry_offset(node) else {
            return Ok(false);
        };
        if entry_offset < sealed_data_end {
            return Ok(false);
        }
        let Some(stored_key_len) = self.node_key_len(node) else {
            return Ok(false);
        };
        let Some(stored_value_len) = self.read_u32_le(entry_offset + 4) else {
            return Ok(false);
        };
        let stored_value_len = stored_value_len as usize;
        let value_len = value.encoded_len(num_columns);
        if stored_key_len != key_len || stored_value_len != value_len {
            return Ok(false);
        }
        let value_start = entry_offset + 8 + stored_key_len;
        let mut value_buf = &mut self.buffer[value_start..value_start + stored_value_len];
        encode_value_ref_into(value, num_columns, &mut value_buf);
        Ok(true)
    }

    fn iter(&self) -> Self::KvIter<'_> {
        MemtableKvIterator::new(self)
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
        while self.next_node != NULL_OFFSET {
            let node = self.next_node;
            self.next_node = self.mem.node_next(node, 0).unwrap_or(NULL_OFFSET);
            let (entry_key, value) = self.mem.node_key_value(node)?;
            if entry_key == self.key {
                return Some(value);
            }
            if entry_key > self.key.as_slice() {
                self.next_node = NULL_OFFSET;
                return None;
            }
        }
        None
    }
}

impl Drop for SkiplistMemtable {
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
        let mut mem = SkiplistMemtable::with_capacity(1024);
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

    fn assert_compare_matches_full_key_ordering(keys: &[&[u8]]) {
        let mut mem = SkiplistMemtable::with_capacity(8192);
        for &key in keys {
            mem.put(key, b"value").unwrap();
        }

        for &stored_key in keys {
            let node = mem.lower_bound_node(stored_key);
            assert_ne!(node, NULL_OFFSET);
            assert_eq!(mem.node_key(node), Some(stored_key));
            for &target in keys {
                assert_eq!(
                    mem.compare_node_key(node, target),
                    Some(stored_key.cmp(target)),
                    "stored {:?}, target {:?}",
                    stored_key,
                    target
                );
            }
        }
    }

    #[test]
    fn put_and_get() {
        let mut mem = SkiplistMemtable::with_capacity(1024);
        mem.put(b"key1", b"value1").unwrap();
        mem.put(b"key2", b"value2").unwrap();
        assert_eq!(mem.get(b"key1").unwrap(), b"value1");
        assert_eq!(mem.get(b"key2").unwrap(), b"value2");
        assert!(mem.get(b"missing").is_none());
    }

    #[test]
    fn get_all_returns_latest_first() {
        let mut mem = SkiplistMemtable::with_capacity(2048);
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
        let mut mem = SkiplistMemtable::with_capacity(4096);
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

    #[test]
    fn capacity_enforced() {
        let mut mem = SkiplistMemtable::with_capacity(64);
        mem.put(b"k1", b"v1").unwrap();
        let err = mem.put(b"k2", b"value_too_long").unwrap_err();
        match err {
            Error::MemtableFull { .. } => {}
            _ => panic!("unexpected error type"),
        }
    }

    #[test]
    fn predecessor_search_matches_lower_bound_for_missing_and_present_keys() {
        let mut mem = SkiplistMemtable::with_capacity(8192);
        for (key, value) in [
            (b"aa".as_slice(), b"v1".as_slice()),
            (b"ab", b"v2"),
            (b"ac", b"v3"),
            (b"b", b"v4"),
            (b"ba", b"v5"),
            (b"c", b"v6"),
        ] {
            mem.put(key, value).unwrap();
        }

        for (target, expected_pred, expected_lower_bound) in [
            (b"a".as_slice(), None, Some(b"aa".as_slice())),
            (b"aa", None, Some(b"aa")),
            (b"aad", Some(b"aa".as_slice()), Some(b"ab".as_slice())),
            (b"ab", Some(b"aa".as_slice()), Some(b"ab".as_slice())),
            (b"ad", Some(b"ac".as_slice()), Some(b"b".as_slice())),
            (b"bb", Some(b"ba".as_slice()), Some(b"c".as_slice())),
            (b"d", Some(b"c".as_slice()), None),
        ] {
            let lower_bound = mem.lower_bound_node(target);
            let lower_bound_key = if lower_bound == NULL_OFFSET {
                None
            } else {
                Some(mem.node_entry(lower_bound).unwrap().0)
            };
            assert_eq!(lower_bound_key, expected_lower_bound, "target {:?}", target);

            let update = mem.find_predecessors_for_key(target);
            let predecessor = update[0];
            let predecessor_key = if predecessor == NULL_OFFSET {
                None
            } else {
                Some(mem.node_entry(predecessor).unwrap().0)
            };
            assert_eq!(predecessor_key, expected_pred, "target {:?}", target);

            let derived_lower_bound = if predecessor == NULL_OFFSET {
                mem.heads[0]
            } else {
                mem.node_next(predecessor, 0).unwrap_or(NULL_OFFSET)
            };
            assert_eq!(derived_lower_bound, lower_bound, "target {:?}", target);
        }
    }

    #[test]
    fn cached_prefix_comparison_matches_full_key_ordering() {
        let ascii_keys: [&[u8]; 9] = [
            b"1234567".as_slice(),
            b"12345678",
            b"123456789",
            b"shared08-a",
            b"shared08-z",
            b"0123456789abcde",
            b"0123456789abcdef",
            b"0123456789abcdef-a",
            b"0123456789abcdef-z",
        ];
        let eight_byte_keys = [
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7f],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff],
            [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            [0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        ];
        let key = |first, second| {
            let mut key = [0u8; NODE_KEY_PREFIX_SIZE];
            key[0] = first;
            key[U64_PREFIX_SIZE] = second;
            key
        };
        let sixteen_byte_keys = [
            key(0x00, 0x00),
            key(0x7f, 0x00),
            key(0x80, 0x00),
            key(0xff, 0x00),
            key(0x40, 0x00),
            key(0x40, 0x7f),
            key(0x40, 0x80),
            key(0x40, 0xff),
        ];
        let mut keys = ascii_keys.to_vec();
        keys.extend(eight_byte_keys.iter().map(|key| key.as_slice()));
        keys.extend(sixteen_byte_keys.iter().map(|key| key.as_slice()));
        assert_compare_matches_full_key_ordering(&keys);
    }

    #[test]
    fn prefix_comparison_preserves_lower_bound_put_get_and_iteration_order() {
        let entries = [
            (b"0123456789abcdef-z".as_slice(), b"z1".as_slice()),
            (b"123456789", b"nine"),
            (b"shared08-z", b"z"),
            (b"12345678", b"eight"),
            (b"0123456789abcdef", b"sixteen"),
            (b"shared08-a", b"a"),
            (b"1234567", b"seven"),
            (b"0123456789abcdef-a", b"a1"),
            (b"12345678", b"latest-eight"),
        ];
        let mut mem = SkiplistMemtable::with_capacity(8192);
        for (key, value) in entries {
            mem.put(key, value).unwrap();
        }

        assert_eq!(mem.get(b"12345678"), Some(b"latest-eight".as_slice()));
        assert_eq!(mem.get(b"123456789"), Some(b"nine".as_slice()));
        assert_eq!(mem.get(b"shared08-a"), Some(b"a".as_slice()));

        for (target, expected) in [
            (b"1234567".as_slice(), Some(b"1234567".as_slice())),
            (b"1234567\0", Some(b"12345678".as_slice())),
            (b"12345678\x01", Some(b"123456789".as_slice())),
            (b"shared08-m", Some(b"shared08-z".as_slice())),
            (
                b"0123456789abcdef-y",
                Some(b"0123456789abcdef-z".as_slice()),
            ),
            (b"zzzz", None),
        ] {
            let node = mem.lower_bound_node(target);
            let found = if node == NULL_OFFSET {
                None
            } else {
                mem.node_key(node)
            };
            assert_eq!(found, expected, "target {:?}", target);
        }

        let mut iter = mem.iter();
        iter.seek_to_first().unwrap();
        let mut keys = Vec::new();
        while iter.next().unwrap() {
            keys.push(iter.take_key().unwrap().unwrap());
        }
        assert_eq!(
            keys,
            vec![
                Bytes::from_static(b"0123456789abcdef"),
                Bytes::from_static(b"0123456789abcdef-a"),
                Bytes::from_static(b"0123456789abcdef-z"),
                Bytes::from_static(b"1234567"),
                Bytes::from_static(b"12345678"),
                Bytes::from_static(b"12345678"),
                Bytes::from_static(b"123456789"),
                Bytes::from_static(b"shared08-a"),
                Bytes::from_static(b"shared08-z"),
            ]
        );
    }

    #[test]
    fn large_scale_put_get_and_iteration_order() {
        const ENTRY_COUNT: usize = 20_000;
        let mut mem = SkiplistMemtable::with_capacity(16 * 1024 * 1024);
        for i in 0..ENTRY_COUNT {
            let key_id = (i * 11939 + 7) % ENTRY_COUNT;
            let key = format!("k{:08}", key_id);
            let value = format!("v{:08}", key_id);
            mem.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        for i in 0..ENTRY_COUNT {
            let key = format!("k{:08}", i);
            let expected = format!("v{:08}", i);
            assert_eq!(mem.get(key.as_bytes()).unwrap(), expected.as_bytes());
        }

        let mut iter = mem.iter();
        iter.seek_to_first().unwrap();
        let mut last_key = Vec::<u8>::new();
        let mut seen = 0usize;
        while iter.next().unwrap() {
            let key = iter.take_key().unwrap().unwrap();
            if !last_key.is_empty() {
                assert!(last_key.as_slice() <= key.as_ref());
            }
            last_key.clear();
            last_key.extend_from_slice(key.as_ref());
            seen += 1;
        }
        assert_eq!(seen, ENTRY_COUNT);
    }

    #[test]
    fn large_scale_overwrite_keeps_latest_and_full_history() {
        const KEY_COUNT: usize = 2_000;
        const VERSIONS_PER_KEY: usize = 10;
        let mut mem = SkiplistMemtable::with_capacity(16 * 1024 * 1024);

        for version in 0..VERSIONS_PER_KEY {
            for key_id in 0..KEY_COUNT {
                let key = format!("k{:05}", key_id);
                let value = format!("v{:02}-{:05}", version, key_id);
                mem.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        }

        for key_id in [0usize, 17, 311, 1023, KEY_COUNT - 1] {
            let key = format!("k{:05}", key_id);
            let latest = format!("v{:02}-{:05}", VERSIONS_PER_KEY - 1, key_id);
            assert_eq!(mem.get(key.as_bytes()).unwrap(), latest.as_bytes());

            let collected = mem
                .get_all(key.as_bytes())
                .map(|v| String::from_utf8(v.to_vec()).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(collected.len(), VERSIONS_PER_KEY);
            for (idx, value) in collected.iter().enumerate() {
                let expected_version = VERSIONS_PER_KEY - 1 - idx;
                assert_eq!(*value, format!("v{:02}-{:05}", expected_version, key_id));
            }
        }
    }
}
