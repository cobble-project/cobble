//! A simple in-memory memtable implementation backed by bytes-owned KV pairs.
use crate::error::{Error, Result};
use crate::memtable::iter::OrderedMemtableKvIterator;
use crate::memtable::{Memtable, MemtableReclaimer};
use crate::sst::row_codec::{encode_key_ref_into, encode_value_ref_into};
use crate::r#type::{RefKey, RefValue};
use bytes::Bytes;

pub(crate) type MemtableKvIterator<'a> = OrderedMemtableKvIterator<'a>;

pub(crate) struct VecMemtable {
    entries: Vec<(Bytes, Bytes)>,
    entry_data_offsets: Vec<usize>,
    data_end: usize,
    capacity: usize,
    used_bytes: usize,
    reclaimer: Option<MemtableReclaimer>,
}

pub(crate) struct MemtableValueIter<'a> {
    mem: &'a VecMemtable,
    key: Vec<u8>,
    next_idx: usize,
}

/// Appends one entry in the canonical Vec memtable stream format.
///
/// The WAL uses this same append-only representation so replayed entries follow the identical
/// key/value encoding as active-memtable snapshot data.
pub(crate) fn encode_vec_entry_stream_entry(output: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    output.extend_from_slice(&(key.len() as u32).to_le_bytes());
    output.extend_from_slice(&(value.len() as u32).to_le_bytes());
    output.extend_from_slice(key);
    output.extend_from_slice(value);
}

/// Appends a reference row without allocating intermediate encoded key/value buffers.
pub(crate) fn encode_vec_entry_stream_ref(
    output: &mut Vec<u8>,
    key: &RefKey<'_>,
    value: &RefValue<'_>,
    num_columns: usize,
) {
    let key_len = key.encoded_len();
    let value_len = value.encoded_len(num_columns);
    let entry_start = output.len();
    output.resize(entry_start + 8 + key_len + value_len, 0);
    output[entry_start..entry_start + 4].copy_from_slice(&(key_len as u32).to_le_bytes());
    output[entry_start + 4..entry_start + 8].copy_from_slice(&(value_len as u32).to_le_bytes());
    let (_, encoded) = output.split_at_mut(entry_start + 8);
    let (key_output, value_output) = encoded.split_at_mut(key_len);
    let mut key_output = key_output;
    let mut value_output = value_output;
    encode_key_ref_into(key, &mut key_output);
    encode_value_ref_into(value, num_columns, &mut value_output);
}

/// Decodes entries written by [`encode_vec_entry_stream_entry`].
pub(crate) fn decode_vec_entry_stream(bytes: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
    let mut entries = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let header_end = offset
            .checked_add(8)
            .ok_or_else(|| Error::IoError("vec entry header overflow".to_string()))?;
        if header_end > bytes.len() {
            return Err(Error::InvalidState(
                "truncated vec memtable entry header".to_string(),
            ));
        }
        let key_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        let value_len =
            u32::from_le_bytes(bytes[offset + 4..header_end].try_into().unwrap()) as usize;
        let key_start = header_end;
        let value_start = key_start
            .checked_add(key_len)
            .ok_or_else(|| Error::IoError("vec entry key length overflow".to_string()))?;
        let entry_end = value_start
            .checked_add(value_len)
            .ok_or_else(|| Error::IoError("vec entry value length overflow".to_string()))?;
        if entry_end > bytes.len() {
            return Err(Error::InvalidState(
                "truncated vec memtable entry body".to_string(),
            ));
        }
        entries.push((
            Bytes::copy_from_slice(&bytes[key_start..value_start]),
            Bytes::copy_from_slice(&bytes[value_start..entry_end]),
        ));
        offset = entry_end;
    }
    Ok(entries)
}

impl VecMemtable {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::new(),
            entry_data_offsets: Vec::new(),
            data_end: 0,
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
    ) -> usize {
        let key_len = key.encoded_len();
        let value_len = value.encoded_len(num_columns);
        Self::entry_size(key_len, value_len)
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
        self.entry_data_offsets.push(self.data_end);
        self.data_end += needed;
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
        self.entry_data_offsets.push(self.data_end);
        self.data_end += needed;
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
                "invalid vec memtable data range [{}, {}) with end {}",
                start_offset, end_offset, self.data_end
            )));
        }
        if start_offset == end_offset {
            return Ok(0);
        }
        let start_idx = self
            .entry_data_offsets
            .iter()
            .position(|entry_offset| *entry_offset >= start_offset)
            .ok_or_else(|| Error::InvalidState("missing memtable data offset".to_string()))?;
        if self.entry_data_offsets[start_idx] != start_offset {
            return Err(Error::InvalidState(format!(
                "unaligned vec memtable data start {}",
                start_offset
            )));
        }
        let end_idx = if end_offset == self.data_end {
            self.entries.len()
        } else {
            self.entry_data_offsets
                .iter()
                .position(|entry_offset| *entry_offset == end_offset)
                .ok_or_else(|| {
                    Error::InvalidState(format!("unaligned vec memtable data end {}", end_offset))
                })?
        };
        let mut written = 0usize;
        for (key, value) in self.entries[start_idx..end_idx].iter() {
            let key_len = (key.len() as u32).to_le_bytes();
            let value_len = (value.len() as u32).to_le_bytes();
            writer.write(&key_len)?;
            writer.write(&value_len)?;
            writer.write(key)?;
            writer.write(value)?;
            written = written
                .saturating_add(8)
                .saturating_add(key.len())
                .saturating_add(value.len());
        }
        Ok(written)
    }

    fn try_replace_latest_ref(
        &mut self,
        key: &RefKey<'_>,
        value: &RefValue<'_>,
        num_columns: usize,
        sealed_data_end: usize,
    ) -> Result<bool> {
        let Some((last_key, last_value)) = self.entries.last_mut() else {
            return Ok(false);
        };
        let entry_start = *self
            .entry_data_offsets
            .last()
            .expect("vec memtable entries and offsets must match");
        if entry_start < sealed_data_end {
            return Ok(false);
        }
        let key_len = key.encoded_len();
        if last_key.len() != key_len {
            return Ok(false);
        }
        let mut encoded_key = vec![0; key_len];
        encode_key_ref_into(key, &mut encoded_key.as_mut_slice());
        if last_key.as_ref() != encoded_key {
            return Ok(false);
        }
        let value_len = value.encoded_len(num_columns);
        if value_len > last_value.len() {
            return Ok(false);
        }
        let mut encoded_value = vec![0; value_len];
        encode_value_ref_into(value, num_columns, &mut encoded_value.as_mut_slice());
        let old_entry_len = Self::entry_size(last_key.len(), last_value.len());
        let new_entry_len = Self::entry_size(last_key.len(), value_len);
        *last_value = Bytes::from(encoded_value);
        self.data_end = self.data_end - old_entry_len + new_entry_len;
        self.used_bytes = self.used_bytes - old_entry_len + new_entry_len;
        Ok(true)
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
#[path = "../../tests/unit/memtable/vec.rs"]
mod tests;
