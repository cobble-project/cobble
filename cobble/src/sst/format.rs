use crate::error::{Error, Result};
use crate::file::SequentialWriteFile;
use crate::util::unsafe_bytes;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::sync::Arc;

/// Magic number at the end of SST file for validation
const SST_FILE_MAGIC: u32 = 0x53535431; // "SST1"
const SST_FOOTER_VERSION_LEGACY: u32 = 1;
const SST_FOOTER_VERSION_CURRENT: u32 = 2;

/// Footer structure at the end of SST file
/// Layout: [index_block_offset: u64][index_block_size: u64]
///         [filter_block_offset: u64][filter_block_size: u64]
///         [flags: u32][version: u32][magic: u32]
pub(crate) const FOOTER_SIZE: usize = 44; // 8 + 8 + 8 + 8 + 4 + 4 + 4

const FOOTER_FLAG_FILTER_PRESENT: u32 = 0x1;
const FOOTER_FLAG_PARTITIONED_INDEX: u32 = 0x2;
const FOOTER_FLAG_VALUE_WITHOUT_TTL: u32 = 0x4;
const FOOTER_FLAG_BLOCK_CHECKSUMS: u32 = 0x8;
const BLOCK_HEADER_SIZE: usize = 6;
// Covers common encoded state keys without imposing a key-size limit on the format.
const PREFIX_SEARCH_STACK_BYTES: usize = 128;

fn block_header(
    prefix_compressed: bool,
    restart_interval: usize,
    num_entries: usize,
) -> [u8; BLOCK_HEADER_SIZE] {
    let restart_interval = if prefix_compressed {
        restart_interval.max(2).min(u16::MAX as usize) as u16
    } else {
        1
    };
    let mut header = [0; BLOCK_HEADER_SIZE];
    header[..2].copy_from_slice(&restart_interval.to_le_bytes());
    header[2..].copy_from_slice(&(num_entries as u32).to_le_bytes());
    header
}

#[inline]
fn with_prefix_search_scratch<T>(
    target_len: usize,
    search: impl FnOnce(&mut [u8]) -> Result<T>,
) -> Result<T> {
    let mut stack_scratch = [0u8; PREFIX_SEARCH_STACK_BYTES];
    if target_len <= stack_scratch.len() {
        return search(&mut stack_scratch[..target_len]);
    }
    let mut heap_scratch = vec![0u8; target_len];
    search(heap_scratch.as_mut_slice())
}

#[derive(Debug, Clone)]
pub struct Footer {
    pub index_block_offset: u64,
    pub index_block_size: u64,
    pub filter_block_offset: u64,
    pub filter_block_size: u64,
    pub filter_present: bool,
    pub partitioned_index: bool,
    pub value_has_ttl: bool,
    pub block_checksums: bool,
}

impl Footer {
    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(FOOTER_SIZE);
        buf.put_u64_le(self.index_block_offset);
        buf.put_u64_le(self.index_block_size);
        buf.put_u64_le(self.filter_block_offset);
        buf.put_u64_le(self.filter_block_size);
        let mut flags = 0;
        if self.filter_present {
            flags |= FOOTER_FLAG_FILTER_PRESENT;
        }
        if self.partitioned_index {
            flags |= FOOTER_FLAG_PARTITIONED_INDEX;
        }
        if !self.value_has_ttl {
            flags |= FOOTER_FLAG_VALUE_WITHOUT_TTL;
        }
        if self.block_checksums {
            flags |= FOOTER_FLAG_BLOCK_CHECKSUMS;
        }
        buf.put_u32_le(flags);
        // The format version identifies the writer generation, while the flag
        // independently records whether this file contains block checksums.
        // Therefore every newly written SST is v2, even when checksums are
        // disabled by configuration. Version 1 is reserved for legacy files.
        buf.put_u32_le(SST_FOOTER_VERSION_CURRENT);
        buf.put_u32_le(SST_FILE_MAGIC);
        buf.freeze()
    }

    pub(crate) fn decode(data: &[u8]) -> Result<Self> {
        if data.len() != FOOTER_SIZE {
            return Err(Error::IoError(format!(
                "Invalid footer size: expected {}, got {}",
                FOOTER_SIZE,
                data.len()
            )));
        }

        let mut buf = data;
        let index_block_offset = buf.get_u64_le();
        let index_block_size = buf.get_u64_le();
        let filter_block_offset = buf.get_u64_le();
        let filter_block_size = buf.get_u64_le();
        let flags = buf.get_u32_le();
        let version = buf.get_u32_le();
        let magic = buf.get_u32_le();

        if version != SST_FOOTER_VERSION_LEGACY && version != SST_FOOTER_VERSION_CURRENT {
            return Err(Error::IoError(format!(
                "Unsupported SST footer version: {} (expected {} or {})",
                version, SST_FOOTER_VERSION_LEGACY, SST_FOOTER_VERSION_CURRENT
            )));
        }
        let block_checksums = (flags & FOOTER_FLAG_BLOCK_CHECKSUMS) != 0;
        // Legacy v1 blocks never carry checksum trailers. In v2, the flag is
        // authoritative and supports both checksum-enabled and disabled files.
        if version == SST_FOOTER_VERSION_LEGACY && block_checksums {
            return Err(Error::IoError(format!(
                "Invalid SST footer checksum flag for version {}",
                version
            )));
        }

        if magic != SST_FILE_MAGIC {
            return Err(Error::IoError(format!(
                "Invalid SST file magic: expected 0x{:08X}, got 0x{:08X}",
                SST_FILE_MAGIC, magic
            )));
        }

        Ok(Self {
            index_block_offset,
            index_block_size,
            filter_block_offset,
            filter_block_size,
            filter_present: (flags & FOOTER_FLAG_FILTER_PRESENT) != 0,
            partitioned_index: (flags & FOOTER_FLAG_PARTITIONED_INDEX) != 0,
            value_has_ttl: (flags & FOOTER_FLAG_VALUE_WITHOUT_TTL) == 0,
            block_checksums,
        })
    }
}

/// Immutable SST read metadata shared by iterators for one DataFile.
///
/// This deliberately excludes decoded blocks and bloom filters. Those remain
/// owned by the existing block cache and retain its eviction behavior.
#[derive(Debug)]
pub(crate) struct SstReadMetadata {
    footer: Footer,
    index_partitions: Arc<[(u64, u64)]>,
}

impl SstReadMetadata {
    pub(crate) fn from_index_block(footer: Footer, index_block: &Block) -> Result<Self> {
        let mut index_partitions = Vec::with_capacity(index_block.offsets_len());
        if footer.partitioned_index {
            for idx in 0..index_block.offsets_len() {
                let value = index_block.value(idx)?;
                if value.len() != 16 {
                    return Err(Error::IoError("Invalid index partition entry".to_string()));
                }
                let offset = u64::from_le_bytes(value[0..8].try_into().unwrap());
                let size = u64::from_le_bytes(value[8..16].try_into().unwrap());
                if size == 0 {
                    return Err(Error::IoError("Index partition size is zero".to_string()));
                }
                index_partitions.push((offset, size));
            }
        } else if footer.index_block_size > 0 {
            index_partitions.push((footer.index_block_offset, footer.index_block_size));
        } else {
            return Err(Error::IoError("Index block size is zero".to_string()));
        }

        Self::from_parts(footer, index_partitions)
    }

    pub(crate) fn from_parts(footer: Footer, index_partitions: Vec<(u64, u64)>) -> Result<Self> {
        if index_partitions.is_empty() {
            return Err(Error::IoError("SST index has no partitions".to_string()));
        }
        if index_partitions.iter().any(|(_, size)| *size == 0) {
            return Err(Error::IoError("Index partition size is zero".to_string()));
        }
        if !footer.partitioned_index
            && (index_partitions.len() != 1
                || index_partitions[0] != (footer.index_block_offset, footer.index_block_size))
        {
            return Err(Error::IoError(
                "Unpartitioned SST index metadata does not match footer".to_string(),
            ));
        }

        Ok(Self {
            footer,
            index_partitions: index_partitions.into(),
        })
    }

    pub(crate) fn footer(&self) -> &Footer {
        &self.footer
    }

    pub(crate) fn index_partitions(&self) -> Arc<[(u64, u64)]> {
        Arc::clone(&self.index_partitions)
    }
}

/// Block structure
/// layout: [restart_interval: u16][num_entries: u32]
///         [entries...][offsets: u32 * num_entries]
/// prefix-compressed entry: [shared: u16][suffix_len: u32][value_len: u32][suffix][value]
/// non-prefix entry: [key_len: u32][key][value_len: u32][value]
#[derive(Debug, Clone)]
pub struct Block {
    data: Bytes,
    offsets: Vec<u32>,
    block_id: u32,
    size_in_bytes: usize,
    prefix_compressed: bool,
    restart_interval: u32,
}

impl Block {
    pub(crate) fn encode(&self) -> Bytes {
        let offsets_size = self.offsets.len() * 4;
        let total_size = BLOCK_HEADER_SIZE + self.data.len() + offsets_size;
        let mut buf = BytesMut::with_capacity(total_size);
        buf.put_slice(&block_header(
            self.prefix_compressed,
            self.restart_interval as usize,
            self.offsets.len(),
        ));
        buf.put_slice(&self.data);
        for offset in &self.offsets {
            buf.put_u32_le(*offset);
        }
        buf.freeze()
    }

    pub(crate) fn decode(data: Bytes) -> Result<Self> {
        if data.len() < BLOCK_HEADER_SIZE {
            return Err(Error::IoError("Block too small".to_string()));
        }

        let size_in_bytes = data.len();
        let mut buf = data.clone();
        let restart_interval_raw = buf.get_u16_le();
        let num_entries = buf.get_u32_le() as usize;
        let offsets_size = num_entries * 4;
        if data.len() < BLOCK_HEADER_SIZE + offsets_size {
            return Err(Error::IoError("Block data corrupted".to_string()));
        }

        let data_size = data.len() - BLOCK_HEADER_SIZE - offsets_size;
        let block_data = data.slice(BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + data_size);

        let mut offsets = Vec::with_capacity(num_entries);
        let mut offset_buf = data.slice(BLOCK_HEADER_SIZE + data_size..);
        for _ in 0..num_entries {
            offsets.push(offset_buf.get_u32_le());
        }
        let prefix_compressed = restart_interval_raw > 1;
        let restart_interval = if prefix_compressed {
            restart_interval_raw as u32
        } else {
            1
        };

        Ok(Self {
            data: block_data,
            offsets,
            block_id: 0,
            size_in_bytes,
            prefix_compressed,
            restart_interval,
        })
    }

    pub(crate) fn set_block_id(&mut self, block_id: u32) {
        self.block_id = block_id;
    }

    pub(crate) fn block_id(&self) -> u32 {
        self.block_id
    }

    pub(crate) fn is_prefix_compressed(&self) -> bool {
        self.prefix_compressed
    }

    pub(crate) fn get(&self, idx: usize) -> Result<(Bytes, Bytes)> {
        if idx >= self.offsets.len() {
            return Err(Error::IoError(format!(
                "Index out of bounds: {} >= {}",
                idx,
                self.offsets.len()
            )));
        }
        if self.prefix_compressed {
            let key = self.decode_prefix_key(idx)?;
            let (_, _, value_start, value_end) = self.decode_prefix_entry_bounds(idx)?;
            return Ok((key, self.data.slice(value_start..value_end)));
        }

        let offset = self.offsets[idx] as usize;
        let data = self.data.as_ref();
        if offset + 4 > data.len() {
            return Err(Error::IoError("Corrupted block entry".to_string()));
        }

        let key_len = u32::from_le_bytes(
            data[offset..offset + 4]
                .try_into()
                .expect("slice length checked"),
        ) as usize;
        let key_start = offset + 4;
        let key_end = key_start + key_len;
        if key_end > data.len() {
            return Err(Error::IoError("Corrupted key data".to_string()));
        }

        if key_end + 4 > data.len() {
            return Err(Error::IoError("Corrupted value length".to_string()));
        }

        let value_len = u32::from_le_bytes(
            data[key_end..key_end + 4]
                .try_into()
                .expect("slice length checked"),
        ) as usize;
        let value_start = key_end + 4;
        let value_end = value_start + value_len;
        if value_end > data.len() {
            return Err(Error::IoError("Corrupted value data".to_string()));
        }

        Ok((
            self.data.slice(key_start..key_end),
            self.data.slice(value_start..value_end),
        ))
    }

    pub(crate) fn key(&self, idx: usize) -> Result<Bytes> {
        if idx >= self.offsets.len() {
            return Err(Error::IoError(format!(
                "Index out of bounds: {} >= {}",
                idx,
                self.offsets.len()
            )));
        }
        if self.prefix_compressed {
            return self.decode_prefix_key(idx);
        }

        let offset = self.offsets[idx] as usize;
        let data = self.data.as_ref();
        if offset + 4 > data.len() {
            return Err(Error::IoError("Corrupted block entry".to_string()));
        }

        let key_len = u32::from_le_bytes(
            data[offset..offset + 4]
                .try_into()
                .expect("slice length checked"),
        ) as usize;
        let key_start = offset + 4;
        let key_end = key_start + key_len;
        if key_end > data.len() {
            return Err(Error::IoError("Corrupted key data".to_string()));
        }

        Ok(self.data.slice(key_start..key_end))
    }

    pub(crate) fn value(&self, idx: usize) -> Result<Bytes> {
        if idx >= self.offsets.len() {
            return Err(Error::IoError(format!(
                "Index out of bounds: {} >= {}",
                idx,
                self.offsets.len()
            )));
        }
        if self.prefix_compressed {
            let (_, _, value_start, value_end) = self.decode_prefix_entry_bounds(idx)?;
            return Ok(self.data.slice(value_start..value_end));
        }

        let offset = self.offsets[idx] as usize;
        let data = self.data.as_ref();
        if offset + 4 > data.len() {
            return Err(Error::IoError("Corrupted block entry".to_string()));
        }

        let key_len = u32::from_le_bytes(
            data[offset..offset + 4]
                .try_into()
                .expect("slice length checked"),
        ) as usize;
        let key_start = offset + 4;
        let key_end = key_start + key_len;
        if key_end > data.len() {
            return Err(Error::IoError("Corrupted key data".to_string()));
        }

        if key_end + 4 > data.len() {
            return Err(Error::IoError("Corrupted value length".to_string()));
        }

        let value_len = u32::from_le_bytes(
            data[key_end..key_end + 4]
                .try_into()
                .expect("slice length checked"),
        ) as usize;
        let value_start = key_end + 4;
        let value_end = value_start + value_len;
        if value_end > data.len() {
            return Err(Error::IoError("Corrupted value data".to_string()));
        }

        Ok(self.data.slice(value_start..value_end))
    }

    pub(crate) fn offsets_len(&self) -> usize {
        self.offsets.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    pub(crate) fn find_equal_or_greater_idx(&self, target: &Bytes) -> Result<usize> {
        if self.prefix_compressed {
            return self.find_equal_or_greater_idx_prefix(target);
        }
        let mut left = 0;
        let mut right = self.offsets_len();
        while left < right {
            let mid = (left + right) / 2;
            let key = self.key(mid)?;
            match key.cmp(target) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Ok(left)
    }

    pub(crate) fn find_lower_or_equal_idx(&self, target: &Bytes) -> Result<usize> {
        if self.prefix_compressed {
            return self.find_lower_or_equal_idx_prefix(target);
        }
        let mut left = 0;
        let mut right = self.offsets_len() - 1;
        while left < right {
            let mid = (left + right).div_ceil(2);
            let key = self.key(mid)?;
            match key.cmp(target) {
                std::cmp::Ordering::Less => left = mid,
                std::cmp::Ordering::Greater => right = mid - 1,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Ok(left)
    }

    /// Returns the encoded value for an exact key match without materializing a scan cursor.
    pub(crate) fn get_exact(&self, target: &[u8]) -> Result<Option<Bytes>> {
        if self.is_empty() {
            return Ok(None);
        }
        if !self.prefix_compressed {
            let target = unsafe_bytes(target);
            let idx = self.find_equal_or_greater_idx(&target)?;
            if idx >= self.offsets_len() || self.key(idx)?.as_ref() != target.as_ref() {
                return Ok(None);
            }
            return self.value(idx).map(Some);
        }

        with_prefix_search_scratch(target.len(), |scratch| {
            self.get_exact_prefix(target, scratch)
        })
    }

    fn get_exact_prefix(&self, target: &[u8], key_prefix: &mut [u8]) -> Result<Option<Bytes>> {
        let target_len = target.len();
        let (start_idx, end_idx) = self.prefix_restart_search_window(target)?;
        let data = self.data.as_ref();
        let mut key_len = 0usize;
        for entry_idx in start_idx..end_idx {
            let offset = self.offsets[entry_idx] as usize;
            if offset + 10 > data.len() {
                return Err(Error::IoError("Corrupted prefix entry header".to_string()));
            }
            let shared = u16::from_le_bytes(
                data[offset..offset + 2]
                    .try_into()
                    .expect("prefix entry header checked"),
            ) as usize;
            let (suffix_start, suffix_end) = self.decode_prefix_suffix_bounds(entry_idx)?;
            if entry_idx == start_idx && shared != 0 {
                return Err(Error::IoError(
                    "Corrupted prefix restart entry (shared != 0)".to_string(),
                ));
            }
            if shared > key_len {
                return Err(Error::IoError(
                    "Corrupted prefix key (shared prefix out of bounds)".to_string(),
                ));
            }
            let compare_len = Self::update_prefix_search_buffer(
                key_prefix,
                target_len,
                &mut key_len,
                shared,
                &data[suffix_start..suffix_end],
            );
            match key_prefix[..compare_len].cmp(&target[..compare_len]) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal if key_len == target_len => {
                    return self.value(entry_idx).map(Some);
                }
                std::cmp::Ordering::Equal if key_len < target_len => {}
                std::cmp::Ordering::Equal => return Ok(None),
                std::cmp::Ordering::Greater => return Ok(None),
            }
        }
        Ok(None)
    }

    pub(crate) fn size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }

    fn decode_prefix_suffix_bounds(&self, idx: usize) -> Result<(usize, usize)> {
        let offset = self.offsets[idx] as usize;
        let data = self.data.as_ref();
        if offset + 10 > data.len() {
            return Err(Error::IoError("Corrupted prefix entry header".to_string()));
        }
        let suffix_len = u32::from_le_bytes(
            data[offset + 2..offset + 6]
                .try_into()
                .expect("slice length checked"),
        ) as usize;
        let suffix_start = offset + 10;
        let suffix_end = suffix_start + suffix_len;
        if suffix_end > data.len() {
            return Err(Error::IoError("Corrupted prefix key suffix".to_string()));
        }
        Ok((suffix_start, suffix_end))
    }

    fn decode_prefix_entry_bounds(&self, idx: usize) -> Result<(usize, usize, usize, usize)> {
        let (suffix_start, suffix_end) = self.decode_prefix_suffix_bounds(idx)?;
        let data = self.data.as_ref();
        let value_len = u32::from_le_bytes(
            data[suffix_start - 4..suffix_start]
                .try_into()
                .expect("slice length checked"),
        ) as usize;
        let value_start = suffix_end;
        let value_end = value_start + value_len;
        if value_end > data.len() {
            return Err(Error::IoError("Corrupted prefix value bytes".to_string()));
        }
        Ok((suffix_start, suffix_end, value_start, value_end))
    }

    fn compare_prefix_restart_key_with_target(
        &self,
        idx: usize,
        target: &[u8],
    ) -> Result<std::cmp::Ordering> {
        let offset = self.offsets[idx] as usize;
        let data = self.data.as_ref();
        if offset + 10 > data.len() {
            return Err(Error::IoError("Corrupted prefix entry header".to_string()));
        }
        let shared = u16::from_le_bytes(
            data[offset..offset + 2]
                .try_into()
                .expect("slice length checked"),
        );
        if shared != 0 {
            return Err(Error::IoError(
                "Corrupted prefix restart entry (shared != 0)".to_string(),
            ));
        }
        let suffix_len = u32::from_le_bytes(
            data[offset + 2..offset + 6]
                .try_into()
                .expect("slice length checked"),
        ) as usize;
        let suffix_start = offset + 10;
        let suffix_end = suffix_start + suffix_len;
        if suffix_end > data.len() {
            return Err(Error::IoError("Corrupted prefix key suffix".to_string()));
        }
        let compare_len = suffix_len.min(target.len());
        let restart_prefix = &data[suffix_start..suffix_start + compare_len];
        let target_prefix = &target[..compare_len];
        match restart_prefix.cmp(target_prefix) {
            std::cmp::Ordering::Equal => Ok(suffix_len.cmp(&target.len())),
            other => Ok(other),
        }
    }

    fn prefix_restart_search_window(&self, target: &[u8]) -> Result<(usize, usize)> {
        let entry_count = self.offsets_len();
        if entry_count == 0 {
            return Ok((0, 0));
        }
        let interval = self.restart_interval.max(1) as usize;
        let restart_count = entry_count.div_ceil(interval);

        let mut left = -1isize;
        let mut right = restart_count as isize - 1;
        while left != right {
            let mid = left + (right - left + 1) / 2;
            let restart_idx = mid as usize * interval;
            match self.compare_prefix_restart_key_with_target(restart_idx, target)? {
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal => left = mid,
                std::cmp::Ordering::Greater => right = mid - 1,
            }
        }

        let start_restart = if left < 0 { 0 } else { left as usize };
        let start_idx = start_restart * interval;
        let end_idx = (start_idx + interval).min(entry_count);
        Ok((start_idx, end_idx))
    }

    #[inline]
    fn update_prefix_search_buffer(
        key_prefix: &mut [u8],
        target_len: usize,
        key_len: &mut usize,
        shared: usize,
        suffix: &[u8],
    ) -> usize {
        let next_key_len = shared + suffix.len();
        let compare_len = next_key_len.min(target_len);
        let compare_shared = shared.min(target_len);
        if compare_len > compare_shared {
            key_prefix[compare_shared..compare_len]
                .copy_from_slice(&suffix[..compare_len - compare_shared]);
        }
        *key_len = next_key_len;
        compare_len
    }

    fn find_equal_or_greater_idx_prefix(&self, target: &Bytes) -> Result<usize> {
        let target_bytes = target.as_ref();
        let target_len = target_bytes.len();
        let (start_idx, end_idx) = self.prefix_restart_search_window(target_bytes)?;
        if start_idx == end_idx {
            return Ok(0);
        }
        with_prefix_search_scratch(target_len, |scratch| {
            self.find_equal_or_greater_idx_prefix_in_window(
                target_bytes,
                start_idx,
                end_idx,
                scratch,
            )
        })
    }

    fn find_equal_or_greater_idx_prefix_in_window(
        &self,
        target_bytes: &[u8],
        start_idx: usize,
        end_idx: usize,
        key_prefix: &mut [u8],
    ) -> Result<usize> {
        let target_len = target_bytes.len();
        let data = self.data.as_ref();
        let mut key_len = 0usize;

        for entry_idx in start_idx..end_idx {
            let offset = self.offsets[entry_idx] as usize;
            if offset + 10 > data.len() {
                return Err(Error::IoError("Corrupted prefix entry header".to_string()));
            }
            let shared = u16::from_le_bytes(
                data[offset..offset + 2]
                    .try_into()
                    .expect("slice length checked"),
            ) as usize;
            let (suffix_start, suffix_end) = self.decode_prefix_suffix_bounds(entry_idx)?;
            if entry_idx == start_idx && shared != 0 {
                return Err(Error::IoError(
                    "Corrupted prefix restart entry (shared != 0)".to_string(),
                ));
            }
            if shared > key_len {
                return Err(Error::IoError(
                    "Corrupted prefix key (shared prefix out of bounds)".to_string(),
                ));
            }
            let compare_len = Self::update_prefix_search_buffer(
                key_prefix,
                target_len,
                &mut key_len,
                shared,
                &data[suffix_start..suffix_end],
            );

            match key_prefix[..compare_len].cmp(&target_bytes[..compare_len]) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Greater => return Ok(entry_idx),
                std::cmp::Ordering::Equal if key_len >= target_len => return Ok(entry_idx),
                std::cmp::Ordering::Equal => {}
            }
        }

        Ok(end_idx)
    }

    fn find_lower_or_equal_idx_prefix(&self, target: &Bytes) -> Result<usize> {
        let target_bytes = target.as_ref();
        let target_len = target_bytes.len();
        let (start_idx, end_idx) = self.prefix_restart_search_window(target_bytes)?;
        if start_idx == end_idx {
            return Ok(0);
        }
        with_prefix_search_scratch(target_len, |scratch| {
            self.find_lower_or_equal_idx_prefix_in_window(target_bytes, start_idx, end_idx, scratch)
        })
    }

    fn find_lower_or_equal_idx_prefix_in_window(
        &self,
        target_bytes: &[u8],
        start_idx: usize,
        end_idx: usize,
        key_prefix: &mut [u8],
    ) -> Result<usize> {
        let target_len = target_bytes.len();
        let data = self.data.as_ref();
        let mut key_len = 0usize;

        for entry_idx in start_idx..end_idx {
            let offset = self.offsets[entry_idx] as usize;
            if offset + 10 > data.len() {
                return Err(Error::IoError("Corrupted prefix entry header".to_string()));
            }
            let shared = u16::from_le_bytes(
                data[offset..offset + 2]
                    .try_into()
                    .expect("slice length checked"),
            ) as usize;
            let (suffix_start, suffix_end) = self.decode_prefix_suffix_bounds(entry_idx)?;
            if entry_idx == start_idx && shared != 0 {
                return Err(Error::IoError(
                    "Corrupted prefix restart entry (shared != 0)".to_string(),
                ));
            }
            if shared > key_len {
                return Err(Error::IoError(
                    "Corrupted prefix key (shared prefix out of bounds)".to_string(),
                ));
            }
            let compare_len = Self::update_prefix_search_buffer(
                key_prefix,
                target_len,
                &mut key_len,
                shared,
                &data[suffix_start..suffix_end],
            );

            match key_prefix[..compare_len].cmp(&target_bytes[..compare_len]) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Greater => {
                    return Ok(if entry_idx == start_idx {
                        start_idx
                    } else {
                        entry_idx - 1
                    });
                }
                std::cmp::Ordering::Equal if key_len > target_len => {
                    return Ok(if entry_idx == start_idx {
                        start_idx
                    } else {
                        entry_idx - 1
                    });
                }
                std::cmp::Ordering::Equal => {}
            }
        }

        Ok(end_idx - 1)
    }

    fn decode_prefix_key(&self, idx: usize) -> Result<Bytes> {
        let mut key = Vec::<u8>::new();
        self.decode_prefix_key_into(idx, &mut key)?;
        Ok(Bytes::from(key))
    }

    pub(crate) fn advance_prefix_key(&self, idx: usize, key: &mut Vec<u8>) -> Result<()> {
        if !self.prefix_compressed {
            return Err(Error::IoError(
                "advance_prefix_key requires prefix-compressed block".to_string(),
            ));
        }
        let interval = self.restart_interval.max(1) as usize;
        let restart_idx = idx - (idx % interval);
        self.apply_prefix_entry(idx, restart_idx, key)
    }

    pub(crate) fn decode_prefix_key_into(&self, idx: usize, key: &mut Vec<u8>) -> Result<()> {
        let interval = self.restart_interval.max(1) as usize;
        let restart_idx = idx - (idx % interval);
        key.clear();
        for entry_idx in restart_idx..=idx {
            self.apply_prefix_entry(entry_idx, restart_idx, key)?;
        }
        Ok(())
    }

    fn apply_prefix_entry(
        &self,
        entry_idx: usize,
        restart_idx: usize,
        key: &mut Vec<u8>,
    ) -> Result<()> {
        let data = self.data.as_ref();
        let offset = self.offsets[entry_idx] as usize;
        if offset + 10 > data.len() {
            return Err(Error::IoError("Corrupted prefix entry header".to_string()));
        }
        let shared = u16::from_le_bytes(
            data[offset..offset + 2]
                .try_into()
                .expect("slice length checked"),
        ) as usize;
        let (suffix_start, suffix_end) = self.decode_prefix_suffix_bounds(entry_idx)?;
        if entry_idx == restart_idx && shared != 0 {
            return Err(Error::IoError(
                "Corrupted prefix restart entry (shared != 0)".to_string(),
            ));
        }
        if shared > key.len() {
            return Err(Error::IoError(
                "Corrupted prefix key (shared prefix out of bounds)".to_string(),
            ));
        }
        key.truncate(shared);
        key.extend_from_slice(&data[suffix_start..suffix_end]);
        Ok(())
    }
}

/// Builder for creating blocks
pub struct BlockBuilder {
    data: BytesMut,
    offsets: Vec<u32>,
    target_size: usize,
    prefix_compressed: bool,
    restart_interval: usize,
    last_key: Vec<u8>,
    entries_since_restart: usize,
}

impl BlockBuilder {
    pub(crate) fn new(target_size: usize) -> Self {
        Self::new_with_prefix(target_size, 1, false)
    }

    pub(crate) fn new_with_prefix(
        target_size: usize,
        restart_interval: usize,
        prefix_compressed: bool,
    ) -> Self {
        let mut data = BytesMut::with_capacity(target_size.max(BLOCK_HEADER_SIZE));
        data.resize(BLOCK_HEADER_SIZE, 0);
        Self {
            data,
            offsets: Vec::new(),
            target_size,
            prefix_compressed,
            restart_interval: restart_interval.max(1),
            last_key: Vec::new(),
            entries_since_restart: 0,
        }
    }

    pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) {
        let offset = (self.data.len() - BLOCK_HEADER_SIZE) as u32;
        self.offsets.push(offset);
        if self.prefix_compressed {
            let is_restart = self.entries_since_restart == 0;
            let shared = if is_restart {
                0
            } else {
                common_prefix_len(self.last_key.as_slice(), key).min(u16::MAX as usize)
            };
            let suffix = &key[shared..];
            self.data.put_u16_le(shared as u16);
            self.data.put_u32_le(suffix.len() as u32);
            self.data.put_u32_le(value.len() as u32);
            self.data.put_slice(suffix);
            self.data.put_slice(value);
            self.last_key.clear();
            self.last_key.extend_from_slice(key);
            self.entries_since_restart += 1;
            if self.entries_since_restart >= self.restart_interval {
                self.entries_since_restart = 0;
            }
            return;
        }

        self.data.put_u32_le(key.len() as u32);
        self.data.put_slice(key);
        self.data.put_u32_le(value.len() as u32);
        self.data.put_slice(value);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    pub(crate) fn estimated_size(&self) -> usize {
        self.data.len() + self.offsets.len() * 4
    }

    pub(crate) fn should_finish(&self) -> bool {
        !self.is_empty() && self.estimated_size() >= self.target_size
    }

    pub(crate) fn write_to<W: SequentialWriteFile>(&self, writer: &mut W) -> Result<usize> {
        let size = self.estimated_size();
        writer.write(&block_header(
            self.prefix_compressed,
            self.restart_interval,
            self.offsets.len(),
        ))?;
        let data_bytes = &self.data[BLOCK_HEADER_SIZE..];
        if !data_bytes.is_empty() {
            writer.write(data_bytes)?;
        }
        for offset in &self.offsets {
            writer.write(&offset.to_le_bytes())?;
        }
        Ok(size)
    }

    pub(crate) fn build(self) -> Block {
        let size_in_bytes = self.estimated_size();
        let data = self.data.freeze();
        Block {
            data: data.slice(BLOCK_HEADER_SIZE..),
            offsets: self.offsets,
            block_id: 0,
            size_in_bytes,
            prefix_compressed: self.prefix_compressed,
            restart_interval: self.restart_interval as u32,
        }
    }

    pub(crate) fn build_encoded(mut self) -> Bytes {
        // The builder reserves the header at construction time. Finalizing it in place and
        // appending offsets avoids building a `Block` and copying its data into a second buffer.
        let header = block_header(
            self.prefix_compressed,
            self.restart_interval,
            self.offsets.len(),
        );
        self.data[..BLOCK_HEADER_SIZE].copy_from_slice(&header);
        for offset in self.offsets {
            self.data.put_u32_le(offset);
        }
        self.data.freeze()
    }

    pub(crate) fn clear(&mut self) {
        self.data.clear();
        self.data.resize(BLOCK_HEADER_SIZE, 0);
        self.offsets.clear();
        self.last_key.clear();
        self.entries_since_restart = 0;
    }
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let mut idx = 0usize;
    let max = a.len().min(b.len());
    while idx < max && a[idx] == b[idx] {
        idx += 1;
    }
    idx
}

#[cfg(test)]
mod tests {
    use super::*;

    const FOOTER_VERSION_OFFSET: usize =
        4 * std::mem::size_of::<u64>() + std::mem::size_of::<u32>();

    fn footer_version(encoded: &[u8]) -> u32 {
        u32::from_le_bytes(
            encoded[FOOTER_VERSION_OFFSET..FOOTER_VERSION_OFFSET + 4]
                .try_into()
                .unwrap(),
        )
    }

    fn set_footer_version(encoded: &mut [u8], version: u32) {
        encoded[FOOTER_VERSION_OFFSET..FOOTER_VERSION_OFFSET + 4]
            .copy_from_slice(&version.to_le_bytes());
    }

    #[test]
    fn test_footer_encode_decode() {
        let footer = Footer {
            index_block_offset: 100,
            index_block_size: 200,
            filter_block_offset: 300,
            filter_block_size: 400,
            filter_present: true,
            partitioned_index: false,
            value_has_ttl: true,
            block_checksums: true,
        };
        let encoded = footer.encode();
        assert_eq!(encoded.len(), FOOTER_SIZE);
        assert_eq!(footer_version(&encoded), SST_FOOTER_VERSION_CURRENT);

        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.index_block_offset, 100);
        assert_eq!(decoded.index_block_size, 200);
        assert_eq!(decoded.filter_block_offset, 300);
        assert_eq!(decoded.filter_block_size, 400);
        assert!(decoded.filter_present);
        assert!(!decoded.partitioned_index);
        assert!(decoded.value_has_ttl);
        assert!(decoded.block_checksums);
    }

    #[test]
    fn test_sst_read_metadata_rejects_zero_sized_unpartitioned_index() {
        let mut builder = BlockBuilder::new(1024);
        builder.add(b"key", b"value");
        let footer = Footer {
            index_block_offset: 0,
            index_block_size: 0,
            filter_block_offset: 0,
            filter_block_size: 0,
            filter_present: false,
            partitioned_index: false,
            value_has_ttl: true,
            block_checksums: false,
        };

        assert!(SstReadMetadata::from_index_block(footer, &builder.build()).is_err());
    }

    #[test]
    fn test_footer_without_checksums_still_uses_current_version() {
        let footer = Footer {
            index_block_offset: 10,
            index_block_size: 20,
            filter_block_offset: 30,
            filter_block_size: 40,
            filter_present: true,
            partitioned_index: true,
            value_has_ttl: false,
            block_checksums: false,
        };
        let encoded = footer.encode();
        assert_eq!(footer_version(&encoded), SST_FOOTER_VERSION_CURRENT);
        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.index_block_offset, 10);
        assert_eq!(decoded.index_block_size, 20);
        assert_eq!(decoded.filter_block_offset, 30);
        assert_eq!(decoded.filter_block_size, 40);
        assert!(decoded.filter_present);
        assert!(decoded.partitioned_index);
        assert!(!decoded.value_has_ttl);
        assert!(!decoded.block_checksums);
    }

    #[test]
    fn test_legacy_footer_without_checksums_is_supported() {
        let footer = Footer {
            index_block_offset: 10,
            index_block_size: 20,
            filter_block_offset: 30,
            filter_block_size: 40,
            filter_present: false,
            partitioned_index: false,
            value_has_ttl: true,
            block_checksums: false,
        };
        let mut encoded = footer.encode().to_vec();
        set_footer_version(&mut encoded, SST_FOOTER_VERSION_LEGACY);

        let decoded = Footer::decode(&encoded).unwrap();
        assert!(!decoded.block_checksums);
    }

    #[test]
    fn test_legacy_footer_rejects_checksum_flag() {
        let footer = Footer {
            index_block_offset: 10,
            index_block_size: 20,
            filter_block_offset: 30,
            filter_block_size: 40,
            filter_present: false,
            partitioned_index: false,
            value_has_ttl: true,
            block_checksums: true,
        };
        let mut encoded = footer.encode().to_vec();
        set_footer_version(&mut encoded, SST_FOOTER_VERSION_LEGACY);

        assert!(Footer::decode(&encoded).is_err());
    }

    #[test]
    fn test_block_encode_decode() {
        let mut builder = BlockBuilder::new(4096);
        builder.add(b"key1", b"value1");
        builder.add(b"key2", b"value2");
        builder.add(b"key3", b"value3");

        let block = builder.build();
        assert_eq!(block.offsets_len(), 3);

        let encoded = block.encode();
        let decoded = Block::decode(encoded).unwrap();

        assert_eq!(decoded.offsets_len(), 3);

        let (key, value) = decoded.get(0).unwrap();
        assert_eq!(&key[..], b"key1");
        assert_eq!(&value[..], b"value1");

        let (key, value) = decoded.get(1).unwrap();
        assert_eq!(&key[..], b"key2");
        assert_eq!(&value[..], b"value2");

        let (key, value) = decoded.get(2).unwrap();
        assert_eq!(&key[..], b"key3");
        assert_eq!(&value[..], b"value3");
    }

    #[test]
    fn test_block_builder_encoded_matches_block_encoding() {
        for prefix_compressed in [false, true] {
            let populate = |builder: &mut BlockBuilder| {
                builder.add(b"map:key:0001", b"value1");
                builder.add(b"map:key:0002", b"value2");
                builder.add(b"map:key:0010", b"value3");
            };
            let mut block_builder = BlockBuilder::new_with_prefix(4096, 2, prefix_compressed);
            let mut encoded_builder = BlockBuilder::new_with_prefix(4096, 2, prefix_compressed);
            populate(&mut block_builder);
            populate(&mut encoded_builder);

            let expected = block_builder.build().encode();
            let actual = encoded_builder.build_encoded();
            assert_eq!(actual, expected);

            let decoded = Block::decode(actual).unwrap();
            assert_eq!(decoded.offsets_len(), 3);
            assert_eq!(decoded.get(1).unwrap().0.as_ref(), b"map:key:0002");
            assert_eq!(decoded.get(1).unwrap().1.as_ref(), b"value2");
        }
    }

    #[test]
    fn test_empty_block_builder_encoded_round_trip() {
        let builder = BlockBuilder::new(32);
        assert!(builder.is_empty());
        assert_eq!(builder.estimated_size(), BLOCK_HEADER_SIZE);

        let encoded = builder.build_encoded();
        assert_eq!(encoded.len(), BLOCK_HEADER_SIZE);
        let decoded = Block::decode(encoded).unwrap();
        assert_eq!(decoded.offsets_len(), 0);
        assert_eq!(decoded.size_in_bytes(), BLOCK_HEADER_SIZE);
    }

    #[test]
    fn test_block_builder_size_boundary_includes_header_and_offsets() {
        let entry_size = 4 + b"key".len() + 4 + b"value".len();
        let encoded_size = BLOCK_HEADER_SIZE + entry_size + 4;
        let mut builder = BlockBuilder::new(encoded_size);
        assert_eq!(builder.estimated_size(), BLOCK_HEADER_SIZE);
        assert!(!builder.should_finish());

        builder.add(b"key", b"value");
        assert_eq!(builder.estimated_size(), encoded_size);
        assert!(builder.should_finish());
        assert_eq!(builder.build_encoded().len(), encoded_size);
    }

    #[test]
    fn test_block_builder_should_finish() {
        let mut builder = BlockBuilder::new(100);
        assert!(!builder.should_finish());

        // Add enough data to exceed target size
        builder.add(b"key1", b"value1_with_long_data");
        builder.add(b"key2", b"value2_with_long_data");
        builder.add(b"key3", b"value3_with_long_data");

        assert!(builder.should_finish());
    }

    #[test]
    fn test_block_prefix_encode_decode() {
        let mut builder = BlockBuilder::new_with_prefix(4096, 2, true);
        builder.add(b"map:key:0001", b"v1");
        builder.add(b"map:key:0002", b"v2");
        builder.add(b"map:key:0003", b"v3");

        let encoded = builder.build().encode();
        let decoded = Block::decode(encoded).unwrap();
        assert_eq!(&decoded.key(0).unwrap()[..], b"map:key:0001");
        assert_eq!(&decoded.key(1).unwrap()[..], b"map:key:0002");
        assert_eq!(&decoded.key(2).unwrap()[..], b"map:key:0003");
        assert_eq!(&decoded.value(1).unwrap()[..], b"v2");
        assert_eq!(
            decoded
                .find_equal_or_greater_idx(&Bytes::from("map:key:0002"))
                .unwrap(),
            1
        );
    }

    #[test]
    fn test_block_exact_lookup() {
        for prefix_compressed in [false, true] {
            let mut builder = BlockBuilder::new_with_prefix(4096, 2, prefix_compressed);
            builder.add(b"map:key:0001", b"v1");
            builder.add(b"map:key:0002", b"v2");
            builder.add(b"map:key:0003", b"v3");
            let long_key = vec![b'z'; PREFIX_SEARCH_STACK_BYTES + 1];
            builder.add(long_key.as_slice(), b"long");
            let block = Block::decode(builder.build().encode()).unwrap();

            assert_eq!(
                block.get_exact(b"map:key:0002").unwrap().as_deref(),
                Some(b"v2".as_slice())
            );
            assert!(block.get_exact(b"map:key:0002a").unwrap().is_none());
            assert!(block.get_exact(b"map:key:9999").unwrap().is_none());
            assert_eq!(
                block.get_exact(long_key.as_slice()).unwrap().as_deref(),
                Some(b"long".as_slice())
            );
        }
    }

    #[test]
    fn test_block_prefix_restart_interval_repeats() {
        let mut builder = BlockBuilder::new_with_prefix(4096, 2, true);
        builder.add(b"map:key:0001", b"v1");
        builder.add(b"map:key:0002", b"v2");
        builder.add(b"map:key:0003", b"v3");
        builder.add(b"map:key:0004", b"v4");
        let decoded = Block::decode(builder.build().encode()).unwrap();

        fn shared_len(block: &Block, idx: usize) -> u16 {
            let offset = block.offsets[idx] as usize;
            let data = block.data.as_ref();
            u16::from_le_bytes(
                data[offset..offset + 2]
                    .try_into()
                    .expect("prefix entry header exists"),
            )
        }

        assert_eq!(shared_len(&decoded, 0), 0);
        assert!(shared_len(&decoded, 1) > 0);
        assert_eq!(shared_len(&decoded, 2), 0);
        assert!(shared_len(&decoded, 3) > 0);
    }

    #[test]
    fn test_block_prefix_seek_binary_then_linear_interval() {
        let mut builder = BlockBuilder::new_with_prefix(4096, 3, true);
        for i in 1..=8 {
            let key = format!("map:key:{i:04}");
            let value = format!("v{i}");
            builder.add(key.as_bytes(), value.as_bytes());
        }
        let decoded = Block::decode(builder.build().encode()).unwrap();

        assert_eq!(
            decoded
                .find_equal_or_greater_idx(&Bytes::from("map:key:0000"))
                .unwrap(),
            0
        );
        assert_eq!(
            decoded
                .find_equal_or_greater_idx(&Bytes::from("map:key:0004"))
                .unwrap(),
            3
        );
        assert_eq!(
            decoded
                .find_equal_or_greater_idx(&Bytes::from("map:key:0005"))
                .unwrap(),
            4
        );
        assert_eq!(
            decoded
                .find_equal_or_greater_idx(&Bytes::from("map:key:9999"))
                .unwrap(),
            decoded.offsets_len()
        );
        assert_eq!(
            decoded
                .find_lower_or_equal_idx(&Bytes::from("map:key:0000"))
                .unwrap(),
            0
        );
        assert_eq!(
            decoded
                .find_lower_or_equal_idx(&Bytes::from("map:key:0004"))
                .unwrap(),
            3
        );
        assert_eq!(
            decoded
                .find_lower_or_equal_idx(&Bytes::from("map:key:0004a"))
                .unwrap(),
            3
        );
        assert_eq!(
            decoded
                .find_lower_or_equal_idx(&Bytes::from("map:key:0005"))
                .unwrap(),
            4
        );
        assert_eq!(
            decoded
                .find_lower_or_equal_idx(&Bytes::from("map:key:9999"))
                .unwrap(),
            7
        );
    }

    #[test]
    fn test_block_prefix_seek_uses_target_prefix_and_key_len() {
        let mut builder = BlockBuilder::new_with_prefix(4096, 4, true);
        builder.add(b"abc", b"v1");
        builder.add(b"abcx", b"v2");
        builder.add(b"abcxy", b"v3");
        builder.add(b"abcz", b"v4");
        let decoded = Block::decode(builder.build().encode()).unwrap();

        assert_eq!(
            decoded
                .find_equal_or_greater_idx(&Bytes::from("abc"))
                .unwrap(),
            0
        );
        assert_eq!(
            decoded
                .find_equal_or_greater_idx(&Bytes::from("abcd"))
                .unwrap(),
            1
        );
        assert_eq!(
            decoded
                .find_equal_or_greater_idx(&Bytes::from("abcxz"))
                .unwrap(),
            3
        );
        assert_eq!(
            decoded
                .find_lower_or_equal_idx(&Bytes::from("abc"))
                .unwrap(),
            0
        );
        assert_eq!(
            decoded
                .find_lower_or_equal_idx(&Bytes::from("abcd"))
                .unwrap(),
            0
        );
        assert_eq!(
            decoded
                .find_lower_or_equal_idx(&Bytes::from("abcxz"))
                .unwrap(),
            2
        );
    }
}
