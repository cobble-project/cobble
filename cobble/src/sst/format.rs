use crate::error::{Error, Result};
use crate::file::SequentialWriteFile;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Magic number at the end of SST file for validation
const SST_FILE_MAGIC: u32 = 0x53535431; // "SST1"
const SST_FOOTER_VERSION_CURRENT: u32 = 1;

/// Footer structure at the end of SST file
/// Layout: [index_block_offset: u64][index_block_size: u64]
///         [filter_block_offset: u64][filter_block_size: u64]
///         [flags: u32][version: u32][magic: u32]
pub(crate) const FOOTER_SIZE: usize = 44; // 8 + 8 + 8 + 8 + 4 + 4 + 4

const FOOTER_FLAG_FILTER_PRESENT: u32 = 0x1;
const FOOTER_FLAG_PARTITIONED_INDEX: u32 = 0x2;

#[derive(Debug, Clone)]
pub struct Footer {
    pub index_block_offset: u64,
    pub index_block_size: u64,
    pub filter_block_offset: u64,
    pub filter_block_size: u64,
    pub filter_present: bool,
    pub partitioned_index: bool,
}

impl Footer {
    pub(crate) fn new(
        index_block_offset: u64,
        index_block_size: u64,
        filter_block_offset: u64,
        filter_block_size: u64,
        filter_present: bool,
        partitioned_index: bool,
    ) -> Self {
        Self {
            index_block_offset,
            index_block_size,
            filter_block_offset,
            filter_block_size,
            filter_present,
            partitioned_index,
        }
    }

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
        buf.put_u32_le(flags);
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

        if version != SST_FOOTER_VERSION_CURRENT {
            return Err(Error::IoError(format!(
                "Unsupported SST footer version: {} (expected {})",
                version, SST_FOOTER_VERSION_CURRENT
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
        })
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
        let total_size = 6 + self.data.len() + offsets_size;
        let mut buf = BytesMut::with_capacity(total_size);
        let restart_interval = if self.prefix_compressed {
            self.restart_interval.max(2).min(u16::MAX as u32) as u16
        } else {
            1u16
        };
        buf.put_u16_le(restart_interval);
        buf.put_u32_le(self.offsets.len() as u32);
        buf.put_slice(&self.data);
        for offset in &self.offsets {
            buf.put_u32_le(*offset);
        }
        buf.freeze()
    }

    pub(crate) fn decode(data: Bytes) -> Result<Self> {
        if data.len() < 6 {
            return Err(Error::IoError("Block too small".to_string()));
        }

        let size_in_bytes = data.len();
        let mut buf = data.clone();
        let restart_interval_raw = buf.get_u16_le();
        let num_entries = buf.get_u32_le() as usize;
        let offsets_size = num_entries * 4;
        if data.len() < 6 + offsets_size {
            return Err(Error::IoError("Block data corrupted".to_string()));
        }

        let data_size = data.len() - 6 - offsets_size;
        let block_data = data.slice(6..6 + data_size);

        let mut offsets = Vec::with_capacity(num_entries);
        let mut offset_buf = data.slice(6 + data_size..);
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

    pub(crate) fn size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }

    fn decode_prefix_entry_bounds(&self, idx: usize) -> Result<(usize, usize, usize, usize)> {
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
        let value_len = u32::from_le_bytes(
            data[offset + 6..offset + 10]
                .try_into()
                .expect("slice length checked"),
        ) as usize;
        let suffix_start = offset + 10;
        let suffix_end = suffix_start + suffix_len;
        if suffix_end > data.len() {
            return Err(Error::IoError("Corrupted prefix key suffix".to_string()));
        }
        let value_start = suffix_end;
        let value_end = value_start + value_len;
        if value_end > data.len() {
            return Err(Error::IoError("Corrupted prefix value bytes".to_string()));
        }
        Ok((suffix_start, suffix_end, value_start, value_end))
    }

    fn decode_prefix_key(&self, idx: usize) -> Result<Bytes> {
        let interval = self.restart_interval.max(1) as usize;
        let restart_idx = idx - (idx % interval);
        let data = self.data.as_ref();
        let mut key = Vec::<u8>::new();
        for entry_idx in restart_idx..=idx {
            let offset = self.offsets[entry_idx] as usize;
            if offset + 10 > data.len() {
                return Err(Error::IoError("Corrupted prefix entry header".to_string()));
            }
            let shared = u16::from_le_bytes(
                data[offset..offset + 2]
                    .try_into()
                    .expect("slice length checked"),
            ) as usize;
            let (suffix_start, suffix_end, _, _) = self.decode_prefix_entry_bounds(entry_idx)?;
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
        }
        Ok(Bytes::from(key))
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
        Self {
            data: BytesMut::new(),
            offsets: Vec::new(),
            target_size,
            prefix_compressed,
            restart_interval: restart_interval.max(1),
            last_key: Vec::new(),
            entries_since_restart: 0,
        }
    }

    pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) {
        let offset = self.data.len() as u32;
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
        6 + self.data.len() + self.offsets.len() * 4
    }

    pub(crate) fn should_finish(&self) -> bool {
        !self.is_empty() && self.estimated_size() >= self.target_size
    }

    pub(crate) fn write_to<W: SequentialWriteFile>(&self, writer: &mut W) -> Result<usize> {
        let size = self.estimated_size();
        let restart_interval = if self.prefix_compressed {
            self.restart_interval.max(2).min(u16::MAX as usize) as u16
        } else {
            1u16
        };
        writer.write(&restart_interval.to_le_bytes())?;
        writer.write(&(self.offsets.len() as u32).to_le_bytes())?;
        let data_bytes = self.data.as_ref();
        if !data_bytes.is_empty() {
            writer.write(data_bytes)?;
        }
        for offset in &self.offsets {
            writer.write(&offset.to_le_bytes())?;
        }
        Ok(size)
    }

    pub(crate) fn build(self) -> Block {
        let size_in_bytes = 6 + self.data.len() + self.offsets.len() * 4;
        Block {
            data: self.data.freeze(),
            offsets: self.offsets,
            block_id: 0,
            size_in_bytes,
            prefix_compressed: self.prefix_compressed,
            restart_interval: self.restart_interval as u32,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.data.clear();
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

    #[test]
    fn test_footer_encode_decode() {
        let footer = Footer::new(100, 200, 300, 400, true, false);
        let encoded = footer.encode();
        assert_eq!(encoded.len(), FOOTER_SIZE);

        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.index_block_offset, 100);
        assert_eq!(decoded.index_block_size, 200);
        assert_eq!(decoded.filter_block_offset, 300);
        assert_eq!(decoded.filter_block_size, 400);
        assert!(decoded.filter_present);
        assert!(!decoded.partitioned_index);
    }

    #[test]
    fn test_footer_contains_version() {
        let footer = Footer::new(10, 20, 30, 40, true, true);
        let encoded = footer.encode();
        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.index_block_offset, 10);
        assert_eq!(decoded.index_block_size, 20);
        assert_eq!(decoded.filter_block_offset, 30);
        assert_eq!(decoded.filter_block_size, 40);
        assert!(decoded.filter_present);
        assert!(decoded.partitioned_index);
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
}
