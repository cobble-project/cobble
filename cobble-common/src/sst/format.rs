use crate::error::{Error, Result};
use crate::file::SequentialWriteFile;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Magic number at the end of SST file for validation
pub const SST_FILE_MAGIC: u32 = 0x53535431; // "SST1"

/// Footer structure at the end of SST file
/// Layout: [index_block_offset: u64][index_block_size: u64]
///         [filter_block_offset: u64][filter_block_size: u64]
///         [flags: u32][magic: u32]
pub const FOOTER_SIZE: usize = 40; // 8 + 8 + 8 + 8 + 4 + 4

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
    pub fn new(
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

    pub fn encode(&self) -> Bytes {
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
        buf.put_u32_le(SST_FILE_MAGIC);
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
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
        let magic = buf.get_u32_le();

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
/// Layout: [num_entries: u32][entries...][offsets: u32 * num_entries]
/// Each entry: [key_len: u32][key][value_len: u32][value]
#[derive(Debug, Clone)]
pub struct Block {
    data: Bytes,
    offsets: Vec<u32>,
    block_id: u32,
    size_in_bytes: usize,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let offsets_size = self.offsets.len() * 4;
        let total_size = 4 + self.data.len() + offsets_size;
        let mut buf = BytesMut::with_capacity(total_size);

        // Write number of entries
        buf.put_u32_le(self.offsets.len() as u32);

        // Write data
        buf.put_slice(&self.data);

        // Write offsets
        for offset in &self.offsets {
            buf.put_u32_le(*offset);
        }

        buf.freeze()
    }

    pub fn decode(data: Bytes) -> Result<Self> {
        if data.len() < 4 {
            return Err(Error::IoError("Block too small".to_string()));
        }

        let size_in_bytes = data.len();
        let mut buf = data.clone();
        let num_entries = buf.get_u32_le() as usize;

        if num_entries == 0 {
            return Ok(Self {
                data: Bytes::new(),
                offsets: vec![],
                block_id: 0,
                size_in_bytes,
            });
        }

        let offsets_size = num_entries * 4;
        if data.len() < 4 + offsets_size {
            return Err(Error::IoError("Block data corrupted".to_string()));
        }

        let data_size = data.len() - 4 - offsets_size;
        let block_data = data.slice(4..4 + data_size);

        let mut offsets = Vec::with_capacity(num_entries);
        let mut offset_buf = data.slice(4 + data_size..);
        for _ in 0..num_entries {
            offsets.push(offset_buf.get_u32_le());
        }

        Ok(Self {
            data: block_data,
            offsets,
            block_id: 0,
            size_in_bytes,
        })
    }

    pub fn set_block_id(&mut self, block_id: u32) {
        self.block_id = block_id;
    }

    pub fn block_id(&self) -> u32 {
        self.block_id
    }

    pub fn get(&self, idx: usize) -> Result<(Bytes, Bytes)> {
        if idx >= self.offsets.len() {
            return Err(Error::IoError(format!(
                "Index out of bounds: {} >= {}",
                idx,
                self.offsets.len()
            )));
        }

        let offset = self.offsets[idx] as usize;
        let mut buf = self.data.slice(offset..);

        if buf.remaining() < 4 {
            return Err(Error::IoError("Corrupted block entry".to_string()));
        }

        let key_len = buf.get_u32_le() as usize;
        if buf.remaining() < key_len {
            return Err(Error::IoError("Corrupted key data".to_string()));
        }

        let key = buf.copy_to_bytes(key_len);

        if buf.remaining() < 4 {
            return Err(Error::IoError("Corrupted value length".to_string()));
        }

        let value_len = buf.get_u32_le() as usize;
        if buf.remaining() < value_len {
            return Err(Error::IoError("Corrupted value data".to_string()));
        }

        let value = buf.copy_to_bytes(value_len);

        Ok((key, value))
    }

    pub fn offsets_len(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    pub(crate) fn lower_bound(&self, target: &[u8]) -> Result<usize> {
        let mut left = 0;
        let mut right = self.offsets_len();
        while left < right {
            let mid = (left + right) / 2;
            let (key, _) = self.get(mid)?;
            match key.as_ref().cmp(target) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => right = mid,
            }
        }
        Ok(left)
    }

    pub(crate) fn valid_lower_bound(&self, target: &[u8]) -> Result<usize> {
        let left = self.lower_bound(target)?;
        if left == self.offsets_len() {
            Ok(left.saturating_sub(1))
        } else {
            Ok(left)
        }
    }

    pub(crate) fn size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }
}

/// Builder for creating blocks
pub struct BlockBuilder {
    data: BytesMut,
    offsets: Vec<u32>,
    target_size: usize,
}

impl BlockBuilder {
    pub fn new(target_size: usize) -> Self {
        Self {
            data: BytesMut::new(),
            offsets: Vec::new(),
            target_size,
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let offset = self.data.len() as u32;
        self.offsets.push(offset);

        self.data.put_u32_le(key.len() as u32);
        self.data.put_slice(key);
        self.data.put_u32_le(value.len() as u32);
        self.data.put_slice(value);
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    pub fn estimated_size(&self) -> usize {
        4 + self.data.len() + self.offsets.len() * 4
    }

    pub fn should_finish(&self) -> bool {
        !self.is_empty() && self.estimated_size() >= self.target_size
    }

    pub fn write_to<W: SequentialWriteFile>(&self, writer: &mut W) -> Result<usize> {
        let size = self.estimated_size();
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

    pub fn build(self) -> Block {
        let size_in_bytes = 4 + self.data.len() + self.offsets.len() * 4;
        Block {
            data: self.data.freeze(),
            offsets: self.offsets,
            block_id: 0,
            size_in_bytes,
        }
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.offsets.clear();
    }
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
}
