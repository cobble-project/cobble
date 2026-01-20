use crate::error::{Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Magic number at the end of SST file for validation
pub const SST_FILE_MAGIC: u32 = 0x53535431; // "SST1"

/// Footer structure at the end of SST file
/// Layout: [index_block_offset: u64][index_block_size: u64][magic: u32]
pub const FOOTER_SIZE: usize = 20; // 8 + 8 + 4

#[derive(Debug, Clone)]
pub struct Footer {
    pub index_block_offset: u64,
    pub index_block_size: u64,
}

impl Footer {
    pub fn new(index_block_offset: u64, index_block_size: u64) -> Self {
        Self {
            index_block_offset,
            index_block_size,
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(FOOTER_SIZE);
        buf.put_u64_le(self.index_block_offset);
        buf.put_u64_le(self.index_block_size);
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
}

impl Block {
    pub fn new(data: Bytes, offsets: Vec<u32>) -> Self {
        Self { data, offsets }
    }

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

        let mut buf = data.clone();
        let num_entries = buf.get_u32_le() as usize;

        if num_entries == 0 {
            return Ok(Self {
                data: Bytes::new(),
                offsets: vec![],
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
        })
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

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
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

    pub fn build(self) -> Block {
        Block {
            data: self.data.freeze(),
            offsets: self.offsets,
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
        let footer = Footer::new(100, 200);
        let encoded = footer.encode();
        assert_eq!(encoded.len(), FOOTER_SIZE);

        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.index_block_offset, 100);
        assert_eq!(decoded.index_block_size, 200);
    }

    #[test]
    fn test_block_encode_decode() {
        let mut builder = BlockBuilder::new(4096);
        builder.add(b"key1", b"value1");
        builder.add(b"key2", b"value2");
        builder.add(b"key3", b"value3");

        let block = builder.build();
        assert_eq!(block.len(), 3);

        let encoded = block.encode();
        let decoded = Block::decode(encoded).unwrap();

        assert_eq!(decoded.len(), 3);

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
