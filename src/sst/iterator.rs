use crate::error::{Error, Result};
use crate::file::RandomAccessFile;
use crate::sst::format::{Block, FOOTER_SIZE, Footer};
use crate::sst::row_codec::{decode_key, decode_value, encode_key};
use crate::r#type::{Key, Value};
use bytes::Bytes;
use std::cmp::Ordering;

pub struct SSTIteratorOptions {
    pub block_cache_size: usize,
    /// Number of columns in the value schema.
    /// Used for decoding values with the row codec.
    pub num_columns: usize,
}

impl Default for SSTIteratorOptions {
    fn default() -> Self {
        Self {
            block_cache_size: 4096,
            num_columns: 1,
        }
    }
}

/// Iterator for reading key-value pairs from an SST file
pub struct SSTIterator {
    file: Box<dyn RandomAccessFile>,
    footer: Footer,
    index_block: Block,
    current_data_block: Option<Block>,
    current_block_idx: usize,
    current_entry_idx: usize,
    options: SSTIteratorOptions,
}

impl SSTIterator {
    pub fn new(file: Box<dyn RandomAccessFile>, options: SSTIteratorOptions) -> Result<Self> {
        // Read footer
        let footer = Self::read_footer(&*file)?;

        // Read index block
        let index_data = file.read_at(
            footer.index_block_offset as usize,
            footer.index_block_size as usize,
        )?;
        let index_block = Block::decode(index_data)?;

        Ok(Self {
            file,
            footer,
            index_block,
            current_data_block: None,
            current_block_idx: 0,
            current_entry_idx: 0,
            options,
        })
    }

    fn read_footer(file: &dyn RandomAccessFile) -> Result<Footer> {
        // Read footer from the end of the file using the file size
        let file_size = file.size();

        if file_size < FOOTER_SIZE {
            return Err(Error::IoError(format!(
                "File too small to contain footer: {} bytes",
                file_size
            )));
        }

        let footer_offset = file_size - FOOTER_SIZE;
        let data = file.read_at(footer_offset, FOOTER_SIZE)?;

        if data.len() != FOOTER_SIZE {
            return Err(Error::IoError(format!(
                "Failed to read complete footer: expected {} bytes, got {}",
                FOOTER_SIZE,
                data.len()
            )));
        }

        Footer::decode(&data)
    }

    /// Seek to the first key >= target
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        // Binary search in index block to find the data block
        let mut left = 0;
        let mut right = self.index_block.len();

        while left < right {
            let mid = (left + right) / 2;
            let (key, _) = self.index_block.get(mid)?;

            match key.as_ref().cmp(target) {
                Ordering::Less => left = mid + 1,
                Ordering::Equal | Ordering::Greater => right = mid,
            }
        }

        // If left == index_block.len(), the target is beyond all keys
        if left == self.index_block.len() {
            if left > 0 {
                // Load the last block and seek within it
                self.current_block_idx = left - 1;
                self.load_data_block(self.current_block_idx)?;
                self.seek_in_current_block(target)?;
            } else {
                // No blocks, iterator is exhausted
                self.current_data_block = None;
            }
        } else {
            // Load the block at left
            self.current_block_idx = left;
            self.load_data_block(left)?;
            self.seek_in_current_block(target)?;
        }

        Ok(())
    }

    fn load_data_block(&mut self, block_idx: usize) -> Result<()> {
        if block_idx >= self.index_block.len() {
            return Err(Error::IoError(format!(
                "Block index out of bounds: {}",
                block_idx
            )));
        }

        let (_, value) = self.index_block.get(block_idx)?;
        // Index block value format: [offset: u64][size: u64]
        if value.len() != 16 {
            return Err(Error::IoError("Invalid index entry".to_string()));
        }

        let offset = u64::from_le_bytes(value[0..8].try_into().unwrap()) as usize;
        let size = u64::from_le_bytes(value[8..16].try_into().unwrap()) as usize;

        let data = self.file.read_at(offset, size)?;
        self.current_data_block = Some(Block::decode(data)?);
        self.current_entry_idx = 0;

        Ok(())
    }

    fn seek_in_current_block(&mut self, target: &[u8]) -> Result<()> {
        if let Some(block) = &self.current_data_block {
            // Linear search in the current block
            for i in 0..block.len() {
                let (key, _) = block.get(i)?;
                if key.as_ref() >= target {
                    self.current_entry_idx = i;
                    return Ok(());
                }
            }
            // Target is beyond all keys in this block
            self.current_entry_idx = block.len();
        }
        Ok(())
    }

    /// Move to the first entry
    pub fn seek_to_first(&mut self) -> Result<()> {
        if self.index_block.is_empty() {
            self.current_data_block = None;
            return Ok(());
        }

        self.current_block_idx = 0;
        self.load_data_block(0)?;
        self.current_entry_idx = 0;
        Ok(())
    }

    /// Get the current key-value pair
    pub fn current(&self) -> Result<Option<(Bytes, Bytes)>> {
        if let Some(block) = &self.current_data_block
            && self.current_entry_idx < block.len()
        {
            let (key, value) = block.get(self.current_entry_idx)?;
            return Ok(Some((key, value)));
        }
        Ok(None)
    }

    /// Get the current key only
    pub fn key(&self) -> Result<Option<Bytes>> {
        if let Some(block) = &self.current_data_block
            && self.current_entry_idx < block.len()
        {
            let (key, _) = block.get(self.current_entry_idx)?;
            return Ok(Some(key));
        }
        Ok(None)
    }

    /// Get the current value only
    pub fn value(&self) -> Result<Option<Bytes>> {
        if let Some(block) = &self.current_data_block
            && self.current_entry_idx < block.len()
        {
            let (_, value) = block.get(self.current_entry_idx)?;
            return Ok(Some(value));
        }
        Ok(None)
    }

    /// Move to the next entry
    pub fn next(&mut self) -> Result<bool> {
        if let Some(block) = &self.current_data_block {
            self.current_entry_idx += 1;

            if self.current_entry_idx >= block.len() {
                // Move to next block
                self.current_block_idx += 1;
                if self.current_block_idx < self.index_block.len() {
                    self.load_data_block(self.current_block_idx)?;
                    self.current_entry_idx = 0;
                    return Ok(true);
                } else {
                    // No more blocks
                    self.current_data_block = None;
                    return Ok(false);
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Check if the iterator is valid (has a current entry)
    pub fn valid(&self) -> bool {
        self.current_data_block.is_some()
            && self
                .current_data_block
                .as_ref()
                .map(|b| self.current_entry_idx < b.len())
                .unwrap_or(false)
    }

    /// Get the current typed Key, decoding from the row codec format.
    pub fn current_key(&self) -> Result<Option<Key>> {
        if let Some(bytes) = self.key()? {
            let key = decode_key(&bytes)?;
            return Ok(Some(key));
        }
        Ok(None)
    }

    /// Get the current typed Value, decoding from the row codec format.
    /// Returns a Value containing optional columns.
    pub fn current_value(&self) -> Result<Option<Value>> {
        if let Some(bytes) = self.value()? {
            let value = decode_value(&bytes, self.options.num_columns)?;
            return Ok(Some(value));
        }
        Ok(None)
    }

    /// Get the current typed Key and Value pair, decoding from the row codec format.
    pub fn current_kv(&self) -> Result<Option<(Key, Value)>> {
        if let Some((key_bytes, value_bytes)) = self.current()? {
            let key = decode_key(&key_bytes)?;
            let value = decode_value(&value_bytes, self.options.num_columns)?;
            return Ok(Some((key, value)));
        }
        Ok(None)
    }

    /// Seek to a typed Key (first key >= target).
    pub fn seek_key(&mut self, target: &Key) -> Result<()> {
        let encoded = encode_key(target);
        self.seek(&encoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FileSystemRegistry;
    use crate::sst::writer::{SSTWriter, SSTWriterOptions};

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_iterator_basic() {
        let _ = std::fs::remove_dir_all("/tmp/sst_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/sst_test".to_string())
            .unwrap();

        // Write SST file
        {
            let writer_file = fs.open_write("test.sst").unwrap();
            let mut writer = SSTWriter::new(writer_file, SSTWriterOptions::default());

            writer.add(b"key1", b"value1").unwrap();
            writer.add(b"key2", b"value2").unwrap();
            writer.add(b"key3", b"value3").unwrap();

            writer.finish().unwrap();
        }

        // Read SST file
        {
            let reader_file = fs.open_read("test.sst").unwrap();
            let mut iter = SSTIterator::new(reader_file, SSTIteratorOptions::default()).unwrap();

            iter.seek_to_first().unwrap();

            let mut count = 0;
            while iter.valid() {
                let (key, value) = iter.current().unwrap().unwrap();
                count += 1;
                match count {
                    1 => {
                        assert_eq!(&key[..], b"key1");
                        assert_eq!(&value[..], b"value1");
                    }
                    2 => {
                        assert_eq!(&key[..], b"key2");
                        assert_eq!(&value[..], b"value2");
                    }
                    3 => {
                        assert_eq!(&key[..], b"key3");
                        assert_eq!(&value[..], b"value3");
                    }
                    _ => panic!("Too many entries"),
                }
                iter.next().unwrap();
            }

            assert_eq!(count, 3);
        }

        let _ = std::fs::remove_dir_all("/tmp/sst_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_iterator_seek() {
        let _ = std::fs::remove_dir_all("/tmp/sst_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/sst_test".to_string())
            .unwrap();

        // Write SST file
        {
            let writer_file = fs.open_write("test_seek.sst").unwrap();
            let mut writer = SSTWriter::new(writer_file, SSTWriterOptions::default());

            writer.add(b"key1", b"value1").unwrap();
            writer.add(b"key3", b"value3").unwrap();
            writer.add(b"key5", b"value5").unwrap();
            writer.add(b"key7", b"value7").unwrap();

            writer.finish().unwrap();
        }

        // Read and seek
        {
            let reader_file = fs.open_read("test_seek.sst").unwrap();
            let mut iter = SSTIterator::new(reader_file, SSTIteratorOptions::default()).unwrap();

            // Seek to exact key
            iter.seek(b"key3").unwrap();
            assert!(iter.valid());
            let (key, value) = iter.current().unwrap().unwrap();
            assert_eq!(&key[..], b"key3");
            assert_eq!(&value[..], b"value3");

            // Seek to key between entries
            iter.seek(b"key4").unwrap();
            assert!(iter.valid());
            let (key, value) = iter.current().unwrap().unwrap();
            assert_eq!(&key[..], b"key5");
            assert_eq!(&value[..], b"value5");

            // Seek to first
            iter.seek(b"key0").unwrap();
            assert!(iter.valid());
            let (key, value) = iter.current().unwrap().unwrap();
            assert_eq!(&key[..], b"key1");
            assert_eq!(&value[..], b"value1");
        }

        let _ = std::fs::remove_dir_all("/tmp/sst_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_typed_kv() {
        use crate::r#type::{Column, Key, Value, ValueType};

        let _ = std::fs::remove_dir_all("/tmp/sst_typed_kv_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/sst_typed_kv_test".to_string())
            .unwrap();

        let num_columns = 2;

        // Write SST file using typed Key/Value API
        {
            let writer_file = fs.open_write("typed.sst").unwrap();
            let mut writer = SSTWriter::new(
                writer_file,
                SSTWriterOptions {
                    num_columns,
                    ..SSTWriterOptions::default()
                },
            );

            let key1 = Key::new(1, b"user:1".to_vec());
            let value1 = Value::new(vec![
                Some(Column::new(ValueType::Put, b"Alice".to_vec())),
                Some(Column::new(ValueType::Put, b"alice@example.com".to_vec())),
            ]);
            writer.add_kv(&key1, &value1).unwrap();

            let key2 = Key::new(1, b"user:2".to_vec());
            // user:2 has no email (optional column)
            let value2 = Value::new(vec![
                Some(Column::new(ValueType::Put, b"Bob".to_vec())),
                None,
            ]);
            writer.add_kv(&key2, &value2).unwrap();

            let key3 = Key::new(2, b"order:100".to_vec());
            let value3 = Value::new(vec![
                Some(Column::new(ValueType::Delete, b"".to_vec())),
                None,
            ]);
            writer.add_kv(&key3, &value3).unwrap();

            writer.finish().unwrap();
        }

        // Read SST file using typed Key/Value API
        {
            let reader_file = fs.open_read("typed.sst").unwrap();
            let mut iter = SSTIterator::new(
                reader_file,
                SSTIteratorOptions {
                    num_columns,
                    ..SSTIteratorOptions::default()
                },
            )
            .unwrap();

            iter.seek_to_first().unwrap();

            // First entry
            assert!(iter.valid());
            let (key, value) = iter.current_kv().unwrap().unwrap();
            let cols = value.columns();
            assert_eq!(key.group(), 1);
            assert_eq!(key.data(), b"user:1");
            assert!(cols[0].is_some());
            assert_eq!(cols[0].as_ref().unwrap().data(), b"Alice");
            assert!(cols[1].is_some());
            assert_eq!(cols[1].as_ref().unwrap().data(), b"alice@example.com");

            // Second entry
            iter.next().unwrap();
            assert!(iter.valid());
            let key = iter.current_key().unwrap().unwrap();
            let value = iter.current_value().unwrap().unwrap();
            let cols = value.columns();
            assert_eq!(key.group(), 1);
            assert_eq!(key.data(), b"user:2");
            assert!(cols[0].is_some());
            assert_eq!(cols[0].as_ref().unwrap().data(), b"Bob");
            assert!(cols[1].is_none());

            // Third entry
            iter.next().unwrap();
            assert!(iter.valid());
            let (key, value) = iter.current_kv().unwrap().unwrap();
            let cols = value.columns();
            assert_eq!(key.group(), 2);
            assert_eq!(key.data(), b"order:100");
            assert!(cols[0].is_some());
            assert!(matches!(
                cols[0].as_ref().unwrap().value_type(),
                ValueType::Delete
            ));

            // No more entries
            iter.next().unwrap();
            assert!(!iter.valid());
        }

        // Test seek_key
        {
            let reader_file = fs.open_read("typed.sst").unwrap();
            let mut iter = SSTIterator::new(
                reader_file,
                SSTIteratorOptions {
                    num_columns,
                    ..SSTIteratorOptions::default()
                },
            )
            .unwrap();

            let target = Key::new(1, b"user:2".to_vec());
            iter.seek_key(&target).unwrap();
            assert!(iter.valid());
            let key = iter.current_key().unwrap().unwrap();
            assert_eq!(key.data(), b"user:2");
        }

        let _ = std::fs::remove_dir_all("/tmp/sst_typed_kv_test");
    }
}
