use crate::error::{Error, Result};
use crate::file::{BufferedWriter, SequentialWriteFile};
use crate::sst::format::{BlockBuilder, Footer};
use bytes::{BufMut, BytesMut};

pub struct SSTWriterOptions {
    pub block_size: usize,
    pub buffer_size: usize,
}

impl Default for SSTWriterOptions {
    fn default() -> Self {
        Self {
            block_size: 4096,
            buffer_size: 8192,
        }
    }
}

/// Writer for creating SST files
pub struct SSTWriter<W: SequentialWriteFile> {
    writer: BufferedWriter<W>,
    options: SSTWriterOptions,
    data_block_builder: BlockBuilder,
    index_block_builder: BlockBuilder,
    last_key: Vec<u8>,
    current_block_first_key: Option<Vec<u8>>,
    pending_data_blocks: Vec<(Vec<u8>, u64, u64)>, // (first_key, offset, size)
}

impl<W: SequentialWriteFile> SSTWriter<W> {
    pub fn new(writer: W, options: SSTWriterOptions) -> Self {
        let buffered_writer = BufferedWriter::new(writer, options.buffer_size);
        let data_block_builder = BlockBuilder::new(options.block_size);
        let index_block_builder = BlockBuilder::new(options.block_size);

        Self {
            writer: buffered_writer,
            options,
            data_block_builder,
            index_block_builder,
            last_key: Vec::new(),
            current_block_first_key: None,
            pending_data_blocks: Vec::new(),
        }
    }

    /// Add a key-value pair to the SST file
    /// Keys must be added in sorted order
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // Ensure keys are added in sorted order
        if !self.last_key.is_empty() && key <= self.last_key.as_slice() {
            return Err(Error::IoError(format!(
                "Keys must be added in sorted order: {:?} <= {:?}",
                key, self.last_key
            )));
        }

        // If this is the first key in the block, remember it for the index
        if self.data_block_builder.is_empty() {
            self.current_block_first_key = Some(key.to_vec());
        }

        // Add to current data block
        self.data_block_builder.add(key, value);
        self.last_key = key.to_vec();

        // Check if we should finish the current block
        if self.data_block_builder.should_finish() {
            let first_key = self.current_block_first_key.take().unwrap();
            self.finish_data_block(first_key)?;
        }

        Ok(())
    }

    fn finish_data_block(&mut self, first_key: Vec<u8>) -> Result<()> {
        if self.data_block_builder.is_empty() {
            return Ok(());
        }

        // Replace the builder with a new one and build the old one
        let old_builder = std::mem::replace(
            &mut self.data_block_builder,
            BlockBuilder::new(self.options.block_size),
        );
        let block = old_builder.build();
        let encoded = block.encode();
        let size = encoded.len();
        let offset = self.writer.offset();

        // Write the block
        self.writer.write(&encoded)?;

        // Remember block info for index
        self.pending_data_blocks
            .push((first_key, offset as u64, size as u64));

        Ok(())
    }

    /// Finish writing the SST file
    /// This writes the index block and footer
    pub fn finish(mut self) -> Result<()> {
        // Finish any pending data block
        if !self.data_block_builder.is_empty() {
            let first_key = self.current_block_first_key.take().unwrap_or_default();
            self.finish_data_block(first_key)?;
        }

        // Build index block
        for (first_key, offset, size) in &self.pending_data_blocks {
            let mut value = BytesMut::with_capacity(16);
            value.put_u64_le(*offset);
            value.put_u64_le(*size);
            self.index_block_builder.add(first_key, &value);
        }

        let index_builder = std::mem::replace(
            &mut self.index_block_builder,
            BlockBuilder::new(self.options.block_size),
        );
        let index_block = index_builder.build();
        let index_encoded = index_block.encode();
        let index_offset = self.writer.offset();
        let index_size = index_encoded.len();

        // Write index block
        self.writer.write(&index_encoded)?;

        // Write footer
        let footer = Footer::new(index_offset as u64, index_size as u64);
        let footer_encoded = footer.encode();
        self.writer.write(&footer_encoded)?;

        // Flush and close
        self.writer.close()
    }

    pub fn offset(&self) -> usize {
        self.writer.offset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FileSystemRegistry;

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_writer_basic() {
        let _ = std::fs::remove_dir_all("/tmp/sst_writer_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/sst_writer_test".to_string())
            .unwrap();

        let writer_file = fs.open_write("test.sst").unwrap();
        let mut writer = SSTWriter::new(writer_file, SSTWriterOptions::default());

        writer.add(b"key1", b"value1").unwrap();
        writer.add(b"key2", b"value2").unwrap();
        writer.add(b"key3", b"value3").unwrap();

        writer.finish().unwrap();

        assert!(fs.exists("test.sst").unwrap());

        let _ = std::fs::remove_dir_all("/tmp/sst_writer_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_writer_sorted_keys() {
        let _ = std::fs::remove_dir_all("/tmp/sst_writer_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/sst_writer_test".to_string())
            .unwrap();

        let writer_file = fs.open_write("test_order.sst").unwrap();
        let mut writer = SSTWriter::new(writer_file, SSTWriterOptions::default());

        writer.add(b"key1", b"value1").unwrap();

        // Try to add a key out of order
        let result = writer.add(b"key0", b"value0");
        assert!(result.is_err());

        let _ = std::fs::remove_dir_all("/tmp/sst_writer_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_writer_multiple_blocks() {
        let _ = std::fs::remove_dir_all("/tmp/sst_writer_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/sst_writer_test".to_string())
            .unwrap();

        let writer_file = fs.open_write("test_blocks.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                block_size: 100, // Small block size to force multiple blocks
                buffer_size: 8192,
            },
        );

        for i in 0..20 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}_with_some_extra_data_to_fill_space", i);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }

        writer.finish().unwrap();

        assert!(fs.exists("test_blocks.sst").unwrap());

        let _ = std::fs::remove_dir_all("/tmp/sst_writer_test");
    }
}
