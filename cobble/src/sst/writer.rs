use crate::cache::{WriterHotBlockCache, bucket_scoped_cache_namespace, data_block_cache_key};
use crate::config::SstReadMetadataCacheMode;
use crate::error::{Error, Result};
use crate::file::{BufferedWriter, File, SequentialWriteFile};
use crate::format::{FileBuildResult, FileBuilder};
use crate::sst::bloom::BloomFilterBuilder;
use crate::sst::compression::{SstCompressionAlgorithm, write_block};
use crate::sst::format::{BlockBuilder, Footer, SstReadMetadata};
use crate::sst::row_codec::{encode_key, encode_value};
use crate::r#type::{Key, KvValue, Value, key_bucket};
use bytes::{BufMut, BytesMut};
use metrics::{Histogram, histogram};

#[derive(Clone)]
struct DataBlockMeta {
    first_key: Vec<u8>,
    offset: u64,
    size: u64,
    key_hashes: Vec<u64>,
}

#[derive(Clone)]
pub(crate) struct SSTWriterMetrics {
    compression_ratio: Histogram,
}

impl SSTWriterMetrics {
    pub(crate) fn new(db_id: &str, compression: SstCompressionAlgorithm) -> Self {
        Self {
            compression_ratio: histogram!(
                "sst_block_compression_ratio",
                "db_id" => db_id.to_string(),
                "compression" => compression.label()
            ),
        }
    }
}

#[derive(Clone)]
pub struct SSTWriterOptions {
    /// Optional metrics handles to reuse across writers.
    pub metrics: Option<SSTWriterMetrics>,
    pub block_size: usize,
    /// Buffered file-output size. This is independent of the SST data-block size.
    pub buffer_size: usize,
    /// Number of columns in the value schema.
    /// Used for encoding values with the row codec.
    pub num_columns: usize,
    /// Enable bloom filter generation for SST files.
    pub bloom_filter_enabled: bool,
    /// Bits per key for bloom filter when enabled.
    pub bloom_bits_per_key: u32,
    /// Enable two-level index/filter blocks.
    pub partitioned_index: bool,
    /// Caching policy for decoded footer and index-partition descriptors.
    pub read_metadata_cache_mode: SstReadMetadataCacheMode,
    /// Number of entries between full restart keys in prefix-compressed data blocks.
    pub data_block_restart_interval: usize,
    /// Compression algorithm for data blocks.
    pub compression: SstCompressionAlgorithm,
    /// Whether encoded values written to this SST include the 4-byte TTL header.
    pub value_has_ttl: bool,
    /// Append a CRC32 trailer to every SST data block.
    pub block_checksum_enabled: bool,
}

impl Default for SSTWriterOptions {
    fn default() -> Self {
        Self {
            metrics: None,
            block_size: 4096,
            buffer_size: 256 * 1024,
            num_columns: 1,
            bloom_filter_enabled: false,
            bloom_bits_per_key: 10,
            partitioned_index: false,
            read_metadata_cache_mode: SstReadMetadataCacheMode::Eager,
            data_block_restart_interval: 16,
            compression: SstCompressionAlgorithm::None,
            value_has_ttl: true,
            block_checksum_enabled: true,
        }
    }
}

/// Writer for creating SST files
pub struct SSTWriter<W: SequentialWriteFile> {
    writer: BufferedWriter<W>,
    options: SSTWriterOptions,
    metrics: SSTWriterMetrics,
    data_block_builder: BlockBuilder,
    bloom_filter_builder: Option<BloomFilterBuilder>,
    first_key: Option<Vec<u8>>,
    last_key: Vec<u8>,
    current_block_first_key: Option<Vec<u8>>,
    pending_data_blocks: Vec<DataBlockMeta>,
    cache_current_data_block: bool,
    hot_block_cache: Option<WriterHotBlockCache>,
    hot_block_cache_namespace: Option<u64>,
    hot_block_output_file_id: Option<u64>,
}

impl<W: SequentialWriteFile> SSTWriter<W> {
    pub fn new(writer: W, options: SSTWriterOptions) -> Self {
        Self::new_with_hot_block_cache(writer, options, None, None, None)
    }

    pub(crate) fn new_with_hot_block_cache(
        writer: W,
        options: SSTWriterOptions,
        hot_block_cache: Option<WriterHotBlockCache>,
        hot_block_cache_namespace: Option<u64>,
        hot_block_output_file_id: Option<u64>,
    ) -> Self {
        let buffered_writer = BufferedWriter::new(writer, options.buffer_size);
        let data_block_builder = BlockBuilder::new_with_prefix(
            options.block_size,
            options.data_block_restart_interval.max(1),
            options.data_block_restart_interval > 1,
        );
        let metrics = options
            .metrics
            .clone()
            .unwrap_or_else(|| SSTWriterMetrics::new("unknown", options.compression));
        let bloom_filter_builder = if options.bloom_filter_enabled {
            Some(BloomFilterBuilder::new(options.bloom_bits_per_key))
        } else {
            None
        };

        Self {
            writer: buffered_writer,
            options,
            metrics,
            data_block_builder,
            bloom_filter_builder,
            first_key: None,
            last_key: Vec::new(),
            current_block_first_key: None,
            pending_data_blocks: Vec::new(),
            cache_current_data_block: false,
            hot_block_cache,
            hot_block_cache_namespace,
            hot_block_output_file_id,
        }
    }

    /// Add a key-value pair to the SST file
    /// Keys must be added in sorted order
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.refresh_hot_block_observation();

        // Ensure keys are added in sorted order
        if !self.last_key.is_empty() && key <= self.last_key.as_slice() {
            return Err(Error::IoError(format!(
                "Keys must be added in sorted order: {:?} <= {:?}",
                key, self.last_key
            )));
        }

        // Track first key of the entire file
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }

        // If this is the first key in the block, remember it for the index
        if self.data_block_builder.is_empty() {
            self.current_block_first_key = Some(key.to_vec());
        }

        let normalized_value = if self.options.value_has_ttl {
            value
        } else {
            if value.len() < 4 {
                return Err(Error::IoError(format!(
                    "Invalid encoded value for no-ttl SST writer: expected >= 4 bytes, got {}",
                    value.len()
                )));
            }
            &value[4..]
        };

        // Add to current data block
        self.data_block_builder.add(key, normalized_value);
        if let Some(builder) = &mut self.bloom_filter_builder {
            builder.add(key);
        }
        self.last_key = key.to_vec();

        // Check if we should finish the current block
        if self.data_block_builder.should_finish() {
            let first_key = self.current_block_first_key.take().unwrap();
            self.finish_data_block(first_key)?;
        }

        Ok(())
    }

    /// Add a typed Key and Value to the SST file.
    /// Uses the row codec to serialize the key and value.
    /// Keys must be added in sorted order (by encoded key bytes).
    ///
    /// # Arguments
    /// * `key` - The typed Key to add
    /// * `value` - The Value containing optional columns
    pub fn add_kv(&mut self, key: &Key, value: &Value) -> Result<()> {
        let encoded_key = encode_key(key);
        let encoded_value = encode_value(value, self.options.num_columns);
        self.add(&encoded_key, &encoded_value)
    }

    fn finish_data_block(&mut self, first_key: Vec<u8>) -> Result<()> {
        if self.data_block_builder.is_empty() {
            return Ok(());
        }

        // Replace the builder with a new one and build the old one
        let old_builder = std::mem::replace(
            &mut self.data_block_builder,
            BlockBuilder::new_with_prefix(
                self.options.block_size,
                self.options.data_block_restart_interval.max(1),
                self.options.data_block_restart_interval > 1,
            ),
        );
        let offset = self.writer.offset();
        let encoded = old_builder.build_encoded();
        let raw_len = encoded.len();

        // Write the block
        let size = write_block(
            &mut self.writer,
            encoded,
            self.options.compression,
            self.options.block_checksum_enabled,
        )?;
        self.record_block_cache_preload_if_hot(&first_key, offset as u64, size);
        self.record_compression_ratio(raw_len, size);
        self.cache_current_data_block = false;

        // Remember block info for index
        let mut key_hashes = Vec::new();
        if let Some(builder) = &mut self.bloom_filter_builder
            && self.options.partitioned_index
        {
            // For partitioned index, we need to collect key hashes per data block
            key_hashes = builder.drain_recent_hashes();
        }
        self.pending_data_blocks.push(DataBlockMeta {
            first_key,
            offset: offset as u64,
            size: size as u64,
            key_hashes,
        });

        Ok(())
    }

    fn refresh_hot_block_observation(&mut self) {
        let Some(cache) = &self.hot_block_cache else {
            return;
        };
        // This is the writer half of the registry handoff. The reader side lives
        // in `SSTIterator::read_data_block_from_partition`; when it observes a
        // scan-hot input block, this counter changes and the current output block
        // is marked for preload in `record_block_cache_preload_if_hot`.
        cache.refresh_observation(&mut self.cache_current_data_block);
    }

    fn record_block_cache_preload_if_hot(&self, first_key: &[u8], offset: u64, size: usize) {
        let Some(cache) = &self.hot_block_cache else {
            return;
        };
        let Some(base_cache_namespace) = self.hot_block_cache_namespace else {
            return;
        };
        let Some(output_file_id) = self.hot_block_output_file_id else {
            return;
        };
        let namespace = key_bucket(first_key)
            .map(|bucket| bucket_scoped_cache_namespace(base_cache_namespace, bucket))
            .unwrap_or(base_cache_namespace);
        cache.push_preload(
            data_block_cache_key(namespace, output_file_id, offset),
            size,
            self.options.block_checksum_enabled,
            self.cache_current_data_block,
        );
    }

    fn record_compression_ratio(&self, raw_len: usize, compressed_len: usize) {
        if raw_len == 0 {
            return;
        }
        let ratio = compressed_len as f64 / raw_len as f64;
        self.metrics.compression_ratio.record(ratio);
    }

    /// Finish writing the SST file and return its key range and read metadata.
    fn finish_internal(mut self) -> Result<FileBuildResult> {
        // Capture first/last keys before finishing
        let first_key = self.first_key.clone().unwrap_or_default();
        let last_key = self.last_key.clone();

        // Finish any pending data block
        if !self.data_block_builder.is_empty() {
            let block_first_key = self.current_block_first_key.take().unwrap_or_default();
            self.finish_data_block(block_first_key)?;
        }

        if !self.options.partitioned_index {
            // Write single-level index
            let mut index_builder = BlockBuilder::new(self.options.block_size);

            for meta in &self.pending_data_blocks {
                let mut value = BytesMut::with_capacity(16);
                value.put_u64_le(meta.offset);
                value.put_u64_le(meta.size);
                index_builder.add(&meta.first_key, &value);
            }

            // Write index block
            let index_offset = self.writer.offset();
            let index_size = index_builder.write_to(&mut self.writer)?;
            // Write bloom filter block if enabled
            let (filter_offset, filter_size, filter_enabled) =
                if let Some(filter_builder) = &mut self.bloom_filter_builder {
                    let filter_offset = self.writer.offset();
                    let filter_size = filter_builder.write_to(&mut self.writer)?;
                    (filter_offset as u64, filter_size as u64, filter_size > 0)
                } else {
                    (0u64, 0u64, false)
                };

            // Write footer
            let footer = Footer {
                index_block_offset: index_offset as u64,
                index_block_size: index_size as u64,
                filter_block_offset: filter_offset,
                filter_block_size: filter_size,
                filter_present: filter_enabled,
                partitioned_index: false,
                value_has_ttl: self.options.value_has_ttl,
                block_checksums: self.options.block_checksum_enabled,
            };
            let read_metadata = SstReadMetadata::from_parts(
                footer.clone(),
                vec![(index_offset as u64, index_size as u64)],
            )?;
            let footer_encoded = footer.encode();
            let meta_bytes = footer_encoded.clone();
            self.writer.write(&footer_encoded)?;

            let file_size = self.writer.offset();
            let mut writer = self.writer;
            writer.close()?;

            let result = FileBuildResult::new(first_key, last_key, file_size, meta_bytes);
            return Ok(if self.options.read_metadata_cache_mode.embeds_on_write() {
                result.with_sst_read_metadata(read_metadata)
            } else {
                result
            });
        }

        // Write two-level index
        let mut index_top_builder = BlockBuilder::new(self.options.block_size);
        let mut filter_index_builder = BlockBuilder::new(self.options.block_size);

        let mut index_builder = BlockBuilder::new(self.options.block_size);
        let mut index_partition_first_key: Option<&Vec<u8>> = None;
        let mut index_partitions = Vec::new();
        // Current bloom filter builder for the partition, maybe optional if bloom filter is disabled
        let mut current_filter_builder = self
            .bloom_filter_builder
            .as_ref()
            .map(|_| BloomFilterBuilder::new(self.options.bloom_bits_per_key));

        // Build index and filter blocks for each data block
        for meta in &self.pending_data_blocks {
            if index_partition_first_key.is_none() {
                index_partition_first_key = Some(&meta.first_key);
            }

            let mut value = BytesMut::with_capacity(16);
            value.put_u64_le(meta.offset);
            value.put_u64_le(meta.size);
            index_builder.add(&meta.first_key, &value);

            if let Some(filter_builder) = &mut current_filter_builder {
                filter_builder.extend_hashes(&meta.key_hashes);
            }

            // Finish one partition if needed
            if index_builder.should_finish() {
                let index_offset = self.writer.offset();
                let index_size = index_builder.write_to(&mut self.writer)?;
                index_partitions.push((index_offset as u64, index_size as u64));
                let first_key = index_partition_first_key
                    .take()
                    .expect("There should be at least one key");
                let mut top_value = BytesMut::with_capacity(16);
                top_value.put_u64_le(index_offset as u64);
                top_value.put_u64_le(index_size as u64);
                index_top_builder.add(first_key, &top_value);
                index_builder.clear();

                if let Some(filter_builder) = &mut current_filter_builder {
                    let filter_offset = self.writer.offset();
                    let filter_size = filter_builder.write_to(&mut self.writer)?;
                    if filter_size > 0 {
                        let mut filter_value = BytesMut::with_capacity(16);
                        filter_value.put_u64_le(filter_offset as u64);
                        filter_value.put_u64_le(filter_size as u64);
                        filter_index_builder.add(first_key, &filter_value);
                    }
                    *filter_builder = BloomFilterBuilder::new(self.options.bloom_bits_per_key);
                }
            }
        }

        // Finish any remaining index partition
        if !index_builder.is_empty() {
            let index_offset = self.writer.offset();
            let index_size = index_builder.write_to(&mut self.writer)?;
            index_partitions.push((index_offset as u64, index_size as u64));
            let first_key = index_partition_first_key
                .take()
                .expect("There should be at least one key");
            let mut top_value = BytesMut::with_capacity(16);
            top_value.put_u64_le(index_offset as u64);
            top_value.put_u64_le(index_size as u64);
            index_top_builder.add(first_key, &top_value);

            if let Some(filter_builder) = &mut current_filter_builder {
                let filter_offset = self.writer.offset();
                let filter_size = filter_builder.write_to(&mut self.writer)?;
                if filter_size > 0 {
                    let mut filter_value = BytesMut::with_capacity(16);
                    filter_value.put_u64_le(filter_offset as u64);
                    filter_value.put_u64_le(filter_size as u64);
                    filter_index_builder.add(first_key, &filter_value);
                }
            }
        }

        // Write filter index block if enabled and not empty
        let filter_enabled =
            self.bloom_filter_builder.is_some() && !filter_index_builder.is_empty();
        let (filter_index_offset, filter_index_size) = if filter_enabled {
            let filter_index_offset = self.writer.offset();
            let filter_index_size = filter_index_builder.write_to(&mut self.writer)?;
            (filter_index_offset, filter_index_size)
        } else {
            (0usize, 0usize)
        };
        // Write top-level index block
        let index_top_offset = self.writer.offset();
        let index_top_size = index_top_builder.write_to(&mut self.writer)?;
        let filter_offset = filter_index_offset as u64;
        let filter_size = filter_index_size as u64;

        // Write footer
        let footer = Footer {
            index_block_offset: index_top_offset as u64,
            index_block_size: index_top_size as u64,
            filter_block_offset: filter_offset,
            filter_block_size: filter_size,
            filter_present: filter_enabled,
            partitioned_index: self.options.partitioned_index,
            value_has_ttl: self.options.value_has_ttl,
            block_checksums: self.options.block_checksum_enabled,
        };
        let read_metadata = SstReadMetadata::from_parts(footer.clone(), index_partitions)?;
        let footer_encoded = footer.encode();
        let meta_bytes = footer_encoded.clone();
        self.writer.write(&footer_encoded)?;

        // Get final file size before closing
        let file_size = self.writer.offset();

        // Flush and close
        let mut writer = self.writer;
        writer.close()?;

        let result = FileBuildResult::new(first_key, last_key, file_size, meta_bytes);
        Ok(if self.options.read_metadata_cache_mode.embeds_on_write() {
            result.with_sst_read_metadata(read_metadata)
        } else {
            result
        })
    }

    /// Finish writing the SST file
    /// This writes the index block and footer
    pub fn finish(self) -> Result<()> {
        self.finish_internal()?;
        Ok(())
    }

    /// Finish writing the SST file and return its immutable build result.
    pub(crate) fn finish_with_range(self) -> Result<FileBuildResult> {
        self.finish_internal()
    }

    /// Returns the current offset (bytes written) in the file.
    pub fn offset(&self) -> usize {
        self.writer.offset()
    }

    /// Returns true if no keys have been added yet.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_none()
    }

    /// Returns the first key added to this file, if any.
    pub fn first_key(&self) -> Option<&[u8]> {
        self.first_key.as_deref()
    }

    /// Returns the last key added to this file.
    pub fn last_key(&self) -> &[u8] {
        &self.last_key
    }
}

/// Implement FileBuilder trait for SSTWriter to support compaction.
impl<W: SequentialWriteFile + 'static> FileBuilder for SSTWriter<W> {
    fn add(&mut self, key: &[u8], value: &KvValue) -> Result<()> {
        match value {
            KvValue::Encoded(bytes) => SSTWriter::add(self, key, bytes),
            KvValue::Decoded(v) => {
                let encoded = encode_value(v, v.columns().len());
                SSTWriter::add(self, key, &encoded)
            }
        }
    }

    fn finish(self: Box<Self>) -> Result<FileBuildResult> {
        (*self).finish_with_range()
    }

    fn offset(&self) -> usize {
        SSTWriter::offset(self)
    }

    fn is_empty(&self) -> bool {
        SSTWriter::is_empty(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FileSystemRegistry;
    use crate::sst::{SSTIterator, SSTIteratorOptions};

    #[test]
    fn test_sst_writer_default_buffer_size() {
        assert_eq!(SSTWriterOptions::default().buffer_size, 256 * 1024);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_writer_basic() {
        let _ = std::fs::remove_dir_all("/tmp/sst_writer_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/sst_writer_test")
            .unwrap();

        let writer_file = fs.open_write("test.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            },
        );

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
            .get_or_register("file:///tmp/sst_writer_test")
            .unwrap();

        let writer_file = fs.open_write("test_order.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            },
        );

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
            .get_or_register("file:///tmp/sst_writer_test")
            .unwrap();

        let writer_file = fs.open_write("test_blocks.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                metrics: None,
                block_size: 100, // Small block size to force multiple blocks
                buffer_size: 8192,
                num_columns: 1,
                bloom_filter_enabled: true,
                bloom_bits_per_key: 10,
                partitioned_index: false,
                read_metadata_cache_mode: SstReadMetadataCacheMode::Eager,
                data_block_restart_interval: 16,
                compression: crate::SstCompressionAlgorithm::None,
                value_has_ttl: true,
                block_checksum_enabled: false,
            },
        );

        for i in 0..20 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}_with_some_extra_data_to_fill_space", i);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }

        writer.finish().unwrap();

        assert!(fs.exists("test_blocks.sst").unwrap());

        let reader_file = fs.open_read("test_blocks.sst").unwrap();
        let mut iter = SSTIterator::new(reader_file, SSTIteratorOptions::default()).unwrap();
        iter.seek_to_first().unwrap();
        for i in 0..20 {
            assert!(iter.valid());
            let key = format!("key{:03}", i);
            let value = format!("value{:03}_with_some_extra_data_to_fill_space", i);
            let (actual_key, actual_value) = iter.current().unwrap().unwrap();
            assert_eq!(actual_key.as_ref(), key.as_bytes());
            assert_eq!(actual_value.as_ref(), value.as_bytes());
            iter.next().unwrap();
        }
        assert!(!iter.valid());

        let _ = std::fs::remove_dir_all("/tmp/sst_writer_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_writer_only_embeds_read_metadata_in_eager_mode() {
        let root = "/tmp/sst_writer_metadata_cache_test";
        let _ = std::fs::remove_dir_all(root);
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(format!("file://{root}")).unwrap();
        for (index, mode) in [
            SstReadMetadataCacheMode::Eager,
            SstReadMetadataCacheMode::Lazy,
            SstReadMetadataCacheMode::Off,
        ]
        .into_iter()
        .enumerate()
        {
            let writer_file = fs.open_write(&format!("test-{index}.sst")).unwrap();
            let mut writer = SSTWriter::new(
                writer_file,
                SSTWriterOptions {
                    read_metadata_cache_mode: mode,
                    ..SSTWriterOptions::default()
                },
            );
            writer.add(b"key", b"value").unwrap();

            let result = writer.finish_with_range().unwrap();
            assert_eq!(result.sst_read_metadata.is_some(), index == 0);
        }

        let _ = std::fs::remove_dir_all(root);
    }
}
