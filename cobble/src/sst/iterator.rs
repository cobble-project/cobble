use crate::block_cache::{BlockCache, BlockCacheKey, BlockCacheKind, CachedBlock};
use crate::data_file::DataFile;
use crate::error::{Error, Result};
use crate::file::RandomAccessFile;
use crate::iterator::KvIterator;
use crate::sst::bloom::BloomFilter;
use crate::sst::compression::decode_block_bytes;
use crate::sst::format::{Block, FOOTER_SIZE, Footer};
use crate::sst::row_codec::{decode_key, decode_value, encode_key};
use crate::r#type::{Key, KvValue, Value};
use crate::util::unsafe_bytes;
use bytes::{BufMut, Bytes, BytesMut};
use metrics::{Counter, counter};
use std::cell::{Cell, RefCell};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct SSTIteratorOptions {
    /// Optional metrics handles to reuse across iterators.
    pub metrics: Option<Arc<SSTIteratorMetrics>>,
    /// Size of the block cache in bytes.
    /// If zero, block caching is disabled.
    pub block_cache_size: usize,
    /// Number of columns in the value schema.
    /// Used for decoding values with the row codec.
    pub num_columns: usize,
    /// Whether to use bloom filter for point lookups.
    pub bloom_filter_enabled: bool,
}

#[derive(Clone)]
pub(crate) struct SSTIteratorMetrics {
    index_hits: Counter,
    index_misses: Counter,
    data_hits: Counter,
    data_misses: Counter,
    filter_hits: Counter,
    filter_misses: Counter,
}

impl SSTIteratorMetrics {
    pub(crate) fn new(db_id: &str) -> Self {
        let db_id = db_id.to_string();
        Self {
            index_hits: counter!(
                "block_cache_hits_total",
                "file" => "sst",
                "kind" => "index",
                "db_id" => db_id.clone()
            ),
            index_misses: counter!(
                "block_cache_misses_total",
                "file" => "sst",
                "kind" => "index",
                "db_id" => db_id.clone()
            ),
            data_hits: counter!(
                "block_cache_hits_total",
                "file" => "sst",
                "kind" => "data",
                "db_id" => db_id.clone()
            ),
            data_misses: counter!(
                "block_cache_misses_total",
                "file" => "sst",
                "kind" => "data",
                "db_id" => db_id.clone()
            ),
            filter_hits: counter!(
                "block_cache_hits_total",
                "file" => "sst",
                "kind" => "filter",
                "db_id" => db_id.clone()
            ),
            filter_misses: counter!(
                "block_cache_misses_total",
                "file" => "sst",
                "kind" => "filter",
                "db_id" => db_id
            ),
        }
    }
}

#[cfg(test)]
pub struct SSTIteratorTestCache {
    inner: SSTIterator,
}

impl Default for SSTIteratorOptions {
    fn default() -> Self {
        Self {
            block_cache_size: 64 * 1024 * 1024, // 64 MB
            num_columns: 1,
            metrics: None,
            bloom_filter_enabled: false,
        }
    }
}

/// Iterator for reading key-value pairs from an SST file
pub(crate) struct SSTIterator {
    file: Box<dyn RandomAccessFile>,
    file_id: u64,
    footer: Footer,
    index_block: Arc<Block>,
    index_partitions: Vec<(u64, u64)>,
    bloom_filter: Option<Arc<BloomFilter>>,
    bloom_filter_partition_idx: Option<usize>,
    current_data_block: Option<Arc<Block>>,
    current_index_partition_idx: usize,
    current_index_partition: Option<Arc<Block>>,
    current_block_idx: usize,
    current_entry_idx: usize,
    options: SSTIteratorOptions,
    block_cache: Option<BlockCache>,
    metrics: Arc<SSTIteratorMetrics>,
    cache_valid: Cell<bool>,
    cached_entry_idx: Cell<Option<usize>>,
    // Use RefCell to allow interior mutability for cached bytes, which can be shared as slices.
    cached_key_bytes: RefCell<Option<Bytes>>,
    cached_value_bytes: RefCell<Option<Bytes>>,
}

impl SSTIterator {
    #[inline]
    fn normalized_encoded_value(&self, value: Bytes) -> Bytes {
        if self.footer.value_has_ttl {
            return value;
        }
        let mut out = BytesMut::with_capacity(value.len() + 4);
        out.put_u32_le(0);
        // todo: avoid copy when ttl is null
        out.extend_from_slice(value.as_ref());
        out.freeze()
    }

    #[cfg(test)]
    pub(crate) fn new(
        file: Box<dyn RandomAccessFile>,
        options: SSTIteratorOptions,
    ) -> Result<Self> {
        Self::with_file_id(file, 0, options)
    }

    #[cfg(test)]
    pub(crate) fn with_file_id(
        file: Box<dyn RandomAccessFile>,
        file_id: u64,
        options: SSTIteratorOptions,
    ) -> Result<Self> {
        Self::with_cache(file, file_id, options, None, None)
    }

    #[cfg(test)]
    pub(crate) fn with_cache(
        file: Box<dyn RandomAccessFile>,
        file_id: u64,
        options: SSTIteratorOptions,
        block_cache: Option<BlockCache>,
        footer_bytes: Option<Bytes>,
    ) -> Result<Self> {
        let (iter, _) =
            Self::with_cache_and_footer_bytes(file, file_id, options, block_cache, footer_bytes)?;
        Ok(iter)
    }

    pub(crate) fn with_cache_and_file(
        file: Box<dyn RandomAccessFile>,
        data_file: &DataFile,
        options: SSTIteratorOptions,
        block_cache: Option<BlockCache>,
    ) -> Result<Self> {
        let footer_bytes = data_file.meta_bytes();
        let (iter, cached_footer) = Self::with_cache_and_footer_bytes(
            file,
            data_file.file_id,
            options,
            block_cache,
            footer_bytes,
        )?;
        if let Some(bytes) = cached_footer {
            data_file.set_meta_bytes(bytes);
        }
        Ok(iter)
    }

    fn with_cache_and_footer_bytes(
        file: Box<dyn RandomAccessFile>,
        file_id: u64,
        options: SSTIteratorOptions,
        block_cache: Option<BlockCache>,
        footer_bytes: Option<Bytes>,
    ) -> Result<(Self, Option<Bytes>)> {
        let metrics = options
            .metrics
            .clone()
            .unwrap_or_else(|| Arc::new(SSTIteratorMetrics::new("unknown")));
        // Read footer
        let (footer, cached_footer) = if let Some(bytes) = footer_bytes {
            (Footer::decode(bytes.as_ref())?, None)
        } else {
            let bytes = Self::read_footer_bytes(&*file)?;
            let footer = Footer::decode(bytes.as_ref())?;
            (footer, Some(bytes))
        };

        // Read index block
        let index_block = if let Some(cache) = &block_cache {
            let cache_key = BlockCacheKey {
                file_id,
                block_id: footer.index_block_offset,
                kind: BlockCacheKind::IndexTop,
            };
            if let Some(cached) = cache.get(&cache_key) {
                metrics.index_hits.increment(1);
                match cached {
                    CachedBlock::Block(block) => block,
                    CachedBlock::BloomFilter(_) => {
                        return Err(Error::IoError(
                            "Index block cache entry invalid".to_string(),
                        ));
                    }
                    CachedBlock::ParquetBlock(_) => {
                        return Err(Error::IoError(
                            "Index block cache entry invalid".to_string(),
                        ));
                    }
                }
            } else {
                metrics.index_misses.increment(1);
                let index_data = file.read_at(
                    footer.index_block_offset as usize,
                    footer.index_block_size as usize,
                )?;
                let mut index_block = Block::decode(index_data)?;
                index_block.set_block_id(u32::MAX);
                let index_block = Arc::new(index_block);
                cache.insert(cache_key, CachedBlock::Block(index_block.clone()));
                index_block
            }
        } else {
            let index_data = file.read_at(
                footer.index_block_offset as usize,
                footer.index_block_size as usize,
            )?;
            let mut index_block = Block::decode(index_data)?;
            index_block.set_block_id(u32::MAX);
            Arc::new(index_block)
        };
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
        Ok((
            Self {
                file,
                file_id,
                footer,
                index_block,
                index_partitions,
                bloom_filter: None,
                bloom_filter_partition_idx: None,
                current_data_block: None,
                current_index_partition_idx: 0,
                current_index_partition: None,
                current_block_idx: 0,
                current_entry_idx: 0,
                options,
                block_cache,
                metrics,
                cache_valid: Cell::new(false),
                cached_entry_idx: Cell::new(None),
                cached_key_bytes: RefCell::new(None),
                cached_value_bytes: RefCell::new(None),
            },
            cached_footer,
        ))
    }

    #[cfg(test)]
    pub fn with_cache_test(
        file: Box<dyn RandomAccessFile>,
        file_id: u64,
        options: SSTIteratorOptions,
        block_cache: BlockCache,
    ) -> Result<SSTIteratorTestCache> {
        let inner = Self::with_cache(file, file_id, options, Some(block_cache), None)?;
        Ok(SSTIteratorTestCache { inner })
    }

    fn read_footer_bytes(file: &dyn RandomAccessFile) -> Result<Bytes> {
        // Read footer from the end of the file using the file size
        let file_size = file.size();

        if file_size < FOOTER_SIZE {
            return Err(Error::IoError(format!(
                "File too small to contain footer: {} bytes",
                file_size
            )));
        }

        let footer_offset = file_size - FOOTER_SIZE;
        file.read_at(footer_offset, FOOTER_SIZE)
    }

    /// Seek to the first key >= target
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        let target = unsafe_bytes(target);
        if self.index_partitions.is_empty() {
            self.current_data_block = None;
            self.clear_cached_entry();
            return Ok(());
        }
        if !self.footer.partitioned_index {
            if self.index_block.is_empty() {
                self.current_data_block = None;
                self.clear_cached_entry();
                return Ok(());
            }
            self.current_index_partition_idx = 0;
            self.current_index_partition = Some(self.index_block.clone());
            let block_idx = self.index_block.find_lower_or_equal_idx(&target)?;
            self.current_block_idx = block_idx;
            let partition = self.index_block.clone();
            self.load_data_block_from_partition(&partition, block_idx)?;
            self.seek_in_current_block(&target)?;
            return Ok(());
        }

        let partition_idx = self.index_block.find_lower_or_equal_idx(&target)?;
        let partition = self.load_index_partition(partition_idx)?;
        let block_idx = partition.find_lower_or_equal_idx(&target)?;
        self.current_block_idx = block_idx;
        self.load_data_block_from_partition(&partition, block_idx)?;
        self.seek_in_current_block(&target)?;
        Ok(())
    }

    pub(crate) fn may_contain(&mut self, key: &[u8]) -> Result<bool> {
        if !self.options.bloom_filter_enabled
            || !self.footer.filter_present
            || self.footer.filter_block_size == 0
        {
            return Ok(true);
        }
        if self.index_partitions.is_empty() {
            return Ok(true);
        }
        let partition_idx = if self.footer.partitioned_index {
            if self.index_block.is_empty() {
                return Ok(true);
            }
            let key = unsafe_bytes(key);
            self.index_block.find_lower_or_equal_idx(&key)?
        } else {
            0
        };
        self.ensure_bloom_filter_loaded(partition_idx)?;
        Ok(self
            .bloom_filter
            .as_ref()
            .is_some_and(|filter| filter.may_contain(key)))
    }

    fn ensure_bloom_filter_loaded(&mut self, partition_idx: usize) -> Result<()> {
        if self.bloom_filter_partition_idx == Some(partition_idx) {
            return Ok(());
        }
        let filter = self.load_filter_partition(partition_idx)?;
        self.bloom_filter = Some(filter);
        self.bloom_filter_partition_idx = Some(partition_idx);
        Ok(())
    }

    fn load_index_partition(&mut self, partition_idx: usize) -> Result<Arc<Block>> {
        if partition_idx >= self.index_partitions.len() {
            return Err(Error::IoError(format!(
                "Index partition out of bounds: {}",
                partition_idx
            )));
        }
        if let Some(block) = self.current_index_partition.as_ref()
            && self.current_index_partition_idx == partition_idx
        {
            return Ok(block.clone());
        }
        let (offset, size) = self.index_partitions[partition_idx];
        let cache_key = BlockCacheKey {
            file_id: self.file_id,
            block_id: offset,
            kind: if self.footer.partitioned_index {
                BlockCacheKind::IndexPartition
            } else {
                BlockCacheKind::IndexTop
            },
        };
        let block = if let Some(cache) = &self.block_cache {
            if let Some(cached) = cache.get(&cache_key) {
                self.metrics.index_hits.increment(1);
                match cached {
                    CachedBlock::Block(block) => block,
                    CachedBlock::BloomFilter(_) => {
                        return Err(Error::IoError("Index partition cache invalid".to_string()));
                    }
                    CachedBlock::ParquetBlock(_) => {
                        return Err(Error::IoError("Index partition cache invalid".to_string()));
                    }
                }
            } else {
                self.metrics.index_misses.increment(1);
                let data = self.file.read_at(offset as usize, size as usize)?;
                let mut block = Block::decode(data)?;
                block.set_block_id(partition_idx as u32);
                let block = Arc::new(block);
                cache.insert(cache_key, CachedBlock::Block(block.clone()));
                block
            }
        } else {
            let data = self.file.read_at(offset as usize, size as usize)?;
            let mut block = Block::decode(data)?;
            block.set_block_id(partition_idx as u32);
            Arc::new(block)
        };
        self.current_index_partition_idx = partition_idx;
        self.current_index_partition = Some(block.clone());
        Ok(block)
    }

    fn load_data_block_from_partition(
        &mut self,
        partition: &Arc<Block>,
        block_idx: usize,
    ) -> Result<()> {
        if block_idx >= partition.offsets_len() {
            return Err(Error::IoError(format!(
                "Block index out of bounds: {}",
                block_idx
            )));
        }

        let value = partition.value(block_idx)?;
        if value.len() != 16 {
            return Err(Error::IoError("Invalid index entry".to_string()));
        }

        let offset = u64::from_le_bytes(value[0..8].try_into().unwrap()) as usize;
        let size = u64::from_le_bytes(value[8..16].try_into().unwrap()) as usize;
        if size == 0 {
            return Err(Error::IoError("Data block size is zero".to_string()));
        }

        let cache_key = BlockCacheKey {
            file_id: self.file_id,
            block_id: offset as u64,
            kind: BlockCacheKind::Data,
        };
        let block = if let Some(cache) = &self.block_cache {
            if let Some(cached) = cache.get(&cache_key) {
                self.metrics.data_hits.increment(1);
                match cached {
                    CachedBlock::Block(block) => block,
                    CachedBlock::BloomFilter(_) => {
                        return Err(Error::IoError("Block cache entry invalid".to_string()));
                    }
                    CachedBlock::ParquetBlock(_) => {
                        return Err(Error::IoError("Block cache entry invalid".to_string()));
                    }
                }
            } else {
                self.metrics.data_misses.increment(1);
                let data = self.file.read_at(offset, size)?;
                let decoded = decode_block_bytes(data)?;
                let mut block = Block::decode(decoded)?;
                block.set_block_id(block_idx as u32);
                let block = Arc::new(block);
                cache.insert(cache_key, CachedBlock::Block(block.clone()));
                block
            }
        } else {
            let data = self.file.read_at(offset, size)?;
            let decoded = decode_block_bytes(data)?;
            let mut block = Block::decode(decoded)?;
            block.set_block_id(block_idx as u32);
            Arc::new(block)
        };
        self.current_data_block = Some(block);
        self.current_entry_idx = 0;
        self.clear_cached_entry();

        Ok(())
    }

    /// Load the filter index block.
    /// Used for partitioned filter index.
    fn load_filter_index(&mut self) -> Result<Arc<Block>> {
        let cache_key = BlockCacheKey {
            file_id: self.file_id,
            block_id: self.footer.filter_block_offset,
            kind: BlockCacheKind::FilterIndex,
        };
        let block = if let Some(cache) = &self.block_cache {
            if let Some(cached) = cache.get(&cache_key) {
                self.metrics.filter_hits.increment(1);
                match cached {
                    CachedBlock::Block(block) => block,
                    CachedBlock::BloomFilter(_) => {
                        return Err(Error::IoError("Filter index cache invalid".to_string()));
                    }
                    CachedBlock::ParquetBlock(_) => {
                        return Err(Error::IoError("Filter index cache invalid".to_string()));
                    }
                }
            } else {
                self.metrics.filter_misses.increment(1);
                let data = self.file.read_at(
                    self.footer.filter_block_offset as usize,
                    self.footer.filter_block_size as usize,
                )?;
                let mut block = Block::decode(data)?;
                block.set_block_id(u32::MAX - 1);
                let block = Arc::new(block);
                cache.insert(cache_key, CachedBlock::Block(block.clone()));
                block
            }
        } else {
            let data = self.file.read_at(
                self.footer.filter_block_offset as usize,
                self.footer.filter_block_size as usize,
            )?;
            let mut block = Block::decode(data)?;
            block.set_block_id(u32::MAX - 1);
            Arc::new(block)
        };
        Ok(block)
    }

    /// Load the bloom filter for the given partition index.
    /// If the SST file does not use partitioned filters, the same filter is returned for any partition index.
    fn load_filter_partition(&mut self, partition_idx: usize) -> Result<Arc<BloomFilter>> {
        if self.footer.partitioned_index {
            let filter_index = self.load_filter_index()?;
            if partition_idx >= filter_index.offsets_len() {
                return Err(Error::IoError(format!(
                    "Filter partition out of bounds: {}",
                    partition_idx
                )));
            }
            let value = filter_index.value(partition_idx)?;
            if value.len() != 16 {
                return Err(Error::IoError("Invalid filter index entry".to_string()));
            }
            let offset = u64::from_le_bytes(value[0..8].try_into().unwrap()) as usize;
            let size = u64::from_le_bytes(value[8..16].try_into().unwrap()) as usize;
            if size == 0 {
                return Err(Error::IoError("Filter partition size is zero".to_string()));
            }
            let cache_key = BlockCacheKey {
                file_id: self.file_id,
                block_id: offset as u64,
                kind: BlockCacheKind::FilterPartition,
            };
            return self.load_filter(cache_key, offset, size);
        }

        let offset = self.footer.filter_block_offset as usize;
        let size = self.footer.filter_block_size as usize;
        if size == 0 {
            return Err(Error::IoError("Filter block size is zero".to_string()));
        }
        let cache_key = BlockCacheKey {
            file_id: self.file_id,
            block_id: self.footer.filter_block_offset,
            kind: BlockCacheKind::FilterPartition,
        };
        self.load_filter(cache_key, offset, size)
    }

    /// Load bloom filter block from file or cache.
    fn load_filter(
        &self,
        cache_key: BlockCacheKey,
        offset: usize,
        size: usize,
    ) -> Result<Arc<BloomFilter>> {
        let filter = if let Some(cache) = &self.block_cache {
            if let Some(cached) = cache.get(&cache_key) {
                self.metrics.filter_hits.increment(1);
                match cached {
                    CachedBlock::BloomFilter(filter) => filter,
                    CachedBlock::Block(_) => {
                        return Err(Error::IoError("Filter cache entry invalid".to_string()));
                    }
                    CachedBlock::ParquetBlock(_) => {
                        return Err(Error::IoError("Filter cache entry invalid".to_string()));
                    }
                }
            } else {
                self.metrics.filter_misses.increment(1);
                let filter_data = self.file.read_at(offset, size)?;
                let filter = Arc::new(BloomFilter::decode(filter_data)?);
                cache.insert(cache_key, CachedBlock::BloomFilter(filter.clone()));
                filter
            }
        } else {
            let filter_data = self.file.read_at(offset, size)?;
            Arc::new(BloomFilter::decode(filter_data)?)
        };
        Ok(filter)
    }

    fn clear_cached_entry(&self) {
        self.cache_valid.set(false);
        self.cached_entry_idx.set(None);
        *self.cached_key_bytes.borrow_mut() = None;
        *self.cached_value_bytes.borrow_mut() = None;
    }

    fn ensure_cached_bytes(&self) -> Result<()> {
        if self.cache_valid.get() {
            return Ok(());
        }
        if let Some(block) = &self.current_data_block
            && self.current_entry_idx < block.offsets_len()
        {
            let (key, value) = block.get(self.current_entry_idx)?;
            self.cached_entry_idx.set(Some(self.current_entry_idx));
            *self.cached_key_bytes.borrow_mut() = Some(key);
            *self.cached_value_bytes.borrow_mut() = Some(value);
            self.cache_valid.set(true);
            return Ok(());
        }
        self.clear_cached_entry();
        Ok(())
    }

    fn seek_in_current_block(&mut self, target: &Bytes) -> Result<()> {
        if let Some(block) = &self.current_data_block {
            self.current_entry_idx = block.find_equal_or_greater_idx(target)?;
        }
        self.clear_cached_entry();
        Ok(())
    }

    /// Move to the first entry
    pub fn seek_to_first(&mut self) -> Result<()> {
        if self.index_partitions.is_empty() {
            self.current_data_block = None;
            self.clear_cached_entry();
            return Ok(());
        }

        if !self.footer.partitioned_index {
            if self.index_block.is_empty() {
                self.current_data_block = None;
                self.clear_cached_entry();
                return Ok(());
            }
            self.current_index_partition_idx = 0;
            self.current_index_partition = Some(self.index_block.clone());
            self.current_block_idx = 0;
            let partition = self.index_block.clone();
            self.load_data_block_from_partition(&partition, 0)?;
            self.current_entry_idx = 0;
            return Ok(());
        }

        let partition = self.load_index_partition(0)?;
        self.current_block_idx = 0;
        self.load_data_block_from_partition(&partition, 0)?;
        self.current_entry_idx = 0;
        Ok(())
    }

    /// Get the current key-value pair
    pub fn current(&self) -> Result<Option<(Bytes, Bytes)>> {
        if let Some(block) = &self.current_data_block
            && self.current_entry_idx < block.offsets_len()
        {
            let (key, value) = block.get(self.current_entry_idx)?;
            return Ok(Some((key, self.normalized_encoded_value(value))));
        }
        Ok(None)
    }

    /// Get the current key only
    pub fn key(&self) -> Result<Option<Bytes>> {
        if let Some(block) = &self.current_data_block
            && self.current_entry_idx < block.offsets_len()
        {
            let key = block.key(self.current_entry_idx)?;
            return Ok(Some(key));
        }
        Ok(None)
    }

    /// Get the current value only
    pub fn value(&self) -> Result<Option<Bytes>> {
        if let Some(block) = &self.current_data_block
            && self.current_entry_idx < block.offsets_len()
        {
            let value = block.value(self.current_entry_idx)?;
            return Ok(Some(self.normalized_encoded_value(value)));
        }
        Ok(None)
    }

    /// Move to the next entry
    pub fn next(&mut self) -> Result<bool> {
        if let Some(block) = &self.current_data_block {
            self.current_entry_idx += 1;

            if self.current_entry_idx >= block.offsets_len() {
                // Move to next block
                self.current_block_idx += 1;
                let reuse_partition = self.current_index_partition.is_some()
                    && self.current_block_idx
                        < self
                            .current_index_partition
                            .as_ref()
                            .map(|partition| partition.offsets_len())
                            .unwrap_or(0);
                if reuse_partition {
                    let partition = self.current_index_partition.clone().unwrap();
                    self.load_data_block_from_partition(&partition, self.current_block_idx)?;
                    self.current_entry_idx = 0;
                    self.clear_cached_entry();
                    return Ok(true);
                }
                if !self.footer.partitioned_index {
                    self.current_data_block = None;
                    self.clear_cached_entry();
                    return Ok(false);
                }
                let next_partition_idx = self.current_index_partition_idx + 1;
                if next_partition_idx < self.index_partitions.len() {
                    let partition = self.load_index_partition(next_partition_idx)?;
                    self.current_block_idx = 0;
                    self.load_data_block_from_partition(&partition, 0)?;
                    self.current_entry_idx = 0;
                    self.clear_cached_entry();
                    return Ok(true);
                }
                // No more blocks
                self.current_data_block = None;
                self.clear_cached_entry();
                return Ok(false);
            }

            self.clear_cached_entry();
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
                .map(|b| self.current_entry_idx < b.offsets_len())
                .unwrap_or(false)
    }

    /// Get the current typed Key, decoding from the row codec format.
    pub fn current_key(&self) -> Result<Option<Key>> {
        if let Some(mut bytes) = self.key()? {
            let key = decode_key(&mut bytes)?;
            return Ok(Some(key));
        }
        Ok(None)
    }

    /// Get the current typed Value, decoding from the row codec format.
    /// Returns a Value containing optional columns.
    pub fn current_value(&self) -> Result<Option<Value>> {
        if let Some(mut bytes) = self.value()? {
            let value = decode_value(&mut bytes, self.options.num_columns)?;
            return Ok(Some(value));
        }
        Ok(None)
    }

    /// Get the current typed Key and Value pair, decoding from the row codec format.
    pub fn current_kv(&self) -> Result<Option<(Key, Value)>> {
        if let Some((mut key_bytes, mut value_bytes)) = self.current()? {
            let key = decode_key(&mut key_bytes)?;
            let value = decode_value(&mut value_bytes, self.options.num_columns)?;
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

impl<'a> KvIterator<'a> for SSTIterator {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        SSTIterator::seek(self, target)
    }

    fn seek_to_first(&mut self) -> Result<()> {
        SSTIterator::seek_to_first(self)
    }

    fn next(&mut self) -> Result<bool> {
        SSTIterator::next(self)
    }

    fn valid(&self) -> bool {
        SSTIterator::valid(self)
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        self.ensure_cached_bytes()?;
        let cached = self.cached_key_bytes.borrow();
        if let Some(bytes) = cached.as_ref() {
            let ptr = bytes.as_ptr();
            let len = bytes.len();
            drop(cached);
            // SAFETY: cached bytes live as long as the iterator entry remains unchanged.
            return Ok(Some(unsafe { std::slice::from_raw_parts(ptr, len) }));
        }
        Ok(None)
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        SSTIterator::key(self)
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        Ok(SSTIterator::value(self)?.map(KvValue::Encoded))
    }

    fn take_current(&mut self) -> Result<Option<(Bytes, KvValue)>> {
        Ok(SSTIterator::current(self)?.map(|(k, v)| (k, KvValue::Encoded(v))))
    }
}

#[cfg(test)]
impl SSTIteratorTestCache {
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.inner.seek(target)
    }

    pub fn valid(&self) -> bool {
        self.inner.valid()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FileSystemRegistry;
    use crate::sst::bloom::{BloomFilter, BloomFilterBuilder};
    use crate::sst::writer::{SSTWriter, SSTWriterOptions};

    fn build_filter(bits_per_key: u32, keys: &[&[u8]]) -> BloomFilter {
        let mut builder = BloomFilterBuilder::new(bits_per_key);
        for key in keys {
            builder.add(key);
        }
        builder.finish()
    }

    fn find_missing_key(prefix: &str, filter: &BloomFilter) -> Vec<u8> {
        for idx in 0..1000 {
            let candidate = format!("{}{}", prefix, idx);
            if !filter.may_contain(candidate.as_bytes()) {
                return candidate.into_bytes();
            }
        }
        panic!("unable to find missing key not in filter");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_iterator_basic() {
        let _ = std::fs::remove_dir_all("/tmp/sst_test");
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register("file:///tmp/sst_test").unwrap();

        // Write SST file
        {
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
        }

        // Read SST file
        {
            let reader_file = fs.open_read("test.sst").unwrap();
            let mut iter = SSTIterator::with_cache(
                reader_file,
                0,
                SSTIteratorOptions {
                    bloom_filter_enabled: true,
                    ..SSTIteratorOptions::default()
                },
                None,
                None,
            )
            .unwrap();

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
    fn test_sst_iterator_with_compression() {
        let _ = std::fs::remove_dir_all("/tmp/sst_compressed_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/sst_compressed_test")
            .unwrap();

        {
            let writer_file = fs.open_write("compressed.sst").unwrap();
            let mut writer = SSTWriter::new(
                writer_file,
                SSTWriterOptions {
                    bloom_filter_enabled: true,
                    compression: crate::SstCompressionAlgorithm::Lz4,
                    ..SSTWriterOptions::default()
                },
            );

            writer.add(b"key1", b"value1").unwrap();
            writer.add(b"key2", b"value2").unwrap();
            writer.finish().unwrap();
        }

        {
            let reader_file = fs.open_read("compressed.sst").unwrap();
            let mut iter = SSTIterator::with_cache(
                reader_file,
                0,
                SSTIteratorOptions {
                    bloom_filter_enabled: true,
                    ..SSTIteratorOptions::default()
                },
                None,
                None,
            )
            .unwrap();

            iter.seek_to_first().unwrap();
            let (key, value) = iter.current().unwrap().unwrap();
            assert_eq!(&key[..], b"key1");
            assert_eq!(&value[..], b"value1");
        }

        let _ = std::fs::remove_dir_all("/tmp/sst_compressed_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_iterator_may_contain_single_level() {
        let _ = std::fs::remove_dir_all("/tmp/sst_filter_single_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/sst_filter_single_test")
            .unwrap();

        let keys: [&[u8]; 3] = [b"key1", b"key2", b"key3"];
        let bits_per_key = 100;
        let filter = build_filter(bits_per_key, &keys);
        let missing_key = find_missing_key("missing_", &filter);

        {
            let writer_file = fs.open_write("filter_single.sst").unwrap();
            let mut writer = SSTWriter::new(
                writer_file,
                SSTWriterOptions {
                    bloom_filter_enabled: true,
                    bloom_bits_per_key: bits_per_key,
                    partitioned_index: false,
                    ..SSTWriterOptions::default()
                },
            );

            for key in keys {
                writer.add(key, b"value").unwrap();
            }

            writer.finish().unwrap();
        }

        {
            let reader_file = fs.open_read("filter_single.sst").unwrap();
            let mut iter = SSTIterator::with_cache(
                reader_file,
                0,
                SSTIteratorOptions {
                    bloom_filter_enabled: true,
                    ..SSTIteratorOptions::default()
                },
                None,
                None,
            )
            .unwrap();

            assert!(iter.may_contain(b"key2").unwrap());
            assert!(!iter.may_contain(&missing_key).unwrap());
        }

        let _ = std::fs::remove_dir_all("/tmp/sst_filter_single_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_iterator_may_contain_partitioned() {
        let _ = std::fs::remove_dir_all("/tmp/sst_filter_partitioned_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/sst_filter_partitioned_test")
            .unwrap();

        let keys: [&[u8]; 4] = [b"key000", b"key001", b"key002", b"key003"];
        let bits_per_key = 100;
        let partition0_filter = build_filter(bits_per_key, &[keys[0]]);
        assert!(!partition0_filter.may_contain(keys[3]));
        let partition3_filter = build_filter(bits_per_key, &[keys[3]]);
        let missing_key = find_missing_key("key003_missing_", &partition3_filter);

        let value = vec![b'v'; 64];
        {
            let writer_file = fs.open_write("filter_partitioned.sst").unwrap();
            let mut writer = SSTWriter::new(
                writer_file,
                SSTWriterOptions {
                    metrics: None,
                    block_size: 32,
                    buffer_size: 8192,
                    num_columns: 1,
                    bloom_filter_enabled: true,
                    bloom_bits_per_key: bits_per_key,
                    partitioned_index: true,
                    data_block_restart_interval: 16,
                    compression: crate::SstCompressionAlgorithm::None,
                    value_has_ttl: true,
                },
            );

            for key in keys {
                writer.add(key, &value).unwrap();
            }

            writer.finish().unwrap();
        }

        {
            let reader_file = fs.open_read("filter_partitioned.sst").unwrap();
            let mut iter = SSTIterator::with_cache(
                reader_file,
                0,
                SSTIteratorOptions {
                    bloom_filter_enabled: true,
                    ..SSTIteratorOptions::default()
                },
                None,
                None,
            )
            .unwrap();

            assert!(iter.may_contain(b"key003").unwrap());
            assert!(!iter.may_contain(&missing_key).unwrap());
        }

        let _ = std::fs::remove_dir_all("/tmp/sst_filter_partitioned_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_iterator_seek() {
        let _ = std::fs::remove_dir_all("/tmp/sst_test");
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register("file:///tmp/sst_test").unwrap();

        // Write SST file
        {
            let writer_file = fs.open_write("test_seek.sst").unwrap();
            let mut writer = SSTWriter::new(
                writer_file,
                SSTWriterOptions {
                    bloom_filter_enabled: true,
                    ..SSTWriterOptions::default()
                },
            );

            writer.add(b"key0001", b"value0001").unwrap();
            writer.add(b"key0003", b"value0003").unwrap();
            writer.add(b"key0005", b"value0005").unwrap();
            writer.add(b"key0007", b"value0007").unwrap();
            // fill more entries to ensure multiple blocks
            for i in 0..1000 {
                let key = format!("key{:04}", i * 2 + 10);
                let value = format!("value{:04}", i * 2 + 10);
                writer.add(key.as_bytes(), value.as_bytes()).unwrap();
            }

            writer.finish().unwrap();
        }

        // Read and seek
        {
            let reader_file = fs.open_read("test_seek.sst").unwrap();
            let mut iter = SSTIterator::with_cache(
                reader_file,
                0,
                SSTIteratorOptions {
                    bloom_filter_enabled: true,
                    ..SSTIteratorOptions::default()
                },
                None,
                None,
            )
            .unwrap();

            // Seek to exact key
            iter.seek(b"key0003").unwrap();
            assert!(iter.valid());
            let (key, value) = iter.current().unwrap().unwrap();
            assert_eq!(&key[..], b"key0003");
            assert_eq!(&value[..], b"value0003");

            // Seek to key between entries
            iter.seek(b"key0004").unwrap();
            assert!(iter.valid());
            let (key, value) = iter.current().unwrap().unwrap();
            assert_eq!(&key[..], b"key0005");
            assert_eq!(&value[..], b"value0005");

            // Seek to first
            iter.seek(b"key0000").unwrap();
            assert!(iter.valid());
            let (key, value) = iter.current().unwrap().unwrap();
            assert_eq!(&key[..], b"key0001");
            assert_eq!(&value[..], b"value0001");
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
            .get_or_register("file:///tmp/sst_typed_kv_test")
            .unwrap();

        let num_columns = 2;

        // Write SST file using typed Key/Value API
        {
            let writer_file = fs.open_write("typed.sst").unwrap();
            let mut writer = SSTWriter::new(
                writer_file,
                SSTWriterOptions {
                    num_columns,
                    bloom_filter_enabled: true,
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
            let mut iter = SSTIterator::with_cache(
                reader_file,
                0,
                SSTIteratorOptions {
                    bloom_filter_enabled: true,
                    num_columns,
                    ..SSTIteratorOptions::default()
                },
                None,
                None,
            )
            .unwrap();

            iter.seek_to_first().unwrap();

            // First entry
            assert!(iter.valid());
            let (key, value) = iter.current_kv().unwrap().unwrap();
            let cols = value.columns();
            assert_eq!(key.bucket(), 1);
            assert_eq!(key.data().as_ref(), b"user:1");
            assert!(cols[0].is_some());
            assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"Alice");
            assert!(cols[1].is_some());
            assert_eq!(
                cols[1].as_ref().unwrap().data().as_ref(),
                b"alice@example.com"
            );

            // Second entry
            iter.next().unwrap();
            assert!(iter.valid());
            let key = iter.current_key().unwrap().unwrap();
            let value = iter.current_value().unwrap().unwrap();
            let cols = value.columns();
            assert_eq!(key.bucket(), 1);
            assert_eq!(key.data().as_ref(), b"user:2");
            assert!(cols[0].is_some());
            assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"Bob");
            assert!(cols[1].is_none());

            // Third entry
            iter.next().unwrap();
            assert!(iter.valid());
            let (key, value) = iter.current_kv().unwrap().unwrap();
            let cols = value.columns();
            assert_eq!(key.bucket(), 2);
            assert_eq!(key.data().as_ref(), b"order:100");
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
            let mut iter = SSTIterator::with_cache(
                reader_file,
                0,
                SSTIteratorOptions {
                    bloom_filter_enabled: true,
                    num_columns,
                    ..SSTIteratorOptions::default()
                },
                None,
                None,
            )
            .unwrap();

            let target = Key::new(1, b"user:2".to_vec());
            iter.seek_key(&target).unwrap();
            assert!(iter.valid());
            let key = iter.current_key().unwrap().unwrap();
            assert_eq!(key.data().as_ref(), b"user:2");
        }

        let _ = std::fs::remove_dir_all("/tmp/sst_typed_kv_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_typed_kv_without_ttl_header() {
        use crate::r#type::{Column, Key, Value, ValueType};

        let _ = std::fs::remove_dir_all("/tmp/sst_typed_kv_no_ttl_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/sst_typed_kv_no_ttl_test")
            .unwrap();

        {
            let writer_file = fs.open_write("typed_no_ttl.sst").unwrap();
            let mut writer = SSTWriter::new(
                writer_file,
                SSTWriterOptions {
                    num_columns: 1,
                    value_has_ttl: false,
                    ..SSTWriterOptions::default()
                },
            );
            let key = Key::new(1, b"user:1".to_vec());
            let value = Value::new_with_expired_at(
                vec![Some(Column::new(ValueType::Put, b"Alice".to_vec()))],
                Some(12345),
            );
            writer.add_kv(&key, &value).unwrap();
            writer.finish().unwrap();
        }

        {
            let reader_file = fs.open_read("typed_no_ttl.sst").unwrap();
            let mut iter = SSTIterator::with_cache(
                reader_file,
                0,
                SSTIteratorOptions {
                    num_columns: 1,
                    ..SSTIteratorOptions::default()
                },
                None,
                None,
            )
            .unwrap();
            iter.seek_to_first().unwrap();
            let value = iter.current_value().unwrap().unwrap();
            assert_eq!(value.expired_at(), None);
            assert_eq!(
                value.columns()[0].as_ref().unwrap().data().as_ref(),
                b"Alice"
            );
        }

        let _ = std::fs::remove_dir_all("/tmp/sst_typed_kv_no_ttl_test");
    }
}
