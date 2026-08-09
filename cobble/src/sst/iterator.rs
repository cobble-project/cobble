use crate::cache::{
    BlockCache, BlockCacheKey, BlockCacheKind, CachedBlock, ScanHotBlockHandle,
    ScanHotBlockRegistry, data_block_cache_key,
};
use crate::config::SstReadMetadataCacheMode;
use crate::data_file::DataFile;
use crate::error::{Error, Result};
use crate::file::RandomAccessFile;
use crate::iterator::KvIterator;
use crate::sst::bloom::BloomFilter;
use crate::sst::compression::{decode_block_bytes, verify_block_checksum};
use crate::sst::format::{Block, FOOTER_SIZE, Footer, SstReadMetadata};
use crate::sst::read::read_metadata_block;
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
    /// Caching policy for decoded footer and index-partition descriptors.
    pub read_metadata_cache_mode: SstReadMetadataCacheMode,
    /// Build DataFile-level pinned metadata on first read when it is absent.
    pub pin_metadata: bool,
    /// Include second-level index and filter partitions in a newly-built metadata pin.
    pub pin_metadata_partitions: bool,
    /// Namespace used to isolate block-cache keys across shards/dbs.
    pub cache_namespace: u64,
    /// Preload the data block after the current one during scan iteration.
    pub preload_next_data_block: bool,
    /// Shared physical-block tracker for cursor-adjacent scan blocks.
    pub hot_block_registry: Option<Arc<ScanHotBlockRegistry>>,
    /// Mark registry hits when this iterator is used as compaction input.
    pub observe_hot_blocks: bool,
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

    pub(crate) fn record_index_hit(&self) {
        self.index_hits.increment(1);
    }

    pub(crate) fn record_index_miss(&self) {
        self.index_misses.increment(1);
    }

    pub(crate) fn record_data_hit(&self) {
        self.data_hits.increment(1);
    }

    pub(crate) fn record_data_miss(&self) {
        self.data_misses.increment(1);
    }

    pub(crate) fn record_filter_hit(&self) {
        self.filter_hits.increment(1);
    }

    pub(crate) fn record_filter_miss(&self) {
        self.filter_misses.increment(1);
    }
}

impl Default for SSTIteratorOptions {
    fn default() -> Self {
        Self {
            block_cache_size: 64 * 1024 * 1024, // 64 MB
            num_columns: 1,
            metrics: None,
            bloom_filter_enabled: false,
            read_metadata_cache_mode: SstReadMetadataCacheMode::Eager,
            pin_metadata: false,
            pin_metadata_partitions: false,
            cache_namespace: 0,
            preload_next_data_block: false,
            hot_block_registry: None,
            observe_hot_blocks: false,
        }
    }
}

/// Iterator for reading key-value pairs from an SST file
pub(crate) struct SSTIterator {
    file: Box<dyn RandomAccessFile>,
    file_id: u64,
    footer: Footer,
    index_block: Arc<Block>,
    index_partitions: Arc<[(u64, u64)]>,
    bloom_filter: Option<Arc<BloomFilter>>,
    bloom_filter_partition_idx: Option<usize>,
    current_data_block: Option<Arc<Block>>,
    current_index_partition_idx: usize,
    current_index_partition: Option<Arc<Block>>,
    current_block_idx: usize,
    current_entry_idx: usize,
    options: SSTIteratorOptions,
    block_cache: Option<BlockCache>,
    pinned_metadata: Option<Arc<crate::sst::PinnedSstReadMetadata>>,
    hot_block_handle: Option<ScanHotBlockHandle>,
    metrics: Arc<SSTIteratorMetrics>,
    cached_key_entry_idx: Cell<Option<usize>>,
    cached_value_entry_idx: Cell<Option<usize>>,
    cached_prefix_key_block_id: Cell<Option<u32>>,
    cached_prefix_key_entry_idx: Cell<Option<usize>>,
    cached_prefix_key_bytes: RefCell<Vec<u8>>,
    cached_key_bytes: RefCell<Option<Bytes>>,
    cached_value_bytes: RefCell<Option<Bytes>>,
    /// Runtime configuration for whether crossing into a new SST data block
    /// should be surfaced as a stop instead of continuing immediately.
    should_stop_at_block_boundary: bool,
    /// Data-block boundary lifecycle for the leaf SST iterator.
    boundary_state: BoundaryState,
}

/// Data-block boundary state for the SST iterator.
///
/// The SST iterator can surface a stop immediately when it moves past the last
/// entry in the current data block. After callers clear that stop, the next
/// `next()` resumes by loading the next physical block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundaryState {
    None,
    Stopped,
    ReadyToResume,
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

    pub(crate) fn with_cache_and_file(
        file: Box<dyn RandomAccessFile>,
        data_file: &DataFile,
        options: SSTIteratorOptions,
        block_cache: Option<BlockCache>,
    ) -> Result<Self> {
        if let Some(metadata) = crate::sst::PinnedSstReadMetadata::get_or_load(
            &*file,
            data_file,
            options.pin_metadata,
            options.pin_metadata_partitions,
        )? {
            let metrics = Self::metrics_for(&options);
            return Ok(Self::from_pinned_metadata(
                file,
                data_file.file_id,
                options,
                block_cache,
                metrics,
                metadata,
            ));
        }
        let cached_metadata = options
            .read_metadata_cache_mode
            .caches_reads()
            .then(|| data_file.sst_read_metadata())
            .flatten();
        let (footer, cached_footer) = if let Some(metadata) = &cached_metadata {
            (metadata.footer().clone(), None)
        } else {
            Self::decode_footer(data_file.meta_bytes(), &*file)?
        };
        let metrics = Self::metrics_for(&options);
        let index_block = Self::load_index_block(
            &*file,
            data_file.file_id,
            &options,
            &block_cache,
            &metrics,
            &footer,
        )?;
        let metadata = match cached_metadata {
            Some(metadata) => metadata,
            None => {
                let metadata = Arc::new(SstReadMetadata::from_index_block(footer, &index_block)?);
                if options.read_metadata_cache_mode.caches_reads() {
                    data_file.set_sst_read_metadata(Arc::clone(&metadata));
                }
                if let Some(bytes) = cached_footer {
                    data_file.set_meta_bytes(bytes);
                }
                metadata
            }
        };
        Ok(Self::from_read_metadata(
            file,
            data_file.file_id,
            options,
            block_cache,
            metrics,
            index_block,
            &metadata,
        ))
    }

    fn with_cache_and_footer_bytes(
        file: Box<dyn RandomAccessFile>,
        file_id: u64,
        options: SSTIteratorOptions,
        block_cache: Option<BlockCache>,
        footer_bytes: Option<Bytes>,
    ) -> Result<(Self, Option<Bytes>)> {
        let metrics = Self::metrics_for(&options);
        let (footer, cached_footer) = Self::decode_footer(footer_bytes, &*file)?;
        let index_block =
            Self::load_index_block(&*file, file_id, &options, &block_cache, &metrics, &footer)?;
        let metadata = SstReadMetadata::from_index_block(footer, &index_block)?;
        Ok((
            Self::from_read_metadata(
                file,
                file_id,
                options,
                block_cache,
                metrics,
                index_block,
                &metadata,
            ),
            cached_footer,
        ))
    }

    pub(crate) fn metrics_for(options: &SSTIteratorOptions) -> Arc<SSTIteratorMetrics> {
        options
            .metrics
            .clone()
            .unwrap_or_else(|| Arc::new(SSTIteratorMetrics::new("unknown")))
    }

    pub(crate) fn decode_footer(
        footer_bytes: Option<Bytes>,
        file: &dyn RandomAccessFile,
    ) -> Result<(Footer, Option<Bytes>)> {
        if let Some(bytes) = footer_bytes {
            Ok((Footer::decode(bytes.as_ref())?, None))
        } else {
            let bytes = Self::read_footer_bytes(file)?;
            let footer = Footer::decode(bytes.as_ref())?;
            Ok((footer, Some(bytes)))
        }
    }

    pub(crate) fn load_index_block(
        file: &dyn RandomAccessFile,
        file_id: u64,
        options: &SSTIteratorOptions,
        block_cache: &Option<BlockCache>,
        metrics: &SSTIteratorMetrics,
        footer: &Footer,
    ) -> Result<Arc<Block>> {
        let index_block = if let Some(cache) = block_cache {
            let cache_key = BlockCacheKey {
                namespace: options.cache_namespace,
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
                let index_block = read_metadata_block(
                    file,
                    footer.index_block_offset,
                    footer.index_block_size,
                    u32::MAX,
                )?;
                cache.insert(cache_key, CachedBlock::Block(index_block.clone()));
                index_block
            }
        } else {
            read_metadata_block(
                file,
                footer.index_block_offset,
                footer.index_block_size,
                u32::MAX,
            )?
        };
        Ok(index_block)
    }

    fn from_read_metadata(
        file: Box<dyn RandomAccessFile>,
        file_id: u64,
        options: SSTIteratorOptions,
        block_cache: Option<BlockCache>,
        metrics: Arc<SSTIteratorMetrics>,
        index_block: Arc<Block>,
        metadata: &SstReadMetadata,
    ) -> Self {
        let hot_block_handle = options
            .preload_next_data_block
            .then(|| options.hot_block_registry.as_ref().map(Arc::clone))
            .flatten()
            .map(|registry| registry.handle());
        Self {
            file,
            file_id,
            footer: metadata.footer().clone(),
            index_block,
            index_partitions: metadata.index_partitions(),
            bloom_filter: None,
            bloom_filter_partition_idx: None,
            current_data_block: None,
            current_index_partition_idx: 0,
            current_index_partition: None,
            current_block_idx: 0,
            current_entry_idx: 0,
            options,
            block_cache,
            pinned_metadata: None,
            hot_block_handle,
            metrics,
            cached_key_entry_idx: Cell::new(None),
            cached_value_entry_idx: Cell::new(None),
            cached_prefix_key_block_id: Cell::new(None),
            cached_prefix_key_entry_idx: Cell::new(None),
            cached_prefix_key_bytes: RefCell::new(Vec::new()),
            cached_key_bytes: RefCell::new(None),
            cached_value_bytes: RefCell::new(None),
            should_stop_at_block_boundary: false,
            boundary_state: BoundaryState::None,
        }
    }

    fn from_pinned_metadata(
        file: Box<dyn RandomAccessFile>,
        file_id: u64,
        options: SSTIteratorOptions,
        block_cache: Option<BlockCache>,
        metrics: Arc<SSTIteratorMetrics>,
        metadata: Arc<crate::sst::PinnedSstReadMetadata>,
    ) -> Self {
        let mut iter = Self::from_read_metadata(
            file,
            file_id,
            options,
            block_cache,
            metrics,
            metadata.index_top(),
            metadata.read_metadata(),
        );
        iter.pinned_metadata = Some(metadata);
        iter
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
        self.boundary_state = BoundaryState::None;
        let target = unsafe_bytes(target);
        if self.index_partitions.is_empty() {
            self.current_data_block = None;
            self.clear_cached_entry();
            self.clear_scan_hot_blocks();
            return Ok(());
        }
        if !self.footer.partitioned_index {
            if self.index_block.is_empty() {
                self.current_data_block = None;
                self.clear_cached_entry();
                self.clear_scan_hot_blocks();
                return Ok(());
            }
            self.current_index_partition_idx = 0;
            self.current_index_partition = Some(self.index_block.clone());
            let block_idx = self.index_block.find_lower_or_equal_idx(&target)?;
            self.current_block_idx = block_idx;
            let partition = self.index_block.clone();
            self.load_data_block_from_partition(&partition, block_idx)?;
            self.seek_in_current_block(&target)?;
            self.finish_seek_positioning()?;
            return Ok(());
        }

        let partition_idx = self.index_block.find_lower_or_equal_idx(&target)?;
        let partition = self.load_index_partition(partition_idx)?;
        let block_idx = partition.find_lower_or_equal_idx(&target)?;
        self.current_block_idx = block_idx;
        self.load_data_block_from_partition(&partition, block_idx)?;
        self.seek_in_current_block(&target)?;
        self.finish_seek_positioning()?;
        Ok(())
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
        if let Some(metadata) = &self.pinned_metadata
            && let Some(block) = metadata.index_partition(partition_idx)?
        {
            self.current_index_partition_idx = partition_idx;
            self.current_index_partition = Some(Arc::clone(&block));
            return Ok(block);
        }
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
            namespace: self.options.cache_namespace,
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
        let block = self.read_data_block_from_partition(partition, block_idx)?;
        self.current_data_block = Some(block);
        self.current_entry_idx = 0;
        self.clear_cached_entry();

        Ok(())
    }

    fn data_block_location(&self, partition: &Block, block_idx: usize) -> Result<(usize, usize)> {
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
        Ok((offset, size))
    }

    fn read_data_block_from_partition(
        &self,
        partition: &Arc<Block>,
        block_idx: usize,
    ) -> Result<Arc<Block>> {
        let (offset, size) = self.data_block_location(partition, block_idx)?;
        let offset = offset as u64;
        let cache_key = BlockCacheKey {
            namespace: self.options.cache_namespace,
            file_id: self.file_id,
            block_id: offset,
            kind: BlockCacheKind::Data,
        };
        if self.options.observe_hot_blocks
            && let Some(registry) = &self.options.hot_block_registry
        {
            // Compaction input iterators set `observe_hot_blocks`. When they read
            // a block that a scan iterator registered as current/next, the registry
            // counter drives `SSTWriterHotBlockCache` to record the output block
            // key for asynchronous warming after compaction.
            registry.observe_if_hot(cache_key);
        }
        let block = if let Some(cache) = &self.block_cache {
            if let Some(cached) = cache.get(&cache_key) {
                // Cached SST blocks are trusted: every internal cache-fill path verifies bytes
                // immediately after read_at and before insertion. Keep checksum work on storage
                // reads only so cache hits do not rescan block contents.
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
                let data = self.file.read_at(offset as usize, size)?;
                let verified =
                    verify_block_checksum(data, self.footer.block_checksums, "SST data block")?;
                let decoded = decode_block_bytes(verified)?;
                let mut block = Block::decode(decoded)?;
                block.set_block_id(block_idx as u32);
                let block = Arc::new(block);
                cache.insert(cache_key, CachedBlock::Block(block.clone()));
                block
            }
        } else {
            let data = self.file.read_at(offset as usize, size)?;
            let verified =
                verify_block_checksum(data, self.footer.block_checksums, "SST data block")?;
            let decoded = decode_block_bytes(verified)?;
            let mut block = Block::decode(decoded)?;
            block.set_block_id(block_idx as u32);
            Arc::new(block)
        };
        Ok(block)
    }

    fn data_block_key_from_partition(
        &self,
        partition: &Block,
        block_idx: usize,
    ) -> Result<BlockCacheKey> {
        let (offset, _) = self.data_block_location(partition, block_idx)?;
        Ok(data_block_cache_key(
            self.options.cache_namespace,
            self.file_id,
            offset as u64,
        ))
    }

    fn clear_scan_hot_blocks(&mut self) {
        if let Some(handle) = &mut self.hot_block_handle {
            handle.replace(Vec::new());
        }
    }

    fn after_data_block_positioned(&mut self) -> Result<()> {
        if !self.options.preload_next_data_block {
            return Ok(());
        }
        if !self.valid() {
            self.clear_scan_hot_blocks();
            return Ok(());
        }
        let Some(partition) = self.current_index_partition.clone() else {
            self.clear_scan_hot_blocks();
            return Ok(());
        };

        let mut hot_blocks =
            vec![self.data_block_key_from_partition(partition.as_ref(), self.current_block_idx)?];
        let next_block_idx = self.current_block_idx + 1;
        if next_block_idx < partition.offsets_len() {
            hot_blocks
                .push(self.data_block_key_from_partition(partition.as_ref(), next_block_idx)?);
            // The scan path already needs the current block. Pulling the next block
            // through the normal cache path keeps cursor-driven scans warm without
            // changing iterator position or duplicating decode/cache-key logic.
            if self.block_cache.is_some() {
                let _ = self.read_data_block_from_partition(&partition, next_block_idx)?;
            }
        }
        if let Some(handle) = &mut self.hot_block_handle {
            handle.replace(hot_blocks);
        }
        Ok(())
    }

    /// Load the filter index block.
    /// Used for partitioned filter index.
    fn load_filter_index(&mut self) -> Result<Arc<Block>> {
        if let Some(metadata) = &self.pinned_metadata
            && let Some(filter_index) = metadata.filter_index()
        {
            return Ok(filter_index);
        }
        let cache_key = BlockCacheKey {
            namespace: self.options.cache_namespace,
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
        if let Some(metadata) = &self.pinned_metadata
            && let Some(filter) = metadata.filter_partition(partition_idx)?
        {
            return Ok(filter);
        }
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
                namespace: self.options.cache_namespace,
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
            namespace: self.options.cache_namespace,
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
        self.clear_materialized_entry();
        self.clear_prefix_key_cache();
    }

    fn clear_materialized_entry(&self) {
        self.cached_key_entry_idx.set(None);
        self.cached_value_entry_idx.set(None);
        *self.cached_key_bytes.borrow_mut() = None;
        *self.cached_value_bytes.borrow_mut() = None;
    }

    fn clear_prefix_key_cache(&self) {
        self.cached_prefix_key_block_id.set(None);
        self.cached_prefix_key_entry_idx.set(None);
        self.cached_prefix_key_bytes.borrow_mut().clear();
    }

    fn ensure_cached_key_view(&self) -> Result<()> {
        let Some(block) = &self.current_data_block else {
            self.clear_cached_entry();
            return Ok(());
        };
        if self.current_entry_idx >= block.offsets_len() {
            self.clear_cached_entry();
            return Ok(());
        }
        if !block.is_prefix_compressed() {
            if self.cached_key_entry_idx.get() != Some(self.current_entry_idx) {
                *self.cached_key_bytes.borrow_mut() = Some(block.key(self.current_entry_idx)?);
                self.cached_key_entry_idx.set(Some(self.current_entry_idx));
            }
            return Ok(());
        }

        let block_id = block.block_id();
        if self.cached_prefix_key_block_id.get() == Some(block_id)
            && self.cached_prefix_key_entry_idx.get() == Some(self.current_entry_idx)
        {
            return Ok(());
        }

        let previous_entry = self.current_entry_idx.checked_sub(1);
        let mut key = self.cached_prefix_key_bytes.borrow_mut();
        if self.cached_prefix_key_block_id.get() == Some(block_id)
            && self.cached_prefix_key_entry_idx.get() == previous_entry
        {
            block.advance_prefix_key(self.current_entry_idx, &mut key)?;
        } else {
            block.decode_prefix_key_into(self.current_entry_idx, &mut key)?;
        }
        self.cached_prefix_key_block_id.set(Some(block_id));
        self.cached_prefix_key_entry_idx
            .set(Some(self.current_entry_idx));
        Ok(())
    }

    fn materialize_current_key(&self) -> Result<Option<Bytes>> {
        self.ensure_cached_key_view()?;
        let Some(block) = &self.current_data_block else {
            return Ok(None);
        };
        if self.current_entry_idx >= block.offsets_len() {
            return Ok(None);
        }
        if self.cached_key_entry_idx.get() == Some(self.current_entry_idx) {
            return Ok(self.cached_key_bytes.borrow().clone());
        }
        let key = if block.is_prefix_compressed() {
            let key = self.cached_prefix_key_bytes.borrow();
            Bytes::copy_from_slice(key.as_slice())
        } else {
            self.cached_key_bytes
                .borrow()
                .clone()
                .expect("non-prefix key cache populated")
        };
        *self.cached_key_bytes.borrow_mut() = Some(key.clone());
        self.cached_key_entry_idx.set(Some(self.current_entry_idx));
        Ok(Some(key))
    }

    fn materialize_current_value(&self) -> Result<Option<Bytes>> {
        let Some(block) = &self.current_data_block else {
            self.clear_cached_entry();
            return Ok(None);
        };
        if self.current_entry_idx >= block.offsets_len() {
            self.clear_cached_entry();
            return Ok(None);
        }
        if self.cached_value_entry_idx.get() == Some(self.current_entry_idx) {
            return Ok(self.cached_value_bytes.borrow().clone());
        }
        let value = block.value(self.current_entry_idx)?;
        *self.cached_value_bytes.borrow_mut() = Some(value.clone());
        self.cached_value_entry_idx
            .set(Some(self.current_entry_idx));
        Ok(Some(value))
    }

    fn current_key_slice(&self) -> Result<Option<(*const u8, usize)>> {
        self.ensure_cached_key_view()?;
        let Some(block) = &self.current_data_block else {
            return Ok(None);
        };
        if self.current_entry_idx >= block.offsets_len() {
            return Ok(None);
        }
        if block.is_prefix_compressed() {
            let key = self.cached_prefix_key_bytes.borrow();
            return Ok(Some((key.as_ptr(), key.len())));
        }
        let cached = self.cached_key_bytes.borrow();
        Ok(cached.as_ref().map(|bytes| (bytes.as_ptr(), bytes.len())))
    }

    fn invalidate_current_entry_cache(&self) {
        self.clear_materialized_entry();
        if self
            .current_data_block
            .as_ref()
            .is_some_and(|block| !block.is_prefix_compressed())
        {
            self.clear_prefix_key_cache();
        }
    }

    fn position_at_current_entry(&self) {
        self.invalidate_current_entry_cache();
    }

    fn position_after_sequential_next(&self) {
        self.invalidate_current_entry_cache();
    }

    fn position_after_random_access(&self) {
        self.clear_cached_entry();
    }

    fn position_after_block_change(&self) {
        self.clear_cached_entry();
    }

    fn seek_in_current_block(&mut self, target: &Bytes) -> Result<()> {
        if let Some(block) = &self.current_data_block {
            self.current_entry_idx = block.find_equal_or_greater_idx(target)?;
        }
        self.position_after_random_access();
        Ok(())
    }

    fn finish_seek_positioning(&mut self) -> Result<()> {
        if self.valid() {
            return self.after_data_block_positioned();
        }

        // The index stores the first key of each data block. A target between two blocks
        // initially selects the preceding block, whose in-block seek lands just past its end.
        // Continue with the next block so callers observe the first key >= target.
        loop {
            self.current_block_idx += 1;
            if !self.load_next_position()? || self.valid() {
                return Ok(());
            }
        }
    }

    /// Move to the first entry
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.boundary_state = BoundaryState::None;
        if self.index_partitions.is_empty() {
            self.current_data_block = None;
            self.clear_cached_entry();
            self.clear_scan_hot_blocks();
            return Ok(());
        }

        if !self.footer.partitioned_index {
            if self.index_block.is_empty() {
                self.current_data_block = None;
                self.clear_cached_entry();
                self.clear_scan_hot_blocks();
                return Ok(());
            }
            self.current_index_partition_idx = 0;
            self.current_index_partition = Some(self.index_block.clone());
            self.current_block_idx = 0;
            let partition = self.index_block.clone();
            self.load_data_block_from_partition(&partition, 0)?;
            self.current_entry_idx = 0;
            self.position_at_current_entry();
            self.after_data_block_positioned()?;
            return Ok(());
        }

        let partition = self.load_index_partition(0)?;
        self.current_block_idx = 0;
        self.load_data_block_from_partition(&partition, 0)?;
        self.current_entry_idx = 0;
        self.position_at_current_entry();
        self.after_data_block_positioned()?;
        Ok(())
    }

    fn load_next_position(&mut self) -> Result<bool> {
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
            self.position_after_block_change();
            self.after_data_block_positioned()?;
            return Ok(true);
        }
        if !self.footer.partitioned_index {
            self.current_data_block = None;
            self.position_after_block_change();
            self.clear_scan_hot_blocks();
            return Ok(false);
        }
        let next_partition_idx = self.current_index_partition_idx + 1;
        if next_partition_idx < self.index_partitions.len() {
            let partition = self.load_index_partition(next_partition_idx)?;
            self.current_block_idx = 0;
            self.load_data_block_from_partition(&partition, 0)?;
            self.current_entry_idx = 0;
            self.position_after_block_change();
            self.after_data_block_positioned()?;
            return Ok(true);
        }
        self.current_data_block = None;
        self.position_after_block_change();
        self.clear_scan_hot_blocks();
        Ok(false)
    }

    /// Get the current key-value pair
    pub fn current(&self) -> Result<Option<(Bytes, Bytes)>> {
        let Some(key) = self.materialize_current_key()? else {
            return Ok(None);
        };
        let Some(value) = self.materialize_current_value()? else {
            return Ok(None);
        };
        Ok(Some((key, self.normalized_encoded_value(value))))
    }

    /// Get the current key only
    pub fn key(&self) -> Result<Option<Bytes>> {
        self.materialize_current_key()
    }

    /// Get the current value only
    pub fn value(&self) -> Result<Option<Bytes>> {
        Ok(self
            .materialize_current_value()?
            .map(|value| self.normalized_encoded_value(value)))
    }

    /// Move to the next entry
    pub fn next(&mut self) -> Result<bool> {
        match self.boundary_state {
            BoundaryState::Stopped => return Ok(false),
            BoundaryState::ReadyToResume => {
                self.boundary_state = BoundaryState::None;
                return self.load_next_position();
            }
            BoundaryState::None => {}
        }
        if let Some(block) = &self.current_data_block {
            self.current_entry_idx += 1;

            if self.current_entry_idx >= block.offsets_len() {
                // Move to next block
                self.current_block_idx += 1;
                if self.should_stop_at_block_boundary {
                    self.current_data_block = None;
                    self.position_after_block_change();
                    self.clear_scan_hot_blocks();
                    self.boundary_state = BoundaryState::Stopped;
                    return Ok(false);
                }
                return self.load_next_position();
            }

            self.position_after_sequential_next();
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
        if let Some((ptr, len)) = self.current_key_slice()? {
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

    fn set_stop_at_block_boundary(&mut self, enabled: bool) {
        self.should_stop_at_block_boundary = enabled;
        self.boundary_state = BoundaryState::None;
    }

    fn clear_stop_at_block_boundary(&mut self) {
        if self.boundary_state == BoundaryState::Stopped {
            self.boundary_state = BoundaryState::ReadyToResume;
        }
    }

    fn stopped_at_block_boundary(&self) -> bool {
        self.boundary_state == BoundaryState::Stopped
    }
}

#[cfg(test)]
#[path = "../../tests/unit/sst/iterator.rs"]
mod tests;
