use crate::cache::{BlockCache, BlockCacheKey, BlockCacheKind, CachedBlock};
use crate::data_file::DataFile;
use crate::error::{Error, Result};
use crate::file::RandomAccessFile;
use crate::sst::bloom::BloomFilter;
use crate::sst::compression::{decode_block_bytes, verify_block_checksum};
use crate::sst::format::{Block, Footer, SstReadMetadata};
use crate::sst::iterator::{SSTIterator, SSTIteratorMetrics, SSTIteratorOptions};
use crate::sst::read::{
    indexed_block_location, read_bloom_filter, read_data_block, read_metadata_block,
};
use crate::util::unsafe_bytes;
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Keep one batch read bounded even when an SST contains a very large run of
/// physically adjacent data blocks. This bound applies to one FileManager
/// `read_at` call; it does not change the logical block-cache granularity.
const MAX_BATCH_DATA_READ_BYTES: usize = 8 * 1024 * 1024;

struct BatchDataBlock {
    offset: u64,
    size: usize,
    block_id: u32,
    key_slots: Vec<usize>,
    data_cache_namespaces: Vec<u64>,
}

/// Immutable SST read metadata held by a `DataFile`, outside the block-cache budget.
#[derive(Debug)]
pub(crate) struct PinnedSstReadMetadata {
    read_metadata: Arc<SstReadMetadata>,
    index_top: Arc<Block>,
    index_partitions: Option<Arc<[Arc<Block>]>>,
    filter_index: Option<Arc<Block>>,
    filter_partitions: Option<Arc<[Arc<BloomFilter>]>>,
}

impl PinnedSstReadMetadata {
    pub(crate) fn get_or_load(
        file: &dyn RandomAccessFile,
        data_file: &DataFile,
        pin_eligible: bool,
        pin_partitions: bool,
    ) -> Result<Option<Arc<Self>>> {
        if let Some(metadata) = data_file.pinned_sst_read_metadata() {
            return Ok(Some(metadata));
        }
        if !pin_eligible {
            return Ok(None);
        }

        // The file is immutable. Competing first readers may duplicate this work, but OnceLock
        // publishes only one fully-built Arc and failures never become visible as a pin.
        let metadata = Arc::new(Self::load(file, data_file, pin_partitions)?);
        data_file.set_pinned_sst_read_metadata(metadata);
        Ok(data_file.pinned_sst_read_metadata())
    }

    fn load(
        file: &dyn RandomAccessFile,
        data_file: &DataFile,
        pin_partitions: bool,
    ) -> Result<Self> {
        let (footer, cached_footer) = SSTIterator::decode_footer(data_file.meta_bytes(), file)?;
        let index_top = read_index_block(file, &footer, u32::MAX)?;
        let read_metadata = Arc::new(SstReadMetadata::from_index_block(
            footer.clone(),
            &index_top,
        )?);
        let index_partitions = if footer.partitioned_index && pin_partitions {
            Some(
                read_metadata
                    .index_partitions()
                    .iter()
                    .enumerate()
                    .map(|(idx, &(offset, size))| {
                        read_metadata_block(file, offset, size, idx as u32)
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into(),
            )
        } else {
            None
        };
        let (filter_index, filter_partitions) = if !footer.filter_present {
            (None, None)
        } else if footer.partitioned_index {
            let filter_index = read_filter_index(file, &footer)?;
            let filters = if pin_partitions {
                Some(
                    (0..filter_index.offsets_len())
                        .map(|idx| {
                            let (offset, size) =
                                indexed_block_location(&filter_index, idx, "filter partition")?;
                            read_bloom_filter(file, offset, size)
                        })
                        .collect::<Result<Vec<_>>>()?
                        .into(),
                )
            } else {
                None
            };
            (Some(filter_index), filters)
        } else {
            validate_filter(&footer)?;
            let filter = read_bloom_filter(
                file,
                footer.filter_block_offset,
                footer.filter_block_size as usize,
            )?;
            (None, Some(vec![filter].into()))
        };
        if let Some(bytes) = cached_footer {
            data_file.set_meta_bytes(bytes);
        }
        Ok(Self {
            read_metadata,
            index_top,
            index_partitions,
            filter_index,
            filter_partitions,
        })
    }

    pub(crate) fn read_metadata(&self) -> &SstReadMetadata {
        self.read_metadata.as_ref()
    }

    pub(crate) fn index_top(&self) -> Arc<Block> {
        Arc::clone(&self.index_top)
    }

    pub(crate) fn index_partition(&self, partition_idx: usize) -> Result<Option<Arc<Block>>> {
        let Some(partitions) = &self.index_partitions else {
            return Ok(None);
        };
        partitions
            .get(partition_idx)
            .cloned()
            .map(Some)
            .ok_or_else(|| {
                Error::IoError(format!(
                    "Pinned index partition out of bounds: {partition_idx}"
                ))
            })
    }

    pub(crate) fn filter_index(&self) -> Option<Arc<Block>> {
        self.filter_index.as_ref().cloned()
    }

    pub(crate) fn filter_partition(
        &self,
        partition_idx: usize,
    ) -> Result<Option<Arc<BloomFilter>>> {
        let Some(partitions) = &self.filter_partitions else {
            return Ok(None);
        };
        let index = if self.read_metadata.footer().partitioned_index {
            partition_idx
        } else {
            0
        };
        partitions.get(index).cloned().map(Some).ok_or_else(|| {
            Error::IoError(format!(
                "Pinned filter partition out of bounds: {partition_idx}"
            ))
        })
    }
}

/// Stateless SST exact-key reader. It never constructs scan cursor state.
pub(crate) struct SSTPointReader;

impl SSTPointReader {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn get_exact(
        file: Box<dyn RandomAccessFile>,
        data_file: &DataFile,
        options: SSTIteratorOptions,
        block_cache: Option<BlockCache>,
        key: &[u8],
    ) -> Result<Option<Bytes>> {
        let metrics = SSTIterator::metrics_for(&options);
        if let Some(metadata) = PinnedSstReadMetadata::get_or_load(
            file.as_ref(),
            data_file,
            options.pin_metadata,
            options.pin_metadata_partitions,
        )? {
            return Self::get_with_pinned(
                file.as_ref(),
                data_file.file_id,
                &options,
                &block_cache,
                &metrics,
                metadata,
                key,
            );
        }

        let cached_metadata = options
            .read_metadata_cache_mode
            .caches_reads()
            .then(|| data_file.sst_read_metadata())
            .flatten();
        let (footer, cached_footer) = if let Some(metadata) = &cached_metadata {
            (metadata.footer().clone(), None)
        } else {
            SSTIterator::decode_footer(data_file.meta_bytes(), file.as_ref())?
        };
        let index_top = SSTIterator::load_index_block(
            file.as_ref(),
            data_file.file_id,
            &options,
            &block_cache,
            &metrics,
            &footer,
        )?;
        let read_metadata = match cached_metadata {
            Some(metadata) => metadata,
            None => {
                let metadata = Arc::new(SstReadMetadata::from_index_block(footer, &index_top)?);
                if options.read_metadata_cache_mode.caches_reads() {
                    data_file.set_sst_read_metadata(Arc::clone(&metadata));
                }
                if let Some(bytes) = cached_footer {
                    data_file.set_meta_bytes(bytes);
                }
                metadata
            }
        };
        Self::get_with_cached_metadata(
            file.as_ref(),
            data_file.file_id,
            &options,
            &block_cache,
            &metrics,
            &read_metadata,
            index_top,
            key,
        )
    }

    /// Looks up multiple keys in one immutable SST using per-key data-cache
    /// namespaces.
    ///
    /// Unlike repeatedly calling [`Self::get_exact`], this prepares the footer
    /// and top-level metadata once, shares filter/index partitions between
    /// keys, and reads each required data block at most once. Results retain
    /// the input order (including duplicate keys).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn get_exact_many(
        file: Box<dyn RandomAccessFile>,
        data_file: &DataFile,
        options: SSTIteratorOptions,
        block_cache: Option<BlockCache>,
        keys: &[&[u8]],
        data_cache_namespaces: &[u64],
    ) -> Result<Vec<Option<Bytes>>> {
        if keys.len() != data_cache_namespaces.len() {
            return Err(Error::InvalidState(format!(
                "SST batch key/cache namespace length mismatch: {} keys, {} namespaces",
                keys.len(),
                data_cache_namespaces.len()
            )));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let metrics = SSTIterator::metrics_for(&options);
        if let Some(pinned) = PinnedSstReadMetadata::get_or_load(
            file.as_ref(),
            data_file,
            options.pin_metadata,
            options.pin_metadata_partitions,
        )? {
            return Self::get_exact_many_with_metadata(
                file.as_ref(),
                data_file.file_id,
                &options,
                &block_cache,
                &metrics,
                pinned.read_metadata(),
                pinned.index_top(),
                Some(pinned.as_ref()),
                keys,
                data_cache_namespaces,
            );
        }

        let cached_metadata = options
            .read_metadata_cache_mode
            .caches_reads()
            .then(|| data_file.sst_read_metadata())
            .flatten();
        let (footer, cached_footer) = if let Some(metadata) = &cached_metadata {
            (metadata.footer().clone(), None)
        } else {
            SSTIterator::decode_footer(data_file.meta_bytes(), file.as_ref())?
        };
        let index_top = SSTIterator::load_index_block(
            file.as_ref(),
            data_file.file_id,
            &options,
            &block_cache,
            &metrics,
            &footer,
        )?;
        let metadata = match cached_metadata {
            Some(metadata) => metadata,
            None => {
                let metadata = Arc::new(SstReadMetadata::from_index_block(footer, &index_top)?);
                if options.read_metadata_cache_mode.caches_reads() {
                    data_file.set_sst_read_metadata(Arc::clone(&metadata));
                }
                if let Some(bytes) = cached_footer {
                    data_file.set_meta_bytes(bytes);
                }
                metadata
            }
        };
        Self::get_exact_many_with_metadata(
            file.as_ref(),
            data_file.file_id,
            &options,
            &block_cache,
            &metrics,
            metadata.as_ref(),
            index_top,
            None,
            keys,
            data_cache_namespaces,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn get_exact_many_with_metadata(
        file: &dyn RandomAccessFile,
        file_id: u64,
        options: &SSTIteratorOptions,
        block_cache: &Option<BlockCache>,
        metrics: &SSTIteratorMetrics,
        metadata: &SstReadMetadata,
        index_top: Arc<Block>,
        pinned: Option<&PinnedSstReadMetadata>,
        keys: &[&[u8]],
        data_cache_namespaces: &[u64],
    ) -> Result<Vec<Option<Bytes>>> {
        let footer = metadata.footer();
        if index_top.is_empty() {
            return Ok(vec![None; keys.len()]);
        }

        let mut by_partition: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for (slot, key) in keys.iter().enumerate() {
            by_partition
                .entry(find_partition(footer, &index_top, key)?)
                .or_default()
                .push(slot);
        }

        let mut requests = Vec::<BatchDataBlock>::new();
        let mut request_by_location = HashMap::<(u64, usize), usize>::new();
        for (partition_idx, slots) in by_partition {
            let slots = if should_check_bloom(options, footer) {
                let filter = if let Some(metadata) = pinned {
                    match metadata.filter_partition(partition_idx)? {
                        Some(filter) => filter,
                        None => {
                            let filter_index = metadata.filter_index();
                            load_cached_filter(
                                file,
                                file_id,
                                options,
                                block_cache,
                                metrics,
                                footer,
                                partition_idx,
                                filter_index.as_deref(),
                            )?
                        }
                    }
                } else {
                    load_cached_filter(
                        file,
                        file_id,
                        options,
                        block_cache,
                        metrics,
                        footer,
                        partition_idx,
                        None,
                    )?
                };
                slots
                    .into_iter()
                    .filter(|slot| filter.may_contain(keys[*slot]))
                    .collect::<Vec<_>>()
            } else {
                slots
            };
            if slots.is_empty() {
                continue;
            }
            Self::add_partition_requests(
                file,
                file_id,
                options,
                block_cache,
                metrics,
                metadata,
                &index_top,
                pinned,
                partition_idx,
                slots,
                keys,
                data_cache_namespaces,
                &mut requests,
                &mut request_by_location,
            )?;
        }

        for request in &mut requests {
            request.data_cache_namespaces.sort_unstable();
            request.data_cache_namespaces.dedup();
        }

        let blocks =
            Self::load_many_data_blocks(file, file_id, block_cache, metrics, footer, &requests)?;
        let mut out = vec![None; keys.len()];
        for (request, block) in requests.iter().zip(blocks) {
            for slot in &request.key_slots {
                out[*slot] = block
                    .get_exact(keys[*slot])?
                    .map(|value| normalize_encoded_value(footer, value));
            }
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    fn add_partition_requests(
        file: &dyn RandomAccessFile,
        file_id: u64,
        options: &SSTIteratorOptions,
        block_cache: &Option<BlockCache>,
        metrics: &SSTIteratorMetrics,
        metadata: &SstReadMetadata,
        index_top: &Arc<Block>,
        pinned: Option<&PinnedSstReadMetadata>,
        partition_idx: usize,
        slots: Vec<usize>,
        keys: &[&[u8]],
        data_cache_namespaces: &[u64],
        requests: &mut Vec<BatchDataBlock>,
        request_by_location: &mut HashMap<(u64, usize), usize>,
    ) -> Result<()> {
        let footer = metadata.footer();
        let partition = if footer.partitioned_index {
            match pinned
                .map(|metadata| metadata.index_partition(partition_idx))
                .transpose()?
                .flatten()
            {
                Some(partition) => partition,
                None => {
                    let (offset, size) = metadata.index_partitions()[partition_idx];
                    load_cached_block(
                        file,
                        file_id,
                        options,
                        block_cache,
                        metrics,
                        offset,
                        size,
                        partition_idx as u32,
                        BlockCacheKind::IndexPartition,
                    )?
                }
            }
        } else {
            // The top-level index is the only index partition here and was
            // already loaded while preparing the shared metadata.
            Arc::clone(index_top)
        };
        for slot in slots {
            let block_idx = partition.find_lower_or_equal_idx(&unsafe_bytes(keys[slot]))?;
            let (offset, size) = indexed_block_location(&partition, block_idx, "data")?;
            let request_slot = match request_by_location.entry((offset, size)) {
                std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let request_slot = requests.len();
                    requests.push(BatchDataBlock {
                        offset,
                        size,
                        block_id: block_idx as u32,
                        key_slots: Vec::new(),
                        data_cache_namespaces: Vec::new(),
                    });
                    entry.insert(request_slot);
                    request_slot
                }
            };
            requests[request_slot].key_slots.push(slot);
            requests[request_slot]
                .data_cache_namespaces
                .push(data_cache_namespaces[slot]);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn load_many_data_blocks(
        file: &dyn RandomAccessFile,
        file_id: u64,
        block_cache: &Option<BlockCache>,
        metrics: &SSTIteratorMetrics,
        footer: &Footer,
        requests: &[BatchDataBlock],
    ) -> Result<Vec<Arc<Block>>> {
        let mut blocks = vec![None; requests.len()];
        let mut misses = Vec::new();
        for (idx, request) in requests.iter().enumerate() {
            if let Some(cache) = block_cache {
                let mut cached_block = None;
                for namespace in &request.data_cache_namespaces {
                    let cache_key = BlockCacheKey {
                        namespace: *namespace,
                        file_id,
                        block_id: request.offset,
                        kind: BlockCacheKind::Data,
                    };
                    if let Some(cached) = cache.get(&cache_key) {
                        let block = match cached {
                            CachedBlock::Block(block) => block,
                            _ => {
                                return Err(Error::IoError(
                                    "Data block cache entry invalid".to_string(),
                                ));
                            }
                        };
                        cached_block = Some(block);
                        break;
                    }
                }
                if let Some(block) = cached_block {
                    metrics.record_data_hit();
                    for namespace in &request.data_cache_namespaces {
                        cache.insert(
                            BlockCacheKey {
                                namespace: *namespace,
                                file_id,
                                block_id: request.offset,
                                kind: BlockCacheKind::Data,
                            },
                            CachedBlock::Block(Arc::clone(&block)),
                        );
                    }
                    blocks[idx] = Some(block);
                    continue;
                }
                metrics.record_data_miss();
            }
            misses.push(idx);
        }
        misses.sort_by_key(|idx| requests[*idx].offset);
        let mut start = 0;
        while start < misses.len() {
            let first = misses[start];
            let range_start = requests[first].offset;
            let first_size = u64::try_from(requests[first].size).map_err(|_| {
                Error::IoError("SST batch data block size does not fit u64".to_string())
            })?;
            let mut range_end = range_start.checked_add(first_size).ok_or_else(|| {
                Error::IoError("SST batch data block range overflows u64".to_string())
            })?;
            let mut end = start + 1;
            while end < misses.len() {
                let next = &requests[misses[end]];
                let next_size = u64::try_from(next.size).map_err(|_| {
                    Error::IoError("SST batch data block size does not fit u64".to_string())
                })?;
                let next_end = next.offset.checked_add(next_size).ok_or_else(|| {
                    Error::IoError("SST batch data block range overflows u64".to_string())
                })?;
                if next.offset != range_end
                    || next_end.saturating_sub(range_start) > MAX_BATCH_DATA_READ_BYTES as u64
                {
                    break;
                }
                range_end = next_end;
                end += 1;
            }
            let range_len = usize::try_from(range_end - range_start).map_err(|_| {
                Error::IoError("SST batch data range does not fit usize".to_string())
            })?;
            let range_offset = usize::try_from(range_start).map_err(|_| {
                Error::IoError("SST batch data offset does not fit usize".to_string())
            })?;
            let bytes = file.read_at(range_offset, range_len)?;
            if bytes.len() != range_len {
                return Err(Error::IoError(format!(
                    "Short SST batch data read at offset {range_start}: expected {range_len} bytes, got {}",
                    bytes.len()
                )));
            }
            for request_idx in &misses[start..end] {
                let request = &requests[*request_idx];
                let begin =
                    usize::try_from(request.offset.checked_sub(range_start).ok_or_else(|| {
                        Error::IoError("SST batch data block precedes merged read".to_string())
                    })?)
                    .map_err(|_| {
                        Error::IoError("SST batch data block offset does not fit usize".to_string())
                    })?;
                let end = begin.checked_add(request.size).ok_or_else(|| {
                    Error::IoError("SST batch data block range overflow".to_string())
                })?;
                if end > bytes.len() {
                    return Err(Error::IoError(format!(
                        "SST batch data block exceeds merged read at offset {}",
                        request.offset
                    )));
                }
                let encoded = bytes.slice(begin..end);
                let verified =
                    verify_block_checksum(encoded, footer.block_checksums, "SST data block")?;
                let decoded = decode_block_bytes(verified)?;
                let mut block = Block::decode(decoded)?;
                block.set_block_id(request.block_id);
                let block = Arc::new(block);
                if let Some(cache) = block_cache {
                    for namespace in &request.data_cache_namespaces {
                        cache.insert(
                            BlockCacheKey {
                                namespace: *namespace,
                                file_id,
                                block_id: request.offset,
                                kind: BlockCacheKind::Data,
                            },
                            CachedBlock::Block(Arc::clone(&block)),
                        );
                    }
                }
                blocks[*request_idx] = Some(block);
            }
            start = end;
        }
        blocks
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| Error::IoError("Missing batch data block".to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    fn get_with_pinned(
        file: &dyn RandomAccessFile,
        file_id: u64,
        options: &SSTIteratorOptions,
        block_cache: &Option<BlockCache>,
        metrics: &SSTIteratorMetrics,
        metadata: Arc<PinnedSstReadMetadata>,
        key: &[u8],
    ) -> Result<Option<Bytes>> {
        let footer = metadata.read_metadata().footer();
        let index_top = metadata.index_top();
        if index_top.is_empty() {
            return Ok(None);
        }
        let partition_idx = find_partition(footer, &index_top, key)?;
        if should_check_bloom(options, footer) {
            let filter = match metadata.filter_partition(partition_idx)? {
                Some(filter) => filter,
                None => load_cached_filter(
                    file,
                    file_id,
                    options,
                    block_cache,
                    metrics,
                    footer,
                    partition_idx,
                    metadata.filter_index().as_deref(),
                )?,
            };
            if !filter.may_contain(key) {
                return Ok(None);
            }
        }
        let partition = if footer.partitioned_index {
            match metadata.index_partition(partition_idx)? {
                Some(partition) => partition,
                None => {
                    let (offset, size) = metadata.read_metadata().index_partitions()[partition_idx];
                    load_cached_block(
                        file,
                        file_id,
                        options,
                        block_cache,
                        metrics,
                        offset,
                        size,
                        partition_idx as u32,
                        BlockCacheKind::IndexPartition,
                    )?
                }
            }
        } else {
            index_top
        };
        Self::read_exact_data_block(
            file,
            file_id,
            options,
            block_cache,
            metrics,
            footer,
            &partition,
            key,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn get_with_cached_metadata(
        file: &dyn RandomAccessFile,
        file_id: u64,
        options: &SSTIteratorOptions,
        block_cache: &Option<BlockCache>,
        metrics: &SSTIteratorMetrics,
        metadata: &SstReadMetadata,
        index_top: Arc<Block>,
        key: &[u8],
    ) -> Result<Option<Bytes>> {
        let footer = metadata.footer();
        if index_top.is_empty() {
            return Ok(None);
        }
        let partition_idx = find_partition(footer, &index_top, key)?;
        if should_check_bloom(options, footer)
            && !load_cached_filter(
                file,
                file_id,
                options,
                block_cache,
                metrics,
                footer,
                partition_idx,
                None,
            )?
            .may_contain(key)
        {
            return Ok(None);
        }
        let partition = if footer.partitioned_index {
            let (offset, size) = metadata.index_partitions()[partition_idx];
            load_cached_block(
                file,
                file_id,
                options,
                block_cache,
                metrics,
                offset,
                size,
                partition_idx as u32,
                BlockCacheKind::IndexPartition,
            )?
        } else {
            index_top
        };
        Self::read_exact_data_block(
            file,
            file_id,
            options,
            block_cache,
            metrics,
            footer,
            &partition,
            key,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn read_exact_data_block(
        file: &dyn RandomAccessFile,
        file_id: u64,
        options: &SSTIteratorOptions,
        block_cache: &Option<BlockCache>,
        metrics: &SSTIteratorMetrics,
        footer: &Footer,
        partition: &Block,
        key: &[u8],
    ) -> Result<Option<Bytes>> {
        let block_idx = partition.find_lower_or_equal_idx(&unsafe_bytes(key))?;
        let (offset, size) = indexed_block_location(partition, block_idx, "data")?;
        let cache_key = BlockCacheKey {
            namespace: options.cache_namespace,
            file_id,
            block_id: offset,
            kind: BlockCacheKind::Data,
        };
        let block = if let Some(cache) = block_cache {
            if let Some(cached) = cache.get(&cache_key) {
                metrics.record_data_hit();
                match cached {
                    CachedBlock::Block(block) => block,
                    _ => return Err(Error::IoError("Data block cache entry invalid".to_string())),
                }
            } else {
                metrics.record_data_miss();
                let block = read_data_block(file, footer, offset, size, block_idx as u32)?;
                cache.insert(cache_key, CachedBlock::Block(Arc::clone(&block)));
                block
            }
        } else {
            read_data_block(file, footer, offset, size, block_idx as u32)?
        };
        Ok(block
            .get_exact(key)?
            .map(|value| normalize_encoded_value(footer, value)))
    }
}

fn should_check_bloom(options: &SSTIteratorOptions, footer: &Footer) -> bool {
    options.bloom_filter_enabled && footer.filter_present && footer.filter_block_size > 0
}

fn find_partition(footer: &Footer, index_top: &Block, key: &[u8]) -> Result<usize> {
    if footer.partitioned_index {
        index_top.find_lower_or_equal_idx(&unsafe_bytes(key))
    } else {
        Ok(0)
    }
}

fn validate_filter(footer: &Footer) -> Result<()> {
    if !footer.filter_present || footer.filter_block_size == 0 {
        return Err(Error::IoError("SST filter block is missing".to_string()));
    }
    Ok(())
}

fn read_index_block(
    file: &dyn RandomAccessFile,
    footer: &Footer,
    block_id: u32,
) -> Result<Arc<Block>> {
    read_metadata_block(
        file,
        footer.index_block_offset,
        footer.index_block_size,
        block_id,
    )
}

fn read_filter_index(file: &dyn RandomAccessFile, footer: &Footer) -> Result<Arc<Block>> {
    validate_filter(footer)?;
    read_metadata_block(
        file,
        footer.filter_block_offset,
        footer.filter_block_size,
        u32::MAX - 1,
    )
}

#[allow(clippy::too_many_arguments)]
fn load_cached_block(
    file: &dyn RandomAccessFile,
    file_id: u64,
    options: &SSTIteratorOptions,
    block_cache: &Option<BlockCache>,
    metrics: &SSTIteratorMetrics,
    offset: u64,
    size: u64,
    block_id: u32,
    kind: BlockCacheKind,
) -> Result<Arc<Block>> {
    let cache_key = BlockCacheKey {
        namespace: options.cache_namespace,
        file_id,
        block_id: offset,
        kind,
    };
    if let Some(cache) = block_cache {
        if let Some(cached) = cache.get(&cache_key) {
            if kind == BlockCacheKind::FilterIndex {
                metrics.record_filter_hit();
            } else {
                metrics.record_index_hit();
            }
            match cached {
                CachedBlock::Block(block) => return Ok(block),
                _ => return Err(Error::IoError("Index cache entry invalid".to_string())),
            }
        }
        if kind == BlockCacheKind::FilterIndex {
            metrics.record_filter_miss();
        } else {
            metrics.record_index_miss();
        }
        let block = read_metadata_block(file, offset, size, block_id)?;
        cache.insert(cache_key, CachedBlock::Block(Arc::clone(&block)));
        Ok(block)
    } else {
        read_metadata_block(file, offset, size, block_id)
    }
}

#[allow(clippy::too_many_arguments)]
fn load_cached_filter(
    file: &dyn RandomAccessFile,
    file_id: u64,
    options: &SSTIteratorOptions,
    block_cache: &Option<BlockCache>,
    metrics: &SSTIteratorMetrics,
    footer: &Footer,
    partition_idx: usize,
    pinned_filter_index: Option<&Block>,
) -> Result<Arc<BloomFilter>> {
    let (offset, size, kind) = if footer.partitioned_index {
        let cached_filter_index;
        let filter_index = if let Some(filter_index) = pinned_filter_index {
            filter_index
        } else {
            cached_filter_index = load_cached_block(
                file,
                file_id,
                options,
                block_cache,
                metrics,
                footer.filter_block_offset,
                footer.filter_block_size,
                u32::MAX - 1,
                BlockCacheKind::FilterIndex,
            )?;
            cached_filter_index.as_ref()
        };
        let (offset, size) =
            indexed_block_location(filter_index, partition_idx, "filter partition")?;
        (offset, size, BlockCacheKind::FilterPartition)
    } else {
        (
            footer.filter_block_offset,
            footer.filter_block_size as usize,
            BlockCacheKind::FilterPartition,
        )
    };
    if size == 0 {
        return Err(Error::IoError("Filter block size is zero".to_string()));
    }
    let cache_key = BlockCacheKey {
        namespace: options.cache_namespace,
        file_id,
        block_id: offset,
        kind,
    };
    if let Some(cache) = block_cache {
        if let Some(cached) = cache.get(&cache_key) {
            metrics.record_filter_hit();
            match cached {
                CachedBlock::BloomFilter(filter) => return Ok(filter),
                _ => return Err(Error::IoError("Filter cache entry invalid".to_string())),
            }
        }
        metrics.record_filter_miss();
        let filter = read_bloom_filter(file, offset, size)?;
        cache.insert(cache_key, CachedBlock::BloomFilter(Arc::clone(&filter)));
        Ok(filter)
    } else {
        read_bloom_filter(file, offset, size)
    }
}

fn normalize_encoded_value(footer: &Footer, value: Bytes) -> Bytes {
    if footer.value_has_ttl {
        return value;
    }
    let mut out = BytesMut::with_capacity(value.len() + 4);
    out.put_u32_le(0);
    out.extend_from_slice(value.as_ref());
    out.freeze()
}

#[cfg(test)]
#[path = "../../tests/unit/sst/point_reader.rs"]
mod tests;
