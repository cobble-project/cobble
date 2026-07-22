use crate::cache::{BlockCache, BlockCacheKey, BlockCacheKind, CachedBlock};
use crate::data_file::DataFile;
use crate::error::{Error, Result};
use crate::file::RandomAccessFile;
use crate::sst::bloom::BloomFilter;
use crate::sst::format::{Block, Footer, SstReadMetadata};
use crate::sst::iterator::{SSTIterator, SSTIteratorMetrics, SSTIteratorOptions};
use crate::sst::read::{
    indexed_block_location, read_bloom_filter, read_data_block, read_metadata_block,
};
use crate::util::unsafe_bytes;
use bytes::{BufMut, Bytes, BytesMut};
use std::sync::Arc;

/// Immutable SST read metadata held by a `DataFile`, outside the block-cache budget.
#[derive(Debug)]
pub(crate) struct PinnedSstReadMetadata {
    read_metadata: Arc<SstReadMetadata>,
    index_top: Arc<Block>,
    index_partitions: Arc<[Arc<Block>]>,
    filter_index: Option<Arc<Block>>,
    filter_partitions: Arc<[Arc<BloomFilter>]>,
}

impl PinnedSstReadMetadata {
    pub(crate) fn get_or_load(
        file: &dyn RandomAccessFile,
        data_file: &DataFile,
        pin_eligible: bool,
    ) -> Result<Option<Arc<Self>>> {
        if let Some(metadata) = data_file.pinned_sst_read_metadata() {
            return Ok(Some(metadata));
        }
        if !pin_eligible {
            return Ok(None);
        }

        // The file is immutable. Competing first readers may duplicate this work, but OnceLock
        // publishes only one fully-built Arc and failures never become visible as a pin.
        let metadata = Arc::new(Self::load(file, data_file)?);
        data_file.set_pinned_sst_read_metadata(metadata);
        Ok(data_file.pinned_sst_read_metadata())
    }

    fn load(file: &dyn RandomAccessFile, data_file: &DataFile) -> Result<Self> {
        let (footer, cached_footer) = SSTIterator::decode_footer(data_file.meta_bytes(), file)?;
        let index_top = read_index_block(file, &footer, u32::MAX)?;
        let read_metadata = Arc::new(SstReadMetadata::from_index_block(
            footer.clone(),
            &index_top,
        )?);
        let index_partitions = if footer.partitioned_index {
            read_metadata
                .index_partitions()
                .iter()
                .enumerate()
                .map(|(idx, &(offset, size))| read_metadata_block(file, offset, size, idx as u32))
                .collect::<Result<Vec<_>>>()?
                .into()
        } else {
            vec![Arc::clone(&index_top)].into()
        };
        let (filter_index, filter_partitions) = if !footer.filter_present {
            (None, Vec::new().into())
        } else if footer.partitioned_index {
            let filter_index = read_filter_index(file, &footer)?;
            let filters = (0..filter_index.offsets_len())
                .map(|idx| {
                    let (offset, size) =
                        indexed_block_location(&filter_index, idx, "filter partition")?;
                    read_bloom_filter(file, offset, size)
                })
                .collect::<Result<Vec<_>>>()?;
            (Some(filter_index), filters.into())
        } else {
            validate_filter(&footer)?;
            let filter = read_bloom_filter(
                file,
                footer.filter_block_offset,
                footer.filter_block_size as usize,
            )?;
            (None, vec![filter].into())
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

    pub(crate) fn index_partition(&self, partition_idx: usize) -> Result<Arc<Block>> {
        self.index_partitions
            .get(partition_idx)
            .cloned()
            .ok_or_else(|| {
                Error::IoError(format!(
                    "Pinned index partition out of bounds: {partition_idx}"
                ))
            })
    }

    pub(crate) fn filter_index(&self) -> Result<Arc<Block>> {
        self.filter_index
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::IoError("Pinned SST has no filter index".to_string()))
    }

    pub(crate) fn filter_partition(&self, partition_idx: usize) -> Result<Arc<BloomFilter>> {
        let index = if self.read_metadata.footer().partitioned_index {
            partition_idx
        } else {
            0
        };
        self.filter_partitions.get(index).cloned().ok_or_else(|| {
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
        if let Some(metadata) =
            PinnedSstReadMetadata::get_or_load(file.as_ref(), data_file, options.pin_metadata)?
        {
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
        if should_check_bloom(options, footer)
            && !metadata.filter_partition(partition_idx)?.may_contain(key)
        {
            return Ok(None);
        }
        let partition = if footer.partitioned_index {
            metadata.index_partition(partition_idx)?
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
) -> Result<Arc<BloomFilter>> {
    let (offset, size, kind) = if footer.partitioned_index {
        let filter_index = load_cached_block(
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
        let (offset, size) =
            indexed_block_location(&filter_index, partition_idx, "filter partition")?;
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
mod tests {
    use super::*;
    use crate::cache::CacheHandle;
    use crate::data_file::DataFileType;
    use crate::file::{File, FileSystemRegistry};
    use crate::sst::{SSTIterator, SSTWriter, SSTWriterOptions};
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingCache {
        entries: Mutex<HashMap<BlockCacheKey, CachedBlock>>,
        gets: Mutex<Vec<BlockCacheKind>>,
    }

    impl RecordingCache {
        fn clear_history(&self) {
            self.gets.lock().unwrap().clear();
        }

        fn requested_kinds(&self) -> Vec<BlockCacheKind> {
            self.gets.lock().unwrap().clone()
        }
    }

    impl CacheHandle<BlockCacheKey, CachedBlock> for RecordingCache {
        fn get(&self, key: &BlockCacheKey) -> Option<CachedBlock> {
            self.gets.lock().unwrap().push(key.kind.clone());
            self.entries.lock().unwrap().get(key).cloned()
        }

        fn insert(&self, key: BlockCacheKey, value: CachedBlock) {
            self.entries.lock().unwrap().insert(key, value);
        }

        fn remove(&self, key: &BlockCacheKey) {
            self.entries.lock().unwrap().remove(key);
        }

        fn clear(&self) {
            self.entries.lock().unwrap().clear();
        }
    }

    fn write_sst(
        partitioned_index: bool,
    ) -> (
        tempfile::TempDir,
        Arc<dyn crate::file::FileSystem>,
        DataFile,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", directory.path().display()))
            .unwrap();
        let mut writer = SSTWriter::new(
            fs.open_write("point-read.sst").unwrap(),
            SSTWriterOptions {
                block_size: 32,
                bloom_filter_enabled: true,
                bloom_bits_per_key: 100,
                partitioned_index,
                block_checksum_enabled: false,
                ..SSTWriterOptions::default()
            },
        );
        for key in [b"key000".as_slice(), b"key001", b"key002", b"key003"] {
            writer.add(key, b"value").unwrap();
        }
        writer.finish().unwrap();
        let size = fs.open_read("point-read.sst").unwrap().size();
        let data_file = DataFile::new_detached(
            DataFileType::SSTable,
            b"key000".to_vec(),
            b"key003".to_vec(),
            73,
            0,
            size,
            0..=0,
            0..=0,
        );
        (directory, fs, data_file)
    }

    fn write_partitioned_sst() -> (
        tempfile::TempDir,
        Arc<dyn crate::file::FileSystem>,
        DataFile,
    ) {
        write_sst(true)
    }

    fn options(pin_metadata: bool) -> SSTIteratorOptions {
        SSTIteratorOptions {
            bloom_filter_enabled: true,
            pin_metadata,
            ..SSTIteratorOptions::default()
        }
    }

    fn assert_only_data_block_cache_accesses(cache: &RecordingCache) {
        let kinds = cache.requested_kinds();
        assert!(!kinds.is_empty());
        assert!(kinds.iter().all(|kind| *kind == BlockCacheKind::Data));
    }

    #[test]
    fn point_reader_hits_and_misses_without_constructing_scan_state() {
        let (_directory, fs, data_file) = write_partitioned_sst();

        assert_eq!(
            SSTPointReader::get_exact(
                fs.open_read("point-read.sst").unwrap(),
                &data_file,
                options(false),
                None,
                b"key002",
            )
            .unwrap()
            .as_deref(),
            Some(b"value".as_slice())
        );
        assert!(
            SSTPointReader::get_exact(
                fs.open_read("point-read.sst").unwrap(),
                &data_file,
                options(false),
                None,
                b"missing",
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn point_reader_supports_unpartitioned_index_and_filter() {
        let (_directory, fs, data_file) = write_sst(false);

        assert_eq!(
            SSTPointReader::get_exact(
                fs.open_read("point-read.sst").unwrap(),
                &data_file,
                options(true),
                None,
                b"key001",
            )
            .unwrap()
            .as_deref(),
            Some(b"value".as_slice())
        );
        assert!(data_file.pinned_sst_read_metadata().is_some());
    }

    #[test]
    fn point_read_pin_is_reused_by_scan_and_compaction_style_iterators() {
        let (_directory, fs, data_file) = write_partitioned_sst();
        let recording_cache = Arc::new(RecordingCache::default());
        let block_cache: BlockCache = recording_cache.clone();

        assert_eq!(
            SSTPointReader::get_exact(
                fs.open_read("point-read.sst").unwrap(),
                &data_file,
                options(true),
                Some(block_cache.clone()),
                b"key002",
            )
            .unwrap()
            .as_deref(),
            Some(b"value".as_slice())
        );
        assert!(data_file.pinned_sst_read_metadata().is_some());
        assert_only_data_block_cache_accesses(&recording_cache);

        recording_cache.clear_history();
        let mut scan_iter = SSTIterator::with_cache_and_file(
            fs.open_read("point-read.sst").unwrap(),
            &data_file,
            options(false),
            Some(block_cache.clone()),
        )
        .unwrap();
        scan_iter.seek_to_first().unwrap();
        assert!(scan_iter.valid());
        assert_only_data_block_cache_accesses(&recording_cache);

        recording_cache.clear_history();
        let mut compaction_iter = SSTIterator::with_cache_and_file(
            fs.open_read("point-read.sst").unwrap(),
            &data_file,
            SSTIteratorOptions {
                observe_hot_blocks: true,
                ..options(false)
            },
            Some(block_cache),
        )
        .unwrap();
        compaction_iter.seek_to_first().unwrap();
        assert!(compaction_iter.valid());
        assert_only_data_block_cache_accesses(&recording_cache);
    }

    #[test]
    fn scan_created_pin_is_reused_by_point_read() {
        let (_directory, fs, data_file) = write_partitioned_sst();
        let recording_cache = Arc::new(RecordingCache::default());
        let block_cache: BlockCache = recording_cache.clone();

        let mut scan_iter = SSTIterator::with_cache_and_file(
            fs.open_read("point-read.sst").unwrap(),
            &data_file,
            options(true),
            Some(block_cache.clone()),
        )
        .unwrap();
        scan_iter.seek_to_first().unwrap();
        let mut keys = Vec::new();
        while scan_iter.valid() {
            keys.push(scan_iter.key().unwrap().unwrap().to_vec());
            scan_iter.next().unwrap();
        }
        assert_eq!(
            keys,
            [b"key000", b"key001", b"key002", b"key003"]
                .into_iter()
                .map(|key| key.to_vec())
                .collect::<Vec<_>>()
        );
        assert!(data_file.pinned_sst_read_metadata().is_some());
        assert_only_data_block_cache_accesses(&recording_cache);

        recording_cache.clear_history();
        assert_eq!(
            SSTPointReader::get_exact(
                fs.open_read("point-read.sst").unwrap(),
                &data_file,
                options(false),
                Some(block_cache),
                b"key002",
            )
            .unwrap()
            .as_deref(),
            Some(b"value".as_slice())
        );
        assert_only_data_block_cache_accesses(&recording_cache);
    }

    #[test]
    fn metadata_pin_level_gate_can_leave_a_file_unpinned() {
        let (_directory, fs, data_file) = write_partitioned_sst();
        let reader = fs.open_read("point-read.sst").unwrap();

        assert!(
            PinnedSstReadMetadata::get_or_load(reader.as_ref(), &data_file, false,)
                .unwrap()
                .is_none()
        );
        assert!(data_file.pinned_sst_read_metadata().is_none());
        let reader = fs.open_read("point-read.sst").unwrap();
        assert!(
            PinnedSstReadMetadata::get_or_load(reader.as_ref(), &data_file, true,)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn existing_pin_wins_over_a_later_ineligible_path() {
        let (_directory, fs, data_file) = write_partitioned_sst();
        let reader = fs.open_read("point-read.sst").unwrap();
        let pin = PinnedSstReadMetadata::get_or_load(reader.as_ref(), &data_file, true)
            .unwrap()
            .unwrap();
        let reader = fs.open_read("point-read.sst").unwrap();
        let reused = PinnedSstReadMetadata::get_or_load(reader.as_ref(), &data_file, false)
            .unwrap()
            .unwrap();

        assert!(Arc::ptr_eq(&pin, &reused));
    }
}
