use super::{CacheHandle, FoyerCache};
use crate::Config;
use crate::config::HybridCacheVolumePlan;
use crate::error::{Error, Result};
use crate::sst::bloom::BloomFilter;
use crate::sst::format::Block;
use bytes::Bytes;
use dashmap::DashMap;
use foyer::{Code, Error as FoyerError};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{io::Read, io::Write, path::PathBuf};
use url::Url;

#[derive(Clone)]
pub enum CachedBlock {
    Block(Arc<Block>),
    BloomFilter(Arc<BloomFilter>),
    ParquetBlock(Bytes),
}

impl CachedBlock {
    pub fn size_in_bytes(&self) -> usize {
        match self {
            CachedBlock::Block(block) => block.size_in_bytes(),
            CachedBlock::BloomFilter(filter) => filter.size_in_bytes(),
            CachedBlock::ParquetBlock(bytes) => bytes.len(),
        }
    }
}

pub type BlockCache = Arc<dyn CacheHandle<BlockCacheKey, CachedBlock>>;

/// Tracks data blocks currently adjacent to cursor-driven scans.
///
/// Scan iterators keep only their current and next SST data block in this map.
/// Compaction iterators check the same block-cache keys when reading input; if
/// they touch one, the compaction writer can directly seed the output block cache
/// for the block that receives those hot keys. Values are reference counts so
/// overlapping scans do not clear each other's hot blocks when one iterator
/// advances or drops.
pub(crate) struct ScanHotBlockRegistry {
    hot_blocks: DashMap<BlockCacheKey, usize>,
    observed_hot_blocks: AtomicU64,
}

impl ScanHotBlockRegistry {
    pub(crate) fn new() -> Self {
        Self {
            hot_blocks: DashMap::new(),
            observed_hot_blocks: AtomicU64::new(0),
        }
    }

    pub(crate) fn handle(self: &Arc<Self>) -> ScanHotBlockHandle {
        ScanHotBlockHandle {
            registry: Arc::clone(self),
            blocks: Vec::new(),
        }
    }

    pub(crate) fn observe_if_hot(&self, key: BlockCacheKey) {
        if self.hot_blocks.contains_key(&key) {
            self.observed_hot_blocks.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub(crate) fn observed_count(&self) -> u64 {
        self.observed_hot_blocks.load(Ordering::Acquire)
    }

    pub(crate) fn from_keys(keys: impl IntoIterator<Item = BlockCacheKey>) -> Arc<Self> {
        let registry = Arc::new(Self::new());
        for key in keys {
            registry.acquire(key);
        }
        registry
    }

    pub(crate) fn snapshot_keys(&self) -> Vec<BlockCacheKey> {
        self.hot_blocks.iter().map(|entry| *entry.key()).collect()
    }

    fn acquire(&self, key: BlockCacheKey) {
        self.hot_blocks
            .entry(key)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    fn release(&self, key: BlockCacheKey) {
        if let Some(mut count) = self.hot_blocks.get_mut(&key)
            && *count > 1
        {
            *count -= 1;
            return;
        }
        self.hot_blocks.remove(&key);
    }
}

pub(crate) struct ScanHotBlockHandle {
    registry: Arc<ScanHotBlockRegistry>,
    blocks: Vec<BlockCacheKey>,
}

impl ScanHotBlockHandle {
    pub(crate) fn replace(&mut self, keys: Vec<BlockCacheKey>) {
        for key in self.blocks.drain(..) {
            self.registry.release(key);
        }
        for key in &keys {
            self.registry.acquire(*key);
        }
        self.blocks = keys;
    }
}

impl Drop for ScanHotBlockHandle {
    fn drop(&mut self) {
        for key in self.blocks.drain(..) {
            self.registry.release(key);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum BlockCacheKind {
    Data,
    IndexPartition,
    IndexTop,
    FilterPartition,
    FilterIndex,
    ParquetData(u32),
}

impl Code for BlockCacheKind {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        match self {
            BlockCacheKind::Data => writer.write_all(&[0u8]).map_err(FoyerError::io_error),
            BlockCacheKind::IndexPartition => {
                writer.write_all(&[1u8]).map_err(FoyerError::io_error)
            }
            BlockCacheKind::IndexTop => writer.write_all(&[2u8]).map_err(FoyerError::io_error),
            BlockCacheKind::FilterPartition => {
                writer.write_all(&[3u8]).map_err(FoyerError::io_error)
            }
            BlockCacheKind::FilterIndex => writer.write_all(&[4u8]).map_err(FoyerError::io_error),
            BlockCacheKind::ParquetData(length) => {
                writer.write_all(&[5u8]).map_err(FoyerError::io_error)?;
                writer
                    .write_all(&length.to_le_bytes())
                    .map_err(FoyerError::io_error)
            }
        }
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self>
    where
        Self: Sized,
    {
        let mut tag = [0u8; 1];
        reader.read_exact(&mut tag).map_err(FoyerError::io_error)?;
        match tag[0] {
            0 => Ok(BlockCacheKind::Data),
            1 => Ok(BlockCacheKind::IndexPartition),
            2 => Ok(BlockCacheKind::IndexTop),
            3 => Ok(BlockCacheKind::FilterPartition),
            4 => Ok(BlockCacheKind::FilterIndex),
            5 => {
                let mut length = [0u8; 4];
                reader
                    .read_exact(&mut length)
                    .map_err(FoyerError::io_error)?;
                Ok(BlockCacheKind::ParquetData(u32::from_le_bytes(length)))
            }
            kind => Err(FoyerError::io_error(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unknown block cache kind tag: {}", kind),
            ))),
        }
    }

    fn estimated_size(&self) -> usize {
        match self {
            BlockCacheKind::ParquetData(_) => 1 + 4,
            _ => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct BlockCacheKey {
    pub namespace: u64,
    pub file_id: u64,
    pub block_id: u64,
    pub kind: BlockCacheKind,
}

pub(crate) fn bucket_scoped_cache_namespace(base_namespace: u64, bucket: u16) -> u64 {
    base_namespace ^ ((bucket as u64) << 48)
}

pub(crate) fn cache_namespace_for_db_id(db_id: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    db_id.hash(&mut hasher);
    hasher.finish()
}

pub(crate) fn data_block_cache_key(
    namespace: u64,
    file_id: u64,
    block_offset: u64,
) -> BlockCacheKey {
    BlockCacheKey {
        namespace,
        file_id,
        block_id: block_offset,
        kind: BlockCacheKind::Data,
    }
}

impl Code for BlockCacheKey {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        writer
            .write_all(&self.namespace.to_le_bytes())
            .map_err(FoyerError::io_error)?;
        writer
            .write_all(&self.file_id.to_le_bytes())
            .map_err(FoyerError::io_error)?;
        writer
            .write_all(&self.block_id.to_le_bytes())
            .map_err(FoyerError::io_error)?;
        self.kind.encode(writer)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self>
    where
        Self: Sized,
    {
        let mut namespace = [0u8; 8];
        let mut file_id = [0u8; 8];
        let mut block_id = [0u8; 8];
        reader
            .read_exact(&mut namespace)
            .map_err(FoyerError::io_error)?;
        reader
            .read_exact(&mut file_id)
            .map_err(FoyerError::io_error)?;
        reader
            .read_exact(&mut block_id)
            .map_err(FoyerError::io_error)?;
        let kind = BlockCacheKind::decode(reader)?;
        Ok(Self {
            namespace: u64::from_le_bytes(namespace),
            file_id: u64::from_le_bytes(file_id),
            block_id: u64::from_le_bytes(block_id),
            kind,
        })
    }

    fn estimated_size(&self) -> usize {
        8 + 8 + 8 + self.kind.estimated_size()
    }
}

impl Code for CachedBlock {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        let (tag, payload) = match self {
            CachedBlock::Block(block) => (0u8, block.encode()),
            CachedBlock::BloomFilter(filter) => (1u8, filter.encode()),
            CachedBlock::ParquetBlock(bytes) => (2u8, bytes.clone()),
        };
        writer.write_all(&[tag]).map_err(FoyerError::io_error)?;
        let len = payload.len() as u32;
        writer
            .write_all(&len.to_le_bytes())
            .map_err(FoyerError::io_error)?;
        writer.write_all(&payload).map_err(FoyerError::io_error)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self>
    where
        Self: Sized,
    {
        let mut tag = [0u8; 1];
        let mut len = [0u8; 4];
        reader.read_exact(&mut tag).map_err(FoyerError::io_error)?;
        reader.read_exact(&mut len).map_err(FoyerError::io_error)?;
        let len = u32::from_le_bytes(len) as usize;
        let mut data = vec![0u8; len];
        reader.read_exact(&mut data).map_err(FoyerError::io_error)?;
        let payload = Bytes::from(data);
        match tag[0] {
            0 => {
                let block = Block::decode(payload).map_err(|err| {
                    FoyerError::io_error(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        err.to_string(),
                    ))
                })?;
                Ok(CachedBlock::Block(Arc::new(block)))
            }
            1 => {
                let filter = BloomFilter::decode(payload).map_err(|err| {
                    FoyerError::io_error(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        err.to_string(),
                    ))
                })?;
                Ok(CachedBlock::BloomFilter(Arc::new(filter)))
            }
            2 => Ok(CachedBlock::ParquetBlock(payload)),
            kind => Err(FoyerError::io_error(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unknown cached block tag: {}", kind),
            ))),
        }
    }

    fn estimated_size(&self) -> usize {
        1 + 4 + self.size_in_bytes()
    }
}

pub fn new_block_cache(capacity: usize) -> BlockCache {
    Arc::new(FoyerCache::new(capacity, |_, v: &CachedBlock| {
        v.size_in_bytes()
    }))
}

pub(crate) fn new_block_cache_with_config(
    config: &Config,
    db_id: &str,
    memory_capacity: usize,
    selected_plan: Option<&HybridCacheVolumePlan>,
) -> Result<BlockCache> {
    if memory_capacity == 0 {
        return Err(Error::ConfigError(
            "block cache size must be greater than 0".to_string(),
        ));
    }
    let plan = if let Some(plan) = selected_plan {
        Some(plan.clone())
    } else {
        config.resolve_hybrid_cache_volume_plan(memory_capacity)?
    };
    let Some(plan) = plan else {
        return Ok(new_block_cache(memory_capacity));
    };
    let cache_dir = build_hybrid_cache_dir(&plan.base_dir, db_id)?;
    let hybrid = FoyerCache::new_hybrid(
        memory_capacity,
        plan.disk_capacity_bytes,
        cache_dir,
        |_, value: &CachedBlock| value.size_in_bytes(),
    )?;
    Ok(Arc::new(hybrid))
}

fn build_hybrid_cache_dir(base_dir: &str, db_id: &str) -> Result<PathBuf> {
    let url = Url::parse(base_dir).map_err(|err| {
        Error::ConfigError(format!("Invalid cache volume URL {}: {}", base_dir, err))
    })?;
    if !url.scheme().eq_ignore_ascii_case("file") {
        return Err(Error::ConfigError(format!(
            "Hybrid cache requires a local file:// volume, got {}",
            base_dir
        )));
    }
    let mut path = url.to_file_path().map_err(|_| {
        Error::ConfigError(format!("Invalid file URL for cache volume: {}", base_dir))
    })?;
    path.push(db_id);
    path.push("cache");
    Ok(path)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::cache::MockCache;
    use crate::cache::{
        BlockCache, BlockCacheKey, BlockCacheKind, CachedBlock, ScanHotBlockRegistry,
    };
    use crate::file::FileSystemRegistry;
    use crate::sst::iterator::{SSTIterator, SSTIteratorOptions};
    use crate::sst::writer::{SSTWriter, SSTWriterOptions};
    use bytes::Bytes;
    use foyer::Code;

    #[test]
    #[serial_test::serial(file)]
    fn test_block_cache_used_on_seek() {
        let _ = std::fs::remove_dir_all("/tmp/cache_it_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/cache_it_test")
            .unwrap();

        {
            let writer_file = fs.open_write("cached.sst").unwrap();
            let mut writer = SSTWriter::new(
                writer_file,
                SSTWriterOptions {
                    metrics: None,
                    block_size: 64,
                    buffer_size: 8192,
                    num_columns: 1,
                    bloom_filter_enabled: true,
                    bloom_bits_per_key: 10,
                    partitioned_index: false,
                    data_block_restart_interval: 16,
                    compression: crate::SstCompressionAlgorithm::None,
                    value_has_ttl: true,
                },
            );

            for i in 0..10 {
                let key = format!("key{:03}", i);
                let value = format!("value{:03}_with_padding", i);
                writer.add(key.as_bytes(), value.as_bytes()).unwrap();
            }
            writer.finish().unwrap();
        }

        let reader_file = fs.open_read("cached.sst").unwrap();
        let mock_cache = Arc::new(MockCache::<BlockCacheKey, CachedBlock>::default());
        let cache: BlockCache = mock_cache.clone();
        let hot_blocks = Arc::new(ScanHotBlockRegistry::new());
        let mut iter = SSTIterator::with_cache_test(
            reader_file,
            7,
            SSTIteratorOptions {
                metrics: None,
                block_cache_size: 0,
                num_columns: 1,
                bloom_filter_enabled: true,
                cache_namespace: 0,
                preload_next_data_block: true,
                hot_block_registry: Some(hot_blocks),
                ..SSTIteratorOptions::default()
            },
            cache,
        )
        .unwrap();

        iter.seek(b"key005").unwrap();
        assert!(mock_cache.get_count() > 0);
        assert!(mock_cache.insert_count() >= 3);
        assert!(iter.valid());

        iter.seek(b"key006").unwrap();
        assert!(mock_cache.get_count() > 0);

        let _ = std::fs::remove_dir_all("/tmp/cache_it_test");
    }

    #[test]
    fn test_parquet_cache_key_and_value_codec() {
        let key = BlockCacheKey {
            namespace: 0,
            file_id: 42,
            block_id: 1024,
            kind: BlockCacheKind::ParquetData(4096),
        };
        let block = CachedBlock::ParquetBlock(Bytes::from_static(b"parquet-page"));

        let mut key_buf = Vec::new();
        key.encode(&mut key_buf).unwrap();
        let mut key_read = key_buf.as_slice();
        let decoded_key = BlockCacheKey::decode(&mut key_read).unwrap();
        assert_eq!(decoded_key, key);

        let mut block_buf = Vec::new();
        block.encode(&mut block_buf).unwrap();
        let mut block_read = block_buf.as_slice();
        let decoded_block = CachedBlock::decode(&mut block_read).unwrap();
        match decoded_block {
            CachedBlock::ParquetBlock(bytes) => assert_eq!(bytes.as_ref(), b"parquet-page"),
            _ => panic!("expected parquet block"),
        }
    }
}
