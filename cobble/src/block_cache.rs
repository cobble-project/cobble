use crate::Config;
use crate::cache::{CacheHandle, FoyerCache};
use crate::config::HybridCacheVolumePlan;
use crate::error::{Error, Result};
use crate::sst::bloom::BloomFilter;
use crate::sst::format::Block;
use bytes::Bytes;
use foyer::{Code, Error as FoyerError};
use std::sync::Arc;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct BlockCacheKey {
    pub file_id: u64,
    pub block_id: u64,
    pub kind: BlockCacheKind,
}

impl Code for BlockCacheKey {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
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
        let mut file_id = [0u8; 8];
        let mut block_id = [0u8; 8];
        reader
            .read_exact(&mut file_id)
            .map_err(FoyerError::io_error)?;
        reader
            .read_exact(&mut block_id)
            .map_err(FoyerError::io_error)?;
        let kind = BlockCacheKind::decode(reader)?;
        Ok(Self {
            file_id: u64::from_le_bytes(file_id),
            block_id: u64::from_le_bytes(block_id),
            kind,
        })
    }

    fn estimated_size(&self) -> usize {
        8 + 8 + self.kind.estimated_size()
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

    use crate::block_cache::{BlockCache, BlockCacheKey, BlockCacheKind, CachedBlock};
    use crate::cache::MockCache;
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
                    compression: crate::SstCompressionAlgorithm::None,
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
        let mut iter = SSTIterator::with_cache_test(
            reader_file,
            7,
            SSTIteratorOptions {
                metrics: None,
                block_cache_size: 0,
                num_columns: 1,
                bloom_filter_enabled: true,
            },
            cache,
        )
        .unwrap();

        iter.seek(b"key005").unwrap();
        assert!(mock_cache.get_count() > 0);
        assert!(mock_cache.insert_count() > 0);
        assert!(iter.valid());

        iter.seek(b"key006").unwrap();
        assert!(mock_cache.get_count() > 0);

        let _ = std::fs::remove_dir_all("/tmp/cache_it_test");
    }

    #[test]
    fn test_parquet_cache_key_and_value_codec() {
        let key = BlockCacheKey {
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
