use crate::cache::{CacheHandle, FoyerCache};
use crate::sst::format::Block;
use std::sync::Arc;

pub type BlockCache = Arc<dyn CacheHandle<BlockCacheKey, Block>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct BlockCacheKey {
    pub file_id: u64,
    pub block_id: u32,
}

pub fn new_block_cache(capacity: usize) -> BlockCache {
    Arc::new(FoyerCache::new(capacity, |_, v: &Block| v.size_in_bytes()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::cache::MockCache;
    use crate::file::FileSystemRegistry;
    use crate::sst::block_cache::{BlockCache, BlockCacheKey};
    use crate::sst::format::Block;
    use crate::sst::iterator::{SSTIterator, SSTIteratorOptions};
    use crate::sst::writer::{SSTWriter, SSTWriterOptions};

    #[test]
    #[serial_test::serial(file)]
    fn test_block_cache_used_on_seek() {
        let _ = std::fs::remove_dir_all("/tmp/cache_it_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/cache_it_test".to_string())
            .unwrap();

        {
            let writer_file = fs.open_write("cached.sst").unwrap();
            let mut writer = SSTWriter::new(
                writer_file,
                SSTWriterOptions {
                    block_size: 64,
                    buffer_size: 8192,
                    num_columns: 1,
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
        let mock_cache = Arc::new(MockCache::<BlockCacheKey, Block>::default());
        let cache: BlockCache = mock_cache.clone();
        let mut iter = SSTIterator::with_cache_test(
            reader_file,
            7,
            SSTIteratorOptions {
                block_cache_size: 0,
                num_columns: 1,
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
}
