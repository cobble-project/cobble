use super::{BlockCacheUsage, BlockCacheUsageListener, new_block_cache_with_config};
use crate::{Config, VolumeDescriptor, VolumeUsageKind};
use size::Size;
use std::sync::Arc;

use crate::cache::MockCache;
use crate::cache::{BlockCache, BlockCacheKey, BlockCacheKind, CachedBlock, ScanHotBlockRegistry};
use crate::file::FileSystemRegistry;
use crate::sst::iterator::{SSTIterator, SSTIteratorOptions};
use crate::sst::writer::{SSTWriter, SSTWriterOptions};
use bytes::Bytes;
use foyer::Code;
use foyer::{Event, EventListener};
use std::sync::atomic::Ordering;

impl BlockCacheUsage {
    fn current(&self, kind: &BlockCacheKind) -> u64 {
        self.metric_for_kind(kind).0.load(Ordering::Acquire)
    }
}

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
                read_metadata_cache_mode: crate::SstReadMetadataCacheMode::Eager,
                data_block_restart_interval: 16,
                compression: crate::SstCompressionAlgorithm::None,
                value_has_ttl: true,
                block_checksum_enabled: false,
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
fn block_cache_usage_tracks_insert_replace_leave_and_never_underflows() {
    let usage = Arc::new(BlockCacheUsage::new("block-cache-usage-test"));
    let listener = BlockCacheUsageListener {
        usage: Arc::clone(&usage),
    };
    let data_key = BlockCacheKey {
        namespace: 1,
        file_id: 2,
        block_id: 3,
        kind: BlockCacheKind::Data,
    };
    let old = CachedBlock::ParquetBlock(Bytes::from_static(b"old"));
    let replacement = CachedBlock::ParquetBlock(Bytes::from_static(b"replacement"));

    usage.add(&data_key, old.size_in_bytes() as u64);
    assert_eq!(usage.current(&data_key.kind), 3);
    usage.add(&data_key, replacement.size_in_bytes() as u64);
    listener.on_leave(Event::Replace, &data_key, &old);
    assert_eq!(
        usage.current(&data_key.kind),
        replacement.size_in_bytes() as u64
    );
    listener.on_leave(Event::Evict, &data_key, &replacement);
    assert_eq!(usage.current(&data_key.kind), 0);
    listener.on_leave(Event::Clear, &data_key, &replacement);
    assert_eq!(usage.current(&data_key.kind), 0);

    let index_key = BlockCacheKey {
        kind: BlockCacheKind::IndexTop,
        ..data_key
    };
    let filter_key = BlockCacheKey {
        kind: BlockCacheKind::FilterIndex,
        ..data_key
    };
    let parquet_key = BlockCacheKey {
        kind: BlockCacheKind::ParquetData(16),
        ..data_key
    };
    for key in [&index_key, &filter_key, &parquet_key] {
        usage.add(key, 1);
        assert_eq!(usage.current(&key.kind), 1);
        listener.on_leave(Event::Remove, key, &old);
        assert_eq!(usage.current(&key.kind), 0);
    }
}

#[test]
fn foyer_block_cache_usage_tracks_replacement_remove_and_clear() {
    crate::metrics_registry::init_metrics();
    let db_id = "foyer-block-cache-usage-test";
    let cache = new_block_cache_with_config(&Config::default(), db_id, 8, None).unwrap();
    let key = BlockCacheKey {
        namespace: 7,
        file_id: 8,
        block_id: 9,
        kind: BlockCacheKind::Data,
    };
    cache.insert(key, CachedBlock::ParquetBlock(Bytes::from_static(b"four")));
    assert_eq!(block_cache_usage_sample(db_id, "data"), 4.0);

    cache.insert(
        key,
        CachedBlock::ParquetBlock(Bytes::from_static(b"replace")),
    );
    assert_eq!(block_cache_usage_sample(db_id, "data"), 7.0);

    cache.remove(&key);
    assert_eq!(block_cache_usage_sample(db_id, "data"), 0.0);

    cache.insert(key, CachedBlock::ParquetBlock(Bytes::from_static(b"clear")));
    assert_eq!(block_cache_usage_sample(db_id, "data"), 5.0);
    cache.clear();
    assert_eq!(block_cache_usage_sample(db_id, "data"), 0.0);
}

#[test]
#[serial_test::serial(file)]
fn hybrid_block_cache_usage_tracks_memory_tier_entries() {
    crate::metrics_registry::init_metrics();
    let root = "/tmp/hybrid-block-cache-usage-test";
    let _ = std::fs::remove_dir_all(root);
    let db_id = "hybrid-block-cache-usage-test";
    let config = Config {
        volumes: vec![VolumeDescriptor::new(
            format!("file://{root}"),
            vec![VolumeUsageKind::Cache],
        )],
        block_cache_hybrid_enabled: true,
        block_cache_hybrid_disk_size: Some(Size::from_mib(32)),
        ..Config::default()
    };
    let cache = new_block_cache_with_config(&config, db_id, 4 * 1024 * 1024, None).unwrap();
    let key = BlockCacheKey {
        namespace: 17,
        file_id: 18,
        block_id: 19,
        kind: BlockCacheKind::Data,
    };
    cache.insert(
        key,
        CachedBlock::ParquetBlock(Bytes::from_static(b"hybrid")),
    );
    assert_eq!(block_cache_usage_sample(db_id, "data"), 6.0);
    cache.remove(&key);
    assert_eq!(block_cache_usage_sample(db_id, "data"), 0.0);
    drop(cache);
    let _ = std::fs::remove_dir_all(root);
}

fn block_cache_usage_sample(db_id: &str, kind: &str) -> f64 {
    crate::metrics_registry::snapshot_metrics(Some(db_id))
        .into_iter()
        .find(|sample| {
            sample.name == "block_cache_usage_bytes"
                && sample
                    .labels
                    .iter()
                    .any(|(key, value)| key == "kind" && value == kind)
        })
        .and_then(|sample| match sample.value {
            crate::MetricValue::Gauge(value) => Some(value),
            _ => None,
        })
        .unwrap_or_default()
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
