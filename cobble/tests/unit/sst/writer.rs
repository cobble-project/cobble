use super::*;
use crate::data_file::DataFile;
use crate::file::FileSystemRegistry;
use crate::format::FileBuilder;
use crate::sst::{SSTIterator, SSTIteratorOptions};
use crate::r#type::Column;

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
fn test_sst_writer_orders_bucket_255_before_256_and_preserves_range_metadata() {
    let root = tempfile::tempdir().unwrap();
    let root_url = url::Url::from_directory_path(root.path())
        .unwrap()
        .to_string();
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register(&root_url).unwrap();
    let key_255 = encode_key(&Key::new(255, b"key".to_vec()));
    let key_256 = encode_key(&Key::new(256, b"key".to_vec()));

    let writer_file = fs.open_write("bucket-boundary.sst").unwrap();
    let mut writer = SSTWriter::new(writer_file, SSTWriterOptions::default());
    writer.add(&key_255, b"value-255").unwrap();
    writer.add(&key_256, b"value-256").unwrap();
    let result = writer.finish_with_range().unwrap();

    assert_eq!(result.first_key, key_255);
    assert_eq!(result.last_key, key_256);
    assert_eq!(
        DataFile::bucket_range_from_keys(&result.first_key, &result.last_key),
        255..=256
    );

    let reader_file = fs.open_read("bucket-boundary.sst").unwrap();
    let mut iter = SSTIterator::new(reader_file, SSTIteratorOptions::default()).unwrap();
    iter.seek_to_first().unwrap();
    assert_eq!(iter.current().unwrap().unwrap().0, key_255);
    iter.next().unwrap();
    assert_eq!(iter.current().unwrap().unwrap().0, key_256);
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

fn make_kv_value(expired_at: Option<u32>) -> KvValue {
    let column = Column::new(crate::r#type::ValueType::Put, b"v".to_vec());
    KvValue::Decoded(Value::new_with_expired_at(vec![Some(column)], expired_at))
}

#[test]
#[serial_test::serial(file)]
fn test_sst_writer_tracks_max_expired_at() {
    let root = "/tmp/sst_writer_ttl_test";
    let _ = std::fs::remove_dir_all(root);
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register(format!("file://{root}")).unwrap();

    let writer_file = fs.open_write("test.sst").unwrap();
    let mut writer =
        Box::new(SSTWriter::new(writer_file, SSTWriterOptions::default())) as Box<dyn FileBuilder>;
    writer.add(b"key1", &make_kv_value(Some(100))).unwrap();
    writer.add(b"key2", &make_kv_value(Some(500))).unwrap();
    writer.add(b"key3", &make_kv_value(Some(300))).unwrap();
    let result = writer.finish().unwrap();
    assert_eq!(result.max_expired_at, 500);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_sst_writer_no_ttl_max_expired_at_zero() {
    let root = "/tmp/sst_writer_no_ttl_test";
    let _ = std::fs::remove_dir_all(root);
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register(format!("file://{root}")).unwrap();

    let writer_file = fs.open_write("test.sst").unwrap();
    let mut writer =
        Box::new(SSTWriter::new(writer_file, SSTWriterOptions::default())) as Box<dyn FileBuilder>;
    writer.add(b"key1", &make_kv_value(None)).unwrap();
    writer.add(b"key2", &make_kv_value(None)).unwrap();
    let result = writer.finish().unwrap();
    assert_eq!(result.max_expired_at, 0);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_sst_writer_mixed_ttl_and_no_ttl_yields_zero() {
    // A file with both a TTL entry and a no-TTL entry must report max_expired_at=0,
    // because the no-TTL entry should never be dropped at the file level.
    let root = "/tmp/sst_writer_mixed_ttl_test";
    let _ = std::fs::remove_dir_all(root);
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register(format!("file://{root}")).unwrap();

    let writer_file = fs.open_write("test.sst").unwrap();
    let mut writer =
        Box::new(SSTWriter::new(writer_file, SSTWriterOptions::default())) as Box<dyn FileBuilder>;
    writer.add(b"key1", &make_kv_value(Some(100))).unwrap();
    writer.add(b"key2", &make_kv_value(None)).unwrap();
    let result = writer.finish().unwrap();
    assert_eq!(result.max_expired_at, 0);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_sst_writer_mixed_ttl_different_order_yields_zero() {
    // Same as above but with no-TTL entry first, then TTL, then no-TTL again.
    let root = "/tmp/sst_writer_mixed_ttl_order_test";
    let _ = std::fs::remove_dir_all(root);
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register(format!("file://{root}")).unwrap();

    let writer_file = fs.open_write("test.sst").unwrap();
    let mut writer =
        Box::new(SSTWriter::new(writer_file, SSTWriterOptions::default())) as Box<dyn FileBuilder>;
    writer.add(b"key1", &make_kv_value(None)).unwrap();
    writer.add(b"key2", &make_kv_value(Some(500))).unwrap();
    writer.add(b"key3", &make_kv_value(None)).unwrap();
    let result = writer.finish().unwrap();
    assert_eq!(result.max_expired_at, 0);

    let _ = std::fs::remove_dir_all(root);
}
