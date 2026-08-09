use super::*;

#[test]
fn test_encode_decode_key() {
    let key = Key::new(42, b"hello world".to_vec());
    let encoded = encode_key(&key);

    // Verify encoded size: 2 (group) + 1 (cf) + 11 (data) = 14
    assert_eq!(encoded.len(), 14);
    assert_eq!(key_encoded_size(&key), 14);

    let mut encoded_for_decode = encoded.clone();
    let decoded = decode_key(&mut encoded_for_decode).unwrap();
    assert_eq!(decoded.bucket(), 42);
    assert_eq!(decoded.column_family(), 0);
    assert_eq!(decoded.data().as_ref(), b"hello world");
}

#[test]
fn encoded_bucket_prefixes_sort_across_255_256_boundary() {
    let key_255 = encode_key(&Key::new(255, b"key".to_vec()));
    let key_256 = encode_key(&Key::new(256, b"key".to_vec()));

    assert_eq!(&key_255[..ENCODED_KEY_BUCKET_BYTES], &[0, 255]);
    assert_eq!(&key_256[..ENCODED_KEY_BUCKET_BYTES], &[1, 0]);
    assert!(key_255 < key_256);
}

#[test]
fn test_key_empty_data() {
    let key = Key::new(0, Vec::new());
    let encoded = encode_key(&key);

    assert_eq!(encoded.len(), ENCODED_KEY_PREFIX_BYTES);

    let mut encoded_for_decode = encoded.clone();
    let decoded = decode_key(&mut encoded_for_decode).unwrap();
    assert_eq!(decoded.bucket(), 0);
    assert_eq!(decoded.column_family(), 0);
    assert_eq!(decoded.data().as_ref(), b"");
}

#[test]
fn test_key_max_group() {
    let key = Key::new(u16::MAX, b"test".to_vec());
    let encoded = encode_key(&key);

    let mut encoded_for_decode = encoded.clone();
    let decoded = decode_key(&mut encoded_for_decode).unwrap();
    assert_eq!(decoded.bucket(), u16::MAX);
    assert_eq!(decoded.column_family(), 0);
    assert_eq!(decoded.data().as_ref(), b"test");
}

#[test]
fn test_key_decode_too_small() {
    let mut encoded = Bytes::from_static(&[0]);
    let result = decode_key(&mut encoded);
    assert!(result.is_err());
}

#[test]
fn test_value_type_encode_decode() {
    assert_eq!(encode_value_type(&ValueType::Put), 0b0000_0001);
    assert_eq!(encode_value_type(&ValueType::Delete), 0b0001_0001);
    assert_eq!(encode_value_type(&ValueType::Merge), 0b0000_0010);
    assert_eq!(encode_value_type(&ValueType::PutSeparated), 0b0000_0101);
    assert_eq!(encode_value_type(&ValueType::MergeSeparated), 0b0000_0110);
    assert_eq!(
        encode_value_type(&ValueType::MergeSeparatedArray),
        0b0000_1110
    );
    assert_eq!(
        encode_value_type(&ValueType::PutSeparatedArray),
        0b0000_1111
    );

    assert!(matches!(
        decode_value_type(0b0000_0001).unwrap(),
        ValueType::Put
    ));
    assert!(matches!(
        decode_value_type(0b0001_0001).unwrap(),
        ValueType::Delete
    ));
    assert!(matches!(
        decode_value_type(0b0000_0010).unwrap(),
        ValueType::Merge
    ));
    assert!(matches!(
        decode_value_type(0b0000_0101).unwrap(),
        ValueType::PutSeparated
    ));
    assert!(matches!(
        decode_value_type(0b0000_0110).unwrap(),
        ValueType::MergeSeparated
    ));
    assert!(matches!(
        decode_value_type(0b0000_1110).unwrap(),
        ValueType::MergeSeparatedArray
    ));
    assert!(matches!(
        decode_value_type(0b0000_1111).unwrap(),
        ValueType::PutSeparatedArray
    ));
}

#[test]
fn test_value_type_decode_invalid() {
    assert!(decode_value_type(0).is_err());
    assert!(decode_value_type(0b0000_0011).is_err());
    assert!(decode_value_type(255).is_err());
}

#[test]
fn test_encode_decode_value_all_present() {
    let col1 = Column::new(ValueType::Put, b"data1".to_vec());
    let col2 = Column::new(ValueType::Delete, b"data2".to_vec());
    let value = Value::new(vec![Some(col1), Some(col2)]);

    let encoded = encode_value(&value, 2);

    // Expired_at: 4 bytes
    // Bitmap size: 1 byte for 2 columns
    // Column 1: 1 + 4 + 5 = 10
    // Column 2 (last): 1 + 5 = 6 (data_len omitted)
    // Total: 4 + 1 + 10 + 6 = 21
    assert_eq!(encoded.len(), 21);
    assert_eq!(value_encoded_size(&value, 2), 21);

    let mut encoded_for_decode = encoded.clone();
    let decoded = decode_value(&mut encoded_for_decode, 2).unwrap();
    let cols = decoded.columns();
    assert_eq!(cols.len(), 2);

    assert!(cols[0].is_some());
    let c0 = cols[0].as_ref().unwrap();
    assert!(matches!(c0.value_type(), ValueType::Put));
    assert_eq!(c0.data().as_ref(), b"data1");

    assert!(cols[1].is_some());
    let c1 = cols[1].as_ref().unwrap();
    assert!(matches!(c1.value_type(), ValueType::Delete));
    assert_eq!(c1.data().as_ref(), b"data2");
}

#[test]
fn test_encode_decode_value_with_optional() {
    let col1 = Column::new(ValueType::Put, b"present".to_vec());
    let value = Value::new(vec![Some(col1), None, None]);

    let encoded = encode_value(&value, 3);

    // Expired_at: 4 bytes
    // Bitmap size: 1 byte for 3 columns
    // Only column 0 is present (and is last): 1 + 7 = 8 (data_len omitted)
    // Total: 4 + 1 + 8 = 13
    assert_eq!(encoded.len(), 13);

    let mut encoded_for_decode = encoded.clone();
    let decoded = decode_value(&mut encoded_for_decode, 3).unwrap();
    let cols = decoded.columns();
    assert_eq!(cols.len(), 3);

    assert!(cols[0].is_some());
    assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"present");

    assert!(cols[1].is_none());
    assert!(cols[2].is_none());
}

#[test]
fn test_encode_decode_value_all_absent() {
    let value = Value::new(vec![None, None, None, None]);

    let encoded = encode_value(&value, 4);

    // Expired_at: 4 bytes
    // Bitmap size: 1 byte for 4 columns, no column data
    assert_eq!(encoded.len(), 5);

    let mut encoded_for_decode = encoded.clone();
    let decoded = decode_value(&mut encoded_for_decode, 4).unwrap();
    let cols = decoded.columns();
    assert_eq!(cols.len(), 4);
    assert!(cols.iter().all(|c| c.is_none()));
}

#[test]
fn test_encode_decode_value_many_columns() {
    // Test with 16 columns (2 bytes bitmap)
    let col = Column::new(ValueType::Merge, b"x".to_vec());
    let mut columns: Vec<Option<Column>> = vec![None; 16];
    columns[0] = Some(col.clone());
    columns[8] = Some(col.clone());
    columns[15] = Some(col);
    let value = Value::new(columns);

    let encoded = encode_value(&value, 16);

    // Expired_at: 4 bytes
    // Bitmap size: 2 bytes for 16 columns
    // 2 non-last columns: 2 * (1 + 4 + 1) = 12
    // Last column (idx 15): 1 + 1 = 2 (data_len omitted)
    // Total: 4 + 2 + 12 + 2 = 20
    assert_eq!(encoded.len(), 20);

    let mut encoded_for_decode = encoded.clone();
    let decoded = decode_value(&mut encoded_for_decode, 16).unwrap();
    let cols = decoded.columns();
    assert_eq!(cols.len(), 16);

    assert!(cols[0].is_some());
    assert!(cols[1].is_none());
    assert!(cols[8].is_some());
    assert!(cols[15].is_some());
}

#[test]
fn test_bitmap_size() {
    assert_eq!(bitmap_size(0), 0);
    assert_eq!(bitmap_size(1), 0); // Optimized: no bitmap for single column
    assert_eq!(bitmap_size(2), 1);
    assert_eq!(bitmap_size(8), 1);
    assert_eq!(bitmap_size(9), 2);
    assert_eq!(bitmap_size(16), 2);
    assert_eq!(bitmap_size(17), 3);
}

#[test]
fn test_value_decode_too_small() {
    // For 2 columns, need at least 1 byte bitmap
    let mut encoded = Bytes::new();
    let result = decode_value(&mut encoded, 2);
    assert!(result.is_err());
}

#[test]
fn test_single_column_no_bitmap() {
    // Single column optimization: no bitmap, column must be present
    // Also the last (and only) column, so data_len is omitted
    let col = Column::new(ValueType::Put, b"single".to_vec());
    let value = Value::new(vec![Some(col)]);

    let encoded = encode_value(&value, 1);

    // Expired_at: 4 bytes
    // No bitmap for single column, and data_len omitted (last column)
    // 1 (value_type) + 6 (data) = 7
    // Total: 4 + 7 = 11
    assert_eq!(encoded.len(), 11);
    assert_eq!(value_encoded_size(&value, 1), 11);

    let mut encoded_for_decode = encoded.clone();
    let decoded = decode_value(&mut encoded_for_decode, 1).unwrap();
    let cols = decoded.columns();
    assert_eq!(cols.len(), 1);
    assert!(cols[0].is_some());
    assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"single");
}

#[test]
fn test_large_data() {
    let large_data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

    let key = Key::new(1234, large_data.clone());
    let encoded_key = encode_key(&key);
    let mut encoded_key_for_decode = encoded_key.clone();
    let decoded_key = decode_key(&mut encoded_key_for_decode).unwrap();
    assert_eq!(decoded_key.bucket(), 1234);
    assert_eq!(decoded_key.data().as_ref(), large_data.as_slice());

    let col = Column::new(ValueType::Put, large_data.clone());
    let value = Value::new(vec![Some(col)]);
    let encoded = encode_value(&value, 1);
    let mut encoded_for_decode = encoded.clone();
    let decoded = decode_value(&mut encoded_for_decode, 1).unwrap();
    let cols = decoded.columns();
    assert!(cols[0].is_some());
    assert_eq!(
        cols[0].as_ref().unwrap().data().as_ref(),
        large_data.as_slice()
    );
}

#[test]
fn test_binary_data_with_nulls() {
    let binary_data = vec![0u8, 1, 0, 255, 0, 128, 0];

    let key = Key::new(100, binary_data.clone());
    let encoded = encode_key(&key);
    let mut encoded_for_decode = encoded.clone();
    let decoded = decode_key(&mut encoded_for_decode).unwrap();
    assert_eq!(decoded.data().as_ref(), binary_data.as_slice());
}

#[test]
#[serial_test::serial(file)]
fn test_sst_key_value_codec() {
    use crate::file::FileSystemRegistry;
    use crate::sst::iterator::{SSTIterator, SSTIteratorOptions};
    use crate::sst::writer::{SSTWriter, SSTWriterOptions};

    let _ = std::fs::remove_dir_all("/tmp/sst_row_codec_test");
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/sst_row_codec_test")
        .unwrap();

    // Define schema: 2 columns (name, email)
    let num_columns = 2;

    // Create test Key and Value using the codec
    let key1 = Key::new(1, b"user:1".to_vec());
    let value1 = Value::new(vec![
        Some(Column::new(ValueType::Put, b"Alice".to_vec())),
        Some(Column::new(ValueType::Put, b"alice@example.com".to_vec())),
    ]);

    let key2 = Key::new(1, b"user:2".to_vec());
    // user:2 has no email (optional column)
    let value2 = Value::new(vec![
        Some(Column::new(ValueType::Put, b"Bob".to_vec())),
        None,
    ]);

    let key3 = Key::new(2, b"order:100".to_vec());
    // order:100 is deleted (all columns absent)
    let value3 = Value::new(vec![None, None]);

    // Write SST file with encoded Key/Value
    {
        let writer_file = fs.open_write("codec_test.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            },
        );

        writer
            .add(&encode_key(&key1), &encode_value(&value1, num_columns))
            .unwrap();
        writer
            .add(&encode_key(&key2), &encode_value(&value2, num_columns))
            .unwrap();
        writer
            .add(&encode_key(&key3), &encode_value(&value3, num_columns))
            .unwrap();

        writer.finish().unwrap();
    }

    // Read SST file and decode Key/Value
    {
        let reader_file = fs.open_read("codec_test.sst").unwrap();
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

        // First entry: user:1 with name="Alice", email="alice@example.com"
        assert!(iter.valid());
        let (mut key_bytes, mut value_bytes) = iter.current().unwrap().unwrap();
        let decoded_key = decode_key(&mut key_bytes).unwrap();
        let decoded_value = decode_value(&mut value_bytes, num_columns).unwrap();
        let decoded_cols = decoded_value.columns();

        assert_eq!(decoded_key.bucket(), 1);
        assert_eq!(decoded_key.data().as_ref(), b"user:1");
        assert_eq!(decoded_cols.len(), 2);
        assert!(decoded_cols[0].is_some());
        assert_eq!(decoded_cols[0].as_ref().unwrap().data().as_ref(), b"Alice");
        assert!(decoded_cols[1].is_some());
        assert_eq!(
            decoded_cols[1].as_ref().unwrap().data().as_ref(),
            b"alice@example.com"
        );

        // Second entry: user:2 with name="Bob", email=None
        iter.next().unwrap();
        assert!(iter.valid());
        let (mut key_bytes, mut value_bytes) = iter.current().unwrap().unwrap();
        let decoded_key = decode_key(&mut key_bytes).unwrap();
        let decoded_value = decode_value(&mut value_bytes, num_columns).unwrap();
        let decoded_cols = decoded_value.columns();

        assert_eq!(decoded_key.bucket(), 1);
        assert_eq!(decoded_key.data().as_ref(), b"user:2");
        assert!(decoded_cols[0].is_some());
        assert_eq!(decoded_cols[0].as_ref().unwrap().data().as_ref(), b"Bob");
        assert!(decoded_cols[1].is_none());

        // Third entry: order:100 with all columns absent
        iter.next().unwrap();
        assert!(iter.valid());
        let (mut key_bytes, mut value_bytes) = iter.current().unwrap().unwrap();
        let decoded_key = decode_key(&mut key_bytes).unwrap();
        let decoded_value = decode_value(&mut value_bytes, num_columns).unwrap();
        let decoded_cols = decoded_value.columns();

        assert_eq!(decoded_key.bucket(), 2);
        assert_eq!(decoded_key.data().as_ref(), b"order:100");
        assert!(decoded_cols[0].is_none());
        assert!(decoded_cols[1].is_none());

        // No more entries
        iter.next().unwrap();
        assert!(!iter.valid());
    }

    let _ = std::fs::remove_dir_all("/tmp/sst_row_codec_test");
}

#[test]
#[serial_test::serial(file)]
fn test_sst_key_value_codec_seek() {
    use crate::file::FileSystemRegistry;
    use crate::sst::iterator::{SSTIterator, SSTIteratorOptions};
    use crate::sst::writer::{SSTWriter, SSTWriterOptions};

    let _ = std::fs::remove_dir_all("/tmp/sst_row_codec_seek_test");
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/sst_row_codec_seek_test")
        .unwrap();

    let num_columns = 1;

    let key1 = Key::new(1, b"aaa".to_vec());
    let key2 = Key::new(1, b"bbb".to_vec());
    let key3 = Key::new(2, b"aaa".to_vec());

    let value = Value::new(vec![Some(Column::new(ValueType::Put, b"test".to_vec()))]);

    // Write SST file
    {
        let writer_file = fs.open_write("codec_seek_test.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            },
        );

        writer
            .add(&encode_key(&key1), &encode_value(&value, num_columns))
            .unwrap();
        writer
            .add(&encode_key(&key2), &encode_value(&value, num_columns))
            .unwrap();
        writer
            .add(&encode_key(&key3), &encode_value(&value, num_columns))
            .unwrap();

        writer.finish().unwrap();
    }

    // Read and seek using encoded key
    {
        let reader_file = fs.open_read("codec_seek_test.sst").unwrap();
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

        // Seek to second key
        let seek_key = Key::new(1, b"bbb".to_vec());
        iter.seek(&encode_key(&seek_key)).unwrap();
        assert!(iter.valid());

        let (mut key_bytes, _) = iter.current().unwrap().unwrap();
        let decoded_key = decode_key(&mut key_bytes).unwrap();
        assert_eq!(decoded_key.bucket(), 1);
        assert_eq!(decoded_key.data().as_ref(), b"bbb");
    }

    let _ = std::fs::remove_dir_all("/tmp/sst_row_codec_seek_test");
}

#[test]
#[serial_test::serial(file)]
fn test_sst_key_value_codec_multiple_blocks() {
    use crate::file::FileSystemRegistry;
    use crate::sst::iterator::{SSTIterator, SSTIteratorOptions};
    use crate::sst::writer::{SSTWriter, SSTWriterOptions};

    let _ = std::fs::remove_dir_all("/tmp/sst_row_codec_blocks_test");
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/sst_row_codec_blocks_test")
        .unwrap();

    let num_columns = 2;
    let num_entries = 50;

    // Write SST file with many entries across multiple blocks
    {
        let writer_file = fs.open_write("codec_blocks_test.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                metrics: None,
                block_size: 200, // Small block size to force multiple blocks
                buffer_size: 8192,
                num_columns,
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

        for i in 0..num_entries {
            let key = Key::new(i as u16, format!("key{:04}", i).into_bytes());
            let col1 = Column::new(ValueType::Put, format!("val{:04}", i).into_bytes());
            let col2 = Column::new(ValueType::Merge, b"extra".to_vec());
            // Alternate: some entries have second column, some don't
            let value = if i % 2 == 0 {
                Value::new(vec![Some(col1), Some(col2)])
            } else {
                Value::new(vec![Some(col1), None])
            };
            writer
                .add(&encode_key(&key), &encode_value(&value, num_columns))
                .unwrap();
        }

        writer.finish().unwrap();
    }

    // Read and verify all entries
    {
        let reader_file = fs.open_read("codec_blocks_test.sst").unwrap();
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
            let (mut key_bytes, mut value_bytes) = iter.current().unwrap().unwrap();
            let decoded_key = decode_key(&mut key_bytes).unwrap();
            let decoded_value = decode_value(&mut value_bytes, num_columns).unwrap();
            let decoded_cols = decoded_value.columns();

            assert_eq!(decoded_key.bucket(), count as u16);
            assert_eq!(
                decoded_key.data().as_ref(),
                format!("key{:04}", count).as_bytes()
            );

            assert!(decoded_cols[0].is_some());
            assert_eq!(
                decoded_cols[0].as_ref().unwrap().data().as_ref(),
                format!("val{:04}", count).as_bytes()
            );

            // Even entries have second column
            if count % 2 == 0 {
                assert!(decoded_cols[1].is_some());
            } else {
                assert!(decoded_cols[1].is_none());
            }

            count += 1;
            iter.next().unwrap();
        }

        assert_eq!(count, num_entries);
    }

    let _ = std::fs::remove_dir_all("/tmp/sst_row_codec_blocks_test");
}
