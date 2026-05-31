pub(crate) mod file_adapter;
mod iterator;
mod meta;
mod writer;

pub(crate) use file_adapter::{
    RandomAccessChunkReader, cache_parquet_data_block, parquet_row_group_cache_keys,
};
#[allow(unused_imports)]
pub(crate) use iterator::{ParquetIterator, ParquetIteratorOptions};
#[allow(unused_imports)]
pub(crate) use meta::{decode_meta_row_count, decode_meta_row_group_ranges};
#[allow(unused_imports)]
pub(crate) use writer::{ParquetWriter, ParquetWriterOptions};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{BlockCache, BlockCacheKey, CachedBlock, MockCache};
    use crate::data_file::{DataFile, DataFileType};
    use crate::file::{FileSystemRegistry, RandomAccessFile, TrackedFileId};
    use crate::iterator::KvIterator;
    use crate::parquet::meta::decode_meta_row_group_ranges;
    use crate::sst::row_codec::{decode_value, encode_value};
    use crate::r#type::{Column, Value, ValueType};
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::sync::Arc;

    fn cleanup_test_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn build_reader(path: &str) -> Box<dyn RandomAccessFile> {
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(path).unwrap();
        fs.open_read("test.parquet").unwrap()
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_parquet_iterator_preloads_next_row_group_into_cache() {
        let root = "file:///tmp/parquet_row_group_preload_test";
        cleanup_test_root("/tmp/parquet_row_group_preload_test");
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(root).unwrap();
        let writer_file = fs.open_write("test.parquet").unwrap();
        let file_id = 91u64;
        let cache_namespace = 77u64;
        let mut writer = ParquetWriter::with_options(
            writer_file,
            ParquetWriterOptions {
                row_group_size_bytes: 6,
                num_columns: 1,
                ..ParquetWriterOptions::default()
            },
        )
        .unwrap();
        for (key, value) in [
            (b"a1".as_slice(), b"v1".as_slice()),
            (b"a2".as_slice(), b"v2".as_slice()),
            (b"b1".as_slice(), b"v3".as_slice()),
            (b"b2".as_slice(), b"v4".as_slice()),
        ] {
            let encoded = encode_value(
                &Value::new(vec![Some(Column::new(ValueType::Put, value.to_vec()))]),
                1,
            );
            writer.add(key, &encoded).unwrap();
        }
        let (start_key, end_key, file_size, meta) = writer.finish().unwrap();
        let data_file = DataFile::new(
            DataFileType::Parquet,
            start_key,
            end_key,
            file_id,
            TrackedFileId::detached(file_id),
            0,
            file_size,
            0..=0,
            0..=0,
        );
        data_file.set_meta_bytes(meta);

        let mock = Arc::new(MockCache::<BlockCacheKey, CachedBlock>::default());
        let cache: BlockCache = mock.clone();
        let reader = fs.open_read("test.parquet").unwrap();
        let mut iter = ParquetIterator::from_data_file_with_options(
            Box::new(reader),
            &data_file,
            Some(cache.clone()),
            None,
            ParquetIteratorOptions {
                cache_namespace,
                preload_next_row_group: true,
                ..Default::default()
            },
        )
        .unwrap();
        iter.seek_to_first().unwrap();
        assert!(iter.valid());

        let meta_reader: Arc<dyn RandomAccessFile> =
            Arc::new(fs.open_read("test.parquet").unwrap());
        let parquet_reader =
            SerializedFileReader::new(RandomAccessChunkReader::from_arc(meta_reader)).unwrap();
        let expected_next_row_group_keys = parquet_row_group_cache_keys(
            parquet_reader.metadata().row_group(1),
            cache_namespace,
            file_id,
            None,
            1,
        )
        .unwrap();
        assert!(!expected_next_row_group_keys.is_empty());
        for key in expected_next_row_group_keys {
            assert!(
                cache.get(&key).is_some(),
                "expected next row-group cache key {:?} to be warmed",
                key
            );
        }

        cleanup_test_root("/tmp/parquet_row_group_preload_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_parquet_writer_and_iterator_basic() {
        let root = "file:///tmp/parquet_writer_iter_test";
        cleanup_test_root("/tmp/parquet_writer_iter_test");
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(root).unwrap();
        let writer_file = fs.open_write("test.parquet").unwrap();
        let mut writer = ParquetWriter::with_options(
            writer_file,
            ParquetWriterOptions {
                num_columns: 1,
                ..ParquetWriterOptions::default()
            },
        )
        .unwrap();
        let encode = |v: &[u8]| {
            encode_value(
                &Value::new(vec![Some(Column::new(ValueType::Put, v.to_vec()))]),
                1,
            )
        };
        writer.add(b"aa", &encode(b"11")).unwrap();
        writer.add(b"bb", &encode(b"22")).unwrap();
        writer.add(b"cc", &encode(b"33")).unwrap();
        let (_, _, _, meta) = writer.finish().unwrap();
        assert_eq!(decode_meta_row_count(Some(meta)).unwrap(), Some(3));

        let mut iter = ParquetIterator::new(build_reader(root)).unwrap();
        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().unwrap().as_ref(), b"aa");
        let mut value = iter.value().unwrap().unwrap();
        let decoded = decode_value(&mut value, 1).unwrap();
        assert_eq!(
            decoded.columns()[0].as_ref().unwrap().data().as_ref(),
            b"11"
        );
        assert!(iter.next().unwrap());
        assert_eq!(iter.key().unwrap().unwrap().as_ref(), b"bb");
        assert!(iter.next().unwrap());
        assert_eq!(iter.key().unwrap().unwrap().as_ref(), b"cc");
        assert!(!iter.next().unwrap());
        assert!(!iter.valid());
        cleanup_test_root("/tmp/parquet_writer_iter_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_parquet_writer_order_check() {
        let root = "file:///tmp/parquet_writer_order_test";
        cleanup_test_root("/tmp/parquet_writer_order_test");
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(root).unwrap();
        let writer_file = fs.open_write("test.parquet").unwrap();
        let mut writer = ParquetWriter::with_options(
            writer_file,
            ParquetWriterOptions {
                num_columns: 1,
                ..ParquetWriterOptions::default()
            },
        )
        .unwrap();
        let encode = |v: &[u8]| {
            encode_value(
                &Value::new(vec![Some(Column::new(ValueType::Put, v.to_vec()))]),
                1,
            )
        };
        writer.add(b"bb", &encode(b"11")).unwrap();
        let err = writer.add(b"aa", &encode(b"22")).unwrap_err();
        assert!(err.to_string().contains("sorted order"));
        cleanup_test_root("/tmp/parquet_writer_order_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_parquet_iterator_seek() {
        let root = "file:///tmp/parquet_iterator_seek_test";
        cleanup_test_root("/tmp/parquet_iterator_seek_test");
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(root).unwrap();
        let writer_file = fs.open_write("test.parquet").unwrap();
        let mut writer = ParquetWriter::with_options(
            writer_file,
            ParquetWriterOptions {
                num_columns: 1,
                ..ParquetWriterOptions::default()
            },
        )
        .unwrap();
        let encode = |v: &[u8]| {
            encode_value(
                &Value::new(vec![Some(Column::new(ValueType::Put, v.to_vec()))]),
                1,
            )
        };
        writer.add(b"aa", &encode(b"11")).unwrap();
        writer.add(b"bb", &encode(b"22")).unwrap();
        writer.add(b"dd", &encode(b"44")).unwrap();
        writer.finish().unwrap();

        let mut iter = ParquetIterator::new(build_reader(root)).unwrap();
        iter.seek(b"bc").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().unwrap().as_ref(), b"dd");
        iter.seek(b"zz").unwrap();
        assert!(!iter.valid());
        cleanup_test_root("/tmp/parquet_iterator_seek_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_parquet_iterator_large_dataset_seek() {
        let root = "file:///tmp/parquet_iterator_large_seek_test";
        cleanup_test_root("/tmp/parquet_iterator_large_seek_test");
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(root).unwrap();
        let writer_file = fs.open_write("test.parquet").unwrap();
        let mut writer = ParquetWriter::with_options(
            writer_file,
            ParquetWriterOptions {
                num_columns: 1,
                ..ParquetWriterOptions::default()
            },
        )
        .unwrap();
        for i in 0..5000u32 {
            let key = format!("k{:05}", i);
            let value = format!("v{:05}", i);
            let encoded = encode_value(
                &Value::new(vec![Some(Column::new(
                    ValueType::Put,
                    value.as_bytes().to_vec(),
                ))]),
                1,
            );
            writer.add(key.as_bytes(), &encoded).unwrap();
        }
        writer.finish().unwrap();

        let mut iter = ParquetIterator::new(build_reader(root)).unwrap();
        iter.seek(b"k04990").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().unwrap().as_ref(), b"k04990");
        let mut value = iter.value().unwrap().unwrap();
        let decoded = decode_value(&mut value, 1).unwrap();
        assert_eq!(
            decoded.columns()[0].as_ref().unwrap().data().as_ref(),
            b"v04990"
        );
        assert!(iter.next().unwrap());
        assert_eq!(iter.key().unwrap().unwrap().as_ref(), b"k04991");
        cleanup_test_root("/tmp/parquet_iterator_large_seek_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_parquet_iterator_column_projection() {
        let root = "file:///tmp/parquet_iterator_column_projection_test";
        cleanup_test_root("/tmp/parquet_iterator_column_projection_test");
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(root).unwrap();
        let writer_file = fs.open_write("test.parquet").unwrap();
        let mut writer = ParquetWriter::with_options(
            writer_file,
            ParquetWriterOptions {
                num_columns: 3,
                ..ParquetWriterOptions::default()
            },
        )
        .unwrap();
        let encoded = encode_value(
            &Value::new(vec![
                Some(Column::new(ValueType::Put, b"c0".to_vec())),
                Some(Column::new(ValueType::Put, b"c1".to_vec())),
                Some(Column::new(ValueType::Put, b"c2".to_vec())),
            ]),
            3,
        );
        writer.add(b"k1", &encoded).unwrap();
        writer.finish().unwrap();

        let mut iter = ParquetIterator::new_with_columns(build_reader(root), Some(&[1])).unwrap();
        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        let mut value = iter.value().unwrap().unwrap();
        let decoded = decode_value(&mut value, 3).unwrap();
        assert!(decoded.columns()[0].is_none());
        assert_eq!(
            decoded.columns()[1].as_ref().unwrap().data().as_ref(),
            b"c1"
        );
        assert!(decoded.columns()[2].is_none());

        cleanup_test_root("/tmp/parquet_iterator_column_projection_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_parquet_meta_row_group_ranges() {
        let root = "file:///tmp/parquet_row_group_meta_test";
        cleanup_test_root("/tmp/parquet_row_group_meta_test");
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(root).unwrap();
        let writer_file = fs.open_write("test.parquet").unwrap();
        let mut writer = ParquetWriter::with_options(
            writer_file,
            ParquetWriterOptions {
                row_group_size_bytes: 6,
                buffer_size: 8192,
                num_columns: 1,
            },
        )
        .unwrap();
        let encoded = encode_value(
            &Value::new(vec![Some(Column::new(ValueType::Put, b"v".to_vec()))]),
            1,
        );
        writer.add(b"a1", &encoded).unwrap();
        writer.add(b"b1", &encoded).unwrap();
        writer.add(b"c1", &encoded).unwrap();
        writer.add(b"d1", &encoded).unwrap();
        let (_, _, _, meta) = writer.finish().unwrap();
        let groups = decode_meta_row_group_ranges(Some(meta)).unwrap().unwrap();
        assert!(groups.len() >= 2);
        assert_eq!(groups.first().unwrap().start_key, b"a1".to_vec());
        assert_eq!(groups.last().unwrap().end_key, b"d1".to_vec());
        cleanup_test_root("/tmp/parquet_row_group_meta_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_parquet_iterator_stops_at_row_group_boundary() {
        let root = "file:///tmp/parquet_row_group_boundary_test";
        cleanup_test_root("/tmp/parquet_row_group_boundary_test");
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(root).unwrap();
        let writer_file = fs.open_write("test.parquet").unwrap();
        let mut writer = ParquetWriter::with_options(
            writer_file,
            ParquetWriterOptions {
                row_group_size_bytes: 6,
                buffer_size: 8192,
                num_columns: 1,
            },
        )
        .unwrap();
        let encoded = encode_value(
            &Value::new(vec![Some(Column::new(ValueType::Put, b"v".to_vec()))]),
            1,
        );
        writer.add(b"a1", &encoded).unwrap();
        writer.add(b"b1", &encoded).unwrap();
        writer.add(b"c1", &encoded).unwrap();
        writer.add(b"d1", &encoded).unwrap();
        let (_, _, _, meta) = writer.finish().unwrap();
        let groups = decode_meta_row_group_ranges(Some(meta.clone()))
            .unwrap()
            .unwrap();
        assert!(groups.len() >= 2);

        let mut iter = ParquetIterator::new(build_reader(root)).expect("parquet iterator");
        iter.seek_to_first().unwrap();
        iter.set_stop_at_block_boundary(true);

        let mut saw = 1;
        while iter.next().unwrap() {
            saw += 1;
        }
        assert!(saw < 4);
        assert!(iter.stopped_at_block_boundary());
        cleanup_test_root("/tmp/parquet_row_group_boundary_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_parquet_iterator_can_resume_after_row_group_boundary_stop() {
        let root = "file:///tmp/parquet_row_group_resume_test";
        cleanup_test_root("/tmp/parquet_row_group_resume_test");
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(root).unwrap();
        let writer_file = fs.open_write("test.parquet").unwrap();
        let mut writer = ParquetWriter::with_options(
            writer_file,
            ParquetWriterOptions {
                row_group_size_bytes: 6,
                buffer_size: 8192,
                num_columns: 1,
            },
        )
        .unwrap();
        let encoded = encode_value(
            &Value::new(vec![Some(Column::new(ValueType::Put, b"v".to_vec()))]),
            1,
        );
        for key in [b"a1", b"b1", b"c1", b"d1"] {
            writer.add(key.as_slice(), &encoded).unwrap();
        }
        writer.finish().unwrap();

        let mut iter = ParquetIterator::new(build_reader(root)).expect("parquet iterator");
        iter.seek_to_first().unwrap();
        iter.set_stop_at_block_boundary(true);

        let mut keys = vec![iter.key().unwrap().unwrap().to_vec()];
        loop {
            if iter.next().unwrap() {
                keys.push(iter.key().unwrap().unwrap().to_vec());
                continue;
            }
            if iter.stopped_at_block_boundary() {
                iter.clear_stop_at_block_boundary();
                continue;
            }
            break;
        }

        assert_eq!(
            keys,
            vec![
                b"a1".to_vec(),
                b"b1".to_vec(),
                b"c1".to_vec(),
                b"d1".to_vec()
            ]
        );
        cleanup_test_root("/tmp/parquet_row_group_resume_test");
    }
}
