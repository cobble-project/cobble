pub(crate) mod file_adapter;
mod iterator;
mod meta;
mod writer;

#[allow(unused_imports)]
pub(crate) use iterator::ParquetIterator;
#[allow(unused_imports)]
pub(crate) use meta::{decode_meta_row_count, decode_meta_row_group_ranges};
#[allow(unused_imports)]
pub(crate) use writer::{ParquetWriter, ParquetWriterOptions};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::{FileSystemRegistry, RandomAccessFile};
    use crate::parquet::meta::decode_meta_row_group_ranges;
    use crate::sst::row_codec::{decode_value, encode_value};
    use crate::r#type::{Column, Value, ValueType};

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
}
