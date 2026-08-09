use super::*;
use crate::data_file::{DataFile, DataFileType};
use crate::file::{FileManager, FileSystemRegistry, TrackedFileId};
use crate::format::FileBuildResult;
use crate::metrics_manager::MetricsManager;
use crate::parquet::ParquetWriter;
use crate::sst::row_codec::{encode_key, encode_value};
use crate::r#type::{Column, Key, Value, ValueType};

fn cleanup_test_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

#[test]
#[serial_test::serial(file)]
fn test_create_iterator_parquet_with_bucket_filter() {
    cleanup_test_root("/tmp/iterator_factory_parquet_filter_test");
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/iterator_factory_parquet_filter_test")
        .unwrap();
    let metrics = Arc::new(MetricsManager::new("iterator-factory-parquet-test"));
    let file_manager = Arc::new(FileManager::with_defaults(fs, metrics).unwrap());

    let (file_id, writer_file) = file_manager.create_data_file().unwrap();
    let mut writer = ParquetWriter::with_options(
        writer_file,
        crate::parquet::ParquetWriterOptions {
            num_columns: 1,
            ..crate::parquet::ParquetWriterOptions::default()
        },
    )
    .unwrap();
    let encoded_v1 = encode_value(
        &Value::new(vec![Some(Column::new(ValueType::Put, b"v1".to_vec()))]),
        1,
    );
    let encoded_v2 = encode_value(
        &Value::new(vec![Some(Column::new(ValueType::Put, b"v2".to_vec()))]),
        1,
    );
    let key_a = encode_key(&Key::new(1, b"a".to_vec()));
    let key_b = encode_key(&Key::new(2, b"b".to_vec()));
    writer.add(&key_a, &encoded_v1).unwrap();
    writer.add(&key_b, &encoded_v2).unwrap();
    let FileBuildResult {
        first_key: start_key,
        last_key: end_key,
        file_size,
        meta_bytes,
        ..
    } = writer.finish().unwrap();

    let data_file = DataFile::new(
        DataFileType::Parquet,
        start_key,
        end_key,
        file_id,
        TrackedFileId::new(&file_manager, file_id),
        0,
        file_size,
        1..=2,
        2..=2,
    );
    data_file.set_meta_bytes(meta_bytes);

    let options = IteratorFactoryOptions::default();
    let mut iter = create_iterator(&data_file, &file_manager, &options).unwrap();
    iter.seek_to_first().unwrap();
    assert!(iter.valid());
    let key = iter.key().unwrap().unwrap();
    assert_eq!(key, key_b);
    let decoded = iter.take_value().unwrap().unwrap().into_decoded(1).unwrap();
    assert_eq!(
        decoded.columns()[0].as_ref().unwrap().data().as_ref(),
        b"v2"
    );
    assert!(!iter.next().unwrap());

    cleanup_test_root("/tmp/iterator_factory_parquet_filter_test");
}
