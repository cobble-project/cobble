use super::*;
use crate::cache::ScanHotBlockRegistry;
use crate::file::{FileSystemRegistry, TrackedFileId};
use crate::metrics_manager::MetricsManager;
use crate::parquet::ParquetWriterOptions;
use crate::parquet::{
    ParquetIterator, ParquetWriter, RandomAccessChunkReader, parquet_row_group_cache_keys,
};
use crate::schema::Schema;
use crate::sst::row_codec::{encode_key, encode_value};
use crate::sst::{SSTWriter, SSTWriterOptions};
use crate::r#type::{Column, ValueType, decode_merge_separated_array};
use crate::r#type::{Key, Value};
use crate::writer_options::{WriterOptions, WriterOptionsFactory};
use bytes::Bytes;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex as StdMutex};
use std::time::Duration;

#[test]
fn compaction_runtime_caps_blocking_tasks() {
    const MAX_THREADS: usize = 2;
    const TASKS: usize = 8;

    let runtime = build_compaction_runtime("cobble-compaction-cap-test", MAX_THREADS).unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new((StdMutex::new(false), Condvar::new()));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let mut handles = Vec::with_capacity(TASKS);

    for _ in 0..TASKS {
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        let gate = Arc::clone(&gate);
        let started_tx = started_tx.clone();
        handles.push(runtime.spawn_blocking(move || {
            let current = active.fetch_add(1, Ordering::AcqRel) + 1;
            peak.fetch_max(current, Ordering::AcqRel);
            started_tx.send(()).unwrap();

            let (lock, condition) = &*gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
            active.fetch_sub(1, Ordering::AcqRel);
        }));
    }
    drop(started_tx);

    for _ in 0..MAX_THREADS {
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    assert!(
        started_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "more than {MAX_THREADS} blocking tasks started before the gate was released"
    );

    {
        let (lock, condition) = &*gate;
        *lock.lock().unwrap() = true;
        condition.notify_all();
    }
    runtime.block_on(async {
        for handle in handles {
            handle.await.unwrap();
        }
    });

    assert_eq!(peak.load(Ordering::Acquire), MAX_THREADS);
}

#[test]
fn compaction_runtime_shutdown_drains_queued_blocking_tasks() {
    const TASKS: usize = 4;

    let mut executor = CompactionExecutor::new(
        CompactionConfig {
            max_threads: 1,
            ..CompactionConfig::default()
        },
        Arc::new(DbLifecycle::new_open()),
    )
    .unwrap();
    let completed = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new((StdMutex::new(false), Condvar::new()));
    let (started_tx, started_rx) = std::sync::mpsc::channel();

    for _ in 0..TASKS {
        let completed = Arc::clone(&completed);
        let gate = Arc::clone(&gate);
        let started_tx = started_tx.clone();
        executor
            .tasks
            .spawn(executor.runtime.as_ref().unwrap(), move || {
                started_tx.send(()).unwrap();
                let (lock, condition) = &*gate;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
                completed.fetch_add(1, Ordering::AcqRel);
            })
            .unwrap();
    }
    drop(started_tx);
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let (drained_tx, drained_rx) = std::sync::mpsc::channel();
    let shutdown = std::thread::spawn(move || {
        executor.shutdown();
        drained_tx.send(()).unwrap();
    });
    assert!(
        drained_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "shutdown returned while a blocking task was still queued"
    );

    {
        let (lock, condition) = &*gate;
        *lock.lock().unwrap() = true;
        condition.notify_all();
    }
    drained_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    shutdown.join().unwrap();
    assert_eq!(completed.load(Ordering::Acquire), TASKS);
}

fn make_value_bytes(data: &[u8], num_columns: usize) -> Vec<u8> {
    let value = Value::new(vec![Some(Column::new(ValueType::Put, data.to_vec()))]);
    encode_value(&value, num_columns).to_vec()
}

fn make_typed_value_bytes(value_type: ValueType, data: &[u8], num_columns: usize) -> Vec<u8> {
    let value = Value::new(vec![Some(Column::new(value_type, data.to_vec()))]);
    encode_value(&value, num_columns).to_vec()
}

fn make_typed_value_bytes_with_expired(
    value_type: ValueType,
    data: &[u8],
    num_columns: usize,
    expired_at: Option<u32>,
) -> Vec<u8> {
    let value = Value::new_with_expired_at(
        vec![Some(Column::new(value_type, data.to_vec()))],
        expired_at,
    );
    encode_value(&value, num_columns).to_vec()
}

fn schema_manager_for(num_columns: usize) -> Arc<SchemaManager> {
    Arc::new(SchemaManager::from_schemas(
        vec![Schema::new(0, num_columns, Vec::new())],
        num_columns,
    ))
}

fn cleanup_test_dir(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn create_test_sst(
    file_manager: &Arc<FileManager>,
    entries: Vec<(&[u8], &[u8])>,
) -> Result<Arc<DataFile>> {
    let (file_id, writer_file) = file_manager.create_data_file_with_offload()?;
    let mut writer = SSTWriter::new(
        writer_file,
        SSTWriterOptions {
            bloom_filter_enabled: true,
            ..SSTWriterOptions::default()
        },
    );

    for (key, value) in entries {
        writer.add(key, value)?;
    }

    let FileBuildResult {
        first_key,
        last_key,
        file_size,
        meta_bytes,
        sst_read_metadata,
        max_expired_at,
    } = writer.finish_with_range()?;
    let bucket_range = DataFile::bucket_range_from_keys(&first_key, &last_key);

    let data_file = DataFile::new(
        DataFileType::SSTable,
        first_key,
        last_key,
        file_id,
        TrackedFileId::new(file_manager, file_id),
        0,
        file_size,
        bucket_range.clone(),
        bucket_range,
    )
    .with_separated_values(true);
    data_file.set_meta_bytes(meta_bytes);
    data_file.set_max_expired_at(max_expired_at);
    file_manager.finalize_data_file(&data_file)?;
    if let Some(metadata) = sst_read_metadata {
        data_file.set_sst_read_metadata(metadata);
    }
    Ok(Arc::new(data_file))
}

fn create_test_parquet(
    file_manager: &Arc<FileManager>,
    entries: Vec<(&[u8], &[u8])>,
) -> Result<Arc<DataFile>> {
    let (file_id, writer_file) = file_manager.create_data_file_with_offload()?;
    let mut writer = ParquetWriter::with_options(
        writer_file,
        ParquetWriterOptions {
            num_columns: 1,
            ..ParquetWriterOptions::default()
        },
    )?;
    for (key, value) in entries {
        writer.add(key, value)?;
    }
    let FileBuildResult {
        first_key,
        last_key,
        file_size,
        meta_bytes,
        ..
    } = writer.finish()?;
    let bucket_range = DataFile::bucket_range_from_keys(&first_key, &last_key);
    let data_file = DataFile::new(
        DataFileType::Parquet,
        first_key,
        last_key,
        file_id,
        TrackedFileId::new(file_manager, file_id),
        0,
        file_size,
        bucket_range.clone(),
        bucket_range,
    );
    data_file.set_meta_bytes(meta_bytes);
    data_file.set_max_expired_at(0);
    file_manager.finalize_data_file(&data_file)?;
    Ok(Arc::new(data_file))
}

fn parquet_hot_keys_for_first_row_group(
    file_manager: &Arc<FileManager>,
    file: &DataFile,
    cache_namespace: u64,
    num_columns: usize,
) -> Vec<crate::cache::BlockCacheKey> {
    let reader = file_manager.open_data_file_reader(file.file_id).unwrap();
    let reader: Arc<dyn crate::file::RandomAccessFile> = Arc::new(reader);
    let parquet_reader =
        SerializedFileReader::new(RandomAccessChunkReader::from_arc(reader)).unwrap();
    parquet_row_group_cache_keys(
        parquet_reader.metadata().row_group(0),
        cache_namespace,
        file.file_id,
        None,
        num_columns,
    )
    .unwrap()
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_basic() {
    let test_dir = "/tmp/compaction_basic_test";
    cleanup_test_dir(test_dir);

    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();

    let metrics_manager = Arc::new(MetricsManager::new("compaction-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );

    let num_columns = 1;

    // Create first SST file with entries a, c, e
    let file1 = create_test_sst(
        &file_manager,
        vec![
            (b"a", &make_value_bytes(b"v1", num_columns)),
            (b"c", &make_value_bytes(b"v3", num_columns)),
            (b"e", &make_value_bytes(b"v5", num_columns)),
        ],
    )
    .unwrap();

    // Create second SST file with entries b, d, f
    let file2 = create_test_sst(
        &file_manager,
        vec![
            (b"b", &make_value_bytes(b"v2", num_columns)),
            (b"d", &make_value_bytes(b"v4", num_columns)),
            (b"f", &make_value_bytes(b"v6", num_columns)),
        ],
    )
    .unwrap();

    let file1_handle = Arc::clone(&file1);
    let file2_handle = Arc::clone(&file2);

    // Create sorted runs
    let run1 = SortedRun::new(0, vec![file1]);
    let run2 = SortedRun::new(1, vec![file2]);

    let options = CompactionConfig {
        num_columns,
        target_file_size: 1024 * 1024, // 1MB - all entries fit in one file
        bloom_filter_enabled: true,
        bloom_bits_per_key: 10,
        pinned_metadata_max_level: Some(0),
        ..Default::default()
    };

    // Create and execute compaction
    let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions {
        metrics: None,
        block_size: options.block_size,
        buffer_size: options.buffer_size,
        num_columns: options.num_columns,
        bloom_filter_enabled: options.bloom_filter_enabled,
        bloom_bits_per_key: options.bloom_bits_per_key,
        partitioned_index: options.partitioned_index,
        read_metadata_cache_mode: crate::SstReadMetadataCacheMode::Eager,
        data_block_restart_interval: 16,
        compression: crate::SstCompressionAlgorithm::None,
        value_has_ttl: true,
        block_checksum_enabled: false,
    });
    crate::metrics_registry::init_metrics();
    let compaction_metrics = Arc::new(CompactionTaskMetrics::new("compaction-success-test"));
    let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
    let task = CompactionTask::new(
        compaction_metrics,
        sst_metrics,
        0,
        vec![run1, run2],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::SSTable,
        Arc::new(crate::ttl::TTLProvider::disabled()),
        schema_manager_for(num_columns),
    );

    let executor = CompactionExecutor::new(options, Arc::new(DbLifecycle::new_open())).unwrap();

    let result = executor.execute_blocking(task, None).unwrap();
    assert!(file1_handle.pinned_sst_read_metadata().is_some());
    assert!(file2_handle.pinned_sst_read_metadata().is_none());
    assert_eq!(result.edit().level_edits.len(), 2);
    assert!(
        result
            .edit()
            .level_edits
            .iter()
            .any(|edit| edit.new_files.len() == 1)
    );

    // Verify output
    assert!(!result.new_files().is_empty());
    let completed = crate::metrics_registry::snapshot_metrics(Some("compaction-success-test"))
        .into_iter()
        .find(|sample| sample.name == "compactions_total")
        .expect("successful compaction must increment its completion counter");
    assert_eq!(completed.value, crate::MetricValue::Counter(1));

    // Verify first file has correct key range
    let first_file = &result.new_files()[0];
    assert_eq!(first_file.start_key, b"a");
    assert_eq!(first_file.end_key, b"f");

    // Verify file exists and is readable
    let reader = file_manager
        .open_data_file_reader(first_file.file_id)
        .unwrap();
    let mut iter = crate::sst::SSTIterator::with_cache_and_file(
        Box::new(reader),
        first_file,
        crate::sst::SSTIteratorOptions {
            bloom_filter_enabled: true,
            ..crate::sst::SSTIteratorOptions::default()
        },
        None,
    )
    .unwrap();
    iter.seek_to_first().unwrap();

    // Verify entries are merged and sorted
    let mut keys = vec![];
    while iter.valid() {
        let (key, _) = iter.current().unwrap().unwrap();
        keys.push(key.to_vec());
        iter.next().unwrap();
    }

    assert_eq!(
        keys,
        vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
            b"d".to_vec(),
            b"e".to_vec(),
            b"f".to_vec()
        ]
    );

    cleanup_test_dir(test_dir);
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_prunes_truncated_keys() {
    let test_dir = "/tmp/compaction_truncation_test";
    cleanup_test_dir(test_dir);

    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("compaction-truncation-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );
    let num_columns = 1;
    let encoded_a = encode_key(&Key::new(0, Bytes::from_static(b"a")));
    let encoded_b = encode_key(&Key::new(0, Bytes::from_static(b"b")));
    let encoded_c = encode_key(&Key::new(0, Bytes::from_static(b"c")));
    let file = create_test_sst(
        &file_manager,
        vec![
            (encoded_a.as_ref(), &make_value_bytes(b"va", num_columns)),
            (encoded_b.as_ref(), &make_value_bytes(b"vb", num_columns)),
            (encoded_c.as_ref(), &make_value_bytes(b"vc", num_columns)),
        ],
    )
    .unwrap();
    let run = SortedRun::new(0, vec![file]);
    let mut truncation_cursors = TruncationCursorMap::new();
    truncation_cursors.insert(TruncationCursorId::new(0, 0), b"a".to_vec());
    let options = CompactionConfig {
        num_columns,
        target_file_size: 1024 * 1024,
        bloom_filter_enabled: true,
        bloom_bits_per_key: 10,
        ..Default::default()
    };
    let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions {
        metrics: None,
        block_size: options.block_size,
        buffer_size: options.buffer_size,
        num_columns: options.num_columns,
        bloom_filter_enabled: options.bloom_filter_enabled,
        bloom_bits_per_key: options.bloom_bits_per_key,
        partitioned_index: options.partitioned_index,
        read_metadata_cache_mode: crate::SstReadMetadataCacheMode::Eager,
        data_block_restart_interval: 16,
        compression: crate::SstCompressionAlgorithm::None,
        value_has_ttl: true,
        block_checksum_enabled: false,
    });
    let task = CompactionTask::new(
        Arc::new(CompactionTaskMetrics::new("test")),
        Arc::new(crate::sst::SSTIteratorMetrics::new("test")),
        0,
        vec![run],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::SSTable,
        Arc::new(crate::ttl::TTLProvider::disabled()),
        schema_manager_for(num_columns),
    )
    .with_truncation_cursors(truncation_cursors);
    let executor = CompactionExecutor::new(options, Arc::new(DbLifecycle::new_open())).unwrap();
    let result = executor.execute_blocking(task, None).unwrap();

    assert_eq!(result.new_files().len(), 1);
    let output = &result.new_files()[0];
    assert_eq!(output.start_key, encoded_b.to_vec());
    assert_eq!(output.end_key, encoded_c.to_vec());

    let reader = file_manager.open_data_file_reader(output.file_id).unwrap();
    let mut iter = crate::sst::SSTIterator::with_cache_and_file(
        Box::new(reader),
        output,
        crate::sst::SSTIteratorOptions {
            bloom_filter_enabled: true,
            ..crate::sst::SSTIteratorOptions::default()
        },
        None,
    )
    .unwrap();
    iter.seek_to_first().unwrap();
    let mut keys = Vec::new();
    while iter.valid() {
        let (key, _) = iter.current().unwrap().unwrap();
        keys.push(key);
        iter.next().unwrap();
    }
    assert_eq!(keys, vec![encoded_b, encoded_c]);

    cleanup_test_dir(test_dir);
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_with_duplicates() {
    let test_dir = "/tmp/compaction_duplicates_test";
    cleanup_test_dir(test_dir);

    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();

    let metrics_manager = Arc::new(MetricsManager::new("compaction-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );

    let num_columns = 1;

    // Create first SST file (newer) with entries a, b
    let file1 = create_test_sst(
        &file_manager,
        vec![
            (b"a", &make_value_bytes(b"new_a", num_columns)),
            (b"b", &make_value_bytes(b"new_b", num_columns)),
        ],
    )
    .unwrap();

    // Create second SST file (older) with entries a, b, c
    let file2 = create_test_sst(
        &file_manager,
        vec![
            (b"a", &make_value_bytes(b"old_a", num_columns)),
            (b"b", &make_value_bytes(b"old_b", num_columns)),
            (b"c", &make_value_bytes(b"old_c", num_columns)),
        ],
    )
    .unwrap();

    // Create sorted runs (first run is newer)
    let run1 = SortedRun::new(0, vec![file1]);
    let run2 = SortedRun::new(0, vec![file2]);

    let options = CompactionConfig {
        num_columns,
        bloom_filter_enabled: true,
        bloom_bits_per_key: 10,
        ..Default::default()
    };

    // Create and execute compaction
    let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions {
        metrics: None,
        block_size: options.block_size,
        buffer_size: options.buffer_size,
        num_columns: options.num_columns,
        bloom_filter_enabled: options.bloom_filter_enabled,
        bloom_bits_per_key: options.bloom_bits_per_key,
        partitioned_index: options.partitioned_index,
        read_metadata_cache_mode: crate::SstReadMetadataCacheMode::Eager,
        data_block_restart_interval: 16,
        compression: crate::SstCompressionAlgorithm::None,
        value_has_ttl: true,
        block_checksum_enabled: false,
    });
    let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
    let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
    let task = CompactionTask::new(
        compaction_metrics,
        sst_metrics,
        0,
        vec![run1, run2],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::SSTable,
        Arc::new(crate::ttl::TTLProvider::disabled()),
        schema_manager_for(num_columns),
    );

    let executor = CompactionExecutor::new(options, Arc::new(DbLifecycle::new_open())).unwrap();

    let result = executor.execute_blocking(task, None).unwrap();
    assert_eq!(result.edit().level_edits.len(), 2);
    assert!(
        result
            .edit()
            .level_edits
            .iter()
            .any(|edit| edit.new_files.len() == 1)
    );

    // Verify output
    assert_eq!(result.new_files().len(), 1);

    // Read and verify merged entries
    let reader = file_manager
        .open_data_file_reader(result.new_files()[0].file_id)
        .unwrap();
    let mut iter = crate::sst::SSTIterator::with_cache_and_file(
        Box::new(reader),
        result.new_files()[0].as_ref(),
        crate::sst::SSTIteratorOptions {
            bloom_filter_enabled: true,
            num_columns,
            ..Default::default()
        },
        None,
    )
    .unwrap();

    iter.seek_to_first().unwrap();

    // Key "a" - newer value should win
    assert!(iter.valid());
    let (key, mut value) = iter.current().unwrap().unwrap();
    assert_eq!(&key[..], b"a");
    let decoded = crate::sst::row_codec::decode_value(&mut value, num_columns).unwrap();
    assert_eq!(
        decoded.columns()[0].as_ref().unwrap().data().as_ref(),
        b"new_a"
    );

    // Key "b" - newer value should win
    iter.next().unwrap();
    assert!(iter.valid());
    let (key, mut value) = iter.current().unwrap().unwrap();
    assert_eq!(&key[..], b"b");
    let decoded = crate::sst::row_codec::decode_value(&mut value, num_columns).unwrap();
    assert_eq!(
        decoded.columns()[0].as_ref().unwrap().data().as_ref(),
        b"new_b"
    );

    // Key "c" - only in older file
    iter.next().unwrap();
    assert!(iter.valid());
    let (key, mut value) = iter.current().unwrap().unwrap();
    assert_eq!(&key[..], b"c");
    let decoded = crate::sst::row_codec::decode_value(&mut value, num_columns).unwrap();
    assert_eq!(
        decoded.columns()[0].as_ref().unwrap().data().as_ref(),
        b"old_c"
    );

    cleanup_test_dir(test_dir);
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_lazy_merge_with_separated_values() {
    let test_dir = "/tmp/compaction_separated_merge_test";
    cleanup_test_dir(test_dir);

    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();

    let metrics_manager = Arc::new(MetricsManager::new("compaction-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );

    let num_columns = 1;
    let old_put_separated = [0x11u8; 8];
    let new_merge_separated_a = [0x22u8; 8];
    let new_merge_separated_b = [0x33u8; 8];

    // Newer run contains merge-separated values.
    let file1 = create_test_sst(
        &file_manager,
        vec![
            (
                b"a",
                &make_typed_value_bytes(
                    ValueType::MergeSeparated,
                    &new_merge_separated_a,
                    num_columns,
                ),
            ),
            (
                b"b",
                &make_typed_value_bytes(
                    ValueType::MergeSeparated,
                    &new_merge_separated_b,
                    num_columns,
                ),
            ),
        ],
    )
    .unwrap();

    // Older run contains one separated base and one inline base.
    let file2 = create_test_sst(
        &file_manager,
        vec![
            (
                b"a",
                &make_typed_value_bytes(ValueType::PutSeparated, &old_put_separated, num_columns),
            ),
            (
                b"b",
                &make_typed_value_bytes(ValueType::Put, b"base_b", num_columns),
            ),
        ],
    )
    .unwrap();

    let run1 = SortedRun::new(0, vec![file1]);
    let run2 = SortedRun::new(0, vec![file2]);

    let options = CompactionConfig {
        num_columns,
        bloom_filter_enabled: true,
        bloom_bits_per_key: 10,
        ..Default::default()
    };
    let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions {
        metrics: None,
        block_size: options.block_size,
        buffer_size: options.buffer_size,
        num_columns: options.num_columns,
        bloom_filter_enabled: options.bloom_filter_enabled,
        bloom_bits_per_key: options.bloom_bits_per_key,
        partitioned_index: options.partitioned_index,
        read_metadata_cache_mode: crate::SstReadMetadataCacheMode::Eager,
        data_block_restart_interval: 16,
        compression: crate::SstCompressionAlgorithm::None,
        value_has_ttl: true,
        block_checksum_enabled: false,
    });
    let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
    let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
    let task = CompactionTask::new(
        compaction_metrics,
        sst_metrics,
        0,
        vec![run1, run2],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::SSTable,
        Arc::new(crate::ttl::TTLProvider::disabled()),
        schema_manager_for(num_columns),
    );
    let executor = CompactionExecutor::new(options, Arc::new(DbLifecycle::new_open())).unwrap();
    let result = executor.execute_blocking(task, None).unwrap();

    assert_eq!(result.new_files().len(), 1);
    let reader = file_manager
        .open_data_file_reader(result.new_files()[0].file_id)
        .unwrap();
    let mut iter = crate::sst::SSTIterator::with_cache_and_file(
        Box::new(reader),
        result.new_files()[0].as_ref(),
        crate::sst::SSTIteratorOptions {
            bloom_filter_enabled: true,
            num_columns,
            ..Default::default()
        },
        None,
    )
    .unwrap();

    iter.seek_to_first().unwrap();
    assert!(iter.valid());
    let (key, mut value) = iter.current().unwrap().unwrap();
    assert_eq!(&key[..], b"a");
    let decoded = crate::sst::row_codec::decode_value(&mut value, num_columns).unwrap();
    let column = decoded.columns()[0].as_ref().unwrap();
    assert_eq!(column.value_type, ValueType::PutSeparatedArray);
    let merged_items = decode_merge_separated_array(column.data()).unwrap();
    assert_eq!(merged_items.len(), 2);
    assert_eq!(merged_items[0].value_type, ValueType::PutSeparated);
    assert_eq!(merged_items[0].data(), old_put_separated);
    assert_eq!(merged_items[1].value_type, ValueType::MergeSeparated);
    assert_eq!(merged_items[1].data(), new_merge_separated_a);

    iter.next().unwrap();
    assert!(iter.valid());
    let (key, mut value) = iter.current().unwrap().unwrap();
    assert_eq!(&key[..], b"b");
    let decoded = crate::sst::row_codec::decode_value(&mut value, num_columns).unwrap();
    let column = decoded.columns()[0].as_ref().unwrap();
    assert_eq!(column.value_type, ValueType::PutSeparatedArray);
    let merged_items = decode_merge_separated_array(column.data()).unwrap();
    assert_eq!(merged_items.len(), 2);
    assert_eq!(merged_items[0].value_type, ValueType::Put);
    assert_eq!(merged_items[0].data(), b"base_b");
    assert_eq!(merged_items[1].value_type, ValueType::MergeSeparated);
    assert_eq!(merged_items[1].data(), new_merge_separated_b);

    cleanup_test_dir(test_dir);
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_evolves_older_schema_values() {
    let test_dir = "/tmp/compaction_schema_evolution_test";
    cleanup_test_dir(test_dir);

    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("compaction-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );

    let old_num_columns = 1;
    let old_file = create_test_sst(
        &file_manager,
        vec![(b"k", &make_value_bytes(b"old", old_num_columns))],
    )
    .unwrap();

    let schema_manager = Arc::new(SchemaManager::new(old_num_columns));
    let mut schema_builder = schema_manager.builder();
    schema_builder.add_column(1, None, None, None).unwrap();
    let target_schema = schema_builder.commit();

    let options = CompactionConfig {
        num_columns: target_schema.num_columns(),
        bloom_filter_enabled: true,
        bloom_bits_per_key: 10,
        ..Default::default()
    };
    let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions {
        metrics: None,
        block_size: options.block_size,
        buffer_size: options.buffer_size,
        num_columns: options.num_columns,
        bloom_filter_enabled: options.bloom_filter_enabled,
        bloom_bits_per_key: options.bloom_bits_per_key,
        partitioned_index: options.partitioned_index,
        read_metadata_cache_mode: crate::SstReadMetadataCacheMode::Eager,
        data_block_restart_interval: 16,
        compression: crate::SstCompressionAlgorithm::None,
        value_has_ttl: true,
        block_checksum_enabled: false,
    });
    let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
    let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
    let task = CompactionTask::new(
        compaction_metrics,
        sst_metrics,
        0,
        vec![SortedRun::new(0, vec![old_file])],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::SSTable,
        Arc::new(crate::ttl::TTLProvider::disabled()),
        Arc::clone(&schema_manager),
    );

    let executor = CompactionExecutor::new(options, Arc::new(DbLifecycle::new_open())).unwrap();
    let result = executor.execute_blocking(task, None).unwrap();
    assert_eq!(result.new_files().len(), 1);
    assert_eq!(result.new_files()[0].schema_id, target_schema.version());
    let write_metadata = result.new_files()[0]
        .sst_read_metadata()
        .expect("compaction should install SST read metadata eagerly");

    let reader = file_manager
        .open_data_file_reader(result.new_files()[0].file_id)
        .unwrap();
    let mut iter = crate::sst::SSTIterator::with_cache_and_file(
        Box::new(reader),
        result.new_files()[0].as_ref(),
        crate::sst::SSTIteratorOptions {
            bloom_filter_enabled: true,
            num_columns: target_schema.num_columns(),
            ..Default::default()
        },
        None,
    )
    .unwrap();
    assert!(Arc::ptr_eq(
        &write_metadata,
        &result.new_files()[0].sst_read_metadata().unwrap()
    ));
    iter.seek_to_first().unwrap();
    let (_, mut value) = iter.current().unwrap().unwrap();
    let decoded =
        crate::sst::row_codec::decode_value(&mut value, target_schema.num_columns()).unwrap();
    assert_eq!(decoded.columns().len(), 2);
    assert_eq!(
        decoded.columns()[0].as_ref().unwrap().data().as_ref(),
        b"old"
    );
    assert!(decoded.columns()[1].is_none());

    cleanup_test_dir(test_dir);
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_uses_latest_schema_width_when_schema_evolves_after_task_creation() {
    let test_dir = "/tmp/compaction_runtime_schema_width_test";
    cleanup_test_dir(test_dir);

    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("compaction-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );

    let old_num_columns = 1;
    let old_file = create_test_sst(
        &file_manager,
        vec![(b"k", &make_value_bytes(b"old", old_num_columns))],
    )
    .unwrap();

    let schema_manager = Arc::new(SchemaManager::new(old_num_columns));
    let options = CompactionConfig {
        num_columns: old_num_columns,
        bloom_filter_enabled: true,
        bloom_bits_per_key: 10,
        ..Default::default()
    };
    let writer_options = WriterOptions::Sst(SSTWriterOptions {
        metrics: None,
        block_size: options.block_size,
        buffer_size: options.buffer_size,
        num_columns: old_num_columns,
        bloom_filter_enabled: options.bloom_filter_enabled,
        bloom_bits_per_key: options.bloom_bits_per_key,
        partitioned_index: options.partitioned_index,
        read_metadata_cache_mode: crate::SstReadMetadataCacheMode::Eager,
        data_block_restart_interval: 16,
        compression: crate::SstCompressionAlgorithm::None,
        value_has_ttl: true,
        block_checksum_enabled: false,
    });
    let factory = crate::compaction::make_data_file_builder_factory(writer_options.clone());
    let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
    let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
    let task = CompactionTask::new(
        compaction_metrics,
        sst_metrics,
        0,
        vec![SortedRun::new(0, vec![old_file])],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::SSTable,
        Arc::new(crate::ttl::TTLProvider::disabled()),
        Arc::clone(&schema_manager),
    )
    .with_writer_options_factory(WriterOptionsFactory::from(writer_options));

    let mut schema_builder = schema_manager.builder();
    schema_builder.add_column(1, None, None, None).unwrap();
    let target_schema = schema_builder.commit();

    let executor = CompactionExecutor::new(options, Arc::new(DbLifecycle::new_open())).unwrap();
    let result = executor.execute_blocking(task, None).unwrap();
    assert_eq!(result.new_files().len(), 1);
    assert_eq!(result.new_files()[0].schema_id, target_schema.version());

    let reader = file_manager
        .open_data_file_reader(result.new_files()[0].file_id)
        .unwrap();
    let mut iter = crate::sst::SSTIterator::with_cache_and_file(
        Box::new(reader),
        result.new_files()[0].as_ref(),
        crate::sst::SSTIteratorOptions {
            bloom_filter_enabled: true,
            num_columns: target_schema.num_columns(),
            ..Default::default()
        },
        None,
    )
    .unwrap();
    iter.seek_to_first().unwrap();
    let (_, mut value) = iter.current().unwrap().unwrap();
    let decoded =
        crate::sst::row_codec::decode_value(&mut value, target_schema.num_columns()).unwrap();
    assert_eq!(decoded.columns().len(), 2);
    assert_eq!(
        decoded.columns()[0].as_ref().unwrap().data().as_ref(),
        b"old"
    );
    assert!(decoded.columns()[1].is_none());

    cleanup_test_dir(test_dir);
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_multiple_output_files() {
    let test_dir = "/tmp/compaction_multi_output_test";
    cleanup_test_dir(test_dir);

    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();

    let metrics_manager = Arc::new(MetricsManager::new("compaction-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );

    let num_columns = 1;

    // Create a large SST file
    let mut entries = vec![];
    for i in 0..100 {
        let key = format!("key{:04}", i);
        let value = format!("value{:04}_with_some_extra_padding_data", i);
        entries.push((
            key.into_bytes(),
            make_value_bytes(value.as_bytes(), num_columns),
        ));
    }

    let (file_id, writer_file) = file_manager.create_data_file().unwrap();
    let mut writer = SSTWriter::new(
        writer_file,
        SSTWriterOptions {
            bloom_filter_enabled: true,
            ..SSTWriterOptions::default()
        },
    );

    for (key, value) in &entries {
        writer.add(key, value).unwrap();
    }
    let FileBuildResult {
        first_key,
        last_key,
        file_size,
        meta_bytes,
        sst_read_metadata,
        max_expired_at,
    } = writer.finish_with_range().unwrap();
    let bucket_range = DataFile::bucket_range_from_keys(&first_key, &last_key);

    let file = DataFile::new(
        DataFileType::SSTable,
        first_key,
        last_key,
        file_id,
        TrackedFileId::new(&file_manager, file_id),
        0,
        file_size,
        bucket_range.clone(),
        bucket_range,
    );
    file.set_meta_bytes(meta_bytes);
    file.set_max_expired_at(max_expired_at);
    if let Some(metadata) = sst_read_metadata {
        file.set_sst_read_metadata(metadata);
    }
    let file = Arc::new(file);

    let run = SortedRun::new(0, vec![file]);

    let options = CompactionConfig {
        num_columns,
        target_file_size: 500, // Very small to force multiple files
        bloom_filter_enabled: true,
        bloom_bits_per_key: 10,
        ..Default::default()
    };

    // Create compaction with very small target file size to force multiple output files
    let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions {
        metrics: None,
        block_size: options.block_size,
        buffer_size: options.buffer_size,
        num_columns: options.num_columns,
        bloom_filter_enabled: options.bloom_filter_enabled,
        bloom_bits_per_key: options.bloom_bits_per_key,
        partitioned_index: options.partitioned_index,
        read_metadata_cache_mode: crate::SstReadMetadataCacheMode::Eager,
        data_block_restart_interval: 16,
        compression: crate::SstCompressionAlgorithm::None,
        value_has_ttl: true,
        block_checksum_enabled: false,
    });
    let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
    let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
    let task = CompactionTask::new(
        compaction_metrics,
        sst_metrics,
        0,
        vec![run],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::SSTable,
        Arc::new(crate::ttl::TTLProvider::disabled()),
        schema_manager_for(num_columns),
    );

    let executor = CompactionExecutor::new(options, Arc::new(DbLifecycle::new_open())).unwrap();

    let result = executor.execute_blocking(task, None).unwrap();
    assert_eq!(result.edit().level_edits.len(), 2);
    assert!(
        result
            .edit()
            .level_edits
            .iter()
            .any(|edit| edit.new_files.len() > 1)
    );

    // Should have multiple output files
    assert!(result.new_files().len() > 1);

    // Verify files are sorted by key range
    for i in 1..result.new_files().len() {
        let prev_file = &result.new_files()[i - 1];
        let curr_file = &result.new_files()[i];
        assert!(
            prev_file.end_key < curr_file.start_key,
            "Files should have non-overlapping, sorted key ranges"
        );
    }

    cleanup_test_dir(test_dir);
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_empty_input() {
    let test_dir = "/tmp/compaction_empty_test";
    cleanup_test_dir(test_dir);

    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();

    let metrics_manager = Arc::new(MetricsManager::new("compaction-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );

    let options = CompactionConfig::default();

    // Create compaction with no sorted runs
    let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions {
        metrics: None,
        block_size: options.block_size,
        buffer_size: options.buffer_size,
        num_columns: options.num_columns,
        bloom_filter_enabled: options.bloom_filter_enabled,
        bloom_bits_per_key: options.bloom_bits_per_key,
        partitioned_index: options.partitioned_index,
        read_metadata_cache_mode: crate::SstReadMetadataCacheMode::Eager,
        data_block_restart_interval: 16,
        compression: crate::SstCompressionAlgorithm::None,
        value_has_ttl: true,
        block_checksum_enabled: false,
    });
    let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
    let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
    let task = CompactionTask::new(
        compaction_metrics,
        sst_metrics,
        0,
        vec![],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::SSTable,
        Arc::new(crate::ttl::TTLProvider::disabled()),
        schema_manager_for(options.num_columns),
    );

    let executor = CompactionExecutor::with_defaults(Arc::new(DbLifecycle::new_open())).unwrap();
    let result = executor.execute_blocking(task, None).unwrap();
    assert_eq!(result.edit().level_edits.len(), 1);
    assert!(result.edit().level_edits[0].new_files.is_empty());

    // Should have no output files
    assert!(result.new_files().is_empty());

    cleanup_test_dir(test_dir);
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_tracks_vlog_entry_deletions_for_shadowed_values() {
    let test_dir = "/tmp/compaction_vlog_entry_delta_test";
    cleanup_test_dir(test_dir);

    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("compaction-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );
    let num_columns = 1;
    let pointer = crate::vlog::VlogPointer::new(9, 0);
    let older = create_test_sst(
        &file_manager,
        vec![(
            b"k",
            &make_typed_value_bytes(ValueType::PutSeparated, &pointer.to_bytes(), num_columns),
        )],
    )
    .unwrap();
    let newer = create_test_sst(
        &file_manager,
        vec![(b"k", &make_value_bytes(b"inline", num_columns))],
    )
    .unwrap();

    let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions::default());
    let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
    let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
    let task = CompactionTask::new(
        compaction_metrics,
        sst_metrics,
        0,
        vec![
            SortedRun::new(0, vec![newer]),
            SortedRun::new(1, vec![older]),
        ],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::SSTable,
        Arc::new(crate::ttl::TTLProvider::disabled()),
        schema_manager_for(num_columns),
    );
    let executor = CompactionExecutor::with_defaults(Arc::new(DbLifecycle::new_open())).unwrap();
    let result = executor.execute_blocking(task, None).unwrap();
    let deltas: std::collections::HashMap<u32, i64> = result
        .vlog_edit()
        .unwrap()
        .entry_deltas()
        .into_iter()
        .collect();
    assert_eq!(deltas.get(&9).copied(), Some(-1));
    cleanup_test_dir(test_dir);
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_expired_separated_values_emit_vlog_deltas() {
    // End-to-end: a file with separated values whose entries are expired should
    // produce VLOG entry-delta = -1 per separated pointer when rewritten.
    let test_dir = "/tmp/compaction_expired_vlog_delta_test";
    cleanup_test_dir(test_dir);

    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("compaction-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );
    let num_columns = 1;
    let pointer = crate::vlog::VlogPointer::new(9, 0);
    // Create an SST with a PutSeparated value that has expired_at = 100.
    let expired_sep = create_test_sst(
        &file_manager,
        vec![(
            b"k",
            &make_typed_value_bytes_with_expired(
                ValueType::PutSeparated,
                &pointer.to_bytes(),
                num_columns,
                Some(100),
            ),
        )],
    )
    .unwrap();

    // Use a manual time provider set past the expiration (now = 200 > 100).
    let time_provider = Arc::new(crate::time::ManualTimeProvider::new(200));
    let ttl_provider = Arc::new(crate::ttl::TTLProvider::new(
        &crate::ttl::TtlConfig {
            enabled: true,
            default_ttl_seconds: None,
        },
        time_provider,
    ));

    let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions::default());
    let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
    let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
    let task = CompactionTask::new(
        compaction_metrics,
        sst_metrics,
        0,
        vec![SortedRun::new(1, vec![expired_sep])],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::SSTable,
        ttl_provider,
        schema_manager_for(num_columns),
    );
    let executor = CompactionExecutor::with_defaults(Arc::new(DbLifecycle::new_open())).unwrap();
    let result = executor.execute_blocking(task, None).unwrap();
    // The expired separated value should produce a -1 delta for VLOG file seq 9.
    let deltas: std::collections::HashMap<u32, i64> = result
        .vlog_edit()
        .expect("VLOG edit should be present for expired separated values")
        .entry_deltas()
        .into_iter()
        .collect();
    assert_eq!(
        deltas.get(&9).copied(),
        Some(-1),
        "expired separated value should produce VLOG removal delta"
    );
    cleanup_test_dir(test_dir);
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_corrupt_expired_separated_value_returns_error() {
    // A corrupt expired separated value must abort compaction with an error, not be
    // silently dropped (which would leak the VLOG entry-count reference).
    let test_dir = "/tmp/compaction_corrupt_expired_sep_test";
    cleanup_test_dir(test_dir);

    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("compaction-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );
    let num_columns = 1;

    // Build a corrupt value: 4-byte expired_at = 100 (little-endian), then a truncated
    // column body with no value_type byte. `value_expired_at` reads only the first 4 bytes
    // and succeeds, but `decode_value` (called via `into_decoded` in the expired callback)
    // fails because there is no value_type byte.
    let corrupt_expired_value: Vec<u8> = 100u32.to_le_bytes().to_vec();

    let corrupt_file = create_test_sst(
        &file_manager,
        vec![(b"k", corrupt_expired_value.as_slice())],
    )
    .unwrap();

    let time_provider = Arc::new(crate::time::ManualTimeProvider::new(200));
    let ttl_provider = Arc::new(crate::ttl::TTLProvider::new(
        &crate::ttl::TtlConfig {
            enabled: true,
            default_ttl_seconds: None,
        },
        time_provider,
    ));

    let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions::default());
    let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
    let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
    let task = CompactionTask::new(
        compaction_metrics,
        sst_metrics,
        0,
        vec![SortedRun::new(1, vec![corrupt_file])],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::SSTable,
        ttl_provider,
        schema_manager_for(num_columns),
    );
    let executor = CompactionExecutor::with_defaults(Arc::new(DbLifecycle::new_open())).unwrap();
    let result = executor.execute_blocking(task, None);
    assert!(
        result.is_err(),
        "compaction must return an error for a corrupt expired separated value, not silently drop it"
    );
    cleanup_test_dir(test_dir);
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_expired_sep_plus_live_inline_output_not_separated() {
    // An expired separated value plus a surviving inline value in the same compaction
    // must: (1) emit a VLOG removal delta for the expired separated pointer, and
    // (2) produce an output file whose has_separated_values is false (since the only
    // surviving value is inline). This verifies that on_expired_value does not pollute
    // the output file's separated-values flag.
    let test_dir = "/tmp/compaction_expired_sep_live_inline_test";
    cleanup_test_dir(test_dir);

    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("compaction-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );
    let num_columns = 1;
    let pointer = crate::vlog::VlogPointer::new(7, 0);

    // File A: an expired PutSeparated value at key "a".
    let expired_sep = create_test_sst(
        &file_manager,
        vec![(
            b"a",
            &make_typed_value_bytes_with_expired(
                ValueType::PutSeparated,
                &pointer.to_bytes(),
                num_columns,
                Some(100),
            ),
        )],
    )
    .unwrap();

    // File B: a live (non-expired) inline Put value at key "b".
    let live_inline = create_test_sst(
        &file_manager,
        vec![(
            b"b",
            &make_typed_value_bytes_with_expired(ValueType::Put, b"alive", num_columns, Some(500)),
        )],
    )
    .unwrap();

    let time_provider = Arc::new(crate::time::ManualTimeProvider::new(200));
    let ttl_provider = Arc::new(crate::ttl::TTLProvider::new(
        &crate::ttl::TtlConfig {
            enabled: true,
            default_ttl_seconds: None,
        },
        time_provider,
    ));

    let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions::default());
    let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
    let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
    let task = CompactionTask::new(
        compaction_metrics,
        sst_metrics,
        0,
        vec![SortedRun::new(1, vec![expired_sep, live_inline])],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::SSTable,
        ttl_provider,
        schema_manager_for(num_columns),
    );
    let executor = CompactionExecutor::with_defaults(Arc::new(DbLifecycle::new_open())).unwrap();
    let result = executor.execute_blocking(task, None).unwrap();

    // (1) VLOG removal delta for the expired separated pointer (file seq 7).
    let deltas: std::collections::HashMap<u32, i64> = result
        .vlog_edit()
        .expect("VLOG edit should be present for expired separated values")
        .entry_deltas()
        .into_iter()
        .collect();
    assert_eq!(
        deltas.get(&7).copied(),
        Some(-1),
        "expired separated value should produce VLOG removal delta"
    );

    // (2) The output file must NOT be marked as having separated values, because the
    // only surviving value is inline.
    let output_files = result.new_files();
    assert_eq!(output_files.len(), 1);
    assert!(
        !output_files[0].has_separated_values(),
        "output file must not be marked as separated when all surviving values are inline"
    );
    cleanup_test_dir(test_dir);
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_output_parquet() {
    let test_dir = "/tmp/compaction_output_parquet_test";
    cleanup_test_dir(test_dir);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("compaction-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );
    let num_columns = 1;
    let file1 = create_test_sst(
        &file_manager,
        vec![
            (b"a", &make_value_bytes(b"v1", num_columns)),
            (b"c", &make_value_bytes(b"v3", num_columns)),
        ],
    )
    .unwrap();
    let file2 = create_test_sst(
        &file_manager,
        vec![
            (b"b", &make_value_bytes(b"v2", num_columns)),
            (b"d", &make_value_bytes(b"v4", num_columns)),
        ],
    )
    .unwrap();
    let options = CompactionConfig {
        num_columns,
        target_file_size: 1024 * 1024,
        ..Default::default()
    };
    let factory = crate::compaction::make_data_file_builder_factory(WriterOptions::Parquet(
        ParquetWriterOptions {
            row_group_size_bytes: 256 * 1024,
            buffer_size: options.buffer_size,
            num_columns,
        },
    ));
    let task = CompactionTask::new(
        Arc::new(CompactionTaskMetrics::new("test")),
        Arc::new(crate::sst::SSTIteratorMetrics::new("test")),
        0,
        vec![
            SortedRun::new(0, vec![file1]),
            SortedRun::new(1, vec![file2]),
        ],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::Parquet,
        Arc::new(crate::ttl::TTLProvider::disabled()),
        schema_manager_for(num_columns),
    );
    let executor = CompactionExecutor::new(options, Arc::new(DbLifecycle::new_open())).unwrap();
    let result = executor.execute_blocking(task, None).unwrap();
    assert!(!result.new_files().is_empty());
    assert!(
        result
            .new_files()
            .iter()
            .all(|file| file.file_type == DataFileType::Parquet)
    );
    let output = result.new_files()[0].clone();
    let reader = file_manager.open_data_file_reader(output.file_id).unwrap();
    let mut iter =
        ParquetIterator::from_data_file(Box::new(reader), output.as_ref(), None).unwrap();
    iter.seek_to_first().unwrap();
    let mut keys = Vec::new();
    while iter.valid() {
        keys.push(iter.key().unwrap().unwrap().to_vec());
        iter.next().unwrap();
    }
    assert_eq!(
        keys,
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
    );
    cleanup_test_dir(test_dir);
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_input_parquet_output_parquet() {
    let test_dir = "/tmp/compaction_parquet_to_parquet_test";
    cleanup_test_dir(test_dir);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("compaction-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );
    let num_columns = 1;
    let file1 = create_test_parquet(
        &file_manager,
        vec![
            (b"a", &make_value_bytes(b"new_a", num_columns)),
            (b"b", &make_value_bytes(b"new_b", num_columns)),
        ],
    )
    .unwrap();
    let file2 = create_test_parquet(
        &file_manager,
        vec![
            (b"a", &make_value_bytes(b"old_a", num_columns)),
            (b"c", &make_value_bytes(b"old_c", num_columns)),
        ],
    )
    .unwrap();
    let options = CompactionConfig {
        num_columns,
        target_file_size: 1024 * 1024,
        ..Default::default()
    };
    let factory = crate::compaction::make_data_file_builder_factory(WriterOptions::Parquet(
        ParquetWriterOptions {
            row_group_size_bytes: 256 * 1024,
            buffer_size: options.buffer_size,
            num_columns,
        },
    ));
    let task = CompactionTask::new(
        Arc::new(CompactionTaskMetrics::new("test")),
        Arc::new(crate::sst::SSTIteratorMetrics::new("test")),
        0,
        vec![
            SortedRun::new(0, vec![file1]),
            SortedRun::new(1, vec![file2]),
        ],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::Parquet,
        Arc::new(crate::ttl::TTLProvider::disabled()),
        schema_manager_for(num_columns),
    );
    let executor = CompactionExecutor::new(options, Arc::new(DbLifecycle::new_open())).unwrap();
    let result = executor.execute_blocking(task, None).unwrap();
    let output = result.new_files()[0].clone();
    let reader = file_manager.open_data_file_reader(output.file_id).unwrap();
    let mut iter =
        ParquetIterator::from_data_file(Box::new(reader), output.as_ref(), None).unwrap();
    iter.seek_to_first().unwrap();
    let mut rows = Vec::new();
    while iter.valid() {
        rows.push((
            iter.key().unwrap().unwrap().to_vec(),
            iter.value().unwrap().unwrap().to_vec(),
        ));
        iter.next().unwrap();
    }
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, b"a".to_vec());
    assert_eq!(rows[1].0, b"b".to_vec());
    assert_eq!(rows[2].0, b"c".to_vec());
    cleanup_test_dir(test_dir);
}

#[test]
#[serial_test::serial(file)]
fn test_compaction_input_parquet_output_parquet_records_hot_row_group_preloads() {
    let test_dir = "/tmp/compaction_parquet_hot_preload_test";
    cleanup_test_dir(test_dir);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", test_dir))
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("compaction-hot-parquet-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
    );
    let num_columns = 1;
    let input = create_test_parquet(
        &file_manager,
        vec![
            (b"a", &make_value_bytes(b"va", num_columns)),
            (b"b", &make_value_bytes(b"vb", num_columns)),
            (b"c", &make_value_bytes(b"vc", num_columns)),
        ],
    )
    .unwrap();
    let options = CompactionConfig {
        num_columns,
        target_file_size: 1024 * 1024,
        ..Default::default()
    };
    let factory = crate::compaction::make_data_file_builder_factory(WriterOptions::Parquet(
        ParquetWriterOptions {
            row_group_size_bytes: 16,
            buffer_size: options.buffer_size,
            num_columns,
        },
    ));
    let writer_options = WriterOptions::Parquet(ParquetWriterOptions {
        row_group_size_bytes: 16,
        buffer_size: options.buffer_size,
        num_columns,
    });
    let hot_cache_namespace = 1234u64;
    let scan_hot_blocks = ScanHotBlockRegistry::from_keys(parquet_hot_keys_for_first_row_group(
        &file_manager,
        input.as_ref(),
        hot_cache_namespace,
        num_columns,
    ));
    let task = CompactionTask::new(
        Arc::new(CompactionTaskMetrics::new("test")),
        Arc::new(crate::sst::SSTIteratorMetrics::new("test")),
        0,
        vec![SortedRun::new(0, vec![input])],
        1,
        Arc::clone(&file_manager),
        factory,
        DataFileType::Parquet,
        Arc::new(crate::ttl::TTLProvider::disabled()),
        schema_manager_for(num_columns),
    )
    .with_writer_options_factory(WriterOptionsFactory::from(&writer_options))
    .with_scan_hot_blocks(hot_cache_namespace, scan_hot_blocks);
    let executor = CompactionExecutor::new(options, Arc::new(DbLifecycle::new_open())).unwrap();
    let result = executor.execute_blocking(task, None).unwrap();
    assert!(!result.preload_block_keys().is_empty());
    assert!(result.preload_block_keys().iter().all(|preload| matches!(
        preload.key.kind,
        crate::cache::BlockCacheKind::ParquetData(_)
    )));
    cleanup_test_dir(test_dir);
}
