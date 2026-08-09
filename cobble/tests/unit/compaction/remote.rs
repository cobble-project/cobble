use super::*;
use crate::cache::{
    BlockCache, BlockCacheKind, CachedBlock, MockCache, bucket_scoped_cache_namespace,
};
use crate::compaction::{build_sst_writer_options, make_data_file_builder_factory};
use crate::db_state::{DbState, DbStateHandle, LSMTreeScope, MultiLSMTreeVersion};
use crate::file::{File, RandomAccessFile, SequentialWriteFile};
use crate::format::FileBuildResult;
use crate::lsm::{LSMTree, LSMTreeVersion, Level};
use crate::parquet::{ParquetIterator, RandomAccessChunkReader, parquet_row_group_cache_keys};
use crate::sst::row_codec::{decode_value, encode_key, encode_value};
use crate::r#type::{Column, Key, KvValue, Value, ValueType};
use crate::writer_options::WriterOptions;
use crate::{VolumeDescriptor, VolumeUsageKind};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use serial_test::serial;
use size::Size;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

fn cleanup_test_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn test_file_manager(
    config: &Config,
    db_id: &str,
    metrics_manager: &Arc<MetricsManager>,
) -> Arc<FileManager> {
    let file_manager =
        Arc::new(FileManager::from_config(config, db_id, Arc::clone(metrics_manager)).unwrap());
    crate::properties::persist_db_properties(file_manager.as_ref(), db_id, config).unwrap();
    file_manager
}

#[test]
fn remote_file_manager_uses_writer_property_volumes() {
    let meta_dir = tempfile::tempdir().unwrap();
    let writer_data_dir = tempfile::tempdir().unwrap();
    let server_data_dir = tempfile::tempdir().unwrap();
    let db_id = "remote-properties-volume";
    let writer_config = Config {
        volumes: vec![
            VolumeDescriptor::new(
                format!("file://{}", meta_dir.path().display()),
                vec![VolumeUsageKind::Meta],
            ),
            VolumeDescriptor::new(
                format!("file://{}", writer_data_dir.path().display()),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
        ],
        ..Config::default()
    };
    let metrics = Arc::new(MetricsManager::new(db_id));
    let writer_file_manager =
        FileManager::from_config(&writer_config, db_id, Arc::clone(&metrics)).unwrap();
    crate::properties::persist_db_properties(&writer_file_manager, db_id, &writer_config).unwrap();

    let server_config = Config {
        volumes: vec![
            VolumeDescriptor::new(
                format!("file://{}", meta_dir.path().display()),
                vec![VolumeUsageKind::Meta],
            ),
            VolumeDescriptor::new(
                format!("file://{}", server_data_dir.path().display()),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
        ],
        ..Config::default()
    };
    let remote_file_manager =
        RemoteCompactionServer::file_manager_for_with(&server_config, db_id, &metrics).unwrap();
    let (file_id, mut writer) = remote_file_manager.create_data_file().unwrap();
    writer.write(b"property-selected").unwrap();
    writer.close().unwrap();
    let output_path = remote_file_manager
        .get_data_file_full_path(file_id)
        .unwrap();

    assert!(output_path.starts_with(&format!("file://{}", writer_data_dir.path().display())));
    assert!(!output_path.starts_with(&format!("file://{}", server_data_dir.path().display())));
}

fn make_typed_value_bytes(value_type: ValueType, data: &[u8], num_columns: usize) -> Vec<u8> {
    let value = Value::new(vec![Some(Column::new(value_type, data.to_vec()))]);
    encode_value(&value, num_columns).to_vec()
}

fn make_test_key(raw_key: &[u8]) -> Vec<u8> {
    encode_key(&Key::new(0, raw_key.to_vec())).to_vec()
}

fn make_test_key_in_family(column_family_id: u8, raw_key: &[u8]) -> Vec<u8> {
    encode_key(&Key::new_with_column_family(
        0,
        column_family_id,
        raw_key.to_vec(),
    ))
    .to_vec()
}

fn create_test_sst(
    file_manager: &Arc<FileManager>,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    options: SSTWriterOptions,
) -> Result<Arc<DataFile>> {
    create_test_sst_with_schema(file_manager, entries, options, 0)
}

fn create_test_sst_with_schema(
    file_manager: &Arc<FileManager>,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    options: SSTWriterOptions,
    schema_id: u64,
) -> Result<Arc<DataFile>> {
    let (file_id, writer_file) = file_manager.create_data_file_with_offload()?;
    let mut writer = crate::sst::SSTWriter::new(writer_file, options);

    for (key, value) in entries {
        writer.add(&key, &value)?;
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
        schema_id,
        file_size,
        bucket_range.clone(),
        bucket_range,
    );
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
    entries: Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<Arc<DataFile>> {
    let (file_id, writer_file) = file_manager.create_data_file_with_offload()?;
    let factory = make_data_file_builder_factory(WriterOptions::Parquet(ParquetWriterOptions {
        row_group_size_bytes: 256 * 1024,
        buffer_size: 8192,
        num_columns: 1,
    }));
    let mut writer = factory(Box::new(writer_file));
    for (key, value) in entries {
        writer.add(&key, &KvValue::Encoded(Bytes::from(value)))?;
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
) -> Vec<BlockCacheKey> {
    let reader = file_manager.open_data_file_reader(file.file_id).unwrap();
    let reader: Arc<dyn RandomAccessFile> = Arc::new(reader);
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
#[serial(file)]
fn test_remote_compaction_roundtrip_multiple_files() {
    crate::metrics_registry::init_metrics();
    let root = "/tmp/remote_compaction_roundtrip";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        base_file_size: Size::from_const(128),
        sst_bloom_filter_enabled: true,
        compaction_threads: 2,
        ..Config::default()
    };
    let db_id = "remote-compaction-roundtrip".to_string();
    let metrics_manager = Arc::new(MetricsManager::new(&db_id));
    let file_manager = test_file_manager(&config, &db_id, &metrics_manager);
    let mut sst_options = build_sst_writer_options(&config, 0, config.num_columns);
    sst_options.metrics = Some(metrics_manager.sst_writer_metrics(sst_options.compression));
    let value_payload = vec![b'x'; 128];
    let num_columns = sst_options.num_columns;

    let entries_a = (0..40)
        .map(|idx| {
            let key = format!("a{:03}", idx).into_bytes();
            let value = make_typed_value_bytes(ValueType::Put, &value_payload, num_columns);
            (key, value)
        })
        .collect::<Vec<_>>();
    let entries_b = (0..40)
        .map(|idx| {
            let key = format!("b{:03}", idx).into_bytes();
            let value = make_typed_value_bytes(ValueType::Put, &value_payload, num_columns);
            (key, value)
        })
        .collect::<Vec<_>>();

    let file_a = create_test_sst(&file_manager, entries_a, sst_options.clone()).unwrap();
    let file_b = create_test_sst(&file_manager, entries_b, sst_options.clone()).unwrap();
    let expected_input_bytes = (file_a.size as u64).saturating_add(file_b.size as u64);

    let lsm_version = LSMTreeVersion {
        levels: vec![
            Level {
                ordinal: 0,
                tiered: true,
                files: vec![Arc::clone(&file_a), Arc::clone(&file_b)],
            },
            Level {
                ordinal: 1,
                tiered: false,
                files: Vec::new(),
            },
        ],
    };
    let db_state = Arc::new(DbStateHandle::new());
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(lsm_version),
        vlog_version: crate::vlog::VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let mut lsm_tree = LSMTree::with_state(Arc::clone(&db_state), Arc::clone(&metrics_manager));
    let block_cache: BlockCache = Arc::new(MockCache::<BlockCacheKey, CachedBlock>::default());
    lsm_tree.set_block_cache(Some(block_cache));
    let lsm_tree = Arc::new(lsm_tree);

    let remote_timeout = Duration::from_millis(config.compaction_remote_timeout_ms);
    let server = Arc::new(RemoteCompactionServer::new(config.clone()).unwrap());
    let server_metrics_db_id = server.metrics_manager.db_id().to_string();
    assert_ne!(server_metrics_db_id, db_id);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_thread = {
        let server = Arc::clone(&server);
        std::thread::spawn(move || {
            for _ in 0..2 {
                if let Ok((stream, _)) = listener.accept() {
                    server.handle_connection(stream).unwrap();
                }
            }
        })
    };
    let schema_manager = Arc::new(SchemaManager::new(config.num_columns));

    let worker = RemoteCompactionWorker::new(
        addr.to_string(),
        Arc::clone(&file_manager),
        Arc::downgrade(&lsm_tree),
        config.clone(),
        TtlConfig {
            enabled: false,
            default_ttl_seconds: None,
        },
        remote_timeout,
        Arc::clone(&metrics_manager),
        Arc::clone(&schema_manager),
    )
    .unwrap();

    let runs = vec![
        SortedRun::new(0, vec![file_a]),
        SortedRun::new(0, vec![file_b]),
    ];
    lsm_tree.on_compaction_started(0);
    let handle = worker
        .submit_runs(
            0,
            runs,
            1,
            DataFileType::SSTable,
            Arc::new(TTLProvider::disabled()),
        )
        .expect("compaction handle");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let result = runtime.block_on(handle).unwrap().unwrap();
    let _ = server_thread.join();
    let expected_output_bytes = result
        .new_files()
        .iter()
        .fold(0u64, |total, file| total.saturating_add(file.size as u64));

    let level0 = lsm_tree.level_files(0);
    let level1 = lsm_tree.level_files(1);
    assert!(level0.is_empty());
    assert!(level1.len() > 1);
    assert!(
        level1
            .iter()
            .all(|file| file.file_id < REMOTE_FILE_ID_START)
    );
    assert!(
        level1
            .iter()
            .all(|file| file.file_type == DataFileType::SSTable)
    );
    assert_eq!(metric_counter(&db_id, "compactions_total"), 1);
    assert_eq!(
        metric_counter(&db_id, "compaction_read_bytes_total"),
        expected_input_bytes
    );
    assert_eq!(
        metric_counter(&db_id, "compaction_write_bytes_total"),
        expected_output_bytes
    );

    cleanup_test_root(root);
}

fn metric_counter(db_id: &str, name: &str) -> u64 {
    crate::metrics_registry::snapshot_metrics(Some(db_id))
        .into_iter()
        .find(|sample| sample.name == name)
        .and_then(|sample| match sample.value {
            crate::MetricValue::Counter(value) => Some(value),
            _ => None,
        })
        .unwrap_or_default()
}

#[test]
#[serial(file)]
fn test_remote_compaction_loads_writer_schema_from_request() {
    // Reproduces the Flink state-backend scenario: the writer registers several column
    // families, so each registration bumps the schema version. The resulting SST files are
    // stamped with a non-zero schema id. The writer carries the schema definitions in the
    // compaction request itself, and the remote compactor registers them from the request —
    // never reading the shared volume — so it can decode the input files and stamp output with
    // the highest input schema version (here equal to the writer's latest). This holds even
    // when no schema has been persisted to the volume (e.g. compaction triggered by a flush
    // before the first checkpoint).
    let root = "/tmp/remote_compaction_schema_request";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        base_file_size: Size::from_const(128),
        sst_bloom_filter_enabled: true,
        compaction_threads: 2,
        ..Config::default()
    };
    let db_id = "remote-compaction-schema-request".to_string();
    let metrics_manager = Arc::new(MetricsManager::new(&db_id));
    let file_manager = test_file_manager(&config, &db_id, &metrics_manager);

    // Evolve the schema to version 2, mimicking a writer that registers two column families.
    // Each commit bumps the schema version, exactly like the Flink state backend does when it
    // calls updateSchema().commit() for a new state or timer queue.
    let schema_manager = Arc::new(SchemaManager::new(config.num_columns));
    {
        let mut builder = schema_manager.builder();
        builder.ensure_column_family_exists("first-family").unwrap();
        builder.commit();
    }
    {
        let mut builder = schema_manager.builder();
        builder
            .ensure_column_family_exists("second-family")
            .unwrap();
        builder.commit();
    }
    let latest_schema = schema_manager.latest_schema();
    let latest_schema_id = latest_schema.version();
    assert_eq!(
        latest_schema_id, 2,
        "test setup: schema should have evolved to version 2"
    );

    // Intentionally do NOT persist schemas to the volume: the compactor must get them from the
    // request. Asserting the volume stays empty after compaction proves the protocol path.
    assert!(
        file_manager
            .open_metadata_file_reader_untracked(&crate::paths::schema_file_relative_path(
                latest_schema_id
            ))
            .is_err(),
        "test setup: schema file must not exist on the volume"
    );

    // Build SST files stamped with the latest schema id, as a flush would.
    let mut sst_options = build_sst_writer_options(&config, 0, config.num_columns);
    sst_options.metrics = Some(metrics_manager.sst_writer_metrics(sst_options.compression));
    let num_columns = sst_options.num_columns;
    let value_payload = vec![b'x'; 128];
    let entries_a = (0..40)
        .map(|idx| {
            let key = format!("a{:03}", idx).into_bytes();
            let value = make_typed_value_bytes(ValueType::Put, &value_payload, num_columns);
            (key, value)
        })
        .collect::<Vec<_>>();
    let entries_b = (0..40)
        .map(|idx| {
            let key = format!("b{:03}", idx).into_bytes();
            let value = make_typed_value_bytes(ValueType::Put, &value_payload, num_columns);
            (key, value)
        })
        .collect::<Vec<_>>();
    let file_a = create_test_sst_with_schema(
        &file_manager,
        entries_a,
        sst_options.clone(),
        latest_schema_id,
    )
    .unwrap();
    let file_b = create_test_sst_with_schema(
        &file_manager,
        entries_b,
        sst_options.clone(),
        latest_schema_id,
    )
    .unwrap();
    assert_eq!(file_a.schema_id, latest_schema_id);
    assert_eq!(file_b.schema_id, latest_schema_id);

    let lsm_version = LSMTreeVersion {
        levels: vec![
            Level {
                ordinal: 0,
                tiered: true,
                files: vec![Arc::clone(&file_a), Arc::clone(&file_b)],
            },
            Level {
                ordinal: 1,
                tiered: false,
                files: Vec::new(),
            },
        ],
    };
    let db_state = Arc::new(DbStateHandle::new());
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(lsm_version),
        vlog_version: crate::vlog::VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let mut lsm_tree = LSMTree::with_state(Arc::clone(&db_state), Arc::clone(&metrics_manager));
    let block_cache: BlockCache = Arc::new(MockCache::<BlockCacheKey, CachedBlock>::default());
    lsm_tree.set_block_cache(Some(block_cache));
    let lsm_tree = Arc::new(lsm_tree);

    let remote_timeout = Duration::from_millis(config.compaction_remote_timeout_ms);
    let server = Arc::new(RemoteCompactionServer::new(config.clone()).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_thread = {
        let server = Arc::clone(&server);
        std::thread::spawn(move || {
            for _ in 0..2 {
                if let Ok((stream, _)) = listener.accept() {
                    server.handle_connection(stream).unwrap();
                }
            }
        })
    };

    let worker = RemoteCompactionWorker::new(
        addr.to_string(),
        Arc::clone(&file_manager),
        Arc::downgrade(&lsm_tree),
        config.clone(),
        TtlConfig {
            enabled: false,
            default_ttl_seconds: None,
        },
        remote_timeout,
        Arc::clone(&metrics_manager),
        Arc::clone(&schema_manager),
    )
    .unwrap();

    let runs = vec![
        SortedRun::new(0, vec![file_a]),
        SortedRun::new(0, vec![file_b]),
    ];
    lsm_tree.on_compaction_started(0);
    let handle = worker
        .submit_runs(
            0,
            runs,
            1,
            DataFileType::SSTable,
            Arc::new(TTLProvider::disabled()),
        )
        .expect("compaction handle");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    // Before the fix, this failed with "Missing schema version 2".
    let result = runtime.block_on(handle).unwrap().unwrap();
    let _ = server_thread.join();

    // Compaction succeeded and output files are stamped with the highest input schema id
    // (here the writer's latest, since all input files share it).
    let level1 = lsm_tree.level_files(1);
    assert!(level1.len() > 1, "compaction should produce level-1 files");
    assert!(
        level1.iter().all(|file| file.schema_id == latest_schema_id),
        "output files must be stamped with schema id {}, got {:?}",
        latest_schema_id,
        level1.iter().map(|f| f.schema_id).collect::<Vec<_>>()
    );
    let _ = result;

    // The volume never held a schema file — the compactor reconstructed the schema registry
    // purely from the request, proving the protocol-carrying path.
    assert!(
        file_manager
            .open_metadata_file_reader_untracked(&crate::paths::schema_file_relative_path(
                latest_schema_id
            ))
            .is_err(),
        "schema file must not have been written to the volume"
    );

    cleanup_test_root(root);
}

#[test]
fn test_build_schema_manager_fails_on_malformed_carried_schema() {
    // A malformed carried schema must fail the request immediately rather than silently falling
    // back to the version-0 schema. A silent fallback would only surface later inside the
    // compaction executor as an opaque "Missing schema version N" error.
    let malformed = crate::schema::SchemaFile {
        // Unsupported format version forces schema_from_file to error during registration.
        format_version: u32::MAX,
        id: 7,
        column_families: Vec::new(),
    };
    let result =
        RemoteCompactionServer::build_schema_manager(0, &[], Vec::new(), None, 1, &[malformed]);
    let err = match result {
        Ok(_) => panic!("malformed schema must fail the request"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("schema version 7"),
        "error should name the offending schema id, got: {err}"
    );
}

#[test]
fn test_remote_compaction_protocol_rejects_mismatched_versions() {
    // The schemas field requires protocol v3, and the min-compatible version was raised to 3
    // too, so v2 and v3 are mutually incompatible in both directions. There is no safe rolling
    // order: a v3 writer talking to a v2 compactor, and a v2 writer talking to a v3 compactor,
    // must both be rejected at the handshake (rather than silently ignoring the schemas field
    // and failing later with "Missing schema version N").
    //
    // validate_protocol_compatibility(role, peer_version, peer_min, local_version, local_min)
    // is a pure function, so we exercise the full version matrix directly.

    // v3 peer against v3 local: accepted (the matched-pair steady state).
    assert!(
        validate_protocol_compatibility("request", 3, 3, 3, 3).is_ok(),
        "a v3 peer against a v3 local must be accepted"
    );

    // v2 writer (peer v2, min 2) against a v3 compactor (local v3, min 3): rejected,
    // because peer_version(2) < local_min(3).
    assert!(
        validate_protocol_compatibility("request", 2, 2, 3, 3).is_err(),
        "a v2 writer must be rejected by a v3 compactor"
    );

    // v3 writer (peer v3, min 3) against a v2 compactor (local v2, min 2): rejected,
    // because peer_min(3) > local_version(2).
    assert!(
        validate_protocol_compatibility("request", 3, 3, 2, 2).is_err(),
        "a v3 writer must be rejected by a v2 compactor"
    );

    // v2 peer against v2 local: this is the pre-schemas behavior. It is accepted by the
    // compatibility rule itself, but a v2 endpoint is not produced by the current code
    // (CURRENT and MIN_COMPATIBLE are both 3). This case is documented here for completeness
    // and is not part of the v3 guarantee.
    assert!(
        validate_protocol_compatibility("request", 2, 2, 2, 2).is_ok(),
        "a v2 peer against a v2 local is compatible by the rule (legacy behavior)"
    );
}

#[test]
fn test_remote_sst_options_default_missing_checksum_field_to_disabled() {
    let sst: RemoteSstOptions = serde_json::from_value(serde_json::json!({
        "block_size": 4096,
        "buffer_size": 8192,
        "num_columns": 1,
        "bloom_filter_enabled": false,
        "bloom_bits_per_key": 10,
        "partitioned_index": false,
        "data_block_restart_interval": 16,
        "compression": "none"
    }))
    .unwrap();
    assert!(!sst.block_checksum_enabled);
    assert_eq!(
        sst.read_metadata_cache_mode,
        SstReadMetadataCacheMode::Eager
    );
}

#[test]
#[serial(file)]
fn test_remote_compaction_with_u64_counter_merge_operator_in_non_default_family() {
    let root = "/tmp/remote_compaction_u64_counter";
    cleanup_test_root(root);
    let column_family_id = 1;
    let num_columns = 1;
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        base_file_size: Size::from_const(128),
        sst_bloom_filter_enabled: true,
        compaction_threads: 2,
        num_columns: 2,
        ..Config::default()
    };
    let db_id = "remote-compaction-u64-counter".to_string();
    let metrics_manager = Arc::new(MetricsManager::new(&db_id));
    let file_manager = test_file_manager(&config, &db_id, &metrics_manager);
    let mut sst_options = build_sst_writer_options(&config, 0, num_columns);
    sst_options.metrics = Some(metrics_manager.sst_writer_metrics(sst_options.compression));

    let schema_manager = Arc::new(SchemaManager::new(config.num_columns));
    let mut schema_builder = schema_manager.builder();
    schema_builder
        .add_column(0, None, None, Some("metrics".to_string()))
        .unwrap();
    schema_builder
        .set_column_operator(
            Some("metrics".to_string()),
            0,
            Arc::new(U64CounterMergeOperator),
        )
        .unwrap();
    let _ = schema_builder.commit();

    let mut expected = HashMap::new();
    let entries_old = (0..20u64)
        .map(|idx| {
            let key = make_test_key_in_family(column_family_id, format!("k{:03}", idx).as_bytes());
            let base = idx + 1;
            let delta = 10u64;
            expected.insert(key.clone(), base + delta);
            (
                key,
                make_typed_value_bytes(ValueType::Put, &base.to_le_bytes(), num_columns),
            )
        })
        .collect::<Vec<_>>();
    let entries_new = (0..20u64)
        .map(|idx| {
            let key = make_test_key_in_family(column_family_id, format!("k{:03}", idx).as_bytes());
            let delta = 10u64;
            (
                key,
                make_typed_value_bytes(ValueType::Merge, &delta.to_le_bytes(), num_columns),
            )
        })
        .collect::<Vec<_>>();

    let older_file = create_test_sst(&file_manager, entries_old, sst_options.clone()).unwrap();
    let newer_file = create_test_sst(&file_manager, entries_new, sst_options.clone()).unwrap();

    let lsm_version = LSMTreeVersion {
        levels: vec![
            Level {
                ordinal: 0,
                tiered: true,
                files: vec![Arc::clone(&newer_file), Arc::clone(&older_file)],
            },
            Level {
                ordinal: 1,
                tiered: false,
                files: Vec::new(),
            },
        ],
    };
    let db_state = Arc::new(DbStateHandle::new());
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::from_scopes_with_tree_versions(
            1,
            &[LSMTreeScope::new(0u16..=0u16, column_family_id)],
            vec![Arc::new(lsm_version)],
        )
        .unwrap(),
        vlog_version: crate::vlog::VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let mut lsm_tree = LSMTree::with_state(Arc::clone(&db_state), Arc::clone(&metrics_manager));
    let block_cache: BlockCache = Arc::new(MockCache::<BlockCacheKey, CachedBlock>::default());
    lsm_tree.set_block_cache(Some(block_cache));
    let lsm_tree = Arc::new(lsm_tree);

    let remote_timeout = Duration::from_millis(config.compaction_remote_timeout_ms);
    let server = Arc::new(RemoteCompactionServer::new(config.clone()).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_thread = {
        let server = Arc::clone(&server);
        std::thread::spawn(move || {
            for _ in 0..2 {
                if let Ok((stream, _)) = listener.accept() {
                    server.handle_connection(stream).unwrap();
                }
            }
        })
    };

    let worker = RemoteCompactionWorker::new(
        addr.to_string(),
        Arc::clone(&file_manager),
        Arc::downgrade(&lsm_tree),
        config.clone(),
        TtlConfig {
            enabled: false,
            default_ttl_seconds: None,
        },
        remote_timeout,
        Arc::clone(&metrics_manager),
        Arc::clone(&schema_manager),
    )
    .unwrap();

    let runs = vec![
        SortedRun::new(0, vec![newer_file]),
        SortedRun::new(0, vec![older_file]),
    ];
    lsm_tree.on_compaction_started(0);
    let handle = worker
        .submit_runs(
            0,
            runs,
            1,
            DataFileType::SSTable,
            Arc::new(TTLProvider::disabled()),
        )
        .expect("compaction handle");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(handle).unwrap().unwrap();
    let _ = server_thread.join();

    let mut actual = HashMap::new();
    for file in lsm_tree.level_files(1) {
        let reader = file_manager.open_data_file_reader(file.file_id).unwrap();
        let mut iter = crate::sst::SSTIterator::with_cache_and_file(
            Box::new(reader),
            file.as_ref(),
            crate::sst::SSTIteratorOptions {
                num_columns,
                bloom_filter_enabled: true,
                ..Default::default()
            },
            None,
        )
        .unwrap();
        iter.seek_to_first().unwrap();
        while iter.valid() {
            let (key, mut value) = iter.current().unwrap().unwrap();
            let decoded = decode_value(&mut value, num_columns).unwrap();
            let bytes = decoded.columns()[0].as_ref().unwrap().data();
            let merged = u64::from_le_bytes(bytes.as_ref().try_into().unwrap());
            actual.insert(key.to_vec(), merged);
            iter.next().unwrap();
        }
    }
    assert_eq!(actual, expected);
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_remote_compaction_roundtrip_parquet_output() {
    let root = "/tmp/remote_compaction_roundtrip_parquet";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        base_file_size: Size::from_const(128),
        sst_bloom_filter_enabled: true,
        compaction_threads: 2,
        ..Config::default()
    };
    let db_id = "remote-compaction-roundtrip-parquet".to_string();
    let metrics_manager = Arc::new(MetricsManager::new(&db_id));
    let file_manager = test_file_manager(&config, &db_id, &metrics_manager);
    let num_columns = config.num_columns;
    let entries_a = (0..40)
        .map(|idx| {
            let key = format!("a{:03}", idx).into_bytes();
            let value = make_typed_value_bytes(ValueType::Put, b"va", num_columns);
            (key, value)
        })
        .collect::<Vec<_>>();
    let entries_b = (0..40)
        .map(|idx| {
            let key = format!("b{:03}", idx).into_bytes();
            let value = make_typed_value_bytes(ValueType::Put, b"vb", num_columns);
            (key, value)
        })
        .collect::<Vec<_>>();
    let file_a = create_test_parquet(&file_manager, entries_a).unwrap();
    let file_b = create_test_parquet(&file_manager, entries_b).unwrap();

    let lsm_version = LSMTreeVersion {
        levels: vec![
            Level {
                ordinal: 0,
                tiered: true,
                files: vec![Arc::clone(&file_a), Arc::clone(&file_b)],
            },
            Level {
                ordinal: 1,
                tiered: false,
                files: Vec::new(),
            },
        ],
    };
    let db_state = Arc::new(DbStateHandle::new());
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(lsm_version),
        vlog_version: crate::vlog::VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let mut lsm_tree = LSMTree::with_state(Arc::clone(&db_state), Arc::clone(&metrics_manager));
    let block_cache: BlockCache = Arc::new(MockCache::<BlockCacheKey, CachedBlock>::default());
    lsm_tree.set_block_cache(Some(block_cache));
    let lsm_tree = Arc::new(lsm_tree);

    let remote_timeout = Duration::from_millis(config.compaction_remote_timeout_ms);
    let server = Arc::new(RemoteCompactionServer::new(config.clone()).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_thread = {
        let server = Arc::clone(&server);
        std::thread::spawn(move || {
            for _ in 0..2 {
                if let Ok((stream, _)) = listener.accept() {
                    server.handle_connection(stream).unwrap();
                }
            }
        })
    };
    let schema_manager = Arc::new(SchemaManager::new(config.num_columns));
    let worker = RemoteCompactionWorker::new(
        addr.to_string(),
        Arc::clone(&file_manager),
        Arc::downgrade(&lsm_tree),
        config.clone(),
        TtlConfig {
            enabled: false,
            default_ttl_seconds: None,
        },
        remote_timeout,
        Arc::clone(&metrics_manager),
        Arc::clone(&schema_manager),
    )
    .unwrap();

    let base_cache_namespace = lsm_tree.cache_namespace();
    let hot_cache_namespace = if file_a.effective_bucket_range.start()
        == file_a.effective_bucket_range.end()
    {
        bucket_scoped_cache_namespace(base_cache_namespace, *file_a.effective_bucket_range.start())
    } else {
        base_cache_namespace
    };
    let mut hot_handle = lsm_tree.scan_hot_blocks().handle();
    hot_handle.replace(parquet_hot_keys_for_first_row_group(
        &file_manager,
        file_a.as_ref(),
        hot_cache_namespace,
        num_columns,
    ));

    let runs = vec![
        SortedRun::new(0, vec![file_a]),
        SortedRun::new(0, vec![file_b]),
    ];
    lsm_tree.on_compaction_started(0);
    let handle = worker
        .submit_runs(
            0,
            runs,
            1,
            DataFileType::Parquet,
            Arc::new(TTLProvider::disabled()),
        )
        .expect("compaction handle");
    let runtime = Runtime::new().unwrap();
    let result = runtime.block_on(handle).unwrap().unwrap();
    let _ = server_thread.join();

    assert!(!result.preload_block_keys().is_empty());
    assert!(
        result
            .preload_block_keys()
            .iter()
            .all(|preload| matches!(preload.key.kind, BlockCacheKind::ParquetData(_)))
    );

    let level1 = lsm_tree.level_files(1);
    assert!(!level1.is_empty());
    assert!(
        level1
            .iter()
            .all(|file| file.file_type == DataFileType::Parquet)
    );
    for file in level1 {
        let reader = file_manager.open_data_file_reader(file.file_id).unwrap();
        let mut iter =
            ParquetIterator::from_data_file(Box::new(reader), file.as_ref(), None).unwrap();
        iter.seek_to_first().unwrap();
        assert!(iter.valid());
    }
    cleanup_test_root(root);
}

#[test]
fn test_request_limiter_basic() {
    let limiter = RequestLimiter::new(2, 1); // max 2 concurrent, 1 queued
    // Accept 3 requests (2 active + 1 queued)
    assert!(limiter.try_accept());
    assert!(limiter.try_accept());
    assert!(limiter.try_accept());
    // 4th should be rejected (over capacity)
    assert!(!limiter.try_accept());
}

#[test]
fn test_request_limiter_acquire_and_release() {
    let limiter = Arc::new(RequestLimiter::new(1, 1));
    // First request: accept and acquire
    assert!(limiter.try_accept());
    assert!(limiter.acquire_slot(None));
    // Second: accept (queued) and acquire blocks, so release first
    assert!(limiter.try_accept());
    // Third: rejected
    assert!(!limiter.try_accept());
    // Release first slot
    limiter.release_slot();
    // Now second can acquire
    assert!(limiter.acquire_slot(None));
    limiter.release_slot();
}

#[test]
fn test_request_limiter_shutdown_unblocks() {
    let limiter = Arc::new(RequestLimiter::new(1, 2));
    // Fill up the active slot
    assert!(limiter.try_accept());
    assert!(limiter.acquire_slot(None));
    // Queue a second request
    assert!(limiter.try_accept());
    let limiter2 = Arc::clone(&limiter);
    let handle = std::thread::spawn(move || limiter2.acquire_slot(None));
    std::thread::sleep(Duration::from_millis(50));
    // Shutdown should unblock the waiting thread
    limiter.shutdown();
    let acquired = handle.join().unwrap();
    assert!(!acquired, "should return false on shutdown");
    // After shutdown, try_accept should fail
    assert!(!limiter.try_accept());
}

#[test]
#[serial(file)]
fn test_server_rejects_when_overloaded() {
    let root = "/tmp/remote_compaction_overload";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        compaction_threads: 1,
        compaction_server_max_concurrent: 1,
        compaction_server_max_queued: 0,
        ..Config::default()
    };
    let server = Arc::new(RemoteCompactionServer::new(config).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();

    // Occupy the only active slot with a slow request that blocks the server
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let barrier_clone = barrier.clone();
    let server_clone = Arc::clone(&server);
    let accept_handle = std::thread::spawn(move || {
        // Accept connections in a loop
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false).ok();
                    let _ = server_clone.handle_connection(stream);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
            // After accepting first connection, signal
            barrier_clone.wait();
        }
    });

    // First connection — will occupy the active slot
    let slow_handle = std::thread::spawn({
        let addr_str = addr.to_string();
        move || {
            let mut stream = TcpStream::connect(&addr_str).expect("connect should succeed");
            stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
            stream.set_write_timeout(Some(Duration::from_secs(5))).ok();
            write_message(
                &mut stream,
                &RemoteCompactionCommand::SupportedMergeOperators,
            )
            .unwrap();
            // This will succeed because the server processes it
            let reply: Result<RemoteCompactionReply> = read_message(&mut stream);
            reply
        }
    });

    // Wait until first connection is accepted
    barrier.wait();
    std::thread::sleep(Duration::from_millis(200));

    // Second connection should be rejected (max_concurrent=1, max_queued=0)
    let mut stream2 = TcpStream::connect(addr.to_string()).unwrap();
    stream2.set_read_timeout(Some(Duration::from_secs(2))).ok();
    stream2.set_write_timeout(Some(Duration::from_secs(2))).ok();
    write_message(
        &mut stream2,
        &RemoteCompactionCommand::SupportedMergeOperators,
    )
    .unwrap();
    let reply: Result<RemoteCompactionReply> = read_message(&mut stream2);

    // First connection completes fine
    let first_reply = slow_handle.join().unwrap();
    assert!(
        matches!(
            first_reply,
            Ok(RemoteCompactionReply::SupportedMergeOperators(_))
        ),
        "first request should succeed"
    );

    // Second connection should get an overload error
    match reply {
        Ok(RemoteCompactionReply::Error(msg)) => {
            assert!(
                msg.contains("overloaded") || msg.contains("queue full"),
                "error should mention overload, got: {}",
                msg
            );
        }
        other => {
            // It's also acceptable if the connection was reset/closed
            // This can happen if the server rejects before reading the full message
            if let Ok(ref r) = other {
                panic!("expected error reply, got: {:?}", r);
            }
        }
    }

    drop(accept_handle);
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_server_close_detected_by_client() {
    let root = "/tmp/remote_compaction_close";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        compaction_threads: 1,
        ..Config::default()
    };
    let server = Arc::new(RemoteCompactionServer::new(config).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    // Server thread using serve()
    let server_clone = Arc::clone(&server);
    let addr_str = addr.to_string();
    let server_thread = std::thread::spawn(move || {
        let _ = server_clone.serve(&addr_str);
    });
    std::thread::sleep(Duration::from_millis(100));

    // Verify server works
    let ids =
        fetch_supported_merge_operator_ids(&addr.to_string(), Duration::from_secs(5)).unwrap();
    assert!(!ids.is_empty());

    // Close the server
    server.close();
    server_thread.join().unwrap();

    // After close, client should fail to connect
    let result = fetch_supported_merge_operator_ids(&addr.to_string(), Duration::from_secs(1));
    assert!(result.is_err(), "should fail after server close");

    cleanup_test_root(root);
}

#[test]
fn test_client_timeout_on_connect() {
    // Connect to a non-routable address - should timeout
    let result = send_command_to(
        "192.0.2.1:9999", // RFC 5737 TEST-NET, non-routable
        RemoteCompactionCommand::SupportedMergeOperators,
        Duration::from_millis(200),
    );
    assert!(result.is_err(), "should timeout on non-routable address");
}

#[test]
#[serial(file)]
fn test_remote_data_file_round_trips_max_expired_at() {
    let root = "/tmp/remote_data_file_ttl_roundtrip";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        ..Config::default()
    };
    let db_id = "remote-data-file-ttl-roundtrip".to_string();
    let metrics_manager = Arc::new(MetricsManager::new(&db_id));
    let file_manager = test_file_manager(&config, &db_id, &metrics_manager);

    // Build a DataFile with a non-zero max_expired_at.
    let (file_id, mut writer) = file_manager.create_data_file().unwrap();
    writer.write(b"placeholder").unwrap();
    writer.close().unwrap();
    let full_path = file_manager.get_data_file_full_path(file_id).unwrap();
    let start_key = b"a".to_vec();
    let end_key = b"z".to_vec();
    let bucket_range = DataFile::bucket_range_from_keys(&start_key, &end_key);
    let data_file = DataFile::new(
        DataFileType::SSTable,
        start_key,
        end_key,
        file_id,
        TrackedFileId::new(&file_manager, file_id),
        0,
        42,
        bucket_range.clone(),
        bucket_range,
    );
    data_file.set_max_expired_at(500);

    // from_data_file must capture max_expired_at.
    let remote = RemoteDataFile::from_data_file(&data_file, full_path.clone());
    assert_eq!(remote.max_expired_at, 500);

    // Serialize -> deserialize (simulates crossing the process boundary).
    let json = serde_json::to_string(&remote).unwrap();
    let remote: RemoteDataFile = serde_json::from_str(&json).unwrap();
    assert_eq!(remote.max_expired_at, 500);

    // into_data_file must restore max_expired_at.
    let restored = remote
        .into_data_file(&file_manager, file_id, RemoteReplicaUse::ReadonlyView)
        .unwrap();
    assert_eq!(restored.max_expired_at(), 500);

    cleanup_test_root(root);
}

#[test]
fn test_remote_data_file_backward_compatible_without_max_expired_at() {
    // A RemoteDataFile serialized without the max_expired_at field (as produced by
    // older binaries) must decode with the default value of 0.
    let json = serde_json::json!({
        "file_id": 1,
        "file_type": "SSTable",
        "full_path": "file:///tmp/legacy",
        "start_key": [],
        "end_key": [],
        "schema_id": 0,
        "size": 0,
        "has_separated_values": false,
        "bucket_range_start": 0,
        "bucket_range_end": 0,
        "effective_bucket_range_start": 0,
        "effective_bucket_range_end": 0,
        "vlog_file_seq_offset": 0,
        "meta_bytes": null,
    });
    let remote: RemoteDataFile = serde_json::from_value(json).unwrap();
    assert_eq!(remote.max_expired_at, 0);
}
