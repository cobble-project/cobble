use super::*;
use crate::VolumeDescriptor;
use crate::data_file::DataFile;
use crate::db_state::{DbState, DbStateHandle, LSMTreeScope, MultiLSMTreeVersion};
use crate::format::FileBuildResult;
use crate::iterator::SortedRun;
use crate::lsm::{LSMTree, LSMTreeVersion, Level};
use crate::metrics_manager::MetricsManager;
use crate::sst::row_codec::{decode_value, encode_key, encode_value};
use crate::sst::{SSTIterator, SSTIteratorOptions, SSTWriter, SSTWriterOptions};
use crate::r#type::{Column, Key, Value, ValueType};
use crate::vlog::VlogVersion;
use serial_test::serial;
use size::Size;
use std::collections::VecDeque;
use std::sync::Arc;

fn cleanup_test_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn make_value_bytes(data: &[u8], num_columns: usize) -> Vec<u8> {
    let value = Value::new(vec![Some(Column::new(ValueType::Put, data.to_vec()))]);
    encode_value(&value, num_columns).to_vec()
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
    file_manager: &Arc<crate::file::FileManager>,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    options: SSTWriterOptions,
    schema_id: u64,
) -> Result<Arc<DataFile>> {
    let (file_id, writer_file) = file_manager.create_data_file_with_offload()?;
    let mut writer = SSTWriter::new(writer_file, options);
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
        crate::data_file::DataFileType::SSTable,
        first_key,
        last_key,
        file_id,
        crate::file::TrackedFileId::new(file_manager, file_id),
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

#[test]
#[serial(file)]
fn test_local_compaction_worker_uses_tree_scope_column_family_width() {
    let root = "/tmp/local_compaction_worker_cf_width";
    cleanup_test_root(root);

    let column_family_id = 1;
    let num_columns = 1;
    let config = crate::Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        base_file_size: Size::from_const(128),
        sst_bloom_filter_enabled: true,
        compaction_threads: 2,
        num_columns: 2,
        ..crate::Config::default()
    };
    let db_id = "local-compaction-worker-cf-width".to_string();
    let metrics_manager = Arc::new(MetricsManager::new(&db_id));
    let file_manager = Arc::new(
        crate::file::FileManager::from_config(&config, &db_id, Arc::clone(&metrics_manager))
            .unwrap(),
    );

    let mut sst_options = build_sst_writer_options(&config, 0, num_columns);
    sst_options.metrics = Some(metrics_manager.sst_writer_metrics(sst_options.compression));

    let schema_manager = Arc::new(SchemaManager::new(config.num_columns));
    let mut schema_builder = schema_manager.builder();
    schema_builder
        .add_column(0, None, None, Some("metrics".to_string()))
        .unwrap();
    let target_schema = schema_builder.commit();

    let expected_key = make_test_key_in_family(column_family_id, b"k1");
    let source_file = create_test_sst(
        &file_manager,
        vec![(expected_key.clone(), make_value_bytes(b"v1", num_columns))],
        sst_options,
        target_schema.version(),
    )
    .unwrap();

    let lsm_version = LSMTreeVersion {
        levels: vec![
            Level {
                ordinal: 0,
                tiered: true,
                files: vec![Arc::clone(&source_file)],
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
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });

    let db_lifecycle = Arc::new(DbLifecycle::new_open());
    let lsm_tree = Arc::new(LSMTree::with_state_and_ttl(
        Arc::clone(&db_state),
        Arc::new(crate::ttl::TTLProvider::disabled()),
        Arc::clone(&db_lifecycle),
        Arc::clone(&metrics_manager),
    ));

    let executor = CompactionExecutor::new(
        build_compaction_config(&config, num_columns).unwrap(),
        Arc::clone(&db_lifecycle),
    )
    .unwrap();
    let worker = LocalCompactionWorker::new(
        executor,
        Arc::clone(&file_manager),
        Arc::downgrade(&lsm_tree),
        config.clone(),
        Arc::clone(&db_lifecycle),
        Arc::clone(&metrics_manager),
        Arc::clone(&schema_manager),
    );

    lsm_tree.on_compaction_started(0);
    let handle = worker
        .submit_runs(
            0,
            vec![SortedRun::new(0, vec![source_file])],
            1,
            schema_manager.latest_schema().version(),
            VlogVersion::new(),
            crate::data_file::DataFileType::SSTable,
            Arc::new(crate::ttl::TTLProvider::disabled()),
        )
        .expect("compaction handle");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let result = runtime.block_on(handle).unwrap().unwrap();
    assert_eq!(result.new_files().len(), 1);

    let reader = file_manager
        .open_data_file_reader(result.new_files()[0].file_id)
        .unwrap();
    let mut iter = SSTIterator::with_cache_and_file(
        Box::new(reader),
        result.new_files()[0].as_ref(),
        SSTIteratorOptions {
            num_columns,
            bloom_filter_enabled: true,
            ..SSTIteratorOptions::default()
        },
        None,
    )
    .unwrap();
    iter.seek_to_first().unwrap();
    let (key, mut value) = iter.current().unwrap().unwrap();
    assert_eq!(key.as_ref(), expected_key.as_slice());
    let decoded = decode_value(&mut value, num_columns).unwrap();
    assert_eq!(
        decoded.columns()[0].as_ref().unwrap().data().as_ref(),
        b"v1"
    );

    cleanup_test_root(root);
}
