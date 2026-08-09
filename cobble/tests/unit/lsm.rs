use super::*;
use crate::data_file::DataFileType;
use crate::db_state::{DbState, DbStateHandle, MultiLSMTreeVersion};
use crate::file::{FileId, FileManager, FileSystemRegistry, TrackedFileId};
use crate::format::FileBuildResult;
use crate::sst::row_codec::{encode_key, encode_value};
use crate::sst::{SSTWriter, SSTWriterOptions};
use crate::r#type::{Column, Key, Value, ValueType};
use crate::vlog::{VlogEdit, VlogVersion};
use std::collections::VecDeque;
use std::sync::Mutex;

static mut FILE_ID_COUNTER: FileId = 0;

impl LSMTree {
    pub(crate) fn on_compaction_started(&self, tree_idx: usize) {
        let mut state = self.state.lock().unwrap();
        let snapshot = self.db_state.load();
        let expected_scope = snapshot.multi_lsm_version.tree_scope_of_tree(tree_idx);
        state.pending_compaction.insert(
            tree_idx,
            PendingCompaction {
                scope: expected_scope,
                topology_epoch: snapshot.topology_epoch,
            },
        );
    }
}

fn create_data_file(start: &[u8], end: &[u8]) -> Arc<DataFile> {
    unsafe {
        let id = FILE_ID_COUNTER;
        FILE_ID_COUNTER += 1;
        let bucket_range = DataFile::bucket_range_from_keys(start, end);
        Arc::new(DataFile::new_untracked(
            DataFileType::SSTable,
            start.to_vec(),
            end.to_vec(),
            id,
            0,
            0,
            bucket_range.clone(),
            bucket_range,
        ))
    }
}

#[test]
fn file_intersection_respects_inclusive_start_and_exclusive_end() {
    let first = create_data_file(b"a", b"c");
    let second = create_data_file(b"d", b"f");
    assert!(file_intersects_scan(&first, b"c", Some(b"d")));
    assert!(!file_intersects_scan(&second, b"a", Some(b"d")));
    assert!(file_intersects_scan(&first, b"b", Some(b"e")));
    assert!(file_intersects_scan(&second, b"b", Some(b"e")));
    assert!(file_intersects_scan(&second, b"e", None));
}

fn create_data_file_with_size(start: &[u8], end: &[u8], size: usize) -> Arc<DataFile> {
    unsafe {
        let id = FILE_ID_COUNTER;
        FILE_ID_COUNTER += 1;
        let bucket_range = DataFile::bucket_range_from_keys(start, end);
        Arc::new(DataFile::new_untracked(
            DataFileType::SSTable,
            start.to_vec(),
            end.to_vec(),
            id,
            0,
            size,
            bucket_range.clone(),
            bucket_range,
        ))
    }
}

fn create_data_file_with_bucket(bucket: u16, size: usize) -> Arc<DataFile> {
    let start_key = encode_key(&Key::new(bucket, b"a".to_vec())).to_vec();
    let end_key = encode_key(&Key::new(bucket, b"z".to_vec())).to_vec();
    create_data_file_with_size(start_key.as_slice(), end_key.as_slice(), size)
}

fn create_data_file_in_scope(
    start_bucket: u16,
    end_bucket: u16,
    column_family_id: u8,
    size: usize,
) -> Arc<DataFile> {
    let start_key = encode_key(&Key::new_with_column_family(
        start_bucket,
        column_family_id,
        b"a".to_vec(),
    ))
    .to_vec();
    let end_key = encode_key(&Key::new_with_column_family(
        end_bucket,
        column_family_id,
        b"z".to_vec(),
    ))
    .to_vec();
    create_data_file_with_size(start_key.as_slice(), end_key.as_slice(), size)
}

fn cleanup_test_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn create_test_sst(
    file_manager: &Arc<FileManager>,
    _seq: u64,
    entries: Vec<(&[u8], &[u8])>,
) -> Result<Arc<DataFile>> {
    create_test_sst_in_bucket(file_manager, 0, entries)
}

fn create_test_sst_in_bucket(
    file_manager: &Arc<FileManager>,
    bucket: u16,
    entries: Vec<(&[u8], &[u8])>,
) -> Result<Arc<DataFile>> {
    let (file_id, writer_file) = file_manager.create_data_file_with_offload()?;
    let mut writer = SSTWriter::new(
        writer_file,
        SSTWriterOptions {
            num_columns: 1,
            bloom_filter_enabled: true,
            bloom_bits_per_key: 10,
            partitioned_index: false,
            ..SSTWriterOptions::default()
        },
    );
    for (key, value) in entries {
        let encoded_key = encode_key(&Key::new(bucket, key.to_vec()));
        writer.add(encoded_key.as_ref(), value)?;
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
    );
    data_file.set_meta_bytes(meta_bytes);
    data_file.set_max_expired_at(max_expired_at);
    if let Some(metadata) = sst_read_metadata {
        data_file.set_sst_read_metadata(metadata);
    }
    Ok(Arc::new(data_file))
}

fn make_value_bytes(data: &[u8], num_columns: usize) -> Vec<u8> {
    let value = Value::new(vec![Some(Column::new(ValueType::Put, data.to_vec()))]);
    encode_value(&value, num_columns).to_vec()
}

fn batch_request_for_key(key: &[u8], stopped: bool) -> BatchGetRequest {
    BatchGetRequest {
        bucket: 0,
        encoded_key: Bytes::copy_from_slice(key),
        values: Vec::new(),
        terminal_mask: None,
        decode_mask: vec![0x01],
        stopped,
    }
}

#[test]
fn non_tiered_routing_skips_gaps_and_preserves_boundaries() {
    let files = vec![create_data_file(b"a", b"b"), create_data_file(b"d", b"e")];
    let mut requests = vec![
        batch_request_for_key(b"0", false),
        batch_request_for_key(b"a", false),
        batch_request_for_key(b"b", false),
        batch_request_for_key(b"c", false),
        batch_request_for_key(b"d", false),
        batch_request_for_key(b"e", false),
        batch_request_for_key(b"z", false),
    ];
    let mut indices = (0..requests.len()).collect::<Vec<_>>();
    assert_eq!(
        route_non_tiered_requests(&files, &indices, &requests),
        vec![vec![1, 2], vec![4, 5]]
    );

    requests[2].stopped = true;
    retain_active_request_indices(&mut indices, &requests);
    assert_eq!(
        route_non_tiered_requests(&files, &indices, &requests),
        vec![vec![1], vec![4, 5]]
    );
}

#[test]
fn non_tiered_routing_selects_binary_for_small_or_sparse_batches() {
    assert!(should_use_binary_non_tiered_routing(8, 1_000_000));
    assert!(should_use_binary_non_tiered_routing(9, 1_000_000));
    assert!(!should_use_binary_non_tiered_routing(9, 3));
}

#[test]
fn non_tiered_routing_rechecks_after_active_requests_shrink() {
    let mut requests = (b'a'..=b'i')
        .map(|key| batch_request_for_key(&[key], false))
        .collect::<Vec<_>>();
    let mut request_indices = (0..requests.len()).collect::<Vec<_>>();

    assert!(
        !should_use_binary_non_tiered_routing(request_indices.len(), 3),
        "the initial nine-key batch should use merge routing"
    );

    for request in requests.iter_mut().take(8) {
        request.stopped = true;
    }
    retain_active_request_indices(&mut request_indices, &requests);

    assert_eq!(request_indices, vec![8]);
    assert!(
        should_use_binary_non_tiered_routing(request_indices.len(), 3),
        "the next level must choose from its one active key"
    );
    let files = vec![
        create_data_file(b"a", b"b"),
        create_data_file(b"d", b"e"),
        create_data_file(b"h", b"i"),
    ];
    assert_eq!(
        route_non_tiered_requests_binary(&files, &request_indices, &requests),
        BTreeMap::from([(2, vec![8])])
    );
}

#[test]
fn multi_get_groups_shared_versions_by_tree_index() {
    let shared = Arc::new(LSMTreeVersion { levels: Vec::new() });
    let scopes = vec![
        LSMTreeScope::new(0u16..=0u16, DEFAULT_COLUMN_FAMILY_ID),
        LSMTreeScope::new(1u16..=1u16, DEFAULT_COLUMN_FAMILY_ID),
    ];
    let multi_lsm_version = MultiLSMTreeVersion::from_scopes_with_tree_versions(
        2,
        &scopes,
        vec![Arc::clone(&shared), Arc::clone(&shared)],
    )
    .unwrap();
    let snapshot = DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version,
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    };
    let requests = vec![
        BatchGetRequest {
            bucket: 0,
            ..batch_request_for_key(b"a", false)
        },
        BatchGetRequest {
            bucket: 1,
            ..batch_request_for_key(b"b", false)
        },
    ];

    assert_eq!(
        group_request_indices_by_tree(&snapshot, &requests, DEFAULT_COLUMN_FAMILY_ID),
        BTreeMap::from([(0, vec![0]), (1, vec![1])])
    );
}

fn empty_lsm_versions(len: usize) -> Vec<Arc<LSMTreeVersion>> {
    let mut v: Vec<Arc<LSMTreeVersion>> = Vec::with_capacity(len);
    (0..len).for_each(|_| v.push(Arc::new(LSMTreeVersion { levels: vec![] })));
    v
}

#[derive(Default)]
struct RecordingCompactionWorker {
    submitted_tree_idxs: Mutex<Vec<usize>>,
    submitted_data_file_types: Mutex<Vec<DataFileType>>,
    submitted_file_ids: Mutex<Vec<Vec<FileId>>>,
}

impl CompactionWorker for RecordingCompactionWorker {
    fn submit_runs(
        &self,
        lsm_tree_idx: usize,
        sorted_runs: Vec<SortedRun>,
        _output_level: u8,
        data_file_type: DataFileType,
        _ttl_provider: Arc<TTLProvider>,
    ) -> Option<tokio::task::JoinHandle<Result<crate::compaction::CompactionResult>>> {
        self.submitted_tree_idxs.lock().unwrap().push(lsm_tree_idx);
        self.submitted_file_ids.lock().unwrap().push(
            sorted_runs
                .iter()
                .flat_map(|run| run.files())
                .map(|file| file.file_id)
                .collect(),
        );
        self.submitted_data_file_types
            .lock()
            .unwrap()
            .push(data_file_type);
        None
    }

    fn shutdown(&self) {}
}

#[test]
fn test_lsm_tree_apply_edit() {
    let db_state = Arc::new(DbStateHandle::new());
    let lsm_version = LSMTreeVersion {
        levels: vec![
            Level {
                ordinal: 0,
                tiered: true,
                files: vec![create_data_file(b"a", b"b"), create_data_file(b"c", b"d")],
            },
            Level {
                ordinal: 1,
                tiered: false,
                files: vec![create_data_file(b"e", b"f"), create_data_file(b"g", b"h")],
            },
        ],
    };
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(lsm_version),
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);

    // Create a version edit to remove one file from level 0 and add two new files
    let current_version = db_state.load().multi_lsm_version.version_of_index(0);
    let edit = VersionEdit {
        level_edits: vec![
            LevelEdit {
                level: 0,
                removed_files: vec![current_version.as_ref().levels[0].files[0].clone()],
                new_files: vec![
                    create_data_file(b"a1", b"a2"),
                    create_data_file(b"b1", b"b2"),
                ],
            },
            LevelEdit {
                level: 1,
                removed_files: vec![],
                new_files: vec![create_data_file(b"d1", b"d2")],
            },
        ],
    };

    lsm_tree.apply_edit(0, edit, None);

    // Verify the new version
    let version = db_state.load().multi_lsm_version.version_of_index(0);
    assert_eq!(version.as_ref().levels.len(), 2);

    let level0 = &version.as_ref().levels[0];
    assert_eq!(level0.ordinal, 0);
    assert_eq!(level0.files.len(), 3);
    assert_eq!(level0.files[0].start_key, b"a1");
    assert_eq!(level0.files[1].start_key, b"b1");
    assert_eq!(level0.files[2].start_key, b"c");

    let level1 = &version.as_ref().levels[1];
    assert_eq!(level1.ordinal, 1);
    assert_eq!(level1.files.len(), 3);
    assert_eq!(level1.files[0].start_key, b"d1");
    assert_eq!(level1.files[1].start_key, b"e");
    assert_eq!(level1.files[2].start_key, b"g");
}

#[test]
#[serial_test::serial(file)]
fn test_lsm_edit_removes_data_file() {
    let root = "/tmp/lsm_edit_remove_file";
    cleanup_test_root(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs.clone(), Arc::clone(&metrics_manager)).unwrap());
    let num_columns = 1;
    let to_remove = create_test_sst(
        &file_manager,
        1,
        vec![(b"k1", &make_value_bytes(b"value", num_columns))],
    )
    .unwrap();
    let file_id = to_remove.file_id;
    let path = file_manager.get_data_file_path(file_id).unwrap();
    assert!(fs.exists(&path).unwrap());

    let db_state = Arc::new(DbStateHandle::new());
    let lsm_version = LSMTreeVersion {
        levels: vec![Level {
            ordinal: 0,
            tiered: true,
            files: vec![Arc::clone(&to_remove)],
        }],
    };
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(lsm_version),
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
    lsm_tree.apply_edit(
        0,
        VersionEdit {
            level_edits: vec![LevelEdit {
                level: 0,
                removed_files: vec![Arc::clone(&to_remove)],
                new_files: Vec::new(),
            }],
        },
        None,
    );
    assert!(lsm_tree.level_files(0).is_empty());
    drop(to_remove);

    crate::file::test_utils::wait_for_file_deletion(&fs, &path);
    for _ in 0..50 {
        if !fs.exists(&path).unwrap() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(!fs.exists(&path).unwrap());
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_lsm_trivial_move_compaction() {
    let root = "/tmp/lsm_trivial_move";
    cleanup_test_root(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("lsm-compaction-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let config = crate::compaction::CompactionConfig {
        l1_base_bytes: 1,
        level_size_multiplier: 1,
        max_level: 3,
        bloom_filter_enabled: true,
        bloom_bits_per_key: 10,
        partitioned_index: false,
        ..crate::compaction::CompactionConfig::default()
    };
    let db_config = crate::Config::default();
    let db_state = Arc::new(DbStateHandle::new());
    let lsm_version = LSMTreeVersion {
        levels: vec![
            Level {
                ordinal: 0,
                tiered: true,
                files: Vec::new(),
            },
            Level {
                ordinal: 1,
                tiered: false,
                files: vec![
                    create_data_file_with_size(b"a", b"b", 10),
                    create_data_file_with_size(b"c", b"d", 10),
                ],
            },
            Level {
                ordinal: 2,
                tiered: false,
                files: vec![create_data_file_with_size(b"e", b"f", 1)],
            },
        ],
    };
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(lsm_version),
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let lsm_tree = Arc::new(LSMTree::with_state(
        Arc::clone(&db_state),
        Arc::clone(&metrics_manager),
    ));
    let worker: Arc<dyn crate::compaction::CompactionWorker> =
        Arc::new(crate::compaction::LocalCompactionWorker::new(
            crate::compaction::CompactionExecutor::new(config, Arc::clone(&lsm_tree.db_lifecycle))
                .unwrap(),
            Arc::clone(&file_manager),
            Arc::downgrade(&lsm_tree),
            db_config,
            Arc::clone(&lsm_tree.db_lifecycle),
            Arc::clone(&metrics_manager),
            Arc::new(crate::schema::SchemaManager::new(1)),
        ));
    lsm_tree.configure_compaction(config, Some(Arc::clone(&worker)));
    let target = lsm_tree
        .db_state
        .load()
        .multi_lsm_version
        .version_of_index(0)
        .levels
        .iter()
        .find(|level| level.ordinal == 1)
        .and_then(|level| level.files.iter().find(|file| file.start_key == b"a"))
        .cloned()
        .expect("target file");
    lsm_tree.apply_edit(
        0,
        VersionEdit {
            level_edits: vec![LevelEdit {
                level: 1,
                removed_files: vec![target],
                new_files: Vec::new(),
            }],
        },
        None,
    );
    let level1 = lsm_tree.level_files(1);
    let level2 = lsm_tree.level_files(2);
    assert_eq!(level1.len(), 0);
    assert_eq!(level2.len(), 1);
    assert!(level2.iter().any(|file| file.start_key == b"e"));
    cleanup_test_root(root);
}

#[test]
fn test_lsm_compaction_drops_fully_truncated_file_without_worker_submit() {
    let db_state = Arc::new(DbStateHandle::new());
    let file = create_data_file_with_bucket(0, 10);
    let lsm_version = LSMTreeVersion {
        levels: vec![
            Level {
                ordinal: 0,
                tiered: true,
                files: Vec::new(),
            },
            Level {
                ordinal: 1,
                tiered: false,
                files: vec![Arc::clone(&file)],
            },
            Level {
                ordinal: 2,
                tiered: false,
                files: Vec::new(),
            },
        ],
    };
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(lsm_version),
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    db_state
        .load()
        .truncation_cursors
        .advance(0, DEFAULT_COLUMN_FAMILY_ID, b"z");
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
    let worker = Arc::new(RecordingCompactionWorker::default());
    let worker_dyn: Arc<dyn CompactionWorker> = worker.clone();
    lsm_tree.configure_compaction(
        crate::compaction::CompactionConfig::default(),
        Some(worker_dyn),
    );

    lsm_tree.apply_edit(
        0,
        VersionEdit {
            level_edits: Vec::new(),
        },
        None,
    );

    assert!(lsm_tree.level_files(1).is_empty());
    assert!(worker.submitted_tree_idxs.lock().unwrap().is_empty());
}

#[test]
#[serial_test::serial(file)]
fn test_lsm_get_in_bucket_routes_to_bucket_tree_state() {
    let root = "/tmp/lsm_get_in_bucket_routes";
    cleanup_test_root(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs.clone(), Arc::clone(&metrics_manager)).unwrap());
    let num_columns = 1;
    let file_bucket0 = create_test_sst_in_bucket(
        &file_manager,
        0,
        vec![(b"k", &make_value_bytes(b"v0", num_columns))],
    )
    .unwrap();
    let file_bucket1 = create_test_sst_in_bucket(
        &file_manager,
        1,
        vec![(b"k", &make_value_bytes(b"v1", num_columns))],
    )
    .unwrap();

    let db_state = Arc::new(DbStateHandle::new());
    let multi_lsm_version = MultiLSMTreeVersion::from_parts(
        2,
        vec![0u32, 1u32],
        vec![
            Arc::new(LSMTreeVersion {
                levels: vec![Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![Arc::clone(&file_bucket0)],
                }],
            }),
            Arc::new(LSMTreeVersion {
                levels: vec![Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![Arc::clone(&file_bucket1)],
                }],
            }),
        ],
    );
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version,
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);

    let schema_manager = SchemaManager::new(1);
    let schema = schema_manager.latest_schema();
    let encoded_bucket0 = encode_key(&Key::new(0, b"k".to_vec()));
    let encoded_bucket1 = encode_key(&Key::new(1, b"k".to_vec()));
    let bucket0_values = lsm_tree
        .get(
            &file_manager,
            0,
            encoded_bucket0.as_ref(),
            schema.as_ref(),
            &schema_manager,
            None,
            None,
            None,
        )
        .unwrap();
    let bucket1_values = lsm_tree
        .get(
            &file_manager,
            1,
            encoded_bucket1.as_ref(),
            schema.as_ref(),
            &schema_manager,
            None,
            None,
            None,
        )
        .unwrap();
    assert_eq!(bucket0_values.len(), 1);
    assert_eq!(bucket1_values.len(), 1);
    assert_eq!(
        bucket0_values[0].columns()[0].as_ref().unwrap().data(),
        b"v0".as_slice()
    );
    assert_eq!(
        bucket1_values[0].columns()[0].as_ref().unwrap().data(),
        b"v1".as_slice()
    );
    let unknown_bucket_values = lsm_tree
        .get(
            &file_manager,
            3,
            encoded_bucket0.as_ref(),
            schema.as_ref(),
            &schema_manager,
            None,
            None,
            None,
        )
        .unwrap();
    assert!(unknown_bucket_values.is_empty());
    cleanup_test_root(root);
}

#[test]
fn test_lsm_compaction_submits_only_changed_tree() {
    let db_state = Arc::new(DbStateHandle::new());
    let multi_lsm_version = MultiLSMTreeVersion::from_parts(
        2,
        vec![0u32, 1u32],
        vec![
            Arc::new(LSMTreeVersion {
                levels: vec![
                    Level {
                        ordinal: 0,
                        tiered: true,
                        files: vec![create_data_file_with_size(b"a", b"b", 1)],
                    },
                    Level {
                        ordinal: 1,
                        tiered: false,
                        files: Vec::new(),
                    },
                ],
            }),
            Arc::new(LSMTreeVersion {
                levels: vec![
                    Level {
                        ordinal: 0,
                        tiered: true,
                        files: vec![create_data_file_with_size(b"c", b"d", 1)],
                    },
                    Level {
                        ordinal: 1,
                        tiered: false,
                        files: Vec::new(),
                    },
                ],
            }),
        ],
    );
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version,
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
    let worker = Arc::new(RecordingCompactionWorker::default());
    let worker_dyn: Arc<dyn CompactionWorker> = worker.clone();
    let config = crate::compaction::CompactionConfig {
        l0_file_limit: 0,
        ..crate::compaction::CompactionConfig::default()
    };
    lsm_tree.configure_compaction(config, Some(worker_dyn));
    lsm_tree.apply_edit(
        0,
        VersionEdit {
            level_edits: Vec::new(),
        },
        None,
    );
    let mut submitted = worker.submitted_tree_idxs.lock().unwrap().clone();
    submitted.sort_unstable();
    assert_eq!(submitted, vec![0]);
}

#[test]
fn test_lsm_compaction_submits_configured_output_file_type() {
    let db_state = Arc::new(DbStateHandle::new());
    let multi_lsm_version = MultiLSMTreeVersion::from_parts(
        1,
        vec![0u32],
        vec![Arc::new(LSMTreeVersion {
            levels: vec![
                Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![create_data_file_with_size(b"a", b"b", 1)],
                },
                Level {
                    ordinal: 1,
                    tiered: false,
                    files: Vec::new(),
                },
            ],
        })],
    );
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version,
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
    let worker = Arc::new(RecordingCompactionWorker::default());
    let worker_dyn: Arc<dyn CompactionWorker> = worker.clone();
    let config = crate::compaction::CompactionConfig {
        l0_file_limit: 0,
        output_file_type: DataFileType::Parquet,
        ..crate::compaction::CompactionConfig::default()
    };
    lsm_tree.configure_compaction(config, Some(worker_dyn));
    lsm_tree.apply_edit(
        0,
        VersionEdit {
            level_edits: Vec::new(),
        },
        None,
    );
    let submitted = worker.submitted_data_file_types.lock().unwrap().clone();
    assert_eq!(submitted, vec![DataFileType::Parquet]);
}

#[test]
fn test_lsm_compaction_submits_default_sst_output_file_type() {
    let db_state = Arc::new(DbStateHandle::new());
    let multi_lsm_version = MultiLSMTreeVersion::from_parts(
        1,
        vec![0u32],
        vec![Arc::new(LSMTreeVersion {
            levels: vec![
                Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![create_data_file_with_size(b"a", b"b", 1)],
                },
                Level {
                    ordinal: 1,
                    tiered: false,
                    files: Vec::new(),
                },
            ],
        })],
    );
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version,
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
    let worker = Arc::new(RecordingCompactionWorker::default());
    let worker_dyn: Arc<dyn CompactionWorker> = worker.clone();
    let config = crate::compaction::CompactionConfig {
        l0_file_limit: 0,
        ..crate::compaction::CompactionConfig::default()
    };
    lsm_tree.configure_compaction(config, Some(worker_dyn));
    lsm_tree.apply_edit(
        0,
        VersionEdit {
            level_edits: Vec::new(),
        },
        None,
    );
    let submitted = worker.submitted_data_file_types.lock().unwrap().clone();
    assert_eq!(submitted, vec![DataFileType::SSTable]);
}

/// Regression: a worker that declines to submit (returns `None`) must not leave a stale
/// pending-compaction entry that blocks all future compactions for that tree. The scheduler
/// only inserts pending when `submit_runs` returns `Some(handle)`, so a `None` result leaves
/// the slot free and a subsequent trigger must reach the worker again. We trigger compaction
/// twice and assert two submissions.
///
/// Note on the pending lifecycle: completion (`on_compaction_complete`) acquires the same
/// `state` mutex that `maybe_trigger_compaction_locked` holds, so a worker's async completion
/// task can never run (and remove pending) until the scheduler has released the lock — which
/// is after the pending insert. The insert therefore always precedes the remove; there is no
/// race. A worker that tried to call `on_compaction_complete` synchronously inside
/// `submit_runs` would deadlock on this mutex, which is why all real workers defer completion
/// to a spawned task.
#[test]
fn test_lsm_compaction_declining_worker_does_not_block_subsequent_compaction() {
    let db_state = Arc::new(DbStateHandle::new());
    let multi_lsm_version = MultiLSMTreeVersion::from_parts(
        1,
        vec![0u32],
        vec![Arc::new(LSMTreeVersion {
            levels: vec![
                Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![create_data_file_with_size(b"a", b"b", 1)],
                },
                Level {
                    ordinal: 1,
                    tiered: false,
                    files: Vec::new(),
                },
            ],
        })],
    );
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version,
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
    let worker = Arc::new(RecordingCompactionWorker::default());
    let worker_dyn: Arc<dyn CompactionWorker> = worker.clone();
    let config = crate::compaction::CompactionConfig {
        l0_file_limit: 0,
        ..crate::compaction::CompactionConfig::default()
    };
    lsm_tree.configure_compaction(config, Some(worker_dyn));
    // First trigger: worker declines (returns None). No pending entry is inserted.
    lsm_tree.apply_edit(
        0,
        VersionEdit {
            level_edits: Vec::new(),
        },
        None,
    );
    // Second trigger: must still reach the worker. A stale pending entry would suppress it.
    lsm_tree.apply_edit(
        0,
        VersionEdit {
            level_edits: Vec::new(),
        },
        None,
    );
    let mut submitted = worker.submitted_tree_idxs.lock().unwrap().clone();
    submitted.sort_unstable();
    assert_eq!(
        submitted,
        vec![0, 0],
        "both compaction triggers must reach the worker (declining worker leaves no stale pending)"
    );
}

#[test]
fn test_compaction_result_is_visible_before_next_compaction_is_submitted() {
    let db_state = Arc::new(DbStateHandle::new());
    let old_file = create_data_file_with_size(b"a", b"z", 1);
    let replacement = create_data_file_with_size(b"a", b"z", 1);
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(LSMTreeVersion {
            levels: vec![
                Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![Arc::clone(&old_file)],
                },
                Level {
                    ordinal: 1,
                    tiered: false,
                    files: Vec::new(),
                },
            ],
        }),
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
    let worker = Arc::new(RecordingCompactionWorker::default());
    let worker_dyn: Arc<dyn CompactionWorker> = worker.clone();
    lsm_tree.configure_compaction(
        crate::compaction::CompactionConfig {
            l0_file_limit: 0,
            ..crate::compaction::CompactionConfig::default()
        },
        Some(worker_dyn),
    );
    lsm_tree.on_compaction_started(0);

    assert_eq!(
        lsm_tree.apply_compaction_result(
            0,
            VersionEdit {
                level_edits: vec![LevelEdit {
                    level: 0,
                    removed_files: vec![Arc::clone(&old_file)],
                    new_files: vec![Arc::clone(&replacement)],
                }],
            },
            None,
        ),
        Some(0)
    );

    assert_eq!(
        worker.submitted_file_ids.lock().unwrap().as_slice(),
        &[vec![replacement.file_id]]
    );
    let files = &db_state.load().multi_lsm_version.version_of_index(0).levels[0].files;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].file_id, replacement.file_id);
}

#[test]
fn test_duplicate_compaction_result_does_not_reinstall_non_tiered_output() {
    let db_state = Arc::new(DbStateHandle::new());
    let input = create_data_file_with_size(b"m", b"n", 1);
    let before = create_data_file_with_size(b"a", b"b", 1);
    let output = create_data_file_with_size(b"m", b"n", 1);
    let after = create_data_file_with_size(b"t", b"z", 1);
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(LSMTreeVersion {
            levels: vec![
                Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![Arc::clone(&input)],
                },
                Level {
                    ordinal: 1,
                    tiered: false,
                    files: vec![Arc::clone(&before), Arc::clone(&after)],
                },
            ],
        }),
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let lsm_tree = LSMTree::with_state(
        Arc::clone(&db_state),
        Arc::new(MetricsManager::new("lsm-test")),
    );
    lsm_tree.on_compaction_started(0);
    let edit = VersionEdit {
        level_edits: vec![
            LevelEdit {
                level: 0,
                removed_files: vec![input],
                new_files: Vec::new(),
            },
            LevelEdit {
                level: 1,
                removed_files: Vec::new(),
                new_files: vec![Arc::clone(&output)],
            },
        ],
    };

    assert_eq!(
        lsm_tree.apply_compaction_result(0, edit.clone(), None),
        Some(0)
    );
    assert_eq!(lsm_tree.apply_compaction_result(0, edit, None), None);

    let level = &db_state.load().multi_lsm_version.version_of_index(0).levels[1];
    assert_eq!(
        level
            .files
            .iter()
            .map(|file| file.file_id)
            .collect::<Vec<_>>(),
        vec![before.file_id, output.file_id, after.file_id]
    );
    assert!(
        level
            .files
            .windows(2)
            .all(|files| files[0].end_key < files[1].start_key),
        "non-tiered compaction output must remain sorted and non-overlapping"
    );
}

#[test]
fn test_lsm_auto_split_rewrites_tree_ranges() {
    let db_state = Arc::new(DbStateHandle::new());
    let initial_version = Arc::new(LSMTreeVersion {
        levels: vec![
            Level {
                ordinal: 0,
                tiered: true,
                files: Vec::new(),
            },
            Level {
                ordinal: 1,
                tiered: false,
                files: vec![
                    create_data_file_with_bucket(0, 10),
                    create_data_file_with_bucket(1, 10),
                    create_data_file_with_bucket(2, 10),
                    create_data_file_with_bucket(3, 10),
                ],
            },
        ],
    });
    let scopes = crate::db_state::default_column_family_scopes(&[0u16..=3u16]);
    let multi_lsm_version =
        MultiLSMTreeVersion::from_scopes_with_tree_versions(4, &scopes, vec![initial_version])
            .unwrap();
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version,
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
    let worker = Arc::new(RecordingCompactionWorker::default());
    let worker_dyn: Arc<dyn CompactionWorker> = worker.clone();
    let config = crate::compaction::CompactionConfig {
        l1_base_bytes: 10,
        level_size_multiplier: 1,
        split_trigger_level: Some(1),
        ..crate::compaction::CompactionConfig::default()
    };
    lsm_tree.configure_compaction(config, Some(worker_dyn));

    lsm_tree.apply_edit(
        0,
        VersionEdit {
            level_edits: Vec::new(),
        },
        None,
    );

    let snapshot = db_state.load();
    assert_eq!(snapshot.multi_lsm_version.tree_count(), 4);
    for bucket in 0..4u16 {
        let tree_idx = snapshot
            .multi_lsm_version
            .tree_index_for_bucket_and_column_family(bucket, DEFAULT_COLUMN_FAMILY_ID)
            .expect("tree idx for bucket");
        let range = snapshot
            .multi_lsm_version
            .bucket_range_of_tree(tree_idx)
            .expect("bucket range for tree");
        assert_eq!(range, bucket..=bucket);
        let level1 = snapshot.multi_lsm_version.version_of_index(tree_idx);
        let level1_files = level1
            .levels
            .iter()
            .find(|level| level.ordinal == 1)
            .map(|level| level.files.clone())
            .unwrap_or_default();
        assert_eq!(level1_files.len(), 1);
        assert_eq!(
            key_bucket(&level1_files[0].start_key).expect("encoded test key bucket"),
            bucket
        );
    }
}

#[test]
fn test_lsm_auto_split_skips_l0_trigger_level() {
    let db_state = Arc::new(DbStateHandle::new());
    let initial_version = Arc::new(LSMTreeVersion {
        levels: vec![
            Level {
                ordinal: 0,
                tiered: true,
                files: vec![
                    create_data_file_with_bucket(0, 10),
                    create_data_file_with_bucket(1, 10),
                    create_data_file_with_bucket(2, 10),
                    create_data_file_with_bucket(3, 10),
                ],
            },
            Level {
                ordinal: 1,
                tiered: false,
                files: Vec::new(),
            },
        ],
    });
    let scopes = crate::db_state::default_column_family_scopes(&[0u16..=3u16]);
    let multi_lsm_version =
        MultiLSMTreeVersion::from_scopes_with_tree_versions(4, &scopes, vec![initial_version])
            .unwrap();
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version,
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
    let worker = Arc::new(RecordingCompactionWorker::default());
    let worker_dyn: Arc<dyn CompactionWorker> = worker.clone();
    let config = crate::compaction::CompactionConfig {
        split_trigger_level: Some(0),
        ..crate::compaction::CompactionConfig::default()
    };
    lsm_tree.configure_compaction(config, Some(worker_dyn));
    lsm_tree.apply_edit(
        0,
        VersionEdit {
            level_edits: Vec::new(),
        },
        None,
    );
    assert_eq!(db_state.load().multi_lsm_version.tree_count(), 1);
}

#[test]
fn test_lsm_compaction_completion_rejects_stale_topology_epoch() {
    let db_state = Arc::new(DbStateHandle::new());
    let base_version = Arc::new(LSMTreeVersion {
        levels: vec![Level {
            ordinal: 0,
            tiered: true,
            files: vec![create_data_file_with_bucket(2, 8)],
        }],
    });
    let initial_scopes = crate::db_state::default_column_family_scopes(&[0u16..=1u16, 2u16..=3u16]);
    let initial_multi = MultiLSMTreeVersion::from_scopes_with_tree_versions(
        4,
        &initial_scopes,
        vec![Arc::clone(&base_version), Arc::clone(&base_version)],
    )
    .unwrap();
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: initial_multi,
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
    lsm_tree.on_compaction_started(1);

    let shifted_scopes =
        crate::db_state::default_column_family_scopes(&[0u16..=0u16, 1u16..=1u16, 2u16..=3u16]);
    let shifted_multi = MultiLSMTreeVersion::from_scopes_with_tree_versions(
        4,
        &shifted_scopes,
        vec![
            Arc::clone(&base_version),
            Arc::clone(&base_version),
            Arc::clone(&base_version),
        ],
    )
    .unwrap();
    db_state.store(DbState {
        seq_id: 1,
        topology_epoch: 1,
        bucket_ranges: Vec::new(),
        multi_lsm_version: shifted_multi,
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });

    assert_eq!(lsm_tree.on_compaction_complete(1), None);
    assert_eq!(lsm_tree.on_compaction_complete(1), None);
}

#[test]
fn test_lsm_compaction_completion_skips_when_db_not_open() {
    let db_state = Arc::new(DbStateHandle::new());
    let base_version = Arc::new(LSMTreeVersion {
        levels: vec![Level {
            ordinal: 0,
            tiered: true,
            files: vec![create_data_file_with_bucket(1, 8)],
        }],
    });
    let scopes = crate::db_state::default_column_family_scopes(&[0u16..=1u16]);
    let initial_multi = MultiLSMTreeVersion::from_scopes_with_tree_versions(
        2,
        &scopes,
        vec![Arc::clone(&base_version)],
    )
    .unwrap();
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: initial_multi,
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let lsm_tree = LSMTree::with_state_and_ttl(
        Arc::clone(&db_state),
        Arc::new(TTLProvider::disabled()),
        Arc::new(DbLifecycle::new_initializing()),
        metrics_manager,
    );
    lsm_tree.on_compaction_started(0);
    assert_eq!(lsm_tree.on_compaction_complete(0), None);
}

#[test]
#[serial_test::serial(file)]
fn test_lsm_get_tiered_returns_newest_first() {
    let root = "/tmp/lsm_get_tiered_order";
    cleanup_test_root(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let num_columns = 1;
    let older = create_test_sst(
        &file_manager,
        1,
        vec![(b"k1", &make_value_bytes(b"old", num_columns))],
    )
    .unwrap();
    let newer = create_test_sst(
        &file_manager,
        3,
        vec![(b"k1", &make_value_bytes(b"new", num_columns))],
    )
    .unwrap();
    let db_state = Arc::new(DbStateHandle::new());
    let lsm_version = LSMTreeVersion {
        levels: vec![Level {
            ordinal: 0,
            tiered: true,
            files: vec![older, newer],
        }],
    };
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(lsm_version),
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
    let schema_manager = Arc::new(crate::schema::SchemaManager::new(num_columns));
    let schema = schema_manager.latest_schema();
    let encoded_key = encode_key(&crate::r#type::Key::new(0, b"k1".to_vec()));
    let value = lsm_tree
        .get(
            &file_manager,
            0,
            encoded_key.as_ref(),
            schema.as_ref(),
            schema_manager.as_ref(),
            None,
            None,
            None,
        )
        .unwrap();
    assert_eq!(value.len(), 2);
    assert_eq!(
        value[0].columns()[0].as_ref().unwrap().data().as_ref(),
        b"new"
    );
    assert_eq!(
        value[1].columns()[0].as_ref().unwrap().data().as_ref(),
        b"old"
    );
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_lsm_many_tiered_skips_older_file_after_terminal_per_key() {
    let root = "/tmp/lsm_many_tiered_terminal";
    cleanup_test_root(root);
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register(format!("file://{root}")).unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("lsm-many-tiered"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let num_columns = 1;
    let older = create_test_sst(
        &file_manager,
        1,
        vec![
            (b"k", &make_value_bytes(b"old-k", num_columns)),
            (b"m", &make_value_bytes(b"old-m", num_columns)),
        ],
    )
    .unwrap();
    let newer = create_test_sst(
        &file_manager,
        2,
        vec![
            (b"k", &make_value_bytes(b"new-k", num_columns)),
            (b"z", &make_value_bytes(b"new-z", num_columns)),
        ],
    )
    .unwrap();
    let db_state = Arc::new(DbStateHandle::new());
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(LSMTreeVersion {
            levels: vec![Level {
                ordinal: 0,
                tiered: true,
                files: vec![older, newer],
            }],
        }),
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
    let schema_manager = SchemaManager::new(num_columns);
    let schema = schema_manager.latest_schema();
    let mut requests = vec![
        BatchGetRequest {
            bucket: 0,
            encoded_key: encode_key(&Key::new(0, b"m".to_vec())),
            values: Vec::new(),
            terminal_mask: None,
            decode_mask: vec![0x01],
            stopped: false,
        },
        BatchGetRequest {
            bucket: 0,
            encoded_key: encode_key(&Key::new(0, b"k".to_vec())),
            values: Vec::new(),
            terminal_mask: None,
            decode_mask: vec![0x01],
            stopped: false,
        },
    ];

    lsm_tree
        .get_many_with_snapshot(
            &file_manager,
            db_state.load(),
            &mut requests,
            schema.as_ref(),
            &schema_manager,
            None,
            None,
            DEFAULT_COLUMN_FAMILY_ID,
        )
        .unwrap();

    assert_eq!(requests[0].values.len(), 1);
    assert_eq!(
        requests[0].values[0].columns()[0]
            .as_ref()
            .unwrap()
            .data()
            .as_ref(),
        b"old-m"
    );
    assert_eq!(requests[1].values.len(), 1, "terminal key skips older file");
    assert_eq!(
        requests[1].values[0].columns()[0]
            .as_ref()
            .unwrap()
            .data()
            .as_ref(),
        b"new-k"
    );
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_lsm_many_non_tiered_routes_unsorted_small_batch_across_gaps() {
    let root = "/tmp/lsm_many_non_tiered_routing";
    cleanup_test_root(root);
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register(format!("file://{root}")).unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("lsm-many-non-tiered"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let num_columns = 1;
    let left = create_test_sst(
        &file_manager,
        1,
        vec![
            (b"a", &make_value_bytes(b"left-a", num_columns)),
            (b"b", &make_value_bytes(b"left-b", num_columns)),
        ],
    )
    .unwrap();
    let right = create_test_sst(
        &file_manager,
        2,
        vec![
            (b"d", &make_value_bytes(b"right-d", num_columns)),
            (b"e", &make_value_bytes(b"right-e", num_columns)),
        ],
    )
    .unwrap();
    let db_state = Arc::new(DbStateHandle::new());
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(LSMTreeVersion {
            levels: vec![Level {
                ordinal: 1,
                tiered: false,
                files: vec![left, right],
            }],
        }),
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
    let schema_manager = SchemaManager::new(num_columns);
    let schema = schema_manager.latest_schema();
    let mut requests = [b"e", b"c", b"a", b"b"]
        .into_iter()
        .map(|key| BatchGetRequest {
            bucket: 0,
            encoded_key: encode_key(&Key::new(0, key.to_vec())),
            values: Vec::new(),
            terminal_mask: None,
            decode_mask: vec![0x01],
            stopped: false,
        })
        .collect::<Vec<_>>();

    lsm_tree
        .get_many_with_snapshot(
            &file_manager,
            db_state.load(),
            &mut requests,
            schema.as_ref(),
            &schema_manager,
            None,
            None,
            DEFAULT_COLUMN_FAMILY_ID,
        )
        .unwrap();

    assert_eq!(
        requests[0].values[0].columns()[0]
            .as_ref()
            .unwrap()
            .data()
            .as_ref(),
        b"right-e"
    );
    assert!(
        requests[1].values.is_empty(),
        "key in the file gap is absent"
    );
    assert_eq!(
        requests[2].values[0].columns()[0]
            .as_ref()
            .unwrap()
            .data()
            .as_ref(),
        b"left-a"
    );
    assert_eq!(
        requests[3].values[0].columns()[0]
            .as_ref()
            .unwrap()
            .data()
            .as_ref(),
        b"left-b"
    );
    cleanup_test_root(root);
}

#[test]
fn test_add_level0_files_routes_only_to_matching_column_family_scope() {
    let db_state = Arc::new(DbStateHandle::new());
    let scopes = vec![
        LSMTreeScope::new(0u16..=0u16, DEFAULT_COLUMN_FAMILY_ID),
        LSMTreeScope::new(0u16..=0u16, 1),
    ];
    let multi_lsm_version = MultiLSMTreeVersion::from_scopes_with_tree_versions(
        1,
        &scopes,
        empty_lsm_versions(scopes.len()),
    )
    .unwrap();
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version,
        vlog_version: VlogVersion::new(),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);

    let file = create_data_file_in_scope(0, 0, 1, 8);
    lsm_tree
        .add_level0_files(
            Uuid::new_v4(),
            vec![(LSMTreeScope::new(0u16..=0u16, 1), file)],
            None,
        )
        .unwrap();

    assert!(lsm_tree.level_files_in_tree(0, 0).is_empty());
    assert_eq!(lsm_tree.level_files_in_tree(1, 0).len(), 1);
}

#[test]
fn test_add_level0_files_split_remap_stays_in_same_cf_and_applies_vlog_once() {
    let db_state = Arc::new(DbStateHandle::new());
    let scopes = vec![
        LSMTreeScope::new(0u16..=0u16, DEFAULT_COLUMN_FAMILY_ID),
        LSMTreeScope::new(1u16..=1u16, DEFAULT_COLUMN_FAMILY_ID),
        LSMTreeScope::new(0u16..=1u16, 1),
    ];
    let multi_lsm_version = MultiLSMTreeVersion::from_scopes_with_tree_versions(
        2,
        &scopes,
        empty_lsm_versions(scopes.len()),
    )
    .unwrap();
    let tracked_vlog = TrackedFileId::untracked(700);
    db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version,
        vlog_version: VlogVersion::from_files_with_entries(vec![(7, tracked_vlog, 0)]),
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let metrics_manager = Arc::new(MetricsManager::new("lsm-test"));
    let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);

    let file = create_data_file_in_scope(0, 1, DEFAULT_COLUMN_FAMILY_ID, 16);
    lsm_tree
        .add_level0_files(
            Uuid::new_v4(),
            vec![(
                LSMTreeScope::new(0u16..=1u16, DEFAULT_COLUMN_FAMILY_ID),
                Arc::clone(&file),
            )],
            Some(VlogEdit::from_entry_deltas(vec![(7, 1)])),
        )
        .unwrap();

    let tree0_files = lsm_tree.level_files_in_tree(0, 0);
    let tree1_files = lsm_tree.level_files_in_tree(1, 0);
    let tree2_files = lsm_tree.level_files_in_tree(2, 0);
    assert_eq!(tree0_files.len(), 1);
    assert_eq!(tree1_files.len(), 1);
    assert!(tree2_files.is_empty());
    assert_eq!(tree0_files[0].effective_bucket_range, (0u16..=0u16));
    assert_eq!(tree1_files[0].effective_bucket_range, (1u16..=1u16));

    let files_with_entries = db_state.load().vlog_version.files_with_entries();
    let (_, _, valid_entries) = files_with_entries
        .into_iter()
        .find(|(seq, _, _)| *seq == 7)
        .expect("vlog file seq 7 should remain tracked");
    assert_eq!(valid_entries, 1);
}
