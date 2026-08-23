use super::*;
use crate::config::MemtableType;
use crate::db_iter::{DbIterator, DbIteratorOptions};
use crate::db_state::{DbState, DbStateHandle, MultiLSMTreeVersion};
use crate::file::{FileManager, FileSystemRegistry};
use crate::key_codec::encode_scan_key;
use crate::lsm::LSMTreeVersion;
use crate::sst::row_codec::decode_value;
use crate::sst::{SSTIterator, SSTIteratorOptions, SSTWriterOptions};
use crate::r#type::ValueType;
use crate::r#type::{RefColumn, RefKey, RefValue};
use crate::vlog::VlogStore;
use bytes::Bytes;
use std::collections::VecDeque;
use std::ops::ControlFlow;
use uuid::Uuid;

#[test]
fn default_options_use_skiplist_memtable() {
    assert_eq!(
        MemtableManagerOptions::default().memtable_type,
        MemtableType::Skiplist
    );
}

fn cleanup_test_root() {
    let _ = std::fs::remove_dir_all("/tmp/memtable_manager_test");
}

fn empty_lsm_versions(len: usize) -> Vec<Arc<LSMTreeVersion>> {
    let mut v: Vec<Arc<LSMTreeVersion>> = Vec::with_capacity(len);
    (0..len).for_each(|_| v.push(Arc::new(LSMTreeVersion { levels: vec![] })));
    v
}

fn build_test_memtable(
    memtable_type: MemtableType,
    entries: &[(&[u8], ValueType, &[u8])],
) -> MemtableImpl {
    let mut memtable = match memtable_type {
        MemtableType::Hash => MemtableImpl::Hash(HashMemtable::with_capacity(4096)),
        MemtableType::Skiplist | MemtableType::Adaptive => {
            MemtableImpl::Skiplist(SkiplistMemtable::with_capacity(4096))
        }
        MemtableType::Vec => MemtableImpl::Vec(VecMemtable::with_capacity(4096)),
    };
    for (key_bytes, value_type, value_bytes) in entries {
        let key = RefKey::new(0, key_bytes);
        let value = RefValue::new(vec![Some(RefColumn::new(*value_type, value_bytes))]);
        memtable.put_ref(&key, &value, 1).unwrap();
    }
    memtable
}

fn active_data_offset(manager: &MemtableManager) -> usize {
    manager
        .db_state
        .load()
        .active
        .as_ref()
        .unwrap()
        .read()
        .unwrap()
        .readable_memtable()
        .unwrap()
        .data_offset()
}

#[test]
fn inplace_update_requires_full_terminal_row() {
    let terminal = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"value"))]);
    let merge = RefValue::new(vec![Some(RefColumn::new(ValueType::Merge, b"value"))]);
    let partial = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"value")), None]);
    assert!(is_full_terminal_ref_value(&terminal, 1));
    assert!(!is_full_terminal_ref_value(&merge, 1));
    assert!(!is_full_terminal_ref_value(&partial, 2));
}

#[test]
fn active_scan_seals_entries_appended_after_iterator_creation() {
    for memtable_type in [
        MemtableType::Hash,
        MemtableType::Skiplist,
        MemtableType::Vec,
    ] {
        let schema = Arc::new(Schema::new(1, 1, Vec::new()));
        let active = Arc::new(RwLock::new(ActiveMemtable {
            id: Uuid::new_v4(),
            schema,
            sealed_data_end: 0,
            contents: ActiveMemtableContents::Writable(build_test_memtable(memtable_type, &[])),
        }));
        let mut iter = MemtableScanIterator::for_active(
            Arc::clone(&active),
            None,
            None,
            None,
            None,
            None,
            false,
        );

        let key = RefKey::new(0, b"key");
        let old = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"old"))]);
        let new = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"new"))]);
        {
            let mut active = active.write().unwrap();
            active
                .writable_memtable()
                .unwrap()
                .put_ref(&key, &old, 1)
                .unwrap();
        }

        iter.seek_to_first().unwrap();
        let captured_end = active
            .read()
            .unwrap()
            .readable_memtable()
            .unwrap()
            .data_offset();
        {
            let mut active = active.write().unwrap();
            assert_eq!(active.sealed_data_end, captured_end, "{memtable_type:?}");
            active.put_ref_or_replace(&key, &new, 1).unwrap();
            assert!(
                active.readable_memtable().unwrap().data_offset() > captured_end,
                "{memtable_type:?}"
            );
        }

        let mut retained = iter.take_value().unwrap().unwrap().unwrap_encoded();
        let retained = decode_value(&mut retained, 1).unwrap();
        assert_eq!(
            retained.columns()[0].as_ref().unwrap().data().as_ref(),
            b"old",
            "{memtable_type:?}"
        );
    }
}

#[test]
#[serial_test::serial(file)]
fn active_snapshot_capture_end_excludes_later_tail_and_restores() {
    let root = "/tmp/memtable_snapshot_capture_end";
    let _ = std::fs::remove_dir_all(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let metrics = Arc::new(MetricsManager::new("memtable-capture-end-test"));
    let file_manager = Arc::new(FileManager::with_defaults(fs, metrics).unwrap());
    let schema_manager = Arc::new(SchemaManager::new(1));
    let snapshot_manager = SnapshotManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&schema_manager),
        Arc::new(DbLifecycle::new_open()),
        None,
        false,
        false,
        vec![0..=u16::MAX],
        Arc::new(crate::time::SystemTimeProvider),
    );
    let active = Arc::new(RwLock::new(ActiveMemtable {
        id: Uuid::new_v4(),
        schema: schema_manager.latest_schema(),
        sealed_data_end: 0,
        contents: ActiveMemtableContents::Writable(MemtableImpl::Vec(VecMemtable::with_capacity(
            1024,
        ))),
    }));
    let first_key = RefKey::new(0, b"first");
    let first_value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"one"))]);
    let replacement_value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"two"))]);
    let capture_end = {
        let mut active = active.write().unwrap();
        active
            .writable_memtable()
            .unwrap()
            .put_ref(&first_key, &first_value, 1)
            .unwrap();
        let capture_end = active.readable_memtable().unwrap().data_offset();
        active.seal_data_end(capture_end);
        let sealed_data_end = active.sealed_data_end;
        assert!(
            !active
                .writable_memtable()
                .unwrap()
                .try_replace_latest_ref(&first_key, &replacement_value, 1, sealed_data_end)
                .unwrap()
        );
        active
            .writable_memtable()
            .unwrap()
            .put_ref(&first_key, &replacement_value, 1)
            .unwrap();
        capture_end
    };
    let snapshot_write = MemtableManager::write_active_memtable_snapshot_data(
        1,
        None,
        &active,
        capture_end,
        &snapshot_manager,
        &file_manager,
    )
    .unwrap();
    assert_eq!(snapshot_write.active_data[0].end_offset, capture_end as u64);
    let restored =
        decode_active_snapshot_segments_into_memtable(&file_manager, &snapshot_write.active_data)
            .unwrap();
    let mut restored_value = Bytes::copy_from_slice(
        restored
            .get(&encode_scan_key(0, 0, b"first"))
            .expect("captured entry is restored"),
    );
    let restored_value = decode_value(&mut restored_value, 1).unwrap();
    assert_eq!(
        restored_value.columns()[0]
            .as_ref()
            .unwrap()
            .data()
            .as_ref(),
        b"one"
    );
    assert_eq!(restored.data_offset(), capture_end);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn incremental_snapshot_scheduling_seals_active_before_later_writes() {
    let root = "/tmp/memtable_incremental_snapshot_seal";
    let _ = std::fs::remove_dir_all(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let metrics = Arc::new(MetricsManager::new("memtable-snapshot-seal-test"));
    let file_manager = Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics)).unwrap());
    let db_lifecycle = Arc::new(DbLifecycle::new_open());
    let schema_manager = Arc::new(SchemaManager::new(1));
    let lsm_tree = Arc::new(LSMTree::with_state(
        Arc::new(DbStateHandle::new()),
        Arc::clone(&metrics),
    ));
    let snapshot_manager = SnapshotManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&schema_manager),
        Arc::clone(&db_lifecycle),
        None,
        false,
        false,
        vec![0..=u16::MAX],
        Arc::new(crate::time::SystemTimeProvider),
    );
    let manager = MemtableManager::new(
        file_manager,
        lsm_tree,
        MemtableManagerOptions {
            memtable_capacity: 4096,
            buffer_count: 2,
            num_columns: 1,
            write_stall_limit: 8,
            schema_manager: Some(schema_manager),
            active_memtable_incremental_snapshot_ratio: 1.0,
            db_lifecycle: Some(db_lifecycle),
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    manager.open().unwrap();
    let key = RefKey::new(0, b"key");
    let old = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"old"))]);
    let replacement = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"new"))]);
    manager.put(&key, &old).unwrap();
    let capture_end = active_data_offset(&manager);

    manager
        .create_snapshot(snapshot_manager.clone(), None)
        .unwrap();
    let active = manager.db_state.load().active.clone().unwrap();
    assert_eq!(active.read().unwrap().sealed_data_end, capture_end);
    manager.put(&key, &replacement).unwrap();
    assert!(active_data_offset(&manager) > capture_end);

    assert!(manager.wait_for_flushes().iter().all(Result::is_ok));
    assert!(snapshot_manager.wait_for_materialization(Duration::from_secs(5)));
    manager.close().unwrap();
    snapshot_manager.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn manager_inplace_updates_only_full_terminal_rows() {
    for memtable_type in [
        MemtableType::Hash,
        MemtableType::Skiplist,
        MemtableType::Vec,
    ] {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test")
            .unwrap();
        let metrics = Arc::new(MetricsManager::new("memtable-inplace-update-test"));
        let file_manager = Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics)).unwrap());
        let lsm_tree = Arc::new(LSMTree::with_state(
            Arc::new(DbStateHandle::new()),
            Arc::clone(&metrics),
        ));
        let schema_manager = Arc::new(SchemaManager::new(2));
        let manager = MemtableManager::new(
            file_manager,
            lsm_tree,
            MemtableManagerOptions {
                memtable_capacity: 4096,
                buffer_count: 2,
                memtable_type,
                num_columns: 2,
                write_stall_limit: 8,
                schema_manager: Some(schema_manager),
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();
        manager.open().unwrap();

        let key = RefKey::new(0, b"key");
        let old = RefValue::new(vec![
            Some(RefColumn::new(ValueType::Put, b"aa")),
            Some(RefColumn::new(ValueType::Put, b"bb")),
        ]);
        let replacement = RefValue::new(vec![
            Some(RefColumn::new(ValueType::Put, b"cc")),
            Some(RefColumn::new(ValueType::Put, b"dd")),
        ]);
        manager.put(&key, &old).unwrap();
        let offset = active_data_offset(&manager);
        manager.put(&key, &replacement).unwrap();
        let replaced_offset = active_data_offset(&manager);
        assert_eq!(replaced_offset, offset, "{memtable_type:?}");

        let merge = RefValue::new(vec![
            Some(RefColumn::new(ValueType::Merge, b"ee")),
            Some(RefColumn::new(ValueType::Put, b"ff")),
        ]);
        manager.put(&key, &merge).unwrap();
        let merge_offset = active_data_offset(&manager);
        assert!(merge_offset > replaced_offset, "{memtable_type:?}");

        let partial = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"gg")), None]);
        manager.put(&key, &partial).unwrap();
        let partial_offset = active_data_offset(&manager);
        assert!(partial_offset > merge_offset, "{memtable_type:?}");
        manager.close().unwrap();
    }
    cleanup_test_root();
}

#[test]
fn get_all_until_stops_before_older_active_and_immutable_values() {
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_point_get")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-point-get-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let lsm_tree = Arc::new(LSMTree::with_state(
        Arc::new(DbStateHandle::new()),
        Arc::clone(&metrics_manager),
    ));
    let manager = MemtableManager::new(file_manager, lsm_tree, Default::default()).unwrap();
    let schema = manager.schema_manager.latest_schema();
    let snapshot = Arc::new(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(LSMTreeVersion { levels: Vec::new() }),
        vlog_version: crate::vlog::VlogVersion::new(),
        active: Some(Arc::new(RwLock::new(ActiveMemtable {
            id: Uuid::new_v4(),
            schema: Arc::clone(&schema),
            sealed_data_end: 0,
            contents: ActiveMemtableContents::Writable(build_test_memtable(
                MemtableType::Skiplist,
                &[
                    (b"k", ValueType::Put, b"old-active"),
                    (b"k", ValueType::Put, b"new-active"),
                ],
            )),
        }))),
        immutables: VecDeque::from([ImmutableMemtable {
            id: Uuid::new_v4(),
            schema,
            memtable: Arc::new(build_test_memtable(
                MemtableType::Skiplist,
                &[(b"k", ValueType::Put, b"immutable")],
            )),
        }]),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });

    let key = encode_scan_key(0, 0, b"k");
    let mut visited = Vec::new();
    manager
        .get_all_with_snapshot_until(snapshot, key.as_ref(), |raw, _| {
            visited.push(Bytes::copy_from_slice(raw));
            Ok(ControlFlow::Break(()))
        })
        .unwrap();

    assert_eq!(visited.len(), 1);
    let mut value = visited.pop().unwrap();
    let decoded = decode_value(&mut value, 1).unwrap();
    assert_eq!(
        decoded.columns()[0].as_ref().unwrap().data().as_ref(),
        b"new-active"
    );
    manager.force_close();
}

fn build_test_skiplist_with_reclaimer(
    entries: &[(&[u8], ValueType, &[u8])],
    reclaimer: MemtableReclaimer,
) -> Arc<MemtableImpl> {
    let mut memtable = SkiplistMemtable::with_buffer_and_reclaimer(vec![0; 4096], reclaimer);
    for (key_bytes, value_type, value_bytes) in entries {
        let key = RefKey::new(0, key_bytes);
        let value = RefValue::new(vec![Some(RefColumn::new(*value_type, value_bytes))]);
        memtable.put_ref(&key, &value, 1).unwrap();
    }
    Arc::new(MemtableImpl::Skiplist(memtable))
}

#[test]
fn skiplist_scan_cursor_immutable_pins_and_borrows_without_copy() {
    let reclaimed = Arc::new(AtomicUsize::new(0));
    let reclaimer: MemtableReclaimer = {
        let reclaimed = Arc::clone(&reclaimed);
        Arc::new(move |_| {
            reclaimed.fetch_add(1, Ordering::Relaxed);
        })
    };
    let memtable = build_test_skiplist_with_reclaimer(
        &[
            (b"a", ValueType::Put, b"outside"),
            (b"b", ValueType::Put, b"old"),
            (b"b", ValueType::Delete, b""),
            (b"b", ValueType::Put, b"new"),
            (b"c", ValueType::Put, b"outside"),
        ],
        reclaimer,
    );
    let start = encode_scan_key(0, 0, b"b");
    let end = encode_scan_key(0, 0, b"c");
    let expected_key_ptr = match memtable.as_ref() {
        MemtableImpl::Skiplist(skiplist_memtable) => {
            let mut iter = skiplist_memtable.iter();
            iter.seek(start.as_ref()).unwrap();
            assert!(iter.next().unwrap());
            iter.key().unwrap().unwrap().as_ptr()
        }
        _ => unreachable!(),
    };
    let mut cursor = SkiplistScanCursor::new(
        MemtableScanSource::Immutable(Arc::clone(&memtable)),
        Some(end),
    );
    drop(memtable);
    assert_eq!(reclaimed.load(Ordering::Relaxed), 0);

    cursor.seek(start.as_ref()).unwrap();
    let retained_key = cursor.take_key().unwrap().unwrap();
    let retained_value = cursor.take_value().unwrap().unwrap().unwrap_encoded();
    assert_eq!(retained_key.as_ptr(), expected_key_ptr);
    assert_eq!(
        retained_value.as_ptr(),
        retained_key.as_ptr().wrapping_add(retained_key.len())
    );
    assert_eq!(retained_key.as_ref(), start.as_ref());
    assert!(cursor.next().unwrap());
    for (index, (expected_type, expected_data)) in [
        (ValueType::Delete, b"".as_slice()),
        (ValueType::Put, b"old".as_slice()),
    ]
    .into_iter()
    .enumerate()
    {
        assert!(cursor.valid());
        assert_eq!(cursor.key().unwrap(), Some(start.as_ref()));
        let mut value = cursor.take_value().unwrap().unwrap().unwrap_encoded();
        let decoded = decode_value(&mut value, 1).unwrap();
        let column = decoded.columns()[0].as_ref().unwrap();
        assert_eq!(column.value_type, expected_type);
        assert_eq!(column.data().as_ref(), expected_data);
        assert_eq!(cursor.next().unwrap(), index == 0);
    }
    assert!(!cursor.valid());
    drop(cursor);
    assert_eq!(reclaimed.load(Ordering::Relaxed), 0);
    let mut retained_value = retained_value;
    let retained = decode_value(&mut retained_value, 1).unwrap();
    assert_eq!(
        retained.columns()[0].as_ref().unwrap().data().as_ref(),
        b"new"
    );
    drop(retained);
    drop(retained_key);
    drop(retained_value);
    assert_eq!(reclaimed.load(Ordering::Relaxed), 1);
}

#[test]
fn skiplist_scan_cursor_active_releases_lock_and_retains_entry_across_rotation() {
    let schema = Arc::new(Schema::new(1, 1, Vec::new()));
    let active = Arc::new(RwLock::new(ActiveMemtable {
        id: Uuid::new_v4(),
        schema,
        sealed_data_end: 0,
        contents: ActiveMemtableContents::Writable(build_test_memtable(
            MemtableType::Skiplist,
            &[
                (b"a", ValueType::Put, b"value-a"),
                (b"b", ValueType::Put, b"value-b"),
            ],
        )),
    }));
    let mut active_cursor =
        SkiplistScanCursor::new(MemtableScanSource::Active(Arc::clone(&active)), None);

    active_cursor.seek_to_first().unwrap();
    assert!(active.try_write().is_ok());

    let rotated = {
        let mut active_guard = active.write().unwrap();
        active_guard.freeze().unwrap()
    };
    let key = active_cursor.take_key().unwrap().unwrap();
    let value = active_cursor
        .take_value()
        .unwrap()
        .unwrap()
        .unwrap_encoded();
    drop(rotated);
    assert_eq!(key.as_ref(), encode_scan_key(0, 0, b"a").as_ref());
    let mut decoded = value;
    assert_eq!(
        decode_value(&mut decoded, 1).unwrap().columns()[0]
            .as_ref()
            .unwrap()
            .data()
            .as_ref(),
        b"value-a"
    );
    assert!(active_cursor.next().unwrap());
    assert_eq!(
        active_cursor.key().unwrap(),
        Some(encode_scan_key(0, 0, b"b").as_ref())
    );
    let mut value = active_cursor
        .take_value()
        .unwrap()
        .unwrap()
        .unwrap_encoded();
    assert_eq!(
        decode_value(&mut value, 1).unwrap().columns()[0]
            .as_ref()
            .unwrap()
            .data()
            .as_ref(),
        b"value-b"
    );
}

#[test]
fn hash_bucket_advisor_waits_for_three_samples_and_weights_them() {
    let mut advisor = HashMemtableBucketAdvisor::default();
    let capacity = 4096;
    let default_bucket_count = HashMemtable::default_bucket_count(capacity);

    advisor.record_capacity_full(
        HashMemtableCapacitySample {
            entry_count: 1,
            used_entry_bytes: 100,
        },
        capacity,
    );
    advisor.record_capacity_full(
        HashMemtableCapacitySample {
            entry_count: 1,
            used_entry_bytes: 200,
        },
        capacity,
    );
    assert_eq!(advisor.recommended_bucket_count, None);
    assert_eq!(advisor.bucket_count_for(capacity), default_bucket_count);

    advisor.record_capacity_full(
        HashMemtableCapacitySample {
            entry_count: 1,
            used_entry_bytes: 300,
        },
        capacity,
    );
    // 20/30/50 weighting gives 230 bytes per entry, so B = round(4096 / (4 + .70 * 230)).
    assert_eq!(advisor.recommended_bucket_count, Some(25));
    assert_eq!(advisor.bucket_count_for(capacity), 25);
}

#[test]
fn hash_bucket_advisor_keeps_only_the_latest_three_samples() {
    let mut advisor = HashMemtableBucketAdvisor::default();
    let capacity = 4096;
    for used_entry_bytes in [100, 200, 300, 400] {
        advisor.record_capacity_full(
            HashMemtableCapacitySample {
                entry_count: 1,
                used_entry_bytes,
            },
            capacity,
        );
    }

    assert_eq!(
        advisor
            .capacity_full_samples
            .iter()
            .map(|sample| sample.used_entry_bytes)
            .collect::<Vec<_>>(),
        vec![200, 300, 400]
    );
    // 20/30/50 weighting gives 330 bytes per entry.
    assert_eq!(advisor.recommended_bucket_count, Some(17));
}

#[test]
fn hash_bucket_advisor_ignores_manual_and_snapshot_flushes() {
    let mut hash_memtable = HashMemtable::with_capacity(256);
    hash_memtable.put(b"key", b"value").unwrap();
    let memtable = MemtableImpl::Hash(hash_memtable);
    let mut advisor = HashMemtableBucketAdvisor::default();

    let memtable_id = Uuid::new_v4();
    advisor.observe_flush(FlushCause::Manual, memtable_id, &memtable, 256);
    advisor.observe_flush(FlushCause::Snapshot, memtable_id, &memtable, 256);
    advisor.observe_flush(
        FlushCause::CapacityFull(Uuid::new_v4()),
        memtable_id,
        &memtable,
        256,
    );
    assert!(advisor.capacity_full_samples.is_empty());
    assert_eq!(advisor.recommended_bucket_count, None);

    advisor.observe_flush(
        FlushCause::CapacityFull(memtable_id),
        memtable_id,
        &memtable,
        256,
    );
    assert_eq!(advisor.capacity_full_samples.len(), 1);
}

#[test]
#[serial_test::serial(file)]
fn hash_capacity_full_rotation_uses_recommended_bucket_count() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_test")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-hash-advisor-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let db_lifecycle = Arc::new(DbLifecycle::new_open());
    let schema_manager = Arc::new(SchemaManager::new(1));
    let lsm_tree = Arc::new(LSMTree::with_state(
        Arc::new(DbStateHandle::new()),
        Arc::clone(&metrics_manager),
    ));
    let snapshot_manager = SnapshotManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&schema_manager),
        Arc::clone(&db_lifecycle),
        None,
        false,
        false,
        vec![0..=u16::MAX],
        Arc::new(crate::time::SystemTimeProvider),
    );
    let manager = MemtableManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 256,
            buffer_count: 2,
            memtable_type: MemtableType::Hash,
            num_columns: 1,
            write_stall_limit: 8,
            schema_manager: Some(schema_manager),
            auto_snapshot_manager: Some(snapshot_manager.clone()),
            db_lifecycle: Some(db_lifecycle),
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    manager.open().unwrap();

    {
        let mut state = manager.state.lock().unwrap();
        for used_entry_bytes in [100, 200, 300] {
            state.hash_bucket_advisor.record_capacity_full(
                HashMemtableCapacitySample {
                    entry_count: 1,
                    used_entry_bytes,
                },
                256,
            );
        }
        assert_eq!(state.hash_bucket_advisor.recommended_bucket_count, Some(2));
    }

    let large_value = vec![b'v'; 96];
    for key_bytes in [b"key1".as_slice(), b"key2".as_slice(), b"key3".as_slice()] {
        let key = RefKey::new(0, key_bytes);
        let value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, &large_value))]);
        manager.put(&key, &value).unwrap();
    }

    let expected_bucket_count = {
        let state = manager.state.lock().unwrap();
        assert_ne!(
            state
                .hash_bucket_advisor
                .capacity_full_samples
                .front()
                .map(|sample| sample.used_entry_bytes),
            Some(100)
        );
        state.hash_bucket_advisor.bucket_count_for(256)
    };
    let active = manager.db_state.load().active.clone().unwrap();
    let active = active.read().unwrap();
    let MemtableImpl::Hash(memtable) = active.readable_memtable().unwrap() else {
        panic!("expected active HashMemtable");
    };
    assert_eq!(memtable.bucket_count(), expected_bucket_count);
    drop(active);

    let results = manager.wait_for_flushes();
    assert!(!results.is_empty());
    assert!(results.iter().all(Result::is_ok));
    assert!(snapshot_manager.wait_for_materialization(Duration::from_secs(5)));
    assert!(manager.db_state.load().suggested_base_snapshot_id.is_some());
    snapshot_manager.close().unwrap();
    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_scan_max_rows_shadow_allowance_disables_deeper_collected_limits_after_skiplist() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_test")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-shadow-allowance-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let lsm_tree = Arc::new(LSMTree::with_state(
        Arc::new(DbStateHandle::new()),
        Arc::clone(&metrics_manager),
    ));
    let manager = Arc::new(
        MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 4096,
                buffer_count: 2,
                num_columns: 1,
                write_stall_limit: 8,
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap(),
    );
    manager.open().unwrap();

    let schema = manager.schema_manager.latest_schema();
    let active = Arc::new(RwLock::new(ActiveMemtable {
        id: Uuid::new_v4(),
        schema: Arc::clone(&schema),
        sealed_data_end: 0,
        contents: ActiveMemtableContents::Writable(build_test_memtable(
            MemtableType::Hash,
            &[
                (b"a".as_slice(), ValueType::Delete, b""),
                (b"c".as_slice(), ValueType::Delete, b""),
                (b"e".as_slice(), ValueType::Delete, b""),
            ],
        )),
    }));
    let immutables = VecDeque::from(vec![
        ImmutableMemtable {
            id: Uuid::new_v4(),
            schema: Arc::clone(&schema),
            memtable: Arc::new(build_test_memtable(
                MemtableType::Hash,
                &[
                    (b"a".as_slice(), ValueType::Put, b"va"),
                    (b"b".as_slice(), ValueType::Put, b"vb"),
                    (b"c".as_slice(), ValueType::Put, b"vc"),
                    (b"d".as_slice(), ValueType::Put, b"vd"),
                    (b"e".as_slice(), ValueType::Put, b"ve"),
                    (b"f".as_slice(), ValueType::Put, b"vf"),
                ],
            )),
        },
        ImmutableMemtable {
            id: Uuid::new_v4(),
            schema: Arc::clone(&schema),
            memtable: Arc::new(build_test_memtable(
                MemtableType::Skiplist,
                &[
                    (b"b".as_slice(), ValueType::Delete, b""),
                    (b"d".as_slice(), ValueType::Delete, b""),
                ],
            )),
        },
    ]);
    let snapshot = Arc::new(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(LSMTreeVersion { levels: Vec::new() }),
        vlog_version: crate::vlog::VlogVersion::new(),
        active: Some(Arc::clone(&active)),
        immutables,
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });

    let start_key = encode_scan_key(0, 0, b"");
    let end_key = encode_scan_key(0, 0, b"\xff");
    let memtable_iters = manager
        .scan_memtable_iterators_with_snapshot(
            Arc::clone(&snapshot),
            Arc::clone(&schema),
            0,
            Some(&[0]),
            Some(start_key.clone()),
            Some(end_key.clone()),
            Some(1),
        )
        .unwrap();
    let active_guard = active.read().unwrap();
    assert_eq!(
        active_guard.sealed_data_end,
        active_guard.readable_memtable().unwrap().data_offset()
    );
    drop(active_guard);
    let mut iter = DbIterator::new(
        memtable_iters,
        Vec::new(),
        DbIteratorOptions {
            end_bound: Some((end_key.clone(), false)),
            lower_bound_exclusive: None,
            max_rows: Some(1),
            snapshot,
            memtable_manager: Some(Arc::clone(&manager)),
            access_guard: None,
            vlog_store: Arc::clone(&manager.vlog_store),
            ttl_provider: manager.lsm_tree.ttl_provider(),
            schema,
            column_family_id: 0,
            should_stop_at_block_boundary: false,
        },
    );
    iter.seek(start_key.as_ref()).unwrap();

    let rows: Vec<(Bytes, Vec<Option<Bytes>>)> = iter.collect::<Result<Vec<_>>>().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, Bytes::from_static(b"f"));
    assert_eq!(rows[0].1[0].as_deref(), Some(b"vf".as_slice()));

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_memtable_flush_deduplicates() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_test")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let lsm_tree = Arc::new(LSMTree::with_state(
        Arc::new(DbStateHandle::new()),
        Arc::clone(&metrics_manager),
    ));
    let manager = MemtableManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 256,
            buffer_count: 2,
            writer_options: WriterOptions::Sst(SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            }),
            num_columns: 1,
            write_stall_limit: 8,
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    manager.open().unwrap();

    let num_columns = 1;
    let key_a = RefKey::new(0, b"a");
    let key_b = RefKey::new(0, b"b");
    let old = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"old"))]);
    let new = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"new"))]);
    let v1 = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v1"))]);

    manager.put(&key_a, &old).unwrap();
    manager.put(&key_a, &new).unwrap();
    manager.put(&key_b, &v1).unwrap();

    manager.flush_active().unwrap();
    let results = manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    assert!(results[0].is_ok());
    let data_file = results[0].as_ref().unwrap().data_files_by_scope[0]
        .1
        .clone();
    let level0_files = lsm_tree.level_files(0);
    assert_eq!(level0_files.len(), 1);
    assert_eq!(level0_files[0].file_id, data_file.file_id);
    let write_metadata = data_file
        .sst_read_metadata()
        .expect("flush should install SST read metadata eagerly");
    let reader = file_manager
        .open_data_file_reader(data_file.file_id)
        .unwrap();
    let mut iter = SSTIterator::with_cache_and_file(
        Box::new(reader),
        data_file.as_ref(),
        SSTIteratorOptions {
            bloom_filter_enabled: true,
            ..SSTIteratorOptions::default()
        },
        None,
    )
    .unwrap();
    assert!(Arc::ptr_eq(
        &write_metadata,
        &data_file.sst_read_metadata().unwrap()
    ));
    iter.seek_to_first().unwrap();
    let mut entries = Vec::new();
    while iter.valid() {
        let (key, mut value) = iter.current().unwrap().unwrap();
        let decoded = decode_value(&mut value, num_columns).unwrap();
        let raw = decoded
            .columns()
            .first()
            .and_then(|col| col.as_ref())
            .map(|col| Bytes::copy_from_slice(col.data()))
            .unwrap_or_default();
        entries.push((key, raw));
        iter.next().unwrap();
    }
    assert_eq!(
        entries,
        vec![
            (Bytes::from_static(b"\0\0\0a"), Bytes::from("new")),
            (Bytes::from_static(b"\0\0\0b"), Bytes::from("v1"))
        ]
    );
    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_memtable_flush_prunes_truncated_keys() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_test")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-truncation-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let lsm_tree = Arc::new(LSMTree::with_state(
        Arc::new(DbStateHandle::new()),
        Arc::clone(&metrics_manager),
    ));
    let manager = MemtableManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 256,
            buffer_count: 2,
            writer_options: WriterOptions::Sst(SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            }),
            num_columns: 1,
            write_stall_limit: 8,
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    manager.open().unwrap();

    manager
        .put(
            &RefKey::new(0, b"a"),
            &RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"va"))]),
        )
        .unwrap();
    manager
        .put(
            &RefKey::new(0, b"b"),
            &RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"vb"))]),
        )
        .unwrap();
    manager
        .put(
            &RefKey::new(0, b"c"),
            &RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"vc"))]),
        )
        .unwrap();
    manager.db_state.advance_truncation_cursor(0, 0, b"a");

    manager.flush_active().unwrap();
    let results = manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    let flush_result = results[0].as_ref().unwrap();
    assert_eq!(flush_result.data_files_by_scope.len(), 1);

    let data_file = Arc::clone(&flush_result.data_files_by_scope[0].1);
    let reader = file_manager
        .open_data_file_reader(data_file.file_id)
        .unwrap();
    let mut iter = SSTIterator::with_cache_and_file(
        Box::new(reader),
        data_file.as_ref(),
        SSTIteratorOptions {
            bloom_filter_enabled: true,
            ..SSTIteratorOptions::default()
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
    assert_eq!(
        keys,
        vec![
            Bytes::from_static(b"\0\0\0b"),
            Bytes::from_static(b"\0\0\0c")
        ]
    );
    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_memtable_flush_all_truncated_keys_produces_no_l0_file() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_test")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-truncation-empty-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let lsm_tree = Arc::new(LSMTree::with_state(
        Arc::new(DbStateHandle::new()),
        Arc::clone(&metrics_manager),
    ));
    let manager = MemtableManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 256,
            buffer_count: 2,
            num_columns: 1,
            write_stall_limit: 8,
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    manager.open().unwrap();

    manager
        .put(
            &RefKey::new(0, b"a"),
            &RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"va"))]),
        )
        .unwrap();
    manager.db_state.advance_truncation_cursor(0, 0, b"a");

    manager.flush_active().unwrap();
    let results = manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    assert!(results[0].as_ref().unwrap().data_files_by_scope.is_empty());
    assert!(lsm_tree.level_files(0).is_empty());
    assert!(manager.db_state.load().immutables.is_empty());

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_memtable_flush_with_separated_value() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_test")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let lsm_tree = Arc::new(LSMTree::with_state(
        Arc::new(DbStateHandle::new()),
        Arc::clone(&metrics_manager),
    ));
    let vlog_store = Arc::new(VlogStore::new(Arc::clone(&file_manager), 64, 8));
    let manager = MemtableManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 256,
            buffer_count: 2,
            num_columns: 1,
            write_stall_limit: 8,
            vlog_store: Some(vlog_store),
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    manager.open().unwrap();

    let key = RefKey::new(0, b"k1");
    let value = RefValue::new(vec![Some(RefColumn::new(
        ValueType::Put,
        b"value-larger-than-threshold",
    ))]);
    manager.put(&key, &value).unwrap();

    manager.flush_active().unwrap();
    let results = manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    let flush_result = results[0].as_ref().unwrap();
    assert!(flush_result.vlog_edit.is_some());

    let data_file = Arc::clone(&flush_result.data_files_by_scope[0].1);
    let reader = file_manager
        .open_data_file_reader(data_file.file_id)
        .unwrap();
    let mut iter = SSTIterator::with_cache_and_file(
        Box::new(reader),
        data_file.as_ref(),
        SSTIteratorOptions::default(),
        None,
    )
    .unwrap();
    iter.seek_to_first().unwrap();
    let (_, mut value) = iter.current().unwrap().unwrap();
    let decoded = decode_value(&mut value, 1).unwrap();
    let column = decoded.columns()[0].as_ref().unwrap();
    assert_eq!(column.value_type, ValueType::PutSeparated);
    assert_eq!(column.data().len(), 8);

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_memtable_flush_splits_l0_files_by_bucket_tree() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_test")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let lsm_tree = Arc::new(LSMTree::with_state(
        Arc::new(DbStateHandle::new()),
        Arc::clone(&metrics_manager),
    ));
    lsm_tree
        .db_state()
        .configure_multi_lsm(2, &[0u16..=0u16, 1u16..=1u16])
        .unwrap();
    let manager = MemtableManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 256,
            buffer_count: 2,
            num_columns: 1,
            write_stall_limit: 8,
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    manager.open().unwrap();

    let key_bucket0 = RefKey::new(0, b"k0");
    let key_bucket1 = RefKey::new(1, b"k1");
    let v0 = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v0"))]);
    let v1 = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v1"))]);
    manager.put(&key_bucket0, &v0).unwrap();
    manager.put(&key_bucket1, &v1).unwrap();

    manager.flush_active().unwrap();
    let results = manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    let flush_result = results[0].as_ref().unwrap();
    assert_eq!(flush_result.data_files_by_scope.len(), 2);
    assert_eq!(lsm_tree.level_files_in_tree(0, 0).len(), 1);
    assert_eq!(lsm_tree.level_files_in_tree(1, 0).len(), 1);
    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_memtable_flush_checkin_stays_in_matching_column_family_scope() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_test")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let db_state = Arc::new(DbStateHandle::new());
    let scopes = vec![
        LSMTreeScope::new(0u16..=0u16, 0),
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
        vlog_version: crate::vlog::VlogVersion::new(),
        active: None,
        immutables: std::collections::VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let lsm_tree = Arc::new(LSMTree::with_state(
        Arc::clone(&db_state),
        Arc::clone(&metrics_manager),
    ));
    let schema_manager = Arc::new(SchemaManager::new(1));
    let mut builder = schema_manager.builder();
    builder
        .add_column(0, None, None, Some("metrics".to_string()))
        .unwrap();
    let schema = builder.commit();
    let metrics_cf_id = schema.resolve_column_family_id(Some("metrics")).unwrap();
    let manager = MemtableManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 256,
            buffer_count: 2,
            num_columns: 1,
            write_stall_limit: 8,
            schema_manager: Some(Arc::clone(&schema_manager)),
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    manager.open().unwrap();

    let default_key = RefKey::new(0, b"default");
    let metrics_key = RefKey::new_with_column_family(0, metrics_cf_id, b"metric");
    let value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v"))]);
    manager.put(&default_key, &value).unwrap();
    manager.put(&metrics_key, &value).unwrap();

    manager.flush_active().unwrap();
    let results = manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    let flush_result = results[0].as_ref().unwrap();
    assert_eq!(flush_result.data_files_by_scope.len(), 2);

    let mut flushed_scopes = flush_result
        .data_files_by_scope
        .iter()
        .map(|(scope, _)| scope.clone())
        .collect::<Vec<_>>();
    flushed_scopes.sort_by_key(|scope| (scope.column_family_id, *scope.bucket_range.start()));
    assert_eq!(
        flushed_scopes,
        vec![
            LSMTreeScope::new(0u16..=0u16, 0),
            LSMTreeScope::new(0u16..=0u16, metrics_cf_id),
        ]
    );
    assert_eq!(lsm_tree.level_files_in_tree(0, 0).len(), 1);
    assert_eq!(lsm_tree.level_files_in_tree(1, 0).len(), 1);
    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_restore_active_memtable_snapshot_to_l0_stays_in_matching_column_family_scope() {
    let source_root = "/tmp/memtable_manager_restore_source";
    let target_root = "/tmp/memtable_manager_restore_target";
    let _ = std::fs::remove_dir_all(source_root);
    let _ = std::fs::remove_dir_all(target_root);

    let registry = FileSystemRegistry::new();
    let source_fs = registry
        .get_or_register(format!("file://{}", source_root))
        .unwrap();
    let target_fs = registry
        .get_or_register(format!("file://{}", target_root))
        .unwrap();
    let source_metrics = Arc::new(MetricsManager::new("memtable-restore-source"));
    let target_metrics = Arc::new(MetricsManager::new("memtable-restore-target"));
    let source_file_manager =
        Arc::new(FileManager::with_defaults(source_fs, Arc::clone(&source_metrics)).unwrap());
    let target_file_manager =
        Arc::new(FileManager::with_defaults(target_fs, Arc::clone(&target_metrics)).unwrap());
    let schema_manager = Arc::new(SchemaManager::new(1));
    let mut schema_builder = schema_manager.builder();
    schema_builder
        .add_column(0, None, None, Some("metrics".to_string()))
        .unwrap();
    let schema = schema_builder.commit();
    let metrics_cf_id = schema.resolve_column_family_id(Some("metrics")).unwrap();
    let scopes = vec![
        LSMTreeScope::new(0u16..=0u16, 0),
        LSMTreeScope::new(0u16..=0u16, metrics_cf_id),
    ];
    let multi_lsm_version = MultiLSMTreeVersion::from_scopes_with_tree_versions(
        1,
        &scopes,
        empty_lsm_versions(scopes.len()),
    )
    .unwrap();

    let source_db_state = Arc::new(DbStateHandle::new());
    source_db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: multi_lsm_version.clone(),
        vlog_version: crate::vlog::VlogVersion::new(),
        active: None,
        immutables: std::collections::VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let source_lsm_tree = Arc::new(LSMTree::with_state(
        Arc::clone(&source_db_state),
        Arc::clone(&source_metrics),
    ));
    let source_manager = MemtableManager::new(
        Arc::clone(&source_file_manager),
        Arc::clone(&source_lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 256,
            buffer_count: 2,
            num_columns: 1,
            write_stall_limit: 8,
            schema_manager: Some(Arc::clone(&schema_manager)),
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    source_manager.open().unwrap();

    let default_key = RefKey::new(0, b"default");
    let metrics_key = RefKey::new_with_column_family(0, metrics_cf_id, b"metric");
    let value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v"))]);
    source_manager.put(&default_key, &value).unwrap();
    source_manager.put(&metrics_key, &value).unwrap();

    let active = source_manager
        .db_state
        .load()
        .active
        .clone()
        .expect("source active memtable should exist");
    let snapshot_manager = SnapshotManager::new(
        Arc::clone(&source_file_manager),
        Arc::clone(&schema_manager),
        Arc::new(DbLifecycle::new_open()),
        None,
        false,
        false,
        vec![0u16..=0u16],
        Arc::new(crate::time::SystemTimeProvider),
    );
    let snapshot_write = MemtableManager::write_active_memtable_snapshot_data(
        1,
        None,
        &active,
        active
            .read()
            .unwrap()
            .readable_memtable()
            .unwrap()
            .data_offset(),
        &snapshot_manager,
        &source_file_manager,
    )
    .unwrap();

    let target_db_state = Arc::new(DbStateHandle::new());
    target_db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version,
        vlog_version: crate::vlog::VlogVersion::new(),
        active: None,
        immutables: std::collections::VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let target_lsm_tree = Arc::new(LSMTree::with_state(
        Arc::clone(&target_db_state),
        Arc::clone(&target_metrics),
    ));
    let target_vlog_store = Arc::new(VlogStore::new(Arc::clone(&target_file_manager), 64, 8));
    let target_manager = MemtableManager::new(
        Arc::clone(&target_file_manager),
        Arc::clone(&target_lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 256,
            buffer_count: 2,
            num_columns: 1,
            write_stall_limit: 8,
            schema_manager: Some(Arc::clone(&schema_manager)),
            vlog_store: Some(target_vlog_store),
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    target_manager.open().unwrap();

    let restored = target_manager
        .restore_active_memtable_snapshot_to_l0(&source_file_manager, &snapshot_write.active_data)
        .unwrap();

    assert!(restored);
    assert_eq!(target_lsm_tree.level_files_in_tree(0, 0).len(), 1);
    assert_eq!(target_lsm_tree.level_files_in_tree(1, 0).len(), 1);

    let _ = std::fs::remove_dir_all(source_root);
    let _ = std::fs::remove_dir_all(target_root);
}

#[test]
#[serial_test::serial(file)]
fn test_restore_active_memtable_snapshot_to_l0_preserves_shared_vlog_across_scopes() {
    let source_root = "/tmp/memtable_manager_restore_vlog_source";
    let target_root = "/tmp/memtable_manager_restore_vlog_target";
    let _ = std::fs::remove_dir_all(source_root);
    let _ = std::fs::remove_dir_all(target_root);

    let registry = FileSystemRegistry::new();
    let source_fs = registry
        .get_or_register(format!("file://{}", source_root))
        .unwrap();
    let target_fs = registry
        .get_or_register(format!("file://{}", target_root))
        .unwrap();
    let source_metrics = Arc::new(MetricsManager::new("memtable-restore-vlog-source"));
    let target_metrics = Arc::new(MetricsManager::new("memtable-restore-vlog-target"));
    let source_file_manager =
        Arc::new(FileManager::with_defaults(source_fs, Arc::clone(&source_metrics)).unwrap());
    let target_file_manager =
        Arc::new(FileManager::with_defaults(target_fs, Arc::clone(&target_metrics)).unwrap());
    let schema_manager = Arc::new(SchemaManager::new(1));
    let mut schema_builder = schema_manager.builder();
    schema_builder
        .add_column(0, None, None, Some("metrics".to_string()))
        .unwrap();
    let schema = schema_builder.commit();
    let metrics_cf_id = schema.resolve_column_family_id(Some("metrics")).unwrap();
    let scopes = vec![
        LSMTreeScope::new(0u16..=0u16, 0),
        LSMTreeScope::new(0u16..=0u16, metrics_cf_id),
    ];
    let multi_lsm_version = MultiLSMTreeVersion::from_scopes_with_tree_versions(
        1,
        &scopes,
        empty_lsm_versions(scopes.len()),
    )
    .unwrap();

    let source_db_state = Arc::new(DbStateHandle::new());
    source_db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: multi_lsm_version.clone(),
        vlog_version: crate::vlog::VlogVersion::new(),
        active: None,
        immutables: std::collections::VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let source_lsm_tree = Arc::new(LSMTree::with_state(
        Arc::clone(&source_db_state),
        Arc::clone(&source_metrics),
    ));
    let source_vlog_store = Arc::new(VlogStore::new(Arc::clone(&source_file_manager), 64, 8));
    let source_manager = MemtableManager::new(
        Arc::clone(&source_file_manager),
        Arc::clone(&source_lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 256,
            buffer_count: 2,
            num_columns: 1,
            write_stall_limit: 8,
            schema_manager: Some(Arc::clone(&schema_manager)),
            vlog_store: Some(source_vlog_store),
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    source_manager.open().unwrap();

    let long_value = RefValue::new(vec![Some(RefColumn::new(
        ValueType::Put,
        b"value-larger-than-threshold",
    ))]);
    let default_key = RefKey::new(0, b"default");
    let metrics_key = RefKey::new_with_column_family(0, metrics_cf_id, b"metric");
    source_manager.put(&default_key, &long_value).unwrap();
    source_manager.put(&metrics_key, &long_value).unwrap();

    let active = source_manager
        .db_state
        .load()
        .active
        .clone()
        .expect("source active memtable should exist");
    let snapshot_manager = SnapshotManager::new(
        Arc::clone(&source_file_manager),
        Arc::clone(&schema_manager),
        Arc::new(DbLifecycle::new_open()),
        None,
        false,
        false,
        vec![0u16..=0u16],
        Arc::new(crate::time::SystemTimeProvider),
    );
    let snapshot_write = MemtableManager::write_active_memtable_snapshot_data(
        1,
        None,
        &active,
        active
            .read()
            .unwrap()
            .readable_memtable()
            .unwrap()
            .data_offset(),
        &snapshot_manager,
        &source_file_manager,
    )
    .unwrap();

    let target_db_state = Arc::new(DbStateHandle::new());
    target_db_state.store(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version,
        vlog_version: crate::vlog::VlogVersion::new(),
        active: None,
        immutables: std::collections::VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });
    let target_lsm_tree = Arc::new(LSMTree::with_state(
        Arc::clone(&target_db_state),
        Arc::clone(&target_metrics),
    ));
    let target_vlog_store = Arc::new(VlogStore::new(Arc::clone(&target_file_manager), 64, 8));
    let target_manager = MemtableManager::new(
        Arc::clone(&target_file_manager),
        Arc::clone(&target_lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 256,
            buffer_count: 2,
            num_columns: 1,
            write_stall_limit: 8,
            schema_manager: Some(Arc::clone(&schema_manager)),
            vlog_store: Some(target_vlog_store),
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    target_manager.open().unwrap();

    let restored = target_manager
        .restore_active_memtable_snapshot_to_l0(&source_file_manager, &snapshot_write.active_data)
        .unwrap();

    assert!(restored);
    let tree0_files = target_lsm_tree.level_files_in_tree(0, 0);
    let tree1_files = target_lsm_tree.level_files_in_tree(1, 0);
    assert_eq!(tree0_files.len(), 1);
    assert_eq!(tree1_files.len(), 1);
    assert!(tree0_files[0].has_separated_values());
    assert!(tree1_files[0].has_separated_values());
    assert_eq!(tree0_files[0].vlog_file_seq_offset, 0);
    assert_eq!(tree1_files[0].vlog_file_seq_offset, 0);

    let files_with_entries = target_db_state.load().vlog_version.files_with_entries();
    assert_eq!(files_with_entries.len(), 1);
    assert_eq!(files_with_entries[0].2, 2);

    let _ = std::fs::remove_dir_all(source_root);
    let _ = std::fs::remove_dir_all(target_root);
}

#[test]
#[serial_test::serial(file)]
fn test_memtable_schema_change_triggers_flush_and_preserves_flush_schema() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_test")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let lsm_tree = Arc::new(LSMTree::with_state(
        Arc::new(DbStateHandle::new()),
        Arc::clone(&metrics_manager),
    ));
    let schema_manager = Arc::new(SchemaManager::new(1));
    let manager = MemtableManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 256,
            buffer_count: 2,
            num_columns: 1,
            write_stall_limit: 8,
            schema_manager: Some(Arc::clone(&schema_manager)),
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    manager.open().unwrap();

    let key1 = RefKey::new(0, b"k1");
    let value1 = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v1"))]);
    manager.put(&key1, &value1).unwrap();

    let mut builder = schema_manager.builder();
    builder.add_column(1, None, None, None).unwrap();
    let _ = builder.commit();

    let key2 = RefKey::new(0, b"k2");
    let value2 = RefValue::new(vec![
        Some(RefColumn::new(ValueType::Put, b"v2")),
        Some(RefColumn::new(ValueType::Put, b"v2c1")),
    ]);
    manager.put(&key2, &value2).unwrap();

    let results = manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].as_ref().unwrap().data_files_by_scope[0]
            .1
            .schema_id,
        0
    );

    manager.flush_active().unwrap();
    let results = manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    let second_file = results[0].as_ref().unwrap().data_files_by_scope[0]
        .1
        .clone();
    assert_eq!(second_file.schema_id, 1);
    let reader = file_manager
        .open_data_file_reader(second_file.file_id)
        .unwrap();
    let mut iter = SSTIterator::with_cache_and_file(
        Box::new(reader),
        second_file.as_ref(),
        SSTIteratorOptions {
            num_columns: 2,
            ..Default::default()
        },
        None,
    )
    .unwrap();
    iter.seek_to_first().unwrap();
    let (_, mut value) = iter.current().unwrap().unwrap();
    let decoded = decode_value(&mut value, 2).unwrap();
    assert_eq!(decoded.columns().len(), 2);
    assert_eq!(
        decoded.columns()[0].as_ref().unwrap().data().as_ref(),
        b"v2"
    );
    assert_eq!(
        decoded.columns()[1].as_ref().unwrap().data().as_ref(),
        b"v2c1"
    );

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_memtable_reuses_buffer_after_flush() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_test")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let lsm_tree = Arc::new(crate::lsm::LSMTree::with_state(
        Arc::new(crate::db_state::DbStateHandle::new()),
        Arc::clone(&metrics_manager),
    ));
    let manager = MemtableManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 256,
            buffer_count: 2,
            writer_options: WriterOptions::Sst(SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            }),
            num_columns: 1,
            write_stall_limit: 8,
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    manager.open().unwrap();

    let key1 = RefKey::new(0, b"k1");
    let v1 = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v1"))]);
    manager.put(&key1, &v1).unwrap();
    manager.flush_active().unwrap();
    let results = manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    assert_eq!(lsm_tree.level_files(0).len(), 1);

    let key2 = RefKey::new(0, b"k2");
    let v2 = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v2"))]);
    manager.put(&key2, &v2).unwrap();
    manager.flush_active().unwrap();
    let results = manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    assert_eq!(lsm_tree.level_files(0).len(), 2);
    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_vec_memtable_triggers_flush_on_full() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_test")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let lsm_tree = Arc::new(crate::lsm::LSMTree::with_state(
        Arc::new(crate::db_state::DbStateHandle::new()),
        Arc::clone(&metrics_manager),
    ));
    let manager = MemtableManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 192,
            buffer_count: 2,
            memtable_type: MemtableType::Vec,
            writer_options: WriterOptions::Sst(SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            }),
            num_columns: 1,
            write_stall_limit: 8,
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    manager.open().unwrap();

    let key1 = RefKey::new(0, b"k1");
    let key2 = RefKey::new(0, b"k2");
    let large_value = vec![b'v'; 96];
    let value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, &large_value))]);
    manager.put(&key1, &value).unwrap();
    manager.put(&key2, &value).unwrap();

    let results = manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    assert_eq!(lsm_tree.level_files(0).len(), 1);
    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_oversized_put_ref_uses_special_vec_memtable_and_can_overcommit_budget() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_test")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let lsm_tree = Arc::new(crate::lsm::LSMTree::with_state(
        Arc::new(crate::db_state::DbStateHandle::new()),
        Arc::clone(&metrics_manager),
    ));
    let manager = MemtableManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 128,
            buffer_count: 1,
            memtable_type: MemtableType::Hash,
            num_columns: 1,
            write_stall_limit: 8,
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    manager.open().unwrap();

    let key = RefKey::new(0, b"k1");
    let big_value = vec![b'x'; 1024];
    let value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, &big_value))]);
    manager.put(&key, &value).unwrap();
    manager.flush_active().unwrap();

    let results = manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    assert_eq!(lsm_tree.level_files(0).len(), 1);

    let state = manager.state.lock().unwrap();
    assert_eq!(state.in_flight, 0);
    assert_eq!(state.budget, 0);
    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn stale_capacity_full_request_does_not_rotate_the_replacement_memtable() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_test")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-stale-rotation-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let lsm_tree = Arc::new(crate::lsm::LSMTree::with_state(
        Arc::new(crate::db_state::DbStateHandle::new()),
        Arc::clone(&metrics_manager),
    ));
    let manager = MemtableManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 256,
            buffer_count: 3,
            memtable_type: MemtableType::Vec,
            num_columns: 1,
            write_stall_limit: 8,
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    manager.open().unwrap();

    let key = RefKey::new(0, b"key");
    let value_bytes = vec![b'v'; 64];
    let value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, &value_bytes))]);
    manager.put(&key, &value).unwrap();
    let old_active_id = manager
        .db_state
        .load()
        .active
        .as_ref()
        .unwrap()
        .read()
        .unwrap()
        .id;

    assert_eq!(
        manager
            .flush_active_for_capacity_full(old_active_id)
            .unwrap(),
        Some(old_active_id)
    );
    let replacement_id = manager
        .db_state
        .load()
        .active
        .as_ref()
        .unwrap()
        .read()
        .unwrap()
        .id;
    assert_ne!(replacement_id, old_active_id);

    assert_eq!(
        manager
            .flush_active_for_capacity_full(old_active_id)
            .unwrap(),
        None
    );
    assert_eq!(
        manager
            .db_state
            .load()
            .active
            .as_ref()
            .unwrap()
            .read()
            .unwrap()
            .id,
        replacement_id
    );
    assert_eq!(manager.wait_for_flushes().len(), 1);
    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_skiplist_memtable_triggers_flush_on_full() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/memtable_manager_test")
        .unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
    let file_manager =
        Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
    let lsm_tree = Arc::new(crate::lsm::LSMTree::with_state(
        Arc::new(crate::db_state::DbStateHandle::new()),
        Arc::clone(&metrics_manager),
    ));
    let manager = MemtableManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&lsm_tree),
        MemtableManagerOptions {
            memtable_capacity: 224,
            buffer_count: 2,
            memtable_type: MemtableType::Skiplist,
            writer_options: WriterOptions::Sst(SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            }),
            num_columns: 1,
            write_stall_limit: 8,
            ..MemtableManagerOptions::default()
        },
    )
    .unwrap();
    manager.open().unwrap();

    let key1 = RefKey::new(0, b"k1");
    let key2 = RefKey::new(0, b"k2");
    let large_value = vec![b'v'; 96];
    let value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, &large_value))]);
    manager.put(&key1, &value).unwrap();
    manager.put(&key2, &value).unwrap();

    let results = manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    assert_eq!(lsm_tree.level_files(0).len(), 1);
    cleanup_test_root();
}

impl MemtableManager {
    /// Returns whether adaptive evaluation is currently active.
    #[inline]
    pub(crate) fn adaptive_enabled(&self) -> bool {
        self.adaptive_controller.is_enabled()
    }

    pub(crate) fn active_memtable_type(&self) -> Option<MemtableType> {
        let snapshot = self.db_state.load();
        let active = snapshot.active.as_ref()?.read().unwrap();
        active.readable_memtable().map(MemtableImpl::memtable_type)
    }
    pub(crate) fn wait_for_active_memtable_type(&self) -> Result<MemtableType> {
        let mut state = self.state.lock().unwrap();
        loop {
            self.db_lifecycle.ensure_open()?;
            if let Some(memtable_type) = self.active_memtable_type() {
                return Ok(memtable_type);
            }
            state = self.buffer_ready.wait(state).unwrap();
        }
    }
    pub(crate) fn target_memtable_type(&self) -> MemtableType {
        self.state.lock().unwrap().memtable_type
    }
    pub(crate) fn wait_for_flushes(&self) -> Vec<Result<MemtableFlushResult>> {
        let mut state = self.state.lock().unwrap();
        while state.in_flight > 0 {
            state = self.flush_done.wait(state).unwrap();
        }
        std::mem::take(&mut state.flush_results)
    }
}
