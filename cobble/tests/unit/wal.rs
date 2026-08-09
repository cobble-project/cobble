use super::*;
use crate::r#type::{RefColumn, ValueType};
use crate::{DbBuilder, VolumeDescriptor, WriteBatch, WriteOptions};
use std::sync::{Arc, Barrier, mpsc};

#[test]
fn segments_round_trip_and_list_in_wal_id_order() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.wal_enabled = true;
    config.volumes = vec![VolumeDescriptor::new(
        format!("file://{}", temp.path().display()),
        vec![VolumeUsageKind::Wal],
    )];
    let store = WalStore::open(&config, "shard-1", &FileSystemRegistry::new())
        .unwrap()
        .unwrap();

    let data = WalSegment::data_from_entries(
        9,
        4,
        vec![
            (Bytes::from_static(b"key-1"), Bytes::from_static(b"value-1")),
            (Bytes::from_static(b"key-2"), Bytes::from_static(b"value-2")),
        ],
    );
    let cursor = WalSegment::TruncationCursor {
        wal_id: 3,
        edits: vec![WalTruncationCursor {
            bucket: 7,
            column_family_id: 2,
            key: Bytes::from_static(b"cursor"),
        }],
    };

    store.publish(&data).unwrap();
    store.publish(&cursor).unwrap();

    assert_eq!(store.list().unwrap(), vec![3, 9]);
    assert_eq!(store.read(3).unwrap(), cursor);
    assert_eq!(store.read(9).unwrap(), data);
}

#[test]
fn writer_groups_concurrent_writes_and_publishes_batches_and_cursors() {
    let temp = tempfile::tempdir().unwrap();
    let root = format!("file://{}", temp.path().display());
    let mut config = Config::default();
    config.wal_enabled = true;
    config.wal_flush_interval_ms = 25;
    config.volumes = vec![VolumeDescriptor::new(
        root,
        vec![
            VolumeUsageKind::Meta,
            VolumeUsageKind::PrimaryDataPriorityHigh,
            VolumeUsageKind::Wal,
        ],
    )];
    let db = Arc::new(
        DbBuilder::new(config.clone())
            .bucket_ranges(vec![0..=0])
            .db_id("wal-writer")
            .open()
            .unwrap(),
    );
    let barrier = Arc::new(Barrier::new(5));
    let mut writers = Vec::new();
    for i in 0..4 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        writers.push(std::thread::spawn(move || {
            barrier.wait();
            db.put(0, format!("concurrent-{i}").as_bytes(), 0, b"value")
        }));
    }
    barrier.wait();
    for writer in writers {
        writer.join().unwrap().unwrap();
    }

    db.put_column_batch_with_options(
        0,
        0,
        [(b"batch-a".as_slice(), b"a".as_slice()), (b"batch-b", b"b")],
        &crate::WriteOptions::default(),
    )
    .unwrap();
    let mut batch = WriteBatch::new();
    batch.put(0, b"write-batch-a", 0, b"a");
    batch.put(0, b"write-batch-b", 0, b"b");
    db.write_batch(batch).unwrap();
    assert!(db.get(0, b"concurrent-0").unwrap().is_some());
    db.advance_truncation_cursor_by_id(0, 0, b"before-cursor")
        .unwrap();

    let store = WalStore::open(&config, "wal-writer", &FileSystemRegistry::new())
        .unwrap()
        .unwrap();
    let ids = store.list().unwrap();
    assert!(!ids.is_empty());
    assert!(ids.windows(2).all(|ids| ids[0] < ids[1]));
    let mut data_entries = 0;
    let mut max_data_segment_entry_count = 0;
    let mut cursors = 0;
    for id in ids {
        match store.read(id).unwrap() {
            WalSegment::Data { entry_count, .. } => {
                data_entries += entry_count;
                max_data_segment_entry_count = max_data_segment_entry_count.max(entry_count);
            }
            WalSegment::TruncationCursor { edits, .. } => cursors += edits.len(),
        }
    }
    assert_eq!(data_entries, 8);
    assert!(max_data_segment_entry_count >= 4);
    assert_eq!(cursors, 1);

    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let snapshot_barrier = Arc::new(Barrier::new(3));
    let mut snapshots = Vec::new();
    for _ in 0..2 {
        let db = Arc::clone(&db);
        let snapshot_tx = snapshot_tx.clone();
        let snapshot_barrier = Arc::clone(&snapshot_barrier);
        snapshots.push(std::thread::spawn(move || {
            snapshot_barrier.wait();
            db.snapshot_with_callback(move |result| {
                snapshot_tx.send(result).unwrap();
            })
            .unwrap()
        }));
    }
    snapshot_barrier.wait();
    let snapshot_ids = snapshots
        .into_iter()
        .map(|snapshot| snapshot.join().unwrap())
        .collect::<Vec<_>>();
    for _ in &snapshot_ids {
        snapshot_rx.recv().unwrap().unwrap();
    }
    assert!(store.list().unwrap().is_empty());

    let file_manager = Arc::new(
        FileManager::from_config(
            &config,
            "wal-writer",
            Arc::new(crate::metrics_manager::MetricsManager::new(
                "wal-writer-check",
            )),
        )
        .unwrap(),
    );
    let checkpoint_id = snapshot_ids
        .into_iter()
        .map(|snapshot_id| {
            crate::snapshot::load_manifest_for_snapshot(&file_manager, snapshot_id)
                .unwrap()
                .wal_checkpoint_id
        })
        .max()
        .unwrap();
    assert!(checkpoint_id > 0);

    db.put(0, b"after-snapshot", 0, b"value").unwrap();
    db.close().unwrap();
    let ids = store.list().unwrap();
    assert!(ids.iter().all(|id| *id > checkpoint_id));

    let resumed = crate::Db::resume(config.clone(), "wal-writer").unwrap();
    assert!(resumed.get(0, b"concurrent-0").unwrap().is_some());
    resumed.put(0, b"after-resume", 0, b"value").unwrap();
    resumed.close().unwrap();
    assert!(store.list().unwrap().iter().all(|id| *id > checkpoint_id));
}

#[test]
fn abandoned_batch_discards_its_uncommitted_wal_append() {
    let temp = tempfile::tempdir().unwrap();
    let config = Config {
        wal_enabled: true,
        volumes: vec![VolumeDescriptor::new(
            format!("file://{}", temp.path().display()),
            vec![
                VolumeUsageKind::Meta,
                VolumeUsageKind::PrimaryDataPriorityHigh,
                VolumeUsageKind::Wal,
            ],
        )],
        ..Config::default()
    };
    let file_manager = Arc::new(
        FileManager::from_config(
            &config,
            "abandoned-batch",
            Arc::new(crate::metrics_manager::MetricsManager::new(
                "abandoned-batch",
            )),
        )
        .unwrap(),
    );
    let lifecycle = Arc::new(DbLifecycle::new_open());
    let writer = WalWriter::open(
        &config,
        "abandoned-batch",
        file_manager,
        Arc::new(SchemaManager::new(1)),
        lifecycle,
        0,
    )
    .unwrap();

    let key = RefKey::new(0, b"key");
    let value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"value"))]);
    {
        let mut guard = writer.lock_for_schema(0).unwrap();
        {
            let mut batch = guard.begin_batch();
            batch.append_ref(0, &key, &value, 1);
        }
        assert!(guard.state.current.is_none());
        assert_eq!(guard.state.next_wal_id, 1);
    }
    writer.close().unwrap();
}

#[test]
fn disabled_writer_keeps_wal_directory_absent() {
    let temp = tempfile::tempdir().unwrap();
    let root = format!("file://{}", temp.path().display());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(root),
        ..Config::default()
    };
    let db = DbBuilder::new(config)
        .bucket_ranges(vec![0..=0])
        .db_id("wal-disabled")
        .open()
        .unwrap();
    db.put(0, b"key", 0, b"value").unwrap();
    assert!(db.get(0, b"key").unwrap().is_some());
    db.close().unwrap();
    assert!(!temp.path().join("wal-disabled/wal").exists());
}

#[test]
fn wal_async_writes_snapshot_barriers_and_resume_replay() {
    let temp = tempfile::tempdir().unwrap();
    let root = format!("file://{}", temp.path().display());
    let config = Config {
        num_columns: 2,
        wal_enabled: true,
        wal_flush_interval_ms: 1_000,
        volumes: vec![VolumeDescriptor::new(
            root,
            vec![
                VolumeUsageKind::Meta,
                VolumeUsageKind::PrimaryDataPriorityHigh,
                VolumeUsageKind::Wal,
            ],
        )],
        ..Config::default()
    };
    let async_write = WriteOptions::default().with_await_durable(false);
    let store = WalStore::open(&config, "wal-replay", &FileSystemRegistry::new())
        .unwrap()
        .unwrap();

    let db = DbBuilder::new(config.clone())
        .bucket_ranges(vec![0..=0])
        .db_id("wal-replay")
        .open()
        .unwrap();
    db.put_with_options(0, b"initial", 0, b"value", &async_write)
        .unwrap();
    assert!(db.get(0, b"initial").unwrap().is_some());
    assert!(store.list().unwrap().is_empty());
    db.put(0, b"initial-durable", 0, b"value").unwrap();
    assert!(!store.list().unwrap().is_empty());
    db.force_close();

    // A published final WAL segment restores a DB that has no user-created snapshot yet.
    let db = crate::Db::resume(config.clone(), "wal-replay").unwrap();
    assert!(db.get(0, b"initial").unwrap().is_some());
    assert!(db.get(0, b"initial-durable").unwrap().is_some());

    // Durable writes wait for publication, whereas async writes remain visible without waiting.
    db.put(0, b"checkpointed", 0, b"before-snapshot").unwrap();
    assert!(!store.list().unwrap().is_empty());
    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let snapshot_id = db
        .snapshot_with_callback(move |result| snapshot_tx.send(result).unwrap())
        .unwrap();
    snapshot_rx.recv().unwrap().unwrap();
    assert!(store.list().unwrap().is_empty());

    db.put_with_options(0, b"snapshot-tail", 0, b"visible-now", &async_write)
        .unwrap();
    assert!(db.get(0, b"snapshot-tail").unwrap().is_some());
    assert!(store.list().unwrap().is_empty());
    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let checkpoint_snapshot_id = db
        .snapshot_with_callback(move |result| snapshot_tx.send(result).unwrap())
        .unwrap();
    snapshot_rx.recv().unwrap().unwrap();
    assert!(store.list().unwrap().is_empty());

    let file_manager = Arc::new(
        FileManager::from_config(
            &config,
            "wal-replay",
            Arc::new(crate::metrics_manager::MetricsManager::new(
                "wal-replay-check",
            )),
        )
        .unwrap(),
    );
    assert!(
        crate::snapshot::load_manifest_for_snapshot(&file_manager, checkpoint_snapshot_id)
            .unwrap()
            .wal_checkpoint_id
            > crate::snapshot::load_manifest_for_snapshot(&file_manager, snapshot_id)
                .unwrap()
                .wal_checkpoint_id
    );

    // This schema is newer than the checkpoint manifest. WAL publication persists it before
    // the data segment, and recovery must load it through the schema manager.
    let mut schema = db.update_schema();
    schema
        .add_column(0, None, None, Some("tail-family".to_string()))
        .unwrap();
    schema.commit();
    db.put_with_options(
        0,
        b"schema-tail",
        0,
        b"schema-value",
        &WriteOptions::with_column_family("tail-family").with_await_durable(false),
    )
    .unwrap();

    // One entry is larger than the WAL target. The column batch must still occupy one segment.
    let large = vec![7u8; 5 * 1024 * 1024];
    db.put_column_batch_with_options(
        0,
        0,
        [
            (b"large".as_slice(), large.as_slice()),
            (b"small", b"value"),
        ],
        &async_write,
    )
    .unwrap();
    db.delete_with_options(0, b"deleted", 0, &async_write)
        .unwrap();
    db.merge_with_options(0, b"merged", 1, b"tail", &async_write)
        .unwrap();
    db.advance_truncation_cursor_by_id(0, 0, b"after-tail")
        .unwrap();

    let data_counts = store
        .list()
        .unwrap()
        .into_iter()
        .filter_map(|wal_id| match store.read(wal_id).unwrap() {
            WalSegment::Data { entry_count, .. } => Some(entry_count),
            WalSegment::TruncationCursor { .. } => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(data_counts, vec![3, 2]);
    db.force_close();

    let resumed = crate::Db::resume(config.clone(), "wal-replay").unwrap();
    assert!(resumed.get(0, b"large").unwrap().is_some());
    assert!(
        resumed
            .get_with_options(
                0,
                b"schema-tail",
                &crate::ReadOptions::default().with_column_family("tail-family"),
            )
            .unwrap()
            .is_some()
    );
    assert_eq!(
        resumed.truncation_cursor_by_id(0, 0).unwrap(),
        Some(b"after-tail".to_vec())
    );

    // `resume` includes the durable WAL tail, while opening a named snapshot is exact.
    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let exact_snapshot_id = resumed
        .snapshot_with_callback(move |result| snapshot_tx.send(result).unwrap())
        .unwrap();
    snapshot_rx.recv().unwrap().unwrap();
    resumed
        .put(0, b"excluded-by-snapshot-only", 0, b"wal-tail")
        .unwrap();
    resumed.force_close();

    let manifests_before_exact =
        crate::snapshot::list_snapshot_manifest_ids(&file_manager).unwrap();
    let exact = crate::Db::open_from_snapshot(config, exact_snapshot_id, "wal-replay").unwrap();
    assert!(exact.get(0, b"large").unwrap().is_some());
    assert!(
        exact
            .get(0, b"excluded-by-snapshot-only")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        crate::snapshot::list_snapshot_manifest_ids(&file_manager).unwrap(),
        manifests_before_exact
    );
    exact.close().unwrap();
}

#[test]
fn recovery_modes_control_wal_replay_without_creating_snapshots() {
    let temp = tempfile::tempdir().unwrap();
    let root = format!("file://{}", temp.path().display());
    let wal_config = Config {
        wal_enabled: true,
        wal_flush_interval_ms: 1,
        volumes: vec![VolumeDescriptor::new(
            root.clone(),
            vec![
                VolumeUsageKind::Meta,
                VolumeUsageKind::PrimaryDataPriorityHigh,
                VolumeUsageKind::Wal,
            ],
        )],
        ..Config::default()
    };
    let db_id = "wal-recovery-modes";
    let db = DbBuilder::new(wal_config.clone())
        .bucket_ranges(vec![0..=0])
        .db_id(db_id)
        .open()
        .unwrap();
    let snapshot_and_wait = |db: &crate::Db| {
        let (tx, rx) = mpsc::channel();
        let snapshot_id = db
            .snapshot_with_callback(move |result| tx.send(result).unwrap())
            .unwrap();
        rx.recv().unwrap().unwrap();
        snapshot_id
    };
    db.put(0, b"old", 0, b"old-value").unwrap();
    let old_snapshot_id = snapshot_and_wait(&db);
    db.put(0, b"latest", 0, b"latest-value").unwrap();
    let latest_snapshot_id = snapshot_and_wait(&db);
    db.put(0, b"wal-tail", 0, b"tail-value").unwrap();
    db.force_close();

    let store = WalStore::open(&wal_config, db_id, &FileSystemRegistry::new())
        .unwrap()
        .unwrap();
    let replayed_wal_id = *store.list().unwrap().last().unwrap();

    let file_manager = Arc::new(
        FileManager::from_config(
            &wal_config,
            db_id,
            Arc::new(crate::metrics_manager::MetricsManager::new(db_id)),
        )
        .unwrap(),
    );
    let manifests_before = crate::snapshot::list_snapshot_manifest_ids(&file_manager).unwrap();
    assert_eq!(manifests_before.len(), 3);
    assert_eq!(manifests_before[1..], [old_snapshot_id, latest_snapshot_id]);

    // A non-latest selected snapshot never receives the tail, even when WAL replay is requested.
    let historical = crate::Db::open_from_snapshot_with_recovery_mode(
        wal_config.clone(),
        old_snapshot_id,
        db_id,
        crate::RecoveryMode::LatestWithWal,
    )
    .unwrap();
    assert!(historical.get(0, b"latest").unwrap().is_none());
    assert!(historical.get(0, b"wal-tail").unwrap().is_none());
    historical.force_close();

    // Current writes can disable WAL and omit its usage tag while recovery follows the route in
    // the selected manifest.
    let recovery_config = Config {
        wal_enabled: false,
        volumes: vec![VolumeDescriptor::new(
            root.clone(),
            vec![
                VolumeUsageKind::Meta,
                VolumeUsageKind::PrimaryDataPriorityHigh,
            ],
        )],
        ..wal_config.clone()
    };
    let route_change_config = Config {
        wal_enabled: true,
        volumes: vec![
            VolumeDescriptor::new(
                root,
                vec![
                    VolumeUsageKind::Meta,
                    VolumeUsageKind::PrimaryDataPriorityHigh,
                ],
            ),
            VolumeDescriptor::new("file:///tmp/wal-recovery-other", vec![VolumeUsageKind::Wal]),
        ],
        ..wal_config.clone()
    };
    assert!(matches!(
        crate::Db::open_from_snapshot_with_recovery_mode(
            route_change_config,
            latest_snapshot_id,
            db_id,
            crate::RecoveryMode::LatestWithWal,
        ),
        Err(crate::Error::ConfigError(_))
    ));
    let latest = crate::Db::open_from_snapshot_with_recovery_mode(
        recovery_config.clone(),
        latest_snapshot_id,
        db_id,
        crate::RecoveryMode::LatestWithWal,
    )
    .unwrap();
    assert!(latest.get(0, b"wal-tail").unwrap().is_some());
    latest.force_close();

    // A second recovery still discovers the WAL route from the selected manifest.
    let latest_again = crate::Db::open_from_snapshot_with_recovery_mode(
        recovery_config.clone(),
        latest_snapshot_id,
        db_id,
        crate::RecoveryMode::LatestWithWal,
    )
    .unwrap();
    assert!(latest_again.get(0, b"wal-tail").unwrap().is_some());
    let recovery_checkpoint_snapshot_id = snapshot_and_wait(&latest_again);
    let recovery_checkpoint_manifest =
        crate::snapshot::load_manifest_for_snapshot(&file_manager, recovery_checkpoint_snapshot_id)
            .unwrap();
    assert_eq!(
        recovery_checkpoint_manifest.wal_checkpoint_id,
        replayed_wal_id
    );
    assert!(recovery_checkpoint_manifest.wal_volume.is_none());
    assert!(store.list().unwrap().is_empty());
    latest_again.force_close();
    assert_eq!(
        crate::snapshot::list_snapshot_manifest_ids(&file_manager)
            .unwrap()
            .len(),
        manifests_before.len() + 1
    );

    // The checkpoint snapshot contains the recovered tail and leaves no WAL tail to replay.
    let recovered_after_checkpoint = crate::Db::open_from_snapshot_with_recovery_mode(
        recovery_config.clone(),
        recovery_checkpoint_snapshot_id,
        db_id,
        crate::RecoveryMode::LatestWithWal,
    )
    .unwrap();
    assert!(
        recovered_after_checkpoint
            .get(0, b"wal-tail")
            .unwrap()
            .is_some()
    );
    recovered_after_checkpoint.force_close();

    let snapshot_only = crate::Db::open_from_snapshot_with_recovery_mode(
        wal_config,
        latest_snapshot_id,
        db_id,
        crate::RecoveryMode::SnapshotOnly,
    )
    .unwrap();
    assert!(snapshot_only.get(0, b"wal-tail").unwrap().is_none());
    snapshot_only.close().unwrap();
    assert_eq!(
        crate::snapshot::list_snapshot_manifest_ids(&file_manager).unwrap(),
        vec![
            manifests_before[0],
            old_snapshot_id,
            latest_snapshot_id,
            recovery_checkpoint_snapshot_id,
        ]
    );
}
