use super::*;
use crate::{DbBuilder, VolumeDescriptor, WriteBatch};
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
