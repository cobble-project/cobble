use super::*;
use crate::MergeOperator;
use crate::db_state::full_bucket_range;
use crate::file::{File, SequentialWriteFile};
use crate::paths::{GOVERNANCE_MANIFEST_POINTER_NAME, snapshot_active_data_relative_path};
use crate::snapshot::SnapshotLifecycleState;
use crate::r#type::encode_merge_separated_array;
use crate::{
    CompactionMode, DbBuilder, DbGovernance, GovernanceMode, MemtableType, ReadOptions,
    RuntimeManifestMode, ScanOptions, TimeProviderKind, U32CounterMergeOperator,
    U64CounterMergeOperator, VolumeDescriptor, VolumeUsageKind, WriteOptions,
};
use bytes::BytesMut;
use serial_test::serial;
use size::Size;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::time::Duration;

fn cleanup_test_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn config_with_small_memtable(path: &str) -> Config {
    Config {
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        sst_bloom_filter_enabled: true,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", path)),
        ..Config::default()
    }
}

fn open_db(config: Config) -> Db {
    let total_buckets = config.total_buckets;
    Db::open(
        config,
        std::iter::once(full_bucket_range(total_buckets)).collect(),
    )
    .unwrap()
}

fn runtime_manifest_store(db: &Db) -> crate::runtime_manifest::RuntimeManifestStore {
    crate::runtime_manifest::RuntimeManifestStore::new(Arc::clone(&db.file_manager))
}

#[test]
#[serial(file)]
fn writer_persists_plain_db_properties_without_volume_credentials() {
    let root = "/tmp/db_writer_properties";
    cleanup_test_root(root);
    let mut volume = VolumeDescriptor::single_volume(format!("file://{root}")).remove(0);
    volume.access_id = Some("writer-ak".to_string());
    volume.secret_key = Some("writer-sk".to_string());
    let config = Config {
        volumes: vec![volume],
        l0_file_limit: 9,
        ..Config::default()
    };
    let db = DbBuilder::new(config.clone())
        .bucket_ranges(vec![0..=0])
        .db_id("properties-shard")
        .open()
        .unwrap();
    let properties_path = format!("{root}/properties-shard/PROPERTIES");
    let contents = std::fs::read_to_string(&properties_path).unwrap();
    let parsed: toml::Value = toml::from_str(&contents).unwrap();

    assert_eq!(parsed["db_id"].as_str(), Some("properties-shard"));
    assert_eq!(parsed["config"]["l0_file_limit"].as_integer(), Some(9));
    assert!(parsed["config"]["volumes"][0].get("access_id").is_none());
    assert!(parsed["config"]["volumes"][0].get("secret_key").is_none());
    assert!(!contents.contains("writer-ak"));
    assert!(!contents.contains("writer-sk"));

    db.close().unwrap();

    let mut restarted_config = config;
    restarted_config.l0_file_limit = 15;
    restarted_config.volumes[0].access_id = Some("rotated-ak".to_string());
    restarted_config.volumes[0].secret_key = Some("rotated-sk".to_string());
    let restarted = DbBuilder::new(restarted_config)
        .bucket_ranges(vec![0..=0])
        .db_id("properties-shard")
        .open()
        .unwrap();
    let refreshed_contents = std::fs::read_to_string(&properties_path).unwrap();
    let refreshed: toml::Value = toml::from_str(&refreshed_contents).unwrap();

    assert_eq!(refreshed["config"]["l0_file_limit"].as_integer(), Some(15));
    assert!(!refreshed_contents.contains("rotated-ak"));
    assert!(!refreshed_contents.contains("rotated-sk"));

    restarted.close().unwrap();
    cleanup_test_root(root);
}

fn wait_for_runtime_generation_at_least(
    store: &crate::runtime_manifest::RuntimeManifestStore,
    generation: u64,
) -> crate::runtime_manifest::LoadedRuntimeManifest {
    for _ in 0..100 {
        if let Some(current) = store.load_current().unwrap()
            && current.generation >= generation
        {
            return current;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("runtime manifest did not reach generation {generation}");
}

type GovernanceCall = (String, Vec<RangeInclusive<u16>>, u32);

#[derive(Default)]
struct RecordingGovernance {
    register_calls: Mutex<Vec<GovernanceCall>>,
    unregister_calls: Mutex<Vec<String>>,
}

impl DbGovernance for RecordingGovernance {
    fn register_db(
        &self,
        db_id: &str,
        ranges: &[RangeInclusive<u16>],
        total_buckets: u32,
    ) -> Result<()> {
        self.register_calls
            .lock()
            .expect("recording governance register lock")
            .push((db_id.to_string(), ranges.to_vec(), total_buckets));
        Ok(())
    }

    fn unregister_db(&self, db_id: &str) -> Result<()> {
        self.unregister_calls
            .lock()
            .expect("recording governance unregister lock")
            .push(db_id.to_string());
        Ok(())
    }
}

#[test]
#[serial(file)]
fn test_db_rejects_mutation_and_read_after_close() {
    let root = "/tmp/db_state_after_close";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = open_db(config);
    db.put(0, b"k1", 0, b"v1").unwrap();
    db.close().unwrap();
    db.close().unwrap();

    let put_err = db.put(0, b"k2", 0, b"v2").unwrap_err();
    assert!(matches!(put_err, Error::InvalidState(_)));
    let get_err = db.get(0, b"k1").unwrap_err();
    assert!(matches!(get_err, Error::InvalidState(_)));
    let snapshot_err = db.snapshot().unwrap_err();
    assert!(matches!(snapshot_err, Error::InvalidState(_)));
    let cancel_err = db.cancel_snapshot(0).unwrap_err();
    assert!(matches!(cancel_err, Error::InvalidState(_)));
    assert!(!db.retain_snapshot(0));

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_cancel_snapshot_returns_cancelled_error_and_consumes_snapshot_id() {
    let root = "/tmp/db_cancel_snapshot";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = open_db(config);
    for i in 0..256 {
        let key = format!("k-{i}");
        let value = format!("value-{i}");
        db.put(0, key.as_bytes(), 0, value.as_bytes()).unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let snapshot_id = db
        .snapshot_with_callback(move |result| {
            tx.send(result).expect("send cancelled snapshot result");
        })
        .unwrap();

    assert!(db.cancel_snapshot(snapshot_id).unwrap());
    let callback_result = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("receive cancelled snapshot result");
    assert!(matches!(callback_result, Err(Error::CancelledError(_))));
    let _ = db.memtable_manager.wait_for_flushes();
    assert!(
        db.snapshot_manager
            .wait_for_materialization(Duration::from_secs(10))
    );
    assert!(!db.expire_snapshot(snapshot_id).unwrap());
    let manifest_path = db
        .file_manager
        .metadata_path(&snapshot_manifest_name(snapshot_id));
    let active_data_path = db
        .file_manager
        .metadata_path(&snapshot_active_data_relative_path(snapshot_id));
    assert!(
        !db.file_manager
            .has_metadata_file(&snapshot_manifest_name(snapshot_id))
    );
    assert!(
        !db.file_manager
            .meta_volume
            .fs()
            .exists(&manifest_path)
            .expect("check cancelled snapshot manifest")
    );
    assert!(
        !db.file_manager
            .meta_volume
            .fs()
            .exists(&active_data_path)
            .expect("check cancelled active snapshot data")
    );

    let next_snapshot_id = db.snapshot().unwrap();
    assert_eq!(snapshot_id + 1, next_snapshot_id);

    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_cancel_snapshot_returns_false_once_publication_starts() {
    let root = "/tmp/db_cancel_snapshot_after_publication_start";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = open_db(config);

    let snapshot = db.snapshot_manager.create_snapshot(None);
    assert_eq!(snapshot.try_begin_publication(), Ok(()));

    assert!(!db.cancel_snapshot(snapshot.id).unwrap());
    assert!(db.expire_snapshot(snapshot.id).unwrap());
    assert_eq!(
        snapshot.lifecycle_state(),
        SnapshotLifecycleState::CommitStartedExpireRequested
    );

    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_failed_snapshot_completes_callback_and_releases_snapshot() {
    let root = "/tmp/db_failed_snapshot_callback";
    cleanup_test_root(root);
    let db = open_db(config_with_small_memtable(root));
    let (tx, rx) = mpsc::channel();
    let snapshot = db
        .snapshot_manager
        .create_snapshot(Some(Arc::new(move |result| {
            tx.send(result).expect("send failed snapshot result");
        })));

    db.snapshot_manager.fail_snapshot(
        snapshot.id,
        Error::IoError("flush worker failed before materialization".to_string()),
    );

    let callback_result = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive failed snapshot result");
    assert!(matches!(callback_result, Err(Error::IoError(_))));
    assert!(!db.expire_snapshot(snapshot.id).unwrap());

    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_snapshot_callback_completes_when_flush_worker_is_unavailable() {
    let root = "/tmp/db_snapshot_flush_worker_unavailable";
    cleanup_test_root(root);
    let db = open_db(config_with_small_memtable(root));
    db.memtable_manager.force_close();

    let (tx, rx) = mpsc::channel();
    let err = db
        .snapshot_with_callback(move |result| {
            tx.send(result).expect("send failed snapshot result");
        })
        .unwrap_err();
    assert!(matches!(err, Error::IoError(_)));
    let callback_result = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive failed snapshot result");
    assert!(matches!(callback_result, Err(Error::IoError(_))));

    db.force_close();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_snapshot_completes_while_writer_rotates_multiple_memtables() {
    let root = "/tmp/db_snapshot_continuous_writes";
    cleanup_test_root(root);
    let primary = format!("file://{root}/primary");
    let snapshot = format!("file://{root}/snapshot");
    let db = Arc::new(open_db(Config {
        memtable_capacity: Size::from_kib(8),
        memtable_buffer_count: 2,
        l0_file_limit: 64,
        file_transfer_concurrency: 2,
        num_columns: 1,
        volumes: vec![
            VolumeDescriptor::new(primary, vec![VolumeUsageKind::PrimaryDataPriorityHigh]),
            VolumeDescriptor::new(
                snapshot,
                vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
            ),
        ],
        ..Config::default()
    }));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_db = Arc::clone(&db);
    let writer_stop = Arc::clone(&stop);
    let writer = std::thread::spawn(move || {
        let value = vec![b'v'; 512];
        let mut index = 0_u64;
        while !writer_stop.load(AtomicOrdering::Relaxed) {
            let key = format!("key-{}", index % 256);
            writer_db.put(0, key.as_bytes(), 0, &value)?;
            index += 1;
            if index.is_multiple_of(64) {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        Ok::<(), Error>(())
    });

    std::thread::sleep(Duration::from_millis(50));
    let (tx, rx) = mpsc::channel();
    let mut snapshot_id = None;
    let snapshot_result = (|| -> Result<(u64, Result<crate::coordinator::ShardSnapshotInput>)> {
        let id = db.snapshot_with_callback(move |result| {
            let _ = tx.send(result);
        })?;
        snapshot_id = Some(id);
        let result = rx.recv_timeout(Duration::from_secs(5)).map_err(|err| {
            Error::IoError(format!(
                "snapshot did not complete while writes continued: {err}"
            ))
        })?;
        Ok((id, result))
    })();
    stop.store(true, AtomicOrdering::Relaxed);
    let writer_result = writer.join().expect("continuous writer did not panic");

    if let Err(err) = &snapshot_result {
        if let Some(snapshot_id) = snapshot_id {
            let _ = db.cancel_snapshot(snapshot_id);
        }
        db.force_close();
        cleanup_test_root(root);
        panic!("snapshot result: {err}");
    }
    let (snapshot_id, result) = snapshot_result.expect("snapshot result");
    writer_result.expect("continuous writer result");
    assert!(result.is_ok(), "snapshot result: {result:?}");
    assert!(db.expire_snapshot(snapshot_id).unwrap());
    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_concurrent_snapshots_follow_id_order_during_writes() {
    const WRITER_COUNT: usize = 3;
    const SNAPSHOT_COUNT: usize = 8;
    const WRITES_PER_WRITER: usize = 300;

    let root = "/tmp/db_concurrent_snapshots";
    cleanup_test_root(root);
    let db = Arc::new(open_db(Config {
        memtable_capacity: Size::from_kib(8),
        memtable_buffer_count: 4,
        l0_file_limit: 128,
        write_stall_limit: Some(128),
        snapshot_retention: None,
        num_columns: 1,
        volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
        ..Config::default()
    }));
    let start = Arc::new(Barrier::new(WRITER_COUNT + SNAPSHOT_COUNT + 1));

    let writers = (0..WRITER_COUNT)
        .map(|writer_id| {
            let writer_db = Arc::clone(&db);
            let writer_start = Arc::clone(&start);
            std::thread::spawn(move || {
                writer_start.wait();
                for sequence in 0..WRITES_PER_WRITER {
                    let key = format!("snapshot-writer-{writer_id}-{sequence:04}");
                    let value = vec![b'a' + writer_id as u8; 128];
                    writer_db
                        .put(0, key.as_bytes(), 0, &value)
                        .expect("write during concurrent snapshots");
                    if sequence.is_multiple_of(32) {
                        std::thread::yield_now();
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    let snapshots = (0..SNAPSHOT_COUNT)
        .map(|snapshot_thread| {
            let snapshot_db = Arc::clone(&db);
            let snapshot_start = Arc::clone(&start);
            std::thread::spawn(move || {
                snapshot_start.wait();
                std::thread::sleep(Duration::from_millis((snapshot_thread % 4) as u64));
                let (tx, rx) = mpsc::channel();
                let snapshot_id = snapshot_db
                    .snapshot_with_callback(move |result| {
                        let _ = tx.send(result);
                    })
                    .expect("create concurrent snapshot");
                let input = rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("concurrent snapshot callback")
                    .expect("materialize concurrent snapshot");
                assert_eq!(input.snapshot_id, snapshot_id);
                assert!(snapshot_db.retain_snapshot(snapshot_id));
                snapshot_id
            })
        })
        .collect::<Vec<_>>();

    start.wait();
    for writer in writers {
        writer.join().expect("snapshot writer did not panic");
    }
    let mut snapshot_ids = snapshots
        .into_iter()
        .map(|snapshot| snapshot.join().expect("snapshot thread did not panic"))
        .collect::<Vec<_>>();
    snapshot_ids.sort_unstable();
    snapshot_ids.dedup();
    assert_eq!(snapshot_ids.len(), SNAPSHOT_COUNT);

    let mut previous_seq_id = None;
    for snapshot_id in &snapshot_ids {
        let manifest = load_manifest_for_snapshot(&db.file_manager, *snapshot_id)
            .expect("load concurrent snapshot manifest");
        if let Some(previous) = previous_seq_id {
            assert!(
                manifest.seq_id >= previous,
                "snapshot id order regressed from seq {previous} to {} at id {snapshot_id}",
                manifest.seq_id
            );
        }
        previous_seq_id = Some(manifest.seq_id);
    }

    for snapshot_id in snapshot_ids {
        assert!(db.expire_snapshot(snapshot_id).unwrap());
    }
    for writer_id in 0..WRITER_COUNT {
        for sequence in 0..WRITES_PER_WRITER {
            let key = format!("snapshot-writer-{writer_id}-{sequence:04}");
            assert!(db.get(0, key.as_bytes()).unwrap().is_some());
        }
    }
    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_single_writer_with_concurrent_gets_and_scans_across_memtable_rotation() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Db>();

    for memtable_type in [
        MemtableType::Hash,
        MemtableType::Skiplist,
        MemtableType::Vec,
    ] {
        let root = format!(
            "/tmp/db_concurrent_reads_{}",
            format!("{memtable_type:?}").to_lowercase()
        );
        cleanup_test_root(&root);
        let db = Arc::new(open_db(Config {
            memtable_capacity: Size::from_kib(16),
            memtable_buffer_count: 3,
            memtable_type,
            l0_file_limit: 64,
            num_columns: 1,
            volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
            ..Config::default()
        }));
        let published = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let start = Arc::new(Barrier::new(6));

        let writer_db = Arc::clone(&db);
        let writer_published = Arc::clone(&published);
        let writer_done = Arc::clone(&done);
        let writer_start = Arc::clone(&start);
        let writer = std::thread::spawn(move || {
            writer_start.wait();
            for index in 1..=1_000usize {
                let key = format!("key-{index:05}");
                let value = format!("value-{index:05}-{}", "x".repeat(128));
                writer_db
                    .put(0, key.as_bytes(), 0, value.as_bytes())
                    .expect("single writer put");
                writer_published.store(index, AtomicOrdering::Release);
            }
            writer_done.store(true, AtomicOrdering::Release);
        });

        let readers = (0..4usize)
            .map(|reader_id| {
                let reader_db = Arc::clone(&db);
                let reader_published = Arc::clone(&published);
                let reader_done = Arc::clone(&done);
                let reader_start = Arc::clone(&start);
                std::thread::spawn(move || {
                    reader_start.wait();
                    let mut round = 0usize;
                    while !reader_done.load(AtomicOrdering::Acquire) {
                        let high = reader_published.load(AtomicOrdering::Acquire);
                        if high > 0 {
                            let index = 1 + (round.wrapping_mul(97).wrapping_add(reader_id)) % high;
                            let key = format!("key-{index:05}");
                            let expected_prefix = format!("value-{index:05}-");
                            let row = reader_db
                                .get(0, key.as_bytes())
                                .expect("concurrent get")
                                .expect("published key is visible");
                            assert!(
                                row[0]
                                    .as_ref()
                                    .expect("column exists")
                                    .starts_with(expected_prefix.as_bytes())
                            );
                        }
                        if round.is_multiple_of(32) {
                            let mut previous = None;
                            let iter = reader_db
                                .scan(0, b"".as_slice()..b"\xff".as_slice())
                                .expect("concurrent scan");
                            for row in iter {
                                let (key, columns) = row.expect("scan row");
                                if let Some(previous) = previous.as_ref() {
                                    assert!(previous < &key);
                                }
                                assert!(columns[0].is_some());
                                previous = Some(key);
                            }
                        }
                        round += 1;
                    }
                })
            })
            .collect::<Vec<_>>();

        start.wait();
        writer.join().expect("writer did not panic");
        for reader in readers {
            reader.join().expect("reader did not panic");
        }
        for index in 1..=1_000usize {
            let key = format!("key-{index:05}");
            assert!(db.get(0, key.as_bytes()).unwrap().is_some());
        }
        db.close().unwrap();
        cleanup_test_root(&root);
    }
}

#[test]
#[serial(file)]
fn test_concurrent_writers_and_readers_across_memtable_rotation() {
    const WRITER_COUNT: usize = 6;
    const READER_COUNT: usize = 2;
    const WRITES_PER_WRITER: usize = 200;

    for memtable_type in [
        MemtableType::Hash,
        MemtableType::Skiplist,
        MemtableType::Vec,
    ] {
        let root = format!(
            "/tmp/db_concurrent_writes_{}",
            format!("{memtable_type:?}").to_lowercase()
        );
        cleanup_test_root(&root);
        let db = Arc::new(open_db(Config {
            memtable_capacity: Size::from_kib(16),
            memtable_buffer_count: 4,
            memtable_type,
            l0_file_limit: 128,
            write_stall_limit: Some(128),
            num_columns: 1,
            volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
            ..Config::default()
        }));
        let done = Arc::new(AtomicBool::new(false));
        let start = Arc::new(Barrier::new(WRITER_COUNT + READER_COUNT + 1));

        let writers = (0..WRITER_COUNT)
            .map(|writer_id| {
                let writer_db = Arc::clone(&db);
                let writer_start = Arc::clone(&start);
                std::thread::spawn(move || {
                    writer_start.wait();
                    for sequence in 0..WRITES_PER_WRITER {
                        let key = format!("writer-{writer_id}-key-{sequence:04}");
                        let value =
                            format!("writer-{writer_id}-value-{sequence:04}-{}", "x".repeat(128));
                        let latest_key = format!("writer-{writer_id}-latest");
                        let latest = format!("{sequence:04}");
                        match writer_id % 3 {
                            0 => {
                                writer_db
                                    .put(0, key.as_bytes(), 0, value.as_bytes())
                                    .expect("concurrent writer put");
                                writer_db
                                    .put(0, latest_key.as_bytes(), 0, latest.as_bytes())
                                    .expect("concurrent writer ordered put");
                            }
                            1 => {
                                let entries = [
                                    (key.as_bytes(), value.as_bytes()),
                                    (latest_key.as_bytes(), latest.as_bytes()),
                                ];
                                writer_db
                                    .put_column_batch_with_options(
                                        0,
                                        0,
                                        entries,
                                        &WriteOptions::default(),
                                    )
                                    .expect("concurrent column batch");
                            }
                            _ => {
                                let mut batch = WriteBatch::new();
                                batch.put(0, key.as_bytes(), 0, value.as_bytes());
                                batch.put(0, latest_key.as_bytes(), 0, latest.as_bytes());
                                writer_db
                                    .write_batch(batch)
                                    .expect("concurrent write batch");
                            }
                        }

                        if sequence.is_multiple_of(50) {
                            let deleted_key = format!("writer-{writer_id}-deleted-{sequence:04}");
                            writer_db
                                .put(0, deleted_key.as_bytes(), 0, b"temporary")
                                .expect("put before concurrent delete");
                            writer_db
                                .delete(0, deleted_key.as_bytes(), 0)
                                .expect("concurrent delete");

                            let merged_key = format!("writer-{writer_id}-merged-{sequence:04}");
                            writer_db
                                .put(0, merged_key.as_bytes(), 0, b"base")
                                .expect("put before concurrent merge");
                            writer_db
                                .merge(0, merged_key.as_bytes(), 0, b"-tail")
                                .expect("concurrent merge");
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        let readers = (0..READER_COUNT)
            .map(|reader_id| {
                let reader_db = Arc::clone(&db);
                let reader_done = Arc::clone(&done);
                let reader_start = Arc::clone(&start);
                std::thread::spawn(move || {
                    reader_start.wait();
                    let mut round = 0usize;
                    while !reader_done.load(AtomicOrdering::Acquire) {
                        let writer_id = (round + reader_id) % WRITER_COUNT;
                        let key = format!("writer-{writer_id}-latest");
                        let _ = reader_db.get(0, key.as_bytes()).expect("concurrent get");
                        if round.is_multiple_of(64) {
                            let iter = reader_db
                                .scan(0, b"writer-".as_slice()..b"writer.".as_slice())
                                .expect("concurrent scan");
                            for row in iter {
                                row.expect("concurrent scan row");
                            }
                        }
                        round += 1;
                    }
                })
            })
            .collect::<Vec<_>>();

        start.wait();
        for writer in writers {
            writer.join().expect("writer did not panic");
        }
        done.store(true, AtomicOrdering::Release);
        for reader in readers {
            reader.join().expect("reader did not panic");
        }

        for writer_id in 0..WRITER_COUNT {
            for sequence in 0..WRITES_PER_WRITER {
                let key = format!("writer-{writer_id}-key-{sequence:04}");
                assert!(
                    db.get(0, key.as_bytes()).unwrap().is_some(),
                    "missing {key} for {memtable_type:?}"
                );
            }
            let key = format!("writer-{writer_id}-latest");
            let value = db.get(0, key.as_bytes()).unwrap().unwrap();
            assert_eq!(
                value[0].as_deref(),
                Some(format!("{:04}", WRITES_PER_WRITER - 1).as_bytes())
            );
            for sequence in (0..WRITES_PER_WRITER).step_by(50) {
                let deleted_key = format!("writer-{writer_id}-deleted-{sequence:04}");
                assert!(db.get(0, deleted_key.as_bytes()).unwrap().is_none());
                let merged_key = format!("writer-{writer_id}-merged-{sequence:04}");
                let value = db.get(0, merged_key.as_bytes()).unwrap().unwrap();
                assert_eq!(value[0].as_deref(), Some(b"base-tail".as_slice()));
            }
        }
        db.close().unwrap();
        cleanup_test_root(&root);
    }
}

#[test]
#[serial(file)]
fn test_concurrent_oversized_writes_replace_empty_active_once() {
    const WRITER_COUNT: usize = 4;
    const WRITES_PER_WRITER: usize = 12;

    let root = "/tmp/db_concurrent_oversized_writes";
    cleanup_test_root(root);
    let db = Arc::new(open_db(Config {
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 3,
        memtable_type: MemtableType::Hash,
        l0_file_limit: 128,
        write_stall_limit: Some(128),
        num_columns: 1,
        volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
        ..Config::default()
    }));
    let start = Arc::new(Barrier::new(WRITER_COUNT + 1));
    let writers = (0..WRITER_COUNT)
        .map(|writer_id| {
            let writer_db = Arc::clone(&db);
            let writer_start = Arc::clone(&start);
            std::thread::spawn(move || {
                writer_start.wait();
                for sequence in 0..WRITES_PER_WRITER {
                    let key = format!("oversized-{writer_id}-{sequence:03}");
                    let value = vec![b'a' + writer_id as u8; 1_024 + sequence];
                    writer_db
                        .put(0, key.as_bytes(), 0, &value)
                        .expect("concurrent oversized put");
                }
            })
        })
        .collect::<Vec<_>>();

    start.wait();
    for writer in writers {
        writer.join().expect("oversized writer did not panic");
    }
    for writer_id in 0..WRITER_COUNT {
        for sequence in 0..WRITES_PER_WRITER {
            let key = format!("oversized-{writer_id}-{sequence:03}");
            let expected = vec![b'a' + writer_id as u8; 1_024 + sequence];
            let value = db.get(0, key.as_bytes()).unwrap().unwrap();
            assert_eq!(value[0].as_deref(), Some(expected.as_slice()));
        }
    }
    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_switch_memtable_type_replaces_empty_and_flushes_nonempty_active() {
    let root = "/tmp/db_switch_memtable_type";
    cleanup_test_root(root);
    let db = open_db(Config {
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 1,
        memtable_type: MemtableType::Hash,
        volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
        ..Config::default()
    });
    let active_type = || db.memtable_manager.active_memtable_type();

    assert_eq!(active_type(), Some(MemtableType::Hash));
    db.switch_memtable_type(MemtableType::Vec, true).unwrap();
    assert_eq!(active_type(), Some(MemtableType::Vec));
    assert!(db.db_state.load().immutables.is_empty());

    db.switch_memtable_type(MemtableType::Hash, true).unwrap();
    assert_eq!(active_type(), Some(MemtableType::Hash));

    let oversized = vec![b'x'; 1_024];
    db.put(0, b"oversized", 0, &oversized).unwrap();
    assert_eq!(active_type(), Some(MemtableType::Vec));
    assert_eq!(
        db.memtable_manager.target_memtable_type(),
        MemtableType::Hash
    );
    db.switch_memtable_type(MemtableType::Vec, true).unwrap();
    let flush_results = db.memtable_manager.wait_for_flushes();
    assert_eq!(flush_results.len(), 1);
    assert!(flush_results[0].is_ok());
    assert_eq!(
        db.memtable_manager.wait_for_active_memtable_type().unwrap(),
        MemtableType::Vec
    );
    assert_eq!(
        db.get(0, b"oversized").unwrap().unwrap()[0].as_deref(),
        Some(oversized.as_slice())
    );

    db.switch_memtable_type(MemtableType::Hash, true).unwrap();
    assert_eq!(active_type(), Some(MemtableType::Hash));
    db.put(0, b"flushed", 0, b"value").unwrap();
    db.switch_memtable_type(MemtableType::Skiplist, true)
        .unwrap();
    let flush_results = db.memtable_manager.wait_for_flushes();
    assert_eq!(flush_results.len(), 1);
    assert!(flush_results[0].is_ok());
    assert_eq!(
        db.memtable_manager.wait_for_active_memtable_type().unwrap(),
        MemtableType::Skiplist
    );
    assert_eq!(
        db.get(0, b"flushed").unwrap().unwrap()[0].as_deref(),
        Some(b"value".as_slice())
    );

    assert!(db.memtable_manager.wait_for_flushes().is_empty());
    db.put(0, b"same-target", 0, b"value").unwrap();
    db.switch_memtable_type(MemtableType::Skiplist, true)
        .unwrap();
    let flush_results = db.memtable_manager.wait_for_flushes();
    assert_eq!(flush_results.len(), 1);
    assert!(flush_results[0].is_ok());
    assert_eq!(
        db.memtable_manager.wait_for_active_memtable_type().unwrap(),
        MemtableType::Skiplist
    );
    assert_eq!(
        db.get(0, b"same-target").unwrap().unwrap()[0].as_deref(),
        Some(b"value".as_slice())
    );

    db.switch_memtable_type(MemtableType::Vec, true).unwrap();
    assert_eq!(active_type(), Some(MemtableType::Vec));
    db.switch_memtable_type(MemtableType::Hash, true).unwrap();
    assert_eq!(active_type(), Some(MemtableType::Hash));

    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_adaptive_memtable_switches_to_vec_on_pure_writes() {
    let root = "/tmp/db_adaptive_memtable";
    cleanup_test_root(root);
    let db = open_db(Config {
        memtable_capacity: Size::from_kib(64),
        memtable_buffer_count: 2,
        memtable_type: MemtableType::Adaptive,
        l0_file_limit: 64,
        num_columns: 1,
        volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
        ..Config::default()
    });

    // Initial concrete type is Skiplist (Adaptive resolves to Skiplist).
    assert_eq!(
        db.memtable_manager.target_memtable_type(),
        MemtableType::Skiplist
    );

    // 4096 writes should trigger the adaptive controller to switch to Vec
    // (pure writes >= 99.9% with zero reads).
    for i in 0..4097u32 {
        let key = format!("key{i}");
        db.put(0, key.as_bytes(), 0, b"value").unwrap();
    }

    // The controller should have switched the target to Vec (non-disruptive, flush_current=false).
    assert_eq!(
        db.memtable_manager.target_memtable_type(),
        MemtableType::Vec
    );

    // Switching to a concrete type pins it and disables adaptive statistics.
    db.switch_memtable_type(MemtableType::Skiplist, false)
        .unwrap();
    assert_eq!(
        db.memtable_manager.target_memtable_type(),
        MemtableType::Skiplist
    );
    assert!(!db.memtable_manager.adaptive_enabled());
    // Writes no longer trigger adaptive switches (stats disabled).
    for i in 0..8192u32 {
        let key = format!("pure{i}");
        db.put(0, key.as_bytes(), 0, b"value").unwrap();
    }
    assert_eq!(
        db.memtable_manager.target_memtable_type(),
        MemtableType::Skiplist
    );

    // Switching back to Adaptive re-enables statistics, resuming from Skiplist.
    db.switch_memtable_type(MemtableType::Adaptive, false)
        .unwrap();
    assert!(db.memtable_manager.adaptive_enabled());
    assert_eq!(
        db.memtable_manager.target_memtable_type(),
        MemtableType::Skiplist
    );
    // Now pure writes should trigger a switch to Vec again.
    for i in 0..4097u32 {
        let key = format!("resume{i}");
        db.put(0, key.as_bytes(), 0, b"value").unwrap();
    }
    assert_eq!(
        db.memtable_manager.target_memtable_type(),
        MemtableType::Vec
    );

    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_adaptive_memtable_no_deadlock_on_vec_rollback_during_writes() {
    // Regression test for P1 deadlock: recording a write while holding the active-memtable
    // write lock must not re-enter the manager to perform a flush. The adaptive decision is
    // applied only after the write completes and the lock is released.
    let root = "/tmp/db_adaptive_no_deadlock";
    cleanup_test_root(root);
    let db = open_db(Config {
        memtable_capacity: Size::from_kib(64),
        memtable_buffer_count: 2,
        memtable_type: MemtableType::Adaptive,
        l0_file_limit: 64,
        num_columns: 1,
        volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
        ..Config::default()
    });

    // Phase 1: enter Vec via pure writes.
    for i in 0..4097u32 {
        db.put(0, format!("w{i}").as_bytes(), 0, b"value").unwrap();
    }
    assert_eq!(
        db.memtable_manager.target_memtable_type(),
        MemtableType::Vec
    );

    // Phase 2: issue reads to trigger VEC rollback (flush_current=true).
    // This must not deadlock - the decision is applied after the read returns.
    for i in 0..20u32 {
        let _ = db.get(0, format!("w{i}").as_bytes());
    }
    // After reads on VEC, the controller should have rolled back to Skiplist.
    assert_eq!(
        db.memtable_manager.target_memtable_type(),
        MemtableType::Skiplist
    );

    // Phase 3: verify the DB is still usable after the rollback.
    db.put(0, b"after_rollback", 0, b"value").unwrap();
    assert_eq!(
        db.get(0, b"after_rollback").unwrap().unwrap()[0].as_deref(),
        Some(b"value".as_slice())
    );

    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_switch_memtable_type_without_flush_defers_until_natural_rotation() {
    let root = "/tmp/db_switch_memtable_type_deferred";
    cleanup_test_root(root);
    let db = open_db(Config {
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 1,
        memtable_type: MemtableType::Hash,
        volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
        ..Config::default()
    });

    db.put(0, b"deferred", 0, b"value").unwrap();
    assert_eq!(
        db.memtable_manager.active_memtable_type(),
        Some(MemtableType::Hash)
    );
    db.switch_memtable_type(MemtableType::Vec, false).unwrap();
    assert_eq!(
        db.memtable_manager.target_memtable_type(),
        MemtableType::Vec
    );
    assert_eq!(
        db.memtable_manager.active_memtable_type(),
        Some(MemtableType::Hash)
    );
    assert!(db.db_state.load().immutables.is_empty());
    assert!(db.memtable_manager.wait_for_flushes().is_empty());
    assert_eq!(
        db.get(0, b"deferred").unwrap().unwrap()[0].as_deref(),
        Some(b"value".as_slice())
    );

    db.memtable_manager.flush_active().unwrap();
    let flush_results = db.memtable_manager.wait_for_flushes();
    assert_eq!(flush_results.len(), 1);
    assert!(flush_results[0].is_ok());
    assert_eq!(
        db.memtable_manager.wait_for_active_memtable_type().unwrap(),
        MemtableType::Vec
    );
    assert_eq!(
        db.get(0, b"deferred").unwrap().unwrap()[0].as_deref(),
        Some(b"value".as_slice())
    );

    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_builder_uses_custom_governance() {
    let root = "/tmp/db_builder_custom_governance";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let total_buckets = config.total_buckets;
    let ranges = vec![full_bucket_range(total_buckets)];
    let governance = Arc::new(RecordingGovernance::default());
    let db = DbBuilder::new(config)
        .db_id("db-builder-governed")
        .bucket_ranges(ranges.clone())
        .governance(Arc::clone(&governance) as Arc<dyn DbGovernance>)
        .open()
        .unwrap();
    db.close().unwrap();

    let register_calls = governance
        .register_calls
        .lock()
        .expect("recording governance register lock");
    assert_eq!(register_calls.len(), 1);
    assert_eq!(register_calls[0].0, "db-builder-governed");
    assert_eq!(register_calls[0].1, ranges);
    assert_eq!(register_calls[0].2, total_buckets);
    drop(register_calls);

    let unregister_calls = governance
        .unregister_calls
        .lock()
        .expect("recording governance unregister lock");
    assert_eq!(
        unregister_calls.as_slice(),
        &["db-builder-governed".to_string()]
    );
    drop(unregister_calls);
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_uses_noop_governance_when_explicitly_configured() {
    let root = "/tmp/db_noop_governance";
    cleanup_test_root(root);
    let mut config = config_with_small_memtable(root);
    config.governance_mode = GovernanceMode::Noop;

    let db = open_db(config);
    db.close().unwrap();

    assert!(
        !std::path::Path::new(root)
            .join(GOVERNANCE_MANIFEST_POINTER_NAME)
            .exists()
    );
    cleanup_test_root(root);
}

fn decode_u32_counter(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("u32 counter bytes"))
}

fn decode_u64_counter(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("u64 counter bytes"))
}

struct PipeMergeOperator;

impl MergeOperator for PipeMergeOperator {
    fn merge(
        &self,
        existing_value: Bytes,
        value: Bytes,
        _time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        if existing_value.is_empty() {
            Ok((value, None))
        } else {
            let mut merged = BytesMut::with_capacity(existing_value.len() + 1 + value.len());
            merged.extend_from_slice(existing_value.as_ref());
            merged.extend_from_slice(b"|");
            merged.extend_from_slice(value.as_ref());
            Ok((merged.freeze(), None))
        }
    }
}

#[derive(Default)]
struct BatchCountingMergeOperator {
    merge_calls: AtomicUsize,
    merge_batch_calls: AtomicUsize,
}

impl MergeOperator for BatchCountingMergeOperator {
    fn merge(
        &self,
        existing_value: Bytes,
        value: Bytes,
        _time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        self.merge_calls.fetch_add(1, AtomicOrdering::Relaxed);
        let mut merged = BytesMut::with_capacity(existing_value.len() + value.len());
        merged.extend_from_slice(existing_value.as_ref());
        merged.extend_from_slice(value.as_ref());
        Ok((merged.freeze(), None))
    }

    fn merge_batch(
        &self,
        existing_value: Bytes,
        operands: Vec<Bytes>,
        _time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        self.merge_batch_calls.fetch_add(1, AtomicOrdering::Relaxed);
        let mut merged = BytesMut::with_capacity(
            existing_value.len() + operands.iter().map(Bytes::len).sum::<usize>(),
        );
        merged.extend_from_slice(existing_value.as_ref());
        for operand in operands {
            merged.extend_from_slice(operand.as_ref());
        }
        Ok((merged.freeze(), None))
    }
}

#[test]
#[serial(file)]
fn test_db_write_batch_triggers_flush() {
    let root = "/tmp/db_write_batch_flush";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = open_db(config);
    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, vec![b'a'; 64]);
    batch.put(0, b"k2", 0, vec![b'b'; 64]);
    db.write_batch(batch).unwrap();

    let results = db.memtable_manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    assert_eq!(db.lsm_tree.level_files(0).len(), 1);

    db.memtable_manager.flush_active().unwrap();
    let results = db.memtable_manager.wait_for_flushes();
    assert_eq!(results.len(), 1);
    assert_eq!(db.lsm_tree.level_files(0).len(), 2);

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn runtime_manifest_embedded_mode_publishes_initial_flush_and_final_state() {
    let root = "/tmp/db_runtime_manifest_embedded";
    cleanup_test_root(root);
    let mut config = config_with_small_memtable(root);
    config.runtime_manifest_mode = RuntimeManifestMode::Enabled;
    config.governance_mode = GovernanceMode::Noop;
    config.time_provider = TimeProviderKind::Manual;
    let db = open_db(config);
    let store = runtime_manifest_store(&db);

    let initial = store
        .load_current()
        .unwrap()
        .expect("initial runtime manifest");
    assert_eq!(initial.manifest.compaction_mode, CompactionMode::Embedded);
    assert!(
        initial
            .manifest
            .tree_levels
            .iter()
            .all(|levels| { levels.iter().all(|level| level.files.is_empty()) })
    );
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        store
            .load_current()
            .unwrap()
            .expect("unchanged manifest")
            .generation,
        initial.generation
    );

    let current_seq_id = db.db_state.load().seq_id;
    db.set_time(11);
    db.runtime_manifest_publisher
        .as_ref()
        .expect("enabled runtime manifest publisher")
        .publish_at_least(current_seq_id)
        .unwrap();
    let barrier = wait_for_runtime_generation_at_least(&store, initial.generation + 1);
    assert_eq!(
        barrier.generation,
        initial.generation + 1,
        "coalesced no-op observations must not consume generations"
    );
    assert!(barrier.manifest.seq_id >= current_seq_id);
    assert_eq!(barrier.manifest.timestamp_seconds, 11);

    db.set_time(22);
    db.put(0, b"runtime-key", 0, vec![b'x'; 96]).unwrap();
    db.memtable_manager.flush_active().unwrap();
    db.memtable_manager.wait_for_flushes();
    let flushed = wait_for_runtime_generation_at_least(&store, barrier.generation + 1);
    assert!(
        flushed
            .manifest
            .tree_levels
            .iter()
            .flat_map(|levels| levels.iter())
            .any(|level| !level.files.is_empty())
    );
    assert_eq!(flushed.manifest.timestamp_seconds, 22);

    db.advance_truncation_cursor_by_id(0, DEFAULT_COLUMN_FAMILY_ID, b"runtime-key")
        .unwrap();
    let with_cursor = wait_for_runtime_generation_at_least(&store, flushed.generation + 1);
    assert_eq!(with_cursor.manifest.truncation_cursors.len(), 1);

    db.close().unwrap();
    let final_manifest = store
        .load_current()
        .unwrap()
        .expect("final runtime manifest");
    assert!(final_manifest.generation >= with_cursor.generation);
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn runtime_manifest_dedicated_suspension_survives_failure_and_resumes_after_publish() {
    let root = "/tmp/db_runtime_manifest_dedicated_suspension";
    cleanup_test_root(root);
    let mut config = config_with_small_memtable(root);
    config.runtime_manifest_mode = RuntimeManifestMode::Enabled;
    config.governance_mode = GovernanceMode::Noop;
    let db = open_db(config);
    let store = runtime_manifest_store(&db);
    let initial = store.load_current().unwrap().unwrap();
    let publisher = db
        .runtime_manifest_publisher
        .as_ref()
        .expect("enabled runtime manifest publisher");

    assert!(publisher.suspend_for_owner("test-job").unwrap());
    assert!(
        !publisher.suspend_for_owner("test-job").unwrap(),
        "same-job retry must reuse the existing suspension"
    );
    assert!(
        publisher.suspend_for_owner("other-job").is_err(),
        "another job cannot take over an unproven edit"
    );
    db.put(0, b"suspended-key", 0, vec![b'x'; 96]).unwrap();
    db.memtable_manager.flush_active().unwrap();
    db.memtable_manager.wait_for_flushes();
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        store.load_current().unwrap().unwrap().generation,
        initial.generation,
        "background publication must stay suspended after the persisted state changes"
    );

    assert!(
        publisher
            .publish_at_least_and_resume("test-job", u64::MAX)
            .is_err()
    );
    assert!(
        publisher.publish_current().is_err(),
        "a failed barrier publish must leave suspension active"
    );
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        store.load_current().unwrap().unwrap().generation,
        initial.generation
    );

    let current_seq_id = db.db_state.load().seq_id;
    publisher
        .publish_at_least_and_resume("test-job", current_seq_id)
        .unwrap();
    let published =
        wait_for_runtime_generation_at_least(&store, initial.generation.saturating_add(1));
    assert!(published.manifest.seq_id >= current_seq_id);
    publisher
        .publish_current()
        .expect("successful barrier publication must clear suspension");

    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn runtime_manifest_close_rejects_suspended_dedicated_apply() {
    let root = "/tmp/db_runtime_manifest_suspended_close";
    cleanup_test_root(root);
    let mut config = config_with_small_memtable(root);
    config.runtime_manifest_mode = RuntimeManifestMode::Enabled;
    config.governance_mode = GovernanceMode::Noop;
    let db = open_db(config);
    db.runtime_manifest_publisher
        .as_ref()
        .unwrap()
        .suspend_for_owner("close-test-job")
        .unwrap();

    let err = db.close().expect_err("close must reject suspended publish");
    assert!(err.to_string().contains("suspended for close-test-job"));
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn runtime_manifest_auto_mode_only_enables_dedicated_compaction() {
    let embedded_root = "/tmp/db_runtime_manifest_auto_embedded";
    cleanup_test_root(embedded_root);
    let mut embedded = config_with_small_memtable(embedded_root);
    embedded.governance_mode = GovernanceMode::Noop;
    let db = open_db(embedded);
    assert!(
        runtime_manifest_store(&db)
            .load_current()
            .unwrap()
            .is_none()
    );
    db.close().unwrap();
    cleanup_test_root(embedded_root);

    let dedicated_root = "/tmp/db_runtime_manifest_auto_dedicated";
    cleanup_test_root(dedicated_root);
    let mut dedicated = config_with_small_memtable(dedicated_root);
    dedicated.governance_mode = GovernanceMode::Noop;
    dedicated.compaction_mode = CompactionMode::Dedicated;
    let db = open_db(dedicated);
    assert!(
        runtime_manifest_store(&db)
            .load_current()
            .unwrap()
            .is_some()
    );
    db.close().unwrap();
    cleanup_test_root(dedicated_root);

    let disabled_root = "/tmp/db_runtime_manifest_dedicated_disabled";
    cleanup_test_root(disabled_root);
    let mut disabled = config_with_small_memtable(disabled_root);
    disabled.governance_mode = GovernanceMode::Noop;
    disabled.compaction_mode = CompactionMode::Dedicated;
    disabled.runtime_manifest_mode = RuntimeManifestMode::Disabled;
    let db = open_db(disabled);
    assert!(
        runtime_manifest_store(&db)
            .load_current()
            .unwrap()
            .is_none()
    );
    db.close().unwrap();
    cleanup_test_root(disabled_root);
}

#[test]
#[serial(file)]
fn dedicated_runtime_mode_publishes_flush_without_auto_snapshot() {
    let root = "/tmp/db_runtime_manifest_dedicated_flush";
    cleanup_test_root(root);
    let mut config = config_with_small_memtable(root);
    config.governance_mode = GovernanceMode::Noop;
    config.compaction_mode = CompactionMode::Dedicated;
    let db = open_db(config);

    db.put(0, b"runtime-flush", 0, vec![b'x'; 96]).unwrap();
    db.memtable_manager.flush_active().unwrap();
    db.memtable_manager.wait_for_flushes();

    let store = runtime_manifest_store(&db);
    let runtime = wait_for_runtime_generation_at_least(&store, 2);
    assert_eq!(runtime.manifest.compaction_mode, CompactionMode::Dedicated);
    assert!(
        runtime
            .manifest
            .tree_levels
            .iter()
            .flat_map(|levels| levels.iter())
            .any(|level| !level.files.is_empty())
    );
    assert!(
        crate::snapshot::manifest::list_snapshot_manifest_ids(&db.file_manager)
            .unwrap()
            .is_empty()
    );
    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn dedicated_snapshot_mode_publishes_on_flush() {
    let root = "/tmp/db_runtime_manifest_dedicated_snapshot";
    cleanup_test_root(root);
    let mut config = config_with_small_memtable(root);
    config.governance_mode = GovernanceMode::Noop;
    config.compaction_mode = CompactionMode::Dedicated;
    config.runtime_manifest_mode = RuntimeManifestMode::Disabled;
    let db = open_db(config);

    db.put(0, b"snapshot-flush", 0, vec![b'x'; 96]).unwrap();
    db.memtable_manager.flush_active().unwrap();
    db.memtable_manager.wait_for_flushes();
    for _ in 0..100 {
        if !crate::snapshot::manifest::list_snapshot_manifest_ids(&db.file_manager)
            .unwrap()
            .is_empty()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !crate::snapshot::manifest::list_snapshot_manifest_ids(&db.file_manager)
            .unwrap()
            .is_empty()
    );
    assert!(
        runtime_manifest_store(&db)
            .load_current()
            .unwrap()
            .is_none()
    );
    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn runtime_manifest_generation_continues_on_reopen() {
    let root = "/tmp/db_runtime_manifest_reopen";
    cleanup_test_root(root);
    let mut config = config_with_small_memtable(root);
    config.runtime_manifest_mode = RuntimeManifestMode::Enabled;
    config.governance_mode = GovernanceMode::Noop;
    let ranges = vec![full_bucket_range(config.total_buckets)];

    let db = DbBuilder::new(config.clone())
        .bucket_ranges(ranges.clone())
        .db_id("runtime-manifest-reopen")
        .open()
        .unwrap();
    let first_generation = runtime_manifest_store(&db)
        .load_current()
        .unwrap()
        .expect("initial manifest")
        .generation;
    db.close().unwrap();

    let reopened = DbBuilder::new(config)
        .bucket_ranges(ranges)
        .db_id("runtime-manifest-reopen")
        .open()
        .unwrap();
    let second_generation = runtime_manifest_store(&reopened)
        .load_current()
        .unwrap()
        .expect("reopened manifest")
        .generation;
    assert!(second_generation > first_generation);
    reopened.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn runtime_manifest_enabled_open_rejects_corrupt_current_without_overwriting_it() {
    let root = "/tmp/db_runtime_manifest_corrupt_current";
    cleanup_test_root(root);
    let mut config = config_with_small_memtable(root);
    config.runtime_manifest_mode = RuntimeManifestMode::Enabled;
    config.governance_mode = GovernanceMode::Noop;
    let ranges = vec![full_bucket_range(config.total_buckets)];
    let db_id = "runtime-manifest-corrupt-current";

    let db = DbBuilder::new(config.clone())
        .bucket_ranges(ranges.clone())
        .db_id(db_id)
        .open()
        .unwrap();
    let file_manager = Arc::clone(&db.file_manager);
    db.close().unwrap();
    let mut writer = file_manager
        .create_metadata_file("runtime/CURRENT")
        .unwrap();
    writer.write(b"not-a-generation\n").unwrap();
    writer.close().unwrap();

    let error = match DbBuilder::new(config)
        .bucket_ranges(ranges)
        .db_id(db_id)
        .open()
    {
        Ok(_) => panic!("corrupt runtime CURRENT unexpectedly opened the DB"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("Runtime CURRENT"));
    assert!(
        crate::runtime_manifest::RuntimeManifestStore::new(file_manager)
            .load_current()
            .is_err()
    );
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_write_batch_put_coalesces_with_flush() {
    let root = "/tmp/db_write_batch_put";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = open_db(config);
    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"old");
    batch.put(0, b"k1", 0, b"new");
    batch.put(0, b"k2", 0, vec![b'x'; 64]);
    db.write_batch(batch).unwrap();

    let _ = db.memtable_manager.wait_for_flushes();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let value = db.get(0, b"k1").unwrap().expect("value present");
    let col = value[0].as_ref().unwrap();
    assert_eq!(col.as_ref(), b"new");

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_column_batch_rotates_memtables_without_losing_entries() {
    let root = "/tmp/db_column_batch_rotation";
    cleanup_test_root(root);
    let db = open_db(config_with_small_memtable(root));
    let entries = (0..20)
        .map(|index| {
            (
                format!("key-{index}").into_bytes(),
                vec![b'a' + (index % 20) as u8; 48],
            )
        })
        .collect::<Vec<_>>();

    db.put_column_batch_with_options(
        0,
        0,
        entries
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice())),
        &WriteOptions::default(),
    )
    .unwrap();

    for (key, expected) in &entries {
        let value = db.get(0, key).unwrap().expect("batch value present");
        assert_eq!(value[0].as_ref().unwrap().as_ref(), expected.as_slice());
    }
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_write_routes_non_default_column_family_to_separate_tree() {
    let root = "/tmp/db_write_cf_routing";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = open_db(config);
    let mut schema = db.update_schema();
    schema
        .add_column(0, None, None, Some("metrics".to_string()))
        .unwrap();
    let latest_schema = schema.commit();
    let metrics_cf = latest_schema
        .resolve_column_family_id(Some("metrics"))
        .unwrap();

    db.put_with_options(
        0,
        b"k_cf",
        0,
        b"v_cf",
        &WriteOptions::with_column_family("metrics"),
    )
    .unwrap();

    let mut batch = WriteBatch::new();
    batch.put(0, b"k_default", 0, b"v_default");
    batch.put_with_options(
        0,
        b"k_metrics",
        0,
        b"v_metrics",
        &WriteOptions::with_column_family("metrics"),
    );
    batch.delete_with_options(0, b"k_cf", 0, &WriteOptions::with_column_family("metrics"));
    db.write_batch(batch).unwrap();

    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let snapshot = db.db_state.load();
    let default_tree_idx = snapshot
        .multi_lsm_version
        .tree_index_for_bucket_and_column_family(0, DEFAULT_COLUMN_FAMILY_ID)
        .unwrap();
    let metrics_tree_idx = snapshot
        .multi_lsm_version
        .tree_index_for_bucket_and_column_family(0, metrics_cf)
        .unwrap();
    assert_ne!(default_tree_idx, metrics_tree_idx);
    assert!(
        !db.lsm_tree
            .level_files_in_tree(default_tree_idx, 0)
            .is_empty()
    );
    assert!(
        !db.lsm_tree
            .level_files_in_tree(metrics_tree_idx, 0)
            .is_empty()
    );

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_non_default_single_column_family_round_trip_and_scan() {
    let root = "/tmp/db_cf_single_column_roundtrip";
    cleanup_test_root(root);
    let config = Config {
        num_columns: 2,
        ..config_with_small_memtable(root)
    };
    let db = open_db(config);
    let mut schema = db.update_schema();
    schema
        .add_column(0, None, None, Some("metrics".to_string()))
        .unwrap();
    schema.commit();

    db.put_with_options(
        0,
        b"k1",
        0,
        b"v1",
        &WriteOptions::with_column_family("metrics"),
    )
    .unwrap();
    db.put_with_options(
        0,
        b"k2",
        0,
        b"v2",
        &WriteOptions::with_column_family("metrics"),
    )
    .unwrap();

    let value = db
        .get_with_options(0, b"k1", &ReadOptions::for_column_in_family("metrics", 0))
        .unwrap()
        .expect("value present");
    assert_eq!(value.len(), 1);
    assert_eq!(value[0].as_ref().unwrap().as_ref(), b"v1");

    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let mut iter = db
        .scan_with_options(
            0,
            b"k1".as_slice()..b"k3".as_slice(),
            &ScanOptions::for_column(0).with_column_family("metrics"),
        )
        .unwrap();

    let (k1, cols1) = iter.next().unwrap().unwrap();
    assert_eq!(k1.as_ref(), b"k1");
    assert_eq!(cols1.len(), 1);
    assert_eq!(cols1[0].as_ref().unwrap().as_ref(), b"v1");

    let (k2, cols2) = iter.next().unwrap().unwrap();
    assert_eq!(k2.as_ref(), b"k2");
    assert_eq!(cols2.len(), 1);
    assert_eq!(cols2[0].as_ref().unwrap().as_ref(), b"v2");

    assert!(iter.next().is_none());

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_custom_merge_operator_per_column() {
    let root = "/tmp/db_custom_merge_operator";
    cleanup_test_root(root);
    let config = Config {
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 2,
        sst_bloom_filter_enabled: true,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        ..Config::default()
    };
    let db = open_db(config);
    let mut schema = db.update_schema();
    schema
        .set_column_operator(None, 0, Arc::new(PipeMergeOperator))
        .unwrap();
    let _ = schema.commit();

    db.put(0, b"k1", 0, b"base0").unwrap();
    db.merge(0, b"k1", 0, b"a").unwrap();
    db.merge(0, b"k1", 0, b"b").unwrap();
    db.put(0, b"k1", 1, b"base1").unwrap();
    db.merge(0, b"k1", 1, b"a").unwrap();
    db.merge(0, b"k1", 1, b"b").unwrap();

    let value = db.get(0, b"k1").unwrap().expect("value present");
    assert_eq!(value[0].as_ref().unwrap().as_ref(), b"base0|a|b");
    assert_eq!(value[1].as_ref().unwrap().as_ref(), b"base1ab");

    let mut batch = WriteBatch::new();
    batch.merge(0, b"k2", 0, b"a");
    batch.merge(0, b"k2", 0, b"b");
    db.write_batch(batch).unwrap();

    let value = db.get(0, b"k2").unwrap().expect("value present");
    assert_eq!(value[0].as_ref().unwrap().as_ref(), b"a|b");

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_get_batches_contiguous_merge_operands() {
    let root = "/tmp/db_get_batches_merges";
    cleanup_test_root(root);
    let mut config = config_with_small_memtable(root);
    config.memtable_capacity = Size::from_const(1024 * 1024);
    let db = open_db(config);
    let operator = Arc::new(BatchCountingMergeOperator::default());
    let mut schema = db.update_schema();
    schema
        .set_column_operator(None, 0, Arc::clone(&operator) as Arc<dyn MergeOperator>)
        .unwrap();
    schema.commit();

    db.put(0, b"k1", 0, b"base").unwrap();
    for operand in [b"-a".as_slice(), b"-b".as_slice(), b"-c".as_slice()] {
        db.merge(0, b"k1", 0, operand).unwrap();
    }

    let value = db.get(0, b"k1").unwrap().expect("value present");
    assert_eq!(value[0].as_deref(), Some(b"base-a-b-c".as_slice()));
    assert_eq!(operator.merge_batch_calls.load(AtomicOrdering::Relaxed), 1);
    assert_eq!(operator.merge_calls.load(AtomicOrdering::Relaxed), 0);
    cleanup_test_root(root);
}

#[test]
fn test_resolve_separated_array_batches_merges_and_resets_on_put() {
    let operator = Arc::new(BatchCountingMergeOperator::default());
    let first_pointer = VlogPointer::new(1, 10).to_bytes();
    let second_pointer = VlogPointer::new(1, 20).to_bytes();
    let items = [
        Column::new(ValueType::Merge, b"-discarded".to_vec()),
        Column::new(ValueType::MergeSeparated, first_pointer.to_vec()),
        Column::new(ValueType::Put, b"reset".to_vec()),
        Column::new(ValueType::MergeSeparated, second_pointer.to_vec()),
        Column::new(ValueType::Merge, b"-last".to_vec()),
    ];
    let refs: Vec<_> = items
        .iter()
        .map(|item| RefColumn::new(item.value_type, item.data()))
        .collect();
    let encoded = encode_merge_separated_array(&refs).unwrap();
    let column = Column::new(ValueType::PutSeparatedArray, encoded);

    let resolved = resolve_column_with_vlog(
        column,
        &mut |pointer| {
            if pointer.offset() == 10 {
                Ok(Bytes::from_static(b"-first"))
            } else {
                assert_eq!(pointer.offset(), 20);
                Ok(Bytes::from_static(b"-second"))
            }
        },
        operator.as_ref(),
        None,
    )
    .unwrap()
    .unwrap();

    assert_eq!(resolved.as_ref(), b"reset-second-last");
    assert_eq!(operator.merge_batch_calls.load(AtomicOrdering::Relaxed), 1);
    assert_eq!(operator.merge_calls.load(AtomicOrdering::Relaxed), 0);
}

#[test]
#[serial(file)]
fn test_db_counter_merge_operators_code_path() {
    let root = "/tmp/db_counter_merge_operator";
    cleanup_test_root(root);
    let config = Config {
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 2,
        sst_bloom_filter_enabled: true,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        ..Config::default()
    };
    let db = open_db(config);
    let mut schema = db.update_schema();
    schema
        .set_column_operator(None, 0, Arc::new(U32CounterMergeOperator))
        .unwrap();
    schema
        .set_column_operator(None, 1, Arc::new(U64CounterMergeOperator))
        .unwrap();
    let _ = schema.commit();

    db.put(0, b"k1", 0, 10u32.to_le_bytes()).unwrap();
    db.merge(0, b"k1", 0, 2u32.to_le_bytes()).unwrap();
    db.merge(0, b"k1", 0, 3u32.to_le_bytes()).unwrap();
    db.put(0, b"k1", 1, 100u64.to_le_bytes()).unwrap();
    db.merge(0, b"k1", 1, 11u64.to_le_bytes()).unwrap();

    let value = db.get(0, b"k1").unwrap().expect("value present");
    assert_eq!(
        decode_u32_counter(value[0].as_ref().unwrap().as_ref()),
        15u32
    );
    assert_eq!(
        decode_u64_counter(value[1].as_ref().unwrap().as_ref()),
        111u64
    );

    let mut batch = WriteBatch::new();
    batch.merge(0, b"k2", 0, 4u32.to_le_bytes());
    batch.merge(0, b"k2", 0, 5u32.to_le_bytes());
    batch.merge(0, b"k2", 1, 7u64.to_le_bytes());
    batch.merge(0, b"k2", 1, 8u64.to_le_bytes());
    db.write_batch(batch).unwrap();

    let value = db.get(0, b"k2").unwrap().expect("value present");
    assert_eq!(
        decode_u32_counter(value[0].as_ref().unwrap().as_ref()),
        9u32
    );
    assert_eq!(
        decode_u64_counter(value[1].as_ref().unwrap().as_ref()),
        15u64
    );

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_get_evolves_older_schema_values() {
    let root = "/tmp/db_schema_evolution_get";
    cleanup_test_root(root);
    let config = Config {
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        sst_bloom_filter_enabled: true,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        ..Config::default()
    };
    let db = open_db(config);

    db.put(0, b"k1", 0, b"v1").unwrap();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let mut schema = db.update_schema();
    schema.add_column(1, None, None, None).unwrap();
    let _ = schema.commit();

    let value = db.get(0, b"k1").unwrap().expect("value present");
    assert_eq!(value.len(), 2);
    assert_eq!(value[0].as_ref().unwrap().as_ref(), b"v1");
    assert!(value[1].is_none());

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_memtable_read_evolves_older_schema_values() {
    let root = "/tmp/db_schema_evolution_memtable_read";
    cleanup_test_root(root);
    let config = Config {
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        sst_bloom_filter_enabled: true,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        ..Config::default()
    };
    let db = open_db(config);

    db.put(0, b"k1", 0, b"v1").unwrap();

    let mut schema = db.update_schema();
    schema.add_column(1, None, None, None).unwrap();
    let _ = schema.commit();

    let value = db.get(0, b"k1").unwrap().expect("value present");
    assert_eq!(value.len(), 2);
    assert_eq!(value[0].as_ref().unwrap().as_ref(), b"v1");
    assert!(value[1].is_none());

    let mut iter = db.scan(0, b"k1".as_slice()..b"k2".as_slice()).unwrap();
    let (scan_key, columns) = iter.next().unwrap().unwrap();
    assert_eq!(scan_key.as_ref(), b"k1");
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].as_ref().unwrap().as_ref(), b"v1");
    assert!(columns[1].is_none());
    assert!(iter.next().is_none());

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_rejects_separated_value_type_input() {
    let root = "/tmp/db_reject_separated_input";
    cleanup_test_root(root);
    let db = open_db(config_with_small_memtable(root));

    for value_type in [
        ValueType::PutSeparated,
        ValueType::MergeSeparated,
        ValueType::MergeSeparatedArray,
        ValueType::PutSeparatedArray,
    ] {
        let err = db
            .write_ref(0, b"k1", 0, value_type, b"value", &WriteOptions::default())
            .unwrap_err();
        assert!(matches!(err, Error::InputError(_)));
    }

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_value_separation_get_from_memtable_before_flush() {
    let root = "/tmp/db_value_separation_memtable";
    cleanup_test_root(root);
    let config = Config {
        value_separation_threshold: Some(Size::from_const(8)),
        ..config_with_small_memtable(root)
    };
    let db = open_db(config);
    let large = b"value-larger-than-threshold";
    db.put(0, b"k1", 0, large).unwrap();

    let value = db.get(0, b"k1").unwrap().expect("value present");
    assert_eq!(value[0].as_ref().unwrap().as_ref(), large);

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_value_separation_flush_and_get() {
    use crate::sst::row_codec::decode_value;
    use crate::sst::{SSTIterator, SSTIteratorOptions};

    let root = "/tmp/db_value_separation";
    cleanup_test_root(root);
    let config = Config {
        value_separation_threshold: Some(Size::from_const(8)),
        ..config_with_small_memtable(root)
    };
    let db = open_db(config);
    let large = b"value-larger-than-threshold";
    db.put(0, b"k1", 0, large).unwrap();

    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let level0 = db.lsm_tree.level_files(0);
    assert_eq!(level0.len(), 1);
    let data_file = Arc::clone(&level0[0]);
    let reader = db
        .file_manager
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
    let (_, mut raw_value) = iter.current().unwrap().unwrap();
    let decoded = decode_value(&mut raw_value, 1).unwrap();
    let column = decoded
        .columns()
        .first()
        .and_then(|col| col.as_ref())
        .expect("column present");
    assert_eq!(column.value_type, ValueType::PutSeparated);
    assert_eq!(column.data().len(), 8);

    let value = db.get(0, b"k1").unwrap().expect("value present");
    assert_eq!(value[0].as_ref().unwrap().as_ref(), large);

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_get_resolves_merge_separated_array() {
    let root = "/tmp/db_get_merge_separated_array";
    cleanup_test_root(root);
    let config = Config {
        value_separation_threshold: Some(Size::from_const(4)),
        ..config_with_small_memtable(root)
    };
    let db = open_db(config);

    db.put(0, b"k1", 0, b"base-separated").unwrap();
    db.merge(0, b"k1", 0, b"-suffix-separated").unwrap();

    let value = db.get(0, b"k1").unwrap().expect("value present");
    assert_eq!(
        value[0].as_ref().unwrap().as_ref(),
        b"base-separated-suffix-separated"
    );

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_get_prefers_newer_l0_file() {
    let root = "/tmp/db_get_newer_l0";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = open_db(config);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"old");
    batch.put(0, b"k2", 0, vec![b'a'; 64]);
    db.write_batch(batch).unwrap();
    let _ = db.memtable_manager.wait_for_flushes();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"new");
    batch.put(0, b"k3", 0, vec![b'b'; 64]);
    db.write_batch(batch).unwrap();
    let _ = db.memtable_manager.wait_for_flushes();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let value = db.get(0, b"k1").unwrap().expect("value present");
    let col = value[0].as_ref().unwrap();
    assert_eq!(col.as_ref(), b"new");

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_multi_get_preserves_order_duplicates_and_memtable_l0_merges() {
    let root = "/tmp/db_multi_get";
    cleanup_test_root(root);
    let db = open_db(config_with_small_memtable(root));

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"base");
    batch.put(0, b"k2", 0, b"old");
    batch.put(0, b"pad", 0, vec![b'x'; 64]);
    db.write_batch(batch).unwrap();
    db.memtable_manager.flush_active().unwrap();
    db.memtable_manager.wait_for_flushes();

    db.merge(0, b"k1", 0, b"-memtable").unwrap();
    db.put(0, b"k3", 0, b"fresh").unwrap();
    let values = db
        .multi_get(&[
            (0, b"k2".as_slice()),
            (0, b"k1"),
            (0, b"k2"),
            (0, b"missing"),
            (0, b"k3"),
        ])
        .unwrap();

    assert_eq!(values.len(), 5);
    assert_eq!(
        values[0].as_ref().unwrap()[0].as_deref(),
        Some(b"old".as_slice())
    );
    assert_eq!(
        values[1].as_ref().unwrap()[0].as_deref(),
        Some(b"base-memtable".as_slice())
    );
    assert_eq!(values[2], values[0]);
    assert!(values[3].is_none());
    assert_eq!(
        values[4].as_ref().unwrap()[0].as_deref(),
        Some(b"fresh".as_slice())
    );

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_multi_get_matches_get_for_generic_keys_buckets_and_projection() {
    let root = "/tmp/db_multi_get_options";
    cleanup_test_root(root);
    let db = DbBuilder::new(Config {
        total_buckets: 2,
        num_columns: 2,
        ..config_with_small_memtable(root)
    })
    .bucket_ranges(vec![0..=0, 1..=1])
    .open()
    .unwrap();
    db.put(0, b"left", 0, b"left-0").unwrap();
    db.put(0, b"left", 1, b"left-1").unwrap();
    db.put(1, b"right", 0, b"right-0").unwrap();
    db.put(1, b"right", 1, b"right-1").unwrap();
    let options = ReadOptions::for_columns(vec![1]);
    let keys = vec![
        (1, b"right".to_vec()),
        (0, b"left".to_vec()),
        (1, b"right".to_vec()),
        (0, b"missing".to_vec()),
    ];

    let expected = keys
        .iter()
        .map(|(bucket, key)| db.get_with_options(*bucket, key, &options))
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        db.multi_get_with_options(&keys, &options).unwrap(),
        expected
    );
    let empty: Vec<(u16, Vec<u8>)> = Vec::new();
    assert!(db.multi_get(&empty).unwrap().is_empty());

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_read_only_db_multi_get_matches_snapshot_get() {
    let root = "/tmp/read_only_db_multi_get";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = open_db(config.clone());
    db.put(0, b"k1", 0, b"snapshot-1").unwrap();
    db.put(0, b"k2", 0, b"snapshot-2").unwrap();
    let (tx, rx) = mpsc::channel();
    let snapshot_id = db
        .snapshot_with_callback(move |result| {
            tx.send(result).expect("send snapshot result");
        })
        .unwrap();
    rx.recv_timeout(Duration::from_secs(10))
        .expect("receive snapshot result")
        .unwrap();
    let read_only = Db::open_read_only(config, snapshot_id, db.id().to_string()).unwrap();
    let keys = vec![
        (0, b"k2".to_vec()),
        (0, b"k1".to_vec()),
        (0, b"k2".to_vec()),
        (0, b"missing".to_vec()),
    ];

    let expected = keys
        .iter()
        .map(|(bucket, key)| read_only.get(*bucket, key))
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(read_only.multi_get(&keys).unwrap(), expected);

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_get_merges_across_l0_files() {
    let root = "/tmp/db_get_merge_l0";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = open_db(config);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"base");
    batch.put(0, b"k2", 0, vec![b'a'; 64]);
    db.write_batch(batch).unwrap();
    let _ = db.memtable_manager.wait_for_flushes();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let mut batch = WriteBatch::new();
    batch.merge(0, b"k1", 0, b"_x");
    batch.put(0, b"k3", 0, vec![b'b'; 64]);
    db.write_batch(batch).unwrap();
    let _ = db.memtable_manager.wait_for_flushes();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let value = db.get(0, b"k1").unwrap().expect("value present");
    let col = value[0].as_ref().unwrap();
    assert_eq!(col.as_ref(), b"base_x");

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_get_memtable_overlaps_l0_value() {
    let root = "/tmp/db_get_memtable_overlaps_l0";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = open_db(config);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"old");
    batch.put(0, b"k2", 0, vec![b'a'; 64]);
    db.write_batch(batch).unwrap();
    let _ = db.memtable_manager.wait_for_flushes();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"new");
    db.write_batch(batch).unwrap();

    let value = db.get(0, b"k1").unwrap().expect("value present");
    let col = value[0].as_ref().unwrap();
    assert_eq!(col.as_ref(), b"new");

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_get_active_merge_collects_terminal_base() {
    let root = "/tmp/db_get_active_merge_terminal";
    cleanup_test_root(root);
    let db = open_db(config_with_small_memtable(root));

    db.put(0, b"k1", 0, b"base").unwrap();
    db.merge(0, b"k1", 0, b"_merge").unwrap();

    let value = db.get(0, b"k1").unwrap().expect("value present");
    assert_eq!(value[0].as_deref(), Some(b"base_merge".as_slice()));
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_get_active_delete_hides_l0_value() {
    let root = "/tmp/db_get_active_delete_terminal";
    cleanup_test_root(root);
    let db = open_db(config_with_small_memtable(root));

    db.put(0, b"k1", 0, b"old").unwrap();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();
    db.delete(0, b"k1", 0).unwrap();

    assert_eq!(db.get(0, b"k1").unwrap(), None);
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_multi_column_expired_terminal_keeps_l0_column_masked() {
    let root = "/tmp/db_multi_column_expired_terminal_mask";
    cleanup_test_root(root);
    let mut config = Config {
        num_columns: 2,
        ..config_with_small_memtable(root)
    };
    config.ttl_enabled = true;
    let db = open_db(config);

    db.put(0, b"k1", 0, b"c0-old").unwrap();
    db.put(0, b"k1", 1, b"c1-old").unwrap();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    db.put_with_options(0, b"k1", 0, b"c0-expired", &WriteOptions::with_ttl(1))
        .unwrap();
    std::thread::sleep(Duration::from_millis(1_100));

    let value = db.get(0, b"k1").unwrap().expect("remaining column present");
    assert_eq!(value[0], None);
    assert_eq!(value[1].as_deref(), Some(b"c1-old".as_slice()));
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_get_memtable_merges_with_l0_value() {
    let root = "/tmp/db_get_memtable_merge_l0";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = open_db(config);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"base");
    batch.put(0, b"k2", 0, vec![b'a'; 64]);
    db.write_batch(batch).unwrap();
    let _ = db.memtable_manager.wait_for_flushes();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let mut batch = WriteBatch::new();
    batch.merge(0, b"k1", 0, b"_x");
    db.write_batch(batch).unwrap();

    let value = db.get(0, b"k1").unwrap().expect("value present");
    let col = value[0].as_ref().unwrap();
    assert_eq!(col.as_ref(), b"base_x");

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_multi_column_overrides_column_only() {
    let root = "/tmp/db_multi_column_override";
    cleanup_test_root(root);
    let config = Config {
        num_columns: 2,
        ..config_with_small_memtable(root)
    };
    let db = open_db(config);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"c0-old");
    batch.put(0, b"k1", 1, b"c1-old");
    batch.put(0, b"k2", 0, vec![b'a'; 64]);
    db.write_batch(batch).unwrap();
    let _ = db.memtable_manager.wait_for_flushes();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 1, b"c1-new");
    db.write_batch(batch).unwrap();

    let value = db.get(0, b"k1").unwrap().expect("value present");
    let col0 = value[0].as_ref().unwrap();
    let col1 = value[1].as_ref().unwrap();
    assert_eq!(col0.as_ref(), b"c0-old");
    assert_eq!(col1.as_ref(), b"c1-new");

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_multi_column_merge_across_l0() {
    let root = "/tmp/db_multi_column_merge_l0";
    cleanup_test_root(root);
    let config = Config {
        num_columns: 2,
        ..config_with_small_memtable(root)
    };
    let db = open_db(config);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"c0");
    batch.put(0, b"k1", 1, b"c1");
    batch.put(0, b"k2", 0, vec![b'a'; 64]);
    db.write_batch(batch).unwrap();
    let _ = db.memtable_manager.wait_for_flushes();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let mut batch = WriteBatch::new();
    batch.merge(0, b"k1", 1, b"_x");
    batch.put(0, b"k3", 0, vec![b'b'; 64]);
    db.write_batch(batch).unwrap();
    let _ = db.memtable_manager.wait_for_flushes();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let value = db.get(0, b"k1").unwrap().expect("value present");
    let col0 = value[0].as_ref().unwrap();
    let col1 = value[1].as_ref().unwrap();
    assert_eq!(col0.as_ref(), b"c0");
    assert_eq!(col1.as_ref(), b"c1_x");

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_get_with_column_index() {
    let root = "/tmp/db_get_column_index";
    cleanup_test_root(root);
    let config = Config {
        num_columns: 2,
        ..config_with_small_memtable(root)
    };
    let db = open_db(config);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"c0");
    batch.put(0, b"k1", 1, b"c1");
    db.write_batch(batch).unwrap();

    let value = db
        .get_with_options(0, b"k1", &ReadOptions::for_columns(vec![1, 0]))
        .unwrap()
        .expect("value present");
    assert_eq!(value.len(), 2);
    assert_eq!(value[0].as_ref().unwrap().as_ref(), b"c1");
    assert_eq!(value[1].as_ref().unwrap().as_ref(), b"c0");

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_scan_range_merges_memtable_and_l0() {
    let root = "/tmp/db_scan_range";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = open_db(config);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"old");
    batch.put(0, b"z1", 0, vec![b'a'; 64]);
    db.write_batch(batch).unwrap();
    let _ = db.memtable_manager.wait_for_flushes();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    db.put(0, b"k1", 0, b"new").unwrap();
    db.put(0, b"k2", 0, b"v2").unwrap();

    let iter = db.scan(0, b"k1".as_slice()..b"k3".as_slice()).unwrap();
    let mut rows = Vec::new();
    for row in iter {
        let (key, columns) = row.unwrap();
        rows.push((key, columns[0].clone()));
    }
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0.as_ref(), b"k1");
    assert_eq!(rows[0].1.as_ref().unwrap().as_ref(), b"new");
    assert_eq!(rows[1].0.as_ref(), b"k2");
    assert_eq!(rows[1].1.as_ref().unwrap().as_ref(), b"v2");

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_get_with_projected_merge_operator_column() {
    let root = "/tmp/db_get_projected_merge_operator_column";
    cleanup_test_root(root);
    let config = Config {
        num_columns: 2,
        ..config_with_small_memtable(root)
    };
    let db = open_db(config);

    let mut schema = db.update_schema();
    schema
        .set_column_operator(None, 1, Arc::new(U64CounterMergeOperator))
        .unwrap();
    let _ = schema.commit();

    db.put(0, b"k1", 0, b"base").unwrap();
    db.put(0, b"k1", 1, 1u64.to_le_bytes()).unwrap();
    db.merge(0, b"k1", 1, 10u64.to_le_bytes()).unwrap();

    let value = db
        .get_with_options(0, b"k1", &ReadOptions::for_column(1))
        .unwrap()
        .expect("value present");
    assert_eq!(value.len(), 1);
    assert_eq!(
        decode_u64_counter(value[0].as_ref().unwrap().as_ref()),
        11u64
    );

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_scan_holds_snapshot_until_drop() {
    let root = "/tmp/db_scan_snapshot";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = open_db(config);

    db.put(0, b"k1", 0, b"old").unwrap();
    let mut iter = db.scan(0, b"".as_slice()..b"\xff".as_slice()).unwrap();
    db.put(0, b"k1", 0, b"new").unwrap();

    let (key, columns) = iter.next().unwrap().unwrap();
    assert_eq!(key.as_ref(), b"k1");
    assert_eq!(columns[0].as_ref().unwrap().as_ref(), b"old");
    assert!(iter.next().is_none());

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_close_waits_for_scan_iterator_drop() {
    let root = "/tmp/db_close_waits_for_scan_iterator";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = Arc::new(open_db(config));

    db.put(0, b"k1", 0, b"v1").unwrap();
    let iter = db.scan(0, b"".as_slice()..b"\xff".as_slice()).unwrap();

    let close_db = Arc::clone(&db);
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        close_db.close().unwrap();
        done_tx.send(()).unwrap();
    });

    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());

    drop(iter);

    done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    handle.join().unwrap();

    let err = db.get(0, b"k1").unwrap_err();
    assert!(matches!(err, Error::InvalidState(_)));

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_close_waits_for_schema_builder_drop() {
    let root = "/tmp/db_close_waits_for_schema_builder";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = Arc::new(open_db(config));

    let schema_builder = db.update_schema();

    let close_db = Arc::clone(&db);
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        close_db.close().unwrap();
        done_tx.send(()).unwrap();
    });

    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());

    drop(schema_builder);

    done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    handle.join().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_scan_with_column_indices() {
    let root = "/tmp/db_scan_column_indices";
    cleanup_test_root(root);
    let config = Config {
        num_columns: 2,
        ..config_with_small_memtable(root)
    };
    let db = open_db(config);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"c0-1");
    batch.put(0, b"k1", 1, b"c1-1");
    batch.put(0, b"k2", 0, b"c0-2");
    batch.put(0, b"k2", 1, b"c1-2");
    db.write_batch(batch).unwrap();

    let mut iter = db
        .scan_with_options(
            0,
            b"k1".as_slice()..b"k3".as_slice(),
            &ScanOptions::for_columns(vec![1, 0]),
        )
        .unwrap();

    let (k1, cols1) = iter.next().unwrap().unwrap();
    assert_eq!(k1.as_ref(), b"k1");
    assert_eq!(cols1.len(), 2);
    assert_eq!(cols1[0].as_ref().unwrap().as_ref(), b"c1-1");
    assert_eq!(cols1[1].as_ref().unwrap().as_ref(), b"c0-1");

    let (k2, cols2) = iter.next().unwrap().unwrap();
    assert_eq!(k2.as_ref(), b"k2");
    assert_eq!(cols2.len(), 2);
    assert_eq!(cols2[0].as_ref().unwrap().as_ref(), b"c1-2");
    assert_eq!(cols2[1].as_ref().unwrap().as_ref(), b"c0-2");

    assert!(iter.next().is_none());

    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_db_scan_with_read_ahead_option() {
    let root = "/tmp/db_scan_read_ahead";
    cleanup_test_root(root);
    let config = config_with_small_memtable(root);
    let db = open_db(config);

    db.put(0, b"k1", 0, b"v1").unwrap();
    db.put(0, b"k2", 0, b"v2").unwrap();
    db.memtable_manager.flush_active().unwrap();
    let _ = db.memtable_manager.wait_for_flushes();

    let mut options = ScanOptions::default();
    options.read_ahead_bytes = Size::from_const(128);
    let iter = db
        .scan_with_options_bounds(0, None, None, &options)
        .unwrap();
    let mut keys = Vec::new();
    for row in iter {
        let (key, _) = row.unwrap();
        keys.push(key);
    }
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0].as_ref(), b"k1");
    assert_eq!(keys[1].as_ref(), b"k2");

    cleanup_test_root(root);
}
