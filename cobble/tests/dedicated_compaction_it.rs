//! Integration tests for the dedicated compaction mode.
//!
//! These tests exercise the end-to-end flow in-process:
//! - The writer opens in `CompactionMode::Dedicated`, which disables in-process compaction
//!   and starts the result poller.
//! - A `DedicatedCompactor` runs in the same process (on a background thread), reading the
//!   writer's snapshots, executing compaction plans, and publishing result files.
//! - The writer's poller discovers the results, applies them, commits a new manifest, and
//!   deletes the result.
//!
//! The tests verify:
//! - Data is readable after compaction (no loss).
//! - Compaction results are consumed (deleted) by the writer.
//! - The manifest reflects the compacted state.
//! - Restart after compaction preserves all data.
use cobble::{CompactionMode, CompactionPolicyKind, Config, Db, DbBuilder, DedicatedCompactor};
use serial_test::serial;
use size::Size;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

fn cleanup_test_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn dedicated_config(root: &str) -> Config {
    Config {
        volumes: cobble::VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_kib(8),
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: Some(32),
        l1_base_bytes: Size::from_kib(8),
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: Size::from_const(0),
        base_file_size: Size::from_kib(4),
        sst_bloom_filter_enabled: true,
        compaction_mode: CompactionMode::Dedicated,
        compaction_dedicated_poll_interval_ms: 200,
        log_console: false,
        log_level: log::LevelFilter::Info,
        ..Config::default()
    }
}

/// Opens a DB with a stable db_id (needed for restart recovery tests so both processes
/// point at the same volume subdirectory).
fn open_db_with_id(config: Config, db_id: &str) -> Db {
    let total_buckets = config.total_buckets;
    let full_range = 0u16..=u16::try_from(total_buckets - 1).expect("total_buckets must fit u16");
    DbBuilder::new(config)
        .bucket_ranges(std::iter::once(full_range).collect())
        .db_id(db_id)
        .open()
        .expect("open db")
}

/// Runs a `DedicatedCompactor` on a background thread, calling `run_once` in a loop until
/// `stop` is set. Retries `open` until snapshots appear.
fn spawn_compactor(config: Config, db_id: String) -> (Arc<AtomicBool>, JoinHandle<()>) {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop);
    let handle = std::thread::Builder::new()
        .name("test-dedicated-compactor".to_string())
        .spawn(move || {
            // Wait for the writer to produce at least one snapshot before opening the compactor.
            let compactor = loop {
                if stop_clone.load(Ordering::SeqCst) {
                    return;
                }
                match DedicatedCompactor::open(config.clone(), db_id.clone()) {
                    Ok(c) => break c,
                    Err(_) => std::thread::sleep(Duration::from_millis(200)),
                }
            };
            while !stop_clone.load(Ordering::SeqCst) {
                if let Err(err) = compactor.run_once() {
                    // Expected when no plan is found.
                    eprintln!("compactor iteration: {}", err);
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        })
        .expect("spawn compactor");
    (stop, handle)
}

/// Waits for a condition to become true, polling at the given interval.
fn wait_for<F: Fn() -> bool>(timeout: Duration, interval: Duration, cond: F) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(interval);
    }
    false
}

/// Counts SST files recursively under the root directory.
fn count_data_files(root: &str) -> usize {
    fn walk(dir: &std::path::Path) -> usize {
        let mut count = 0;
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    count += walk(&path);
                } else if path.extension().and_then(|e| e.to_str()) == Some("sst") {
                    count += 1;
                }
            }
        }
        count
    }
    walk(std::path::Path::new(root))
}

/// Counts manifest snapshot files recursively under the root directory.
fn count_snapshots(root: &str) -> usize {
    fn walk(dir: &std::path::Path) -> usize {
        let mut count = 0;
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    count += walk(&path);
                } else if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("SNAPSHOT-"))
                    .unwrap_or(false)
                {
                    count += 1;
                }
            }
        }
        count
    }
    walk(std::path::Path::new(root))
}

/// Counts compaction result files recursively under the root directory.
fn count_compaction_results(root: &str) -> usize {
    fn walk(dir: &std::path::Path) -> usize {
        let mut count = 0;
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    count += walk(&path);
                } else if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("COMPACTION-"))
                    .unwrap_or(false)
                {
                    count += 1;
                }
            }
        }
        count
    }
    walk(std::path::Path::new(root))
}

/// Verifies a key-value pair via db.get.
fn verify_get(db: &Db, bucket: u16, key: &[u8], expected: &[u8]) {
    let value = db.get(bucket, key).unwrap().expect("value present");
    let col = value[0].as_ref().expect("column present");
    assert_eq!(col.as_ref(), expected);
}

#[test]
#[serial(file)]
fn test_dedicated_compaction_basic() {
    let root = "/tmp/dedicated_compaction_basic";
    cleanup_test_root(root);
    let db_id = "dedicated-compaction-basic".to_string();

    let config = dedicated_config(root);
    let db = open_db_with_id(config.clone(), &db_id);

    // Write enough data to trigger flush and produce L0 files.
    let value = vec![b'v'; 1024];
    for i in 0..40u32 {
        let key = format!("k{:08}", i).into_bytes();
        db.put(0, &key, 0, &value).expect("put");
    }

    // Wait for flush to produce SST files and snapshots.
    assert!(
        wait_for(Duration::from_secs(10), Duration::from_millis(100), || {
            count_data_files(root) > 0 && count_snapshots(root) > 0
        }),
        "flush should produce SST files and snapshots (files={}, snapshots={})",
        count_data_files(root),
        count_snapshots(root)
    );

    let initial_snapshots = count_snapshots(root);

    // Start the compactor (it waits for snapshots internally).
    let (stop, handle) = spawn_compactor(config.clone(), db_id.clone());

    // Wait for compaction: new snapshots from poller manifest commit, and results consumed.
    // We require snapshots to increase BEYOND what flushes produce (at least 2 new snapshots:
    // one from flush, one from compaction commit), and no pending results.
    let compaction_done = wait_for(Duration::from_secs(60), Duration::from_millis(500), || {
        let snapshots = count_snapshots(root);
        let results = count_compaction_results(root);
        // Compaction produces at least 2 new snapshots beyond the initial flush snapshots.
        // Also require no pending results (writer consumed them).
        snapshots > initial_snapshots + 1 && results == 0
    });

    // Stop the compactor.
    stop.store(true, Ordering::SeqCst);
    let _ = handle.join();

    assert!(
        compaction_done,
        "compaction did not complete within timeout (snapshots={}, results={}, files={})",
        count_snapshots(root),
        count_compaction_results(root),
        count_data_files(root)
    );

    // Verify all data is still readable.
    for i in 0..40u32 {
        let key = format!("k{:08}", i).into_bytes();
        verify_get(&db, 0, &key, &value);
    }

    db.close().expect("close");
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_dedicated_compaction_multiple_rounds() {
    let root = "/tmp/dedicated_compaction_multi";
    cleanup_test_root(root);
    let db_id = "dedicated-compaction-multi".to_string();

    let config = dedicated_config(root);
    let db = open_db_with_id(config.clone(), &db_id);

    // Start the compactor.
    let (stop, handle) = spawn_compactor(config.clone(), db_id.clone());

    let value = vec![b'v'; 1024];

    // Write multiple rounds of data with flushes between them.
    for round in 0..3u32 {
        for i in 0..10u32 {
            let key = format!("r{}k{:06}", round, i).into_bytes();
            db.put(0, &key, 0, &value).expect("put");
        }
        // Give the compactor time to process.
        std::thread::sleep(Duration::from_secs(2));
    }

    // Wait for compaction to produce snapshots (manifest commits from poller).
    let initial_snapshots = count_snapshots(root);
    let _compaction_done = wait_for(Duration::from_secs(15), Duration::from_millis(300), || {
        count_snapshots(root) > initial_snapshots
    });

    // Stop the compactor.
    stop.store(true, Ordering::SeqCst);
    let _ = handle.join();

    // Verify all data is still readable.
    for round in 0..3u32 {
        for i in 0..10u32 {
            let key = format!("r{}k{:06}", round, i).into_bytes();
            verify_get(&db, 0, &key, &value);
        }
    }

    db.close().expect("close");
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_dedicated_compaction_restart_recovery() {
    let root = "/tmp/dedicated_compaction_restart";
    cleanup_test_root(root);
    let db_id = "dedicated-compaction-restart".to_string();

    let config = dedicated_config(root);
    let db = open_db_with_id(config.clone(), &db_id);

    let value = vec![b'v'; 1024];

    // Write data.
    for i in 0..40u32 {
        let key = format!("k{:08}", i).into_bytes();
        db.put(0, &key, 0, &value).expect("put");
    }

    // Wait for flush to produce SST files and snapshots.
    assert!(
        wait_for(Duration::from_secs(10), Duration::from_millis(100), || {
            count_data_files(root) > 0 && count_snapshots(root) > 0
        }),
        "flush should produce SST files and snapshots"
    );

    // Start compactor and wait for compaction.
    let initial_snapshots = count_snapshots(root);
    let (stop, handle) = spawn_compactor(config.clone(), db_id.clone());
    let compaction_done = wait_for(Duration::from_secs(30), Duration::from_millis(300), || {
        let snapshots = count_snapshots(root);
        let results = count_compaction_results(root);
        snapshots > initial_snapshots && results == 0
    });
    stop.store(true, Ordering::SeqCst);
    let _ = handle.join();
    assert!(compaction_done, "compaction should have completed");

    // Close the DB and reopen it with the same db_id.
    db.close().expect("close");

    let config2 = dedicated_config(root);
    let db2 = Db::resume(config2, &db_id).expect("resume db");

    // Verify all data is still readable after restart.
    for i in 0..40u32 {
        let key = format!("k{:08}", i).into_bytes();
        verify_get(&db2, 0, &key, &value);
    }

    db2.close().expect("close");
    cleanup_test_root(root);
}
