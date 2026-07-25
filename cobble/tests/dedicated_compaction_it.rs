//! Integration tests for the dedicated compaction mode.
//!
//! These tests exercise the end-to-end flow in-process:
//! - The writer opens in `CompactionMode::Dedicated`, which disables in-process compaction
//!   and starts the result poller.
//! - A `DedicatedCompactor` runs in the same process (on a background thread), reading runtime
//!   manifests by default, executing compaction plans, and publishing result files.
//! - The writer's poller discovers the results, applies them, commits a new manifest, and
//!   deletes the result.
//!
//! The tests verify:
//! - Data is readable after compaction (no loss).
//! - Compaction results are consumed (deleted) by the writer.
//! - The manifest reflects the compacted state.
//! - Restart after compaction preserves all data.
use cobble::{
    CompactionMode, CompactionPolicyKind, Config, Db, DbBuilder, DedicatedCompactor,
    RuntimeManifestMode,
};
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
/// `stop` is set. The compactor can start before a writer; it idles until runtime CURRENT exists.
fn spawn_compactor(config: Config, db_id: String) -> (Arc<AtomicBool>, JoinHandle<()>) {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop);
    let handle = std::thread::Builder::new()
        .name("test-dedicated-compactor".to_string())
        .spawn(move || {
            let compactor = DedicatedCompactor::open(config, db_id).expect("open compactor");
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

/// Returns whether the writer published a runtime CURRENT pointer.
fn runtime_current_exists(root: &str) -> bool {
    fn walk(dir: &std::path::Path) -> bool {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return false;
        };
        entries.flatten().any(|entry| {
            let path = entry.path();
            path.is_dir().then(|| walk(&path)).unwrap_or_else(|| {
                path.file_name().and_then(|name| name.to_str()) == Some("CURRENT")
                    && path
                        .parent()
                        .and_then(|parent| parent.file_name())
                        .and_then(|name| name.to_str())
                        == Some("runtime")
            })
        })
    }
    walk(std::path::Path::new(root))
}

fn find_file(root: &str, predicate: impl Fn(&std::path::Path) -> bool) -> std::path::PathBuf {
    fn walk(
        dir: &std::path::Path,
        predicate: &impl Fn(&std::path::Path) -> bool,
    ) -> Option<std::path::PathBuf> {
        let entries = std::fs::read_dir(dir).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Some(found) = walk(&path, predicate) {
                    return Some(found);
                }
            } else if predicate(&path) {
                return Some(path);
            }
        }
        None
    }
    walk(std::path::Path::new(root), &predicate).expect("matching test file")
}

/// Returns true if a manifest file for the given snapshot id exists under the root directory.
fn snapshot_exists(root: &str, snapshot_id: u64) -> bool {
    let target = format!("SNAPSHOT-{}", snapshot_id);
    fn walk(dir: &std::path::Path, target: &str) -> bool {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if walk(&path, target) {
                        return true;
                    }
                } else if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n == target)
                    .unwrap_or(false)
                {
                    return true;
                }
            }
        }
        false
    }
    walk(std::path::Path::new(root), &target)
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
    // A runtime-driven compactor may start before its writer. It must idle while CURRENT is
    // absent rather than requiring a bootstrap snapshot or falling back to snapshots.
    let (stop, handle) = spawn_compactor(config.clone(), db_id.clone());
    let db = open_db_with_id(config.clone(), &db_id);

    // Write enough data to trigger flush and produce L0 files.
    let value = vec![b'v'; 1024];
    for i in 0..40u32 {
        let key = format!("k{:08}", i).into_bytes();
        db.put(0, &key, 0, &value).expect("put");
    }

    // Dedicated Auto publishes runtime manifests but does not create flush snapshots.
    assert!(
        wait_for(Duration::from_secs(10), Duration::from_millis(100), || {
            count_data_files(root) > 0 && runtime_current_exists(root)
        }),
        "flush should produce SST files and a runtime manifest (files={}, snapshots={})",
        count_data_files(root),
        count_snapshots(root)
    );
    assert_eq!(
        count_snapshots(root),
        0,
        "flush must not auto-snapshot in runtime mode"
    );

    let initial_snapshots = count_snapshots(root);

    // Result application creates a durable snapshot proof, then advances runtime CURRENT.
    let compaction_done = wait_for(Duration::from_secs(60), Duration::from_millis(500), || {
        let snapshots = count_snapshots(root);
        let results = count_compaction_results(root);
        snapshots > initial_snapshots && runtime_current_exists(root) && results == 0
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
fn dedicated_compaction_disabled_runtime_manifests_uses_snapshots() {
    let root = "/tmp/dedicated_compaction_snapshot_mode";
    cleanup_test_root(root);
    let db_id = "dedicated-compaction-snapshot-mode".to_string();
    let mut config = dedicated_config(root);
    config.runtime_manifest_mode = RuntimeManifestMode::Disabled;

    // Explicitly disabled runtime manifests retain the snapshot-driven compatibility path.
    let (stop, handle) = spawn_compactor(config.clone(), db_id.clone());
    let db = open_db_with_id(config, &db_id);
    let value = vec![b'v'; 1024];
    for i in 0..40u32 {
        db.put(0, format!("snapshot-{i:08}").as_bytes(), 0, &value)
            .expect("put");
    }

    assert!(
        wait_for(Duration::from_secs(10), Duration::from_millis(100), || {
            count_data_files(root) > 0 && count_snapshots(root) > 0
        }),
        "snapshot mode should publish a flush snapshot"
    );
    assert!(
        !runtime_current_exists(root),
        "disabled runtime manifests must not create runtime CURRENT"
    );

    let initial_snapshots = count_snapshots(root);
    assert!(
        wait_for(Duration::from_secs(60), Duration::from_millis(500), || {
            count_snapshots(root) > initial_snapshots && count_compaction_results(root) == 0
        }),
        "snapshot-driven compaction did not complete"
    );

    stop.store(true, Ordering::SeqCst);
    let _ = handle.join();
    for i in 0..40u32 {
        verify_get(&db, 0, format!("snapshot-{i:08}").as_bytes(), &value);
    }
    db.close().expect("close");
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn dedicated_runtime_publish_failure_preserves_result_until_retry() {
    let root = "/tmp/dedicated_compaction_runtime_retry";
    cleanup_test_root(root);
    let db_id = "dedicated-compaction-runtime-retry".to_string();
    let mut config = dedicated_config(root);
    // Leave a deterministic window between result publication and writer polling.
    config.compaction_dedicated_poll_interval_ms = 5_000;
    let db = open_db_with_id(config.clone(), &db_id);
    let value = vec![b'v'; 1024];
    for i in 0..40u32 {
        db.put(0, format!("retry-{i:08}").as_bytes(), 0, &value)
            .expect("put");
    }
    assert!(wait_for(
        Duration::from_secs(10),
        Duration::from_millis(50),
        || count_data_files(root) > 0 && runtime_current_exists(root)
    ));

    let current = find_file(root, |path| {
        path.file_name().and_then(|name| name.to_str()) == Some("CURRENT")
            && path
                .parent()
                .and_then(|parent| parent.file_name())
                .and_then(|name| name.to_str())
                == Some("runtime")
    });
    let valid_current = std::fs::read(&current).unwrap();
    let initial_snapshots = count_snapshots(root);
    let (stop, handle) = spawn_compactor(config, db_id);
    assert!(
        wait_for(Duration::from_secs(10), Duration::from_millis(20), || {
            count_compaction_results(root) == 1
        }),
        "compactor should publish a result before the writer's delayed poll"
    );
    let result_file = find_file(root, |path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("COMPACTION-"))
    });
    let valid_result = std::fs::read(&result_file).unwrap();

    // The LSM edit and snapshot proof may complete, but the corrupted CURRENT makes the runtime
    // barrier fail. The result and outputs must remain available for the next attempt.
    std::fs::write(&current, b"corrupt runtime current\n").unwrap();
    assert!(
        wait_for(Duration::from_secs(15), Duration::from_millis(100), || {
            count_snapshots(root) > initial_snapshots && count_compaction_results(root) == 1
        }),
        "failed runtime publication should retain the compaction result"
    );

    // Once this job owns the suspension, even terminal read and validation failures must retain
    // its only retry record rather than clean up an edit whose durability is still unproven.
    std::fs::write(&result_file, b"corrupt result").unwrap();
    std::thread::sleep(Duration::from_secs(9));
    assert!(
        result_file.exists(),
        "checksum failure must retain the suspended result"
    );

    let payload =
        cobble::test_utils::read_metadata_payload_from_path_for_test(&result_file).unwrap_err();
    assert!(
        payload.to_string().to_lowercase().contains("checksum"),
        "test fixture should be checksum-invalid"
    );
    std::fs::write(&result_file, &valid_result).unwrap();
    let payload =
        cobble::test_utils::read_metadata_payload_from_path_for_test(&result_file).unwrap();
    let mut terminal_result: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    terminal_result["tree_scope"]["column_family_id"] = serde_json::json!(255);
    let terminal_result = cobble::test_utils::encode_metadata_payload_for_test(
        &serde_json::to_vec(&terminal_result).unwrap(),
    );
    std::fs::write(&result_file, terminal_result).unwrap();
    std::thread::sleep(Duration::from_secs(6));
    assert!(
        result_file.exists(),
        "terminal validation failure must retain the suspended result"
    );

    // Restore the last valid pointer and mutate persisted state while suspension is still active.
    // The background publisher must not advance CURRENT before result retry proves durability.
    std::fs::write(&result_file, &valid_result).unwrap();
    std::fs::write(&current, &valid_current).unwrap();
    for i in 0..16u32 {
        db.put(0, format!("post-failure-{i:08}").as_bytes(), 0, &value)
            .unwrap();
    }
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        std::fs::read(&current).unwrap(),
        valid_current,
        "background publication must remain suspended after a failed result barrier"
    );

    assert!(
        wait_for(Duration::from_secs(15), Duration::from_millis(100), || {
            count_compaction_results(root) == 0
                && std::fs::read(&current)
                    .map(|bytes| bytes != valid_current)
                    .unwrap_or(false)
        }),
        "successful retry should advance runtime CURRENT and delete the result"
    );

    stop.store(true, Ordering::SeqCst);
    let _ = handle.join();
    for i in 0..40u32 {
        verify_get(&db, 0, format!("retry-{i:08}").as_bytes(), &value);
    }
    for i in 0..16u32 {
        verify_get(&db, 0, format!("post-failure-{i:08}").as_bytes(), &value);
    }
    db.close().expect("close");
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn dedicated_compactor_runtime_missing_corrupt_and_inaccessible_fail_loud() {
    let root = "/tmp/dedicated_compaction_runtime_errors";
    cleanup_test_root(root);
    let db_id = "dedicated-compaction-runtime-errors".to_string();
    let config = dedicated_config(root);

    // No writer has started: runtime mode is Idle rather than snapshot fallback or open failure.
    let compactor = DedicatedCompactor::open(config.clone(), db_id.clone()).expect("open idle");
    compactor
        .run_once()
        .expect("missing runtime manifest is idle");

    let db = open_db_with_id(config.clone(), &db_id);
    let current = find_file(root, |path| {
        path.file_name().and_then(|name| name.to_str()) == Some("CURRENT")
            && path
                .parent()
                .and_then(|parent| parent.file_name())
                .and_then(|name| name.to_str())
                == Some("runtime")
    });
    let valid_current = std::fs::read(&current).unwrap();
    db.close().unwrap();

    // A corrupt runtime pointer is a hard observation error, never a snapshot fallback.
    std::fs::write(&current, b"corrupt runtime current\n").unwrap();
    let compactor = DedicatedCompactor::open(config.clone(), db_id.clone()).expect("open corrupt");
    assert!(compactor.run_once().is_err());
    std::fs::write(&current, valid_current).unwrap();

    // Restore a valid runtime manifest, then remove a referenced SST. Rebuilding the read-only
    // layout must surface that dangling reference before a plan is executed.
    let db = open_db_with_id(config.clone(), &db_id);
    for i in 0..16 {
        db.put(
            0,
            format!("missing-file-{i}").as_bytes(),
            0,
            vec![b'x'; 1024],
        )
        .unwrap();
    }
    assert!(wait_for(
        Duration::from_secs(5),
        Duration::from_millis(10),
        || { count_data_files(root) > 0 && runtime_current_exists(root) }
    ));
    db.close().unwrap();
    let data_file = find_file(root, |path| {
        path.extension().and_then(|ext| ext.to_str()) == Some("sst")
    });
    std::fs::remove_file(data_file).unwrap();
    let compactor = DedicatedCompactor::open(config, db_id).expect("open dangling");
    assert!(compactor.run_once().is_err());
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
        if round == 0 {
            // The next runtime manifest must carry schema 1 alongside the existing schema 0
            // files. The compactor reloads both schema files without a bootstrap snapshot.
            let mut schema = db.update_schema();
            schema.add_column(1, None, None, None).unwrap();
            let _ = schema.commit();
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

    // Dedicated runtime mode publishes on flush without creating a snapshot.
    assert!(
        wait_for(Duration::from_secs(10), Duration::from_millis(100), || {
            count_data_files(root) > 0 && runtime_current_exists(root)
        }),
        "flush should produce SST files and a runtime manifest"
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

    // Db::close() does not flush the active memtable or create a snapshot. Any data
    // remaining in the active memtable would be lost on restart. Call snapshot() to
    // force a flush + manifest commit before close, matching the pattern used in db_it.rs.
    let final_snapshot_id = db.snapshot().expect("final snapshot");
    // Wait for the specific snapshot to be materialized (flush + manifest write to disk).
    // Waiting by count is unreliable because async snapshots from the poller could satisfy
    // a count-based condition without the final snapshot being durable.
    assert!(
        wait_for(Duration::from_secs(10), Duration::from_millis(100), || {
            snapshot_exists(root, final_snapshot_id)
        },),
        "final snapshot {} should be materialized before close",
        final_snapshot_id
    );

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
