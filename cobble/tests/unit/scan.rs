use super::*;
use crate::config::VolumeDescriptor;
use crate::coordinator::{CoordinatorConfig, DbCoordinator};
use crate::{Db, ScanOptions, WriteBatch, WriteOptions};
use std::collections::BTreeMap;

fn cleanup_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn full_bucket_range() -> Vec<std::ops::RangeInclusive<u16>> {
    vec![0u16..=3u16]
}

/// Write data, snapshot with callback, retain snapshot, return (Db, shard_input).
/// Caller must close the Db when done.
fn write_and_snapshot(
    config: &Config,
    writes: impl FnOnce(&Db),
) -> (Db, crate::coordinator::ShardSnapshotInput) {
    let db = Db::open(config.clone(), full_bucket_range()).unwrap();
    writes(&db);
    let (tx, rx) = std::sync::mpsc::channel();
    db.snapshot_with_callback(move |result| {
        let _ = tx.send(result);
    })
    .unwrap();
    let shard_input = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("snapshot callback timed out")
        .unwrap();
    db.retain_snapshot(shard_input.snapshot_id);
    (db, shard_input)
}

#[test]
#[serial_test::serial(file)]
fn test_scan_plan_basic() {
    let root = "/tmp/cobble_scan_plan_basic";
    cleanup_root(root);

    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
        num_columns: 1,
        total_buckets: 4,
        ..Config::default()
    };
    let (_db, shard_input) = write_and_snapshot(&config, |db| {
        let mut batch = WriteBatch::new();
        batch.put(0, b"key1", 0, b"val1");
        batch.put(0, b"key2", 0, b"val2");
        batch.put(0, b"key3", 0, b"val3");
        db.write_batch(batch).unwrap();
    });

    let coordinator = DbCoordinator::open(CoordinatorConfig {
        volumes: vec![crate::config::VolumeDescriptor::new(
            format!("file://{}/coordinator", root),
            vec![
                crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                crate::config::VolumeUsageKind::Meta,
            ],
        )],
        snapshot_retention: None,
    })
    .unwrap();
    let global = coordinator
        .take_global_snapshot(4, vec![shard_input])
        .unwrap();
    coordinator.materialize_global_snapshot(&global).unwrap();

    // Create scan plan and splits.
    let plan = ScanPlan::new(global);
    let splits = plan.splits();
    assert_eq!(splits.len(), 1);

    // Create scanner from split.
    let scanner = splits[0]
        .create_scanner(config, &ScanOptions::default())
        .unwrap();
    let results: Vec<_> = scanner.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].1.as_ref(), b"key1");
    assert_eq!(results[1].1.as_ref(), b"key2");
    assert_eq!(results[2].1.as_ref(), b"key3");

    cleanup_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_scan_plan_with_range() {
    let root = "/tmp/cobble_scan_plan_range";
    cleanup_root(root);

    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
        num_columns: 1,
        total_buckets: 4,
        ..Config::default()
    };
    let (_db, shard_input) = write_and_snapshot(&config, |db| {
        let mut batch = WriteBatch::new();
        batch.put(0, b"aaa", 0, b"v1");
        batch.put(0, b"bbb", 0, b"v2");
        batch.put(0, b"ccc", 0, b"v3");
        batch.put(0, b"ddd", 0, b"v4");
        db.write_batch(batch).unwrap();
    });
    let coordinator = DbCoordinator::open(CoordinatorConfig {
        volumes: vec![crate::config::VolumeDescriptor::new(
            format!("file://{}/coordinator", root),
            vec![
                crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                crate::config::VolumeUsageKind::Meta,
            ],
        )],
        snapshot_retention: None,
    })
    .unwrap();
    let global = coordinator
        .take_global_snapshot(4, vec![shard_input])
        .unwrap();
    coordinator.materialize_global_snapshot(&global).unwrap();

    // Scan with key range [bbb, ddd).
    let plan = ScanPlan::new(global)
        .with_start(b"bbb".to_vec())
        .with_end(b"ddd".to_vec());
    let splits = plan.splits();
    let scanner = splits[0]
        .create_scanner(config, &ScanOptions::default())
        .unwrap();
    let results: Vec<_> = scanner.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1.as_ref(), b"bbb");
    assert_eq!(results[1].1.as_ref(), b"ccc");

    cleanup_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_scan_split_scanner_consume_next_row() {
    let root = "/tmp/cobble_scan_plan_consume_next_row";
    cleanup_root(root);

    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
        num_columns: 2,
        total_buckets: 4,
        ..Config::default()
    };
    let (_db, shard_input) = write_and_snapshot(&config, |db| {
        let mut batch = WriteBatch::new();
        batch.put(0, b"key1", 0, b"v10");
        batch.put(0, b"key1", 1, b"v11");
        batch.put(0, b"key2", 0, b"v20");
        db.write_batch(batch).unwrap();
    });

    let coordinator = DbCoordinator::open(CoordinatorConfig {
        volumes: vec![crate::config::VolumeDescriptor::new(
            format!("file://{}/coordinator", root),
            vec![
                crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                crate::config::VolumeUsageKind::Meta,
            ],
        )],
        snapshot_retention: None,
    })
    .unwrap();
    let global = coordinator
        .take_global_snapshot(4, vec![shard_input])
        .unwrap();
    coordinator.materialize_global_snapshot(&global).unwrap();

    let mut scanner = ScanPlan::new(global)
        .splits()
        .remove(0)
        .create_scanner(config, &ScanOptions::default())
        .unwrap();
    let mut rows = Vec::new();
    while let Some(row) = scanner
        .consume_next_row(|key, columns| Ok((key.clone(), columns.to_vec())))
        .unwrap()
    {
        rows.push(row);
    }

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0.as_ref(), b"key1");
    assert_eq!(rows[0].1[0].as_deref(), Some(b"v10".as_slice()));
    assert_eq!(rows[0].1[1].as_deref(), Some(b"v11".as_slice()));
    assert_eq!(rows[1].0.as_ref(), b"key2");
    assert_eq!(rows[1].1[0].as_deref(), Some(b"v20".as_slice()));
    assert_eq!(rows[1].1[1].as_deref(), None);

    cleanup_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_scan_split_scanner_resume_after_bucket_key() {
    let root = "/tmp/cobble_scan_plan_resume_after_bucket_key";
    cleanup_root(root);

    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
        num_columns: 1,
        total_buckets: 4,
        ..Config::default()
    };
    let (_db, shard_input) = write_and_snapshot(&config, |db| {
        let mut batch = WriteBatch::new();
        batch.put(0, b"z0", 0, b"v0");
        batch.put(1, b"z1", 0, b"v1");
        batch.put(2, b"m2a", 0, b"v2a");
        batch.put(2, b"m2b", 0, b"v2b");
        batch.put(3, b"a3", 0, b"v3");
        db.write_batch(batch).unwrap();
    });

    let coordinator = DbCoordinator::open(CoordinatorConfig {
        volumes: vec![crate::config::VolumeDescriptor::new(
            format!("file://{}/coordinator", root),
            vec![
                crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                crate::config::VolumeUsageKind::Meta,
            ],
        )],
        snapshot_retention: None,
    })
    .unwrap();
    let global = coordinator
        .take_global_snapshot(4, vec![shard_input])
        .unwrap();
    coordinator.materialize_global_snapshot(&global).unwrap();

    let scanner = ScanPlan::new(global)
        .splits()
        .remove(0)
        .split_after(2, b"m2a".to_vec())
        .unwrap()
        .after
        .create_scanner(config, &ScanOptions::default())
        .unwrap();
    let results: Vec<_> = scanner.map(|r| r.unwrap()).collect();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1.as_ref(), b"m2b");
    assert_eq!(results[1].1.as_ref(), b"a3");

    cleanup_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_scan_plan_column_projection() {
    let root = "/tmp/cobble_scan_plan_col_proj";
    cleanup_root(root);

    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
        num_columns: 3,
        total_buckets: 4,
        ..Config::default()
    };
    let (_db, shard_input) = write_and_snapshot(&config, |db| {
        let mut batch = WriteBatch::new();
        batch.put(0, b"key1", 0, b"a0");
        batch.put(0, b"key1", 1, b"a1");
        batch.put(0, b"key1", 2, b"a2");
        db.write_batch(batch).unwrap();
    });
    let coordinator = DbCoordinator::open(CoordinatorConfig {
        volumes: vec![crate::config::VolumeDescriptor::new(
            format!("file://{}/coordinator", root),
            vec![
                crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                crate::config::VolumeUsageKind::Meta,
            ],
        )],
        snapshot_retention: None,
    })
    .unwrap();
    let global = coordinator
        .take_global_snapshot(4, vec![shard_input])
        .unwrap();
    coordinator.materialize_global_snapshot(&global).unwrap();

    // Scan with column projection: only column 1.
    let opts = ScanOptions::for_column(1);
    let plan = ScanPlan::new(global);
    let splits = plan.splits();
    let scanner = splits[0].create_scanner(config, &opts).unwrap();
    let results: Vec<_> = scanner.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 1);
    // Column projection returns only the selected columns (compact array).
    assert_eq!(results[0].2.len(), 1);
    assert_eq!(results[0].2[0].as_deref(), Some(b"a1".as_slice()));

    cleanup_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_scan_plan_column_family_projection() {
    let root = "/tmp/cobble_scan_plan_cf_proj";
    cleanup_root(root);

    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
        num_columns: 1,
        total_buckets: 4,
        ..Config::default()
    };
    let (_db, shard_input) = write_and_snapshot(&config, |db| {
        let mut schema = db.update_schema();
        schema
            .add_column(0, None, None, Some("metrics".to_string()))
            .unwrap();
        let latest_schema = schema.commit();
        assert_eq!(latest_schema.column_family_ids().get("metrics"), Some(&1));

        db.put(0, b"key1", 0, b"default").unwrap();
        db.put_with_options(
            0,
            b"key1",
            0,
            b"metrics-1",
            &WriteOptions::with_column_family("metrics"),
        )
        .unwrap();
        db.put_with_options(
            0,
            b"key2",
            0,
            b"metrics-2",
            &WriteOptions::with_column_family("metrics"),
        )
        .unwrap();
    });
    assert_eq!(shard_input.column_family_ids.get("metrics"), Some(&1));

    let coordinator = DbCoordinator::open(CoordinatorConfig {
        volumes: vec![crate::config::VolumeDescriptor::new(
            format!("file://{}/coordinator", root),
            vec![
                crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                crate::config::VolumeUsageKind::Meta,
            ],
        )],
        snapshot_retention: None,
    })
    .unwrap();
    let global = coordinator
        .take_global_snapshot(4, vec![shard_input])
        .unwrap();
    coordinator.materialize_global_snapshot(&global).unwrap();

    let plan = ScanPlan::new(global);
    let splits = plan.splits();
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].shard.column_family_ids.get("metrics"), Some(&1));

    let opts = ScanOptions::for_column(0).with_column_family("metrics");
    let scanner = splits[0].create_scanner(config, &opts).unwrap();
    let results: Vec<_> = scanner.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1.as_ref(), b"key1");
    assert_eq!(results[0].2[0].as_deref(), Some(b"metrics-1".as_slice()));
    assert_eq!(results[1].1.as_ref(), b"key2");
    assert_eq!(results[1].2[0].as_deref(), Some(b"metrics-2".as_slice()));

    cleanup_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_scan_plan_without_end_includes_ff_prefixed_keys() {
    let root = "/tmp/cobble_scan_plan_no_end";
    cleanup_root(root);

    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
        num_columns: 1,
        total_buckets: 4,
        ..Config::default()
    };
    let (_db, shard_input) = write_and_snapshot(&config, |db| {
        let mut batch = WriteBatch::new();
        batch.put(0, b"\xff", 0, b"v1");
        batch.put(0, b"\xff\x01", 0, b"v2");
        batch.put(0, b"\xff\xff", 0, b"v3");
        db.write_batch(batch).unwrap();
    });

    let coordinator = DbCoordinator::open(CoordinatorConfig {
        volumes: vec![crate::config::VolumeDescriptor::new(
            format!("file://{}/coordinator", root),
            vec![
                crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                crate::config::VolumeUsageKind::Meta,
            ],
        )],
        snapshot_retention: None,
    })
    .unwrap();
    let global = coordinator
        .take_global_snapshot(4, vec![shard_input])
        .unwrap();
    coordinator.materialize_global_snapshot(&global).unwrap();

    let plan = ScanPlan::new(global);
    let splits = plan.splits();
    let scanner = splits[0]
        .create_scanner(config, &ScanOptions::default())
        .unwrap();
    let results: Vec<_> = scanner.map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].1.as_ref(), b"\xff");
    assert_eq!(results[1].1.as_ref(), b"\xff\x01");
    assert_eq!(results[2].1.as_ref(), b"\xff\xff");

    cleanup_root(root);
}

#[test]
fn test_scan_split_serialization() {
    let split = ScanSplit {
        shard: ShardSnapshotRef {
            ranges: vec![0u16..=3u16],
            column_family_ids: BTreeMap::from([("default".to_string(), 0)]),
            db_id: "test-db".to_string(),
            snapshot_id: 42,
            manifest_path: "file:///tmp/manifest".to_string(),
            timestamp_seconds: 100,
            data_size_bytes: 1234,
            incremental_data_size_bytes: 567,
        },
        start: Some(b"start".to_vec()),
        end: Some(b"end".to_vec()),
        start_bucket: Some(2),
        start_key_exclusive: Some(b"resume".to_vec()),
        end_bucket: Some(3),
        end_key_inclusive: Some(b"tail".to_vec()),
    };

    let json = serde_json::to_string(&split).unwrap();
    let deserialized: ScanSplit = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.shard.db_id, "test-db");
    assert_eq!(deserialized.shard.snapshot_id, 42);
    assert_eq!(deserialized.shard.timestamp_seconds, 100);
    assert_eq!(deserialized.start, Some(b"start".to_vec()));
    assert_eq!(deserialized.end, Some(b"end".to_vec()));
    assert_eq!(deserialized.start_bucket, Some(2));
    assert_eq!(deserialized.start_key_exclusive, Some(b"resume".to_vec()));
    assert_eq!(deserialized.end_bucket, Some(3));
    assert_eq!(deserialized.end_key_inclusive, Some(b"tail".to_vec()));
}
