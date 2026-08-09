use super::*;
use crate::file::FileSystemRegistry;
use crate::paths::{bucket_snapshot_dir, bucket_snapshot_manifest_path};

fn cleanup_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn write_bucket_snapshot(
    fs: Arc<dyn FileSystem>,
    root: &str,
    db_id: &str,
    snapshot_id: u64,
) -> String {
    fs.create_dir(db_id).unwrap();
    let snapshot_dir = bucket_snapshot_dir(db_id);
    fs.create_dir(&snapshot_dir).unwrap();
    let path = bucket_snapshot_manifest_path(db_id, snapshot_id);
    let mut writer = fs.open_write(&path).unwrap();
    writer.write(b"{}").unwrap();
    writer.close().unwrap();
    format!("file://{}/{}", root, path)
}

fn default_column_family_ids() -> BTreeMap<String, u8> {
    BTreeMap::from([("default".to_string(), 0)])
}

#[test]
#[serial_test::serial(file)]
fn test_global_snapshot_round_trip() {
    let root = "/tmp/coordinator_global_snapshot";
    cleanup_root(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let path_a = write_bucket_snapshot(Arc::clone(&fs), root, "db-a", 1);
    let path_b = write_bucket_snapshot(Arc::clone(&fs), root, "db-b", 2);

    let node = DbCoordinator::open(CoordinatorConfig {
        volumes: vec![crate::config::VolumeDescriptor::new(
            format!("file://{}", root),
            vec![
                crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                crate::config::VolumeUsageKind::Meta,
            ],
        )],
        snapshot_retention: None,
    })
    .unwrap();

    let snapshot = node
        .take_global_snapshot(
            4,
            vec![
                ShardSnapshotInput {
                    ranges: vec![0u16..=1u16],
                    column_family_ids: default_column_family_ids(),
                    db_id: "db-a".to_string(),
                    snapshot_id: 1,
                    manifest_path: path_a.clone(),
                    timestamp_seconds: 0,
                    data_size_bytes: 0,
                    incremental_data_size_bytes: 0,
                },
                ShardSnapshotInput {
                    ranges: vec![2u16..=3u16],
                    column_family_ids: default_column_family_ids(),
                    db_id: "db-b".to_string(),
                    snapshot_id: 2,
                    manifest_path: path_b.clone(),
                    timestamp_seconds: 0,
                    data_size_bytes: 0,
                    incremental_data_size_bytes: 0,
                },
            ],
        )
        .unwrap();
    node.materialize_global_snapshot(&snapshot).unwrap();

    let loaded = node.load_current_global_snapshot().unwrap().unwrap();
    assert_eq!(loaded.id, snapshot.id);
    assert_eq!(loaded.column_family_ids, default_column_family_ids());
    assert_eq!(loaded.shard_snapshots, snapshot.shard_snapshots);
    assert_eq!(loaded.shard_snapshots[0].manifest_path, path_a);
    assert_eq!(loaded.shard_snapshots[1].manifest_path, path_b);

    cleanup_root(root);
}

#[test]
fn test_global_snapshot_manifest_requires_version() {
    let raw = br#"{
            "id": 1,
            "total_buckets": 4,
            "shard_snapshots": [],
            "watermark_seconds": 0
        }"#;
    let err = decode_global_manifest(raw).unwrap_err();
    assert!(err.to_string().contains("Failed to decode global manifest"));
}

#[test]
fn test_global_snapshot_manifest_rejects_previous_physical_key_format() {
    let previous = GlobalSnapshotManifest {
        version: 1,
        id: 1,
        total_buckets: 4,
        column_family_ids: default_column_family_ids(),
        shard_snapshots: Vec::new(),
        watermark_seconds: 0,
    };
    let raw = serde_json::to_vec(&previous).unwrap();
    let err = decode_global_manifest(&raw).expect_err("version 1 must be rejected");
    assert!(
        err.to_string()
            .contains("Unsupported global snapshot manifest version: 1 (expected 2)")
    );
}

#[test]
#[serial_test::serial(file)]
fn test_list_global_snapshots_returns_sorted() {
    let root = "/tmp/coordinator_list_global_snapshots";
    cleanup_root(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let path_a = write_bucket_snapshot(Arc::clone(&fs), root, "db-a", 1);
    let path_b = write_bucket_snapshot(Arc::clone(&fs), root, "db-b", 2);

    let node = DbCoordinator::open(CoordinatorConfig {
        volumes: vec![crate::config::VolumeDescriptor::new(
            format!("file://{}", root),
            vec![
                crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                crate::config::VolumeUsageKind::Meta,
            ],
        )],
        snapshot_retention: None,
    })
    .unwrap();

    let snapshot_2 = node
        .take_global_snapshot_with_id(
            4,
            vec![ShardSnapshotInput {
                ranges: vec![0u16..=3u16],
                column_family_ids: default_column_family_ids(),
                db_id: "db-a".to_string(),
                snapshot_id: 1,
                manifest_path: path_a.clone(),
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            }],
            2,
        )
        .unwrap();
    node.materialize_global_snapshot(&snapshot_2).unwrap();

    let snapshot_1 = node
        .take_global_snapshot_with_id(
            4,
            vec![ShardSnapshotInput {
                ranges: vec![0u16..=3u16],
                column_family_ids: default_column_family_ids(),
                db_id: "db-b".to_string(),
                snapshot_id: 2,
                manifest_path: path_b.clone(),
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            }],
            1,
        )
        .unwrap();
    node.materialize_global_snapshot(&snapshot_1).unwrap();

    let listed = node.list_global_snapshots().unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].id, 1);
    assert_eq!(listed[1].id, 2);

    cleanup_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_global_snapshot_auto_retention() {
    let root = "/tmp/coordinator_snapshot_retention";
    cleanup_root(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let path = write_bucket_snapshot(Arc::clone(&fs), root, "db-a", 1);

    let node = DbCoordinator::open(CoordinatorConfig {
        volumes: vec![crate::config::VolumeDescriptor::new(
            format!("file://{}", root),
            vec![
                crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                crate::config::VolumeUsageKind::Meta,
            ],
        )],
        snapshot_retention: Some(1),
    })
    .unwrap();

    let snapshot_1 = node
        .take_global_snapshot_with_id(
            4,
            vec![ShardSnapshotInput {
                ranges: vec![0u16..=3u16],
                column_family_ids: default_column_family_ids(),
                db_id: "db-a".to_string(),
                snapshot_id: 1,
                manifest_path: path.clone(),
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            }],
            1,
        )
        .unwrap();
    node.materialize_global_snapshot(&snapshot_1).unwrap();

    let snapshot_2 = node
        .take_global_snapshot_with_id(
            4,
            vec![ShardSnapshotInput {
                ranges: vec![0u16..=3u16],
                column_family_ids: default_column_family_ids(),
                db_id: "db-a".to_string(),
                snapshot_id: 1,
                manifest_path: path,
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            }],
            2,
        )
        .unwrap();
    node.materialize_global_snapshot(&snapshot_2).unwrap();

    let listed = node.list_global_snapshots().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, 2);

    cleanup_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_global_snapshot_retain_expire() {
    let root = "/tmp/coordinator_snapshot_retain_expire";
    cleanup_root(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let path = write_bucket_snapshot(Arc::clone(&fs), root, "db-a", 1);

    let node = DbCoordinator::open(CoordinatorConfig {
        volumes: vec![crate::config::VolumeDescriptor::new(
            format!("file://{}", root),
            vec![
                crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                crate::config::VolumeUsageKind::Meta,
            ],
        )],
        snapshot_retention: Some(1),
    })
    .unwrap();

    let snapshot_1 = node
        .take_global_snapshot_with_id(
            4,
            vec![ShardSnapshotInput {
                ranges: vec![0u16..=3u16],
                column_family_ids: default_column_family_ids(),
                db_id: "db-a".to_string(),
                snapshot_id: 1,
                manifest_path: path.clone(),
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            }],
            1,
        )
        .unwrap();
    node.materialize_global_snapshot(&snapshot_1).unwrap();
    assert!(node.retain_snapshot(1));

    let snapshot_2 = node
        .take_global_snapshot_with_id(
            4,
            vec![ShardSnapshotInput {
                ranges: vec![0u16..=3u16],
                column_family_ids: default_column_family_ids(),
                db_id: "db-a".to_string(),
                snapshot_id: 1,
                manifest_path: path,
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            }],
            2,
        )
        .unwrap();
    node.materialize_global_snapshot(&snapshot_2).unwrap();

    let listed = node.list_global_snapshots().unwrap();
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().any(|s| s.id == 1));
    assert!(listed.iter().any(|s| s.id == 2));

    assert!(node.expire_snapshot(1).unwrap());
    let listed = node.list_global_snapshots().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, 2);

    cleanup_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_load_latest_snapshot_id_ignores_checksum_mismatch_pointer() {
    let root = "/tmp/coordinator_pointer_checksum";
    cleanup_root(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();

    fs.create_dir(SNAPSHOT_DIR).unwrap();
    let mut writer = fs.open_write(&global_snapshot_current_path()).unwrap();
    writer.write(b"SNAPSHOT-123").unwrap();
    writer.close().unwrap();

    let latest = load_latest_snapshot_id(&fs).unwrap();
    assert!(latest.is_none());

    cleanup_root(root);
}

#[test]
fn test_global_snapshot_rejects_conflicting_column_family_ids_by_name() {
    let err = DbCoordinator::build_global_snapshot(
        4,
        vec![
            ShardSnapshotInput {
                ranges: vec![0u16..=1u16],
                column_family_ids: BTreeMap::from([
                    ("default".to_string(), 0),
                    ("metrics".to_string(), 1),
                ]),
                db_id: "db-a".to_string(),
                snapshot_id: 1,
                manifest_path: "file:///tmp/db-a".to_string(),
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            },
            ShardSnapshotInput {
                ranges: vec![2u16..=3u16],
                column_family_ids: BTreeMap::from([
                    ("default".to_string(), 0),
                    ("metrics".to_string(), 2),
                ]),
                db_id: "db-b".to_string(),
                snapshot_id: 2,
                manifest_path: "file:///tmp/db-b".to_string(),
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            },
        ],
        7,
    )
    .unwrap_err();
    assert!(matches!(err, Error::CoordinationError(_)));
    assert!(err.to_string().contains("conflicting ids"));
}

#[test]
fn test_global_snapshot_rejects_conflicting_column_family_ids_by_id() {
    let err = DbCoordinator::build_global_snapshot(
        4,
        vec![
            ShardSnapshotInput {
                ranges: vec![0u16..=1u16],
                column_family_ids: BTreeMap::from([
                    ("default".to_string(), 0),
                    ("metrics".to_string(), 1),
                ]),
                db_id: "db-a".to_string(),
                snapshot_id: 1,
                manifest_path: "file:///tmp/db-a".to_string(),
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            },
            ShardSnapshotInput {
                ranges: vec![2u16..=3u16],
                column_family_ids: BTreeMap::from([
                    ("default".to_string(), 0),
                    ("events".to_string(), 1),
                ]),
                db_id: "db-b".to_string(),
                snapshot_id: 2,
                manifest_path: "file:///tmp/db-b".to_string(),
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            },
        ],
        8,
    )
    .unwrap_err();
    assert!(matches!(err, Error::CoordinationError(_)));
    assert!(err.to_string().contains("assigned to both"));
}

#[test]
#[serial_test::serial(file)]
fn test_list_global_snapshots_skips_checksum_mismatch_manifest() {
    let root = "/tmp/coordinator_corrupt_manifest";
    cleanup_root(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let path = write_bucket_snapshot(Arc::clone(&fs), root, "db-a", 1);

    let node = DbCoordinator::open(CoordinatorConfig {
        volumes: vec![crate::config::VolumeDescriptor::new(
            format!("file://{}", root),
            vec![
                crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                crate::config::VolumeUsageKind::Meta,
            ],
        )],
        snapshot_retention: None,
    })
    .unwrap();

    for id in [1_u64, 2_u64] {
        let snapshot = node
            .take_global_snapshot_with_id(
                4,
                vec![ShardSnapshotInput {
                    ranges: vec![0u16..=3u16],
                    column_family_ids: default_column_family_ids(),
                    db_id: "db-a".to_string(),
                    snapshot_id: 1,
                    manifest_path: path.clone(),
                    timestamp_seconds: 0,
                    data_size_bytes: 0,
                    incremental_data_size_bytes: 0,
                }],
                id,
            )
            .unwrap();
        node.materialize_global_snapshot(&snapshot).unwrap();
    }

    let corrupt_manifest_path = global_snapshot_manifest_path(1);
    let mut corrupt_writer = fs.open_write(&corrupt_manifest_path).unwrap();
    corrupt_writer.write(br#"{"invalid":"manifest"}"#).unwrap();
    corrupt_writer.close().unwrap();

    let listed = node.list_global_snapshots().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, 2);

    cleanup_root(root);
}
