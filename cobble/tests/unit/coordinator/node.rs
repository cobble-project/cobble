use super::*;
use crate::file::FileSystemRegistry;
use crate::paths::{bucket_snapshot_dir, bucket_snapshot_manifest_path};
use crate::{ColumnFamilyOptions, SnapshotColumnFamily};

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
fn exported_global_snapshot_is_independent_and_retryable() {
    use crate::{Config, DbBuilder, Reader, ReaderConfig, VolumeDescriptor};

    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("online");
    let destination = root.path().join("export");
    let volumes = VolumeDescriptor::single_volume(source.to_str().unwrap());
    let node = DbCoordinator::open(CoordinatorConfig {
        volumes: volumes.clone(),
        snapshot_retention: Some(1),
    })
    .unwrap();
    let mut databases = Vec::new();
    let mut online_shards = Vec::new();
    let mut exported_shards = Vec::new();
    for bucket in 0..2u16 {
        let db = DbBuilder::new(Config {
            volumes: volumes.clone(),
            total_buckets: 2,
            snapshot_on_flush: false,
            snapshot_retention: None,
            ..Config::default()
        })
        .db_id(format!("shard-{bucket}"))
        .bucket_ranges(vec![bucket..=bucket])
        .open()
        .unwrap();
        db.put(bucket, b"key", 0, format!("value-{bucket}").as_bytes())
            .unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let id = db
            .snapshot_with_callback(move |result| tx.send(result).unwrap())
            .unwrap();
        online_shards.push(
            rx.recv_timeout(std::time::Duration::from_secs(30))
                .unwrap()
                .unwrap(),
        );
        exported_shards.push(
            db.export_snapshot(
                id,
                VolumeDescriptor::single_volume(destination.join("shards").to_str().unwrap())
                    .remove(0),
            )
            .unwrap()
            .wait()
            .unwrap(),
        );
        databases.push(db);
    }
    let first = node.take_global_snapshot(2, online_shards.clone()).unwrap();
    node.materialize_global_snapshot(&first).unwrap();
    assert!(node.retain_snapshot(first.id));
    let second = node.take_global_snapshot(2, online_shards.clone()).unwrap();
    node.materialize_global_snapshot(&second).unwrap();
    let before_pointer = std::fs::read(source.join(global_snapshot_current_path())).unwrap();
    let before_manifests = node.list_global_snapshots().unwrap();
    let before_next = node.next_id.load(Ordering::SeqCst);

    let exported = node
        .take_exported_global_snapshot(2, exported_shards)
        .unwrap();
    assert_eq!(exported.id, 0);
    let target = VolumeDescriptor::single_volume(destination.to_str().unwrap()).remove(0);
    // A failed publication leaves all previously exported shards available for a retry.
    std::fs::write(destination.join(SNAPSHOT_DIR), b"publication blocker").unwrap();
    assert!(
        node.materialize_exported_global_snapshot(&exported, target.clone())
            .is_err()
    );
    for shard in &exported.shard_snapshots {
        assert!(
            url::Url::parse(&shard.manifest_path)
                .unwrap()
                .to_file_path()
                .unwrap()
                .exists()
        );
    }
    std::fs::remove_file(destination.join(SNAPSHOT_DIR)).unwrap();
    node.materialize_exported_global_snapshot(&exported, target.clone())
        .unwrap();
    let manifest_path = destination.join(global_snapshot_manifest_path(0));
    let manifest_metadata = std::fs::metadata(&manifest_path).unwrap();
    node.materialize_exported_global_snapshot(&exported, target.clone())
        .unwrap();
    assert_eq!(
        std::fs::metadata(&manifest_path)
            .unwrap()
            .modified()
            .unwrap(),
        manifest_metadata.modified().unwrap()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            std::fs::metadata(&manifest_path).unwrap().ino(),
            manifest_metadata.ino()
        );
    }
    let mut conflicting = exported.clone();
    conflicting.watermark_seconds = conflicting.watermark_seconds.saturating_add(1);
    assert!(matches!(
        node.materialize_exported_global_snapshot(&conflicting, target.clone()),
        Err(Error::CoordinationError(_))
    ));
    let target_fs = FileSystemRegistry::new()
        .get_or_register_volume(&target)
        .unwrap();
    publish_manifest_pointer(&target_fs, "SNAPSHOT-9").unwrap();
    assert!(matches!(
        node.materialize_exported_global_snapshot(&exported, target.clone()),
        Err(Error::CoordinationError(_))
    ));
    publish_manifest_pointer(&target_fs, "SNAPSHOT-0").unwrap();
    assert_eq!(node.next_id.load(Ordering::SeqCst), before_next);
    assert_eq!(
        std::fs::read(source.join(global_snapshot_current_path())).unwrap(),
        before_pointer
    );
    assert_eq!(node.list_global_snapshots().unwrap(), before_manifests);
    assert!(node.retained.contains(&first.id));
    let third = node.take_global_snapshot(2, online_shards).unwrap();
    assert_eq!(third.id, before_next);
    node.materialize_global_snapshot(&third).unwrap();
    assert_eq!(
        node.list_global_snapshots()
            .unwrap()
            .iter()
            .map(|snapshot| snapshot.id)
            .collect::<Vec<_>>(),
        vec![first.id, third.id]
    );

    let reader_config = ReaderConfig {
        volumes: vec![target],
        total_buckets: 2,
        ..ReaderConfig::default()
    };
    {
        let mut reader = Reader::open_current(reader_config.clone()).unwrap();
        for bucket in 0..2u16 {
            assert_eq!(
                reader.get(bucket, b"key").unwrap().unwrap()[0].as_deref(),
                Some(format!("value-{bucket}").as_bytes())
            );
        }
    }
    for db in databases {
        db.close().unwrap();
    }
    std::fs::remove_dir_all(&source).unwrap();
    let mut reader = Reader::open(reader_config, 0).unwrap();
    for bucket in 0..2u16 {
        assert_eq!(
            reader.get(bucket, b"key").unwrap().unwrap()[0].as_deref(),
            Some(format!("value-{bucket}").as_bytes())
        );
    }
}

#[test]
fn exported_global_snapshot_validates_coverage_schema_and_routes() {
    use crate::VolumeDescriptor;

    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("online");
    let destination = root.path().join("export");
    let node = DbCoordinator::open(CoordinatorConfig {
        volumes: VolumeDescriptor::single_volume(source.to_str().unwrap()),
        snapshot_retention: None,
    })
    .unwrap();
    let target = VolumeDescriptor::single_volume(destination.to_str().unwrap()).remove(0);
    let shard = ShardSnapshotMetadata {
        ranges: vec![0..=1],
        column_families: default_column_families(),
        db_id: "exported-shard".into(),
        snapshot_id: 7,
        manifest_path: destination
            .join("shards/exported-shard/snapshot/SNAPSHOT-7")
            .to_str()
            .unwrap()
            .into(),
        schema_id: 0,
        timestamp_seconds: 0,
        data_size_bytes: 0,
        incremental_data_size_bytes: 0,
    };
    let valid = node
        .take_exported_global_snapshot(2, vec![shard.clone()])
        .unwrap();
    // Normal coordinator calls retain their existing acceptance of partial coverage.
    assert!(
        node.take_global_snapshot_with_id(3, vec![shard.clone()], 42)
            .is_ok()
    );
    assert!(
        node.take_exported_global_snapshot(3, vec![shard.clone()])
            .unwrap_err()
            .to_string()
            .contains("cover every bucket")
    );
    let mut overlapping = shard.clone();
    overlapping.ranges = vec![1..=1];
    assert!(
        node.take_exported_global_snapshot(2, vec![shard.clone(), overlapping.clone()])
            .unwrap_err()
            .to_string()
            .contains("overlap")
    );
    overlapping.ranges = vec![2..=2];
    overlapping.column_families.insert(
        "extra".into(),
        SnapshotColumnFamily {
            id: 1,
            num_columns: 1,
            options: ColumnFamilyOptions::default(),
        },
    );
    assert!(
        node.take_exported_global_snapshot(3, vec![shard.clone(), overlapping])
            .unwrap_err()
            .to_string()
            .contains("identical column family ids")
    );
    let mut out_of_range = shard.clone();
    out_of_range.ranges = vec![0..=2];
    assert!(
        node.take_exported_global_snapshot(2, vec![out_of_range])
            .is_err()
    );
    let mut full_range = shard.clone();
    full_range.ranges = vec![0..=u16::MAX];
    assert!(
        node.take_exported_global_snapshot(65536, vec![full_range])
            .is_ok()
    );
    assert!(node.take_exported_global_snapshot(0, vec![shard]).is_err());
    let mut invalid = valid.clone();
    invalid.shard_snapshots[0].ranges = vec![0..=0];
    assert!(
        node.materialize_exported_global_snapshot(&invalid, target.clone())
            .is_err()
    );
    invalid = valid.clone();
    invalid.shard_snapshots[0].manifest_path =
        source.join("shard/SNAPSHOT-7").to_str().unwrap().into();
    assert!(
        node.materialize_exported_global_snapshot(&invalid, target.clone())
            .unwrap_err()
            .to_string()
            .contains("must be located")
    );
    for path in [&source, root.path(), &source.join("child/../exports")] {
        let volume = VolumeDescriptor::single_volume(path.to_str().unwrap()).remove(0);
        assert!(
            node.materialize_exported_global_snapshot(&valid, volume)
                .unwrap_err()
                .to_string()
                .contains("overlaps online storage")
        );
    }
    #[cfg(unix)]
    {
        let alias = root.path().join("online-alias");
        std::os::unix::fs::symlink(&source, &alias).unwrap();
        let volume = VolumeDescriptor::single_volume(alias.to_str().unwrap()).remove(0);
        assert!(
            node.materialize_exported_global_snapshot(&valid, volume)
                .unwrap_err()
                .to_string()
                .contains("overlaps online storage")
        );
    }
    assert!(!destination.exists());
    assert_eq!(node.next_id.load(Ordering::SeqCst), 0);
    assert!(node.load_current_global_snapshot().unwrap().is_none());
}

#[test]
fn non_atomic_current_replacement_keeps_global_snapshots_flat() {
    let dir = tempfile::tempdir().unwrap();
    let config = CoordinatorConfig {
        volumes: crate::config::VolumeDescriptor::single_volume(format!(
            "file://{}",
            dir.path().display()
        )),
        snapshot_retention: None,
    };
    let mut node = DbCoordinator::open(config).unwrap();
    let fs = Arc::new(crate::file::atomic_replace::tests::TestFileSystem::new(
        node.fs.clone(),
    ));
    node.fs = fs.clone();
    let node = Arc::new(node);
    let threads = [1, 2].map(|id| {
        let node = node.clone();
        std::thread::spawn(move || {
            let snapshot = GlobalSnapshotManifest {
                version: GLOBAL_SNAPSHOT_MANIFEST_VERSION_CURRENT,
                id,
                total_buckets: 1,
                column_family_ids: default_column_family_ids(),
                shard_snapshots: Vec::new(),
                watermark_seconds: 0,
            };
            node.materialize_global_snapshot(&snapshot).unwrap();
        })
    });
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(node.list_global_snapshots().unwrap().len(), 2);
    assert!(node.load_current_global_snapshot().unwrap().is_some());
    for id in [1, 2] {
        assert!(fs.exists(&global_snapshot_manifest_path(id)).unwrap());
    }
    assert!(!fs.exists(&global_snapshot_current_path()).unwrap());
    assert!(
        fs.exists(&format!("{}-ATOMIC/", global_snapshot_current_path()))
            .unwrap()
    );
}

fn default_column_families() -> BTreeMap<String, SnapshotColumnFamily> {
    column_families(default_column_family_ids())
}

fn column_families(ids: BTreeMap<String, u8>) -> BTreeMap<String, SnapshotColumnFamily> {
    ids.into_iter()
        .map(|(name, id)| {
            (
                name,
                SnapshotColumnFamily {
                    id,
                    num_columns: 1,
                    options: ColumnFamilyOptions::default(),
                },
            )
        })
        .collect()
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
                ShardSnapshotMetadata {
                    ranges: vec![0u16..=1u16],
                    schema_id: 0,
                    column_families: default_column_families(),
                    db_id: "db-a".to_string(),
                    snapshot_id: 1,
                    manifest_path: path_a.clone(),
                    timestamp_seconds: 0,
                    data_size_bytes: 0,
                    incremental_data_size_bytes: 0,
                },
                ShardSnapshotMetadata {
                    ranges: vec![2u16..=3u16],
                    schema_id: 0,
                    column_families: default_column_families(),
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
        column_family_ids: BTreeMap::from([("default".to_string(), 0)]),
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
            vec![ShardSnapshotMetadata {
                ranges: vec![0u16..=3u16],
                schema_id: 0,
                column_families: default_column_families(),
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
            vec![ShardSnapshotMetadata {
                ranges: vec![0u16..=3u16],
                schema_id: 0,
                column_families: default_column_families(),
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
            vec![ShardSnapshotMetadata {
                ranges: vec![0u16..=3u16],
                schema_id: 0,
                column_families: default_column_families(),
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
            vec![ShardSnapshotMetadata {
                ranges: vec![0u16..=3u16],
                schema_id: 0,
                column_families: default_column_families(),
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
            vec![ShardSnapshotMetadata {
                ranges: vec![0u16..=3u16],
                schema_id: 0,
                column_families: default_column_families(),
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
            vec![ShardSnapshotMetadata {
                ranges: vec![0u16..=3u16],
                schema_id: 0,
                column_families: default_column_families(),
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
fn test_load_latest_snapshot_id_rejects_checksum_mismatch_pointer() {
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

    assert!(matches!(
        load_latest_snapshot_id(&fs),
        Err(Error::ChecksumMismatch(_))
    ));

    cleanup_root(root);
}

#[test]
fn test_global_snapshot_rejects_conflicting_column_family_ids_by_name() {
    let err = DbCoordinator::build_global_snapshot(
        4,
        vec![
            ShardSnapshotMetadata {
                ranges: vec![0u16..=1u16],
                schema_id: 0,
                column_families: column_families(BTreeMap::from([
                    ("default".to_string(), 0),
                    ("metrics".to_string(), 1),
                ])),
                db_id: "db-a".to_string(),
                snapshot_id: 1,
                manifest_path: "file:///tmp/db-a".to_string(),
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            },
            ShardSnapshotMetadata {
                ranges: vec![2u16..=3u16],
                schema_id: 0,
                column_families: column_families(BTreeMap::from([
                    ("default".to_string(), 0),
                    ("metrics".to_string(), 2),
                ])),
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
            ShardSnapshotMetadata {
                ranges: vec![0u16..=1u16],
                schema_id: 0,
                column_families: column_families(BTreeMap::from([
                    ("default".to_string(), 0),
                    ("metrics".to_string(), 1),
                ])),
                db_id: "db-a".to_string(),
                snapshot_id: 1,
                manifest_path: "file:///tmp/db-a".to_string(),
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            },
            ShardSnapshotMetadata {
                ranges: vec![2u16..=3u16],
                schema_id: 0,
                column_families: column_families(BTreeMap::from([
                    ("default".to_string(), 0),
                    ("events".to_string(), 1),
                ])),
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
                vec![ShardSnapshotMetadata {
                    ranges: vec![0u16..=3u16],
                    schema_id: 0,
                    column_families: default_column_families(),
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
