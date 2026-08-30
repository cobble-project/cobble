use super::*;
use crate::VolumeDescriptor;
use crate::coordinator::{CoordinatorConfig, DbCoordinator, ShardSnapshotInput};
use crate::paths::{bucket_snapshot_dir, bucket_snapshot_manifest_path};
use crate::test_utils::{
    encode_metadata_payload_for_test, read_metadata_payload_from_path_for_test,
};
use std::collections::BTreeMap;
use std::path::Path;

fn cleanup_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn wait_for_manifest_in_db(root: &str, db_id: &str, snapshot_id: u64) -> String {
    let full_path = format!(
        "{}/{}",
        root,
        bucket_snapshot_manifest_path(db_id, snapshot_id)
    );
    for _ in 0..50 {
        if Path::new(&full_path).exists() {
            return full_path;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        Path::new(&full_path).exists(),
        "manifest missing at {}",
        full_path
    );
    format!("file://{}", full_path)
}

fn create_bucket_manifest(
    fs: Arc<dyn crate::file::FileSystem>,
    root: &str,
    db_id: &str,
    snapshot_id: u64,
) -> String {
    let snapshot_dir = bucket_snapshot_dir(db_id);
    let manifest_path = bucket_snapshot_manifest_path(db_id, snapshot_id);
    let schema_dir = format!("{}/schema", db_id);
    let schema_path = format!("{}/schema/schema-0", db_id);
    let _ = fs.create_dir(db_id);
    let _ = fs.create_dir(&snapshot_dir);
    let _ = fs.create_dir(&schema_dir);
    let mut schema_writer = fs.open_write(&schema_path).unwrap();
    let schema_payload = br#"{"format_version":2,"id":0,"column_families":[{"id":0,"name":"default","merge_operator_ids":[],"column_metadata":[null],"options":{"value_has_ttl":true},"evolution_id":"noop"}]}"#;
    let schema_bytes = encode_metadata_payload_for_test(schema_payload);
    schema_writer.write(&schema_bytes).unwrap();
    schema_writer.close().unwrap();
    let mut writer = fs.open_write(&manifest_path).unwrap();
    let manifest = format!(
        "{{\"version\":{},\"id\":{},\"seq_id\":0,\"latest_schema_id\":0,\"data_size_bytes\":0,\"incremental_data_size_bytes\":0,\"bucket_ranges\":[{{\"start\":0,\"end\":1}}],\"lsm_tree_bucket_ranges\":[{{\"start\":0,\"end\":1}}],\"tree_scopes\":[{{\"bucket_range\":{{\"start\":0,\"end\":1}},\"column_family_id\":0}}],\"tree_levels\":[[]],\"vlog_files\":[],\"active_memtable_data\":[]}}",
        crate::snapshot::manifest::MANIFEST_VERSION_CURRENT,
        snapshot_id
    );
    let manifest_bytes = encode_metadata_payload_for_test(manifest.as_bytes());
    writer.write(&manifest_bytes).unwrap();
    writer.close().unwrap();
    wait_for_manifest_in_db(root, db_id, snapshot_id)
}

fn wait_for_pointer(root: &str, snapshot_id: u64) {
    let path = format!("{}/{}", root, global_snapshot_current_path());
    let manifest = snapshot_manifest_name(snapshot_id);
    for _ in 0..50 {
        if let Ok(payload) = read_metadata_payload_from_path_for_test(&path)
            && let Ok(contents) = std::str::from_utf8(&payload)
            && contents.trim() == manifest
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let payload = read_metadata_payload_from_path_for_test(&path).expect("read pointer");
    let contents = std::str::from_utf8(&payload).expect("pointer utf8");
    assert_eq!(contents.trim(), manifest);
}

fn default_column_family_ids() -> BTreeMap<String, u8> {
    BTreeMap::from([("default".to_string(), 0)])
}

#[test]
#[serial_test::serial(file)]
fn test_read_proxy_routes_and_evicts() {
    let root = "/tmp/reader";
    cleanup_root(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let db_a = "db-a".to_string();
    let db_b = "db-b".to_string();
    let snap_a = 1;
    let snap_b = 2;
    let path_a = create_bucket_manifest(Arc::clone(&fs), root, &db_a, snap_a);
    let path_b = create_bucket_manifest(Arc::clone(&fs), root, &db_b, snap_b);

    let coordinator = DbCoordinator::open(CoordinatorConfig {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        snapshot_retention: None,
    })
    .unwrap();
    let global = coordinator
        .take_global_snapshot(
            4,
            vec![
                ShardSnapshotInput {
                    ranges: vec![0u16..=1u16],
                    column_family_ids: default_column_family_ids(),
                    db_id: db_a.clone(),
                    snapshot_id: snap_a,
                    manifest_path: path_a,
                    timestamp_seconds: 0,
                    data_size_bytes: 0,
                    incremental_data_size_bytes: 0,
                },
                ShardSnapshotInput {
                    ranges: vec![2u16..=3u16],
                    column_family_ids: default_column_family_ids(),
                    db_id: db_b.clone(),
                    snapshot_id: snap_b,
                    manifest_path: path_b,
                    timestamp_seconds: 0,
                    data_size_bytes: 0,
                    incremental_data_size_bytes: 0,
                },
            ],
        )
        .unwrap();
    coordinator.materialize_global_snapshot(&global).unwrap();
    wait_for_pointer(root, global.id);

    let mut proxy = Reader::open_current(ReaderConfig {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        total_buckets: 4,
        ..ReaderConfig::default()
    })
    .unwrap();
    let value_a = proxy.get(0, b"key-a").unwrap();
    assert!(value_a.is_none());
    assert_eq!(proxy.cache.len(), 1);
    assert!(proxy.cache.contains_key(&Arc::new(BucketSnapshotKey {
        db_id: db_a.clone(),
        snapshot_id: snap_a,
    })));

    proxy.reload_tolerance = Duration::from_millis(0);
    let value_b = proxy.get(3, b"key-b").unwrap();
    assert!(value_b.is_none());
    assert_eq!(proxy.cache.len(), 1);
    assert!(!proxy.cache.contains_key(&Arc::new(BucketSnapshotKey {
        db_id: db_a,
        snapshot_id: snap_a,
    })));
    assert!(proxy.cache.contains_key(&Arc::new(BucketSnapshotKey {
        db_id: db_b,
        snapshot_id: snap_b,
    })));

    let values = proxy
        .multi_get(&[
            (0, b"key-a".as_slice()),
            (3, b"key-b".as_slice()),
            (0, b"key-a".as_slice()),
        ])
        .unwrap();
    assert_eq!(values, vec![None, None, None]);

    cleanup_root(root);
}

#[test]
// #[serial_test::serial(file)]
fn test_read_proxy_refreshes_on_pointer_change() {
    let root = "/tmp/read_proxy_refresh";
    cleanup_root(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let db_a = "db-a".to_string();
    let db_b = "db-b".to_string();
    let snap_a = 10;
    let snap_b = 20;
    let path_a = create_bucket_manifest(Arc::clone(&fs), root, &db_a, snap_a);
    let path_b = create_bucket_manifest(Arc::clone(&fs), root, &db_b, snap_b);

    let coordinator = DbCoordinator::open(CoordinatorConfig {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        snapshot_retention: None,
    })
    .unwrap();
    let global_a = coordinator
        .take_global_snapshot(
            4,
            vec![ShardSnapshotInput {
                ranges: vec![0u16..=3u16],
                column_family_ids: default_column_family_ids(),
                db_id: db_a.clone(),
                snapshot_id: snap_a,
                manifest_path: path_a,
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            }],
        )
        .unwrap();
    coordinator.materialize_global_snapshot(&global_a).unwrap();
    wait_for_pointer(root, global_a.id);

    let mut proxy = Reader::open_current(ReaderConfig {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        total_buckets: 4,
        ..ReaderConfig::default()
    })
    .unwrap();
    proxy.reload_tolerance = Duration::from_millis(0);
    let _ = proxy.get(0, b"key").unwrap();
    assert!(proxy.cache.contains_key(&Arc::new(BucketSnapshotKey {
        db_id: db_a.clone(),
        snapshot_id: snap_a,
    })));

    let global_b = coordinator
        .take_global_snapshot(
            4,
            vec![ShardSnapshotInput {
                ranges: vec![0u16..=3u16],
                column_family_ids: default_column_family_ids(),
                db_id: db_b.clone(),
                snapshot_id: snap_b,
                manifest_path: path_b,
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            }],
        )
        .unwrap();
    coordinator.materialize_global_snapshot(&global_b).unwrap();
    wait_for_pointer(root, global_b.id);

    proxy.refresh().unwrap();
    let _ = proxy.get(0, b"key").unwrap();
    assert!(proxy.cache.contains_key(&Arc::new(BucketSnapshotKey {
        db_id: db_b,
        snapshot_id: snap_b,
    })));
    assert!(!proxy.cache.contains_key(&Arc::new(BucketSnapshotKey {
        db_id: db_a,
        snapshot_id: snap_a,
    })));

    cleanup_root(root);
}
