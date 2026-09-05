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
fn schema_transforms_survive_lazy_shards_eviction_and_refresh() {
    use crate::data_file::DataFileType;
    use crate::{
        BytesMergeOperator, ColumnEvolution, Db, DbBuilder, ReadOnlyDbBuilder, ReaderBuilder,
        U32CounterMergeOperator, WriteOptions,
    };

    let render = |value: Option<Bytes>| -> Result<Option<Bytes>> {
        value
            .map(|value| {
                let bytes: [u8; 4] = value
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::InvalidState("expected u32".into()))?;
                Ok(Bytes::from(format!("sum={}", u32::from_le_bytes(bytes))))
            })
            .transpose()
    };
    let bang = |value: Option<Bytes>| -> Result<Option<Bytes>> {
        Ok(value.map(|value| [value.as_ref(), b"!"].concat().into()))
    };
    let source = |index, id: Option<&str>| ColumnEvolution::Source {
        source_index: index,
        transform_id: id.map(str::to_owned),
    };
    let take_snapshot = |db: &Db| {
        let (tx, rx) = std::sync::mpsc::channel();
        db.snapshot_with_callback(move |result| tx.send(result).unwrap())
            .unwrap();
        rx.recv_timeout(Duration::from_secs(30)).unwrap().unwrap()
    };
    for file_type in [DataFileType::SSTable, DataFileType::Parquet] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_str().unwrap();
        let config = Config {
            total_buckets: 2,
            memtable_capacity: Size::from_const(1 << 20),
            block_cache_size: Size::from_mib(1),
            data_file_type: file_type,
            parquet_row_group_size_bytes: Size::from_const(1024),
            value_separation_threshold: Some(Size::from_const(1)),
            snapshot_on_flush: false,
            l0_file_limit: 1000,
            volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
            ..Config::default()
        };
        let write = WriteOptions::with_column_family("metrics");
        let read = ReadOptions::default().with_column_family("metrics");
        let scan_options = ScanOptions::for_columns(vec![1, 0, 2])
            .with_column_family("metrics")
            .with_stop_at_block_boundary(true);
        let keys: Vec<_> = (0..128).map(|i| format!("k{i:03}").into_bytes()).collect();
        let mut shards = Vec::new();
        for bucket in 0..2 {
            let db = DbBuilder::new(config.clone())
                .db_id(format!("shard-{bucket}"))
                .bucket_ranges(vec![bucket..=bucket])
                .register_schema_transform("render", render)
                .unwrap()
                .open()
                .unwrap();
            let mut schema = db.update_schema();
            schema
                .add_column(
                    0,
                    Some(Arc::new(U32CounterMergeOperator)),
                    None,
                    Some("metrics".into()),
                )
                .unwrap();
            schema
                .add_column(1, None, None, Some("metrics".into()))
                .unwrap();
            schema.commit();
            for key in &keys {
                db.put_with_options(bucket, key, 0, 1u32.to_le_bytes(), &write)
                    .unwrap();
                db.merge_with_options(bucket, key, 0, 2u32.to_le_bytes(), &write)
                    .unwrap();
                db.put_with_options(bucket, key, 1, b"payload", &write)
                    .unwrap();
            }
            take_snapshot(&db);
            for key in &keys {
                db.merge_with_options(bucket, key, 0, 4u32.to_le_bytes(), &write)
                    .unwrap();
            }
            take_snapshot(&db);
            let mut schema = db.update_schema();
            schema
                .remap_columns(
                    Some("metrics".into()),
                    vec![
                        source(0, Some("render")),
                        source(1, None),
                        ColumnEvolution::Default {
                            value: Bytes::from_static(b"D"),
                        },
                        ColumnEvolution::Null,
                    ],
                )
                .unwrap();
            schema
                .replace_column(Some("metrics".into()), 0, Arc::new(BytesMergeOperator))
                .unwrap();
            schema.commit();
            for key in &keys {
                db.merge_with_options(bucket, key, 0, b"-tail", &write)
                    .unwrap();
            }
            db.delete_row_with_options(bucket, &keys[10], &write)
                .unwrap();
            shards.push(db);
        }
        let coordinator = DbCoordinator::open(CoordinatorConfig {
            volumes: config.volumes.clone(),
            snapshot_retention: None,
        })
        .unwrap();
        let publish = |shards: &[Db]| {
            let inputs = shards.iter().map(&take_snapshot).collect();
            let global = coordinator.take_global_snapshot(2, inputs).unwrap();
            coordinator.materialize_global_snapshot(&global).unwrap();
            global
        };
        let first = publish(&shards);
        let shard_snapshot_id = first.shard_snapshots[0].snapshot_id;
        let shard_db_id = first.shard_snapshots[0].db_id.clone();
        assert!(
            ReadOnlyDbBuilder::new(config.clone())
                .open(shard_snapshot_id)
                .err()
                .unwrap()
                .to_string()
                .contains("db_id")
        );
        let direct_builder = || ReadOnlyDbBuilder::new(config.clone()).db_id(shard_db_id.clone());
        assert!(
            direct_builder()
                .open(shard_snapshot_id)
                .err()
                .unwrap()
                .to_string()
                .contains("render")
        );
        let direct = direct_builder()
            .register_schema_transform("render", render)
            .unwrap()
            .open(shard_snapshot_id)
            .unwrap();
        assert!(direct.register_schema_transform("render", render).is_err());
        direct.register_schema_transform("future", bang).unwrap();
        let old_row = vec![
            Some(Bytes::from_static(b"sum=7-tail")),
            Some(Bytes::from_static(b"payload")),
            Some(Bytes::from_static(b"D")),
            None,
        ];
        assert_eq!(
            direct.get_with_options(0, &keys[0], &read).unwrap(),
            Some(old_row.clone())
        );
        assert_eq!(
            direct
                .multi_get_with_options(&[(0, &keys[0]), (0, &keys[10])], &read)
                .unwrap(),
            vec![Some(old_row.clone()), None]
        );

        let reader_config = ReaderConfig {
            pin_partition_in_memory_count: 1,
            reload_tolerance: Duration::from_secs(3600),
            ..ReaderConfig::from_config(&config)
        };
        // Opening stays lazy. A failed shard open is not cached, and registration
        // allows the same reader to retry without losing its global snapshot.
        let mut reader = ReaderBuilder::new(reader_config.clone())
            .open_current()
            .unwrap();
        assert_eq!(reader.cache.len(), 0);
        assert!(
            reader
                .get_with_options(0, &keys[0], &read)
                .unwrap_err()
                .to_string()
                .contains("render")
        );
        assert_eq!(reader.cache.len(), 0);
        reader.register_schema_transform("render", render).unwrap();
        assert!(reader.register_schema_transform("render", render).is_err());
        for bucket in [0, 1, 0] {
            assert_eq!(
                reader.get_with_options(bucket, &keys[0], &read).unwrap(),
                Some(old_row.clone()),
                "bucket {bucket}, format {file_type:?}"
            );
            assert_eq!(reader.cache.len(), 1);
        }
        assert_eq!(
            reader
                .multi_get_with_options(&[(1, &keys[0]), (0, &keys[0]), (1, &keys[10])], &read)
                .unwrap(),
            vec![Some(old_row.clone()), Some(old_row.clone()), None]
        );
        let collect_scan = |mut scan: DbIterator| {
            let mut rows = Vec::new();
            let mut pauses = 0;
            loop {
                rows.extend(scan.by_ref().map(|row| row.unwrap()));
                if !scan.stopped_at_block_boundary() {
                    break;
                }
                pauses += 1;
                assert!(pauses < 512);
                scan.clear_stop_at_block_boundary();
            }
            assert!(pauses > 0);
            rows
        };
        let expected_scan: Vec<_> = keys
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 10)
            .map(|(_, key)| {
                (
                    Bytes::copy_from_slice(key),
                    vec![old_row[1].clone(), old_row[0].clone(), old_row[2].clone()],
                )
            })
            .collect();
        assert_eq!(
            collect_scan(
                direct
                    .scan_with_options(0, b"".as_slice()..b"z".as_slice(), &scan_options)
                    .unwrap()
            ),
            expected_scan
        );
        assert_eq!(
            collect_scan(
                reader
                    .scan_with_options(1, b"".as_slice()..b"z".as_slice(), &scan_options)
                    .unwrap()
            ),
            expected_scan
        );
        let mut fixed = ReaderBuilder::new(reader_config.clone())
            .register_schema_transform("render", render)
            .unwrap()
            .open(first.id)
            .unwrap();
        assert_eq!(
            fixed.get_with_options(0, &keys[0], &read).unwrap(),
            Some(old_row.clone())
        );

        // Refresh preserves existing registrations. A new transform can be
        // registered after a lazy shard open fails, without reopening the reader.
        for db in &shards {
            db.register_schema_transform("bang", bang).unwrap();
            let mut schema = db.update_schema();
            schema
                .remap_columns(
                    Some("metrics".into()),
                    vec![
                        source(0, Some("bang")),
                        source(1, None),
                        source(2, None),
                        source(3, None),
                    ],
                )
                .unwrap();
            schema.commit();
        }
        let second = publish(&shards);
        reader.refresh().unwrap();
        assert_eq!(reader.current_global_snapshot().id, second.id);
        assert!(
            reader
                .get_with_options(0, &keys[0], &read)
                .unwrap_err()
                .to_string()
                .contains("bang")
        );
        reader.register_schema_transform("bang", bang).unwrap();
        let mut new_row = old_row.clone();
        new_row[0] = Some(Bytes::from_static(b"sum=7-tail!"));
        for bucket in [0, 1, 0] {
            assert_eq!(
                reader.get_with_options(bucket, &keys[0], &read).unwrap(),
                Some(new_row.clone())
            );
        }
        assert_eq!(
            fixed.get_with_options(0, &keys[0], &read).unwrap(),
            Some(old_row.clone())
        );
        assert_eq!(
            direct.get_with_options(0, &keys[0], &read).unwrap(),
            Some(old_row)
        );
        let mut reopened = ReaderBuilder::new(reader_config)
            .register_schema_transform("render", render)
            .unwrap()
            .register_schema_transform("bang", bang)
            .unwrap()
            .open_current()
            .unwrap();
        assert_eq!(
            reopened.get_with_options(1, &keys[0], &read).unwrap(),
            Some(new_row)
        );
        for db in shards {
            db.close().unwrap();
        }
    }
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
