use cobble::{Config, ImportStorageMode, RescaleOptions, VolumeDescriptor, VolumeUsageKind};
use cobble_table::catalog::{
    Catalog, FileCatalog, FileCatalogConfig, SchemaChange, TableIdentifier,
};
use cobble_table::{
    LogicalType, ReadOnlyTableStore, Table, TableError, TableKey, TableSchema, TableStore, Value,
};
use std::time::Duration;

#[test]
fn readonly_store_keeps_catalog_tables_and_derived_handles_at_a_fixed_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!(
            "file://{}/source",
            root.path().display()
        )),
        total_buckets: 1,
        ..Config::default()
    };
    let catalog = FileCatalog::open(&config, FileCatalogConfig::new("catalog")).unwrap();
    catalog.create_namespace(vec!["app".into()]).unwrap();
    let left_id = TableIdentifier::new(["app"], "left");
    let right_id = TableIdentifier::new(["app"], "right");
    let left_definition = catalog
        .create_table(left_id.clone(), schema(LogicalType::int32()))
        .unwrap();
    let right_definition = catalog
        .create_table(right_id.clone(), schema(LogicalType::string()))
        .unwrap();
    let writer = TableStore::builder(config.clone())
        .db_id("shared")
        .bucket_ranges(vec![0..=0])
        .open()
        .unwrap();
    let left = writer.materialize_table(&left_definition).unwrap();
    let right = writer.materialize_table(&right_definition).unwrap();
    let left_key = key(&left, 1);
    let right_key = key(&right, 1);
    left.put(&[Value::Int64(1), Value::Int32(10)]).unwrap();
    right
        .put(&[Value::Int64(1), Value::String("old".into())])
        .unwrap();
    let first = writer.snapshot_and_wait().unwrap();
    assert!(writer.retain_snapshot(first.snapshot_id));

    let newer_left = catalog
        .evolve_schema(
            &left_id,
            vec![SchemaChange::AlterFieldType {
                field_name: "value".into(),
                logical_type: LogicalType::int64(),
            }],
        )
        .unwrap();
    let renamed_right = catalog.rename_table(&right_id, "renamed".into()).unwrap();
    drop(left);
    let left = writer.materialize_table(&newer_left).unwrap();
    left.put(&[Value::Int64(1), Value::Int64(99)]).unwrap();
    right
        .put(&[Value::Int64(1), Value::String("new".into())])
        .unwrap();
    let later = catalog
        .create_table(
            TableIdentifier::new(["app"], "later"),
            schema(LogicalType::int64()),
        )
        .unwrap();
    drop(writer.materialize_table(&later).unwrap());
    let latest = writer.snapshot_and_wait().unwrap();
    writer.close().unwrap();
    drop((left, right, writer));

    assert!(
        ReadOnlyTableStore::builder(config.clone())
            .open(first.snapshot_id)
            .is_err()
    );
    let historical = ReadOnlyTableStore::builder(config.clone())
        .db_id("shared")
        .open(first.snapshot_id)
        .unwrap();
    assert_eq!(historical.id(), "shared");
    assert!(!historical.metrics().is_empty());
    let left = historical.open_materialized_table(&newer_left).unwrap();
    let right = historical.open_materialized_table(&renamed_right).unwrap();
    assert_eq!(left.schema(), left_definition.schema());
    assert_eq!(right.schema(), right_definition.schema());
    assert!(historical.open_materialized_table(&later).is_err());
    assert!(historical.open_table("missing").is_err());
    let left_row = vec![Value::Int64(1), Value::Int32(10)];
    let right_row = vec![Value::Int64(1), Value::String("old".into())];
    assert_eq!(left.get(&left_key).unwrap(), Some(left_row.clone()));
    let missing = {
        let mut key = left.key_builder();
        key.push(Value::Int64(2));
        key.build().unwrap()
    };
    assert_eq!(
        left.get_values(&left_key).unwrap(),
        Some(vec![Value::Int32(10)])
    );
    assert_eq!(
        left.multi_get_values(&[left_key.clone(), missing, left_key.clone()])
            .unwrap(),
        vec![
            Some(vec![Value::Int32(10)]),
            None,
            Some(vec![Value::Int32(10)])
        ]
    );
    assert_eq!(
        left.scan_key_prefix(left_key.values())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![left_row.clone()]
    );
    assert_eq!(
        right.multi_get(std::slice::from_ref(&right_key)).unwrap(),
        vec![Some(right_row.clone())]
    );
    assert_eq!(
        right
            .scan(0)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![right_row.clone()]
    );
    let projection = left.project_by_names(&["value"]).unwrap();
    assert_eq!(
        left.scan_encoded_bounds(0, Some(left_key.encoded()), None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![left_row.clone()]
    );
    assert!(
        left.scan_encoded_bounds(0, None, Some(left_key.encoded()))
            .unwrap()
            .next()
            .is_none()
    );
    let prefix = &left_key.encoded()[..left_key.encoded().len() - 1];
    assert_eq!(
        left.scan_encoded_bounds(0, Some(prefix), None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![left_row.clone()]
    );
    let scanner = right.scan_key_prefix(right_key.values()).unwrap();
    drop(historical);
    assert_eq!(left.get(&left_key).unwrap(), Some(left_row));
    assert_eq!(
        projection.get(&left_key).unwrap(),
        Some(vec![Value::Int32(10)])
    );
    drop((left, right));
    assert_eq!(
        projection
            .scan(0)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![vec![Value::Int32(10)]]
    );
    drop(projection);
    assert_eq!(
        scanner.collect::<Result<Vec<_>, _>>().unwrap(),
        vec![right_row]
    );

    let latest = ReadOnlyTableStore::builder(config.clone())
        .db_id("shared")
        .open(latest.snapshot_id)
        .unwrap();
    assert_eq!(
        latest
            .open_materialized_table(&newer_left)
            .unwrap()
            .get(&left_key)
            .unwrap(),
        Some(vec![Value::Int64(1), Value::Int64(99)])
    );
    assert!(latest.open_materialized_table(&later).is_ok());
    let latest_left = latest.open_materialized_table(&newer_left).unwrap();
    assert_eq!(
        latest_left.get_values(&left_key).unwrap(),
        Some(vec![Value::Int64(99)])
    );
    assert_eq!(
        latest_left
            .scan_key_prefix(left_key.values())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![vec![Value::Int64(1), Value::Int64(99)]]
    );

    // The manifest may be nested beneath the configured route instead of its conventional root.
    let routed = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root.path().display())),
        ..config
    };
    assert!(
        ReadOnlyTableStore::builder(routed.clone())
            .db_id("shared")
            .open(first.snapshot_id)
            .is_err()
    );
    let shard = cobble::ShardSnapshotRef {
        column_family_ids: first.column_family_ids(),
        timestamp_seconds: first.timestamp_seconds,
        db_id: first.db_id,
        snapshot_id: first.snapshot_id,
        manifest_path: first.manifest_path,
        ranges: first.ranges,
        data_size_bytes: first.data_size_bytes,
        incremental_data_size_bytes: first.incremental_data_size_bytes,
    };
    let explicit = ReadOnlyTableStore::builder(routed.clone())
        .open_shard_snapshot(&shard)
        .unwrap();
    assert_eq!(
        explicit
            .open_materialized_table(&newer_left)
            .unwrap()
            .get(&left_key)
            .unwrap(),
        Some(vec![Value::Int64(1), Value::Int32(10)])
    );
    // The snapshot identity comes from the payload, not from a SNAPSHOT-N filename.
    let original = url::Url::parse(&shard.manifest_path)
        .unwrap()
        .to_file_path()
        .unwrap();
    let selected = original.with_file_name("selected-manifest");
    std::fs::copy(original, &selected).unwrap();
    for source in [
        ReadOnlyTableStore::builder(routed.clone()),
        ReadOnlyTableStore::builder(routed.clone()).db_id("shared"),
    ] {
        let direct = source
            .open_with_manifest_path(selected.to_str().unwrap())
            .unwrap();
        assert_eq!(direct.id(), "shared");
        assert_eq!(
            direct
                .open_materialized_table(&newer_left)
                .unwrap()
                .get(&left_key)
                .unwrap(),
            Some(vec![Value::Int64(1), Value::Int32(10)])
        );
    }
    let mut wrong_id = shard;
    wrong_id.snapshot_id += 1;
    assert!(
        matches!(ReadOnlyTableStore::builder(routed).open_shard_snapshot(&wrong_id), Err(TableError::Storage(cobble::Error::InvalidState(message))) if message.contains("expected"))
    );
}

#[test]
fn store_snapshots_restore_all_tables_and_require_released_handles() {
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        volumes: vec![VolumeDescriptor::new(
            format!("file://{}", root.path().display()),
            vec![
                VolumeUsageKind::Meta,
                VolumeUsageKind::PrimaryDataPriorityHigh,
                VolumeUsageKind::Wal,
            ],
        )],
        total_buckets: 8,
        wal_enabled: true,
        ..Config::default()
    };
    let catalog = FileCatalog::open(&config, FileCatalogConfig::new("catalog")).unwrap();
    catalog.create_namespace(vec!["app".into()]).unwrap();
    let identifier = TableIdentifier::new(["app"], "left");
    let initial = catalog
        .create_table(identifier.clone(), schema(LogicalType::int32()))
        .unwrap();
    let mut store = TableStore::builder(config.clone())
        .db_id("shared")
        .bucket_ranges(vec![0..=7])
        .open()
        .unwrap();
    assert_eq!(store.id(), "shared");
    assert!(!store.metrics().is_empty());
    let left = store.materialize_table(&initial).unwrap();
    let right = store
        .create_table("right", schema(LogicalType::string()))
        .unwrap();
    let left_key = key(&left, 1);
    let right_key = key(&right, 1);
    left.put(&[Value::Int64(1), Value::Int32(10)]).unwrap();
    right
        .put(&[Value::Int64(1), Value::String("initial".into())])
        .unwrap();
    let first = store.snapshot_and_wait().unwrap();
    assert!(store.retain_snapshot(first.snapshot_id));

    let evolved = catalog
        .evolve_schema(
            &identifier,
            vec![SchemaChange::AlterFieldType {
                field_name: "value".into(),
                logical_type: LogicalType::int64(),
            }],
        )
        .unwrap();
    drop(left);
    let left = store.materialize_table(&evolved).unwrap();
    assert_eq!(
        left.get(&left_key).unwrap(),
        Some(vec![Value::Int64(1), Value::Int64(10)])
    );
    right
        .put(&[Value::Int64(1), Value::String("checkpoint".into())])
        .unwrap();
    let checkpoint = store.snapshot_and_wait().unwrap();
    assert_eq!(checkpoint.db_id, store.id());
    assert_eq!(
        store
            .shard_snapshot_metadata(checkpoint.snapshot_id)
            .unwrap()
            .manifest_path,
        checkpoint.manifest_path
    );
    left.put(&[Value::Int64(1), Value::Int64(99)]).unwrap();
    right
        .put(&[Value::Int64(1), Value::String("after".into())])
        .unwrap();
    drop(
        store
            .create_table("later", schema(LogicalType::string()))
            .unwrap(),
    );
    let projection = left.project_by_names(&["value"]).unwrap();
    let scanner = projection.scan(left_key.bucket()).unwrap();

    // Each independently owned handle blocks a restart before it can change the store.
    assert_handles_live(store.switch_to_snapshot(first.snapshot_id));
    assert_eq!(
        left.get(&left_key).unwrap(),
        Some(vec![Value::Int64(1), Value::Int64(99)])
    );
    drop(left);
    drop(right);
    assert_handles_live(store.switch_to_snapshot(first.snapshot_id));
    assert_eq!(
        projection.get(&left_key).unwrap(),
        Some(vec![Value::Int64(99)])
    );
    drop(projection);
    assert_handles_live(store.switch_to_snapshot(first.snapshot_id));
    drop(scanner);
    store.switch_to_snapshot(first.snapshot_id).unwrap();
    {
        let left = store.open_materialized_table(&evolved).unwrap();
        let right = store.open_table("right").unwrap();
        assert_eq!(left.schema(), initial.schema());
        assert_eq!(
            left.get(&left_key).unwrap(),
            Some(vec![Value::Int64(1), Value::Int32(10)])
        );
        assert_eq!(
            right.get(&right_key).unwrap(),
            Some(vec![Value::Int64(1), Value::String("initial".into())])
        );
        assert!(store.open_table("later").is_err());
    }
    store.switch_to_snapshot(checkpoint.snapshot_id).unwrap();
    let left = store.open_materialized_table(&evolved).unwrap();
    assert_eq!(left.schema(), evolved.schema());
    assert_eq!(
        left.get(&left_key).unwrap(),
        Some(vec![Value::Int64(1), Value::Int64(10)])
    );
    store.close().unwrap();
    assert!(left.get(&left_key).is_err());
    drop(left);
    drop(store);

    // Process-style exact recovery also installs widening transforms before opening the DB,
    // and does not replay the durable post-checkpoint WAL tail.
    let store = TableStore::builder(config.clone())
        .db_id("shared")
        .resume_from_snapshot(checkpoint.snapshot_id)
        .unwrap();
    let left = store.open_materialized_table(&evolved).unwrap();
    let right = store.open_table("right").unwrap();
    assert_eq!(left.schema(), evolved.schema());
    assert_eq!(
        left.get(&left_key).unwrap(),
        Some(vec![Value::Int64(1), Value::Int64(10)])
    );
    assert_eq!(
        right.get(&right_key).unwrap(),
        Some(vec![Value::Int64(1), Value::String("checkpoint".into())])
    );

    // A new job imports the checkpoint without taking over the source identity or WAL tail.
    let imported = TableStore::builder(config.clone())
        .open_new_with_manifest_path(&checkpoint.manifest_path)
        .unwrap();
    assert_ne!(imported.id(), store.id());
    {
        let left = imported.open_materialized_table(&evolved).unwrap();
        let right = imported.open_table("right").unwrap();
        assert_eq!(left.schema(), evolved.schema());
        assert_eq!(
            left.get(&left_key).unwrap(),
            Some(vec![Value::Int64(1), Value::Int64(10)])
        );
        assert_eq!(
            right.get(&right_key).unwrap(),
            Some(vec![Value::Int64(1), Value::String("checkpoint".into())])
        );
        assert!(imported.open_table("later").is_err());
        right
            .put(&[Value::Int64(1), Value::String("new job".into())])
            .unwrap();
    }
    let imported_snapshot = imported.snapshot_and_wait().unwrap();
    assert_eq!(imported_snapshot.db_id, imported.id());
    assert_eq!(imported_snapshot.ranges, checkpoint.ranges);
    imported.close().unwrap();
    drop(imported);
    let imported = TableStore::builder(config)
        .db_id(&imported_snapshot.db_id)
        .resume_from_snapshot(imported_snapshot.snapshot_id)
        .unwrap();
    assert_eq!(
        imported
            .open_table("right")
            .unwrap()
            .get(&right_key)
            .unwrap(),
        Some(vec![Value::Int64(1), Value::String("new job".into())])
    );
    assert_eq!(
        right.get(&right_key).unwrap(),
        Some(vec![Value::Int64(1), Value::String("checkpoint".into())])
    );
    imported.close().unwrap();
    assert!(!store.cancel_snapshot(checkpoint.snapshot_id).unwrap());
    assert!(store.expire_snapshot(first.snapshot_id).unwrap());
    store.close().unwrap();
}

#[test]
fn store_bucket_migration_moves_both_tables_from_selected_source_snapshots() {
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root.path().display())),
        total_buckets: 8,
        ..Config::default()
    };
    let source = TableStore::builder(config.clone())
        .db_id("source")
        .bucket_ranges(vec![4..=7])
        .open()
        .unwrap();
    let mut target = TableStore::builder(config)
        .db_id("target")
        .bucket_ranges(vec![0..=3])
        .open()
        .unwrap();
    let names = ["left", "right"];
    let source_tables = names.map(|name| {
        source
            .create_table(name, schema(LogicalType::int64()))
            .unwrap()
    });
    let target_tables = names.map(|name| {
        target
            .create_table(name, schema(LogicalType::int64()))
            .unwrap()
    });
    let ids = (0..64).collect::<Vec<_>>();
    for table in &source_tables {
        for &id in &ids {
            if key(table, id).bucket() >= 4 {
                table.put(&[Value::Int64(id), Value::Int64(10)]).unwrap();
            }
        }
    }
    for table in &target_tables {
        for &id in &ids {
            if key(table, id).bucket() < 4 {
                table.put(&[Value::Int64(id), Value::Int64(1)]).unwrap();
            }
        }
    }
    let old = source.snapshot_and_wait().unwrap();
    for table in &source_tables {
        for &id in &ids {
            if key(table, id).bucket() >= 4 {
                table.put(&[Value::Int64(id), Value::Int64(20)]).unwrap();
            }
        }
    }
    let new = source.snapshot_and_wait().unwrap();
    assert_handles_live(target.expand_bucket_from_manifest(
        source.id(),
        &old.manifest_path,
        Some(vec![4..=7]),
        ImportStorageMode::AdoptAsync,
    ));
    assert_handles_live(target.shrink_bucket(vec![0..=1]));
    let skip_snapshots = RescaleOptions {
        snapshot_before: false,
        snapshot_after: false,
    };
    assert_handles_live(target.expand_bucket_from_manifest_with_options(
        source.id(),
        &old.manifest_path,
        Some(vec![4..=7]),
        ImportStorageMode::AdoptAsync,
        skip_snapshots,
    ));
    assert_handles_live(target.shrink_bucket_with_options(vec![0..=1], skip_snapshots));
    drop(target_tables);

    for (snapshot, expected, options) in [
        (&old, 10, RescaleOptions::default()),
        (&new, 20, skip_snapshots),
    ] {
        if options == RescaleOptions::default() {
            target
                .expand_bucket_from_manifest(
                    source.id(),
                    &snapshot.manifest_path,
                    Some(vec![4..=7]),
                    ImportStorageMode::AdoptAsync,
                )
                .unwrap();
        } else {
            target
                .expand_bucket_from_manifest_with_options(
                    source.id(),
                    &snapshot.manifest_path,
                    Some(vec![4..=7]),
                    ImportStorageMode::AdoptAsync,
                    options,
                )
                .unwrap();
        }
        target
            .wait_for_import_adoption(Duration::from_secs(10))
            .unwrap();
        for name in names {
            let table = target.open_table(name).unwrap();
            for &id in &ids {
                let key = key(&table, id);
                let value = if key.bucket() < 4 { 1 } else { expected };
                assert_eq!(
                    table.get(&key).unwrap(),
                    Some(vec![Value::Int64(id), Value::Int64(value)])
                );
            }
        }
        let before_shrink = if options == RescaleOptions::default() {
            Some(target.shrink_bucket(vec![4..=7]).unwrap())
        } else {
            target
                .shrink_bucket_with_options(vec![4..=7], options)
                .unwrap()
        };
        assert_eq!(before_shrink.is_some(), options.snapshot_before);
        if let Some(before_shrink) = before_shrink {
            assert_eq!(
                target
                    .shard_snapshot_metadata(before_shrink)
                    .unwrap()
                    .ranges,
                vec![0..=7]
            );
        }
        assert_eq!(target.snapshot_and_wait().unwrap().ranges, vec![0..=3]);
        let table = target.open_table("left").unwrap();
        let migrated_id = ids
            .iter()
            .copied()
            .find(|&id| key(&table, id).bucket() >= 4)
            .unwrap();
        assert_eq!(table.get(&key(&table, migrated_id)).unwrap(), None);
    }
    source.close().unwrap();
    target.close().unwrap();
}

fn schema(value_type: LogicalType) -> TableSchema {
    TableSchema::builder()
        .field("id", LogicalType::int64())
        .field("value", value_type)
        .primary_key(["id"])
        .bucket_key(["id"])
        .build()
        .unwrap()
}

fn key(table: &Table, id: i64) -> TableKey {
    let mut builder = table.key_builder();
    builder.push(Value::Int64(id));
    builder.build().unwrap()
}

fn assert_handles_live<T>(result: cobble_table::Result<T>) {
    assert!(
        matches!(result, Err(TableError::Storage(cobble::Error::InvalidState(message))) if message.contains("release all"))
    );
}
