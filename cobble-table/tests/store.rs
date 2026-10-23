use cobble::{Config, ExpandStorageMode, VolumeDescriptor, VolumeUsageKind};
use cobble_table::catalog::{
    Catalog, FileCatalog, FileCatalogConfig, SchemaChange, TableIdentifier,
};
use cobble_table::{LogicalType, Table, TableError, TableKey, TableSchema, TableStore, Value};
use std::time::Duration;

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
    let store = TableStore::builder(config)
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
        ExpandStorageMode::AdoptAsync,
    ));
    assert_handles_live(target.shrink_bucket(vec![0..=1]));
    drop(target_tables);

    for (snapshot, expected) in [(&old, 10), (&new, 20)] {
        target
            .expand_bucket_from_manifest(
                source.id(),
                &snapshot.manifest_path,
                Some(vec![4..=7]),
                ExpandStorageMode::AdoptAsync,
            )
            .unwrap();
        target
            .wait_for_expand_adoption(Duration::from_secs(10))
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
        let before_shrink = target.shrink_bucket(vec![4..=7]).unwrap();
        assert_eq!(
            target
                .shard_snapshot_metadata(before_shrink)
                .unwrap()
                .ranges,
            vec![0..=7]
        );
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
