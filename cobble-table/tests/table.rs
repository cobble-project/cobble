use cobble::{
    Config, CoordinatorConfig, DbBuilder, DbCoordinator, Reader, ReaderConfig, VolumeDescriptor,
};
use cobble_table::snapshot::TableSnapshotCommitter;
use cobble_table::{
    DataField, LogicalType, ReadOnlyTable, ReadOnlyTableBuilder, Table, TableKey, TableKeyBuilder,
    TableReader, TableReaderBuilder, TableSchema, TableWriterBuilder, Value,
};
use std::sync::{Arc, mpsc};

#[test]
fn table_runtime_create_open_and_typed_rows() {
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root.path().display())),
        total_buckets: 8,
        ..Config::default()
    };
    let schema = TableSchema::new(
        vec![
            DataField::new(1, "tenant", LogicalType::string()).unwrap(),
            DataField::new(2, "id", LogicalType::int64()).unwrap(),
            DataField::new(3, "name", LogicalType::string().nullable()).unwrap(),
            DataField::new(4, "tags", LogicalType::list(LogicalType::string())).unwrap(),
            DataField::new(
                5,
                "attributes",
                LogicalType::map(LogicalType::string(), LogicalType::binary().nullable()),
            )
            .unwrap(),
        ],
        vec![1.into(), 2.into()],
        vec![1.into()],
    )
    .unwrap();
    let row1 = vec![
        Value::String("tenant-a".to_string()),
        Value::Int64(1),
        Value::Null,
        Value::List(vec![Value::String("blue".to_string())]),
        Value::Map(vec![(
            Value::String("tier".to_string()),
            Value::Binary(vec![1, 2].into()),
        )]),
    ];
    let row2 = vec![
        Value::String("tenant-a".to_string()),
        Value::Int64(2),
        Value::String("second".to_string()),
        Value::List(vec![Value::String("green".to_string())]),
        Value::Map(vec![]),
    ];
    let key1 = vec![row1[0].clone(), row1[1].clone()];
    let key2 = vec![row2[0].clone(), row2[1].clone()];
    let missing = vec![Value::String("tenant-a".to_string()), Value::Int64(9)];

    let db = Arc::new(
        DbBuilder::new(config.clone())
            .bucket_ranges(vec![0..=7])
            .db_id("table-runtime")
            .open()
            .unwrap(),
    );
    {
        let table = Table::create(Arc::clone(&db), "events", schema.clone()).unwrap();
        assert_eq!(
            Table::create(Arc::clone(&db), "events", schema.clone())
                .unwrap()
                .schema(),
            &schema
        );
        let mut incomplete = table.key_builder();
        incomplete.push(key1[0].clone());
        assert!(incomplete.build().is_err());
        let key1 = build_key(&table, &key1);
        let key2 = build_key(&table, &key2);
        let missing = build_key(&table, &missing);
        table.put(&row1).unwrap();
        table.put(&row2).unwrap();
        assert_eq!(table.get(&key1).unwrap(), Some(row1.clone()));
        assert_eq!(table.get(&missing).unwrap(), None);
        let multi_keys = vec![key2.clone(), key1.clone(), key2.clone(), missing.clone()];
        assert_eq!(
            table.multi_get(&multi_keys).unwrap(),
            vec![
                Some(row2.clone()),
                Some(row1.clone()),
                Some(row2.clone()),
                None
            ]
        );
        let bucket = key1.bucket();
        assert!(
            table
                .scan_bounds((bucket + 1) % 8, Some(&key1), None)
                .is_err()
        );
        let rows = table
            .scan_bounds(bucket, Some(&key1), Some(&missing))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows, vec![row1.clone(), row2.clone()]);

        let projection = table
            .project_by_names(&["attributes", "tenant", "name"])
            .unwrap();
        let projected1 = vec![row1[4].clone(), row1[0].clone(), row1[2].clone()];
        let projected2 = vec![row2[4].clone(), row2[0].clone(), row2[2].clone()];
        assert_eq!(projection.get(&key1).unwrap(), Some(projected1.clone()));
        assert_eq!(
            projection
                .multi_get(&[key2.clone(), key1.clone(), key2.clone(), missing.clone()])
                .unwrap(),
            vec![
                Some(projected2.clone()),
                Some(projected1.clone()),
                Some(projected2.clone()),
                None
            ]
        );
        assert_eq!(
            projection
                .scan_bounds(bucket, Some(&key1), Some(&missing))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![projected1, projected2]
        );
        let key_projection = table.project_by_names(&["id"]).unwrap();
        assert_eq!(
            key_projection.get(&key1).unwrap(),
            Some(vec![row1[1].clone()])
        );
        assert_eq!(key_projection.get(&missing).unwrap(), None);
        let value_projection = table.project_by_names(&["tags"]).unwrap();
        assert_eq!(
            value_projection.get(&key2).unwrap(),
            Some(vec![row2[3].clone()])
        );
        assert_eq!(
            value_projection
                .scan_bounds(bucket, Some(&key1), Some(&missing))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![vec![row1[3].clone()], vec![row2[3].clone()]]
        );
        assert!(table.project_by_names::<&str>(&[]).is_err());
        assert!(table.project_by_names(&["tenant", "tenant"]).is_err());
        assert!(table.project_by_names(&["missing"]).is_err());

        table.delete(&key1).unwrap();
        assert_eq!(table.get(&key1).unwrap(), None);

        let keys = Table::create(
            Arc::clone(&db),
            "keys",
            TableSchema::new(
                vec![DataField::new(1, "id", LogicalType::int64()).unwrap()],
                vec![1.into()],
                vec![1.into()],
            )
            .unwrap(),
        )
        .unwrap();
        let key_only = vec![Value::Int64(42)];
        let key_only_key = build_key(&keys, &key_only);
        let (other_key_only, other_key_only_key) = (43..100)
            .map(|value| {
                let row = vec![Value::Int64(value)];
                let key = build_key(&keys, &row);
                (row, key)
            })
            .find(|(_, key)| key.bucket() != key_only_key.bucket())
            .unwrap();
        keys.put(&key_only).unwrap();
        keys.put(&other_key_only).unwrap();
        assert_eq!(keys.get(&key_only_key).unwrap(), Some(key_only));
        let key_only_projection = keys.project_by_names(&["id"]).unwrap();
        assert_eq!(
            key_only_projection.get(&key_only_key).unwrap(),
            Some(vec![Value::Int64(42)])
        );
        keys.delete_batch(&[
            key_only_key.clone(),
            other_key_only_key.clone(),
            key_only_key.clone(),
        ])
        .unwrap();
        assert_eq!(keys.get(&key_only_key).unwrap(), None);
        assert_eq!(keys.get(&other_key_only_key).unwrap(), None);
        assert_eq!(key_only_projection.get(&key_only_key).unwrap(), None);
        keys.put(&[Value::Int64(42)]).unwrap();
    }
    let (sender, receiver) = mpsc::sync_channel(1);
    let snapshot_id = db
        .snapshot_with_callback(move |result| sender.send(result).unwrap())
        .unwrap();
    receiver.recv().unwrap().unwrap();
    db.close().unwrap();

    let read_only = Arc::new(
        cobble::ReadOnlyDb::open_with_db_id(config.clone(), snapshot_id, "table-runtime").unwrap(),
    );
    {
        let table = ReadOnlyTable::open(Arc::clone(&read_only), "events").unwrap();
        assert_eq!(table.schema(), &schema);
        let key2 = build_read_only_key(
            &table,
            &[Value::String("tenant-a".to_string()), Value::Int64(2)],
        );
        let missing = build_read_only_key(
            &table,
            &[Value::String("tenant-a".to_string()), Value::Int64(9)],
        );
        assert_eq!(table.get(&key2).unwrap(), Some(row2.clone()));
        assert_eq!(table.get(&missing).unwrap(), None);
        assert_eq!(
            table
                .multi_get(&[key2.clone(), missing.clone(), key2.clone()])
                .unwrap(),
            vec![Some(row2.clone()), None, Some(row2.clone())]
        );
        let projection = table.project_by_names(&["tenant", "attributes"]).unwrap();
        assert_eq!(
            projection.get(&key2).unwrap(),
            Some(vec![row2[0].clone(), row2[4].clone()])
        );
        assert_eq!(
            table.project_by_names(&["id"]).unwrap().get(&key2).unwrap(),
            Some(vec![row2[1].clone()])
        );
        assert_eq!(
            table
                .scan_bounds(key2.bucket(), Some(&key2), Some(&missing))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![row2.clone()]
        );
        assert_eq!(
            table
                .project_by_names(&["name"])
                .unwrap()
                .scan_bounds(key2.bucket(), Some(&key2), Some(&missing))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![vec![row2[2].clone()]]
        );

        let detached_projection = {
            let handle = ReadOnlyTable::open(Arc::clone(&read_only), "events").unwrap();
            handle.project_by_names(&["id"]).unwrap()
        };
        assert_eq!(
            detached_projection.get(&key2).unwrap(),
            Some(vec![row2[1].clone()])
        );
        let detached_scan = {
            let handle = ReadOnlyTable::open(Arc::clone(&read_only), "events").unwrap();
            handle
                .scan_bounds(key2.bucket(), Some(&key2), Some(&missing))
                .unwrap()
        };
        assert_eq!(
            detached_scan.collect::<Result<Vec<_>, _>>().unwrap(),
            vec![row2.clone()]
        );

        let keys = ReadOnlyTable::open(Arc::clone(&read_only), "keys").unwrap();
        let key = build_read_only_key(&keys, &[Value::Int64(42)]);
        assert_eq!(keys.get(&key).unwrap(), Some(vec![Value::Int64(42)]));
        assert_eq!(
            keys.project_by_names(&["id"]).unwrap().get(&key).unwrap(),
            Some(vec![Value::Int64(42)])
        );
    }
    drop(read_only);

    let reopened =
        Arc::new(cobble::Db::open_from_snapshot(config, snapshot_id, "table-runtime").unwrap());
    {
        let table = Table::open(Arc::clone(&reopened), "events").unwrap();
        let key2 = build_key(
            &table,
            &[Value::String("tenant-a".to_string()), Value::Int64(2)],
        );
        assert_eq!(table.get(&key2).unwrap(), Some(row2));
        let keys = Table::open(Arc::clone(&reopened), "keys").unwrap();
        let key = build_key(&keys, &[Value::Int64(42)]);
        assert_eq!(keys.get(&key).unwrap(), Some(vec![Value::Int64(42)]));
    }
    reopened.close().unwrap();
}

#[test]
fn schema_only_snapshot_preserves_empty_table() {
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root.path().display())),
        total_buckets: 8,
        ..Config::default()
    };
    let schema = TableSchema::new(
        vec![
            DataField::new(0, "id", LogicalType::int64()).unwrap(),
            DataField::new(1, "name", LogicalType::string().nullable()).unwrap(),
        ],
        vec![0.into()],
        vec![0.into()],
    )
    .unwrap();
    let db = Arc::new(
        DbBuilder::new(config.clone())
            .bucket_ranges(vec![0..=7])
            .db_id("empty-table")
            .open()
            .unwrap(),
    );
    Table::create(Arc::clone(&db), "events", schema.clone()).unwrap();
    let (sender, receiver) = mpsc::sync_channel(1);
    let snapshot_id = db
        .snapshot_with_callback(move |result| sender.send(result).unwrap())
        .unwrap();
    receiver.recv().unwrap().unwrap();
    db.close().unwrap();

    let read_only =
        Arc::new(cobble::ReadOnlyDb::open_with_db_id(config, snapshot_id, "empty-table").unwrap());
    let table = ReadOnlyTable::open(Arc::clone(&read_only), "events").unwrap();
    assert_eq!(table.schema(), &schema);
}

#[test]
fn standalone_table_shard_owns_storage_snapshots_and_cursors() {
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root.path().display())),
        total_buckets: 1,
        ..Config::default()
    };
    let writer_builder = || {
        TableWriterBuilder::new(config.clone())
            .table_name("events")
            .db_id("owned-shard")
            .bucket_ranges(vec![0..=0])
    };
    let schema = runtime_schema(LogicalType::string().nullable());
    let writer = writer_builder().create(schema.clone()).unwrap();
    let empty = writer.snapshot_and_wait().unwrap();
    let empty_reader = ReadOnlyTableBuilder::new(config.clone())
        .table_name("events")
        .shard_snapshot(&empty.db_id, empty.snapshot_id)
        .open()
        .unwrap();
    assert_eq!(empty_reader.schema(), &schema);
    assert!(empty_reader.scan(0).unwrap().next().is_none());
    drop(empty_reader);

    let row = vec![Value::Int64(1), Value::String("first".into())];
    let other = vec![Value::Int64(2), Value::Null];
    let key = build_runtime_key(writer.key_builder(), &row[..1]);
    let other_key = build_runtime_key(writer.key_builder(), &other[..1]);
    let missing = build_runtime_key(writer.key_builder(), &[Value::Int64(3)]);
    writer.put(&row).unwrap();
    writer.put(&other).unwrap();
    writer
        .delete_batch(&[other_key.clone(), other_key.clone()])
        .unwrap();
    writer.put(&other).unwrap();
    writer.delete(&other_key).unwrap();
    assert_eq!(writer.get(&other_key).unwrap(), None);
    assert_eq!(
        writer
            .multi_get(&[key.clone(), missing.clone(), key.clone()])
            .unwrap(),
        vec![Some(row.clone()), None, Some(row.clone())]
    );
    let snapshot = writer.snapshot_and_wait().unwrap();
    let reader = ReadOnlyTableBuilder::new(config.clone())
        .table_name("events")
        .shard_snapshot(&snapshot.db_id, snapshot.snapshot_id)
        .open()
        .unwrap();
    let reader_key = build_runtime_key(reader.key_builder(), &row[..1]);
    assert_eq!(
        reader.multi_get(&[missing, reader_key.clone()]).unwrap(),
        vec![None, Some(row.clone())]
    );

    let updated = vec![Value::Int64(1), Value::String("second".into())];
    writer.put(&updated).unwrap();
    let latest = writer.snapshot_and_wait().unwrap();
    assert_eq!(reader.get(&reader_key).unwrap(), Some(row.clone()));

    // Both kinds of writer cursor must outlive the owning handle without
    // triggering Db::drop while an iterator still holds its access guard.
    let scan = writer.scan_bounds(0, Some(&key), None).unwrap();
    let projection = writer.project_by_names(&["name", "id"]).unwrap();
    assert_eq!(
        projection.get(&key).unwrap(),
        Some(vec![updated[1].clone(), updated[0].clone()])
    );
    let projected_scan = projection.scan(0).unwrap();
    drop(projection);
    drop(writer);
    assert_eq!(
        scan.collect::<Result<Vec<_>, _>>().unwrap(),
        vec![updated.clone()]
    );
    assert_eq!(
        projected_scan.collect::<Result<Vec<_>, _>>().unwrap(),
        vec![vec![updated[1].clone(), updated[0].clone()]]
    );

    let resumed = writer_builder().resume().unwrap();
    assert_eq!(resumed.schema(), &schema);
    assert_eq!(resumed.get(&key).unwrap(), Some(updated));
    assert!(resumed.shard_snapshot_metadata(latest.snapshot_id).is_ok());
    drop(resumed);

    let projection = reader.project_by_names(&["name"]).unwrap();
    let scan = reader.scan(0).unwrap();
    drop(reader);
    assert_eq!(
        projection.get(&reader_key).unwrap(),
        Some(vec![row[1].clone()])
    );
    assert_eq!(scan.collect::<Result<Vec<_>, _>>().unwrap(), vec![row]);
}

#[test]
fn standalone_table_global_reader_routes_pins_and_validates_schema() {
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root.path().display())),
        total_buckets: 4,
        ..Config::default()
    };
    let schema = runtime_schema(LogicalType::string().nullable());
    let left = TableWriterBuilder::new(config.clone())
        .table_name("events")
        .db_id("left")
        .bucket_ranges(vec![0..=1])
        .create(schema.clone())
        .unwrap();
    let right = TableWriterBuilder::new(config.clone())
        .table_name("events")
        .db_id("right")
        .bucket_ranges(vec![2..=3])
        .create(schema.clone())
        .unwrap();
    let rows = (0..16)
        .map(|id| vec![Value::Int64(id), Value::String(format!("row-{id}"))])
        .collect::<Vec<_>>();
    let keys = rows
        .iter()
        .map(|row| build_runtime_key(left.key_builder(), &row[..1]))
        .collect::<Vec<_>>();
    for (row, key) in rows.iter().zip(&keys) {
        if key.bucket() < 2 {
            left.put(row).unwrap();
        } else {
            right.put(row).unwrap();
        }
    }
    let coordinator = DbCoordinator::open(CoordinatorConfig::from_config(&config)).unwrap();
    let first = coordinator
        .take_global_snapshot(
            4,
            vec![
                left.snapshot_and_wait().unwrap(),
                right.snapshot_and_wait().unwrap(),
            ],
        )
        .unwrap();
    coordinator.materialize_global_snapshot(&first).unwrap();

    // The global manifest, not the caller's default config, determines key hashing.
    let mut reader_config = config.clone();
    reader_config.total_buckets = 64;
    reader_config.reader.pin_partition_in_memory_count = 1;
    let open_reader = || TableReaderBuilder::new(reader_config.clone()).table_name("events");
    let reader = open_reader().current_global_snapshot().open().unwrap();
    assert_eq!(reader.schema(), &schema);
    let reader_keys = rows
        .iter()
        .map(|row| build_runtime_key(reader.key_builder(), &row[..1]))
        .collect::<Vec<_>>();
    assert_eq!(
        reader_keys.iter().map(TableKey::bucket).collect::<Vec<_>>(),
        keys.iter().map(TableKey::bucket).collect::<Vec<_>>()
    );
    let missing = build_runtime_key(reader.key_builder(), &[Value::Int64(100)]);
    let mut requests = reader_keys.clone();
    requests.extend([reader_keys[0].clone(), missing]);
    let mut expected = rows.iter().cloned().map(Some).collect::<Vec<_>>();
    expected.extend([Some(rows[0].clone()), None]);
    assert_eq!(reader.multi_get(&requests).unwrap(), expected);
    let direct_reader = TableReader::open(
        Reader::open_current(ReaderConfig::from_config(&reader_config)).unwrap(),
        "events",
    )
    .unwrap();
    assert_eq!(
        direct_reader.get(&reader_keys[0]).unwrap(),
        Some(rows[0].clone())
    );
    // Alternate routed shards with a one-entry cache, including projected batches.
    for _ in 0..2 {
        for (key, row) in reader_keys.iter().zip(&rows) {
            assert_eq!(reader.get(key).unwrap(), Some(row.clone()));
        }
    }
    let projection = reader.project_by_names(&["name"]).unwrap();
    assert_eq!(
        projection.multi_get(&reader_keys).unwrap(),
        rows.iter()
            .map(|row| Some(vec![row[1].clone()]))
            .collect::<Vec<_>>()
    );
    for bucket in 0..4 {
        let bucket_rows = rows
            .iter()
            .zip(&keys)
            .filter(|(_, key)| key.bucket() == bucket)
            .map(|(row, _)| row.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            reader
                .scan(bucket)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            bucket_rows
        );
    }

    let updated = vec![rows[0][0].clone(), Value::String("updated".into())];
    if keys[0].bucket() < 2 {
        left.put(&updated).unwrap();
    } else {
        right.put(&updated).unwrap();
    }
    let second = coordinator
        .take_global_snapshot(
            4,
            vec![
                left.snapshot_and_wait().unwrap(),
                right.snapshot_and_wait().unwrap(),
            ],
        )
        .unwrap();
    coordinator.materialize_global_snapshot(&second).unwrap();
    assert_eq!(reader.get(&reader_keys[0]).unwrap(), Some(rows[0].clone()));
    assert_eq!(
        direct_reader.get(&reader_keys[0]).unwrap(),
        Some(rows[0].clone())
    );
    drop(direct_reader);
    assert_eq!(
        open_reader()
            .global_snapshot(first.id)
            .open()
            .unwrap()
            .get(&reader_keys[0])
            .unwrap(),
        Some(rows[0].clone())
    );
    assert_eq!(
        open_reader()
            .current_global_snapshot()
            .open()
            .unwrap()
            .get(&reader_keys[0])
            .unwrap(),
        Some(updated)
    );

    let cursor = projection.scan(reader_keys[0].bucket()).unwrap();
    drop(projection);
    drop(reader);
    assert_eq!(
        cursor.collect::<Result<Vec<_>, _>>().unwrap(),
        rows.iter()
            .zip(&keys)
            .filter(|(_, key)| key.bucket() == keys[0].bucket())
            .map(|(row, _)| vec![row[1].clone()])
            .collect::<Vec<_>>()
    );

    // Commit preparation rejects incompatible shard metadata before publishing a mixed snapshot.
    drop(right);
    let incompatible = TableWriterBuilder::new(config.clone())
        .table_name("events")
        .db_id("incompatible")
        .bucket_ranges(vec![2..=3])
        .create(runtime_schema(LogicalType::int64().nullable()))
        .unwrap();
    let committer = TableSnapshotCommitter::new(
        Arc::new(DbCoordinator::open(CoordinatorConfig::from_config(&config)).unwrap()),
        4,
        2,
    )
    .unwrap();
    let current = coordinator.load_current_global_snapshot().unwrap();
    assert!(
        committer
            .commit_batch(
                99,
                vec![
                    left.snapshot_and_wait().unwrap(),
                    incompatible.snapshot_and_wait().unwrap(),
                ],
            )
            .is_err()
    );
    assert_eq!(coordinator.load_current_global_snapshot().unwrap(), current);
    drop(incompatible);
    drop(left);
}

fn runtime_schema(value_type: LogicalType) -> TableSchema {
    TableSchema::builder()
        .field("id", LogicalType::int64())
        .field("name", value_type)
        .primary_key(["id"])
        .bucket_key(["id"])
        .build()
        .unwrap()
}

fn build_key(table: &Table, values: &[Value]) -> TableKey {
    build_runtime_key(table.key_builder(), values)
}

fn build_runtime_key(mut builder: TableKeyBuilder, values: &[Value]) -> TableKey {
    for value in values {
        builder.push(value.clone());
    }
    builder.build().unwrap()
}

fn build_read_only_key(table: &ReadOnlyTable, values: &[Value]) -> TableKey {
    build_runtime_key(table.key_builder(), values)
}
