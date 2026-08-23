use cobble::{Config, DbBuilder, VolumeDescriptor};
use cobble_table::{DataField, LogicalType, Table, TableKey, TableSchema, Value};
use std::sync::mpsc;

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

    let db = DbBuilder::new(config.clone())
        .bucket_ranges(vec![0..=7])
        .db_id("table-runtime")
        .open()
        .unwrap();
    {
        let table = Table::create(&db, "events", schema.clone()).unwrap();
        assert_eq!(
            Table::create(&db, "events", schema.clone())
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
            &db,
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

    let reopened = cobble::Db::open_from_snapshot(config, snapshot_id, "table-runtime").unwrap();
    {
        let table = Table::open(&reopened, "events").unwrap();
        let key2 = build_key(
            &table,
            &[Value::String("tenant-a".to_string()), Value::Int64(2)],
        );
        assert_eq!(table.get(&key2).unwrap(), Some(row2));
        let keys = Table::open(&reopened, "keys").unwrap();
        let key = build_key(&keys, &[Value::Int64(42)]);
        assert_eq!(keys.get(&key).unwrap(), Some(vec![Value::Int64(42)]));
    }
    reopened.close().unwrap();
}

fn build_key(table: &Table<'_>, values: &[Value]) -> TableKey {
    let mut builder = table.key_builder();
    for value in values {
        builder.push(value.clone());
    }
    builder.build().unwrap()
}
