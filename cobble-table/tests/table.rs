use cobble::{Config, DbBuilder, VolumeDescriptor};
use cobble_table::{DataField, LogicalType, Table, TableSchema, Value};
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
        let bucket = table.bucket_for_key(&key1).unwrap();
        assert_eq!(bucket, table.encode_key(&key1).unwrap().0);
        let rows = table
            .scan_bounds(bucket, Some(&key1), Some(&missing))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows, vec![row1.clone(), row2.clone()]);

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
        keys.put(&key_only).unwrap();
        assert_eq!(keys.get(&key_only).unwrap(), Some(key_only));
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
        assert_eq!(table.get(&key2).unwrap(), Some(row2));
        assert_eq!(
            Table::open(&reopened, "keys")
                .unwrap()
                .get(&[Value::Int64(42)])
                .unwrap(),
            Some(vec![Value::Int64(42)])
        );
    }
    reopened.close().unwrap();
}
