use bytes::Bytes;
use cobble::{Config, CoordinatorConfig, DbBuilder, DbCoordinator, VolumeDescriptor};
use cobble_table::{
    DataField, ExtensionType, LogicalType, ReadOnlyTableBuilder, Table, TableKey, TableProjection,
    TableReadSchema, TableReaderBuilder, TableScanPlan, TableScanSplit, TableSchema, Value,
    ValueCodec, ValueProjection,
};
use std::sync::Arc;

fn field(id: u32, name: &str, logical_type: LogicalType) -> DataField {
    DataField::new(id, name, logical_type).unwrap()
}

#[test]
fn value_projection_preserves_framing_nulls_and_zero_copy_without_decoding_skipped_leaves() {
    let source = LogicalType::struct_type(vec![
        field(1, "skip", LogicalType::boolean()),
        field(2, "bytes", LogicalType::binary()),
    ])
    .nullable();
    let selection =
        LogicalType::struct_type(vec![field(2, "renamed", LogicalType::binary())]).nullable();
    let projection = ValueProjection::compile(&source, &selection).unwrap();
    let mut encoded = ValueCodec::encode(
        &source,
        &Value::Struct(vec![
            Value::Boolean(true),
            Value::Binary(Bytes::from_static(b"payload")),
        ]),
    )
    .unwrap();
    encoded[5] = 2; // Invalid Boolean, but the skipped frame is still well formed.
    assert!(ValueCodec::decode(&source, &encoded).is_err());
    let owned = Bytes::from(encoded.clone());
    let Value::Struct(values) = projection.decode_bytes(owned.clone()).unwrap() else {
        panic!()
    };
    let Value::Binary(binary) = &values[0] else {
        panic!()
    };
    assert_eq!(binary.as_ref(), b"payload");
    assert_eq!(binary.as_ptr(), owned[10..].as_ptr());
    let Value::Struct(copied) = projection.decode(&owned).unwrap() else {
        panic!()
    };
    let Value::Binary(copied) = &copied[0] else {
        panic!()
    };
    assert_ne!(copied.as_ptr(), binary.as_ptr());
    assert_eq!(projection.decode(&[0]).unwrap(), Value::Null);
    assert!(projection.decode(&[0, 0]).is_err());
    assert!(projection.decode(&[2]).is_err());
    assert!(projection.decode(&[]).is_err());
    for length in 0..encoded.len() {
        assert!(projection.decode(&encoded[..length]).is_err());
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(projection.decode(&trailing).is_err());
    encoded[1..5].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(projection.decode(&encoded).is_err());

    let empty = LogicalType::struct_type(vec![]).nullable();
    let empty_projection = ValueProjection::compile(&source, &empty).unwrap();
    let valid = ValueCodec::encode(
        &source,
        &Value::Struct(vec![Value::Boolean(false), Value::Binary(Bytes::new())]),
    )
    .unwrap();
    assert_eq!(
        empty_projection.decode(&valid).unwrap(),
        Value::Struct(vec![])
    );
    assert!(ValueProjection::compile(&source, &LogicalType::struct_type(vec![])).is_err());
    assert!(
        ValueProjection::compile(
            &source,
            &LogicalType::struct_type(vec![field(9, "missing", LogicalType::binary())]).nullable()
        )
        .is_err()
    );
    assert!(
        ValueProjection::compile(
            &source,
            &LogicalType::struct_type(vec![field(2, "wrong", LogicalType::string())]).nullable()
        )
        .is_err()
    );
    let duplicate = LogicalType::struct_type(vec![
        field(2, "a", LogicalType::binary()),
        field(2, "b", LogicalType::binary()),
    ])
    .nullable();
    assert!(ValueProjection::compile(&source, &duplicate).is_err());
    let extension = LogicalType::extension(
        ExtensionType::new("opaque", serde_json::json!({}), source.clone()).unwrap(),
    );
    let narrowed_extension = LogicalType::extension(
        ExtensionType::new("opaque", serde_json::json!({}), selection).unwrap(),
    );
    assert!(ValueProjection::compile(&extension, &narrowed_extension).is_err());
    assert!(
        ValueProjection::compile(
            &LogicalType::map(LogicalType::string(), source.clone()),
            &LogicalType::map(LogicalType::binary(), empty)
        )
        .is_err()
    );

    // Independently built struct scopes may reuse local identities.
    let child = LogicalType::struct_type(vec![field(1, "child", LogicalType::int32())]);
    let independent = LogicalType::struct_type(vec![
        field(1, "left", child.clone()),
        field(2, "right", child),
    ]);
    assert!(ValueProjection::compile(&independent, &independent).is_ok());
}

fn check_projection(
    projection: TableProjection,
    key: &TableKey,
    null_key: &TableKey,
    missing: &TableKey,
    expected: &[Value],
) {
    assert_eq!(projection.get(key).unwrap(), Some(expected.to_vec()));
    assert_eq!(projection.get(missing).unwrap(), None);
    assert_eq!(
        projection
            .multi_get(&[key.clone(), missing.clone(), key.clone(), null_key.clone()])
            .unwrap(),
        vec![
            Some(expected.to_vec()),
            None,
            Some(expected.to_vec()),
            Some(vec![Value::Int32(11), Value::Null, Value::Int64(2)]),
        ]
    );
    assert_eq!(
        projection
            .scan(0)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![
            expected.to_vec(),
            vec![Value::Int32(11), Value::Null, Value::Int64(2)]
        ]
    );
}

#[test]
fn table_nested_projection_reuses_selection_across_live_readonly_global_and_transported_scans() {
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root.path().display())),
        total_buckets: 1,
        ..Config::default()
    };
    let source = LogicalType::struct_type(vec![
        field(3, "blob", LogicalType::binary()),
        field(4, "skip", LogicalType::boolean()),
        field(
            5,
            "children",
            LogicalType::list(
                LogicalType::struct_type(vec![
                    field(6, "name", LogicalType::string()),
                    field(7, "count", LogicalType::int32()),
                ])
                .nullable(),
            ),
        ),
        field(
            8,
            "attrs",
            LogicalType::map(
                LogicalType::string(),
                LogicalType::struct_type(vec![
                    field(9, "data", LogicalType::binary()),
                    field(10, "note", LogicalType::string()),
                ])
                .nullable(),
            ),
        ),
    ])
    .nullable();
    let selection = LogicalType::struct_type(vec![
        field(
            8,
            "attrs",
            LogicalType::map(
                LogicalType::string(),
                LogicalType::struct_type(vec![field(9, "data", LogicalType::binary())]).nullable(),
            ),
        ),
        field(
            5,
            "children",
            LogicalType::list(
                LogicalType::struct_type(vec![field(7, "count", LogicalType::int32())]).nullable(),
            ),
        ),
        field(3, "blob", LogicalType::binary()),
    ])
    .nullable();
    let schema = TableSchema::new(
        vec![
            field(1, "id", LogicalType::int64()),
            field(2, "profile", source),
            field(11, "other", LogicalType::int32()),
        ],
        vec![1.into()],
        vec![1.into()],
    )
    .unwrap();
    let read_schema = TableReadSchema::new(vec![
        field(11, "other", LogicalType::int32()),
        field(2, "profile", selection),
        field(1, "renamed_id", LogicalType::int64()),
    ])
    .unwrap();
    let value = Value::Struct(vec![
        Value::Binary(Bytes::from_static(b"blob")),
        Value::Boolean(true),
        Value::List(vec![
            Value::Struct(vec![Value::String("one".to_string()), Value::Int32(7)]),
            Value::Null,
        ]),
        Value::Map(vec![
            (
                Value::String("key".to_string()),
                Value::Struct(vec![
                    Value::Binary(Bytes::from_static(b"data")),
                    Value::String("omit".to_string()),
                ]),
            ),
            (Value::String("null".to_string()), Value::Null),
        ]),
    ]);
    let expected = vec![
        Value::Int32(99),
        Value::Struct(vec![
            Value::Map(vec![
                (
                    Value::String("key".to_string()),
                    Value::Struct(vec![Value::Binary(Bytes::from_static(b"data"))]),
                ),
                (Value::String("null".to_string()), Value::Null),
            ]),
            Value::List(vec![Value::Struct(vec![Value::Int32(7)]), Value::Null]),
            Value::Binary(Bytes::from_static(b"blob")),
        ]),
        Value::Int64(1),
    ];
    let db = Arc::new(
        DbBuilder::new(config.clone())
            .db_id("nested")
            .bucket_ranges(vec![0..=0])
            .open()
            .unwrap(),
    );
    let table = Table::create(Arc::clone(&db), "rows", schema).unwrap();
    table
        .put(&[Value::Int64(1), value, Value::Int32(99)])
        .unwrap();
    table
        .put(&[Value::Int64(2), Value::Null, Value::Int32(11)])
        .unwrap();
    table
        .put(&[Value::Int64(3), Value::Null, Value::Int32(12)])
        .unwrap();
    let key_for = |id| {
        let mut builder = table.key_builder();
        builder.push(Value::Int64(id));
        builder.build().unwrap()
    };
    let key = key_for(1);
    let null_key = key_for(2);
    let missing = key_for(3);
    table.delete(&missing).unwrap();
    let projected = table.project(&read_schema).unwrap();
    assert_eq!(projected.read_schema(), &read_schema);
    check_projection(projected, &key, &null_key, &missing, &expected);
    let empty = TableReadSchema::new(vec![]).unwrap();
    let count = table.project(&empty).unwrap();
    assert_eq!(count.get(&key).unwrap(), Some(vec![]));
    assert_eq!(count.get(&missing).unwrap(), None);
    assert_eq!(
        count
            .scan(0)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![vec![], vec![]]
    );

    let snapshot = table.snapshot_and_wait().unwrap();
    let readonly = ReadOnlyTableBuilder::new(config.clone())
        .table_name("rows")
        .shard_snapshot(&snapshot.db_id, snapshot.snapshot_id)
        .open()
        .unwrap();
    check_projection(
        readonly.project(&read_schema).unwrap(),
        &key,
        &null_key,
        &missing,
        &expected,
    );
    assert_eq!(
        readonly.project(&empty).unwrap().get(&key).unwrap(),
        Some(vec![])
    );
    let coordinator = DbCoordinator::open(CoordinatorConfig::from_config(&config)).unwrap();
    let global = coordinator.take_global_snapshot(1, vec![snapshot]).unwrap();
    coordinator.materialize_global_snapshot(&global).unwrap();
    let reader = TableReaderBuilder::new(config.clone())
        .table_name("rows")
        .current_global_snapshot()
        .open()
        .unwrap();
    check_projection(
        reader.project(&read_schema).unwrap(),
        &key,
        &null_key,
        &missing,
        &expected,
    );
    let projected_plan = reader.scan_plan().unwrap().project(&read_schema).unwrap();
    assert_eq!(projected_plan.read_schema(), read_schema);
    let transported: TableScanPlan =
        serde_json::from_slice(&serde_json::to_vec(&projected_plan).unwrap()).unwrap();
    for plan in [&projected_plan, &transported] {
        let splits = plan.splits().unwrap();
        let transported_split: TableScanSplit =
            serde_json::from_slice(&serde_json::to_vec(&splits[0]).unwrap()).unwrap();
        for split in [&splits[0], &transported_split] {
            assert_eq!(split.read_schema(), read_schema);
            let scanner = split.create_scanner(config.clone()).unwrap();
            assert_eq!(scanner.read_schema(), &read_schema);
            assert_eq!(
                scanner.collect::<Result<Vec<_>, _>>().unwrap(),
                vec![
                    expected.clone(),
                    vec![Value::Int32(11), Value::Null, Value::Int64(2)]
                ]
            );
            assert_eq!(
                split
                    .scanner_builder(config.clone())
                    .project(&empty)
                    .unwrap()
                    .open()
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                vec![vec![], vec![]]
            );
        }
    }
    // Deserialization transports schema choices, never trusted compiled indexes.
    let mut invalid = serde_json::to_value(&projected_plan).unwrap();
    invalid["read_schema"]["fields"][0]["id"] = serde_json::json!(999);
    let invalid: TableScanPlan = serde_json::from_value(invalid).unwrap();
    assert!(invalid.splits().is_err());
    let mut invalid = serde_json::to_value(&projected_plan.splits().unwrap()[0]).unwrap();
    invalid["read_schema"]["fields"][0]["id"] = serde_json::json!(999);
    let invalid: TableScanSplit = serde_json::from_value(invalid).unwrap();
    assert!(invalid.create_scanner(config).is_err());
}
