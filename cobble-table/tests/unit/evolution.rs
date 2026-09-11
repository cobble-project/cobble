use crate::evolution::{apply_schema_changes, compile_column_evolution, schema_field_ids};
use crate::metadata::TableMetadata;
use crate::transform::{TABLE_TRANSFORM_TYPE, table_transform_factory};
use crate::{
    DataField, FieldId, LogicalType, LogicalTypeKind, SchemaChange, TableError, TableSchema,
    TimestampKind, Value, ValueCodec,
};
use bytes::Bytes;
use cobble::{ColumnEvolution, TransformSpec};

#[test]
fn named_changes_preserve_history_and_compile_stable_column_mappings() {
    let source = TableMetadata::compile(
        TableSchema::new(
            vec![
                DataField::new(20, "obsolete", LogicalType::int32().nullable()).unwrap(),
                DataField::new(30, "keep", LogicalType::string().nullable()).unwrap(),
                DataField::new(10, "id", LogicalType::int64()).unwrap(),
                DataField::new(
                    40,
                    "nested",
                    LogicalType::struct_type(vec![
                        DataField::new(70, "child", LogicalType::string()).unwrap(),
                    ])
                    .nullable(),
                )
                .unwrap(),
            ],
            vec![FieldId(10)],
            vec![FieldId(10)],
        )
        .unwrap(),
    )
    .unwrap();
    let history = schema_field_ids(&source.schema);
    let (schema, history, transforms) = apply_schema_changes(
        source.schema.clone(),
        vec![
            SchemaChange::RenameField {
                field_name: "keep".into(),
                new_name: "current".into(),
            },
            SchemaChange::DropField {
                field_name: "obsolete".into(),
            },
            SchemaChange::DropField {
                field_name: "nested".into(),
            },
            SchemaChange::AddField {
                name: "nested".into(),
                logical_type: LogicalType::list(
                    LogicalType::struct_from_fields([("code", LogicalType::string())]).unwrap(),
                )
                .nullable(),
            },
            SchemaChange::AddField {
                name: "after".into(),
                logical_type: LogicalType::int32().nullable(),
            },
        ],
        history,
    )
    .unwrap();
    assert_eq!(
        schema
            .fields
            .iter()
            .map(|field| field.id.0)
            .collect::<Vec<_>>(),
        vec![30, 10, 71, 73]
    );
    assert_eq!(
        history,
        [10, 20, 30, 40, 70, 71, 72, 73]
            .map(FieldId)
            .into_iter()
            .collect()
    );
    assert!(transforms.is_empty());
    let target = TableMetadata::compile(schema).unwrap();
    let keep = ColumnEvolution::Source {
        source_index: 1,
        transform: None,
    };
    let default = |logical_type: &LogicalType| ColumnEvolution::Default {
        value: ValueCodec::encode(logical_type, &Value::Null)
            .unwrap()
            .into(),
    };
    assert_eq!(
        compile_column_evolution(&source, &target, &[]).unwrap(),
        vec![
            keep.clone(),
            default(&target.schema.fields[2].logical_type),
            default(&LogicalType::int32().nullable()),
        ]
    );
    assert_eq!(source.schema.fields[1].name, "keep");
    assert_eq!(target.schema.fields[0].name, "current");
    assert_eq!(source.layout.key_fields, target.layout.key_fields);

    let (schema, history, transforms) = apply_schema_changes(
        target.schema,
        vec![
            SchemaChange::DropField {
                field_name: "nested".into(),
            },
            SchemaChange::DropField {
                field_name: "after".into(),
            },
        ],
        history,
    )
    .unwrap();
    // A catalog persists these two pieces separately; restore both before the
    // next edit so deleted nested and top-level IDs stay reserved.
    assert!(transforms.is_empty());
    let encoded = serde_json::to_vec(&(schema, history)).unwrap();
    let (schema, history) = serde_json::from_slice(&encoded).unwrap();
    let (schema, history, transforms) = apply_schema_changes(
        schema,
        vec![
            SchemaChange::AddField {
                name: "after".into(),
                logical_type: LogicalType::int32().nullable(),
            },
            SchemaChange::RenameField {
                field_name: "id".into(),
                new_name: "account_id".into(),
            },
        ],
        history,
    )
    .unwrap();
    assert!(transforms.is_empty());
    assert_eq!(schema.fields[2].id, FieldId(74));
    assert_eq!(schema.primary_key, vec![FieldId(10)]);
    let final_metadata = TableMetadata::compile(schema.clone()).unwrap();
    assert_eq!(
        compile_column_evolution(&source, &final_metadata, &[]).unwrap(),
        vec![keep, default(&LogicalType::int32().nullable()),]
    );

    for change in [
        SchemaChange::AddField {
            name: "required".into(),
            logical_type: LogicalType::string(),
        },
        SchemaChange::AddField {
            name: "current".into(),
            logical_type: LogicalType::string().nullable(),
        },
        SchemaChange::RenameField {
            field_name: "current".into(),
            new_name: "account_id".into(),
        },
        SchemaChange::RenameField {
            field_name: "missing".into(),
            new_name: "new".into(),
        },
        SchemaChange::DropField {
            field_name: "account_id".into(),
        },
        SchemaChange::DropField {
            field_name: "Current".into(),
        },
        SchemaChange::TransformField {
            field_name: "account_id".into(),
            logical_type: LogicalType::int64(),
            transform: TransformSpec {
                transform_type: "test.transform".into(),
                spec: Vec::new().into(),
            },
        },
    ] {
        assert!(matches!(
            apply_schema_changes(schema.clone(), vec![change], history.clone()),
            Err(TableError::InvalidSchema(_))
        ));
    }

    // A field ID alone does not authorize interpreting old bytes as a new type.
    let mut incompatible = schema.clone();
    incompatible.fields[0].logical_type = LogicalType::int64().nullable();
    assert!(
        compile_column_evolution(&source, &TableMetadata::compile(incompatible).unwrap(), &[])
            .is_err()
    );

    let (transformed, _, transforms) = apply_schema_changes(
        schema,
        vec![
            SchemaChange::RenameField {
                field_name: "current".into(),
                new_name: "renamed".into(),
            },
            SchemaChange::TransformField {
                field_name: "renamed".into(),
                logical_type: LogicalType::struct_from_fields([(
                    "new_child",
                    LogicalType::binary().nullable(),
                )])
                .unwrap()
                .nullable(),
                transform: TransformSpec {
                    transform_type: "test.transform".into(),
                    spec: Vec::new().into(),
                },
            },
        ],
        history,
    )
    .unwrap();
    assert_eq!(transformed.fields[0].id, FieldId(30));
    let LogicalTypeKind::Struct { fields } = &transformed.fields[0].logical_type.kind else {
        panic!("transform changed the field to a struct");
    };
    assert_eq!(fields[0].id, FieldId(75));
    assert_eq!(transforms.len(), 1);
    assert_eq!(transforms[0].field_id, FieldId(30));
}

#[test]
fn builtin_type_changes_are_lossless_and_validate_persisted_specs() {
    let apply = |source: LogicalType, target: LogicalType| {
        let schema = TableSchema::builder()
            .field("id", LogicalType::int64())
            .field("value", source)
            .primary_key(["id"])
            .bucket_key(["id"])
            .build()
            .unwrap();
        let history = schema_field_ids(&schema);
        apply_schema_changes(
            schema,
            vec![SchemaChange::AlterFieldType {
                field_name: "value".into(),
                logical_type: target,
            }],
            history,
        )
        .unwrap()
    };
    let convert = |source: LogicalType, target: LogicalType, value: Value| {
        let (schema, _, transforms) = apply(source.clone(), target.clone());
        assert_eq!(schema.fields[1].id, FieldId(1));
        let transform = transforms.into_iter().next().unwrap();
        assert_eq!(transform.transform.transform_type, TABLE_TRANSFORM_TYPE);
        let callback = table_transform_factory(&transform.transform.spec).unwrap();
        let output = callback(Some(Bytes::from(
            ValueCodec::encode(&source, &value).unwrap(),
        )))
        .unwrap()
        .unwrap();
        ValueCodec::decode(&target, &output).unwrap()
    };

    assert_eq!(
        convert(
            LogicalType::int8().nullable(),
            LogicalType::int64().nullable(),
            Value::Int8(i8::MIN),
        ),
        Value::Int64(i64::from(i8::MIN))
    );
    assert_eq!(
        convert(
            LogicalType::int16(),
            LogicalType::int32().nullable(),
            Value::Int16(i16::MAX),
        ),
        Value::Int32(i32::from(i16::MAX))
    );
    assert_eq!(
        convert(
            LogicalType::int32(),
            LogicalType::int64(),
            Value::Int32(i32::MIN),
        ),
        Value::Int64(i64::from(i32::MIN))
    );
    assert_eq!(
        convert(
            LogicalType::int64(),
            LogicalType::int64().nullable(),
            Value::Int64(7),
        ),
        Value::Int64(7)
    );
    let negative_zero = convert(
        LogicalType::float32(),
        LogicalType::float64(),
        Value::Float32(-0.0),
    );
    assert!(
        matches!(negative_zero, Value::Float64(value) if value == 0.0 && value.is_sign_negative())
    );
    let infinity = convert(
        LogicalType::float32(),
        LogicalType::float64(),
        Value::Float32(f32::INFINITY),
    );
    assert_eq!(infinity, Value::Float64(f64::INFINITY));
    let nan = convert(
        LogicalType::float32(),
        LogicalType::float64(),
        Value::Float32(f32::NAN),
    );
    assert!(matches!(nan, Value::Float64(value) if value.is_nan()));
    assert_eq!(
        convert(
            LogicalType::decimal(3, 1),
            LogicalType::decimal(5, 1),
            Value::Decimal {
                precision: 3,
                scale: 1,
                unscaled: 999,
            },
        ),
        Value::Decimal {
            precision: 5,
            scale: 1,
            unscaled: 999,
        }
    );
    assert_eq!(
        convert(
            LogicalType::decimal(9, 0),
            LogicalType::decimal(10, 0),
            Value::Decimal {
                precision: 9,
                scale: 0,
                unscaled: 999_999_999,
            },
        ),
        Value::Decimal {
            precision: 10,
            scale: 0,
            unscaled: 999_999_999,
        }
    );
    assert_eq!(
        convert(
            LogicalType::decimal(18, 0),
            LogicalType::decimal(19, 0),
            Value::Decimal {
                precision: 18,
                scale: 0,
                unscaled: 999_999_999_999_999_999,
            },
        ),
        Value::Decimal {
            precision: 19,
            scale: 0,
            unscaled: 999_999_999_999_999_999,
        }
    );
    assert_eq!(
        convert(
            LogicalType::time(3),
            LogicalType::time(9),
            Value::Time(123_000_000),
        ),
        Value::Time(123_000_000)
    );
    assert_eq!(
        convert(
            LogicalType::timestamp(3, TimestampKind::WithLocalTimeZone),
            LogicalType::timestamp(9, TimestampKind::WithLocalTimeZone),
            Value::Timestamp {
                precision: 3,
                timestamp_kind: TimestampKind::WithLocalTimeZone,
                seconds: -1,
                nanos: 123_000_000,
            },
        ),
        Value::Timestamp {
            precision: 9,
            timestamp_kind: TimestampKind::WithLocalTimeZone,
            seconds: -1,
            nanos: 123_000_000,
        }
    );

    let source = LogicalType::int8().nullable();
    let target = LogicalType::int64().nullable();
    let (_, _, transforms) = apply(source.clone(), target.clone());
    let callback = table_transform_factory(&transforms[0].transform.spec).unwrap();
    assert_eq!(callback(None).unwrap(), None);
    let encoded_null = ValueCodec::encode(&source, &Value::Null).unwrap();
    let decoded_null = callback(Some(Bytes::from(encoded_null))).unwrap().unwrap();
    assert_eq!(
        ValueCodec::decode(&target, &decoded_null).unwrap(),
        Value::Null
    );
    let mut corrupt = ValueCodec::encode(&source, &Value::Int8(1)).unwrap();
    corrupt.pop();
    assert!(callback(Some(Bytes::from(corrupt))).is_err());

    let mut malformed =
        serde_json::from_slice::<serde_json::Value>(&transforms[0].transform.spec).unwrap();
    malformed["operation"] = serde_json::Value::String("float".into());
    assert!(table_transform_factory(&serde_json::to_vec(&malformed).unwrap()).is_err());
    assert!(table_transform_factory(b"{}").is_err());

    let unchanged = LogicalType::int64().nullable();
    let (_, _, unchanged_transforms) = apply(unchanged.clone(), unchanged);
    assert!(unchanged_transforms.is_empty());

    for (source, target) in [
        (LogicalType::int64(), LogicalType::int32()),
        (LogicalType::int64().nullable(), LogicalType::int64()),
        (LogicalType::int32(), LogicalType::float64()),
        (LogicalType::string(), LogicalType::binary()),
        (LogicalType::decimal(4, 2), LogicalType::decimal(5, 3)),
        (
            LogicalType::timestamp(3, TimestampKind::WithoutTimeZone),
            LogicalType::timestamp(9, TimestampKind::WithLocalTimeZone),
        ),
        (
            LogicalType::list(LogicalType::int8()),
            LogicalType::list(LogicalType::int16()),
        ),
    ] {
        let schema = TableSchema::builder()
            .field("id", LogicalType::int64())
            .field("value", source)
            .primary_key(["id"])
            .bucket_key(["id"])
            .build()
            .unwrap();
        assert!(
            apply_schema_changes(
                schema,
                vec![SchemaChange::AlterFieldType {
                    field_name: "value".into(),
                    logical_type: target,
                }],
                [FieldId(0), FieldId(1)].into_iter().collect(),
            )
            .is_err()
        );
    }
    let key_schema = TableSchema::builder()
        .field("id", LogicalType::int8())
        .primary_key(["id"])
        .bucket_key(["id"])
        .build()
        .unwrap();
    assert!(
        apply_schema_changes(
            key_schema.clone(),
            vec![SchemaChange::AlterFieldType {
                field_name: "id".into(),
                logical_type: LogicalType::int16(),
            }],
            schema_field_ids(&key_schema),
        )
        .is_err()
    );
    let duplicate_schema = TableSchema::builder()
        .field("id", LogicalType::int64())
        .field("value", LogicalType::int8())
        .primary_key(["id"])
        .bucket_key(["id"])
        .build()
        .unwrap();
    assert!(
        apply_schema_changes(
            duplicate_schema.clone(),
            vec![
                SchemaChange::AlterFieldType {
                    field_name: "value".into(),
                    logical_type: LogicalType::int16(),
                },
                SchemaChange::AlterFieldType {
                    field_name: "value".into(),
                    logical_type: LogicalType::int32(),
                },
            ],
            schema_field_ids(&duplicate_schema),
        )
        .is_err()
    );
}
