use crate::metadata::TableMetadata;
use crate::{
    DataField, ExtensionType, FieldId, LogicalType, TableError, TableSchema, TimestampKind,
};

fn fixture_schema() -> TableSchema {
    TableSchema::new(
        vec![
            DataField::new(1, "tenant", LogicalType::string()).unwrap(),
            DataField::new(2, "id", LogicalType::int64()).unwrap(),
            DataField::new(3, "name", LogicalType::string().nullable()).unwrap(),
            DataField::new(
                4,
                "attributes",
                LogicalType::map(LogicalType::string(), LogicalType::binary().nullable()),
            )
            .unwrap(),
            DataField::new(
                5,
                "created_at",
                LogicalType::timestamp(6, TimestampKind::WithoutTimeZone),
            )
            .unwrap(),
        ],
        vec![FieldId(1), FieldId(2)],
        vec![FieldId(1)],
    )
    .unwrap()
}

#[test]
fn table_metadata_contract_and_validation() {
    let metadata = TableMetadata::compile(fixture_schema()).unwrap();
    assert_eq!(metadata.layout.key_fields, vec![FieldId(1), FieldId(2)]);
    assert_eq!(metadata.layout.bucket_fields, vec![FieldId(1)]);
    assert_eq!(metadata.layout.value_columns.len(), 3);

    let encoded = metadata.to_json().unwrap();
    assert!(
        !encoded
            .windows(b"catalog_binding".len())
            .any(|window| window == b"catalog_binding")
    );
    assert_eq!(TableMetadata::from_json(&encoded).unwrap(), metadata);

    let fixture = include_bytes!("../../../spec/table/fixtures/table_metadata_v1.json");
    assert_eq!(TableMetadata::from_json(fixture).unwrap(), metadata);

    let nullable_key = TableSchema::new(
        vec![DataField::new(1, "id", LogicalType::int64().nullable()).unwrap()],
        vec![FieldId(1)],
        vec![FieldId(1)],
    );
    assert!(matches!(nullable_key, Err(TableError::InvalidSchema(_))));

    let non_prefix_bucket = TableSchema::new(
        vec![
            DataField::new(1, "tenant", LogicalType::string()).unwrap(),
            DataField::new(2, "id", LogicalType::int64()).unwrap(),
        ],
        vec![FieldId(1), FieldId(2)],
        vec![FieldId(2)],
    );
    assert!(matches!(
        non_prefix_bucket,
        Err(TableError::InvalidSchema(_))
    ));
}

#[test]
fn schema_builder_assigns_ids_and_preserves_layout_contract() {
    let nested = LogicalType::struct_from_fields([
        (
            "items",
            LogicalType::list(
                LogicalType::struct_from_fields([("code", LogicalType::string())]).unwrap(),
            ),
        ),
        (
            "attributes",
            LogicalType::map(
                LogicalType::string(),
                LogicalType::extension(
                    ExtensionType::new(
                        "score",
                        serde_json::json!({}),
                        LogicalType::struct_from_fields([("score", LogicalType::int64())]).unwrap(),
                    )
                    .unwrap(),
                ),
            ),
        ),
    ])
    .unwrap();
    let schema = TableSchema::builder()
        .field("payload", nested)
        .field("id", LogicalType::int64())
        .field("tenant", LogicalType::string())
        .field("name", LogicalType::string().nullable())
        .primary_key(["tenant", "id"])
        .bucket_key(["tenant"])
        .build()
        .unwrap();
    assert_eq!(
        schema
            .fields
            .iter()
            .map(|field| field.id.0)
            .collect::<Vec<_>>(),
        vec![0, 5, 6, 7]
    );
    assert_eq!(schema.primary_key, vec![FieldId(6), FieldId(5)]);
    // Inspect nested identities as well as the top-level fields. The enclosing
    // schema must rebase the independently constructed nested struct types.
    let json = serde_json::to_value(&schema).unwrap();
    for (path, expected) in [
        ("/fields/0/logical_type/fields/0/id", 1),
        (
            "/fields/0/logical_type/fields/0/logical_type/element_type/fields/0/id",
            2,
        ),
        ("/fields/0/logical_type/fields/1/id", 3),
        (
            "/fields/0/logical_type/fields/1/logical_type/value_type/extension/physical_type/fields/0/id",
            4,
        ),
    ] {
        assert_eq!(
            json.pointer(path).and_then(serde_json::Value::as_u64),
            Some(expected)
        );
    }

    let metadata = TableMetadata::compile(schema).unwrap();
    assert_eq!(metadata.layout.key_fields, vec![FieldId(6), FieldId(5)]);
    assert_eq!(metadata.layout.value_columns[0].field_id, FieldId(0));
    assert_eq!(metadata.layout.value_columns[0].column_index, 0);
    assert_eq!(metadata.layout.value_columns[1].field_id, FieldId(7));
    assert_eq!(metadata.layout.value_columns[1].column_index, 1);
    assert_eq!(
        TableMetadata::from_json(&metadata.to_json().unwrap()).unwrap(),
        metadata
    );

    // Explicit construction/loading keeps sparse IDs, unrelated to row positions.
    let explicit = TableMetadata::compile(
        TableSchema::new(
            vec![
                DataField::new(91, "value", LogicalType::string()).unwrap(),
                DataField::new(42, "id", LogicalType::int64()).unwrap(),
            ],
            vec![FieldId(42)],
            vec![FieldId(42)],
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(explicit.layout.value_columns[0].field_id, FieldId(91));
    assert_eq!(explicit.layout.value_columns[0].column_index, 0);
    assert_eq!(
        TableMetadata::from_json(&explicit.to_json().unwrap()).unwrap(),
        explicit
    );

    for (fields, primary, bucket) in [
        (vec!["id", "id"], vec!["id"], vec!["id"]),
        (vec!["id"], vec!["missing"], vec!["missing"]),
        (vec!["id"], vec!["id"], vec!["missing"]),
        (vec!["id"], vec!["id", "id"], vec!["id"]),
        (vec!["tenant", "id"], vec!["tenant", "id"], vec!["id"]),
        (vec!["id"], vec!["id"], vec![]),
        (vec![""], vec![""], vec![""]),
    ] {
        let mut builder = TableSchema::builder();
        for name in fields {
            builder = builder.field(name, LogicalType::int64());
        }
        assert!(matches!(
            builder.primary_key(primary).bucket_key(bucket).build(),
            Err(TableError::InvalidSchema(_))
        ));
    }
}
