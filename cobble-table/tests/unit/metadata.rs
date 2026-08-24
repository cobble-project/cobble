use crate::metadata::TableMetadata;
use crate::{DataField, FieldId, LogicalType, TableError, TableSchema, TimestampKind};

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
