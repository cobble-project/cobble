use cobble::TransformSpec;
use cobble_table::catalog::{CatalogSchemaId, CatalogSchemaVersion, SchemaChange, TableId};
use cobble_table::{LogicalType, TableSchema};

#[test]
fn catalog_schema_history_is_backend_independent_and_preserves_field_identity() {
    let schema = TableSchema::builder()
        .field("id", LogicalType::int64())
        .field("value", LogicalType::int32().nullable())
        .primary_key(["id"])
        .bucket_key(["id"])
        .build()
        .unwrap();
    let table_id = TableId::new(42);
    let initial = CatalogSchemaVersion::initial(table_id, schema.clone()).unwrap();
    assert_eq!(initial.table_id(), table_id);
    assert_eq!(initial.catalog_schema_id(), CatalogSchemaId::from(0));
    assert_eq!(initial.schema(), &schema);
    assert!(initial.field_transforms().is_empty());

    let widened = initial
        .evolve(vec![SchemaChange::AlterFieldType {
            field_name: "value".into(),
            logical_type: LogicalType::int64().nullable(),
        }])
        .unwrap();
    let value_id = schema.fields[1].id;
    assert_eq!(widened.catalog_schema_id(), CatalogSchemaId::from(1));
    assert_eq!(widened.field_transforms().len(), 1);
    assert_eq!(widened.field_transforms()[0].field_id(), value_id);
    let json = serde_json::to_value(&widened).unwrap();
    assert!(json.get("format").is_none());
    assert!(json.get("version").is_none());
    assert_eq!(json["table_id"], 42);
    assert_eq!(json["catalog_schema_id"], 1);
    let restored: CatalogSchemaVersion = serde_json::from_value(json).unwrap();
    assert_eq!(restored, widened);

    let custom_spec = TransformSpec {
        transform_type: "example.scale".into(),
        spec: vec![0, 255, 7].into(),
    };
    let transformed = restored
        .evolve(vec![SchemaChange::TransformField {
            field_name: "value".into(),
            logical_type: LogicalType::int64().nullable(),
            transform: custom_spec.clone(),
        }])
        .unwrap();
    assert_eq!(transformed.field_transforms()[0].field_id(), value_id);
    assert_eq!(transformed.field_transforms()[0].transform(), &custom_spec);
    let restored: CatalogSchemaVersion =
        serde_json::from_slice(&serde_json::to_vec(&transformed).unwrap()).unwrap();
    assert_eq!(restored, transformed);

    let dropped = restored
        .evolve(vec![SchemaChange::DropField {
            field_name: "value".into(),
        }])
        .unwrap();
    assert!(dropped.field_transforms().is_empty());
    let readded = dropped
        .evolve(vec![SchemaChange::AddField {
            name: "value".into(),
            logical_type: LogicalType::int32().nullable(),
        }])
        .unwrap();
    assert_eq!(readded.catalog_schema_id(), CatalogSchemaId::from(4));
    assert_ne!(readded.schema().fields[1].id, value_id);
    assert!(readded.used_field_ids().contains(&value_id));
    assert!(
        readded
            .used_field_ids()
            .contains(&readded.schema().fields[1].id)
    );
    // Versions are immutable values; computing successors never changes the source.
    assert_eq!(initial.schema(), &schema);
    assert!(
        initial
            .evolve(vec![SchemaChange::DropField {
                field_name: "id".into()
            }])
            .is_err()
    );
}
