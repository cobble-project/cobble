use crate::evolution::{apply_schema_changes, compile_column_evolution, schema_field_ids};
use crate::metadata::TableMetadata;
use crate::{
    DataField, FieldId, LogicalType, SchemaChange, TableError, TableSchema, Value, ValueCodec,
};
use cobble::ColumnEvolution;

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
    let (schema, history) = apply_schema_changes(
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
        compile_column_evolution(&source, &target).unwrap(),
        vec![
            keep.clone(),
            default(&target.schema.fields[2].logical_type),
            default(&LogicalType::int32().nullable()),
        ]
    );
    assert_eq!(source.schema.fields[1].name, "keep");
    assert_eq!(target.schema.fields[0].name, "current");
    assert_eq!(source.layout.key_fields, target.layout.key_fields);

    let (schema, history) = apply_schema_changes(
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
    let encoded = serde_json::to_vec(&(schema, history)).unwrap();
    let (schema, history) = serde_json::from_slice(&encoded).unwrap();
    let (schema, history) = apply_schema_changes(
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
    assert_eq!(schema.fields[2].id, FieldId(74));
    assert_eq!(schema.primary_key, vec![FieldId(10)]);
    let final_metadata = TableMetadata::compile(schema.clone()).unwrap();
    assert_eq!(
        compile_column_evolution(&source, &final_metadata).unwrap(),
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
    ] {
        assert!(matches!(
            apply_schema_changes(schema.clone(), vec![change], history.clone()),
            Err(TableError::InvalidSchema(_))
        ));
    }

    // A field ID alone does not authorize interpreting old bytes as a new type.
    let mut incompatible = schema;
    incompatible.fields[0].logical_type = LogicalType::int64().nullable();
    assert!(
        compile_column_evolution(&source, &TableMetadata::compile(incompatible).unwrap()).is_err()
    );
}
