use crate::logical_type::assign_fresh_field_ids;
use crate::metadata::TableMetadata;
use crate::{
    DataField, FieldId, LogicalType, LogicalTypeKind, Result, TableError, TableSchema, Value,
    ValueCodec,
};
use cobble::ColumnEvolution;
use std::collections::{HashMap, HashSet};

/// A top-level table schema edit addressed by its current field name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaChange {
    /// Append a nullable top-level value field.
    AddField {
        name: String,
        logical_type: LogicalType,
    },
    /// Rename a top-level field while retaining its stable field id.
    RenameField {
        field_name: String,
        new_name: String,
    },
    /// Drop a non-key top-level field.
    DropField { field_name: String },
}

/// Apply sequential schema edits while retaining every historical field id.
///
/// `used_field_ids` must include both active and retired ids from every prior
/// version of the table schema.
pub(crate) fn apply_schema_changes(
    mut schema: TableSchema,
    changes: Vec<SchemaChange>,
    mut used_field_ids: HashSet<FieldId>,
) -> Result<(TableSchema, HashSet<FieldId>)> {
    if changes.is_empty() {
        return Err(TableError::InvalidSchema(
            "schema changes must not be empty".to_string(),
        ));
    }
    let key_ids = schema
        .primary_key
        .iter()
        .chain(&schema.bucket_key)
        .copied()
        .collect::<HashSet<_>>();

    for change in changes {
        match change {
            SchemaChange::AddField { name, logical_type } => {
                if schema.fields.iter().any(|field| field.name == name) {
                    return Err(TableError::InvalidSchema(format!(
                        "duplicate top-level field name: {name}"
                    )));
                }
                if !logical_type.nullable {
                    return Err(TableError::InvalidSchema(format!(
                        "added field '{name}' must be nullable"
                    )));
                }
                let mut added = DataField {
                    id: FieldId(0),
                    name,
                    logical_type,
                };
                let mut next_id = next_field_id(&used_field_ids)?;
                assign_fresh_field_ids(std::slice::from_mut(&mut added), &mut next_id)?;
                collect_field_ids(&added, &mut used_field_ids);
                schema.fields.push(added);
            }
            SchemaChange::RenameField {
                field_name,
                new_name,
            } => {
                let index = field_index(&schema, &field_name)?;
                if new_name != field_name
                    && schema.fields.iter().any(|field| field.name == new_name)
                {
                    return Err(TableError::InvalidSchema(format!(
                        "duplicate top-level field name: {new_name}"
                    )));
                }
                schema.fields[index].name = new_name;
            }
            SchemaChange::DropField { field_name } => {
                let index = field_index(&schema, &field_name)?;
                if key_ids.contains(&schema.fields[index].id) {
                    return Err(TableError::InvalidSchema(format!(
                        "key field '{}' cannot be dropped",
                        field_name
                    )));
                }
                schema.fields.remove(index);
            }
        }
    }
    let schema = TableSchema::new(schema.fields, schema.primary_key, schema.bucket_key)?;
    Ok((schema, used_field_ids))
}

/// Compile core column remapping for two validated table metadata versions.
pub(crate) fn compile_column_evolution(
    existing: &TableMetadata,
    target: &TableMetadata,
) -> Result<Vec<ColumnEvolution>> {
    if existing.layout.key_fields != target.layout.key_fields
        || existing.layout.bucket_fields != target.layout.bucket_fields
    {
        return Err(TableError::InvalidSchema(
            "schema evolution changed the table key".to_string(),
        ));
    }
    let existing_fields = existing
        .schema
        .fields
        .iter()
        .map(|field| (field.id, field))
        .collect::<HashMap<_, _>>();
    for field in &target.schema.fields {
        if let Some(previous) = existing_fields.get(&field.id)
            && previous.logical_type != field.logical_type
        {
            return Err(TableError::InvalidSchema(format!(
                "schema evolution changed the type of field {}",
                field.id.0
            )));
        }
    }

    if target.layout.value_columns.is_empty() {
        return Ok(vec![ColumnEvolution::Default {
            value: Vec::new().into(),
        }]);
    }
    let existing_columns = existing
        .layout
        .value_columns
        .iter()
        .map(|column| (column.field_id, usize::from(column.column_index)))
        .collect::<HashMap<_, _>>();
    let target_fields = target
        .schema
        .fields
        .iter()
        .map(|field| (field.id, &field.logical_type))
        .collect::<HashMap<_, _>>();
    target
        .layout
        .value_columns
        .iter()
        .map(|column| {
            if let Some(source) = existing_columns.get(&column.field_id) {
                return Ok(ColumnEvolution::Source {
                    source_index: *source,
                    transform: None,
                });
            }
            Ok(ColumnEvolution::Default {
                value: ValueCodec::encode_validated(target_fields[&column.field_id], &Value::Null)?
                    .into(),
            })
        })
        .collect()
}

pub(crate) fn schema_field_ids(schema: &TableSchema) -> HashSet<FieldId> {
    let mut field_ids = HashSet::new();
    for field in &schema.fields {
        collect_field_ids(field, &mut field_ids);
    }
    field_ids
}

fn field_index(schema: &TableSchema, field_name: &str) -> Result<usize> {
    schema
        .fields
        .iter()
        .position(|field| field.name == field_name)
        .ok_or_else(|| TableError::InvalidSchema(format!("field '{field_name}' does not exist")))
}

fn next_field_id(used_field_ids: &HashSet<FieldId>) -> Result<u32> {
    used_field_ids
        .iter()
        .map(|field_id| field_id.0)
        .max()
        .map_or(Ok(0), |field_id| {
            field_id
                .checked_add(1)
                .ok_or_else(|| TableError::InvalidSchema("field id space exhausted".to_string()))
        })
}

fn collect_field_ids(field: &DataField, field_ids: &mut HashSet<FieldId>) {
    field_ids.insert(field.id);
    collect_type_field_ids(&field.logical_type, field_ids);
}

fn collect_type_field_ids(logical_type: &LogicalType, field_ids: &mut HashSet<FieldId>) {
    match &logical_type.kind {
        LogicalTypeKind::List { element_type } => collect_type_field_ids(element_type, field_ids),
        LogicalTypeKind::Map {
            key_type,
            value_type,
        } => {
            collect_type_field_ids(key_type, field_ids);
            collect_type_field_ids(value_type, field_ids);
        }
        LogicalTypeKind::Struct { fields } => {
            for field in fields {
                collect_field_ids(field, field_ids);
            }
        }
        LogicalTypeKind::Extension { extension } => {
            collect_type_field_ids(&extension.physical_type, field_ids);
        }
        _ => {}
    }
}
