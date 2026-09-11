use crate::logical_type::{assign_fresh_field_ids, assign_fresh_type_ids};
use crate::metadata::TableMetadata;
use crate::{
    DataField, FieldId, LogicalType, LogicalTypeKind, Result, TableError, TableSchema, Value,
    ValueCodec,
};
use cobble::{ColumnEvolution, TransformSpec};
use serde::{Deserialize, Serialize};
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
    /// Transform one existing non-key field, optionally changing its logical type.
    ///
    /// The field name is resolved when this change is applied. Catalog storage
    /// retains the resulting stable field id with the opaque transform spec.
    TransformField {
        field_name: String,
        logical_type: LogicalType,
        transform: TransformSpec,
    },
}

/// One persisted field transform for a single catalog schema version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FieldTransform {
    pub(crate) field_id: FieldId,
    pub(crate) transform: TransformSpec,
}

/// Apply sequential schema edits while retaining every historical field id.
///
/// `used_field_ids` must include both active and retired ids from every prior
/// version of the table schema.
pub(crate) fn apply_schema_changes(
    mut schema: TableSchema,
    changes: Vec<SchemaChange>,
    mut used_field_ids: HashSet<FieldId>,
) -> Result<(TableSchema, HashSet<FieldId>, Vec<FieldTransform>)> {
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

    let mut added_fields = HashSet::new();
    let mut field_transforms = HashMap::new();
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
                added_fields.insert(added.id);
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
                let field = schema.fields.remove(index);
                // A later drop makes an earlier same-version transform unreachable.
                field_transforms.remove(&field.id);
            }
            SchemaChange::TransformField {
                field_name,
                mut logical_type,
                transform,
            } => {
                if transform.transform_type.trim().is_empty() {
                    return Err(TableError::InvalidSchema(
                        "field transform type must not be empty".to_string(),
                    ));
                }
                let index = field_index(&schema, &field_name)?;
                let field = &mut schema.fields[index];
                if key_ids.contains(&field.id) {
                    return Err(TableError::InvalidSchema(format!(
                        "key field '{}' cannot be transformed",
                        field_name
                    )));
                }
                if added_fields.contains(&field.id) {
                    return Err(TableError::InvalidSchema(format!(
                        "added field '{}' cannot be transformed in the same schema version",
                        field_name
                    )));
                }
                if field_transforms.contains_key(&field.id) {
                    return Err(TableError::InvalidSchema(format!(
                        "field '{}' has multiple transforms in one schema version",
                        field_name
                    )));
                }
                if logical_type != field.logical_type {
                    let mut new_nested_ids = HashSet::new();
                    collect_type_field_ids(&logical_type, &mut new_nested_ids);
                    if !new_nested_ids.is_empty() {
                        let mut next_id = next_field_id(&used_field_ids)?;
                        assign_fresh_type_ids(&mut logical_type, &mut next_id)?;
                        new_nested_ids.clear();
                        collect_type_field_ids(&logical_type, &mut new_nested_ids);
                        used_field_ids.extend(new_nested_ids);
                    }
                }
                field.logical_type = logical_type;
                field_transforms.insert(field.id, transform);
            }
        }
    }
    let schema = TableSchema::new(schema.fields, schema.primary_key, schema.bucket_key)?;
    let mut field_transforms = field_transforms
        .into_iter()
        .map(|(field_id, transform)| FieldTransform {
            field_id,
            transform,
        })
        .collect::<Vec<_>>();
    field_transforms.sort_by_key(|transform| transform.field_id);
    Ok((schema, used_field_ids, field_transforms))
}

/// Compile core column remapping for two validated table metadata versions.
pub(crate) fn compile_column_evolution(
    existing: &TableMetadata,
    target: &TableMetadata,
    field_transforms: &[FieldTransform],
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
    let transforms = field_transforms
        .iter()
        .map(|transform| (transform.field_id, &transform.transform))
        .collect::<HashMap<_, _>>();
    if transforms.len() != field_transforms.len() {
        return Err(TableError::InvalidSchema(
            "catalog schema has duplicate field transforms".to_string(),
        ));
    }
    for field in &target.schema.fields {
        if let Some(previous) = existing_fields.get(&field.id)
            && previous.logical_type != field.logical_type
            && !transforms.contains_key(&field.id)
        {
            return Err(TableError::InvalidSchema(format!(
                "schema evolution changed the type of field {} without a transform",
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
                    transform: transforms.get(&column.field_id).map(|spec| (*spec).clone()),
                });
            }
            if transforms.contains_key(&column.field_id) {
                return Err(TableError::InvalidSchema(format!(
                    "catalog transform cannot target added field {}",
                    column.field_id.0
                )));
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
