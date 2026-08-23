use crate::{DataField, FieldId, LogicalType, LogicalTypeKind, Result, TableError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// User-visible semantic schema of a table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub fields: Vec<DataField>,
    pub primary_key: Vec<FieldId>,
    pub bucket_key: Vec<FieldId>,
}

impl TableSchema {
    pub fn new(
        fields: Vec<DataField>,
        primary_key: Vec<FieldId>,
        bucket_key: Vec<FieldId>,
    ) -> Result<Self> {
        let schema = Self {
            fields,
            primary_key,
            bucket_key,
        };
        schema.validate()?;
        Ok(schema)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.fields.is_empty() {
            return Err(TableError::InvalidSchema(
                "table must contain at least one field".to_string(),
            ));
        }
        if self.primary_key.is_empty() {
            return Err(TableError::InvalidSchema(
                "primary key must not be empty".to_string(),
            ));
        }
        if self.bucket_key.is_empty() {
            return Err(TableError::InvalidSchema(
                "bucket key must not be empty".to_string(),
            ));
        }
        if self.bucket_key.len() > self.primary_key.len()
            || self.primary_key[..self.bucket_key.len()] != self.bucket_key
        {
            return Err(TableError::InvalidSchema(
                "bucket key must be a prefix of the primary key".to_string(),
            ));
        }

        let mut field_ids = HashSet::new();
        let mut field_names = HashSet::new();
        for field in &self.fields {
            validate_field_tree(field, &mut field_ids)?;
            if !field_names.insert(field.name.as_str()) {
                return Err(TableError::InvalidSchema(format!(
                    "duplicate top-level field name: {}",
                    field.name
                )));
            }
        }

        let top_level = self
            .fields
            .iter()
            .map(|field| (field.id, field))
            .collect::<HashMap<_, _>>();
        let mut key_ids = HashSet::new();
        for id in &self.primary_key {
            let field = top_level.get(id).ok_or_else(|| {
                TableError::InvalidSchema(format!("primary-key field {} does not exist", id.0))
            })?;
            if !key_ids.insert(*id) {
                return Err(TableError::InvalidSchema(format!(
                    "duplicate primary-key field: {}",
                    id.0
                )));
            }
            if !field.logical_type.is_key_compatible() {
                return Err(TableError::InvalidSchema(format!(
                    "field '{}' cannot be used in a primary key",
                    field.name
                )));
            }
        }
        Ok(())
    }
}

fn validate_field_tree(field: &DataField, ids: &mut HashSet<FieldId>) -> Result<()> {
    field.validate()?;
    if !ids.insert(field.id) {
        return Err(TableError::InvalidSchema(format!(
            "duplicate field id: {}",
            field.id.0
        )));
    }
    validate_nested_fields(&field.logical_type, ids)
}

fn validate_nested_fields(logical_type: &LogicalType, ids: &mut HashSet<FieldId>) -> Result<()> {
    match &logical_type.kind {
        LogicalTypeKind::List { element_type } => validate_nested_fields(element_type, ids),
        LogicalTypeKind::Map {
            key_type,
            value_type,
        } => {
            validate_nested_fields(key_type, ids)?;
            validate_nested_fields(value_type, ids)
        }
        LogicalTypeKind::Struct { fields } => {
            let mut names = HashSet::new();
            for field in fields {
                if !names.insert(field.name.as_str()) {
                    return Err(TableError::InvalidSchema(format!(
                        "duplicate nested field name: {}",
                        field.name
                    )));
                }
                validate_field_tree(field, ids)?;
            }
            Ok(())
        }
        LogicalTypeKind::Extension { extension } => {
            validate_nested_fields(&extension.physical_type, ids)
        }
        _ => Ok(()),
    }
}
