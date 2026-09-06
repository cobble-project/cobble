use crate::logical_type::assign_fresh_field_ids;
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
    /// Start building a fresh schema with deterministic generated field ids.
    pub fn builder() -> TableSchemaBuilder {
        TableSchemaBuilder::new()
    }

    /// Construct a schema with explicit field ids.
    ///
    /// Prefer [`Self::builder`] for fresh schemas. This constructor preserves
    /// ids exactly and is intended for restoration or advanced interoperability
    /// with an already identified schema.
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

/// Builder for a fresh table schema with generated, deterministic field ids.
///
/// All field ids, including those inside nested types, are assigned from zero
/// in depth-first declaration order. Use [`TableSchema::new`] or load the
/// persisted schema to preserve existing identities instead.
#[derive(Default)]
pub struct TableSchemaBuilder {
    fields: Vec<(String, LogicalType)>,
    primary_key: Vec<String>,
    bucket_key: Vec<String>,
}

impl TableSchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a top-level field in schema order.
    pub fn field(mut self, name: impl Into<String>, logical_type: LogicalType) -> Self {
        self.fields.push((name.into(), logical_type));
        self
    }

    /// Set primary-key fields by their exact top-level names.
    pub fn primary_key<I, S>(mut self, field_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.primary_key = field_names
            .into_iter()
            .map(|field_name| field_name.as_ref().to_string())
            .collect();
        self
    }

    /// Set bucket-key fields by their exact top-level names.
    /// This is required and must be a nonempty prefix of the primary key.
    pub fn bucket_key<I, S>(mut self, field_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.bucket_key = field_names
            .into_iter()
            .map(|field_name| field_name.as_ref().to_string())
            .collect();
        self
    }

    /// Assign preorder ids, resolve keys, and validate the completed schema.
    pub fn build(self) -> Result<TableSchema> {
        let mut fields = self
            .fields
            .into_iter()
            .map(|(name, logical_type)| DataField {
                id: FieldId(0),
                name,
                logical_type,
            })
            .collect::<Vec<_>>();
        let mut next_id = 0;
        assign_fresh_field_ids(&mut fields, &mut next_id)?;
        let primary_key = resolve_key_names(&fields, &self.primary_key, "primary key")?;
        let bucket_key = resolve_key_names(&fields, &self.bucket_key, "bucket key")?;
        TableSchema::new(fields, primary_key, bucket_key)
    }
}

fn resolve_key_names(
    fields: &[DataField],
    field_names: &[String],
    key_name: &str,
) -> Result<Vec<FieldId>> {
    field_names
        .iter()
        .map(|field_name| {
            fields
                .iter()
                .find(|field| field.name == *field_name)
                .map(|field| field.id)
                .ok_or_else(|| {
                    TableError::InvalidSchema(format!(
                        "{key_name} field '{field_name}' does not exist"
                    ))
                })
        })
        .collect()
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
