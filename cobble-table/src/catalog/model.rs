use super::runtime::CatalogRuntimeContext;
use super::{CatalogError, CatalogResult, SchemaChange};
use crate::evolution::{apply_schema_changes, schema_field_ids};
use crate::{FieldId, TableSchema};
use cobble::TransformSpec;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Stable identity of a table, independent of its catalog name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TableId(u32);

impl TableId {
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl Display for TableId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

/// Per-table catalog schema identity, starting at zero and increasing monotonically.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CatalogSchemaId(u32);

impl CatalogSchemaId {
    pub(crate) const INITIAL: Self = Self(0);

    pub(crate) fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }

    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl From<u32> for CatalogSchemaId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl Display for CatalogSchemaId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

/// A semantic table name with an extensible, multi-component namespace.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableIdentifier {
    namespace: Vec<String>,
    name: String,
}

impl TableIdentifier {
    pub fn new<I, S>(namespace: I, name: impl Into<String>) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            namespace: namespace.into_iter().map(Into::into).collect(),
            name: name.into(),
        }
    }

    pub fn namespace(&self) -> &[String] {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn renamed(&self, name: String) -> Self {
        Self {
            namespace: self.namespace.clone(),
            name,
        }
    }
}

/// Semantic table descriptor returned by a catalog.
#[derive(Clone)]
pub struct CatalogTable {
    pub(crate) identifier: TableIdentifier,
    pub(crate) table_id: TableId,
    pub(crate) catalog_schema_id: CatalogSchemaId,
    pub(crate) schema: TableSchema,
    pub(crate) runtime_context: Arc<CatalogRuntimeContext>,
}

impl std::fmt::Debug for CatalogTable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CatalogTable")
            .field("identifier", &self.identifier)
            .field("table_id", &self.table_id)
            .field("catalog_schema_id", &self.catalog_schema_id)
            .field("schema", &self.schema)
            .finish()
    }
}

impl CatalogTable {
    pub fn identifier(&self) -> &TableIdentifier {
        &self.identifier
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn catalog_schema_id(&self) -> CatalogSchemaId {
        self.catalog_schema_id
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }
}

/// One semantic schema version, including stable field-id history and transforms from its
/// predecessor. Unlike a file catalog record, this has no storage-format header.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogSchemaVersion {
    table_id: TableId,
    catalog_schema_id: CatalogSchemaId,
    schema: TableSchema,
    used_field_ids: Vec<FieldId>,
    field_transforms: Vec<FieldTransform>,
}

impl CatalogSchemaVersion {
    /// Create the initial history value for an identity assigned by a catalog backend.
    pub fn initial(table_id: TableId, schema: TableSchema) -> CatalogResult<Self> {
        schema.validate()?;
        Ok(Self {
            table_id,
            catalog_schema_id: CatalogSchemaId::INITIAL,
            used_field_ids: sorted_field_ids(schema_field_ids(&schema)),
            schema,
            field_transforms: Vec::new(),
        })
    }

    /// Compute an unpublished successor; persistence and single-writer commit coordination
    /// remain the responsibility of the catalog backend.
    pub fn evolve(&self, changes: Vec<SchemaChange>) -> CatalogResult<Self> {
        let catalog_schema_id = self.catalog_schema_id.next().ok_or_else(|| {
            CatalogError::InvalidSchemaEvolution("schema id space exhausted".to_string())
        })?;
        let (schema, used_field_ids, field_transforms) = apply_schema_changes(
            self.schema.clone(),
            changes,
            self.used_field_ids.iter().copied().collect(),
        )
        .map_err(|error| CatalogError::InvalidSchemaEvolution(error.to_string()))?;
        Ok(Self {
            table_id: self.table_id,
            catalog_schema_id,
            schema,
            used_field_ids: sorted_field_ids(used_field_ids),
            field_transforms,
        })
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn catalog_schema_id(&self) -> CatalogSchemaId {
        self.catalog_schema_id
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    pub fn used_field_ids(&self) -> &[FieldId] {
        &self.used_field_ids
    }

    pub fn field_transforms(&self) -> &[FieldTransform] {
        &self.field_transforms
    }
}

fn sorted_field_ids(field_ids: HashSet<FieldId>) -> Vec<FieldId> {
    let mut field_ids = field_ids.into_iter().collect::<Vec<_>>();
    field_ids.sort_unstable();
    field_ids
}

/// One field transform introduced by a schema version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldTransform {
    pub(crate) field_id: FieldId,
    pub(crate) transform: TransformSpec,
}

impl FieldTransform {
    pub fn field_id(&self) -> FieldId {
        self.field_id
    }

    pub fn transform(&self) -> &TransformSpec {
        &self.transform
    }
}

/// One catalog schema version materialized into a shard's core schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardSchemaMapping {
    pub(crate) table_id: TableId,
    pub(crate) db_id: String,
    pub(crate) catalog_schema_id: CatalogSchemaId,
    pub(crate) core_schema_id: u64,
}

impl ShardSchemaMapping {
    pub fn table_id(&self) -> TableId {
        self.table_id
    }
    pub fn db_id(&self) -> &str {
        &self.db_id
    }
    pub fn catalog_schema_id(&self) -> CatalogSchemaId {
        self.catalog_schema_id
    }
    pub fn core_schema_id(&self) -> u64 {
        self.core_schema_id
    }
}
