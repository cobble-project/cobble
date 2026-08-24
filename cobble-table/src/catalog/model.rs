use crate::{DataField, FieldId, TableSchema};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Stable identity of a table, independent of its catalog name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TableId(u32);

impl TableId {
    pub(crate) fn new(value: u32) -> Self {
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

/// An explicit, top-level catalog schema change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaChange {
    /// Append a new nullable value field.
    AddField(DataField),
    /// Rename a field while retaining its stable field id.
    RenameField { field_id: FieldId, new_name: String },
    /// Drop a non-key field.
    DropField(FieldId),
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogTable {
    pub(crate) identifier: TableIdentifier,
    pub(crate) table_id: TableId,
    pub(crate) catalog_schema_id: CatalogSchemaId,
    pub(crate) schema: TableSchema,
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
