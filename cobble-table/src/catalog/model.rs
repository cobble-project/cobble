use crate::TableSchema;
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
    pub(crate) schema: TableSchema,
}

impl CatalogTable {
    pub fn identifier(&self) -> &TableIdentifier {
        &self.identifier
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }
}

/// Runtime-only configuration for a file catalog.
///
/// Volume descriptors and credentials remain in [`cobble::Config`] and are never written into
/// catalog metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileCatalogConfig {
    storage_id: String,
}

impl FileCatalogConfig {
    pub fn new(storage_id: impl Into<String>) -> Self {
        Self {
            storage_id: storage_id.into(),
        }
    }

    pub fn storage_id(&self) -> &str {
        &self.storage_id
    }
}
