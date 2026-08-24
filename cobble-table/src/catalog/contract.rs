use super::model::{CatalogSchemaId, CatalogTable, SchemaChange, TableIdentifier};
use crate::{TableError, TableSchema};
use thiserror::Error;

pub type CatalogResult<T> = std::result::Result<T, CatalogError>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CatalogError {
    #[error("invalid catalog identifier: {0}")]
    InvalidIdentifier(String),
    #[error("namespace already exists: {0:?}")]
    NamespaceAlreadyExists(Vec<String>),
    #[error("namespace not found: {0:?}")]
    NamespaceNotFound(Vec<String>),
    #[error("namespace is not empty: {0:?}")]
    NamespaceNotEmpty(Vec<String>),
    #[error("table already exists: {0:?}")]
    TableAlreadyExists(TableIdentifier),
    #[error("table not found: {0:?}")]
    TableNotFound(TableIdentifier),
    #[error("catalog schema {catalog_schema_id} not found for table {table:?}")]
    SchemaNotFound {
        table: TableIdentifier,
        catalog_schema_id: CatalogSchemaId,
    },
    #[error("invalid catalog schema evolution: {0}")]
    InvalidSchemaEvolution(String),
    #[error("invalid catalog metadata: {0}")]
    InvalidMetadata(String),
    #[error("catalog backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    Table(#[from] TableError),
}

/// Semantic catalog operations.
pub trait Catalog: Send + Sync {
    fn create_namespace(&self, namespace: Vec<String>) -> CatalogResult<()>;
    fn list_namespaces(&self) -> CatalogResult<Vec<Vec<String>>>;
    fn drop_namespace(&self, namespace: &[String]) -> CatalogResult<()>;
    fn create_table(
        &self,
        identifier: TableIdentifier,
        schema: TableSchema,
    ) -> CatalogResult<CatalogTable>;
    fn load_table(&self, identifier: &TableIdentifier) -> CatalogResult<CatalogTable>;
    fn load_table_schema(
        &self,
        identifier: &TableIdentifier,
        catalog_schema_id: CatalogSchemaId,
    ) -> CatalogResult<TableSchema>;
    fn evolve_schema(
        &self,
        identifier: &TableIdentifier,
        changes: Vec<SchemaChange>,
    ) -> CatalogResult<CatalogTable>;
    fn list_tables(&self, namespace: &[String]) -> CatalogResult<Vec<TableIdentifier>>;
    fn table_exists(&self, identifier: &TableIdentifier) -> CatalogResult<bool>;
    fn rename_table(
        &self,
        identifier: &TableIdentifier,
        new_name: String,
    ) -> CatalogResult<CatalogTable>;
    fn drop_table(&self, identifier: &TableIdentifier) -> CatalogResult<()>;
}
