mod contract;
mod file_catalog;
mod model;
mod store;

pub use crate::evolution::SchemaChange;
pub use contract::{Catalog, CatalogError, CatalogResult};
pub(crate) use file_catalog::physical_table_name;
pub use file_catalog::{FileCatalog, FileCatalogConfig, ShardSchemaMapping};
pub use model::{CatalogSchemaId, CatalogTable, TableId, TableIdentifier};
