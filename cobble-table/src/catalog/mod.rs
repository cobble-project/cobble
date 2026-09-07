mod contract;
mod file_catalog;
mod model;
mod store;

pub use crate::evolution::SchemaChange;
pub use contract::{Catalog, CatalogError, CatalogResult};
pub(crate) use file_catalog::physical_table_name;
pub use file_catalog::{FileCatalog, FileCatalogConfig, ShardSchemaMapping};
pub(crate) use file_catalog::{
    build_write_plan, materialize_write_plan, writer_builder_from_write_plan,
};
pub use model::{CatalogSchemaId, CatalogTable, TableId, TableIdentifier};
