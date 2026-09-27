mod contract;
mod file_catalog;
mod materialize;
mod model;
mod runtime;
mod store;

pub use crate::evolution::SchemaChange;
pub use contract::{Catalog, CatalogError, CatalogResult};
pub(crate) use file_catalog::materialize_write_plan;
pub use file_catalog::{FileCatalog, FileCatalogConfig};
pub use model::{
    CatalogSchemaId, CatalogSchemaVersion, CatalogTable, FieldTransform, ShardSchemaMapping,
    TableId, TableIdentifier,
};
pub(crate) use runtime::physical_table_name;
pub(crate) use runtime::{build_write_plan, writer_builder_from_write_plan};
