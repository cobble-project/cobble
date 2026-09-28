mod contract;
mod file_catalog;
mod materialize;
mod model;
mod runtime;
mod store;

pub use crate::evolution::SchemaChange;
pub use contract::{Catalog, CatalogError, CatalogResult, CatalogSchemaStore};
pub use file_catalog::{FileCatalog, FileCatalogConfig};
pub(crate) use model::validate_identifier;
pub use model::{
    CatalogSchemaId, CatalogSchemaVersion, CatalogTable, FieldTransform, ShardSchemaMapping,
    TableId, TableIdentifier,
};
pub use runtime::CatalogRuntimeContext;
pub(crate) use runtime::valid_storage_id;
pub(crate) use runtime::{
    build_write_plan, materialize_write_plan, physical_table_name, writer_builder_from_write_plan,
};
