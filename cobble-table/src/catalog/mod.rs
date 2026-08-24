mod contract;
mod model;

pub use contract::{Catalog, CatalogError, CatalogResult};
pub use model::{CatalogSchemaId, CatalogTable, SchemaChange, TableId, TableIdentifier};
