mod contract;
mod model;

pub use crate::evolution::SchemaChange;
pub use contract::{Catalog, CatalogError, CatalogResult};
pub use model::{CatalogSchemaId, CatalogTable, TableId, TableIdentifier};
