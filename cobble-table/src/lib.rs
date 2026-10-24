//! Typed table schemas, codecs, and runtime access for Cobble.
//!
//! Table rows use a versioned cross-language codec. List appends reuse native framing primitives
//! without changing that codec or depending on structured database wrappers.

mod append;
mod bucket;
pub mod catalog;
mod codec;
mod error;
mod evolution;
mod layout;
mod logical_type;
mod metadata;
mod projection;
mod runtime;
mod scan;
mod schema;
pub mod snapshot;
mod store;
mod table;
mod transform;
mod write;

#[cfg(feature = "ffi")]
#[doc(hidden)]
pub mod ffi;

#[cfg(test)]
#[path = "../tests/unit/metadata.rs"]
mod metadata_tests;

#[cfg(test)]
#[path = "../tests/unit/evolution.rs"]
mod evolution_tests;

pub use append::{list_element_transform, table_merge_operator_ids, table_merge_operator_resolver};
pub use bucket::BucketHash;
pub use codec::{KeyCodec, Value, ValueCodec};
pub use error::{Result, TableError};
pub use evolution::SchemaChange;
pub use logical_type::{
    DataField, ExtensionType, FieldId, LogicalType, LogicalTypeKind, TimestampKind,
};
pub use projection::ValueProjection;
pub use runtime::{ReadOnlyTableBuilder, TableReader, TableReaderBuilder, TableWriterBuilder};
pub use scan::{
    TableScanPlan, TableScanSplit, TableScanSplitScanner, TableScanSplitScannerBuilder,
};
pub use schema::{TableReadSchema, TableSchema, TableSchemaBuilder};
pub use store::{TableStore, TableStoreBuilder};
pub use table::{
    ProjectedTableScan, ReadOnlyTable, Table, TableKey, TableKeyBuilder, TableProjection, TableScan,
};
pub use transform::register_schema_transforms;
pub use write::{TableWriteBuilder, TableWritePlan};
