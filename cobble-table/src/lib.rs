//! Typed table schemas, codecs, and runtime access for Cobble.
//!
//! `cobble-table` is independent from `cobble-data-structure`: table rows use a
//! versioned cross-language codec, while specialized structures such as lists
//! and priority queues keep their existing APIs and storage semantics.

mod bucket;
mod codec;
mod error;
mod layout;
mod logical_type;
mod metadata;
mod schema;
mod table;

#[cfg(test)]
#[path = "../tests/unit/metadata.rs"]
mod metadata_tests;

pub use bucket::BucketHash;
pub use codec::{KeyCodec, Value, ValueCodec};
pub use error::{Result, TableError};
pub use logical_type::{
    DataField, ExtensionType, FieldId, LogicalType, LogicalTypeKind, TimestampKind,
};
pub use schema::TableSchema;
pub use table::{Table, TableScan};
