//! Table schema and physical layout contracts for Cobble.
//!
//! `cobble-table` is independent from `cobble-data-structure`: table rows use a
//! versioned cross-language codec, while specialized structures such as lists
//! and priority queues keep their existing APIs and storage semantics.

mod error;
mod layout;
mod logical_type;
mod metadata;
mod schema;

pub use error::{Result, TableError};
pub use layout::{
    COBBLE_TABLE_CODEC_V1, LAYOUT_VERSION_CURRENT, LayoutCompiler, LayoutFingerprint,
    RecordLayoutDescriptor, ValueColumnLayout, ValueStorage,
};
pub use logical_type::{
    DataField, ExtensionType, FieldId, LogicalType, LogicalTypeKind, TimestampKind,
};
pub use metadata::{TABLE_METADATA_FORMAT, TABLE_METADATA_VERSION_CURRENT, TableMetadata};
pub use schema::TableSchema;
