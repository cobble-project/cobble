use std::sync::{Arc, mpsc};

use cobble_binding::structured::{
    ListConfig, StructuredColumnValue, StructuredDb, StructuredReadOptions, StructuredScanOptions,
    StructuredSingleDb,
};

use super::BridgeResult;

pub(crate) struct NativeStructuredDb {
    pub(crate) db: Arc<StructuredDb>,
}

pub(crate) struct NativeStructuredSingleDb {
    pub(crate) db: Arc<StructuredSingleDb>,
}

pub(crate) struct NativeStructuredReadOptions {
    pub(crate) options: StructuredReadOptions,
}

pub(crate) struct NativeStructuredScanOptions {
    pub(crate) options: StructuredScanOptions,
}

pub(crate) struct NativeStructuredRow {
    pub(crate) columns: Option<Vec<Option<StructuredColumnValue>>>,
}

pub(crate) struct NativeStructuredSchemaEdit {
    pub(crate) operations: Vec<SchemaOperation>,
}

pub(crate) struct NativePendingShardSnapshot {
    pub(crate) id: u64,
    pub(crate) receiver: Option<mpsc::Receiver<BridgeResult<cobble_binding::ShardSnapshotInput>>>,
}

pub(crate) struct NativePendingSnapshot {
    pub(crate) id: u64,
    pub(crate) receiver:
        Option<mpsc::Receiver<BridgeResult<cobble_binding::GlobalSnapshotManifest>>>,
}

#[derive(Clone)]
pub(crate) enum SchemaOperation {
    AddBytes(Option<String>, u16),
    AddList(Option<String>, u16, ListConfig),
    Delete(Option<String>, u16),
    SetFamilyTtl(Option<String>, bool),
}
