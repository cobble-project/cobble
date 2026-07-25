//! Neutral data model shared by snapshot and runtime manifests.
//!
//! These records describe persisted LSM files. They deliberately do not carry
//! snapshot lifecycle or runtime publication semantics.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ManifestTruncationCursor {
    pub(crate) bucket: u16,
    pub(crate) column_family_id: u8,
    pub(crate) key: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ManifestLevel {
    pub(crate) ordinal: u8,
    pub(crate) tiered: bool,
    pub(crate) files: Vec<ManifestFile>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ManifestFile {
    pub(crate) file_id: u64,
    pub(crate) file_type: String,
    pub(crate) schema_id: u64,
    pub(crate) size: usize,
    pub(crate) start_key: String,
    pub(crate) end_key: String,
    pub(crate) path: String,
    pub(crate) has_separated_values: bool,
    pub(crate) bucket_range_start: u16,
    pub(crate) bucket_range_end: u16,
    pub(crate) effective_bucket_range_start: u16,
    pub(crate) effective_bucket_range_end: u16,
    pub(crate) vlog_file_seq_offset: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ManifestVlogFile {
    pub(crate) file_seq: u32,
    pub(crate) file_id: u64,
    pub(crate) path: String,
    pub(crate) valid_entries: u64,
}
