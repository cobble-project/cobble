//! Neutral data model shared by snapshot and runtime manifests.
//!
//! These records describe persisted LSM files. They deliberately do not carry
//! snapshot lifecycle or runtime publication semantics.

use crate::data_file::DataFile;
use crate::db_state::TruncationCursorMap;
use crate::error::{Error, Result};
use crate::file::FileManager;
use crate::vlog::VlogVersion;
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

pub(crate) fn manifest_file_from_data_file(file: &DataFile, path: String) -> ManifestFile {
    ManifestFile {
        file_id: file.file_id,
        file_type: file.file_type.as_str().to_string(),
        schema_id: file.schema_id,
        size: file.size,
        start_key: to_hex(&file.start_key),
        end_key: to_hex(&file.end_key),
        path,
        has_separated_values: file.has_separated_values,
        bucket_range_start: *file.bucket_range.start(),
        bucket_range_end: *file.bucket_range.end(),
        effective_bucket_range_start: *file.effective_bucket_range.start(),
        effective_bucket_range_end: *file.effective_bucket_range.end(),
        vlog_file_seq_offset: file.vlog_file_seq_offset,
    }
}

pub(crate) fn manifest_vlog_files(
    version: &VlogVersion,
    file_manager: &FileManager,
) -> Result<Vec<ManifestVlogFile>> {
    version
        .files_with_entries()
        .into_iter()
        .map(|(file_seq, tracked_id, valid_entries)| {
            let file_id = tracked_id.file_id();
            let path = file_manager
                .get_data_file_full_path(file_id)
                .ok_or_else(|| {
                    Error::InvalidState(format!("Unknown value-log file ID {file_id}"))
                })?;
            Ok(ManifestVlogFile {
                file_seq,
                file_id,
                path,
                valid_entries,
            })
        })
        .collect()
}

pub(crate) fn manifest_truncation_cursors(
    cursors: &TruncationCursorMap,
) -> Vec<ManifestTruncationCursor> {
    let mut entries: Vec<_> = cursors.iter().collect();
    entries.sort_by_key(|(id, _)| (id.bucket, id.column_family_id));
    entries
        .into_iter()
        .map(|(id, key)| ManifestTruncationCursor {
            bucket: id.bucket,
            column_family_id: id.column_family_id,
            key: to_hex(key),
        })
        .collect()
}

pub(crate) fn to_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}
