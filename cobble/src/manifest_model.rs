//! Neutral data model shared by snapshot and runtime manifests.
//!
//! These records describe persisted LSM files. They deliberately do not carry
//! snapshot lifecycle or runtime publication semantics.

use crate::data_file::{DataFile, DataFileType};
use crate::db_state::{TruncationCursorId, TruncationCursorMap};
use crate::error::{Error, Result};
use crate::file::logical_file::ReplicaOrigin;
use crate::file::{FileManager, TrackedFileId, VLOG_FILE_PRIORITY, lsm_file_priority_for_level};
use crate::lsm::{LSMTreeVersion, Level};
use crate::vlog::VlogVersion;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use std::str::FromStr;
use std::sync::Arc;

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
    /// Maximum `expired_at` across all values in this file. 0 = no value has an expiration.
    /// Defaults to 0 for manifests written before this field existed.
    #[serde(default)]
    pub(crate) max_expired_at: u32,
    #[serde(default)]
    pub(crate) origin: ReplicaOrigin,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ManifestVlogFile {
    pub(crate) file_seq: u32,
    pub(crate) file_id: u64,
    pub(crate) path: String,
    pub(crate) valid_entries: u64,
    #[serde(default)]
    pub(crate) origin: ReplicaOrigin,
}

pub(crate) fn manifest_file_from_data_file(file: &DataFile, path: String) -> ManifestFile {
    manifest_file_from_data_file_with_origin(file, path, ReplicaOrigin::Owned)
}

pub(crate) fn manifest_file_from_data_file_with_origin(
    file: &DataFile,
    path: String,
    origin: ReplicaOrigin,
) -> ManifestFile {
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
        max_expired_at: file.max_expired_at(),
        origin,
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
                origin: ReplicaOrigin::Owned,
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

pub(crate) fn from_hex(hex: &str) -> Result<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return Err(Error::IoError(format!(
            "Invalid hex string length: {}",
            hex.len()
        )));
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        out.push((hex_value(bytes[idx])? << 4) | hex_value(bytes[idx + 1])?);
        idx += 2;
    }
    Ok(out)
}

/// Rebuilds read-only or writer-owned LSM versions from neutral persisted file descriptors.
/// The same physical file may appear in more than one logical tree; the file manager's readonly
/// registration intentionally shares that single physical handle.
pub(crate) fn build_tree_versions_from_levels(
    file_manager: &Arc<FileManager>,
    tree_levels: &[Vec<ManifestLevel>],
    read_only: bool,
) -> Result<Vec<LSMTreeVersion>> {
    tree_levels
        .iter()
        .map(|levels| {
            levels
                .iter()
                .map(|level| {
                    let files = level
                        .files
                        .iter()
                        .map(|file| build_data_file(file_manager, file, level.ordinal, read_only))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Level {
                        ordinal: level.ordinal,
                        tiered: level.tiered,
                        files,
                    })
                })
                .collect::<Result<Vec<_>>>()
                .map(|levels| LSMTreeVersion { levels })
        })
        .collect()
}

/// Rebuilds a value-log version from neutral persisted descriptors.
pub(crate) fn build_vlog_version_from_files(
    file_manager: &Arc<FileManager>,
    vlog_files: &[ManifestVlogFile],
    read_only: bool,
) -> Result<VlogVersion> {
    let mut files = Vec::with_capacity(vlog_files.len());
    for file in vlog_files {
        let tracked_id = if read_only {
            file_manager.register_data_file_readonly_with_origin(
                file.file_id,
                &file.path,
                file.origin.clone(),
            )?;
            file_manager.set_data_file_priority(file.file_id, VLOG_FILE_PRIORITY)?;
            TrackedFileId::untracked(file.file_id)
        } else {
            if !file_manager.has_data_file(file.file_id) {
                match &file.origin {
                    ReplicaOrigin::Owned => {
                        file_manager.register_data_file(file.file_id, &file.path)?
                    }
                    _ => file_manager.register_data_file_readonly_with_origin(
                        file.file_id,
                        &file.path,
                        file.origin.clone(),
                    )?,
                }
            }
            file_manager.set_data_file_priority(file.file_id, VLOG_FILE_PRIORITY)?;
            TrackedFileId::new(file_manager, file.file_id)
        };
        files.push((file.file_seq, tracked_id, file.valid_entries));
    }
    Ok(VlogVersion::from_files_with_entries(files))
}

pub(crate) fn build_truncation_cursors(
    cursors: &[ManifestTruncationCursor],
) -> Result<TruncationCursorMap> {
    cursors
        .iter()
        .map(|cursor| {
            Ok((
                TruncationCursorId::new(cursor.bucket, cursor.column_family_id),
                from_hex(&cursor.key)?,
            ))
        })
        .collect()
}

/// Returns every schema id needed to interpret a persisted layout, including an otherwise empty
/// latest schema version.
pub(crate) fn manifest_schema_ids(
    latest_schema_id: u64,
    tree_levels: &[Vec<ManifestLevel>],
) -> BTreeSet<u64> {
    let mut schema_ids = BTreeSet::from([latest_schema_id]);
    for levels in tree_levels {
        for level in levels {
            schema_ids.extend(level.files.iter().map(|file| file.schema_id));
        }
    }
    schema_ids
}

fn build_data_file(
    file_manager: &Arc<FileManager>,
    file: &ManifestFile,
    ordinal: u8,
    read_only: bool,
) -> Result<Arc<DataFile>> {
    let file_type = DataFileType::from_str(&file.file_type).map_err(Error::IoError)?;
    let start_key = from_hex(&file.start_key)?;
    let end_key = from_hex(&file.end_key)?;
    let tracked_id = if read_only {
        file_manager.register_data_file_readonly_with_origin(
            file.file_id,
            &file.path,
            file.origin.clone(),
        )?;
        file_manager.set_data_file_priority(file.file_id, lsm_file_priority_for_level(ordinal))?;
        TrackedFileId::untracked(file.file_id)
    } else {
        if !file_manager.has_data_file(file.file_id) {
            match &file.origin {
                ReplicaOrigin::Owned => {
                    file_manager.register_data_file(file.file_id, &file.path)?
                }
                _ => file_manager.register_data_file_readonly_with_origin(
                    file.file_id,
                    &file.path,
                    file.origin.clone(),
                )?,
            }
        }
        file_manager.set_data_file_priority(file.file_id, lsm_file_priority_for_level(ordinal))?;
        TrackedFileId::new(file_manager, file.file_id)
    };
    let data_file = DataFile::new(
        file_type,
        start_key,
        end_key,
        file.file_id,
        tracked_id,
        file.schema_id,
        file.size,
        file.bucket_range_start..=file.bucket_range_end,
        file.effective_bucket_range_start..=file.effective_bucket_range_end,
    )
    .with_vlog_offset(file.vlog_file_seq_offset)
    .with_separated_values(file.has_separated_values);
    data_file.set_max_expired_at(file.max_expired_at);
    file_manager.finalize_data_file(&data_file)?;
    Ok(Arc::new(data_file))
}

/// Verifies every logical file in a durable layout is readable through the path registered in
/// its runtime manifest descriptor.
pub(crate) fn ensure_preferred_replicas_readable(
    file_manager: &Arc<FileManager>,
    tree_levels: &[Vec<ManifestLevel>],
    vlog_files: &[ManifestVlogFile],
) -> Result<()> {
    let mut file_ids = HashSet::new();
    for level in tree_levels.iter().flatten() {
        for file in &level.files {
            file_ids.insert(file.file_id);
        }
    }
    for file in vlog_files {
        file_ids.insert(file.file_id);
    }
    for file_id in file_ids {
        file_manager.open_data_file_reader(file_id)?;
    }
    Ok(())
}

fn hex_value(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(10 + byte - b'a'),
        b'A'..=b'F' => Ok(10 + byte - b'A'),
        _ => Err(Error::IoError(format!(
            "Invalid hex character: {}",
            byte as char
        ))),
    }
}
