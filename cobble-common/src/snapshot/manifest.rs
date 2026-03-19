use super::{ActiveMemtableSnapshotData, DbSnapshot};
use crate::data_file::{DataFile, DataFileType};
use crate::error::{Error, Result};
use crate::file::{
    BufferedWriter, File, FileManager, SequentialWriteFile, TrackedFileId, VLOG_FILE_PRIORITY,
    lsm_file_priority_for_level,
};
use crate::lsm::{LSMTreeVersion, Level};
use crate::vlog::VlogVersion;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ManifestSnapshot {
    pub(crate) id: u64,
    pub(crate) seq_id: u64,
    pub(crate) latest_schema_id: u64,
    pub(crate) bucket_ranges: Vec<RangeInclusive<u16>>,
    pub(crate) lsm_tree_bucket_ranges: Vec<RangeInclusive<u16>>,
    pub(crate) tree_levels: Vec<Vec<ManifestLevel>>,
    pub(crate) vlog_files: Vec<ManifestVlogFile>,
    pub(crate) active_memtable_data: Vec<ActiveMemtableSnapshotData>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ManifestIncrementalSnapshot {
    pub(crate) id: u64,
    pub(crate) seq_id: u64,
    pub(crate) base_snapshot_id: u64,
    pub(crate) latest_schema_id: u64,
    pub(crate) bucket_ranges: Vec<RangeInclusive<u16>>,
    pub(crate) lsm_tree_bucket_ranges: Vec<RangeInclusive<u16>>,
    pub(crate) tree_level_edits: Vec<ManifestTreeLevelEdit>,
    // always include vlog file info in incremental manifests since vlog files are more likely to have changes
    pub(crate) vlog_files: Vec<ManifestVlogFile>,
    pub(crate) active_memtable_data: Vec<ActiveMemtableSnapshotData>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ManifestTreeLevelEdit {
    pub(crate) tree_idx: usize,
    pub(crate) level_edits: Vec<ManifestLevelEdit>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub(crate) enum ManifestPayload {
    IncrementalSnapshot(ManifestIncrementalSnapshot),
    Snapshot(ManifestSnapshot),
}

#[derive(Clone)]
pub(crate) struct LoadedManifest {
    pub(crate) snapshot_id: u64,
    pub(crate) base_snapshot_id: Option<u64>,
    pub(crate) manifest: ManifestSnapshot,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ManifestLevel {
    pub(crate) ordinal: u8,
    pub(crate) tiered: bool,
    pub(crate) files: Vec<ManifestFile>,
}

#[derive(Clone, Deserialize, Serialize)]
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

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ManifestLevelEdit {
    pub(crate) level: u8,
    pub(crate) tiered: bool,
    pub(crate) removed_file_ids: Vec<u64>,
    pub(crate) new_files: Vec<ManifestFile>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ManifestVlogFile {
    pub(crate) file_seq: u32,
    pub(crate) file_id: u64,
    pub(crate) path: String,
    pub(crate) valid_entries: u64,
}

pub(crate) fn decode_manifest(bytes: &[u8]) -> Result<ManifestPayload> {
    serde_json::from_slice(bytes)
        .map_err(|err| Error::IoError(format!("Failed to decode manifest: {}", err)))
}

pub(crate) fn parse_snapshot_manifest_id(name: &str) -> Option<u64> {
    let name = name.rsplit('/').next().unwrap_or(name);
    name.strip_prefix("SNAPSHOT-")?.parse::<u64>().ok()
}

pub(crate) fn list_snapshot_manifest_ids(file_manager: &Arc<FileManager>) -> Result<Vec<u64>> {
    let mut snapshot_ids: Vec<u64> = file_manager
        .list_snapshot_metadata_names()?
        .into_iter()
        .filter_map(|name| parse_snapshot_manifest_id(&name))
        .collect();
    snapshot_ids.sort_unstable();
    snapshot_ids.dedup();
    Ok(snapshot_ids)
}

pub(crate) fn load_manifest_entry(
    file_manager: &Arc<FileManager>,
    snapshot_id: u64,
    loaded_by_id: &HashMap<u64, LoadedManifest>,
) -> Result<LoadedManifest> {
    let manifest_name = snapshot_manifest_name(snapshot_id);
    let reader = file_manager.open_metadata_file_reader_untracked(&manifest_name)?;
    let bytes = reader.read_at(0, reader.size())?;
    let (base_snapshot_id, manifest) = match decode_manifest(bytes.as_ref())? {
        ManifestPayload::Snapshot(manifest) => (None, manifest),
        ManifestPayload::IncrementalSnapshot(incremental) => {
            let base_snapshot_id = Some(incremental.base_snapshot_id);
            let manifest = if let Some(base) = loaded_by_id.get(&incremental.base_snapshot_id) {
                let mut resolved = base.manifest.clone();
                apply_manifest_tree_level_edits(
                    &mut resolved.tree_levels,
                    &incremental.tree_level_edits,
                )?;
                resolved.vlog_files = incremental.vlog_files;
                resolved.id = incremental.id;
                resolved.seq_id = incremental.seq_id;
                resolved.latest_schema_id = incremental.latest_schema_id;
                resolved.active_memtable_data = incremental.active_memtable_data;
                resolved.bucket_ranges = incremental.bucket_ranges;
                resolved.lsm_tree_bucket_ranges = incremental.lsm_tree_bucket_ranges;
                resolved
            } else {
                load_manifest_for_snapshot(file_manager, snapshot_id)?
            };
            (base_snapshot_id, manifest)
        }
    };
    Ok(LoadedManifest {
        snapshot_id,
        base_snapshot_id,
        manifest,
    })
}

/// Load the manifest dependency chain for the given snapshot and resolve each manifest once.
pub(crate) fn load_manifest_chain(
    file_manager: &Arc<FileManager>,
    snapshot_id: u64,
) -> Result<Vec<LoadedManifest>> {
    let mut chain = Vec::new();
    let mut visited = HashSet::new();
    let mut raw_payloads = Vec::new();
    let mut next_id = Some(snapshot_id);
    while let Some(current_id) = next_id {
        if !visited.insert(current_id) {
            return Err(Error::IoError(format!(
                "Snapshot manifest dependency cycle detected for {}",
                current_id
            )));
        }
        let manifest_name = snapshot_manifest_name(current_id);
        let reader = file_manager.open_metadata_file_reader_untracked(&manifest_name)?;
        let bytes = reader.read_at(0, reader.size())?;
        let payload = decode_manifest(bytes.as_ref())?;
        next_id = match &payload {
            ManifestPayload::Snapshot(_) => None,
            ManifestPayload::IncrementalSnapshot(manifest) => Some(manifest.base_snapshot_id),
        };
        raw_payloads.push((current_id, payload));
    }
    raw_payloads.reverse();

    let mut resolved_by_id: HashMap<u64, ManifestSnapshot> = HashMap::new();
    for (current_id, payload) in raw_payloads {
        let (base_snapshot_id, resolved_manifest) = match payload {
            ManifestPayload::Snapshot(manifest) => (None, manifest),
            ManifestPayload::IncrementalSnapshot(manifest) => {
                let mut resolved_base = resolved_by_id
                    .get(&manifest.base_snapshot_id)
                    .cloned()
                    .ok_or_else(|| {
                        Error::IoError(format!(
                            "Missing base manifest {} for snapshot {}",
                            manifest.base_snapshot_id, current_id
                        ))
                    })?;
                apply_manifest_tree_level_edits(
                    &mut resolved_base.tree_levels,
                    &manifest.tree_level_edits,
                )?;
                resolved_base.vlog_files = manifest.vlog_files;
                resolved_base.id = manifest.id;
                resolved_base.seq_id = manifest.seq_id;
                resolved_base.latest_schema_id = manifest.latest_schema_id;
                resolved_base.active_memtable_data = manifest.active_memtable_data;
                resolved_base.bucket_ranges = manifest.bucket_ranges;
                resolved_base.lsm_tree_bucket_ranges = manifest.lsm_tree_bucket_ranges;
                (Some(manifest.base_snapshot_id), resolved_base)
            }
        };
        resolved_by_id.insert(current_id, resolved_manifest.clone());
        chain.push(LoadedManifest {
            snapshot_id: current_id,
            base_snapshot_id,
            manifest: resolved_manifest,
        });
    }
    Ok(chain)
}

/// Load the resolved manifest for the given snapshot id.
pub(crate) fn load_manifest_for_snapshot(
    file_manager: &Arc<FileManager>,
    snapshot_id: u64,
) -> Result<ManifestSnapshot> {
    load_manifest_chain(file_manager, snapshot_id)?
        .into_iter()
        .last()
        .map(|entry| entry.manifest)
        .ok_or_else(|| Error::IoError(format!("Snapshot {} not found", snapshot_id)))
}

pub(crate) fn apply_manifest_tree_level_edits(
    tree_levels: &mut [Vec<ManifestLevel>],
    edits: &[ManifestTreeLevelEdit],
) -> Result<()> {
    for tree_edit in edits {
        if tree_edit.tree_idx >= tree_levels.len() {
            return Err(Error::IoError(format!(
                "Invalid tree index {} for {} trees",
                tree_edit.tree_idx,
                tree_levels.len()
            )));
        }
        let levels = tree_levels
            .get_mut(tree_edit.tree_idx)
            .expect("tree index validated");
        manifest_levels_apply_edits(levels, &tree_edit.level_edits)?;
    }
    Ok(())
}

fn manifest_levels_apply_edits(
    levels: &mut Vec<ManifestLevel>,
    edits: &[ManifestLevelEdit],
) -> Result<()> {
    for edit in edits {
        let level_pos = levels.iter().position(|level| level.ordinal == edit.level);
        let level = if let Some(level_pos) = level_pos {
            &mut levels[level_pos]
        } else {
            levels.push(ManifestLevel {
                ordinal: edit.level,
                tiered: edit.tiered,
                files: Vec::new(),
            });
            levels.last_mut().expect("level inserted")
        };
        for removed_file_id in &edit.removed_file_ids {
            level.files.retain(|file| file.file_id != *removed_file_id);
        }
        if !level.tiered && !edit.removed_file_ids.is_empty() {
            return Err(Error::IoError(format!(
                "Non-tiered incremental edits with removals are not supported for level {}",
                level.ordinal
            )));
        }
        level.files.extend(edit.new_files.clone());
    }
    Ok(())
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
        let hi = hex_value(bytes[idx])?;
        let lo = hex_value(bytes[idx + 1])?;
        out.push((hi << 4) | lo);
        idx += 2;
    }
    Ok(out)
}

fn hex_value(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(10 + (byte - b'a')),
        b'A'..=b'F' => Ok(10 + (byte - b'A')),
        _ => Err(Error::IoError(format!(
            "Invalid hex character: {}",
            byte as char
        ))),
    }
}

pub(crate) fn snapshot_manifest_name(id: u64) -> String {
    crate::paths::snapshot_manifest_relative_path(id)
}

/// Encode a snapshot manifest as JSON.
pub(crate) fn encode_manifest<W: SequentialWriteFile>(
    writer: &mut BufferedWriter<W>,
    snapshot: &DbSnapshot,
    base_snapshot: Option<&DbSnapshot>,
    file_manager: &FileManager,
) -> Result<Option<u64>> {
    let full_manifest = || {
        ManifestPayload::Snapshot(ManifestSnapshot {
            id: snapshot.id,
            seq_id: snapshot.seq_id,
            latest_schema_id: snapshot.latest_schema_id,
            bucket_ranges: snapshot.bucket_ranges.clone(),
            lsm_tree_bucket_ranges: snapshot.lsm_tree_bucket_ranges.clone(),
            tree_levels: manifest_tree_levels_from_snapshot(&snapshot.lsm_versions, file_manager),
            vlog_files: manifest_vlog_files_from_snapshot(snapshot, file_manager),
            active_memtable_data: snapshot.active_memtable_data.clone(),
        })
    };
    let mut incremental_base_id = None;
    let manifest = if let Some(base) = base_snapshot {
        if let Some(tree_level_edits) =
            build_incremental_tree_level_edits(base, snapshot, file_manager)
        {
            incremental_base_id = Some(base.id);
            ManifestPayload::IncrementalSnapshot(ManifestIncrementalSnapshot {
                id: snapshot.id,
                seq_id: snapshot.seq_id,
                base_snapshot_id: base.id,
                latest_schema_id: snapshot.latest_schema_id,
                bucket_ranges: snapshot.bucket_ranges.clone(),
                lsm_tree_bucket_ranges: snapshot.lsm_tree_bucket_ranges.clone(),
                tree_level_edits,
                vlog_files: manifest_vlog_files_from_snapshot(snapshot, file_manager),
                active_memtable_data: snapshot.active_memtable_data.clone(),
            })
        } else {
            full_manifest()
        }
    } else {
        full_manifest()
    };
    let json = serde_json::to_vec(&manifest)
        .map_err(|err| Error::IoError(format!("Failed to encode manifest: {}", err)))?;
    writer.write(&json)?;
    Ok(incremental_base_id)
}

fn manifest_tree_levels_from_snapshot(
    lsm_versions: &[LSMTreeVersion],
    file_manager: &FileManager,
) -> Vec<Vec<ManifestLevel>> {
    lsm_versions
        .iter()
        .map(|version| {
            version
                .levels
                .iter()
                .map(|level| ManifestLevel {
                    ordinal: level.ordinal,
                    tiered: level.tiered,
                    files: level
                        .files
                        .iter()
                        .map(|file| manifest_file_from_data_file(file, file_manager))
                        .collect(),
                })
                .collect()
        })
        .collect()
}

fn manifest_file_from_data_file(file: &DataFile, file_manager: &FileManager) -> ManifestFile {
    let path_file_id = file.snapshot_data_file_id().unwrap_or(file.file_id);
    ManifestFile {
        file_id: file.file_id,
        file_type: file.file_type.as_str().to_string(),
        schema_id: file.schema_id,
        size: file.size,
        start_key: to_hex(&file.start_key),
        end_key: to_hex(&file.end_key),
        path: file_manager
            .get_data_file_full_path(path_file_id)
            .expect("Unknown file ID"),
        has_separated_values: file.has_separated_values,
        bucket_range_start: *file.bucket_range.start(),
        bucket_range_end: *file.bucket_range.end(),
        effective_bucket_range_start: *file.effective_bucket_range.start(),
        effective_bucket_range_end: *file.effective_bucket_range.end(),
        vlog_file_seq_offset: file.vlog_file_seq_offset,
    }
}

fn manifest_vlog_files_from_snapshot(
    snapshot: &DbSnapshot,
    file_manager: &FileManager,
) -> Vec<ManifestVlogFile> {
    snapshot
        .vlog_version
        .files_with_entries()
        .into_iter()
        .map(|(file_seq, tracked_id, valid_entries)| ManifestVlogFile {
            file_seq,
            file_id: tracked_id.file_id(),
            path: file_manager
                .get_data_file_full_path(tracked_id.file_id())
                .expect("Unknown file ID"),
            valid_entries,
        })
        .collect()
}

/// Attempt to build incremental level edits from the base snapshot to the current snapshot.
/// Returns None if incremental edits cannot fully capture the changes (e.g. due to file removals
/// or complex tiered level changes), in which case a full snapshot manifest should be written instead.
fn build_incremental_level_edits(
    base_levels: &[Level],
    snapshot_levels: &[Level],
    file_manager: &FileManager,
) -> Option<Vec<ManifestLevelEdit>> {
    let mut edits = Vec::new();
    for level in snapshot_levels {
        let base_level = base_levels
            .iter()
            .find(|base_level| base_level.ordinal == level.ordinal)?;
        let base_file_ids: HashSet<u64> =
            base_level.files.iter().map(|file| file.file_id).collect();
        let removed: Vec<u64> = base_level
            .files
            .iter()
            .filter(|file| {
                !level
                    .files
                    .iter()
                    .any(|current| current.file_id == file.file_id)
            })
            .map(|file| file.file_id)
            .collect();
        if !removed.is_empty() {
            return None;
        }
        let new_files: Vec<ManifestFile> = level
            .files
            .iter()
            .filter(|file| !base_file_ids.contains(&file.file_id))
            .map(|file| manifest_file_from_data_file(file, file_manager))
            .collect();
        if !new_files.is_empty() {
            if !level.tiered || level.ordinal != 0 || new_files.len() != 1 || !edits.is_empty() {
                return None;
            }
            edits.push(ManifestLevelEdit {
                level: level.ordinal,
                tiered: level.tiered,
                removed_file_ids: Vec::new(),
                new_files,
            });
        }
    }
    for base_level in base_levels {
        if !snapshot_levels
            .iter()
            .any(|level| level.ordinal == base_level.ordinal)
            && !base_level.files.is_empty()
        {
            return None;
        }
    }
    Some(edits)
}

fn build_incremental_tree_level_edits(
    base: &DbSnapshot,
    snapshot: &DbSnapshot,
    file_manager: &FileManager,
) -> Option<Vec<ManifestTreeLevelEdit>> {
    if base.lsm_versions.len() != snapshot.lsm_versions.len() {
        return None;
    }
    let mut tree_edits = Vec::new();
    for (tree_idx, tree_version) in snapshot.lsm_versions.iter().enumerate() {
        let base_tree = base.lsm_versions.get(tree_idx)?;
        let level_edits =
            build_incremental_level_edits(&base_tree.levels, &tree_version.levels, file_manager)?;
        if !level_edits.is_empty() {
            tree_edits.push(ManifestTreeLevelEdit {
                tree_idx,
                level_edits,
            });
        }
    }
    if tree_edits.is_empty() {
        None
    } else {
        Some(tree_edits)
    }
}

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{:02x}", b);
    }
    out
}

/// Extract the file ID and path references for all data files in the manifest, deduplicating by file ID.
pub(crate) fn manifest_data_file_refs(
    manifest: &ManifestSnapshot,
) -> impl Iterator<Item = (u64, String)> {
    let mut refs: BTreeMap<u64, String> = BTreeMap::new();
    for tree_levels in &manifest.tree_levels {
        for level in tree_levels {
            for file in &level.files {
                refs.entry(file.file_id)
                    .or_insert_with(|| file.path.clone());
            }
        }
    }
    for file in &manifest.vlog_files {
        refs.entry(file.file_id)
            .or_insert_with(|| file.path.clone());
    }
    refs.into_iter()
}

pub(crate) fn build_tree_versions_from_manifest_untracked(
    manifest: &ManifestSnapshot,
) -> Result<Vec<LSMTreeVersion>> {
    build_tree_versions_internal(manifest, |file, _ordinal| {
        let file_type = DataFileType::from_str(&file.file_type).map_err(Error::IoError)?;
        let start_key = from_hex(&file.start_key)?;
        let end_key = from_hex(&file.end_key)?;
        Ok(Arc::new(
            DataFile::new_detached(
                file_type,
                start_key,
                end_key,
                file.file_id,
                file.schema_id,
                file.size,
                file.bucket_range_start..=file.bucket_range_end,
                file.effective_bucket_range_start..=file.effective_bucket_range_end,
            )
            .with_vlog_offset(file.vlog_file_seq_offset)
            .with_separated_values(file.has_separated_values),
        ))
    })
}

pub(crate) fn build_vlog_version_from_manifest_untracked(
    manifest: &ManifestSnapshot,
) -> VlogVersion {
    let files = manifest
        .vlog_files
        .iter()
        .map(|file| {
            (
                file.file_seq,
                TrackedFileId::detached(file.file_id),
                file.valid_entries,
            )
        })
        .collect();
    VlogVersion::from_files_with_entries(files)
}

pub(crate) fn build_tree_versions_from_manifest(
    file_manager: &Arc<FileManager>,
    manifest: &ManifestSnapshot,
    read_only: bool,
) -> Result<Vec<LSMTreeVersion>> {
    build_tree_versions_internal(manifest, |file, ordinal| {
        let file_type = DataFileType::from_str(&file.file_type).map_err(Error::IoError)?;
        let start_key = from_hex(&file.start_key)?;
        let end_key = from_hex(&file.end_key)?;
        let tracked_id = if read_only {
            file_manager.register_data_file_readonly(file.file_id, &file.path)?;
            file_manager
                .set_data_file_priority(file.file_id, lsm_file_priority_for_level(ordinal))?;
            TrackedFileId::detached(file.file_id)
        } else {
            if !file_manager.has_data_file(file.file_id) {
                return Err(Error::IoError(format!(
                    "Restored file {} is not tracked by FileManager",
                    file.file_id
                )));
            }
            file_manager
                .set_data_file_priority(file.file_id, lsm_file_priority_for_level(ordinal))?;
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
        Ok(Arc::new(data_file))
    })
}

/// Shared tree-version builder: iterates manifest levels and delegates
/// per-file DataFile construction to the provided closure.
fn build_tree_versions_internal(
    manifest: &ManifestSnapshot,
    build_file: impl Fn(&ManifestFile, u8) -> Result<Arc<DataFile>>,
) -> Result<Vec<LSMTreeVersion>> {
    let mut tree_versions = Vec::with_capacity(manifest.tree_levels.len());
    for levels in &manifest.tree_levels {
        let mut out_levels = Vec::with_capacity(levels.len());
        for level in levels {
            let mut files = Vec::with_capacity(level.files.len());
            for file in &level.files {
                files.push(build_file(file, level.ordinal)?);
            }
            out_levels.push(Level {
                ordinal: level.ordinal,
                tiered: level.tiered,
                files,
            });
        }
        tree_versions.push(LSMTreeVersion { levels: out_levels });
    }
    Ok(tree_versions)
}

pub(crate) fn build_vlog_version_from_manifest(
    file_manager: &Arc<FileManager>,
    manifest: &ManifestSnapshot,
    read_only: bool,
) -> Result<VlogVersion> {
    let mut files = Vec::with_capacity(manifest.vlog_files.len());
    for vlog_file in &manifest.vlog_files {
        let tracked_id = if read_only {
            file_manager.register_data_file_readonly(vlog_file.file_id, &vlog_file.path)?;
            file_manager.set_data_file_priority(vlog_file.file_id, VLOG_FILE_PRIORITY)?;
            TrackedFileId::detached(vlog_file.file_id)
        } else {
            if !file_manager.has_data_file(vlog_file.file_id) {
                return Err(Error::IoError(format!(
                    "Restored VLOG file {} is not tracked by FileManager",
                    vlog_file.file_id
                )));
            }
            file_manager.set_data_file_priority(vlog_file.file_id, VLOG_FILE_PRIORITY)?;
            TrackedFileId::new(file_manager, vlog_file.file_id)
        };
        files.push((vlog_file.file_seq, tracked_id, vlog_file.valid_entries));
    }
    Ok(VlogVersion::from_files_with_entries(files))
}
