use super::{ActiveMemtableSnapshotData, DbSnapshot};
use crate::data_file::{DataFile, DataFileType};
use crate::db_state::{LSMTreeScope, TruncationCursorMap};
use crate::error::{Error, Result};
use crate::file::{
    BufferedWriter, FileManager, MetadataReader, SequentialWriteFile, TrackedFile, TrackedFileId,
};
use crate::lsm::{LSMTreeVersion, Level};
pub(crate) use crate::manifest_model::{
    ManifestFile, ManifestLevel, ManifestTruncationCursor, ManifestVlogFile,
};
use crate::manifest_model::{
    build_tree_versions_from_levels, build_truncation_cursors, build_vlog_version_from_files,
    manifest_file_from_data_file_with_origin, manifest_truncation_cursors,
};
pub(crate) use crate::manifest_model::{from_hex, to_hex};
use crate::paths::sibling_snapshot_manifest_path;
use crate::vlog::VlogVersion;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::Arc;

/// Snapshot manifests version 2 require SST row keys with big-endian bucket prefixes.
/// Version 3 adds per-file `max_expired_at`; version 4 adds replica origins; version 5 adds topology epoch.
pub(crate) const MANIFEST_VERSION_CURRENT: u32 = 5;

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ManifestSnapshot {
    pub(crate) version: u32,
    pub(crate) id: u64,
    pub(crate) seq_id: u64,
    #[serde(default)]
    pub(crate) topology_epoch: u64,
    pub(crate) latest_schema_id: u64,
    pub(crate) data_size_bytes: u64,
    pub(crate) incremental_data_size_bytes: u64,
    pub(crate) bucket_ranges: Vec<RangeInclusive<u16>>,
    pub(crate) lsm_tree_bucket_ranges: Vec<RangeInclusive<u16>>,
    pub(crate) tree_scopes: Vec<LSMTreeScope>,
    pub(crate) tree_levels: Vec<Vec<ManifestLevel>>,
    pub(crate) vlog_files: Vec<ManifestVlogFile>,
    pub(crate) active_memtable_data: Vec<ActiveMemtableSnapshotData>,
    #[serde(default)]
    pub(crate) truncation_cursors: Vec<ManifestTruncationCursor>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ManifestIncrementalSnapshot {
    pub(crate) version: u32,
    pub(crate) id: u64,
    pub(crate) seq_id: u64,
    #[serde(default)]
    pub(crate) topology_epoch: u64,
    pub(crate) base_snapshot_id: u64,
    pub(crate) latest_schema_id: u64,
    pub(crate) data_size_bytes: u64,
    pub(crate) incremental_data_size_bytes: u64,
    pub(crate) bucket_ranges: Vec<RangeInclusive<u16>>,
    pub(crate) lsm_tree_bucket_ranges: Vec<RangeInclusive<u16>>,
    pub(crate) tree_scopes: Vec<LSMTreeScope>,
    pub(crate) tree_level_edits: Vec<ManifestTreeLevelEdit>,
    // always include vlog file info in incremental manifests since vlog files are more likely to have changes
    pub(crate) vlog_files: Vec<ManifestVlogFile>,
    pub(crate) active_memtable_data: Vec<ActiveMemtableSnapshotData>,
    #[serde(default)]
    pub(crate) truncation_cursors: Vec<ManifestTruncationCursor>,
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

impl ManifestPayload {
    fn version(&self) -> u32 {
        match self {
            ManifestPayload::Snapshot(manifest) => manifest.version,
            ManifestPayload::IncrementalSnapshot(manifest) => manifest.version,
        }
    }
}

fn validate_manifest_version(version: u32) -> Result<()> {
    if !(2..=MANIFEST_VERSION_CURRENT).contains(&version) {
        return Err(Error::IoError(format!(
            "Unsupported snapshot manifest version: {version} (expected 2..={MANIFEST_VERSION_CURRENT})"
        )));
    }
    Ok(())
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ManifestLevelEdit {
    pub(crate) level: u8,
    pub(crate) tiered: bool,
    pub(crate) removed_file_ids: Vec<u64>,
    pub(crate) new_files: Vec<ManifestFile>,
}

pub(crate) fn decode_manifest(bytes: &[u8]) -> Result<ManifestPayload> {
    let payload: ManifestPayload = serde_json::from_slice(bytes)
        .map_err(|err| Error::IoError(format!("Failed to decode manifest: {}", err)))?;
    validate_manifest_version(payload.version())?;
    Ok(payload)
}

pub(crate) fn parse_snapshot_manifest_id(name: &str) -> Option<u64> {
    let name = name.rsplit('/').next().unwrap_or(name);
    name.strip_prefix("SNAPSHOT-")?.parse::<u64>().ok()
}

pub(crate) fn list_snapshot_manifest_ids(file_manager: &Arc<FileManager>) -> Result<Vec<u64>> {
    let mut snapshot_ids = Vec::new();
    for snapshot_id in file_manager
        .list_snapshot_metadata_names()?
        .into_iter()
        .filter_map(|name| parse_snapshot_manifest_id(&name))
    {
        let manifest_name = snapshot_manifest_name(snapshot_id);
        match read_manifest_payload(file_manager, &manifest_name) {
            Ok(_) => snapshot_ids.push(snapshot_id),
            Err(Error::ChecksumMismatch(_)) => {}
            Err(err) => return Err(err),
        }
    }
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
    let payload = read_manifest_payload(file_manager, &manifest_name)?;
    let (base_snapshot_id, manifest) = match decode_manifest(payload.as_ref())? {
        ManifestPayload::Snapshot(manifest) => (None, manifest),
        ManifestPayload::IncrementalSnapshot(incremental) => {
            let base_snapshot_id = Some(incremental.base_snapshot_id);
            let manifest = if let Some(base) = loaded_by_id.get(&incremental.base_snapshot_id) {
                let mut resolved = base.manifest.clone();
                apply_manifest_tree_level_edits(
                    &mut resolved.tree_levels,
                    &incremental.tree_level_edits,
                )?;
                resolved.version = incremental.version;
                resolved.topology_epoch = incremental.topology_epoch;
                resolved.vlog_files = incremental.vlog_files;
                resolved.id = incremental.id;
                resolved.seq_id = incremental.seq_id;
                resolved.latest_schema_id = incremental.latest_schema_id;
                resolved.data_size_bytes = incremental.data_size_bytes;
                resolved.incremental_data_size_bytes = incremental.incremental_data_size_bytes;
                resolved.active_memtable_data = incremental.active_memtable_data;
                resolved.bucket_ranges = incremental.bucket_ranges;
                resolved.lsm_tree_bucket_ranges = incremental.lsm_tree_bucket_ranges;
                resolved.tree_scopes = incremental.tree_scopes;
                resolved.truncation_cursors = incremental.truncation_cursors;
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
        let bytes = read_manifest_payload(file_manager, &manifest_name)?;
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
                resolved_base.version = manifest.version;
                resolved_base.topology_epoch = manifest.topology_epoch;
                resolved_base.vlog_files = manifest.vlog_files;
                resolved_base.id = manifest.id;
                resolved_base.seq_id = manifest.seq_id;
                resolved_base.latest_schema_id = manifest.latest_schema_id;
                resolved_base.data_size_bytes = manifest.data_size_bytes;
                resolved_base.incremental_data_size_bytes = manifest.incremental_data_size_bytes;
                resolved_base.active_memtable_data = manifest.active_memtable_data;
                resolved_base.bucket_ranges = manifest.bucket_ranges;
                resolved_base.lsm_tree_bucket_ranges = manifest.lsm_tree_bucket_ranges;
                resolved_base.tree_scopes = manifest.tree_scopes;
                resolved_base.truncation_cursors = manifest.truncation_cursors;
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

/// Load the manifest dependency chain starting from an explicit manifest path.
pub(crate) fn load_manifest_chain_from_path(
    file_manager: &Arc<FileManager>,
    manifest_path: &str,
) -> Result<Vec<LoadedManifest>> {
    let mut chain = Vec::new();
    let mut visited = HashSet::new();
    let mut raw_payloads = Vec::new();
    let mut next_path = Some(manifest_path.to_string());
    while let Some(current_path) = next_path {
        let bytes = read_manifest_payload_at_path(file_manager, &current_path)?;
        let payload = decode_manifest(bytes.as_ref())?;
        let current_id = match &payload {
            ManifestPayload::Snapshot(manifest) => manifest.id,
            ManifestPayload::IncrementalSnapshot(manifest) => manifest.id,
        };
        if !visited.insert(current_id) {
            return Err(Error::IoError(format!(
                "Snapshot manifest dependency cycle detected for {}",
                current_id
            )));
        }
        next_path = match &payload {
            ManifestPayload::Snapshot(_) => None,
            ManifestPayload::IncrementalSnapshot(manifest) => Some(sibling_snapshot_manifest_path(
                &current_path,
                manifest.base_snapshot_id,
            )?),
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
                resolved_base.version = manifest.version;
                resolved_base.topology_epoch = manifest.topology_epoch;
                resolved_base.vlog_files = manifest.vlog_files;
                resolved_base.id = manifest.id;
                resolved_base.seq_id = manifest.seq_id;
                resolved_base.latest_schema_id = manifest.latest_schema_id;
                resolved_base.active_memtable_data = manifest.active_memtable_data;
                resolved_base.bucket_ranges = manifest.bucket_ranges;
                resolved_base.lsm_tree_bucket_ranges = manifest.lsm_tree_bucket_ranges;
                resolved_base.tree_scopes = manifest.tree_scopes;
                resolved_base.truncation_cursors = manifest.truncation_cursors;
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

fn read_manifest_payload(file_manager: &Arc<FileManager>, manifest_name: &str) -> Result<Vec<u8>> {
    let reader = file_manager.open_metadata_file_reader_untracked(manifest_name)?;
    let bytes = MetadataReader::new(reader).read_all()?;
    Ok(bytes.to_vec())
}

fn read_manifest_payload_at_path(
    file_manager: &Arc<FileManager>,
    manifest_path: &str,
) -> Result<Vec<u8>> {
    let reader = file_manager.open_metadata_file_reader_at_path(manifest_path)?;
    let bytes = MetadataReader::new(reader).read_all()?;
    Ok(bytes.to_vec())
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

/// Load the resolved manifest from an explicit manifest path.
pub(crate) fn load_manifest_from_path(
    file_manager: &Arc<FileManager>,
    manifest_path: &str,
) -> Result<ManifestSnapshot> {
    load_manifest_chain_from_path(file_manager, manifest_path)?
        .into_iter()
        .last()
        .map(|entry| entry.manifest)
        .ok_or_else(|| Error::IoError(format!("Snapshot manifest not found: {}", manifest_path)))
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

pub(crate) fn build_truncation_cursors_from_manifest(
    manifest: &ManifestSnapshot,
) -> Result<TruncationCursorMap> {
    build_truncation_cursors(&manifest.truncation_cursors)
}

pub(crate) fn snapshot_manifest_name(id: u64) -> String {
    crate::paths::snapshot_manifest_relative_path(id)
}

/// Encode a snapshot manifest as JSON.
pub(crate) struct ManifestEncodeResult {
    pub(crate) incremental_base_id: Option<u64>,
    pub(crate) data_size_bytes: u64,
    pub(crate) incremental_data_size_bytes: u64,
    pub(crate) active_memtable_total_size_bytes: u64,
}

pub(crate) fn encode_manifest<W: SequentialWriteFile>(
    writer: &mut BufferedWriter<W>,
    snapshot: &DbSnapshot,
    base_snapshot: Option<&DbSnapshot>,
    _file_manager: &FileManager,
) -> Result<ManifestEncodeResult> {
    let base_file_paths = base_snapshot.map(snapshot_file_sizes).unwrap_or_default();
    let current_files = snapshot_file_sizes(snapshot);
    let file_data_size_bytes = current_files.values().copied().sum::<u64>();
    let current_active_memtable_bytes = snapshot
        .active_memtable_data
        .iter()
        .map(|segment| segment.end_offset.saturating_sub(segment.start_offset))
        .sum::<u64>();
    let incremental_data_size_bytes = if base_snapshot.is_some() {
        current_files
            .iter()
            .filter(|(path, _)| !base_file_paths.contains_key(*path))
            .map(|(_, size)| *size)
            .sum::<u64>()
            + current_active_memtable_bytes
    } else {
        file_data_size_bytes + current_active_memtable_bytes
    };

    let manifest = if let Some(base) = base_snapshot {
        if let Some(tree_level_edits) = build_incremental_tree_level_edits(base, snapshot) {
            let active_memtable_total_size_bytes =
                base.active_memtable_total_size_bytes + current_active_memtable_bytes;
            let data_size_bytes = file_data_size_bytes + active_memtable_total_size_bytes;
            ManifestPayload::IncrementalSnapshot(ManifestIncrementalSnapshot {
                version: MANIFEST_VERSION_CURRENT,
                id: snapshot.id,
                seq_id: snapshot.seq_id,
                topology_epoch: snapshot.topology_epoch,
                base_snapshot_id: base.id,
                latest_schema_id: snapshot.latest_schema_id,
                data_size_bytes,
                incremental_data_size_bytes,
                bucket_ranges: snapshot.bucket_ranges.clone(),
                lsm_tree_bucket_ranges: snapshot.lsm_tree_bucket_ranges.clone(),
                tree_scopes: snapshot.tree_scopes.clone(),
                tree_level_edits,
                vlog_files: manifest_vlog_files_from_snapshot(snapshot),
                active_memtable_data: snapshot.active_memtable_data.clone(),
                truncation_cursors: manifest_truncation_cursors_from_snapshot(snapshot),
            })
        } else {
            ManifestPayload::Snapshot(ManifestSnapshot {
                version: MANIFEST_VERSION_CURRENT,
                id: snapshot.id,
                seq_id: snapshot.seq_id,
                topology_epoch: snapshot.topology_epoch,
                latest_schema_id: snapshot.latest_schema_id,
                data_size_bytes: file_data_size_bytes + current_active_memtable_bytes,
                incremental_data_size_bytes,
                bucket_ranges: snapshot.bucket_ranges.clone(),
                lsm_tree_bucket_ranges: snapshot.lsm_tree_bucket_ranges.clone(),
                tree_scopes: snapshot.tree_scopes.clone(),
                tree_levels: manifest_tree_levels_from_snapshot(snapshot),
                vlog_files: manifest_vlog_files_from_snapshot(snapshot),
                active_memtable_data: snapshot.active_memtable_data.clone(),
                truncation_cursors: manifest_truncation_cursors_from_snapshot(snapshot),
            })
        }
    } else {
        ManifestPayload::Snapshot(ManifestSnapshot {
            version: MANIFEST_VERSION_CURRENT,
            id: snapshot.id,
            seq_id: snapshot.seq_id,
            topology_epoch: snapshot.topology_epoch,
            latest_schema_id: snapshot.latest_schema_id,
            data_size_bytes: file_data_size_bytes + current_active_memtable_bytes,
            incremental_data_size_bytes,
            bucket_ranges: snapshot.bucket_ranges.clone(),
            lsm_tree_bucket_ranges: snapshot.lsm_tree_bucket_ranges.clone(),
            tree_scopes: snapshot.tree_scopes.clone(),
            tree_levels: manifest_tree_levels_from_snapshot(snapshot),
            vlog_files: manifest_vlog_files_from_snapshot(snapshot),
            active_memtable_data: snapshot.active_memtable_data.clone(),
            truncation_cursors: manifest_truncation_cursors_from_snapshot(snapshot),
        })
    };
    let json = serde_json::to_vec(&manifest)
        .map_err(|err| Error::IoError(format!("Failed to encode manifest: {}", err)))?;
    writer.write(&json)?;
    let (incremental_base_id, data_size_bytes, active_memtable_total_size_bytes) = match &manifest {
        ManifestPayload::Snapshot(snapshot_manifest) => (
            None,
            snapshot_manifest.data_size_bytes,
            current_active_memtable_bytes,
        ),
        ManifestPayload::IncrementalSnapshot(incremental_manifest) => (
            Some(incremental_manifest.base_snapshot_id),
            incremental_manifest.data_size_bytes,
            base_snapshot
                .map(|base| base.active_memtable_total_size_bytes)
                .unwrap_or(0)
                + current_active_memtable_bytes,
        ),
    };
    Ok(ManifestEncodeResult {
        incremental_base_id,
        data_size_bytes,
        incremental_data_size_bytes,
        active_memtable_total_size_bytes,
    })
}

fn snapshot_file_sizes(snapshot: &DbSnapshot) -> BTreeMap<String, u64> {
    let mut sizes = BTreeMap::new();
    for tracked in snapshot.tracked_data_files.values() {
        sizes
            .entry(tracked.absolute_path())
            .or_insert_with(|| tracked.size_bytes());
    }
    sizes
}

fn manifest_tree_levels_from_snapshot(snapshot: &DbSnapshot) -> Vec<Vec<ManifestLevel>> {
    snapshot
        .lsm_versions
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
                        .map(|file| {
                            manifest_file_from_data_file(
                                file,
                                &snapshot.tracked_data_files,
                                &snapshot.replica_origins,
                            )
                        })
                        .collect(),
                })
                .collect()
        })
        .collect()
}

fn manifest_file_from_data_file(
    file: &DataFile,
    tracked_data_files: &BTreeMap<u64, Arc<TrackedFile>>,
    replica_origins: &BTreeMap<u64, crate::file::logical_file::ReplicaOrigin>,
) -> ManifestFile {
    let path = tracked_data_files
        .get(&file.file_id)
        .map(|tracked| tracked.absolute_path())
        .expect("Unknown file ID");
    manifest_file_from_data_file_with_origin(
        file,
        path,
        replica_origins
            .get(&file.file_id)
            .cloned()
            .unwrap_or_default(),
    )
}

fn manifest_vlog_files_from_snapshot(snapshot: &DbSnapshot) -> Vec<ManifestVlogFile> {
    snapshot
        .vlog_version
        .files_with_entries()
        .into_iter()
        .map(|(file_seq, tracked_id, valid_entries)| {
            let file_id = tracked_id.file_id();
            let tracked = snapshot
                .tracked_data_files
                .get(&file_id)
                .expect("Snapshot references an unknown value-log file");
            ManifestVlogFile {
                file_seq,
                file_id,
                path: tracked.absolute_path(),
                valid_entries,
                origin: snapshot
                    .replica_origins
                    .get(&file_id)
                    .cloned()
                    .unwrap_or_default(),
            }
        })
        .collect()
}

fn manifest_truncation_cursors_from_snapshot(
    snapshot: &DbSnapshot,
) -> Vec<ManifestTruncationCursor> {
    manifest_truncation_cursors(&snapshot.truncation_cursors)
}

/// Attempt to build incremental level edits from the base snapshot to the current snapshot.
/// Returns None if incremental edits cannot fully capture the changes (e.g. due to file removals
/// or complex tiered level changes), in which case a full snapshot manifest should be written instead.
fn build_incremental_level_edits(
    base_levels: &[Level],
    snapshot_levels: &[Level],
    tracked_data_files: &BTreeMap<u64, Arc<TrackedFile>>,
    replica_origins: &BTreeMap<u64, crate::file::logical_file::ReplicaOrigin>,
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
            .map(|file| manifest_file_from_data_file(file, tracked_data_files, replica_origins))
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
) -> Option<Vec<ManifestTreeLevelEdit>> {
    if base.topology_epoch != snapshot.topology_epoch
        || base.lsm_versions.len() != snapshot.lsm_versions.len()
    {
        return None;
    }
    let mut tree_edits = Vec::new();
    for (tree_idx, tree_version) in snapshot.lsm_versions.iter().enumerate() {
        let base_tree = base.lsm_versions.get(tree_idx)?;
        let level_edits = build_incremental_level_edits(
            &base_tree.levels,
            &tree_version.levels,
            &snapshot.tracked_data_files,
            &snapshot.replica_origins,
        )?;
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

/// Extract the file ID and path references for all data files in the manifest, deduplicating by file ID.
pub(crate) fn manifest_data_file_refs(
    manifest: &ManifestSnapshot,
) -> impl Iterator<Item = (u64, String, crate::file::logical_file::ReplicaOrigin)> {
    let mut refs: BTreeMap<u64, (String, crate::file::logical_file::ReplicaOrigin)> =
        BTreeMap::new();
    for tree_levels in &manifest.tree_levels {
        for level in tree_levels {
            for file in &level.files {
                refs.entry(file.file_id)
                    .or_insert_with(|| (file.path.clone(), file.origin.clone()));
            }
        }
    }
    for file in &manifest.vlog_files {
        refs.entry(file.file_id)
            .or_insert_with(|| (file.path.clone(), file.origin.clone()));
    }
    refs.into_iter()
        .map(|(file_id, (path, origin))| (file_id, path, origin))
}

pub(crate) fn build_tree_versions_from_manifest_untracked(
    manifest: &ManifestSnapshot,
) -> Result<Vec<LSMTreeVersion>> {
    build_tree_versions_internal(manifest, |file, _ordinal| {
        let file_type = DataFileType::from_str(&file.file_type).map_err(Error::IoError)?;
        let start_key = from_hex(&file.start_key)?;
        let end_key = from_hex(&file.end_key)?;
        Ok(Arc::new(
            DataFile::new_untracked(
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
                TrackedFileId::untracked(file.file_id),
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
    build_tree_versions_from_levels(file_manager, &manifest.tree_levels, read_only)
}

/// Build tree scopes from a manifest.
pub(crate) fn build_tree_scopes_from_manifest(manifest: &ManifestSnapshot) -> Vec<LSMTreeScope> {
    manifest.tree_scopes.clone()
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
    build_vlog_version_from_files(file_manager, &manifest.vlog_files, read_only)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_file::{DataFile, DataFileType};

    #[test]
    fn decode_manifest_requires_version() {
        let without_version = br#"{
            "id": 1,
            "seq_id": 2,
            "latest_schema_id": 3,
            "bucket_ranges": [],
            "lsm_tree_bucket_ranges": [],
            "tree_levels": [],
            "vlog_files": [],
            "active_memtable_data": []
        }"#;
        let err = match decode_manifest(without_version) {
            Ok(_) => panic!("expected missing version to be rejected"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("Failed to decode manifest"));
    }

    #[test]
    fn decode_manifest_rejects_future_version() {
        let future = format!(
            r#"{{
                "version": {},
                "id": 1,
                "seq_id": 2,
                "latest_schema_id": 3,
                "data_size_bytes": 0,
                "incremental_data_size_bytes": 0,
                "bucket_ranges": [],
                "lsm_tree_bucket_ranges": [],
                "tree_scopes": [],
                "tree_levels": [],
                "vlog_files": [],
                "active_memtable_data": []
            }}"#,
            MANIFEST_VERSION_CURRENT + 1
        );
        let err = match decode_manifest(future.as_bytes()) {
            Ok(_) => panic!("expected future manifest version to be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("Unsupported snapshot manifest version")
        );
    }

    #[test]
    fn decode_manifest_v2_backward_compatible_defaults_max_expired_at() {
        // A version-2 manifest with a file that has no max_expired_at field.
        // serde(default) should fill in 0 (no expiration).
        let v2 = r#"{
            "version": 2,
            "id": 1,
            "seq_id": 2,
            "latest_schema_id": 3,
            "data_size_bytes": 0,
            "incremental_data_size_bytes": 0,
            "bucket_ranges": [],
            "lsm_tree_bucket_ranges": [],
            "tree_scopes": [],
            "tree_levels": [[{"ordinal": 1, "tiered": false, "files": [
                {"file_id": 10, "file_type": "sst", "schema_id": 1, "size": 100,
                 "start_key": "61", "end_key": "7a", "path": "data/10.sst",
                 "has_separated_values": false,
                 "bucket_range_start": 0, "bucket_range_end": 0,
                 "effective_bucket_range_start": 0, "effective_bucket_range_end": 0,
                 "vlog_file_seq_offset": 0}
            ]}]],
            "vlog_files": [],
            "active_memtable_data": []
        }"#;
        let payload = decode_manifest(v2.as_bytes()).expect("v2 manifest should decode");
        match payload {
            ManifestPayload::Snapshot(s) => {
                let file = &s.tree_levels[0][0].files[0];
                assert_eq!(file.max_expired_at, 0);
            }
            _ => panic!("expected snapshot payload"),
        }
    }

    #[test]
    fn decode_manifest_v3_to_v5_preserve_file_extensions() {
        let v3 = r#"{
            "version": 3,
            "id": 1,
            "seq_id": 2,
            "latest_schema_id": 3,
            "data_size_bytes": 0,
            "incremental_data_size_bytes": 0,
            "bucket_ranges": [],
            "lsm_tree_bucket_ranges": [],
            "tree_scopes": [],
            "tree_levels": [[{"ordinal": 1, "tiered": false, "files": [
                {"file_id": 10, "file_type": "sst", "schema_id": 1, "size": 100,
                 "start_key": "61", "end_key": "7a", "path": "data/10.sst",
                 "has_separated_values": false,
                 "bucket_range_start": 0, "bucket_range_end": 0,
                 "effective_bucket_range_start": 0, "effective_bucket_range_end": 0,
                 "vlog_file_seq_offset": 0,
                 "max_expired_at": 5000}
            ]}]],
            "vlog_files": [],
            "active_memtable_data": []
        }"#;
        let payload = decode_manifest(v3.as_bytes()).expect("v3 manifest should decode");
        match payload {
            ManifestPayload::Snapshot(s) => {
                let file = &s.tree_levels[0][0].files[0];
                assert_eq!(file.max_expired_at, 5000);
                assert_eq!(file.origin, crate::file::logical_file::ReplicaOrigin::Owned);
            }
            _ => panic!("expected snapshot payload"),
        }

        let v4 = v3.replace(
            "\"version\": 3",
            "\"version\": 4",
        ).replace(
            "\"max_expired_at\": 5000}",
            "\"max_expired_at\": 5000, \"origin\": {\"kind\": \"external_leased\", \"export_id\": \"runtime-export\"}}",
        );
        let payload = decode_manifest(v4.as_bytes()).expect("v4 manifest should decode");
        match payload {
            ManifestPayload::Snapshot(s) => assert!(matches!(
                s.tree_levels[0][0].files[0].origin,
                crate::file::logical_file::ReplicaOrigin::ExternalLeased { ref export_id }
                    if export_id == "runtime-export"
            )),
            _ => panic!("expected snapshot payload"),
        }

        let v5 = v4
            .replace("\"version\": 4", "\"version\": 5")
            .replace("\"seq_id\": 2,", "\"seq_id\": 2, \"topology_epoch\": 7,");
        let payload = decode_manifest(v5.as_bytes()).expect("v5 manifest should decode");
        match payload {
            ManifestPayload::Snapshot(s) => assert_eq!(s.topology_epoch, 7),
            _ => panic!("expected snapshot payload"),
        }
    }

    #[test]
    fn topology_epoch_change_forces_full_snapshot_manifest() {
        let file = Arc::new(DataFile::new_untracked(
            DataFileType::SSTable,
            b"a".to_vec(),
            b"b".to_vec(),
            1,
            0,
            1,
            0..=0,
            0..=0,
        ));
        let mut base = DbSnapshot::new(1, "snapshot/1", None);
        base.lsm_versions = vec![LSMTreeVersion {
            levels: vec![Level {
                ordinal: 0,
                tiered: true,
                files: Vec::new(),
            }],
        }];
        let mut next = base.clone();
        next.id = 2;
        next.topology_epoch = 1;
        next.lsm_versions[0].levels[0].files.push(file);

        assert!(build_incremental_tree_level_edits(&base, &next).is_none());
    }

    #[test]
    fn decode_manifest_rejects_previous_physical_key_format() {
        let previous = r#"{
            "version": 1,
            "id": 1,
            "seq_id": 2,
            "latest_schema_id": 3,
            "data_size_bytes": 0,
            "incremental_data_size_bytes": 0,
            "bucket_ranges": [],
            "lsm_tree_bucket_ranges": [],
            "tree_scopes": [],
            "tree_levels": [],
            "vlog_files": [],
            "active_memtable_data": []
        }"#;
        let err = match decode_manifest(previous.as_bytes()) {
            Ok(_) => panic!("version 1 must be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("Unsupported snapshot manifest version: 1 (expected 2..=5)")
        );
    }
}
