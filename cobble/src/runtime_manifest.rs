//! Durable runtime LSM manifests for external observers.
//!
//! Runtime manifests describe the writer's current persisted LSM layout. They
//! are intentionally separate from snapshot manifests: snapshots are recovery
//! points, while runtime manifests are a compact, append-only observation stream
//! for services such as a dedicated compactor.

use crate::db_state::LSMTreeScope;
use crate::error::{Error, Result};
use crate::file::{File, FileManager, MetadataReader, SequentialWriteFile};
use crate::manifest_model::{
    ManifestFile, ManifestLevel, ManifestTruncationCursor, ManifestVlogFile,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::sync::Arc;

pub(crate) mod publisher;

/// Runtime manifests version 2 describe SSTs with big-endian bucket prefixes.
/// Version 3 adds per-file `max_expired_at`; version 4 adds replica origins and topology epochs.
pub(crate) const RUNTIME_MANIFEST_VERSION_CURRENT: u32 = 4;
pub(crate) const MAX_RUNTIME_MANIFEST_CHAIN_DEPTH: usize = 64;
const RUNTIME_MANIFEST_DIR: &str = "runtime";
const RUNTIME_CURRENT_NAME: &str = "runtime/CURRENT";
const RUNTIME_MANIFEST_PREFIX: &str = "MANIFEST-";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RuntimeManifest {
    pub(crate) generation: u64,
    pub(crate) seq_id: u64,
    #[serde(default)]
    pub(crate) topology_epoch: u64,
    pub(crate) latest_schema_id: u64,
    pub(crate) bucket_ranges: Vec<RangeInclusive<u16>>,
    pub(crate) lsm_tree_bucket_ranges: Vec<RangeInclusive<u16>>,
    pub(crate) tree_scopes: Vec<LSMTreeScope>,
    pub(crate) tree_levels: Vec<Vec<ManifestLevel>>,
    pub(crate) vlog_files: Vec<ManifestVlogFile>,
    pub(crate) truncation_cursors: Vec<ManifestTruncationCursor>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RuntimeIncrementalManifest {
    pub(crate) generation: u64,
    pub(crate) base_generation: u64,
    pub(crate) seq_id: u64,
    #[serde(default)]
    pub(crate) topology_epoch: u64,
    pub(crate) latest_schema_id: u64,
    pub(crate) tree_level_edits: Vec<RuntimeTreeLevelEdit>,
    pub(crate) vlog_files: Vec<ManifestVlogFile>,
    pub(crate) truncation_cursors: Vec<ManifestTruncationCursor>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RuntimeTreeLevelEdit {
    pub(crate) tree_idx: usize,
    pub(crate) level_edits: Vec<RuntimeLevelEdit>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RuntimeLevelEdit {
    pub(crate) level: u8,
    pub(crate) tiered: bool,
    pub(crate) removed_file_ids: Vec<u64>,
    pub(crate) added_files: Vec<ManifestFile>,
    /// Complete order after this edit. This makes removals and moves
    /// unambiguous for both tiered and non-tiered levels.
    pub(crate) resulting_file_ids: Vec<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub(crate) enum RuntimeManifestPayload {
    Full(RuntimeManifest),
    Incremental(RuntimeIncrementalManifest),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RuntimeManifestEnvelope {
    pub(crate) version: u32,
    pub(crate) manifest: RuntimeManifestPayload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LoadedRuntimeManifest {
    pub(crate) generation: u64,
    pub(crate) base_generation: Option<u64>,
    pub(crate) chain_depth: usize,
    pub(crate) manifest: RuntimeManifest,
}

impl RuntimeManifestEnvelope {
    pub(crate) fn full(manifest: RuntimeManifest) -> Self {
        Self {
            version: RUNTIME_MANIFEST_VERSION_CURRENT,
            manifest: RuntimeManifestPayload::Full(manifest),
        }
    }

    pub(super) fn generation(&self) -> u64 {
        match &self.manifest {
            RuntimeManifestPayload::Full(manifest) => manifest.generation,
            RuntimeManifestPayload::Incremental(manifest) => manifest.generation,
        }
    }
}

/// Builds a full or incremental runtime manifest for `current`.
///
/// Incrementals preserve the exact file order of every edited level. A full
/// manifest is chosen when topology changes, the chain is at its limit, or an
/// incremental encoding is not smaller than the complete state.
pub(crate) fn build_runtime_manifest(
    current: RuntimeManifest,
    base: Option<&LoadedRuntimeManifest>,
) -> Result<RuntimeManifestEnvelope> {
    validate_runtime_manifest(&current)?;
    let full = RuntimeManifestEnvelope::full(current.clone());
    let Some(base) = base else {
        return Ok(full);
    };
    validate_runtime_manifest(&base.manifest)?;
    if current.seq_id < base.manifest.seq_id
        || base.chain_depth >= MAX_RUNTIME_MANIFEST_CHAIN_DEPTH
        || !has_same_runtime_topology(&base.manifest, &current)
        || retained_file_descriptors_changed(&base.manifest, &current)?
    {
        return Ok(full);
    }

    let incremental = RuntimeIncrementalManifest {
        generation: current.generation,
        base_generation: base.generation,
        seq_id: current.seq_id,
        topology_epoch: current.topology_epoch,
        latest_schema_id: current.latest_schema_id,
        tree_level_edits: build_runtime_tree_level_edits(&base.manifest, &current)?,
        vlog_files: current.vlog_files.clone(),
        truncation_cursors: current.truncation_cursors.clone(),
    };
    validate_runtime_incremental(&base.manifest, &incremental)?;
    let incremental = RuntimeManifestEnvelope {
        version: RUNTIME_MANIFEST_VERSION_CURRENT,
        manifest: RuntimeManifestPayload::Incremental(incremental),
    };
    if encode_runtime_manifest(&incremental)?.len() >= encode_runtime_manifest(&full)?.len() {
        Ok(full)
    } else {
        Ok(incremental)
    }
}

pub(crate) fn encode_runtime_manifest(envelope: &RuntimeManifestEnvelope) -> Result<Vec<u8>> {
    validate_runtime_manifest_version(envelope.version)?;
    serde_json::to_vec(envelope)
        .map_err(|err| Error::IoError(format!("Failed to encode runtime manifest: {err}")))
}

pub(crate) fn decode_runtime_manifest(bytes: &[u8]) -> Result<RuntimeManifestEnvelope> {
    let envelope: RuntimeManifestEnvelope = serde_json::from_slice(bytes)
        .map_err(|err| Error::IoError(format!("Failed to decode runtime manifest: {err}")))?;
    validate_runtime_manifest_version(envelope.version)?;
    Ok(envelope)
}

fn validate_runtime_manifest_version(version: u32) -> Result<()> {
    if !(2..=RUNTIME_MANIFEST_VERSION_CURRENT).contains(&version) {
        return Err(Error::IoError(format!(
            "Unsupported runtime manifest version: {version} (expected 2..={RUNTIME_MANIFEST_VERSION_CURRENT})"
        )));
    }
    Ok(())
}

/// Runtime-manifest metadata I/O.
///
/// Both the generation file and `CURRENT` are written through
/// `FileManager::create_metadata_file`, which writes a checksum-protected temp
/// file and atomically renames it on close. The generation is published before
/// `CURRENT`, so readers never observe a pointer to a partial manifest.
pub(crate) struct RuntimeManifestStore {
    file_manager: Arc<FileManager>,
}

impl RuntimeManifestStore {
    pub(crate) fn new(file_manager: Arc<FileManager>) -> Self {
        Self { file_manager }
    }

    pub(crate) fn publish(&self, envelope: &RuntimeManifestEnvelope) -> Result<()> {
        self.file_manager
            .ensure_metadata_dir(RUNTIME_MANIFEST_DIR)?;
        let generation = envelope.generation();
        let current = self.current_generation()?;
        if let Some(current_generation) = current
            && generation <= current_generation
        {
            return Err(Error::InvalidState(format!(
                "Runtime manifest generation {generation} must be greater than current generation {current_generation}"
            )));
        }
        match &envelope.manifest {
            RuntimeManifestPayload::Incremental(incremental) => {
                if current != Some(incremental.base_generation) {
                    return Err(Error::InvalidState(format!(
                        "Runtime incremental manifest {} must extend current generation {:?}, not {}",
                        incremental.generation, current, incremental.base_generation
                    )));
                }
                let base = self.load_generation(incremental.base_generation)?;
                validate_runtime_incremental(&base.manifest, incremental)?;
            }
            RuntimeManifestPayload::Full(manifest) => validate_runtime_manifest(manifest)?,
        }

        let manifest_name = runtime_manifest_name(generation);
        if self
            .file_manager
            .metadata_file_exists_untracked(&manifest_name)?
        {
            return Err(Error::InvalidState(format!(
                "Runtime manifest generation {generation} already exists"
            )));
        }
        write_metadata_file(
            &self.file_manager,
            &manifest_name,
            &encode_runtime_manifest(envelope)?,
        )?;
        write_metadata_file(
            &self.file_manager,
            RUNTIME_CURRENT_NAME,
            format!("{generation}\n").as_bytes(),
        )
    }

    /// Returns a generation that has never been used in this runtime-manifest namespace.
    ///
    /// A generation file is durable before `CURRENT` advances. If publication is interrupted
    /// between those writes, the generation is intentionally orphaned rather than overwritten
    /// by a retry. Runtime manifests remain append-only until a reader-lease protocol can make
    /// cross-process garbage collection provably safe.
    pub(crate) fn allocate_next_generation(&self) -> Result<u64> {
        self.file_manager
            .ensure_metadata_dir(RUNTIME_MANIFEST_DIR)?;
        let mut highest_generation = self.current_generation()?.unwrap_or(0);
        for name in self
            .file_manager
            .list_metadata_names(RUNTIME_MANIFEST_DIR)?
        {
            if let Some(generation) = parse_runtime_manifest_generation(&name) {
                highest_generation = highest_generation.max(generation);
            }
        }

        let mut generation = highest_generation.checked_add(1).ok_or_else(|| {
            Error::InvalidState("Runtime manifest generation space is exhausted".to_string())
        })?;
        while self
            .file_manager
            .metadata_file_exists_untracked(&runtime_manifest_name(generation))?
        {
            generation = generation.checked_add(1).ok_or_else(|| {
                Error::InvalidState("Runtime manifest generation space is exhausted".to_string())
            })?;
        }
        Ok(generation)
    }

    pub(crate) fn load_current(&self) -> Result<Option<LoadedRuntimeManifest>> {
        self.current_generation()?
            .map(|generation| self.load_generation(generation))
            .transpose()
    }

    pub(crate) fn load_generation(&self, generation: u64) -> Result<LoadedRuntimeManifest> {
        self.load_chain(generation)?
            .into_iter()
            .last()
            .ok_or_else(|| Error::IoError(format!("Runtime manifest {generation} not found")))
    }

    pub(crate) fn load_chain(&self, generation: u64) -> Result<Vec<LoadedRuntimeManifest>> {
        let mut raw_payloads = Vec::new();
        let mut visited = HashSet::new();
        let mut next_generation = Some(generation);
        while let Some(current_generation) = next_generation {
            if raw_payloads.len() >= MAX_RUNTIME_MANIFEST_CHAIN_DEPTH {
                return Err(Error::InvalidState(format!(
                    "Runtime manifest chain exceeds maximum depth of {MAX_RUNTIME_MANIFEST_CHAIN_DEPTH}"
                )));
            }
            if !visited.insert(current_generation) {
                return Err(Error::InvalidState(format!(
                    "Runtime manifest dependency cycle detected at generation {current_generation}"
                )));
            }
            let envelope = self.read_generation(current_generation)?;
            if envelope.generation() != current_generation {
                return Err(Error::InvalidState(format!(
                    "Runtime manifest file for generation {current_generation} contains generation {}",
                    envelope.generation()
                )));
            }
            next_generation = match &envelope.manifest {
                RuntimeManifestPayload::Full(_) => None,
                RuntimeManifestPayload::Incremental(manifest) => Some(manifest.base_generation),
            };
            raw_payloads.push((current_generation, envelope));
        }
        raw_payloads.reverse();

        let mut resolved_by_generation = HashMap::new();
        let mut chain = Vec::new();
        for (generation, envelope) in raw_payloads {
            let (base_generation, manifest) = match envelope.manifest {
                RuntimeManifestPayload::Full(manifest) => {
                    validate_runtime_manifest(&manifest)?;
                    (None, manifest)
                }
                RuntimeManifestPayload::Incremental(incremental) => {
                    let base = resolved_by_generation
                        .get(&incremental.base_generation)
                        .ok_or_else(|| {
                            Error::InvalidState(format!(
                                "Missing base runtime manifest {} for generation {generation}",
                                incremental.base_generation
                            ))
                        })?;
                    let manifest = apply_runtime_incremental(base, &incremental)?;
                    (Some(incremental.base_generation), manifest)
                }
            };
            resolved_by_generation.insert(generation, manifest.clone());
            chain.push(LoadedRuntimeManifest {
                generation,
                base_generation,
                chain_depth: chain.len() + 1,
                manifest,
            });
        }
        Ok(chain)
    }

    fn current_generation(&self) -> Result<Option<u64>> {
        if !self
            .file_manager
            .metadata_file_exists_untracked(RUNTIME_CURRENT_NAME)?
        {
            return Ok(None);
        }
        let bytes = read_metadata_file(&self.file_manager, RUNTIME_CURRENT_NAME)?;
        let text = std::str::from_utf8(&bytes).map_err(|err| {
            Error::InvalidState(format!("Runtime CURRENT is not valid UTF-8: {err}"))
        })?;
        let generation = text.trim().parse::<u64>().map_err(|err| {
            Error::InvalidState(format!("Runtime CURRENT is not a generation number: {err}"))
        })?;
        Ok(Some(generation))
    }

    fn read_generation(&self, generation: u64) -> Result<RuntimeManifestEnvelope> {
        let name = runtime_manifest_name(generation);
        if !self.file_manager.metadata_file_exists_untracked(&name)? {
            return Err(Error::InvalidState(format!(
                "Missing runtime manifest generation {generation}"
            )));
        }
        decode_runtime_manifest(&read_metadata_file(&self.file_manager, &name)?)
    }
}

fn runtime_manifest_name(generation: u64) -> String {
    format!("{RUNTIME_MANIFEST_DIR}/{RUNTIME_MANIFEST_PREFIX}{generation}")
}

fn parse_runtime_manifest_generation(name: &str) -> Option<u64> {
    name.rsplit('/')
        .next()
        .and_then(|name| name.strip_prefix(RUNTIME_MANIFEST_PREFIX))
        .and_then(|generation| generation.parse().ok())
}

fn write_metadata_file(file_manager: &FileManager, name: &str, bytes: &[u8]) -> Result<()> {
    let mut writer = file_manager.create_metadata_file(name)?;
    writer.write(bytes)?;
    writer.close()
}

fn read_metadata_file(file_manager: &FileManager, name: &str) -> Result<Vec<u8>> {
    let reader = file_manager.open_metadata_file_reader_untracked(name)?;
    Ok(MetadataReader::new(reader).read_all()?.to_vec())
}

fn has_same_runtime_topology(base: &RuntimeManifest, current: &RuntimeManifest) -> bool {
    base.topology_epoch == current.topology_epoch
        && base.bucket_ranges == current.bucket_ranges
        && base.lsm_tree_bucket_ranges == current.lsm_tree_bucket_ranges
        && base.tree_scopes == current.tree_scopes
        && base.tree_levels.len() == current.tree_levels.len()
        && base
            .tree_levels
            .iter()
            .zip(&current.tree_levels)
            .all(|(base_levels, current_levels)| {
                base_levels.len() == current_levels.len()
                    && base_levels
                        .iter()
                        .zip(current_levels)
                        .all(|(base, current)| {
                            base.ordinal == current.ordinal && base.tiered == current.tiered
                        })
            })
}

fn build_runtime_tree_level_edits(
    base: &RuntimeManifest,
    current: &RuntimeManifest,
) -> Result<Vec<RuntimeTreeLevelEdit>> {
    let mut tree_edits = Vec::new();
    for (tree_idx, (base_levels, current_levels)) in base
        .tree_levels
        .iter()
        .zip(&current.tree_levels)
        .enumerate()
    {
        let mut level_edits = Vec::new();
        for (base_level, current_level) in base_levels.iter().zip(current_levels) {
            let base_ids = file_ids(&base_level.files)?;
            let current_ids = file_ids(&current_level.files)?;
            if base_ids == current_ids && base_level.files == current_level.files {
                continue;
            }
            let removed_file_ids = base_ids
                .iter()
                .filter(|file_id| !current_ids.contains(file_id))
                .copied()
                .collect();
            let added_files = current_level
                .files
                .iter()
                .filter(|file| !base_ids.contains(&file.file_id))
                .cloned()
                .collect();
            level_edits.push(RuntimeLevelEdit {
                level: current_level.ordinal,
                tiered: current_level.tiered,
                removed_file_ids,
                added_files,
                resulting_file_ids: current_level
                    .files
                    .iter()
                    .map(|file| file.file_id)
                    .collect(),
            });
        }
        if !level_edits.is_empty() {
            tree_edits.push(RuntimeTreeLevelEdit {
                tree_idx,
                level_edits,
            });
        }
    }
    Ok(tree_edits)
}

fn retained_file_descriptors_changed(
    base: &RuntimeManifest,
    current: &RuntimeManifest,
) -> Result<bool> {
    let base_files = manifest_file_descriptors(base)?;
    for (file_id, file) in manifest_file_descriptors(current)? {
        if let Some(base_file) = base_files.get(&file_id)
            && base_file != &file
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn apply_runtime_incremental(
    base: &RuntimeManifest,
    incremental: &RuntimeIncrementalManifest,
) -> Result<RuntimeManifest> {
    validate_runtime_incremental(base, incremental)?;
    let mut tree_levels = base.tree_levels.clone();
    for tree_edit in &incremental.tree_level_edits {
        let levels = tree_levels.get_mut(tree_edit.tree_idx).ok_or_else(|| {
            Error::InvalidState(format!(
                "Runtime manifest edit references missing tree {}",
                tree_edit.tree_idx
            ))
        })?;
        for edit in &tree_edit.level_edits {
            let level = levels
                .iter_mut()
                .find(|level| level.ordinal == edit.level)
                .ok_or_else(|| {
                    Error::InvalidState(format!(
                        "Runtime manifest edit references missing level {} in tree {}",
                        edit.level, tree_edit.tree_idx
                    ))
                })?;
            if level.tiered != edit.tiered {
                return Err(Error::InvalidState(format!(
                    "Runtime manifest edit changes tiered layout for tree {} level {}",
                    tree_edit.tree_idx, edit.level
                )));
            }
            apply_runtime_level_edit(level, edit)?;
        }
    }
    let manifest = RuntimeManifest {
        generation: incremental.generation,
        seq_id: incremental.seq_id,
        topology_epoch: incremental.topology_epoch,
        latest_schema_id: incremental.latest_schema_id,
        bucket_ranges: base.bucket_ranges.clone(),
        lsm_tree_bucket_ranges: base.lsm_tree_bucket_ranges.clone(),
        tree_scopes: base.tree_scopes.clone(),
        tree_levels,
        vlog_files: incremental.vlog_files.clone(),
        truncation_cursors: incremental.truncation_cursors.clone(),
    };
    validate_runtime_manifest(&manifest)?;
    Ok(manifest)
}

fn validate_runtime_incremental(
    base: &RuntimeManifest,
    incremental: &RuntimeIncrementalManifest,
) -> Result<()> {
    if base.generation != incremental.base_generation {
        return Err(Error::InvalidState(format!(
            "Runtime incremental manifest expects base generation {}, but received {}",
            incremental.base_generation, base.generation
        )));
    }
    if incremental.generation <= incremental.base_generation {
        return Err(Error::InvalidState(format!(
            "Runtime manifest generation {} must be greater than base generation {}",
            incremental.generation, incremental.base_generation
        )));
    }
    if incremental.seq_id < base.seq_id {
        return Err(Error::InvalidState(format!(
            "Runtime incremental manifest sequence {} cannot precede base sequence {}",
            incremental.seq_id, base.seq_id
        )));
    }
    if incremental.topology_epoch != base.topology_epoch {
        return Err(Error::InvalidState(format!(
            "Runtime incremental manifest topology epoch {} does not match base {}",
            incremental.topology_epoch, base.topology_epoch
        )));
    }
    let base_files = manifest_file_descriptors(base)?;
    let mut edited_trees = HashSet::new();
    for tree_edit in &incremental.tree_level_edits {
        if !edited_trees.insert(tree_edit.tree_idx) {
            return Err(Error::InvalidState(format!(
                "Runtime manifest contains duplicate edits for tree {}",
                tree_edit.tree_idx
            )));
        }
        let levels = base.tree_levels.get(tree_edit.tree_idx).ok_or_else(|| {
            Error::InvalidState(format!(
                "Runtime manifest edit references missing tree {}",
                tree_edit.tree_idx
            ))
        })?;
        let mut edited_levels = HashSet::new();
        for edit in &tree_edit.level_edits {
            if !edited_levels.insert(edit.level) {
                return Err(Error::InvalidState(format!(
                    "Runtime manifest contains duplicate edits for tree {} level {}",
                    tree_edit.tree_idx, edit.level
                )));
            }
            let level = levels
                .iter()
                .find(|level| level.ordinal == edit.level)
                .ok_or_else(|| {
                    Error::InvalidState(format!(
                        "Runtime manifest edit references missing level {} in tree {}",
                        edit.level, tree_edit.tree_idx
                    ))
                })?;
            validate_runtime_level_edit(level, edit)?;
            for file in &edit.added_files {
                if let Some(base_file) = base_files.get(&file.file_id)
                    && base_file != file
                {
                    return Err(Error::InvalidState(format!(
                        "Runtime manifest changes descriptor for existing file id {}",
                        file.file_id
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_runtime_level_edit(level: &ManifestLevel, edit: &RuntimeLevelEdit) -> Result<()> {
    if level.tiered != edit.tiered {
        return Err(Error::InvalidState(format!(
            "Runtime manifest edit changes tiered layout for level {}",
            edit.level
        )));
    }
    let existing = file_map(&level.files)?;
    let removed: HashSet<_> = edit.removed_file_ids.iter().copied().collect();
    if removed.len() != edit.removed_file_ids.len()
        || !removed.iter().all(|file_id| existing.contains_key(file_id))
    {
        return Err(Error::InvalidState(format!(
            "Runtime manifest edit removes an unknown or duplicate file from level {}",
            edit.level
        )));
    }
    let added = file_map(&edit.added_files)?;
    if added.keys().any(|file_id| existing.contains_key(file_id)) {
        return Err(Error::InvalidState(format!(
            "Runtime manifest edit adds an existing file to level {}",
            edit.level
        )));
    }
    let expected: HashSet<_> = existing
        .keys()
        .filter(|file_id| !removed.contains(file_id))
        .chain(added.keys())
        .copied()
        .collect();
    let resulting: HashSet<_> = edit.resulting_file_ids.iter().copied().collect();
    if resulting.len() != edit.resulting_file_ids.len() || resulting != expected {
        return Err(Error::InvalidState(format!(
            "Runtime manifest edit has an invalid resulting file order for level {}",
            edit.level
        )));
    }
    Ok(())
}

fn apply_runtime_level_edit(level: &mut ManifestLevel, edit: &RuntimeLevelEdit) -> Result<()> {
    validate_runtime_level_edit(level, edit)?;
    let removed: HashSet<_> = edit.removed_file_ids.iter().copied().collect();
    let mut files = file_map(&level.files)?;
    files.retain(|file_id, _| !removed.contains(file_id));
    for file in &edit.added_files {
        files.insert(file.file_id, file.clone());
    }
    level.files = edit
        .resulting_file_ids
        .iter()
        .map(|file_id| {
            files.remove(file_id).ok_or_else(|| {
                Error::InvalidState(format!(
                    "Runtime manifest resulting order references missing file {file_id}"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(())
}

fn validate_runtime_manifest(manifest: &RuntimeManifest) -> Result<()> {
    if manifest.tree_levels.len() != manifest.tree_scopes.len() {
        return Err(Error::InvalidState(format!(
            "Runtime manifest has {} trees but {} tree scopes",
            manifest.tree_levels.len(),
            manifest.tree_scopes.len()
        )));
    }
    if manifest.tree_levels.len() != manifest.lsm_tree_bucket_ranges.len() {
        return Err(Error::InvalidState(format!(
            "Runtime manifest has {} trees but {} tree bucket ranges",
            manifest.tree_levels.len(),
            manifest.lsm_tree_bucket_ranges.len()
        )));
    }
    let mut file_descriptors = HashMap::new();
    for (tree_idx, levels) in manifest.tree_levels.iter().enumerate() {
        let mut ordinals = HashSet::new();
        for level in levels {
            if !ordinals.insert(level.ordinal) {
                return Err(Error::InvalidState(format!(
                    "Runtime manifest has duplicate level {} in tree {tree_idx}",
                    level.ordinal
                )));
            }
            let mut level_file_ids = HashSet::new();
            for file in &level.files {
                if !level_file_ids.insert(file.file_id) {
                    return Err(Error::InvalidState(format!(
                        "Runtime manifest has duplicate file id {} within tree {} level {}",
                        file.file_id, tree_idx, level.ordinal
                    )));
                }
                validate_or_insert_file_descriptor(&mut file_descriptors, file)?;
            }
        }
    }
    let mut vlog_file_ids = HashSet::new();
    for file in &manifest.vlog_files {
        if !vlog_file_ids.insert(file.file_id) || file_descriptors.contains_key(&file.file_id) {
            return Err(Error::InvalidState(format!(
                "Runtime manifest has duplicate file id {}",
                file.file_id
            )));
        }
    }
    Ok(())
}

fn manifest_file_descriptors(manifest: &RuntimeManifest) -> Result<HashMap<u64, ManifestFile>> {
    let mut descriptors = HashMap::new();
    for levels in &manifest.tree_levels {
        for level in levels {
            let mut level_file_ids = HashSet::new();
            for file in &level.files {
                if !level_file_ids.insert(file.file_id) {
                    return Err(Error::InvalidState(format!(
                        "Runtime manifest has duplicate file id {} within level {}",
                        file.file_id, level.ordinal
                    )));
                }
                validate_or_insert_file_descriptor(&mut descriptors, file)?;
            }
        }
    }
    Ok(descriptors)
}

fn validate_or_insert_file_descriptor(
    descriptors: &mut HashMap<u64, ManifestFile>,
    file: &ManifestFile,
) -> Result<()> {
    if let Some(existing) = descriptors.get(&file.file_id) {
        if existing != file {
            return Err(Error::InvalidState(format!(
                "Runtime manifest has conflicting descriptors for file id {}",
                file.file_id
            )));
        }
    } else {
        descriptors.insert(file.file_id, file.clone());
    }
    Ok(())
}

fn file_ids(files: &[ManifestFile]) -> Result<HashSet<u64>> {
    Ok(file_map(files)?.into_keys().collect())
}

fn file_map(files: &[ManifestFile]) -> Result<HashMap<u64, ManifestFile>> {
    let mut out = HashMap::new();
    for file in files {
        if out.insert(file.file_id, file.clone()).is_some() {
            return Err(Error::InvalidState(format!(
                "Runtime manifest contains duplicate file id {} within one level",
                file.file_id
            )));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FileSystemRegistry;
    use crate::metrics_manager::MetricsManager;
    use tempfile::TempDir;

    fn file(id: u64) -> ManifestFile {
        ManifestFile {
            file_id: id,
            file_type: "sst".to_string(),
            schema_id: 1,
            size: 1,
            start_key: format!("{id:02x}"),
            end_key: format!("{:02x}", id + 1),
            path: format!("data/{id}.sst"),
            has_separated_values: false,
            bucket_range_start: 0,
            bucket_range_end: 0,
            effective_bucket_range_start: 0,
            effective_bucket_range_end: 0,
            vlog_file_seq_offset: 0,
            max_expired_at: 0,
            origin: crate::file::logical_file::ReplicaOrigin::Owned,
        }
    }

    fn manifest(generation: u64, levels: Vec<Vec<ManifestLevel>>) -> RuntimeManifest {
        let tree_count = levels.len();
        RuntimeManifest {
            generation,
            seq_id: generation,
            topology_epoch: 0,
            latest_schema_id: 1,
            bucket_ranges: vec![0..=0],
            lsm_tree_bucket_ranges: vec![0..=0; tree_count],
            tree_scopes: (0..tree_count)
                .map(|_| LSMTreeScope::new(0..=0, 0))
                .collect(),
            tree_levels: levels,
            vlog_files: Vec::new(),
            truncation_cursors: Vec::new(),
        }
    }

    fn levels(l0: &[u64], l1: &[u64]) -> Vec<ManifestLevel> {
        vec![
            ManifestLevel {
                ordinal: 0,
                tiered: true,
                files: l0.iter().copied().map(file).collect(),
            },
            ManifestLevel {
                ordinal: 1,
                tiered: false,
                files: l1.iter().copied().map(file).collect(),
            },
        ]
    }

    fn loaded(manifest: RuntimeManifest, chain_depth: usize) -> LoadedRuntimeManifest {
        LoadedRuntimeManifest {
            generation: manifest.generation,
            base_generation: None,
            chain_depth,
            manifest,
        }
    }

    fn test_store() -> (TempDir, RuntimeManifestStore) {
        let dir = tempfile::tempdir().unwrap();
        let url = url::Url::from_directory_path(dir.path())
            .unwrap()
            .to_string();
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(&url).unwrap();
        let file_manager = Arc::new(
            FileManager::with_defaults(fs, Arc::new(MetricsManager::new("runtime-manifest-test")))
                .unwrap(),
        );
        (dir, RuntimeManifestStore::new(file_manager))
    }

    fn write_raw(
        store: &RuntimeManifestStore,
        generation: u64,
        envelope: &RuntimeManifestEnvelope,
    ) {
        write_metadata_file(
            &store.file_manager,
            &runtime_manifest_name(generation),
            &encode_runtime_manifest(envelope).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn full_round_trip_and_atomic_current_publication() {
        let (_dir, store) = test_store();
        let current = manifest(1, vec![levels(&[1], &[])]);
        store
            .publish(&RuntimeManifestEnvelope::full(current.clone()))
            .unwrap();

        let loaded = store.load_current().unwrap().unwrap();
        assert_eq!(loaded.chain_depth, 1);
        assert_eq!(loaded.manifest, current);
    }

    #[test]
    fn decode_rejects_previous_physical_key_format() {
        let previous = RuntimeManifestEnvelope {
            version: 1,
            manifest: RuntimeManifestPayload::Full(manifest(1, vec![levels(&[], &[])])),
        };
        let raw = serde_json::to_vec(&previous).unwrap();
        let err = decode_runtime_manifest(&raw).expect_err("version 1 must be rejected");
        assert!(
            err.to_string()
                .contains("Unsupported runtime manifest version: 1 (expected 2..=4)")
        );
    }

    #[test]
    fn interrupted_publish_keeps_current_and_retry_skips_orphan_generation() {
        let (_dir, store) = test_store();
        let base = manifest(1, vec![levels(&[1], &[])]);
        store
            .publish(&RuntimeManifestEnvelope::full(base.clone()))
            .unwrap();
        let loaded_base = store.load_current().unwrap().unwrap();

        // Simulate a crash after MANIFEST-2 is durable but before CURRENT is replaced.
        let orphan = manifest(2, vec![levels(&[1, 2], &[])]);
        let orphan_envelope =
            build_runtime_manifest(orphan, Some(&loaded_base)).expect("build orphan");
        write_raw(&store, 2, &orphan_envelope);
        assert_eq!(
            store.load_current().unwrap().unwrap().manifest,
            base,
            "an unpublished generation must not change the authoritative state"
        );

        let retry_generation = store.allocate_next_generation().unwrap();
        assert_eq!(retry_generation, 3);
        let retry = manifest(retry_generation, vec![levels(&[1, 3], &[])]);
        let retry_envelope =
            build_runtime_manifest(retry.clone(), Some(&loaded_base)).expect("build retry");
        store.publish(&retry_envelope).unwrap();

        let loaded = store.load_current().unwrap().unwrap();
        assert_eq!(loaded.generation, 3);
        assert_eq!(loaded.manifest, retry);
        assert_eq!(
            store
                .load_chain(3)
                .unwrap()
                .iter()
                .map(|entry| entry.generation)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert!(
            store
                .file_manager
                .metadata_file_exists_untracked(&runtime_manifest_name(2))
                .unwrap(),
            "unreachable generations remain until reader-safe GC exists"
        );
    }

    #[test]
    fn l0_append_uses_incremental_and_preserves_order() {
        let (_dir, store) = test_store();
        let base = manifest(1, vec![levels(&[1], &[])]);
        let current = manifest(2, vec![levels(&[1, 2], &[])]);
        store
            .publish(&RuntimeManifestEnvelope::full(base.clone()))
            .unwrap();
        let envelope =
            build_runtime_manifest(current.clone(), Some(&loaded(base.clone(), 1))).unwrap();
        let RuntimeManifestPayload::Incremental(incremental) = &envelope.manifest else {
            panic!("expected incremental manifest");
        };
        assert_eq!(
            incremental.tree_level_edits[0].level_edits[0].resulting_file_ids,
            [1, 2]
        );
        assert_eq!(
            apply_runtime_incremental(&manifest(1, vec![levels(&[1], &[])]), incremental).unwrap(),
            current
        );
        store.publish(&envelope).unwrap();
        assert_eq!(store.load_current().unwrap().unwrap().manifest, current);
    }

    #[test]
    fn full_manifest_allows_the_same_file_in_multiple_trees() {
        let (_dir, store) = test_store();
        let shared = manifest(1, vec![levels(&[7], &[7]), levels(&[7], &[])]);
        store
            .publish(&RuntimeManifestEnvelope::full(shared.clone()))
            .unwrap();

        assert_eq!(store.load_current().unwrap().unwrap().manifest, shared);
    }

    #[test]
    fn full_manifest_rejects_conflicting_descriptors_for_a_shared_file() {
        let (_dir, store) = test_store();
        let mut conflicting_file = file(7);
        conflicting_file.path = "data/conflicting-7.sst".to_string();
        let mut conflicting = manifest(1, vec![levels(&[7], &[]), levels(&[7], &[])]);
        conflicting.tree_levels[1][0].files = vec![conflicting_file];
        write_raw(&store, 1, &RuntimeManifestEnvelope::full(conflicting));

        assert!(
            store
                .load_generation(1)
                .unwrap_err()
                .to_string()
                .contains("conflicting descriptors for file id 7")
        );
    }

    #[test]
    fn descriptor_change_falls_back_to_full_and_incremental_rejects_wrong_base_and_rewound_sequence()
     {
        let base = manifest(1, vec![levels(&[1], &[])]);
        let mut changed = manifest(2, vec![levels(&[1], &[])]);
        changed.tree_levels[0][0].files[0].path = "data/changed-1.sst".to_string();
        assert!(matches!(
            build_runtime_manifest(changed, Some(&loaded(base.clone(), 1)))
                .unwrap()
                .manifest,
            RuntimeManifestPayload::Full(_)
        ));

        let wrong_base = RuntimeIncrementalManifest {
            generation: 2,
            base_generation: 99,
            seq_id: 2,
            topology_epoch: 0,
            latest_schema_id: 1,
            tree_level_edits: Vec::new(),
            vlog_files: Vec::new(),
            truncation_cursors: Vec::new(),
        };
        assert!(
            apply_runtime_incremental(&base, &wrong_base)
                .unwrap_err()
                .to_string()
                .contains("expects base generation 99, but received 1")
        );

        let rewound_sequence = RuntimeIncrementalManifest {
            generation: 2,
            base_generation: 1,
            seq_id: 0,
            topology_epoch: 0,
            latest_schema_id: 1,
            tree_level_edits: Vec::new(),
            vlog_files: Vec::new(),
            truncation_cursors: Vec::new(),
        };
        assert!(
            apply_runtime_incremental(&base, &rewound_sequence)
                .unwrap_err()
                .to_string()
                .contains("cannot precede base sequence 1")
        );
    }

    #[test]
    fn rewrite_preserves_the_exact_resulting_file_order() {
        let base = manifest(1, vec![levels(&[1, 2], &[3, 4])]);
        let current = manifest(2, vec![levels(&[], &[5, 4])]);
        let envelope =
            build_runtime_manifest(current.clone(), Some(&loaded(base.clone(), 1))).unwrap();
        let RuntimeManifestPayload::Incremental(incremental) = envelope.manifest else {
            panic!("expected incremental manifest");
        };
        assert_eq!(
            apply_runtime_incremental(&base, &incremental).unwrap(),
            current
        );
    }

    #[test]
    fn trivial_move_is_an_incremental_edit() {
        let base = manifest(1, vec![levels(&[1], &[2])]);
        let current = manifest(2, vec![levels(&[], &[1, 2])]);
        let envelope =
            build_runtime_manifest(current.clone(), Some(&loaded(base.clone(), 1))).unwrap();
        let RuntimeManifestPayload::Incremental(incremental) = envelope.manifest else {
            panic!("expected incremental manifest");
        };
        assert_eq!(
            apply_runtime_incremental(&base, &incremental).unwrap(),
            current
        );
    }

    #[test]
    fn drop_is_an_incremental_edit() {
        let base = manifest(1, vec![levels(&[1], &[2, 3])]);
        let current = manifest(2, vec![levels(&[1], &[3])]);
        let envelope =
            build_runtime_manifest(current.clone(), Some(&loaded(base.clone(), 1))).unwrap();
        let RuntimeManifestPayload::Incremental(incremental) = envelope.manifest else {
            panic!("expected incremental manifest");
        };
        assert_eq!(
            apply_runtime_incremental(&base, &incremental).unwrap(),
            current
        );
    }

    #[test]
    fn topology_change_or_chain_limit_falls_back_to_full() {
        let base = manifest(1, vec![levels(&[1], &[])]);
        let changed_topology = manifest(2, vec![levels(&[1], &[]), levels(&[], &[])]);
        assert!(matches!(
            build_runtime_manifest(changed_topology, Some(&loaded(base.clone(), 1)))
                .unwrap()
                .manifest,
            RuntimeManifestPayload::Full(_)
        ));
        let current = manifest(2, vec![levels(&[1, 2], &[])]);
        assert!(matches!(
            build_runtime_manifest(
                current,
                Some(&loaded(base, MAX_RUNTIME_MANIFEST_CHAIN_DEPTH))
            )
            .unwrap()
            .manifest,
            RuntimeManifestPayload::Full(_)
        ));
    }

    #[test]
    fn sequence_rewind_starts_a_new_full_chain() {
        let mut base = manifest(1, vec![levels(&[1], &[])]);
        base.seq_id = 10;
        let mut restored = manifest(2, vec![levels(&[1], &[])]);
        restored.seq_id = 9;

        assert!(matches!(
            build_runtime_manifest(restored, Some(&loaded(base, 1)))
                .unwrap()
                .manifest,
            RuntimeManifestPayload::Full(_)
        ));
    }

    #[test]
    fn large_incremental_diff_falls_back_to_full() {
        let base_files: Vec<u64> = (1..=128).collect();
        let base = manifest(1, vec![levels(&base_files, &[])]);
        let current = manifest(2, vec![levels(&[129], &[])]);

        assert!(matches!(
            build_runtime_manifest(current, Some(&loaded(base, 1)))
                .unwrap()
                .manifest,
            RuntimeManifestPayload::Full(_)
        ));
    }

    #[test]
    fn load_rejects_missing_base_cycle_duplicate_ids_and_corrupt_current() {
        let (_dir, store) = test_store();
        let missing_base = RuntimeManifestEnvelope {
            version: RUNTIME_MANIFEST_VERSION_CURRENT,
            manifest: RuntimeManifestPayload::Incremental(RuntimeIncrementalManifest {
                generation: 2,
                base_generation: 1,
                seq_id: 2,
                topology_epoch: 0,
                latest_schema_id: 1,
                tree_level_edits: Vec::new(),
                vlog_files: Vec::new(),
                truncation_cursors: Vec::new(),
            }),
        };
        write_raw(&store, 2, &missing_base);
        assert!(
            store
                .load_generation(2)
                .unwrap_err()
                .to_string()
                .contains("Missing runtime manifest generation 1")
        );

        let cycle_a = RuntimeManifestEnvelope {
            version: RUNTIME_MANIFEST_VERSION_CURRENT,
            manifest: RuntimeManifestPayload::Incremental(RuntimeIncrementalManifest {
                generation: 3,
                base_generation: 4,
                seq_id: 3,
                topology_epoch: 0,
                latest_schema_id: 1,
                tree_level_edits: Vec::new(),
                vlog_files: Vec::new(),
                truncation_cursors: Vec::new(),
            }),
        };
        let cycle_b = RuntimeManifestEnvelope {
            version: RUNTIME_MANIFEST_VERSION_CURRENT,
            manifest: RuntimeManifestPayload::Incremental(RuntimeIncrementalManifest {
                generation: 4,
                base_generation: 3,
                seq_id: 4,
                topology_epoch: 0,
                latest_schema_id: 1,
                tree_level_edits: Vec::new(),
                vlog_files: Vec::new(),
                truncation_cursors: Vec::new(),
            }),
        };
        write_raw(&store, 3, &cycle_a);
        write_raw(&store, 4, &cycle_b);
        assert!(
            store
                .load_generation(3)
                .unwrap_err()
                .to_string()
                .contains("dependency cycle")
        );

        let duplicate = manifest(5, vec![levels(&[9, 9], &[])]);
        write_raw(&store, 5, &RuntimeManifestEnvelope::full(duplicate));
        assert!(
            store
                .load_generation(5)
                .unwrap_err()
                .to_string()
                .contains("duplicate file id 9")
        );

        write_metadata_file(
            &store.file_manager,
            RUNTIME_CURRENT_NAME,
            b"not-a-generation",
        )
        .unwrap();
        assert!(
            store
                .load_current()
                .unwrap_err()
                .to_string()
                .contains("CURRENT is not a generation")
        );
    }

    #[test]
    fn load_rejects_an_incremental_with_an_incomplete_resulting_order() {
        let (_dir, store) = test_store();
        let base = manifest(1, vec![levels(&[1], &[])]);
        write_raw(&store, 1, &RuntimeManifestEnvelope::full(base));
        let invalid = RuntimeManifestEnvelope {
            version: RUNTIME_MANIFEST_VERSION_CURRENT,
            manifest: RuntimeManifestPayload::Incremental(RuntimeIncrementalManifest {
                generation: 2,
                base_generation: 1,
                seq_id: 2,
                topology_epoch: 0,
                latest_schema_id: 1,
                tree_level_edits: vec![RuntimeTreeLevelEdit {
                    tree_idx: 0,
                    level_edits: vec![RuntimeLevelEdit {
                        level: 0,
                        tiered: true,
                        removed_file_ids: Vec::new(),
                        added_files: vec![file(2)],
                        resulting_file_ids: vec![1],
                    }],
                }],
                vlog_files: Vec::new(),
                truncation_cursors: Vec::new(),
            }),
        };
        write_raw(&store, 2, &invalid);

        assert!(
            store
                .load_generation(2)
                .unwrap_err()
                .to_string()
                .contains("invalid resulting file order")
        );
    }
}
