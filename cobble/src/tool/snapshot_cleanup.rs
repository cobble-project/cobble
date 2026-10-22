use crate::coordinator::GlobalSnapshotManifest;
use crate::db_status::DbLifecycle;
use crate::file::atomic_replace;
use crate::file::logical_file::ReplicaOrigin;
use crate::file::{FileManager, FileSystem, FileSystemRegistry, MetadataReader};
use crate::manifest_model::manifest_schema_ids;
use crate::metrics_manager::MetricsManager;
use crate::paths::{
    global_snapshot_manifest_path, schema_file_path_from_snapshot_manifest_path,
    snapshot_manifest_name,
};
use crate::schema::SchemaManager;
use crate::snapshot::ManifestSnapshot;
use crate::snapshot::manifest::{
    ManifestPayload, apply_incremental_manifest, decode_manifest, manifest_data_file_refs,
    parse_snapshot_manifest_id,
};
use crate::snapshot::{SnapshotManager, list_snapshot_manifest_ids, load_manifest_entry};
use crate::storage_url::{
    append_storage_path, match_storage_path, normalize_storage_path_to_url, storage_location_url,
};
use crate::{Config, Error, Result, VolumeDescriptor};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// Prune a shard snapshot as an external maintenance operation.
///
/// This helper always runs with noop governance to avoid affecting runtime
/// ownership coordination when invoked out-of-band by lifecycle managers.
/// Calls that mutate snapshot metadata for the same DB must be externally
/// serialized across processes; this helper does not provide distributed fencing.
pub fn prune_shard_snapshot(
    config: Config,
    db_id: impl Into<String>,
    snapshot_id: u64,
) -> Result<bool> {
    let mut config = config.normalize_volume_paths()?;
    config.governance_mode = crate::GovernanceMode::Noop;
    let db_id = db_id.into();
    let metrics_manager = Arc::new(MetricsManager::new(&db_id));
    let file_manager = Arc::new(FileManager::from_config(&config, &db_id, metrics_manager)?);
    let snapshot_ids = list_snapshot_manifest_ids(&file_manager)?;
    if snapshot_ids.is_empty() {
        return Ok(false);
    }

    let mut loaded = Vec::with_capacity(snapshot_ids.len());
    let mut loaded_by_id = HashMap::new();
    for id in snapshot_ids {
        let entry = load_manifest_entry(&file_manager, id, &loaded_by_id)?;
        loaded_by_id.insert(id, entry.clone());
        loaded.push(entry);
    }

    let schema_manager = Arc::new(SchemaManager::from_manifests(
        &file_manager,
        loaded.iter().map(|entry| &entry.manifest),
        None,
    )?);
    let snapshot_manager = SnapshotManager::new_for_maintenance(
        Arc::clone(&file_manager),
        schema_manager,
        Arc::new(DbLifecycle::new_open()),
        config.snapshot_retention,
        config.snapshot_only_track,
        config.snapshot_disable_incremental_base_link,
        Vec::new(),
        config.time_provider.create(),
    );
    for entry in &loaded {
        snapshot_manager.import_snapshot_from_manifest(
            entry.snapshot_id,
            entry.base_snapshot_id,
            &entry.manifest,
        )?;
    }
    snapshot_manager.expire_snapshot(snapshot_id)
}

/// Options for offline snapshot removal. No writer, reader, or compactor may be active.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotCleanupOptions {
    /// Preserve selected snapshots and files required by surviving snapshots.
    /// Disabling this deliberately allows survivors to become unrecoverable.
    pub preserve_dependencies: bool,
    /// Select all snapshot IDs at or below the requested ID, instead of only that ID.
    /// For global cleanup this compares global IDs, never shard IDs.
    pub include_older_snapshots: bool,
}

impl Default for SnapshotCleanupOptions {
    fn default() -> Self {
        Self {
            preserve_dependencies: true,
            include_older_snapshots: false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardSnapshotIdentity {
    pub db_id: String,
    pub snapshot_id: u64,
    pub manifest_path: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SnapshotCleanupResult {
    /// Removed SST, VLOG, schema, and active-memtable files (not manifests or CURRENT).
    pub deleted_files: usize,
    pub deleted_shard_snapshots: usize,
    pub deleted_global_snapshots: usize,
    /// Selected shard snapshots retained because surviving roots depend on them.
    pub preserved_snapshots: Vec<ShardSnapshotIdentity>,
}

/// Remove selected snapshots from one shard without opening a database.
///
/// `metadata_volume` is the shard's parent volume: `<base>/<db_id>/snapshot/...`.
/// For exported shards use the export's `shards` directory as the base. Other volume
/// descriptors supply additional filesystem routes and credentials. Only this shard's
/// snapshots are preservation roots; callers must account for references from other roots.
/// This is offline maintenance and must not overlap a writer, reader, or compactor.
pub fn cleanup_shard_snapshot(
    metadata_volume: &VolumeDescriptor,
    other_volumes: &[VolumeDescriptor],
    db_id: &str,
    snapshot_id: u64,
    options: SnapshotCleanupOptions,
) -> Result<SnapshotCleanupResult> {
    if db_id.is_empty() || db_id.contains(['/', '\\']) || matches!(db_id, "." | "..") {
        return Err(Error::ConfigError(
            "cleanup requires a single shard directory name".into(),
        ));
    }
    let storage = Storage::new(metadata_volume, other_volumes)?;
    let shard = Shard::load(
        &storage,
        db_id.to_string(),
        FileKey::new(0, format!("{db_id}/snapshot")),
    )?;
    let selected = shard
        .snapshots
        .keys()
        .copied()
        .filter(|id| selected(*id, snapshot_id, options))
        .map(|id| (0, id))
        .collect();
    CleanupScope {
        shards: vec![shard],
        selected,
        globals: None,
        selected_globals: BTreeSet::new(),
    }
    .plan(&storage, options)?
    .execute(&storage)
}

/// Remove selected global snapshots and the union of their actual shard snapshot references.
///
/// The metadata volume contains `snapshot/SNAPSHOT-<global-id>`. All remaining globals
/// and all unselected snapshots in referenced shard directories are preservation roots.
/// Filesystem routes retain the descriptors' URL options and credentials. No recursive
/// volume sweep is performed, and external-origin data files are never removed.
/// This is offline maintenance and must not overlap a writer, reader, or compactor.
pub fn cleanup_global_snapshot(
    metadata_volume: &VolumeDescriptor,
    other_volumes: &[VolumeDescriptor],
    snapshot_id: u64,
    options: SnapshotCleanupOptions,
) -> Result<SnapshotCleanupResult> {
    let storage = Storage::new(metadata_volume, other_volumes)?;
    let mut globals = BTreeMap::new();
    let mut selected_globals = BTreeSet::new();
    let mut shards = Vec::new();
    let mut shard_indices = BTreeMap::new();
    let mut selected_shards = BTreeSet::new();
    for id in storage.list_ids(&FileKey::new(0, "snapshot"))? {
        let path = FileKey::new(0, global_snapshot_manifest_path(id));
        let global: GlobalSnapshotManifest = serde_json::from_slice(&storage.read(&path)?)
            .map_err(|err| Error::IoError(format!("Failed to decode global manifest: {err}")))?;
        global.validate_version()?;
        if global.id != id {
            return Err(Error::IoError(
                "global snapshot ID differs from its file name".into(),
            ));
        }
        let is_selected = selected(id, snapshot_id, options);
        if is_selected {
            selected_globals.insert(id);
        }
        let mut refs = Vec::new();
        for reference in global.shard_snapshots {
            let key = storage.resolve(&reference.manifest_path)?;
            let expected_name = snapshot_manifest_name(reference.snapshot_id);
            let (directory, name) = key
                .path
                .rsplit_once('/')
                .ok_or_else(|| Error::IoError("invalid shard manifest path".into()))?;
            if name != expected_name
                || directory.rsplit_once('/').is_none_or(|(root, name)| {
                    name != "snapshot" || root.rsplit('/').next() != Some(reference.db_id.as_str())
                })
            {
                return Err(Error::IoError(
                    "shard reference differs from its manifest path".into(),
                ));
            }
            let directory = FileKey::new(key.volume, directory);
            let index = if let Some(index) = shard_indices.get(&directory) {
                *index
            } else {
                let index = shards.len();
                shards.push(Shard::load(&storage, reference.db_id, directory.clone())?);
                shard_indices.insert(directory, index);
                index
            };
            refs.push((index, reference.snapshot_id));
            if is_selected {
                selected_shards.insert((index, reference.snapshot_id));
            }
        }
        globals.insert(id, refs);
    }
    CleanupScope {
        shards,
        selected: selected_shards,
        globals: Some(globals),
        selected_globals,
    }
    .plan(&storage, options)?
    .execute(&storage)
}

fn selected(id: u64, threshold: u64, options: SnapshotCleanupOptions) -> bool {
    id == threshold || (options.include_older_snapshots && id < threshold)
}

// An I/O route and relative path, not a physical identity: routes may overlap.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct FileKey {
    volume: usize,
    path: String,
}

impl FileKey {
    fn new(volume: usize, path: impl Into<String>) -> Self {
        Self {
            volume,
            path: path.into(),
        }
    }
}

struct Route {
    base: String,
    fs: Arc<dyn FileSystem>,
}
/// Routes offline I/O through supplied volumes without registering runtime file ownership.
struct Storage {
    routes: Vec<Route>,
}

impl Storage {
    fn new(metadata: &VolumeDescriptor, others: &[VolumeDescriptor]) -> Result<Self> {
        let registry = FileSystemRegistry::new();
        let routes = std::iter::once(metadata)
            .chain(others)
            .map(|volume| {
                Ok(Route {
                    base: normalize_storage_path_to_url(&volume.base_dir)?,
                    fs: registry.get_or_register_volume(volume)?,
                })
            })
            .collect::<Result<_>>()?;
        Ok(Self { routes })
    }
    fn resolve(&self, path: &str) -> Result<FileKey> {
        self.try_resolve(path)?
            .ok_or_else(|| Error::ConfigError("snapshot path is outside cleanup volumes".into()))
    }
    fn try_resolve(&self, path: &str) -> Result<Option<FileKey>> {
        let path = normalize_storage_path_to_url(path)?;
        Ok(match_storage_path(
            &path,
            self.routes
                .iter()
                .enumerate()
                .map(|(i, r)| (i, r.base.as_str())),
        )?
        .map(|(volume, path)| FileKey::new(volume, path)))
    }
    fn absolute(&self, key: &FileKey) -> String {
        append_storage_path(&self.routes[key.volume].base, &key.path)
    }
    fn location(&self, key: &FileKey) -> Result<String> {
        // Overlapping configured routes and option-free exported paths can name the
        // same file. Compare locations conservatively for protection only; actual
        // I/O always uses the original route, including its options and credentials.
        Ok(storage_location_url(&self.absolute(key))?.to_string())
    }
    fn fs(&self, key: &FileKey) -> &dyn FileSystem {
        self.routes[key.volume].fs.as_ref()
    }
    fn read(&self, key: &FileKey) -> Result<Vec<u8>> {
        Ok(MetadataReader::new(self.fs(key).open_read(&key.path)?)
            .read_all()?
            .to_vec())
    }
    fn list_ids(&self, directory: &FileKey) -> Result<Vec<u64>> {
        let mut ids = self
            .fs(directory)
            .list(&directory.path)?
            .iter()
            .filter_map(|name| parse_snapshot_manifest_id(name))
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }
    fn delete(&self, key: &FileKey) -> Result<bool> {
        if !self.fs(key).exists(&key.path)? {
            return Ok(false);
        }
        self.fs(key).delete(&key.path)?;
        Ok(true)
    }
}

struct Shard {
    db_id: String,
    directory: FileKey,
    snapshots: BTreeMap<u64, ManifestPayload>,
}

impl Shard {
    fn load(storage: &Storage, db_id: String, directory: FileKey) -> Result<Self> {
        let mut snapshots = BTreeMap::new();
        for id in storage.list_ids(&directory)? {
            let key = FileKey::new(
                directory.volume,
                format!("{}/{}", directory.path, snapshot_manifest_name(id)),
            );
            let payload = decode_manifest(&storage.read(&key)?)?;
            let encoded_id = match &payload {
                ManifestPayload::Snapshot(m) => m.id,
                ManifestPayload::IncrementalSnapshot(m) => m.id,
            };
            if encoded_id != id {
                return Err(Error::IoError(
                    "shard snapshot ID differs from its file name".into(),
                ));
            }
            snapshots.insert(id, payload);
        }
        Ok(Self {
            db_id,
            directory,
            snapshots,
        })
    }
    fn key(&self, id: u64) -> FileKey {
        FileKey::new(
            self.directory.volume,
            format!("{}/{}", self.directory.path, snapshot_manifest_name(id)),
        )
    }
    fn chain(&self, id: u64, missing_ok: bool) -> Result<Vec<u64>> {
        let mut chain = Vec::new();
        let mut visited = BTreeSet::new();
        let mut next = Some(id);
        while let Some(id) = next {
            if !visited.insert(id) {
                return Err(Error::IoError("snapshot dependency cycle".into()));
            }
            let Some(payload) = self.snapshots.get(&id) else {
                if missing_ok {
                    break;
                }
                return Err(Error::IoError(format!("missing snapshot dependency {id}")));
            };
            chain.push(id);
            next = match payload {
                ManifestPayload::Snapshot(_) => None,
                ManifestPayload::IncrementalSnapshot(m) => Some(m.base_snapshot_id),
            };
        }
        chain.reverse();
        Ok(chain)
    }
    // None means the base is absent; corrupt metadata and invalid edits still return errors.
    fn resolved(&self, id: u64) -> Result<Option<ManifestSnapshot>> {
        let chain = self.chain(id, true)?;
        let mut resolved = None;
        for id in chain {
            match &self.snapshots[&id] {
                ManifestPayload::Snapshot(m) => resolved = Some(m.clone()),
                ManifestPayload::IncrementalSnapshot(m) => {
                    let Some(base) = resolved.as_mut() else {
                        return Ok(None);
                    };
                    apply_incremental_manifest(base, m)?;
                }
            }
        }
        Ok(resolved)
    }
    fn contents(&self, storage: &Storage, id: u64, owned_only: bool) -> Result<BTreeSet<FileKey>> {
        let root = self
            .directory
            .path
            .strip_suffix("/snapshot")
            .expect("shard snapshot directory");
        let absolute_root = storage.absolute(&FileKey::new(self.directory.volume, root));
        let owned_roots = std::iter::once(absolute_root.clone())
            .chain(
                storage
                    .routes
                    .iter()
                    .map(|route| append_storage_path(&route.base, &self.db_id)),
            )
            .collect::<Vec<_>>();
        let mut refs = Vec::new();
        let mut schemas = BTreeSet::new();
        if let Some(manifest) = self.resolved(id)? {
            schemas = manifest_schema_ids(manifest.latest_schema_id, &manifest.tree_levels);
            refs.extend(
                manifest_data_file_refs(&manifest)
                    .filter(|(_, _, origin)| !owned_only || *origin == ReplicaOrigin::Owned)
                    .map(|(_, path, _)| path),
            );
            refs.extend(
                manifest
                    .active_memtable_data
                    .into_iter()
                    .map(|segment| append_storage_path(&absolute_root, &segment.path)),
            );
        } else if let Some(ManifestPayload::IncrementalSnapshot(m)) = self.snapshots.get(&id) {
            // A prior forced cleanup may have removed the base. Direct references are still
            // actionable on retry; never treat a checksum or decoding failure as absence.
            schemas.insert(m.latest_schema_id);
            for file in m
                .tree_level_edits
                .iter()
                .flat_map(|t| &t.level_edits)
                .flat_map(|l| &l.new_files)
            {
                schemas.insert(file.schema_id);
                schemas.extend(file.schema_id..=m.latest_schema_id);
                if !owned_only || file.origin == ReplicaOrigin::Owned {
                    refs.push(file.path.clone());
                }
            }
            refs.extend(
                m.vlog_files
                    .iter()
                    .filter(|f| !owned_only || f.origin == ReplicaOrigin::Owned)
                    .map(|f| f.path.clone()),
            );
            refs.extend(
                m.active_memtable_data
                    .iter()
                    .map(|segment| append_storage_path(&absolute_root, &segment.path)),
            );
        }
        let manifest_path = storage.absolute(&self.key(id));
        for schema in schemas {
            refs.push(schema_file_path_from_snapshot_manifest_path(
                &manifest_path,
                schema,
            )?);
        }
        let mut keys = BTreeSet::new();
        for path in refs {
            let key = if owned_only {
                storage.resolve(&path)?
            } else if let Some(key) = storage.try_resolve(&path)? {
                key
            } else {
                // A reference outside the configured routes cannot overlap a deletion.
                continue;
            };
            if owned_only {
                // Ownership belongs to the logical shard namespace, not to the route
                // chosen for I/O: a more-specific data route may hide that prefix.
                let Some((_, relative)) = match_storage_path(
                    &normalize_storage_path_to_url(&path)?,
                    owned_roots.iter().map(String::as_str).enumerate(),
                )?
                else {
                    continue;
                };
                if !(relative.starts_with("data/")
                    || relative.starts_with("schema/schema-")
                    || relative.starts_with("snapshot/SNAPSHOT-ACTIVE-DATA-")
                    || relative
                        .strip_prefix("compaction/jobs/")
                        .and_then(|job| job.split_once('/'))
                        .is_some_and(|(job, path)| !job.is_empty() && path.starts_with("data/")))
                {
                    continue;
                }
            }
            keys.insert(key);
        }
        Ok(keys)
    }
}

type SnapshotKey = (usize, u64);

struct CleanupScope {
    shards: Vec<Shard>,
    selected: BTreeSet<SnapshotKey>,
    // Some(empty) still identifies global maintenance, including a dangling CURRENT retry.
    globals: Option<BTreeMap<u64, Vec<SnapshotKey>>>,
    selected_globals: BTreeSet<u64>,
}

impl CleanupScope {
    fn preserved_snapshots(&self, enabled: bool) -> Result<BTreeSet<SnapshotKey>> {
        let mut preserved = BTreeSet::new();
        if !enabled {
            return Ok(preserved);
        }
        let roots = self
            .shards
            .iter()
            .enumerate()
            .flat_map(|(i, shard)| shard.snapshots.keys().map(move |id| (i, *id)))
            .filter(|key| !self.selected.contains(key))
            .chain(
                self.globals
                    .iter()
                    .flat_map(|globals| globals.iter())
                    .filter(|(id, _)| !self.selected_globals.contains(id))
                    .flat_map(|(_, refs)| refs.iter().copied()),
            );
        for (index, id) in roots {
            preserved.extend(
                self.shards[index]
                    .chain(id, false)?
                    .into_iter()
                    .map(|id| (index, id)),
            );
        }
        Ok(preserved)
    }

    fn plan(&self, storage: &Storage, options: SnapshotCleanupOptions) -> Result<CleanupPlan> {
        let preserved = self.preserved_snapshots(options.preserve_dependencies)?;
        let mut protected_locations = BTreeSet::new();
        let mut preserved_manifests = BTreeSet::new();
        for &(index, id) in &preserved {
            preserved_manifests.insert(storage.location(&self.shards[index].key(id))?);
            for key in self.shards[index].contents(storage, id, false)? {
                protected_locations.insert(storage.location(&key)?);
            }
        }
        let mut files = BTreeSet::new();
        let mut manifests = Vec::new();
        let mut removed_manifests = BTreeSet::new();
        let mut result = SnapshotCleanupResult::default();
        for &(index, id) in &self.selected {
            let shard = &self.shards[index];
            if !shard.snapshots.contains_key(&id) {
                continue;
            }
            let depth = shard.chain(id, !options.preserve_dependencies)?.len();
            let key = shard.key(id);
            let location = storage.location(&key)?;
            if preserved_manifests.contains(&location) {
                result.preserved_snapshots.push(ShardSnapshotIdentity {
                    db_id: shard.db_id.clone(),
                    snapshot_id: id,
                    manifest_path: storage.absolute(&shard.key(id)),
                });
            } else {
                files.extend(shard.contents(storage, id, true)?);
                manifests.push((depth, key));
                removed_manifests.insert(location);
            }
        }
        let mut deletions = BTreeSet::new();
        let mut removed_locations = BTreeSet::new();
        for key in files {
            let location = storage.location(&key)?;
            if !protected_locations.contains(&location) {
                removed_locations.insert(location);
                deletions.insert(key);
            }
        }
        // Delete descendants before their bases. If a later deletion fails, every remaining
        // selected manifest still has its dependency chain for a safe, metadata-only retry.
        manifests.sort_unstable_by(|(a_depth, a_key), (b_depth, b_key)| {
            b_depth.cmp(a_depth).then_with(|| a_key.cmp(b_key))
        });
        let pointers =
            self.current_pointers(storage, &removed_manifests, &removed_locations, &preserved)?;
        Ok(CleanupPlan {
            files: deletions,
            shard_manifests: manifests.into_iter().map(|(_, key)| key).collect(),
            global_manifests: self
                .selected_globals
                .iter()
                .map(|id| FileKey::new(0, global_snapshot_manifest_path(*id)))
                .collect(),
            pointers,
            result,
        })
    }

    fn current_pointers(
        &self,
        storage: &Storage,
        removed_manifests: &BTreeSet<String>,
        removed_locations: &BTreeSet<String>,
        preserved: &BTreeSet<SnapshotKey>,
    ) -> Result<BTreeSet<FileKey>> {
        // Resolve CURRENT before any deletion. No fallback is published for an invalidated pointer.
        let recoverable = |index: usize, id: u64| -> Result<bool> {
            let shard = &self.shards[index];
            let chain = shard.chain(id, true)?;
            if chain.is_empty()
                || !matches!(
                    shard.snapshots.get(&chain[0]),
                    Some(ManifestPayload::Snapshot(_))
                )
            {
                return Ok(false);
            }
            for id in chain {
                if removed_manifests.contains(&storage.location(&shard.key(id))?) {
                    return Ok(false);
                }
                // Preserved contents were analyzed above and excluded from all deletions.
                if preserved.contains(&(index, id)) {
                    continue;
                }
                for key in shard.contents(storage, id, false)? {
                    if removed_locations.contains(&storage.location(&key)?) {
                        return Ok(false);
                    }
                }
            }
            Ok(true)
        };
        let mut pointers = BTreeSet::new();
        for (index, shard) in self.shards.iter().enumerate() {
            let pointer = FileKey::new(
                shard.directory.volume,
                format!("{}/CURRENT", shard.directory.path),
            );
            if match current_id(storage, &pointer)? {
                Some(id) => !recoverable(index, id)?,
                None => true, // Also remove abandoned commits from an interrupted deletion.
            } {
                pointers.insert(pointer);
            }
        }
        if let Some(globals) = &self.globals {
            let pointer = FileKey::new(0, "snapshot/CURRENT");
            if let Some(id) = current_id(storage, &pointer)? {
                let mut valid = !self.selected_globals.contains(&id);
                if let Some(refs) = globals.get(&id) {
                    for &(index, id) in refs {
                        valid &= recoverable(index, id)?;
                    }
                } else {
                    valid = false;
                }
                if !valid {
                    pointers.insert(pointer);
                }
            } else {
                pointers.insert(pointer);
            }
        }
        Ok(pointers)
    }
}

/// Fully validated, metadata-only analysis. Only execute performs storage mutations.
struct CleanupPlan {
    files: BTreeSet<FileKey>,
    shard_manifests: Vec<FileKey>,
    global_manifests: Vec<FileKey>,
    pointers: BTreeSet<FileKey>,
    result: SnapshotCleanupResult,
}

impl CleanupPlan {
    fn execute(mut self, storage: &Storage) -> Result<SnapshotCleanupResult> {
        // Retain manifests until their data is removed so interrupted cleanup remains retryable.
        for key in self.files {
            self.result.deleted_files += usize::from(storage.delete(&key)?);
        }
        for key in self.shard_manifests {
            self.result.deleted_shard_snapshots += usize::from(storage.delete(&key)?);
        }
        for key in self.global_manifests {
            self.result.deleted_global_snapshots += usize::from(storage.delete(&key)?);
        }
        for key in self.pointers {
            atomic_replace::delete(storage.fs(&key), &key.path)?;
        }
        Ok(self.result)
    }
}

fn current_id(storage: &Storage, key: &FileKey) -> Result<Option<u64>> {
    if !atomic_replace::exists(storage.fs(key), &key.path)? {
        return Ok(None);
    }
    let payload =
        MetadataReader::new(atomic_replace::open_read(storage.fs(key), &key.path)?).read_all()?;
    let text = std::str::from_utf8(&payload)
        .map_err(|err| Error::IoError(format!("Invalid CURRENT pointer: {err}")))?;
    parse_snapshot_manifest_id(text.trim())
        .map(Some)
        .ok_or_else(|| Error::IoError("Invalid CURRENT pointer".into()))
}

#[cfg(test)]
#[path = "../../tests/unit/snapshot_cleanup_faults.rs"]
mod fault_tests;

#[cfg(test)]
#[path = "../../tests/unit/snapshot_cleanup.rs"]
mod tests;
