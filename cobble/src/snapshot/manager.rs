use super::manifest::{
    ManifestSnapshot, build_tree_versions_from_manifest_untracked,
    build_vlog_version_from_manifest_untracked, encode_manifest, manifest_data_file_refs,
    snapshot_manifest_name,
};
use super::{
    ActiveMemtableSnapshotData, DbSnapshot, SnapshotCallback, SnapshotManifestInfo, memtable,
};
use crate::config::MemtableType;
use crate::data_file::DataFile;
use crate::db_state::{DbState, DbStateHandle};
use crate::db_status::DbLifecycle;
use crate::error::{Error, Result};
use crate::file::{
    BufferedWriter, File, FileManager, SnapshotCopyResourceRegistry, TrackedFile, TrackedFileId,
};
use crate::lsm::{LSMTreeVersion, Level};
use crate::paths::schema_file_relative_path;
use crate::schema::SchemaManager;
use crate::vlog::VlogVersion;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::RangeInclusive;
use std::sync::{Arc, Condvar, Mutex, RwLock, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

const CLOSE_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct SnapshotManager {
    file_manager: Arc<FileManager>,
    schema_manager: Arc<SchemaManager>,
    db_lifecycle: Arc<DbLifecycle>,
    bucket_ranges: Arc<RwLock<Vec<RangeInclusive<u16>>>>,
    state: Arc<Mutex<SnapshotManagerState>>,
    retention: Option<usize>,
    /// Background worker for manifest materialization.
    materialize_tx: Arc<Mutex<Option<mpsc::Sender<u64>>>>,
    materialize_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    materialize_done: Arc<Condvar>,
    upload_runtime: Arc<Runtime>,
}

impl Clone for SnapshotManager {
    fn clone(&self) -> Self {
        Self {
            file_manager: Arc::clone(&self.file_manager),
            schema_manager: Arc::clone(&self.schema_manager),
            db_lifecycle: Arc::clone(&self.db_lifecycle),
            bucket_ranges: Arc::clone(&self.bucket_ranges),
            state: Arc::clone(&self.state),
            retention: self.retention,
            materialize_tx: Arc::clone(&self.materialize_tx),
            materialize_worker: Arc::clone(&self.materialize_worker),
            materialize_done: Arc::clone(&self.materialize_done),
            upload_runtime: Arc::clone(&self.upload_runtime),
        }
    }
}

struct SnapshotManagerState {
    next_id: u64,
    snapshots: BTreeMap<u64, Arc<DbSnapshot>>,
    completed: BTreeSet<u64>,
    retained: HashSet<u64>,
    // Map<snapshot_id, set_of_snapshot_ids_that_it_references>.
    // Includes self-reference while the snapshot retains itself.
    incremental_references: HashMap<u64, HashSet<u64>>,
    // Map<snapshot_id, number_of_references_pointing_to_it>.
    // Counts self-reference and incoming references from incremental children.
    incremental_ref_counts: HashMap<u64, usize>,
    // Reference counts for schema files used by live snapshots.
    schema_ref_counts: HashMap<u64, usize>,
    in_flight: usize,
}

struct PreparedSnapshotMaterialization {
    snapshot: DbSnapshot,
    source_snapshot_links: Vec<(Arc<DataFile>, Arc<TrackedFileId>)>,
}

struct MaterializeTempResourceRegistry {
    file_manager: Arc<FileManager>,
    state: Mutex<MaterializeTempResourceState>,
}

struct MaterializeTempResourceState {
    referenced_files: Vec<Arc<TrackedFile>>,
    temp_copied_file_ids: Vec<u64>,
    finalized: bool,
}

impl MaterializeTempResourceRegistry {
    fn new(file_manager: Arc<FileManager>) -> Self {
        Self {
            file_manager,
            state: Mutex::new(MaterializeTempResourceState {
                referenced_files: Vec::new(),
                temp_copied_file_ids: Vec::new(),
                finalized: false,
            }),
        }
    }

    fn register_reference(&self, tracked: Arc<TrackedFile>) {
        let mut state = self.state.lock().unwrap();
        state.referenced_files.push(tracked);
    }

    fn finalize(&self) {
        let mut state = self.state.lock().unwrap();
        state.finalized = true;
    }
}

impl Drop for MaterializeTempResourceRegistry {
    fn drop(&mut self) {
        let (references_to_release, copied_file_ids) = {
            let mut state = self.state.lock().unwrap();
            if state.finalized {
                return;
            }
            (
                std::mem::take(&mut state.referenced_files),
                std::mem::take(&mut state.temp_copied_file_ids),
            )
        };
        for tracked in &references_to_release {
            tracked.dereference();
        }
        for file_id in &copied_file_ids {
            let _ = self.file_manager.remove_data_file(*file_id);
        }
    }
}

impl SnapshotCopyResourceRegistry for MaterializeTempResourceRegistry {
    fn register_temp_copied_file(&self, file_id: u64) {
        let mut state = self.state.lock().unwrap();
        if state.finalized {
            return;
        }
        state.temp_copied_file_ids.push(file_id);
    }
}

impl SnapshotManager {
    pub(crate) fn new(
        file_manager: Arc<FileManager>,
        schema_manager: Arc<SchemaManager>,
        db_lifecycle: Arc<DbLifecycle>,
        retention: Option<usize>,
        bucket_ranges: Vec<RangeInclusive<u16>>,
    ) -> Self {
        let upload_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build snapshot upload runtime");
        Self {
            file_manager,
            schema_manager,
            db_lifecycle,
            bucket_ranges: Arc::new(RwLock::new(bucket_ranges)),
            state: Arc::new(Mutex::new(SnapshotManagerState {
                next_id: 0,
                snapshots: BTreeMap::new(),
                completed: BTreeSet::new(),
                retained: HashSet::new(),
                incremental_references: HashMap::new(),
                incremental_ref_counts: HashMap::new(),
                schema_ref_counts: HashMap::new(),
                in_flight: 0,
            })),
            retention,
            materialize_tx: Arc::new(Mutex::new(None)),
            materialize_worker: Arc::new(Mutex::new(None)),
            materialize_done: Arc::new(Condvar::new()),
            upload_runtime: Arc::new(upload_runtime),
        }
    }

    /// Ensure the materializer worker is running.
    pub(crate) fn start_materializer(&self) -> Result<()> {
        let mut tx_guard = self.materialize_tx.lock().unwrap();
        if tx_guard.is_some() {
            return Ok(());
        }
        let (tx, rx) = mpsc::channel::<u64>();
        let manager = self.clone();
        let handle = std::thread::Builder::new()
            .name("cobble-snapshot".to_string())
            .spawn(move || {
                while let Ok(id) = rx.recv() {
                    let _ = manager.materialize(id);
                }
            })
            .map_err(|e| Error::IoError(format!("Failed to start snapshot worker: {}", e)))?;
        *tx_guard = Some(tx);
        let mut worker_guard = self.materialize_worker.lock().unwrap();
        *worker_guard = Some(handle);
        Ok(())
    }

    pub(crate) fn create_snapshot(&self, callback: Option<SnapshotCallback>) -> Arc<DbSnapshot> {
        let mut state = self.state.lock().unwrap();
        let id = state.next_id;
        state.next_id += 1;
        let manifest_path = self.file_manager.metadata_path(&snapshot_manifest_name(id));
        let mut snapshot = DbSnapshot::new(id, &manifest_path, callback);
        snapshot.bucket_ranges = self.bucket_ranges.read().unwrap().clone();
        let snapshot = Arc::new(snapshot);
        state.snapshots.insert(id, Arc::clone(&snapshot));
        // initialize incremental references with self-reference
        state
            .incremental_references
            .entry(id)
            .or_default()
            .insert(id);
        state.incremental_ref_counts.insert(id, 1);
        snapshot
    }

    pub(crate) fn set_bucket_ranges(&self, bucket_ranges: Vec<RangeInclusive<u16>>) {
        *self.bucket_ranges.write().unwrap() = bucket_ranges;
    }

    /// Complete a snapshot: persist manifest and manage lifecycle.
    ///
    /// Snapshot flow: (1) capture current LSM tree versions, VLOG version,
    /// and active memtable incremental data from DbState, (2) determine if
    /// this is a full or incremental snapshot (based on suggested base),
    /// (3) encode and write manifest JSON to snapshot volume, (4) persist
    /// schema files up to the latest referenced schema id, (5) register
    /// the snapshot in the manager's state with ref-counted dependency
    /// tracking, (6) auto-expire old snapshots if retention is configured.
    pub(crate) fn finish_snapshot(
        &self,
        id: u64,
        db_state: &Arc<DbState>,
        active_memtable_data: Vec<ActiveMemtableSnapshotData>,
        db_state_handle: &DbStateHandle,
    ) -> bool {
        let mut state = self.state.lock().unwrap();
        let Some(snapshot) = state.snapshots.get(&id).cloned() else {
            return false;
        };
        let mut snapshot = (*snapshot).clone();
        snapshot.lsm_tree_bucket_ranges = db_state.multi_lsm_version.bucket_ranges();
        snapshot.tree_scopes = db_state.multi_lsm_version.tree_scopes();
        if snapshot.lsm_tree_bucket_ranges.is_empty() {
            snapshot.lsm_tree_bucket_ranges = snapshot.bucket_ranges.clone();
        }
        let tree_versions = db_state.multi_lsm_version.tree_versions_cloned();
        snapshot.lsm_versions = tree_versions
            .iter()
            .map(|version| clone_lsm_tree_version_untracked(version.as_ref(), &self.file_manager))
            .collect();
        let mut tracked_file_ids: BTreeSet<u64> = tree_versions
            .iter()
            .flat_map(|version| version.levels.iter())
            .flat_map(|level| level.files.iter())
            .map(|file| file.file_id)
            .collect();
        for tracked in db_state.vlog_version.tracked_files() {
            tracked_file_ids.insert(tracked.file_id());
        }
        snapshot.seq_id = db_state.seq_id;
        snapshot.latest_schema_id = db_state
            .multi_lsm_version
            .tree_versions_cloned()
            .iter()
            .flat_map(|version| version.levels.iter())
            .flat_map(|level| level.files.iter())
            .map(|file| file.schema_id)
            .max()
            .unwrap_or(0);
        snapshot.referenced_schema_ids =
            collect_schema_ids_from_lsm_versions(&snapshot.lsm_versions, snapshot.latest_schema_id);
        snapshot.tracked_data_files = tracked_file_ids
            .into_iter()
            .filter_map(|file_id| {
                self.file_manager
                    .data_file_ref(file_id)
                    .ok()
                    .map(|tracked| (file_id, tracked))
            })
            .collect();
        snapshot.vlog_version = clone_vlog_version_untracked(&db_state.vlog_version);
        snapshot.base_snapshot_id = db_state.suggested_base_snapshot_id;
        snapshot.active_memtable_data = active_memtable_data;
        increment_schema_ref_counts(
            &mut state.schema_ref_counts,
            &snapshot.referenced_schema_ids,
        );
        state.snapshots.insert(id, Arc::new(snapshot));
        state.completed.insert(id);
        drop(state);
        db_state_handle.update_suggested_snapshot(db_state.seq_id, id);
        true
    }

    pub(crate) fn retain_snapshot(&self, id: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        if !state.snapshots.contains_key(&id) {
            return false;
        }
        state.retained.insert(id);
        true
    }

    pub(crate) fn active_memtable_snapshot_segments(
        &self,
        base_snapshot_id: Option<u64>,
        memtable_type: MemtableType,
        memtable_id: &str,
    ) -> Vec<ActiveMemtableSnapshotData> {
        let state = self.state.lock().unwrap();
        memtable::collect_active_memtable_snapshot_segments(
            &state.snapshots,
            base_snapshot_id,
            memtable_type,
            memtable_id,
        )
    }

    /// Import a snapshot from an existing manifest. This is used when loading snapshots from disk
    /// on startup, where the manifest files are already materialized, and we just need to populate
    /// the in-memory state.
    pub(crate) fn import_snapshot_from_manifest(
        &self,
        snapshot_id: u64,
        base_snapshot_id: Option<u64>,
        manifest: &ManifestSnapshot,
    ) -> Result<()> {
        let manifest_name = snapshot_manifest_name(snapshot_id);
        let manifest_path = self.file_manager.metadata_path(&manifest_name);
        self.file_manager
            .register_metadata_file(&manifest_name, &manifest_path)?;

        let tracked_data_files = manifest_data_file_refs(manifest)
            .map(|(file_id, path)| {
                if !self.file_manager.has_data_file(file_id) {
                    self.file_manager.register_data_file(file_id, &path)?;
                }
                self.file_manager.make_data_file_owned(file_id)?;
                self.file_manager
                    .data_file_ref(file_id)
                    .map(|tracked| (file_id, tracked))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let lsm_versions = build_tree_versions_from_manifest_untracked(manifest)?;
        let referenced_schema_ids =
            collect_schema_ids_from_lsm_versions(&lsm_versions, manifest.latest_schema_id);
        let snapshot = Arc::new(DbSnapshot {
            id: snapshot_id,
            manifest_path,
            base_snapshot_id,
            lsm_versions,
            tracked_data_files,
            vlog_version: build_vlog_version_from_manifest_untracked(manifest),
            seq_id: manifest.seq_id,
            latest_schema_id: manifest.latest_schema_id,
            referenced_schema_ids,
            active_memtable_data: manifest.active_memtable_data.clone(),
            lsm_tree_bucket_ranges: manifest.lsm_tree_bucket_ranges.clone(),
            tree_scopes: manifest.tree_scopes.clone(),
            bucket_ranges: manifest.bucket_ranges.clone(),
            finished: true,
            callback: None,
        });

        let mut state = self.state.lock().unwrap();
        state.next_id = state.next_id.max(snapshot_id.saturating_add(1));
        if state.snapshots.contains_key(&snapshot_id) {
            return Ok(());
        }
        increment_schema_ref_counts(
            &mut state.schema_ref_counts,
            &snapshot.referenced_schema_ids,
        );
        state.completed.insert(snapshot_id);
        state.snapshots.insert(snapshot_id, snapshot);
        let (inserted_self_ref, inserted_base_ref) = {
            let refs = state.incremental_references.entry(snapshot_id).or_default();
            let inserted_self_ref = refs.insert(snapshot_id);
            let inserted_base_ref = base_snapshot_id.is_some_and(|base_id| refs.insert(base_id));
            (inserted_self_ref, inserted_base_ref)
        };
        if inserted_self_ref {
            *state.incremental_ref_counts.entry(snapshot_id).or_insert(0) += 1;
        }
        if let Some(base_id) = base_snapshot_id
            && inserted_base_ref
        {
            *state.incremental_ref_counts.entry(base_id).or_insert(0) += 1;
        }
        Ok(())
    }

    pub(crate) fn process_retention(&self) -> Result<()> {
        let mut to_expire = Vec::new();
        {
            let state = self.state.lock().unwrap();
            let Some(retention) = self.retention else {
                return Ok(());
            };
            if state.completed.len() <= retention {
                return Ok(());
            }
            let keep_from = state.completed.len().saturating_sub(retention);
            state.completed.iter().take(keep_from).for_each(|id| {
                to_expire.push(*id);
            });
        }
        for id in to_expire {
            if !self.retain_snapshot(id) {
                let _ = self.expire_snapshot(id)?;
            }
        }
        Ok(())
    }

    /// Write the snapshot manifest for the given id.
    pub(crate) fn materialize(&self, id: u64) -> Result<()> {
        let (snapshot, callback, base_snapshot) = {
            let mut state = self.state.lock().unwrap();
            let snapshot = state.snapshots.get(&id).cloned();
            let callback = if let Some(current) = state.snapshots.get(&id).cloned() {
                let mut updated = (*current).clone();
                let callback = updated.callback.take();
                state.snapshots.insert(id, Arc::new(updated));
                callback
            } else {
                None
            };
            // base snapshot must be finished to be used as incremental base, otherwise treat as no base
            let base_snapshot = snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.base_snapshot_id)
                .and_then(|base_id| state.snapshots.get(&base_id))
                .filter(|snapshot| snapshot.finished)
                .cloned();
            (snapshot, callback, base_snapshot)
        };
        let manifest_info = snapshot.as_ref().map(|s| SnapshotManifestInfo {
            id: s.id,
            manifest_path: s.manifest_path.clone(),
            bucket_ranges: s.bucket_ranges.clone(),
        });
        let mut incremental_base_id = None;
        let result = match snapshot {
            Some(snapshot) => {
                let resources = Arc::new(MaterializeTempResourceRegistry::new(Arc::clone(
                    &self.file_manager,
                )));
                let prepared_result =
                    self.prepare_snapshot_for_materialization(snapshot.as_ref(), &resources);
                match prepared_result {
                    Ok(prepared) => {
                        let materialize_result = (|| {
                            self.schema_manager.persist_schemas_up_to(
                                &self.file_manager,
                                prepared.snapshot.latest_schema_id,
                            )?;
                            let writer = self
                                .file_manager
                                .create_metadata_file(&snapshot_manifest_name(id))?;
                            let mut buffered = BufferedWriter::new(writer, 8192);
                            incremental_base_id = encode_manifest(
                                &mut buffered,
                                &prepared.snapshot,
                                base_snapshot.as_deref(),
                                &self.file_manager,
                            )?;
                            buffered.close()?;
                            Ok(())
                        })();
                        match materialize_result {
                            Ok(()) => {
                                self.commit_prepared_materialization(
                                    id,
                                    incremental_base_id,
                                    prepared,
                                );
                                resources.finalize();
                                Ok(())
                            }
                            Err(err) => Err(err),
                        }
                    }
                    Err(err) => Err(err),
                }
            }
            None => Err(Error::IoError(format!("Snapshot {} not found", id))),
        };
        if let Some(callback) = callback {
            callback(
                result
                    .clone()
                    .map(|()| manifest_info.expect("snapshot present when result is Ok")),
            );
        }
        let mut state = self.state.lock().unwrap();
        state.in_flight = state.in_flight.saturating_sub(1);
        if state.in_flight == 0 {
            self.materialize_done.notify_all();
        }
        result
    }

    /// Prepare the snapshot for materialization by ensuring all referenced data files are available
    /// in the snapshot volume, and remapping the snapshot's tracked file references to point to the
    /// snapshot volume. Returns a remapped snapshot ready for manifest encoding, along with links
    /// from source data files to their corresponding snapshot data files for later reference management.
    fn prepare_snapshot_for_materialization(
        &self,
        snapshot: &DbSnapshot,
        resources: &Arc<MaterializeTempResourceRegistry>,
    ) -> Result<PreparedSnapshotMaterialization> {
        let mut file_id_map: HashMap<u64, u64> = HashMap::new();
        let mut copy_candidates = Vec::new();
        let mut source_data_files: HashMap<u64, Arc<DataFile>> = snapshot
            .lsm_versions
            .iter()
            .flat_map(|version| version.levels.iter())
            .flat_map(|level| level.files.iter())
            .map(|file| (file.file_id, Arc::clone(file)))
            .collect();
        let mut source_file_ids: BTreeSet<u64> = source_data_files.keys().copied().collect();
        source_file_ids.extend(
            snapshot
                .vlog_version
                .files_with_entries()
                .iter()
                .map(|(_, tracked_id, _)| tracked_id.file_id()),
        );
        for source_file_id in source_file_ids {
            if self
                .file_manager
                .is_data_file_persistable_for_snapshot(source_file_id)
            {
                file_id_map.insert(source_file_id, source_file_id);
                continue;
            }
            if let Some(mapped_file_id) = source_data_files
                .get(&source_file_id)
                .and_then(|data_file| data_file.snapshot_data_file_id())
                .filter(|mapped_file_id| {
                    self.file_manager
                        .is_data_file_on_snapshot_volume(*mapped_file_id)
                })
            {
                file_id_map.insert(source_file_id, mapped_file_id);
                continue;
            }
            copy_candidates.push(source_file_id);
        }
        let copied_file_id_map = self
            .copy_data_files_to_snapshot_volume_parallel(copy_candidates, Arc::clone(resources))?;
        file_id_map.extend(copied_file_id_map);
        let mut source_snapshot_links = Vec::new();
        for (source_file_id, source_data_file) in source_data_files.drain() {
            if let Some(mapped_file_id) = file_id_map.get(&source_file_id).copied()
                && mapped_file_id != source_file_id
            {
                source_snapshot_links.push((
                    source_data_file,
                    TrackedFileId::new(&self.file_manager, mapped_file_id),
                ));
            }
        }

        let mut tracked_data_files = BTreeMap::new();
        for (source_file_id, mapped_file_id) in &file_id_map {
            let tracked = self.file_manager.data_file_ref(*mapped_file_id)?;
            resources.register_reference(Arc::clone(&tracked));
            tracked_data_files.insert(*source_file_id, tracked);
        }

        let mut remapped_snapshot = snapshot.clone();
        remapped_snapshot.lsm_versions =
            remap_snapshot_tree_file_ids(&snapshot.lsm_versions, &file_id_map, &self.file_manager);
        remapped_snapshot.vlog_version =
            remap_snapshot_vlog_file_ids(&snapshot.vlog_version, &file_id_map);
        remapped_snapshot.tracked_data_files = tracked_data_files;
        Ok(PreparedSnapshotMaterialization {
            snapshot: remapped_snapshot,
            source_snapshot_links,
        })
    }

    /// Copy the given data files to the snapshot volume in parallel, and return a map from source file
    /// ids to copied file ids. Only files that are not already present in the snapshot volume will be copied.
    fn copy_data_files_to_snapshot_volume_parallel(
        &self,
        source_file_ids: Vec<u64>,
        resources: Arc<MaterializeTempResourceRegistry>,
    ) -> Result<HashMap<u64, u64>> {
        if source_file_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let file_manager = Arc::clone(&self.file_manager);
        let copy_resource_registry: Arc<dyn SnapshotCopyResourceRegistry + Send + Sync> = resources;
        self.upload_runtime.block_on(async {
            let mut join_set = JoinSet::new();
            for source_file_id in source_file_ids {
                let file_manager = Arc::clone(&file_manager);
                let copy_resource_registry = Arc::clone(&copy_resource_registry);
                join_set.spawn_blocking(move || {
                    file_manager
                        .copy_data_file_to_snapshot_volume_with_result(
                            source_file_id,
                            Some(copy_resource_registry),
                        )
                        .map(|(copied_id, _)| (source_file_id, copied_id))
                });
            }
            let mut file_id_map = HashMap::new();
            while let Some(result) = join_set.join_next().await {
                let (source_file_id, copied_id) = result.map_err(|err| {
                    Error::IoError(format!("Snapshot upload task failed to join: {}", err))
                })??;
                file_id_map.insert(source_file_id, copied_id);
            }
            Ok(file_id_map)
        })
    }

    fn commit_prepared_materialization(
        &self,
        id: u64,
        incremental_base_id: Option<u64>,
        mut prepared: PreparedSnapshotMaterialization,
    ) {
        for (source_data_file, snapshot_data_file) in &prepared.source_snapshot_links {
            source_data_file.set_snapshot_data_file(Arc::clone(snapshot_data_file));
            self.file_manager.register_snapshot_replica_hint(
                source_data_file.file_id,
                snapshot_data_file.file_id(),
            );
        }
        let mut state = self.state.lock().unwrap();
        if let Some(snapshot) = state.snapshots.get(&id).cloned() {
            snapshot
                .tracked_data_files
                .values()
                .for_each(|tracked| tracked.dereference());
        }
        prepared.snapshot.finished = true;
        prepared.snapshot.base_snapshot_id = incremental_base_id;
        state.snapshots.insert(id, Arc::new(prepared.snapshot));
        // if there is an incremental base, add reference from the new snapshot to the base;
        if let Some(base_id) = incremental_base_id {
            let inserted = state
                .incremental_references
                .entry(id)
                .or_default()
                .insert(base_id);
            if inserted {
                *state.incremental_ref_counts.entry(base_id).or_insert(0) += 1;
            }
        }
    }

    pub(crate) fn expire_snapshot(&self, id: u64) -> Result<bool> {
        let (
            removed_snapshots,
            removed_requested_snapshot,
            removed_schema_ids,
            live_schema_ids,
            live_active_data_paths,
        ) = {
            let mut state = self.state.lock().unwrap();
            if !state.snapshots.contains_key(&id) {
                return Ok(false);
            }
            let mut removed_schema_ids = BTreeSet::new();
            let mut removed_requested_snapshot = false;
            if let Some(refs) = state.incremental_references.get_mut(&id) {
                removed_requested_snapshot = refs.remove(&id);
            }
            let mut pending = Vec::new();
            if removed_requested_snapshot
                && let Some(ref_count) = state.incremental_ref_counts.get_mut(&id)
            {
                *ref_count = ref_count.saturating_sub(1);
                if *ref_count == 0 {
                    pending.push(id);
                }
            }
            let mut removed_snapshots = Vec::new();
            // recursively remove snapshots that no longer have incoming references
            while let Some(snapshot_id) = pending.pop() {
                if state
                    .incremental_ref_counts
                    .get(&snapshot_id)
                    .copied()
                    .unwrap_or(0)
                    > 0
                {
                    continue;
                }
                state.incremental_ref_counts.remove(&snapshot_id);
                let referenced = state
                    .incremental_references
                    .remove(&snapshot_id)
                    .unwrap_or_default();
                state.completed.remove(&snapshot_id);
                state.retained.remove(&snapshot_id);
                let Some(snapshot) = state.snapshots.remove(&snapshot_id) else {
                    continue;
                };
                decrement_schema_ref_counts(
                    &mut state.schema_ref_counts,
                    &snapshot.referenced_schema_ids,
                    &mut removed_schema_ids,
                );
                for referenced_id in referenced {
                    if referenced_id == snapshot_id {
                        continue;
                    }
                    if let Some(ref_count) = state.incremental_ref_counts.get_mut(&referenced_id) {
                        *ref_count = ref_count.saturating_sub(1);
                        if *ref_count == 0 {
                            pending.push(referenced_id);
                        }
                    }
                }
                removed_snapshots.push(snapshot);
            }
            (
                removed_snapshots,
                removed_requested_snapshot,
                removed_schema_ids.into_iter().collect::<Vec<_>>(),
                state
                    .schema_ref_counts
                    .keys()
                    .copied()
                    .collect::<BTreeSet<_>>(),
                state
                    .snapshots
                    .values()
                    .flat_map(|snapshot| {
                        snapshot
                            .active_memtable_data
                            .iter()
                            .map(|segment| segment.path.clone())
                    })
                    .collect::<HashSet<_>>(),
            )
        };
        let mut removed_active_data_paths = HashSet::new();
        for snapshot in &removed_snapshots {
            self.file_manager
                .remove_metadata_file(&snapshot_manifest_name(snapshot.id))?;
            // cleanup the active memtable data segments that are no longer referenced by any live snapshots
            for segment in &snapshot.active_memtable_data {
                if live_active_data_paths.contains(&segment.path) {
                    continue;
                }
                if removed_active_data_paths.insert(segment.path.clone()) {
                    self.file_manager.remove_metadata_file(&segment.path)?;
                }
            }
            snapshot
                .tracked_data_files
                .values()
                .for_each(|file| file.dereference());
        }
        let max_persisted_schema_id = self.schema_manager.max_persisted_schema_id();
        let mut schema_ids_to_remove: BTreeSet<u64> = removed_schema_ids.into_iter().collect();
        if let Some(max_schema_id) = max_persisted_schema_id {
            for schema_id in 0..=max_schema_id {
                if !live_schema_ids.contains(&schema_id) {
                    schema_ids_to_remove.insert(schema_id);
                }
            }
        }
        for schema_id in schema_ids_to_remove {
            self.file_manager
                .remove_metadata_file(&schema_file_relative_path(schema_id))?;
        }
        self.schema_manager
            .update_max_persisted_schema_id_from_live(&live_schema_ids);
        Ok(removed_requested_snapshot)
    }

    /// Enqueue a manifest materialization job on the background worker.
    pub(crate) fn schedule_materialize(&self, id: u64) -> Result<()> {
        self.start_materializer()?;
        {
            let mut state = self.state.lock().unwrap();
            state.in_flight += 1;
        }
        let tx_guard = self.materialize_tx.lock().unwrap();
        if let Some(tx) = tx_guard.as_ref() {
            if tx.send(id).is_err() {
                let mut state = self.state.lock().unwrap();
                state.in_flight = state.in_flight.saturating_sub(1);
                if state.in_flight == 0 {
                    self.materialize_done.notify_all();
                }
                return Err(Error::IoError("Snapshot worker unavailable".to_string()));
            }
        } else {
            let mut state = self.state.lock().unwrap();
            state.in_flight = state.in_flight.saturating_sub(1);
            if state.in_flight == 0 {
                self.materialize_done.notify_all();
            }
            return Err(Error::IoError("Snapshot worker unavailable".to_string()));
        }
        Ok(())
    }

    pub(crate) fn wait_for_materialization(&self, timeout: std::time::Duration) -> bool {
        let guard = self.state.lock().unwrap();
        if guard.in_flight == 0 {
            return true;
        }
        let (guard, _) = self
            .materialize_done
            .wait_timeout_while(guard, timeout, |state| state.in_flight > 0)
            .unwrap();
        guard.in_flight == 0
    }

    pub(crate) fn force_close(&self) {
        self.materialize_tx.lock().unwrap().take();
        let _ = self.materialize_worker.lock().unwrap().take();
    }

    /// Stop the background materializer worker.
    pub(crate) fn close(&self) -> Result<()> {
        if let Some(err) = self.db_lifecycle.error() {
            self.force_close();
            return Err(err);
        }
        let mut tx_guard = self.materialize_tx.lock().unwrap();
        tx_guard.take();
        drop(tx_guard);
        if let Some(err) = self.db_lifecycle.error() {
            self.force_close();
            return Err(err);
        }
        if !self.wait_for_materialization(CLOSE_WAIT_TIMEOUT) {
            self.force_close();
            return Err(Error::IoError(
                "Timed out waiting for snapshot materialization during close".to_string(),
            ));
        }
        if let Some(err) = self.db_lifecycle.error() {
            self.force_close();
            return Err(err);
        }
        let worker = self.materialize_worker.lock().unwrap().take();
        if let Some(worker) = worker {
            worker.join().map_err(|_| {
                Error::IoError("Snapshot materializer worker panicked during close".to_string())
            })?;
        }
        Ok(())
    }
}

fn remap_snapshot_tree_file_ids(
    versions: &[LSMTreeVersion],
    file_id_map: &HashMap<u64, u64>,
    file_manager: &Arc<FileManager>,
) -> Vec<LSMTreeVersion> {
    versions
        .iter()
        .map(|version| LSMTreeVersion {
            levels: version
                .levels
                .iter()
                .map(|level| Level {
                    ordinal: level.ordinal,
                    tiered: level.tiered,
                    files: level
                        .files
                        .iter()
                        .map(|file| {
                            let mapped_file_id = file_id_map
                                .get(&file.file_id)
                                .copied()
                                .unwrap_or(file.file_id);
                            let detached = DataFile::new_detached(
                                file.file_type,
                                file.start_key.clone(),
                                file.end_key.clone(),
                                file.file_id,
                                file.schema_id,
                                file.size,
                                file.bucket_range.clone(),
                                file.effective_bucket_range.clone(),
                            )
                            .with_vlog_offset(file.vlog_file_seq_offset)
                            .with_separated_values(file.has_separated_values);
                            if mapped_file_id != file.file_id {
                                if file_manager.is_data_file_on_snapshot_volume(mapped_file_id) {
                                    detached.set_snapshot_data_file(TrackedFileId::new(
                                        file_manager,
                                        mapped_file_id,
                                    ));
                                }
                            } else if let Some(snapshot_file_id) = file.snapshot_data_file_id()
                                && file_manager.is_data_file_on_snapshot_volume(snapshot_file_id)
                            {
                                detached.set_snapshot_data_file(TrackedFileId::new(
                                    file_manager,
                                    snapshot_file_id,
                                ));
                            }
                            detached.copy_meta_from(file);
                            Arc::new(detached)
                        })
                        .collect(),
                })
                .collect(),
        })
        .collect()
}

fn remap_snapshot_vlog_file_ids(
    vlog_version: &VlogVersion,
    file_id_map: &HashMap<u64, u64>,
) -> VlogVersion {
    let files = vlog_version
        .files_with_entries()
        .into_iter()
        .map(|(file_seq, tracked_id, valid_entries)| {
            let source_file_id = tracked_id.file_id();
            let remapped_file_id = file_id_map
                .get(&source_file_id)
                .copied()
                .unwrap_or(source_file_id);
            (
                file_seq,
                TrackedFileId::detached(remapped_file_id),
                valid_entries,
            )
        })
        .collect();
    VlogVersion::from_files_with_entries(files)
}

/// Collect all schema IDs referenced by the levels.
fn collect_schema_ids_from_lsm_versions(
    lsm_versions: &[LSMTreeVersion],
    latest_schema_id: u64,
) -> BTreeSet<u64> {
    let mut schema_ids = BTreeSet::new();
    schema_ids.insert(latest_schema_id);
    for version in lsm_versions {
        for level in &version.levels {
            for file in &level.files {
                if file.schema_id <= latest_schema_id {
                    for schema_id in file.schema_id..=latest_schema_id {
                        schema_ids.insert(schema_id);
                    }
                } else {
                    schema_ids.insert(file.schema_id);
                }
            }
        }
    }
    schema_ids
}

/// Increment schema reference counts for the given schema IDs.
fn increment_schema_ref_counts(
    schema_ref_counts: &mut HashMap<u64, usize>,
    schema_ids: &BTreeSet<u64>,
) {
    for schema_id in schema_ids {
        *schema_ref_counts.entry(*schema_id).or_insert(0) += 1;
    }
}

/// Decrement schema reference counts for the given schema IDs, and collect any schema IDs that are no longer referenced.
fn decrement_schema_ref_counts(
    schema_ref_counts: &mut HashMap<u64, usize>,
    schema_ids: &BTreeSet<u64>,
    removed_schema_ids: &mut BTreeSet<u64>,
) {
    for schema_id in schema_ids {
        let Some(count) = schema_ref_counts.get_mut(schema_id) else {
            continue;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            schema_ref_counts.remove(schema_id);
            removed_schema_ids.insert(*schema_id);
        }
    }
}

/// Clone an LSM tree version without tracking.
fn clone_lsm_tree_version_untracked(
    version: &LSMTreeVersion,
    file_manager: &Arc<FileManager>,
) -> LSMTreeVersion {
    LSMTreeVersion {
        levels: version
            .levels
            .iter()
            .map(|level| Level {
                ordinal: level.ordinal,
                tiered: level.tiered,
                files: level
                    .files
                    .iter()
                    .map(|file| {
                        let detached = DataFile::new_detached(
                            file.file_type,
                            file.start_key.clone(),
                            file.end_key.clone(),
                            file.file_id,
                            file.schema_id,
                            file.size,
                            file.bucket_range.clone(),
                            file.effective_bucket_range.clone(),
                        )
                        .with_vlog_offset(file.vlog_file_seq_offset)
                        .with_separated_values(file.has_separated_values);
                        if let Some(snapshot_file_id) = file.snapshot_data_file_id()
                            && file_manager.is_data_file_on_snapshot_volume(snapshot_file_id)
                        {
                            detached.set_snapshot_data_file(TrackedFileId::new(
                                file_manager,
                                snapshot_file_id,
                            ));
                        }
                        detached.copy_meta_from(file);
                        Arc::new(detached)
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Clone vlog version from the LSM version without tracking.
fn clone_vlog_version_untracked(vlog_version: &VlogVersion) -> VlogVersion {
    VlogVersion::from_files_with_entries(
        vlog_version
            .files_with_entries()
            .into_iter()
            .map(|(file_seq, tracked_id, valid_entries)| {
                (
                    file_seq,
                    TrackedFileId::detached(tracked_id.file_id()),
                    valid_entries,
                )
            })
            .collect(),
    )
}
