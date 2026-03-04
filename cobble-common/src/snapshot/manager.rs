use super::manifest::{
    ManifestSnapshot, build_levels_from_manifest_untracked,
    build_vlog_version_from_manifest_untracked, encode_manifest, manifest_data_file_refs,
    snapshot_manifest_name,
};
use super::{ActiveMemtableSnapshotData, DbSnapshot, SnapshotCallback, memtable};
use crate::config::MemtableType;
use crate::data_file::DataFile;
use crate::db_state::{DbState, DbStateHandle};
use crate::error::{Error, Result};
use crate::file::{BufferedWriter, File, FileManager, TrackedFileId};
use crate::lsm::Level;
use crate::paths::schema_file_relative_path;
use crate::schema::SchemaManager;
use crate::vlog::VlogVersion;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::JoinHandle;

pub(crate) struct SnapshotManager {
    file_manager: Arc<FileManager>,
    schema_manager: Arc<SchemaManager>,
    bucket_ranges: Vec<Range<u16>>,
    state: Arc<Mutex<SnapshotManagerState>>,
    retention: Option<usize>,
    /// Background worker for manifest materialization.
    materialize_tx: Arc<Mutex<Option<mpsc::Sender<u64>>>>,
    materialize_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    materialize_done: Arc<Condvar>,
}

impl Clone for SnapshotManager {
    fn clone(&self) -> Self {
        Self {
            file_manager: Arc::clone(&self.file_manager),
            schema_manager: Arc::clone(&self.schema_manager),
            bucket_ranges: self.bucket_ranges.clone(),
            state: Arc::clone(&self.state),
            retention: self.retention,
            materialize_tx: Arc::clone(&self.materialize_tx),
            materialize_worker: Arc::clone(&self.materialize_worker),
            materialize_done: Arc::clone(&self.materialize_done),
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

impl SnapshotManager {
    pub(crate) fn new(
        file_manager: Arc<FileManager>,
        schema_manager: Arc<SchemaManager>,
        retention: Option<usize>,
        bucket_ranges: Vec<Range<u16>>,
    ) -> Self {
        Self {
            file_manager,
            schema_manager,
            bucket_ranges,
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
        let mut snapshot = DbSnapshot::new(id, manifest_path, callback);
        snapshot.bucket_ranges = self.bucket_ranges.clone();
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
        let primary_lsm_version = db_state.multi_lsm_version.version_of_index(0);
        snapshot.levels = clone_levels_untracked(&primary_lsm_version.levels);
        let mut tracked_file_ids: BTreeSet<u64> = db_state
            .multi_lsm_version
            .version_of_index(0)
            .levels
            .iter()
            .flat_map(|level| level.files.iter().map(|file| file.file_id))
            .collect();
        for tracked in db_state.vlog_version.tracked_files() {
            tracked_file_ids.insert(tracked.file_id());
        }
        snapshot.seq_id = db_state.seq_id;
        snapshot.latest_schema_id = db_state
            .multi_lsm_version
            .version_of_index(0)
            .levels
            .iter()
            .flat_map(|level| level.files.iter().map(|file| file.schema_id))
            .max()
            .unwrap_or(0);
        snapshot.referenced_schema_ids =
            collect_schema_ids_from_levels(&snapshot.levels, snapshot.latest_schema_id);
        snapshot.tracked_files = tracked_file_ids
            .into_iter()
            .filter_map(|file_id| self.file_manager.data_file_ref(file_id).ok())
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
        memtable_seq: u64,
    ) -> Vec<ActiveMemtableSnapshotData> {
        let state = self.state.lock().unwrap();
        memtable::collect_active_memtable_snapshot_segments(
            &state.snapshots,
            base_snapshot_id,
            memtable_type,
            memtable_seq,
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
            .register_metadata_file(&manifest_name, manifest_path.clone())?;

        let tracked_files = manifest_data_file_refs(manifest)
            .map(|(file_id, path)| {
                if !self.file_manager.has_data_file(file_id) {
                    self.file_manager.register_data_file(file_id, path)?;
                }
                self.file_manager.data_file_ref(file_id)
            })
            .collect::<Result<Vec<_>>>()?;
        let levels = build_levels_from_manifest_untracked(manifest)?;
        let referenced_schema_ids =
            collect_schema_ids_from_levels(&levels, manifest.latest_schema_id);
        let snapshot = Arc::new(DbSnapshot {
            id: snapshot_id,
            manifest_path,
            base_snapshot_id,
            levels,
            tracked_files,
            vlog_version: build_vlog_version_from_manifest_untracked(manifest),
            seq_id: manifest.seq_id,
            latest_schema_id: manifest.latest_schema_id,
            referenced_schema_ids,
            active_memtable_data: manifest.active_memtable_data.clone(),
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
        let mut incremental_base_id = None;
        let result = match snapshot {
            Some(snapshot) => (|| {
                self.schema_manager
                    .persist_schemas_up_to(&self.file_manager, snapshot.latest_schema_id)?;
                let writer = self
                    .file_manager
                    .create_metadata_file(&snapshot_manifest_name(id))?;
                let mut buffered = BufferedWriter::new(writer, 8192);
                incremental_base_id = encode_manifest(
                    &mut buffered,
                    &snapshot,
                    base_snapshot.as_deref(),
                    &self.file_manager,
                )?;
                buffered.close()?;
                Ok(())
            })(),
            None => Err(Error::IoError(format!("Snapshot {} not found", id))),
        };
        if result.is_ok() {
            let mut state = self.state.lock().unwrap();
            if let Some(snapshot) = state.snapshots.get(&id).cloned() {
                let mut updated = (*snapshot).clone();
                updated.finished = true;
                updated.base_snapshot_id = incremental_base_id;
                state.snapshots.insert(id, Arc::new(updated));
            }
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
        if let Some(callback) = callback {
            callback(result.clone().map(|_| id));
        }
        let mut state = self.state.lock().unwrap();
        state.in_flight = state.in_flight.saturating_sub(1);
        if state.in_flight == 0 {
            self.materialize_done.notify_all();
        }
        result
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
                .tracked_files
                .iter()
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

    /// Stop the background materializer worker.
    pub(crate) fn close(&self) -> Result<()> {
        let mut tx_guard = self.materialize_tx.lock().unwrap();
        tx_guard.take();
        let worker = self.materialize_worker.lock().unwrap().take();
        if let Some(worker) = worker {
            let _ = worker.join();
        }
        Ok(())
    }
}

/// Collect all schema IDs referenced by the levels.
fn collect_schema_ids_from_levels(levels: &[Level], latest_schema_id: u64) -> BTreeSet<u64> {
    let mut schema_ids = BTreeSet::new();
    schema_ids.insert(latest_schema_id);
    for level in levels {
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

/// Clone levels and files from the LSM version without tracking.
fn clone_levels_untracked(levels: &[Level]) -> Vec<Level> {
    levels
        .iter()
        .map(|level| Level {
            ordinal: level.ordinal,
            tiered: level.tiered,
            files: level
                .files
                .iter()
                .map(|file| {
                    let detached = DataFile {
                        file_type: file.file_type,
                        start_key: file.start_key.clone(),
                        end_key: file.end_key.clone(),
                        file_id: file.file_id,
                        tracked_id: TrackedFileId::detached(file.file_id),
                        seq: file.seq,
                        schema_id: file.schema_id,
                        size: file.size,
                        has_separated_values: file.has_separated_values,
                        meta_bytes: Default::default(),
                    };
                    if let Some(meta_bytes) = file.meta_bytes() {
                        detached.set_meta_bytes(meta_bytes);
                    }
                    Arc::new(detached)
                })
                .collect(),
        })
        .collect()
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
