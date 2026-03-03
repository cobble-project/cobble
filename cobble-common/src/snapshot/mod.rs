//! Snapshot manager and manifest encoding for LSM state.
mod memtable;

use crate::config::MemtableType;
use crate::data_file::{DataFile, DataFileType};
use crate::db_state::{DbState, DbStateHandle};
use crate::error::{Error, Result};
use crate::file::{
    BufferedWriter, File, FileManager, SequentialWriteFile, TrackedFile, TrackedFileId,
};
use crate::lsm::Level;
use crate::paths::schema_file_relative_path;
use crate::schema::SchemaManager;
use crate::vlog::VlogVersion;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::str::FromStr;
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::JoinHandle;

pub(crate) use memtable::ActiveMemtableSnapshotData;

/// Internal snapshot record tracked by the manager.
#[derive(Clone)]
pub(crate) struct DbSnapshot {
    pub id: u64,
    pub manifest_path: String,
    pub base_snapshot_id: Option<u64>,
    // Levels without tracked file references.
    pub levels: Vec<Level>,
    // Tracked references to all files included in the snapshot, used for reference counting and
    // cleanup when expiring snapshots. This includes both LSM files and vlog files.
    pub tracked_files: Vec<Arc<TrackedFile>>,
    // Vlog version at the time of snapshot creation, without tracked references.
    pub vlog_version: VlogVersion,
    pub seq_id: u64,
    pub latest_schema_id: u64,
    pub referenced_schema_ids: BTreeSet<u64>,
    pub active_memtable_data: Vec<ActiveMemtableSnapshotData>,
    pub bucket_ranges: Vec<Range<u16>>,
    pub finished: bool,
    pub callback: Option<SnapshotCallback>,
}

pub(crate) type SnapshotCallback = Arc<dyn Fn(Result<u64>) + Send + Sync + 'static>;

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ManifestSnapshot {
    pub(crate) id: u64,
    pub(crate) seq_id: u64,
    pub(crate) latest_schema_id: u64,
    #[serde(default)]
    pub(crate) bucket_ranges: Vec<Range<u16>>,
    #[serde(default)]
    pub(crate) levels: Vec<ManifestLevel>,
    #[serde(default)]
    pub(crate) vlog_files: Vec<ManifestVlogFile>,
    #[serde(default)]
    pub(crate) active_memtable_data: Vec<ActiveMemtableSnapshotData>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ManifestIncrementalSnapshot {
    pub(crate) id: u64,
    pub(crate) seq_id: u64,
    pub(crate) base_snapshot_id: u64,
    pub(crate) latest_schema_id: u64,
    #[serde(default)]
    pub(crate) bucket_ranges: Vec<Range<u16>>,
    #[serde(default)]
    pub(crate) level_edits: Vec<ManifestLevelEdit>,
    // always include vlog file info in incremental manifests since vlog files are more likely to have changes
    #[serde(default)]
    pub(crate) vlog_files: Vec<ManifestVlogFile>,
    #[serde(default)]
    pub(crate) active_memtable_data: Vec<ActiveMemtableSnapshotData>,
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
    pub(crate) seq: u64,
    pub(crate) schema_id: u64,
    pub(crate) size: usize,
    pub(crate) start_key: String,
    pub(crate) end_key: String,
    pub(crate) path: String,
    pub(crate) has_separated_values: bool,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ManifestLevelEdit {
    pub(crate) level: u8,
    pub(crate) tiered: bool,
    #[serde(default)]
    pub(crate) removed_file_ids: Vec<u64>,
    #[serde(default)]
    pub(crate) new_files: Vec<ManifestFile>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ManifestVlogFile {
    pub(crate) file_seq: u32,
    pub(crate) file_id: u64,
    pub(crate) path: String,
    pub(crate) valid_entries: u64,
}

impl DbSnapshot {
    pub(crate) fn new(id: u64, manifest_path: String, callback: Option<SnapshotCallback>) -> Self {
        Self {
            id,
            manifest_path,
            base_snapshot_id: None,
            levels: vec![],
            tracked_files: Vec::new(),
            vlog_version: VlogVersion::new(),
            seq_id: 0,
            latest_schema_id: 0,
            referenced_schema_ids: BTreeSet::new(),
            active_memtable_data: Vec::new(),
            bucket_ranges: Vec::new(),
            finished: false,
            callback,
        }
    }
}

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
        snapshot.levels = clone_levels_untracked(&db_state.lsm_version.levels);
        let mut tracked_file_ids: BTreeSet<u64> = db_state
            .lsm_version
            .levels
            .iter()
            .flat_map(|level| level.files.iter().map(|file| file.file_id))
            .collect();
        for tracked in db_state.vlog_version.tracked_files() {
            tracked_file_ids.insert(tracked.file_id());
        }
        snapshot.seq_id = db_state.seq_id;
        snapshot.latest_schema_id = db_state
            .lsm_version
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
            bucket_ranges: if manifest.bucket_ranges.is_empty() {
                self.bucket_ranges.clone()
            } else {
                manifest.bucket_ranges.clone()
            },
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
            levels: manifest_levels_from_snapshot(&snapshot.levels, file_manager),
            vlog_files: manifest_vlog_files_from_snapshot(snapshot, file_manager),
            active_memtable_data: snapshot.active_memtable_data.clone(),
        })
    };
    let mut incremental_base_id = None;
    let manifest = if let Some(base) = base_snapshot {
        if let Some(level_edits) = build_incremental_level_edits(base, snapshot, file_manager) {
            incremental_base_id = Some(base.id);
            ManifestPayload::IncrementalSnapshot(ManifestIncrementalSnapshot {
                id: snapshot.id,
                seq_id: snapshot.seq_id,
                base_snapshot_id: base.id,
                latest_schema_id: snapshot.latest_schema_id,
                bucket_ranges: snapshot.bucket_ranges.clone(),
                level_edits,
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

fn manifest_levels_from_snapshot(
    levels: &[Level],
    file_manager: &FileManager,
) -> Vec<ManifestLevel> {
    levels
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
}

fn manifest_file_from_data_file(file: &DataFile, file_manager: &FileManager) -> ManifestFile {
    ManifestFile {
        file_id: file.file_id,
        file_type: file.file_type.as_str().to_string(),
        seq: file.seq,
        schema_id: file.schema_id,
        size: file.size,
        start_key: to_hex(&file.start_key),
        end_key: to_hex(&file.end_key),
        path: file_manager
            .get_data_file_full_path(file.file_id)
            .expect("Unknown file ID"),
        has_separated_values: file.has_separated_values,
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
    base: &DbSnapshot,
    snapshot: &DbSnapshot,
    file_manager: &FileManager,
) -> Option<Vec<ManifestLevelEdit>> {
    let mut edits = Vec::new();
    for level in &snapshot.levels {
        let base_level = base
            .levels
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
    for base_level in &base.levels {
        if !snapshot
            .levels
            .iter()
            .any(|level| level.ordinal == base_level.ordinal)
            && !base_level.files.is_empty()
        {
            return None;
        }
    }
    if edits.is_empty() { None } else { Some(edits) }
}

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{:02x}", b);
    }
    out
}

pub(crate) fn decode_manifest(bytes: &[u8]) -> Result<ManifestPayload> {
    serde_json::from_slice(bytes)
        .map_err(|err| Error::IoError(format!("Failed to decode manifest: {}", err)))
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
                apply_manifest_level_edits(&mut resolved_base.levels, &manifest.level_edits)?;
                resolved_base.vlog_files = manifest.vlog_files;
                resolved_base.id = manifest.id;
                resolved_base.seq_id = manifest.seq_id;
                resolved_base.latest_schema_id = manifest.latest_schema_id;
                resolved_base.active_memtable_data = manifest.active_memtable_data;
                if !manifest.bucket_ranges.is_empty() {
                    resolved_base.bucket_ranges = manifest.bucket_ranges;
                }
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

pub(crate) fn apply_manifest_level_edits(
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

/// Extract the file ID and path references for all data files in the manifest, deduplicating by file ID.
fn manifest_data_file_refs(manifest: &ManifestSnapshot) -> impl Iterator<Item = (u64, String)> {
    let mut refs: BTreeMap<u64, String> = BTreeMap::new();
    for level in &manifest.levels {
        for file in &level.files {
            refs.entry(file.file_id)
                .or_insert_with(|| file.path.clone());
        }
    }
    for file in &manifest.vlog_files {
        refs.entry(file.file_id)
            .or_insert_with(|| file.path.clone());
    }
    refs.into_iter()
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

fn build_levels_from_manifest_untracked(manifest: &ManifestSnapshot) -> Result<Vec<Level>> {
    let mut levels = Vec::with_capacity(manifest.levels.len());
    for level in &manifest.levels {
        let mut files = Vec::with_capacity(level.files.len());
        for file in &level.files {
            let file_type = DataFileType::from_str(&file.file_type).map_err(Error::IoError)?;
            let start_key = from_hex(&file.start_key)?;
            let end_key = from_hex(&file.end_key)?;
            files.push(Arc::new(DataFile {
                file_type,
                start_key,
                end_key,
                file_id: file.file_id,
                tracked_id: TrackedFileId::detached(file.file_id),
                seq: file.seq,
                schema_id: file.schema_id,
                size: file.size,
                has_separated_values: file.has_separated_values,
                meta_bytes: Default::default(),
            }));
        }
        levels.push(Level {
            ordinal: level.ordinal,
            tiered: level.tiered,
            files,
        });
    }
    Ok(levels)
}

fn build_vlog_version_from_manifest_untracked(manifest: &ManifestSnapshot) -> VlogVersion {
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

pub(crate) fn build_levels_from_manifest(
    file_manager: &Arc<FileManager>,
    manifest: ManifestSnapshot,
    read_only: bool,
) -> Result<Vec<Level>> {
    let mut levels = Vec::with_capacity(manifest.levels.len());
    for level in manifest.levels {
        let mut files = Vec::with_capacity(level.files.len());
        for file in level.files {
            let file_type = DataFileType::from_str(&file.file_type).map_err(Error::IoError)?;
            let start_key = from_hex(&file.start_key)?;
            let end_key = from_hex(&file.end_key)?;
            let tracked_id = if read_only {
                file_manager.register_data_file_readonly(file.file_id, file.path)?;
                TrackedFileId::detached(file.file_id)
            } else {
                file_manager.register_data_file(file.file_id, file.path)?;
                TrackedFileId::new(file_manager, file.file_id)
            };
            files.push(Arc::new(DataFile {
                file_type,
                start_key,
                end_key,
                file_id: file.file_id,
                tracked_id,
                seq: file.seq,
                schema_id: file.schema_id,
                size: file.size,
                has_separated_values: file.has_separated_values,
                meta_bytes: Default::default(),
            }));
        }
        levels.push(Level {
            ordinal: level.ordinal,
            tiered: level.tiered,
            files,
        });
    }
    Ok(levels)
}

pub(crate) fn build_vlog_version_from_manifest(
    file_manager: &Arc<FileManager>,
    manifest: &ManifestSnapshot,
    read_only: bool,
) -> Result<VlogVersion> {
    let mut files = Vec::with_capacity(manifest.vlog_files.len());
    for vlog_file in &manifest.vlog_files {
        let tracked_id = if read_only {
            file_manager.register_data_file_readonly(vlog_file.file_id, vlog_file.path.clone())?;
            TrackedFileId::detached(vlog_file.file_id)
        } else {
            file_manager.register_data_file(vlog_file.file_id, vlog_file.path.clone())?;
            TrackedFileId::new(file_manager, vlog_file.file_id)
        };
        files.push((vlog_file.file_seq, tracked_id, vlog_file.valid_entries));
    }
    Ok(VlogVersion::from_files_with_entries(files))
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
