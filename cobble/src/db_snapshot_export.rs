use super::Db;
use crate::config::VolumeDescriptor;
use crate::error::{Error, Result};
use crate::file::logical_file::ReplicaOrigin;
use crate::file::{File, FileManager, FileSystem, FileSystemRegistry, MetadataWriter};
use crate::manifest_model::manifest_schema_ids;
use crate::paths::{metadata_temporary_path, schema_file_relative_path};
use crate::schema::{SchemaManager, snapshot_column_families_from_schema};
use crate::snapshot::ShardSnapshotMetadata;
use crate::snapshot::manifest::{ManifestPayload, ManifestSnapshot, encode_manifest_payload};
use crate::snapshot::{load_manifest_for_snapshot, snapshot_manifest_name};
use crate::util::normalize_storage_path_to_url;
use std::collections::{BTreeMap, btree_map::Entry};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use tokio::sync::{Notify, OwnedSemaphorePermit};
use tokio::task::JoinSet;
use url::Url;
use uuid::Uuid;

const PENDING: u8 = 0;
const CANCELLED: u8 = 1;
const COMMIT_STARTED: u8 = 2;
const FINISHED: u8 = 3;

struct ExportState {
    phase: AtomicU8,
    result: Mutex<Option<Result<ShardSnapshotMetadata>>>,
    completed: Condvar,
    completed_async: Notify,
    cancelled: Notify,
}

impl ExportState {
    fn cancel(&self) -> bool {
        if self
            .phase
            .compare_exchange(PENDING, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.cancelled.notify_waiters();
            true
        } else {
            false
        }
    }

    fn check_cancelled(&self) -> Result<()> {
        if self.phase.load(Ordering::Acquire) == CANCELLED {
            Err(Error::CancelledError("Snapshot export cancelled".into()))
        } else {
            Ok(())
        }
    }

    async fn cancellation(&self) {
        let notified = self.cancelled.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.phase.load(Ordering::Acquire) != CANCELLED {
            notified.await;
        }
    }

    fn finish(&self, result: Result<ShardSnapshotMetadata>) {
        let mut stored = self.result.lock().unwrap();
        *stored = Some(result);
        self.phase.store(FINISHED, Ordering::Release);
        self.completed.notify_all();
        self.completed_async.notify_waiters();
    }
}

/// A cloneable result and cancellation handle for a protected background snapshot export.
/// Dropping handles does not cancel or abort work; use `cancel` explicitly. The task retains
/// source and database protection until all started I/O has drained and cleanup has finished.
#[derive(Clone)]
pub struct ExportSnapshotHandle {
    state: Arc<ExportState>,
}

impl ExportSnapshotHandle {
    /// Request cancellation before manifest publication starts. Returns whether this request won.
    pub fn cancel(&self) -> bool {
        self.state.cancel()
    }

    pub fn is_finished(&self) -> bool {
        self.state.phase.load(Ordering::Acquire) == FINISHED
    }

    /// Block for the cached result. Do not call on an async runtime worker; use `result` instead.
    pub fn wait(&self) -> Result<ShardSnapshotMetadata> {
        let mut result = self.state.result.lock().unwrap();
        loop {
            if let Some(result) = &*result {
                return result.clone();
            }
            result = self.state.completed.wait(result).unwrap();
        }
    }

    /// Await the cached result without blocking a runtime worker. Concurrent waiters are supported.
    pub async fn result(&self) -> Result<ShardSnapshotMetadata> {
        loop {
            let notified = self.state.completed_async.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.state.result.lock().unwrap().clone() {
                return result;
            }
            notified.await;
        }
    }
}

struct ExportCopy {
    source: String,
    destination: String,
}

struct PreparedExport {
    target: ExportDestination,
    manifest: ManifestSnapshot,
    metadata: ShardSnapshotMetadata,
    copies: Vec<ExportCopy>,
}

async fn acquire_export_transfer_permit(
    file_manager: &FileManager,
    state: &ExportState,
) -> Result<OwnedSemaphorePermit> {
    let permit = tokio::select! {
        biased;
        _ = state.cancellation() => return Err(Error::CancelledError("Snapshot export cancelled while queued".into())),
        permit = file_manager.transfer_semaphore().acquire_owned() => permit.map_err(|_| Error::IoError("Snapshot transfer runtime stopped".into()))?,
    };
    state.check_cancelled()?;
    Ok(permit)
}

struct ExportDestination {
    fs: Arc<dyn FileSystem>,
    files: Vec<String>,
    directories: Vec<String>,
    committed: bool,
}

impl ExportDestination {
    fn create_directory(&mut self, path: String) -> Result<()> {
        self.directories.push(path.clone());
        self.fs.create_dir(&path)
    }

    fn own_file(&mut self, path: String) -> String {
        self.files.push(path.clone());
        path
    }
}

impl Drop for ExportDestination {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        for path in self.files.iter().rev().chain(self.directories.iter().rev()) {
            if let Err(error) = self.fs.delete(path) {
                log::warn!(
                    "failed to clean unpublished snapshot export {}: {}",
                    path,
                    error
                );
            }
        }
    }
}

fn storage_url(path: &str) -> Result<Url> {
    let mut url = Url::parse(&normalize_storage_path_to_url(path)?)
        .map_err(|error| Error::ConfigError(format!("Invalid export storage route: {error}")))?;
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

fn append_path(base: &Url, relative: &str) -> Result<Url> {
    let mut url = base.clone();
    let mut segments = url.path_segments_mut().map_err(|_| {
        Error::ConfigError("Snapshot export requires a hierarchical storage URL".into())
    })?;
    segments.pop_if_empty();
    for component in relative
        .split('/')
        .filter(|component| !component.is_empty())
    {
        segments.push(component);
    }
    drop(segments);
    Ok(url)
}

fn canonical_future_path(path: &Path) -> Result<PathBuf> {
    let mut existing = path;
    let mut missing = Vec::new();
    loop {
        match std::fs::canonicalize(existing) {
            Ok(mut canonical) => {
                for component in missing.into_iter().rev() {
                    canonical.push(component);
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = existing.file_name().ok_or_else(|| {
                    Error::ConfigError("Cannot resolve snapshot export destination".into())
                })?;
                missing.push(name.to_os_string());
                existing = existing.parent().ok_or_else(|| {
                    Error::ConfigError("Cannot resolve snapshot export destination parent".into())
                })?;
            }
            Err(error) => return Err(Error::IoError(error.to_string())),
        }
    }
}

fn overlaps(left: &Url, right: &Url) -> Result<bool> {
    if left.scheme() != right.scheme()
        || left.host_str() != right.host_str()
        || left.port() != right.port()
    {
        return Ok(false);
    }
    if left.scheme() == "file" {
        let local = |url: &Url| {
            url.to_file_path()
                .map_err(|_| Error::ConfigError("Invalid local export path".into()))
                .and_then(|path| canonical_future_path(&path))
        };
        let left = local(left)?;
        let right = local(right)?;
        return Ok(left.starts_with(&right) || right.starts_with(&left));
    }
    let contains = |parent: &str, child: &str| {
        child
            .strip_prefix(parent.trim_end_matches('/'))
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
    };
    Ok(contains(left.path(), right.path()) || contains(right.path(), left.path()))
}

impl Db {
    /// Export one published snapshot into an independent, fresh UUID directory.
    ///
    /// Copies run in parallel on the snapshot upload runtime and share its transfer semaphore.
    /// `destination` supplies filesystem access options, which are not persisted in the returned
    /// paths. The returned database id is the export directory's UUID, not this database's id.
    /// The full manifest preserves logical file ids and captured state, without WAL, incremental
    /// bases, or references to source storage. Exported files are never owned by this database.
    ///
    /// The returned handle supports early cancellation and blocking or asynchronous result retrieval.
    /// Once manifest publication starts, cancellation loses. Failure cleans only the
    /// unpublished export's private files, after its synchronous writers have stopped.
    ///
    /// Snapshot expiration is allowed during export; a counted temporary pin protects all source
    /// dependencies until copying finishes. Uncoordinated external writers and storage aliases
    /// not represented by filesystem URLs are unsupported. Local symlink overlap is rejected.
    pub fn export_snapshot(
        &self,
        snapshot_id: u64,
        destination: VolumeDescriptor,
    ) -> Result<ExportSnapshotHandle> {
        self.export_snapshot_to_filesystem(snapshot_id, destination, None)
    }

    fn export_snapshot_to_filesystem(
        &self,
        snapshot_id: u64,
        destination: VolumeDescriptor,
        fs: Option<Arc<dyn FileSystem>>,
    ) -> Result<ExportSnapshotHandle> {
        let access = self.db_lifecycle.begin_owned_access()?;
        let pin = self.snapshot_manager.pin_completed_snapshot(snapshot_id)?;
        let file_manager = Arc::clone(&self.file_manager);
        let schema_manager = Arc::clone(&self.schema_manager);
        let state = Arc::new(ExportState {
            phase: AtomicU8::new(PENDING),
            result: Mutex::new(None),
            completed: Condvar::new(),
            completed_async: Notify::new(),
            cancelled: Notify::new(),
        });
        let handle = ExportSnapshotHandle {
            state: Arc::clone(&state),
        };
        let runtime = self.snapshot_manager.upload_runtime();
        let runtime_owner = Arc::clone(&runtime);
        runtime.spawn(async move {
            let result = run_export(
                file_manager,
                schema_manager,
                snapshot_id,
                destination,
                fs,
                Arc::clone(&state),
            )
            .await;
            let release = tokio::task::spawn_blocking(move || drop(pin)).await;
            let result = result.and_then(|metadata| {
                release.map(|()| metadata).map_err(|error| {
                    Error::IoError(format!("Snapshot export pin release failed: {error}"))
                })
            });
            state.finish(result);
            drop(access);
            drop(runtime_owner);
        });
        Ok(handle)
    }
}

impl PreparedExport {
    fn prepare(
        file_manager: &Arc<FileManager>,
        schema_manager: &SchemaManager,
        snapshot_id: u64,
        destination: VolumeDescriptor,
        fs: Arc<dyn FileSystem>,
        state: &ExportState,
    ) -> Result<Self> {
        state.check_cancelled()?;
        let mut manifest = load_manifest_for_snapshot(file_manager, snapshot_id)?;
        let export_id = Uuid::new_v4().to_string();
        let export_url = append_path(&storage_url(&destination.base_dir)?, &export_id)?;
        let mut source_roots = file_manager.data_volume_absolute_paths("");
        source_roots.push(file_manager.metadata_file_full_path(""));
        let mut file_paths = BTreeMap::new();
        let data_paths = manifest
            .tree_levels
            .iter()
            .flatten()
            .flat_map(|level| &level.files)
            .map(|file| (file.file_id, &file.path))
            .chain(
                manifest
                    .vlog_files
                    .iter()
                    .map(|file| (file.file_id, &file.path)),
            );
        for (file_id, path) in data_paths {
            match file_paths.entry(file_id) {
                Entry::Vacant(entry) => {
                    entry.insert(path.clone());
                }
                Entry::Occupied(entry) if entry.get() != path => {
                    return Err(Error::InvalidState(format!(
                        "Conflicting paths for file {file_id}"
                    )));
                }
                Entry::Occupied(_) => {}
            }
        }
        for path in file_paths.values() {
            let mut url = storage_url(path)?;
            // Normal data replicas live beneath <database>/data/<physical-file>.
            if let Ok(mut segments) = url.path_segments_mut() {
                segments.pop().pop();
            }
            source_roots.push(url.to_string());
        }
        source_roots.sort_unstable();
        source_roots.dedup();
        for source_root in source_roots {
            if overlaps(&export_url, &storage_url(&source_root)?)? {
                return Err(Error::ConfigError(
                    "Snapshot export destination overlaps source storage".into(),
                ));
            }
        }
        if fs.exists(&export_id)? || fs.exists(&format!("{export_id}/"))? {
            return Err(Error::InvalidState(
                "Snapshot export destination already exists".into(),
            ));
        }
        let schema = schema_manager.schema(manifest.latest_schema_id)?;
        let mut target = ExportDestination {
            fs,
            files: Vec::new(),
            directories: Vec::new(),
            committed: false,
        };
        target.create_directory(export_id.clone())?;
        for directory in ["data", "schema", "snapshot"] {
            target.create_directory(format!("{export_id}/{directory}"))?;
        }
        let mut copies = Vec::new();
        for (file_id, source_path) in &mut file_paths {
            let relative = format!("data/{file_id}.cb");
            let path = target.own_file(format!("{export_id}/{relative}"));
            let source = std::mem::replace(
                source_path,
                append_path(&export_url, &relative)?.to_string(),
            );
            copies.push(ExportCopy {
                source,
                destination: path,
            });
        }
        for file in manifest
            .tree_levels
            .iter_mut()
            .flatten()
            .flat_map(|level| &mut level.files)
        {
            file.path = file_paths[&file.file_id].clone();
            file.origin = ReplicaOrigin::Owned;
        }
        for file in &mut manifest.vlog_files {
            file.path = file_paths[&file.file_id].clone();
            file.origin = ReplicaOrigin::Owned;
        }
        for schema_id in manifest_schema_ids(manifest.latest_schema_id, &manifest.tree_levels) {
            let relative = schema_file_relative_path(schema_id);
            let path = target.own_file(format!("{export_id}/{relative}"));
            copies.push(ExportCopy {
                source: file_manager.metadata_file_full_path(&relative),
                destination: path,
            });
        }
        let mut active_paths: BTreeMap<String, String> = BTreeMap::new();
        for segment in &mut manifest.active_memtable_data {
            let relative = if let Some(relative) = active_paths.get(&segment.path) {
                relative.clone()
            } else {
                let relative = format!("snapshot/SNAPSHOT-ACTIVE-DATA-{}", Uuid::new_v4());
                let path = target.own_file(format!("{export_id}/{relative}"));
                copies.push(ExportCopy {
                    source: file_manager.metadata_file_full_path(&segment.path),
                    destination: path,
                });
                active_paths.insert(segment.path.clone(), relative.clone());
                relative
            };
            segment.path = relative;
        }
        manifest.wal_checkpoint_id = 0;
        manifest.wal_volume = None;
        manifest.incremental_data_size_bytes = manifest.data_size_bytes;
        let manifest_relative = snapshot_manifest_name(snapshot_id);
        let metadata = ShardSnapshotMetadata {
            ranges: manifest.bucket_ranges.clone(),
            db_id: export_id,
            snapshot_id,
            manifest_path: append_path(&export_url, &manifest_relative)?.to_string(),
            timestamp_seconds: manifest.timestamp_seconds,
            data_size_bytes: manifest.data_size_bytes,
            incremental_data_size_bytes: manifest.incremental_data_size_bytes,
            schema_id: manifest.latest_schema_id,
            column_families: snapshot_column_families_from_schema(schema.as_ref()),
        };
        Ok(Self {
            target,
            manifest,
            metadata,
            copies,
        })
    }

    fn commit(mut self, state: &ExportState) -> Result<ShardSnapshotMetadata> {
        state.check_cancelled()?;
        let final_path = self.target.own_file(format!(
            "{}/{}",
            self.metadata.db_id,
            snapshot_manifest_name(self.metadata.snapshot_id)
        ));
        let temporary_path = self.target.own_file(metadata_temporary_path(&final_path));
        let mut writer = MetadataWriter::new(self.target.fs.open_write(&temporary_path)?);
        let write_result =
            encode_manifest_payload(&mut writer, &ManifestPayload::Snapshot(self.manifest));
        let close_result = writer.close();
        drop(writer);
        write_result?;
        close_result?;
        state
            .phase
            .compare_exchange(PENDING, COMMIT_STARTED, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                Error::CancelledError("Snapshot export cancelled before publication".into())
            })?;
        self.target.fs.rename(&temporary_path, &final_path)?;
        self.target.committed = true;
        Ok(self.metadata)
    }
}

async fn run_export(
    file_manager: Arc<FileManager>,
    schema_manager: Arc<SchemaManager>,
    snapshot_id: u64,
    destination: VolumeDescriptor,
    fs: Option<Arc<dyn FileSystem>>,
    state: Arc<ExportState>,
) -> Result<ShardSnapshotMetadata> {
    let prepare_manager = Arc::clone(&file_manager);
    let prepare_state = Arc::clone(&state);
    let mut prepared = tokio::task::spawn_blocking(move || {
        prepare_state.check_cancelled()?;
        let fs = match fs {
            Some(fs) => fs,
            None => FileSystemRegistry::new().get_or_register_volume(&destination)?,
        };
        PreparedExport::prepare(
            &prepare_manager,
            &schema_manager,
            snapshot_id,
            destination,
            fs,
            &prepare_state,
        )
    })
    .await
    .map_err(|error| Error::IoError(format!("Snapshot export preparation failed: {error}")))??;
    let mut tasks = JoinSet::new();
    for copy in std::mem::take(&mut prepared.copies) {
        let file_manager = Arc::clone(&file_manager);
        let fs = Arc::clone(&prepared.target.fs);
        let state = Arc::clone(&state);
        tasks.spawn(async move {
            let permit = acquire_export_transfer_permit(&file_manager, &state).await?;
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                file_manager.copy_file_to_external(
                    &copy.source,
                    fs.as_ref(),
                    &copy.destination,
                    || state.check_cancelled(),
                )
            })
            .await
            .map_err(|error| Error::IoError(format!("Snapshot export copy failed: {error}")))?
        });
    }
    let mut first_error = None;
    // Never abort wrappers: aborting one would detach its still-running blocking copy.
    while let Some(result) = tasks.join_next().await {
        let result = result
            .map_err(|error| Error::IoError(format!("Snapshot export task failed: {error}")))
            .and_then(|result| result);
        if let Err(error) = result {
            if first_error.is_none() {
                first_error = Some(error);
            }
            state.cancel();
        }
    }
    tokio::task::spawn_blocking(move || {
        if let Some(error) = first_error {
            drop(prepared);
            Err(error)
        } else {
            prepared.commit(&state)
        }
    })
    .await
    .map_err(|error| Error::IoError(format!("Snapshot export publication failed: {error}")))?
}

#[cfg(test)]
#[path = "../tests/unit/db_snapshot_export.rs"]
mod tests;
