use super::{
    LoadedRuntimeManifest, RuntimeManifest, RuntimeManifestEnvelope, RuntimeManifestPayload,
    RuntimeManifestStore, build_runtime_manifest,
};
use crate::config::CompactionMode;
use crate::db_state::{DbState, DbStateHandle};
use crate::db_status::DbLifecycle;
use crate::error::{Error, Result};
use crate::file::FileManager;
use crate::lsm::LSMTreeVersion;
use crate::manifest_model::{
    ManifestLevel, ManifestVlogFile, manifest_file_from_data_file_with_origin,
    manifest_truncation_cursors,
};
use crate::schema::SchemaManager;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const PUBLISHER_WAIT: Duration = Duration::from_millis(250);

/// Publishes the writer's persisted LSM state to the runtime-manifest store.
///
/// Publication is serialized so generation assignment and base selection remain
/// consistent even when a future compaction barrier calls `publish_at_least`
/// concurrently with the background observer.
pub(crate) struct RuntimeManifestPublisherHandle {
    publisher: Arc<RuntimeManifestPublisher>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

struct RuntimeManifestPublisher {
    store: RuntimeManifestStore,
    file_manager: Arc<FileManager>,
    schema_manager: Arc<SchemaManager>,
    db_state: Arc<DbStateHandle>,
    lifecycle: Arc<DbLifecycle>,
    compaction_mode: CompactionMode,
    publication: Mutex<PublicationState>,
    stop: AtomicBool,
}

struct PublicationState {
    current: Option<LoadedRuntimeManifest>,
    next_generation: u64,
    suspended_owner: Option<String>,
}

impl RuntimeManifestPublisherHandle {
    /// Loads any existing runtime-manifest chain and publishes the initial state synchronously.
    /// A corrupt existing chain is intentionally fatal: replacing it would hide durable state from
    /// an external observer.
    pub(crate) fn open(
        file_manager: Arc<FileManager>,
        schema_manager: Arc<SchemaManager>,
        db_state: Arc<DbStateHandle>,
        lifecycle: Arc<DbLifecycle>,
        compaction_mode: CompactionMode,
    ) -> Result<Self> {
        let store = RuntimeManifestStore::new(Arc::clone(&file_manager));
        let current = store.load_current()?;
        let next_generation = store.allocate_next_generation()?;
        let publisher = Arc::new(RuntimeManifestPublisher {
            store,
            file_manager,
            schema_manager,
            db_state,
            lifecycle,
            compaction_mode,
            publication: Mutex::new(PublicationState {
                current,
                next_generation,
                suspended_owner: None,
            }),
            stop: AtomicBool::new(false),
        });
        publisher.publish_initial()?;
        Ok(Self {
            publisher,
            worker: Mutex::new(None),
        })
    }

    /// Starts the coalescing DbState observer after the DB becomes open.
    pub(crate) fn start(&self) {
        let mut worker = self.worker.lock().unwrap();
        if worker.is_some() {
            return;
        }
        self.publisher
            .lifecycle
            .register_error_notifier(self.publisher.db_state.changed_condvar());
        let publisher = Arc::clone(&self.publisher);
        *worker = Some(thread::spawn(move || publisher.run()));
    }

    pub(crate) fn publish_current(&self) -> Result<()> {
        self.publisher.publish_current()
    }

    /// Publishes the current state after verifying it includes `seq_id`.
    ///
    /// Dedicated apply will use this as its durable-publication barrier after it
    /// installs an LSM edit.
    pub(crate) fn publish_at_least(&self, seq_id: u64) -> Result<()> {
        self.publisher.publish_at_least(seq_id)
    }

    /// Suspends background publication for one exclusive topology owner.
    ///
    /// Locking `publication` waits for any publication already in progress. The flag persists
    /// across failed result attempts and repeated calls are intentionally idempotent.
    pub(crate) fn suspend_for_owner(&self, owner: &str) -> Result<bool> {
        self.publisher.suspend_for_owner(owner)
    }

    /// Clears a suspension after an attempt proved that no edit was applied.
    pub(crate) fn resume_without_publish(&self, owner: &str) -> Result<()> {
        self.publisher.resume_without_publish(owner)
    }

    /// Publishes a snapshot-proven state and clears suspension only after publication succeeds.
    pub(crate) fn publish_at_least_and_resume(&self, owner: &str, seq_id: u64) -> Result<()> {
        self.publisher.publish_at_least_and_resume(owner, seq_id)
    }

    pub(crate) fn owns_suspension(&self, owner: &str) -> bool {
        self.suspension_owner().as_deref() == Some(owner)
    }

    pub(crate) fn suspension_owner(&self) -> Option<String> {
        self.publisher
            .publication
            .lock()
            .unwrap()
            .suspended_owner
            .clone()
    }

    pub(crate) fn stop(&self) {
        self.publisher.stop.store(true, Ordering::Release);
        self.publisher.db_state.notify_changed();
    }

    pub(crate) fn join(&self) {
        if let Some(worker) = self.worker.lock().unwrap().take() {
            let _ = worker.join();
        }
    }
}

impl RuntimeManifestPublisher {
    fn run(self: Arc<Self>) {
        let mut observed = self.db_state.load();
        loop {
            if self.stop.load(Ordering::Acquire) || !self.lifecycle.is_open_fast() {
                return;
            }
            let guard = self.db_state.lock();
            let current = self.db_state.load();
            let (guard, notified) = if Arc::ptr_eq(&observed, &current) {
                let (guard, timeout) = self
                    .db_state
                    .changed_condvar()
                    .wait_timeout(guard, PUBLISHER_WAIT)
                    .unwrap();
                (guard, !timeout.timed_out())
            } else {
                (guard, true)
            };
            let current = self.db_state.load();
            drop(guard);

            if self.stop.load(Ordering::Acquire) || !self.lifecycle.is_open_fast() {
                return;
            }
            if !notified && Arc::ptr_eq(&observed, &current) {
                continue;
            }
            if let Err(err) = self.publish_background_current() {
                self.lifecycle.mark_error(err);
                self.db_state.notify_changed();
                return;
            }
            observed = current;
        }
    }

    fn publish_at_least(&self, seq_id: u64) -> Result<()> {
        let mut publication = self.publication.lock().unwrap();
        if let Some(owner) = &publication.suspended_owner {
            return Err(suspended_publication_error(owner));
        }
        let state = self.db_state.load();
        if state.seq_id < seq_id {
            return Err(Error::InvalidState(format!(
                "Runtime manifest requested sequence {seq_id}, but DbState is only at {}",
                state.seq_id
            )));
        }
        self.publish_state_locked(&mut publication, state, true)
    }

    fn publish_current(&self) -> Result<()> {
        let mut publication = self.publication.lock().unwrap();
        if let Some(owner) = &publication.suspended_owner {
            return Err(suspended_publication_error(owner));
        }
        let state = self.db_state.load();
        self.publish_state_locked(&mut publication, state, false)
    }

    fn publish_initial(&self) -> Result<()> {
        let mut publication = self.publication.lock().unwrap();
        let state = self.db_state.load();
        self.publish_state_locked(&mut publication, state, true)
    }

    fn publish_background_current(&self) -> Result<()> {
        let mut publication = self.publication.lock().unwrap();
        if publication.suspended_owner.is_some() {
            return Ok(());
        }
        let state = self.db_state.load();
        self.publish_state_locked(&mut publication, state, false)
    }

    fn suspend_for_owner(&self, owner: &str) -> Result<bool> {
        let mut publication = self.publication.lock().unwrap();
        match publication.suspended_owner.as_deref() {
            None => {
                publication.suspended_owner = Some(owner.to_string());
                Ok(true)
            }
            Some(current_owner) if current_owner == owner => Ok(false),
            Some(current_owner) => Err(Error::InvalidState(format!(
                "Runtime manifest publication is suspended for {current_owner}, not {owner}"
            ))),
        }
    }

    fn resume_without_publish(&self, owner: &str) -> Result<()> {
        let mut publication = self.publication.lock().unwrap();
        match publication.suspended_owner.as_deref() {
            Some(current_owner) if current_owner == owner => {
                publication.suspended_owner = None;
                drop(publication);
                self.db_state.notify_changed();
                Ok(())
            }
            None => Ok(()),
            Some(current_owner) => Err(Error::InvalidState(format!(
                "Runtime publication owner {owner} cannot resume publication owned by {current_owner}"
            ))),
        }
    }

    fn publish_at_least_and_resume(&self, owner: &str, seq_id: u64) -> Result<()> {
        let mut publication = self.publication.lock().unwrap();
        if publication.suspended_owner.as_deref() != Some(owner) {
            return Err(Error::InvalidState(match &publication.suspended_owner {
                Some(current_owner) => format!(
                    "Runtime publication owner {owner} cannot publish state owned by {current_owner}"
                ),
                None => "Runtime manifest publisher is not suspended".to_string(),
            }));
        }
        let state = self.db_state.load();
        if state.seq_id < seq_id {
            return Err(Error::InvalidState(format!(
                "Runtime manifest requested sequence {seq_id}, but DbState is only at {}",
                state.seq_id
            )));
        }
        self.publish_state_locked(&mut publication, state, true)?;
        publication.suspended_owner = None;
        drop(publication);
        self.db_state.notify_changed();
        Ok(())
    }

    fn publish_state_locked(
        &self,
        publication: &mut PublicationState,
        state: Arc<DbState>,
        force: bool,
    ) -> Result<()> {
        let generation = publication.next_generation;
        let latest_schema_id = self.schema_manager.latest_schema().version();
        // CURRENT is the runtime reader's commit point. Persist schema files first so it never
        // makes a layout visible before that layout's schema can be reconstructed.
        self.schema_manager
            .persist_schemas_up_to(self.file_manager.as_ref(), latest_schema_id)?;
        let manifest = runtime_manifest_from_state(
            state.as_ref(),
            self.file_manager.as_ref(),
            generation,
            latest_schema_id,
            self.compaction_mode,
        )?;

        if !force
            && publication
                .current
                .as_ref()
                .is_some_and(|current| same_persisted_state(&current.manifest, &manifest))
        {
            return Ok(());
        }

        let envelope = build_runtime_manifest(manifest.clone(), publication.current.as_ref())?;
        publication.next_generation =
            publication.next_generation.checked_add(1).ok_or_else(|| {
                Error::InvalidState("Runtime manifest generation space is exhausted".to_string())
            })?;
        self.store.publish(&envelope)?;
        let tree_versions = state.multi_lsm_version.tree_versions_cloned();
        let mut file_ids = tree_versions
            .into_iter()
            .flat_map(|version| version.levels.clone().into_iter())
            .flat_map(|level| level.files.into_iter())
            .map(|file| file.file_id)
            .collect::<Vec<_>>();
        file_ids.extend(
            state
                .vlog_version
                .tracked_files()
                .into_iter()
                .map(|tracked| tracked.file_id()),
        );
        self.file_manager.commit_logical_files(file_ids);
        publication.current = Some(published_manifest_state(
            manifest,
            &envelope,
            publication.current.as_ref(),
        )?);
        Ok(())
    }
}

fn suspended_publication_error(owner: &str) -> Error {
    Error::InvalidState(format!(
        "Runtime manifest publication is suspended for {owner}"
    ))
}

/// Advances the in-process publication state after `RuntimeManifestStore::publish` succeeds.
///
/// The envelope has already been validated and durably written, so reconstructing its resolved
/// state locally avoids reopening every manifest in the incremental chain after each publish.
fn published_manifest_state(
    manifest: RuntimeManifest,
    envelope: &RuntimeManifestEnvelope,
    base: Option<&LoadedRuntimeManifest>,
) -> Result<LoadedRuntimeManifest> {
    if envelope.generation() != manifest.generation {
        return Err(Error::InvalidState(format!(
            "Runtime manifest envelope generation {} does not match published state generation {}",
            envelope.generation(),
            manifest.generation
        )));
    }
    let (base_generation, chain_depth) = match &envelope.manifest {
        RuntimeManifestPayload::Full(_) => (None, 1),
        RuntimeManifestPayload::Incremental(incremental) => {
            let base = base.ok_or_else(|| {
                Error::InvalidState(format!(
                    "Runtime incremental manifest {} has no in-process base",
                    incremental.generation
                ))
            })?;
            if base.generation != incremental.base_generation {
                return Err(Error::InvalidState(format!(
                    "Runtime incremental manifest {} expects in-process base {}, found {}",
                    incremental.generation, incremental.base_generation, base.generation
                )));
            }
            (Some(base.generation), base.chain_depth + 1)
        }
    };
    Ok(LoadedRuntimeManifest {
        generation: manifest.generation,
        base_generation,
        chain_depth,
        manifest,
    })
}

fn runtime_manifest_from_state(
    state: &DbState,
    file_manager: &FileManager,
    generation: u64,
    latest_schema_id: u64,
    compaction_mode: CompactionMode,
) -> Result<RuntimeManifest> {
    let tree_versions = state.multi_lsm_version.tree_versions_cloned();
    Ok(RuntimeManifest {
        generation,
        seq_id: state.seq_id,
        compaction_mode,
        topology_epoch: state.topology_epoch,
        latest_schema_id,
        bucket_ranges: state.bucket_ranges.clone(),
        lsm_tree_bucket_ranges: state.multi_lsm_version.bucket_ranges(),
        tree_scopes: state.multi_lsm_version.tree_scopes(),
        tree_levels: tree_versions
            .iter()
            .map(|tree| manifest_levels(tree, file_manager))
            .collect::<Result<Vec<_>>>()?,
        vlog_files: manifest_vlog_files(&state.vlog_version, file_manager)?,
        truncation_cursors: manifest_truncation_cursors(&state.truncation_cursors_snapshot()),
    })
}

fn manifest_levels(
    tree: &LSMTreeVersion,
    file_manager: &FileManager,
) -> Result<Vec<ManifestLevel>> {
    tree.levels
        .iter()
        .map(|level| {
            let files = level
                .files
                .iter()
                .map(|file| {
                    let replica = file_manager
                        .durable_data_file_path_with_origin(file.file_id)
                        .ok_or_else(|| {
                            Error::InvalidState(format!(
                                "Runtime manifest references unknown data file ID {}",
                                file.file_id
                            ))
                        })?;
                    Ok(manifest_file_from_data_file_with_origin(
                        file, replica.0, replica.1,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ManifestLevel {
                ordinal: level.ordinal,
                tiered: level.tiered,
                files,
            })
        })
        .collect()
}

fn manifest_vlog_files(
    version: &crate::vlog::VlogVersion,
    file_manager: &FileManager,
) -> Result<Vec<ManifestVlogFile>> {
    version
        .files_with_entries()
        .into_iter()
        .map(|(file_seq, tracked_id, valid_entries)| {
            let file_id = tracked_id.file_id();
            let replica = file_manager
                .durable_data_file_path_with_origin(file_id)
                .ok_or_else(|| {
                    Error::InvalidState(format!("Unknown value-log file ID {file_id}"))
                })?;
            Ok(ManifestVlogFile {
                file_seq,
                file_id,
                path: replica.0,
                valid_entries,
                origin: replica.1,
            })
        })
        .collect()
}

fn same_persisted_state(current: &RuntimeManifest, next: &RuntimeManifest) -> bool {
    current.compaction_mode == next.compaction_mode
        && current.topology_epoch == next.topology_epoch
        && current.latest_schema_id == next.latest_schema_id
        && current.bucket_ranges == next.bucket_ranges
        && current.lsm_tree_bucket_ranges == next.lsm_tree_bucket_ranges
        && current.tree_scopes == next.tree_scopes
        && current.tree_levels == next.tree_levels
        && current.vlog_files == next.vlog_files
        && current.truncation_cursors == next.truncation_cursors
}

#[cfg(test)]
#[path = "../../tests/unit/runtime_manifest/publisher.rs"]
mod tests;
