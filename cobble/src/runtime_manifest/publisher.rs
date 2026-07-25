use super::{
    LoadedRuntimeManifest, RuntimeManifest, RuntimeManifestEnvelope, RuntimeManifestPayload,
    RuntimeManifestStore, build_runtime_manifest,
};
use crate::db_state::{DbState, DbStateHandle};
use crate::db_status::DbLifecycle;
use crate::error::{Error, Result};
use crate::file::FileManager;
use crate::lsm::LSMTreeVersion;
use crate::manifest_model::{
    ManifestLevel, manifest_file_from_data_file, manifest_truncation_cursors, manifest_vlog_files,
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
    publication: Mutex<PublicationState>,
    stop: AtomicBool,
}

#[derive(Default)]
struct PublicationState {
    current: Option<LoadedRuntimeManifest>,
    suspended_job_id: Option<String>,
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
    ) -> Result<Self> {
        let store = RuntimeManifestStore::new(Arc::clone(&file_manager));
        let current = store.load_current()?;
        let publisher = Arc::new(RuntimeManifestPublisher {
            store,
            file_manager,
            schema_manager,
            db_state,
            lifecycle,
            publication: Mutex::new(PublicationState {
                current,
                suspended_job_id: None,
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

    /// Suspends background publication before a dedicated compaction may edit the LSM.
    ///
    /// Locking `publication` waits for any publication already in progress. The flag persists
    /// across failed result attempts and repeated calls are intentionally idempotent.
    pub(crate) fn suspend_for_dedicated_apply(&self, job_id: &str) -> Result<bool> {
        self.publisher.suspend_for_dedicated_apply(job_id)
    }

    /// Clears a suspension after an attempt proved that no edit was applied.
    pub(crate) fn resume_without_publish(&self, job_id: &str) -> Result<()> {
        self.publisher.resume_without_publish(job_id)
    }

    /// Publishes a snapshot-proven state and clears suspension only after publication succeeds.
    pub(crate) fn publish_at_least_and_resume(&self, job_id: &str, seq_id: u64) -> Result<()> {
        self.publisher.publish_at_least_and_resume(job_id, seq_id)
    }

    pub(crate) fn owns_dedicated_apply_suspension(&self, job_id: &str) -> bool {
        self.dedicated_apply_suspension_owner().as_deref() == Some(job_id)
    }

    pub(crate) fn dedicated_apply_suspension_owner(&self) -> Option<String> {
        self.publisher
            .publication
            .lock()
            .unwrap()
            .suspended_job_id
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
        let state = self.db_state.load();
        if state.seq_id < seq_id {
            return Err(Error::InvalidState(format!(
                "Runtime manifest requested sequence {seq_id}, but DbState is only at {}",
                state.seq_id
            )));
        }
        let mut publication = self.publication.lock().unwrap();
        if let Some(job_id) = &publication.suspended_job_id {
            return Err(suspended_publication_error(job_id));
        }
        self.publish_state_locked(&mut publication, state, true)
    }

    fn publish_current(&self) -> Result<()> {
        let state = self.db_state.load();
        let mut publication = self.publication.lock().unwrap();
        if let Some(job_id) = &publication.suspended_job_id {
            return Err(suspended_publication_error(job_id));
        }
        self.publish_state_locked(&mut publication, state, false)
    }

    fn publish_initial(&self) -> Result<()> {
        let state = self.db_state.load();
        let mut publication = self.publication.lock().unwrap();
        self.publish_state_locked(&mut publication, state, true)
    }

    fn publish_background_current(&self) -> Result<()> {
        let state = self.db_state.load();
        let mut publication = self.publication.lock().unwrap();
        if publication.suspended_job_id.is_some() {
            return Ok(());
        }
        self.publish_state_locked(&mut publication, state, false)
    }

    fn suspend_for_dedicated_apply(&self, job_id: &str) -> Result<bool> {
        let mut publication = self.publication.lock().unwrap();
        match publication.suspended_job_id.as_deref() {
            None => {
                publication.suspended_job_id = Some(job_id.to_string());
                Ok(true)
            }
            Some(owner) if owner == job_id => Ok(false),
            Some(owner) => Err(Error::InvalidState(format!(
                "Runtime manifest publication is suspended for dedicated compaction job {owner}, not {job_id}"
            ))),
        }
    }

    fn resume_without_publish(&self, job_id: &str) -> Result<()> {
        let mut publication = self.publication.lock().unwrap();
        match publication.suspended_job_id.as_deref() {
            Some(owner) if owner == job_id => {
                publication.suspended_job_id = None;
                drop(publication);
                self.db_state.notify_changed();
                Ok(())
            }
            None => Ok(()),
            Some(owner) => Err(Error::InvalidState(format!(
                "Dedicated compaction job {job_id} cannot resume runtime publication owned by {owner}"
            ))),
        }
    }

    fn publish_at_least_and_resume(&self, job_id: &str, seq_id: u64) -> Result<()> {
        let mut publication = self.publication.lock().unwrap();
        if publication.suspended_job_id.as_deref() != Some(job_id) {
            return Err(Error::InvalidState(match &publication.suspended_job_id {
                Some(owner) => format!(
                    "Dedicated compaction job {job_id} cannot publish runtime state owned by {owner}"
                ),
                None => {
                    "Runtime manifest publisher is not suspended for dedicated apply".to_string()
                }
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
        publication.suspended_job_id = None;
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
        let generation = publication
            .current
            .as_ref()
            .map_or(1, |current| current.generation.saturating_add(1));
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
        self.store.publish(&envelope)?;
        publication.current = Some(published_manifest_state(
            manifest,
            &envelope,
            publication.current.as_ref(),
        )?);
        Ok(())
    }
}

fn suspended_publication_error(job_id: &str) -> Error {
    Error::InvalidState(format!(
        "Runtime manifest publication is suspended for unproven dedicated compaction job {job_id}"
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
) -> Result<RuntimeManifest> {
    let tree_versions = state.multi_lsm_version.tree_versions_cloned();
    Ok(RuntimeManifest {
        generation,
        seq_id: state.seq_id,
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
                    let path = file_manager
                        .get_data_file_full_path(file.file_id)
                        .ok_or_else(|| {
                            Error::InvalidState(format!(
                                "Runtime manifest references unknown data file ID {}",
                                file.file_id
                            ))
                        })?;
                    Ok(manifest_file_from_data_file(file, path))
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

fn same_persisted_state(current: &RuntimeManifest, next: &RuntimeManifest) -> bool {
    current.latest_schema_id == next.latest_schema_id
        && current.bucket_ranges == next.bucket_ranges
        && current.lsm_tree_bucket_ranges == next.lsm_tree_bucket_ranges
        && current.tree_scopes == next.tree_scopes
        && current.tree_levels == next.tree_levels
        && current.vlog_files == next.vlog_files
        && current.truncation_cursors == next.truncation_cursors
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_manifest::{RUNTIME_MANIFEST_VERSION_CURRENT, RuntimeIncrementalManifest};

    fn manifest(generation: u64) -> RuntimeManifest {
        RuntimeManifest {
            generation,
            seq_id: generation,
            latest_schema_id: 0,
            bucket_ranges: Vec::new(),
            lsm_tree_bucket_ranges: Vec::new(),
            tree_scopes: Vec::new(),
            tree_levels: Vec::new(),
            vlog_files: Vec::new(),
            truncation_cursors: Vec::new(),
        }
    }

    fn incremental_envelope(generation: u64, base_generation: u64) -> RuntimeManifestEnvelope {
        RuntimeManifestEnvelope {
            version: RUNTIME_MANIFEST_VERSION_CURRENT,
            manifest: RuntimeManifestPayload::Incremental(RuntimeIncrementalManifest {
                generation,
                base_generation,
                seq_id: generation,
                latest_schema_id: 0,
                tree_level_edits: Vec::new(),
                vlog_files: Vec::new(),
                truncation_cursors: Vec::new(),
            }),
        }
    }

    #[test]
    fn continuous_incremental_publishes_advance_local_chain_depth() {
        let initial_manifest = manifest(1);
        let initial = LoadedRuntimeManifest {
            generation: 1,
            base_generation: None,
            chain_depth: 1,
            manifest: initial_manifest,
        };
        let second =
            published_manifest_state(manifest(2), &incremental_envelope(2, 1), Some(&initial))
                .unwrap();
        assert_eq!(second.base_generation, Some(1));
        assert_eq!(second.chain_depth, 2);

        let third =
            published_manifest_state(manifest(3), &incremental_envelope(3, 2), Some(&second))
                .unwrap();
        assert_eq!(third.base_generation, Some(2));
        assert_eq!(third.chain_depth, 3);
    }
}
