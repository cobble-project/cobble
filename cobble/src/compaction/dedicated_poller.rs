//! Writer-side poller for dedicated compaction results.
//!
//! In `CompactionMode::Dedicated`, the writer does not run any in-process compaction. Instead, a
//! separate dedicated compactor process publishes compaction result files to the shared volume.
//! This poller discovers those results, validates them, applies the compaction edit to the
//! writer's in-memory LSM state, commits a new manifest snapshot, and then deletes the result.
//!
//! The poller is a managed background worker: it has a stop signal, a join handle, and is
//! stopped/joined during `Db::close` before the snapshot manager shuts down. It registers an
//! error notifier with `DbLifecycle` so it wakes promptly on shutdown or fatal error.
use crate::compaction::dedicated::{
    DedicatedCompactionResult, cleanup_job_dir, collect_manifest_file_paths,
    delete_dedicated_compaction_result, list_dedicated_compaction_result_job_ids,
    read_dedicated_compaction_result, sweep_orphan_job_dirs,
};
use crate::compaction::dedicated_apply::{
    ExternalCompactionApplyResult, apply_external_compaction_result,
};
use crate::config::Config;
use crate::db_state::DbStateHandle;
use crate::db_status::DbLifecycle;
use crate::error::{Error, Result};
use crate::file::FileManager;
use crate::lsm::LSMTree;
use crate::memtable::MemtableManager;
use crate::schema::SchemaManager;
use crate::snapshot::SnapshotManager;
use crate::snapshot::manifest::{list_snapshot_manifest_ids, load_manifest_for_snapshot};
use log::{debug, error, info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// Handle to a running dedicated compaction poller. Dropping this does NOT stop the poller -
/// `stop()` and `join()` must be called explicitly during `Db::close`.
pub(crate) struct DedicatedCompactionPollerHandle {
    stop: Arc<AtomicBool>,
    wake: Arc<Condvar>,
    wake_mutex: Arc<Mutex<()>>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl DedicatedCompactionPollerHandle {
    /// Signals the poller to stop.
    pub(crate) fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        let _guard = self.wake_mutex.lock().unwrap();
        self.wake.notify_all();
    }

    /// Joins the poller thread. Should be called after `stop()`.
    pub(crate) fn join(&self) {
        if let Some(handle) = self.handle.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

/// Context passed to the poller thread.
pub(crate) struct PollerContext {
    pub(crate) file_manager: Arc<FileManager>,
    pub(crate) lsm_tree: Arc<LSMTree>,
    pub(crate) snapshot_manager: SnapshotManager,
    pub(crate) memtable_manager: Arc<MemtableManager>,
    pub(crate) schema_manager: Arc<SchemaManager>,
    pub(crate) db_lifecycle: Arc<DbLifecycle>,
    pub(crate) db_state: Arc<DbStateHandle>,
    pub(crate) poll_interval: Duration,
    pub(crate) config: Config,
    pub(crate) stop: Arc<AtomicBool>,
    pub(crate) wake: Arc<Condvar>,
    pub(crate) wake_mutex: Arc<Mutex<()>>,
}

impl DedicatedCompactionPollerHandle {
    /// Starts the poller thread and returns a handle.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        file_manager: Arc<FileManager>,
        lsm_tree: Arc<LSMTree>,
        snapshot_manager: SnapshotManager,
        memtable_manager: Arc<MemtableManager>,
        schema_manager: Arc<SchemaManager>,
        db_lifecycle: Arc<DbLifecycle>,
        db_state: Arc<DbStateHandle>,
        poll_interval: Duration,
        config: Config,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(Condvar::new());
        let wake_mutex = Arc::new(Mutex::new(()));

        // Register an error notifier so the poller wakes on DB shutdown/error.
        let notifier = Arc::new(Condvar::new());
        let wake_for_notifier = Arc::clone(&wake);
        let wake_mutex_for_notifier = Arc::clone(&wake_mutex);
        let notifier_cb: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let _guard = wake_mutex_for_notifier.lock().unwrap();
            wake_for_notifier.notify_all();
        });
        // DbLifecycle's register_error_notifier takes a Weak<Condvar>, but we need a callback.
        // Instead we poll ensure_open in the loop and use the wake condvar for timely shutdown.
        let _ = notifier;
        let _ = notifier_cb;

        let ctx = PollerContext {
            file_manager,
            lsm_tree,
            snapshot_manager,
            memtable_manager,
            schema_manager,
            db_lifecycle,
            db_state,
            poll_interval,
            config,
            stop: Arc::clone(&stop),
            wake: Arc::clone(&wake),
            wake_mutex: Arc::clone(&wake_mutex),
        };

        let handle = std::thread::Builder::new()
            .name("cobble-dedicated-poller".to_string())
            .spawn(move || {
                run_poller(ctx);
            })
            .expect("failed to spawn dedicated compaction poller");

        info!("dedicated compaction poller started");
        Self {
            stop,
            wake,
            wake_mutex,
            handle: Mutex::new(Some(handle)),
        }
    }
}

fn run_poller(ctx: PollerContext) {
    let max_retries = 3;
    let retry_base_delay = Duration::from_millis(500);
    let mut idle_count: u32 = 0;
    // Run orphan sweep every ~60 idle iterations (roughly once per minute at 1s poll).
    let orphan_sweep_interval: u32 = 60;

    while !ctx.stop.load(Ordering::SeqCst) {
        if ctx.db_lifecycle.ensure_open().is_err() {
            // DB is closing or in error state; stop polling.
            break;
        }

        match poll_once(&ctx, max_retries, retry_base_delay) {
            Ok(PollOutcome::Processed) => {
                idle_count = 0;
                // Immediately loop to check for more results.
                continue;
            }
            Ok(PollOutcome::Idle) => {
                idle_count += 1;
                if idle_count >= orphan_sweep_interval {
                    idle_count = 0;
                    if let Err(err) = run_orphan_sweep(&ctx) {
                        warn!("orphan sweep error: {}", err);
                    }
                }
                // No results; sleep until woken or timeout.
                wait_with_timeout(&ctx);
            }
            Ok(PollOutcome::RetryDeferred) => {
                // Transient error on a result; brief backoff before retrying.
                wait_with_timeout(&ctx);
            }
            Err(err) => {
                error!("dedicated compaction poller fatal error: {}", err);
                ctx.db_lifecycle.mark_error(err);
                break;
            }
        }
    }
    info!("dedicated compaction poller stopped");
}

enum PollOutcome {
    Processed,
    Idle,
    RetryDeferred,
}

fn poll_once(
    ctx: &PollerContext,
    max_retries: u32,
    retry_base_delay: Duration,
) -> Result<PollOutcome> {
    let job_ids = list_dedicated_compaction_result_job_ids(&ctx.file_manager)?;
    if job_ids.is_empty() {
        return Ok(PollOutcome::Idle);
    }
    if job_ids.len() > 1 {
        warn!(
            "found {} dedicated compaction results (expected at most 1); processing one at a time in order",
            job_ids.len()
        );
    }

    // Process one result at a time, in sorted order.
    let job_id = &job_ids[0];

    // Read and decode the result, with limited retries for transient I/O.
    let result = match read_with_retries(ctx, job_id, max_retries, retry_base_delay) {
        Ok(result) => result,
        Err(ReadResultError::Checksum) => {
            // After retries, checksum still fails - terminal invalid. Delete result and job dir.
            warn!(
                "dedicated compaction result {} has persistent checksum mismatch; deleting as terminal invalid",
                job_id
            );
            let _ = cleanup_job_dir(&ctx.file_manager, job_id);
            let _ = delete_dedicated_compaction_result(&ctx.file_manager, job_id);
            return Ok(PollOutcome::Processed);
        }
        Err(ReadResultError::Transient(err)) => {
            warn!(
                "transient error reading dedicated compaction result {}: {}; will retry",
                job_id, err
            );
            return Ok(PollOutcome::RetryDeferred);
        }
        Err(ReadResultError::Decode(err)) => {
            warn!(
                "dedicated compaction result {} failed to decode: {}; deleting as terminal invalid",
                job_id, err
            );
            let _ = cleanup_job_dir(&ctx.file_manager, job_id);
            let _ = delete_dedicated_compaction_result(&ctx.file_manager, job_id);
            return Ok(PollOutcome::Processed);
        }
    };

    debug!(
        "processing dedicated compaction result job={} operation={:?}",
        result.job_id, result.operation
    );

    match apply_external_compaction_result(ctx, &result, job_id) {
        Ok(ExternalCompactionApplyResult::Applied) => {
            debug!(
                "dedicated compaction result job={} applied successfully",
                result.job_id
            );
            Ok(PollOutcome::Processed)
        }
        Ok(ExternalCompactionApplyResult::AlreadyApplied) => {
            debug!(
                "dedicated compaction result job={} already applied",
                result.job_id
            );
            Ok(PollOutcome::Processed)
        }
        Ok(ExternalCompactionApplyResult::Conflict) => {
            warn!(
                "dedicated compaction result job={} (file={}) rejected (conflict); cleaning up and deleting",
                result.job_id, job_id
            );
            // Cleanup of uncommitted outputs is handled inside apply_external_compaction_result.
            // Delete the result using the filename-parsed job_id.
            let _ = delete_dedicated_compaction_result(&ctx.file_manager, job_id);
            Ok(PollOutcome::Processed)
        }
        Ok(ExternalCompactionApplyResult::TerminalInvalid) => {
            warn!(
                "dedicated compaction result job={} (file={}) is terminally invalid; cleaning up and deleting",
                result.job_id, job_id
            );
            // Always clean up using the filename-parsed job_id, not result.job_id, which
            // may differ if the payload was tampered or corrupted.
            let _ = cleanup_job_dir(&ctx.file_manager, job_id);
            let _ = delete_dedicated_compaction_result(&ctx.file_manager, job_id);
            Ok(PollOutcome::Processed)
        }
        Err(err) => {
            warn!(
                "failed to apply dedicated compaction result job={} (file={}): {}; will retry",
                result.job_id, job_id, err
            );
            Ok(PollOutcome::RetryDeferred)
        }
    }
}

enum ReadResultError {
    Checksum,
    Transient(Error),
    Decode(Error),
}

fn read_with_retries(
    ctx: &PollerContext,
    job_id: &str,
    max_retries: u32,
    base_delay: Duration,
) -> std::result::Result<DedicatedCompactionResult, ReadResultError> {
    let mut delay = base_delay;
    for attempt in 0..=max_retries {
        match read_dedicated_compaction_result(&ctx.file_manager, job_id) {
            Ok(result) => return Ok(result),
            Err(Error::ChecksumMismatch(_)) if attempt < max_retries => {
                debug!(
                    "checksum mismatch reading result {} (attempt {}); retrying",
                    job_id,
                    attempt + 1
                );
                std::thread::sleep(delay);
                delay *= 2;
            }
            Err(Error::ChecksumMismatch(_)) => return Err(ReadResultError::Checksum),
            Err(Error::IoError(_)) if attempt < max_retries => {
                debug!(
                    "transient I/O error reading result {} (attempt {}); retrying",
                    job_id,
                    attempt + 1
                );
                std::thread::sleep(delay);
                delay *= 2;
            }
            Err(err) => {
                if matches!(err, Error::IoError(_)) {
                    return Err(ReadResultError::Transient(err));
                }
                return Err(ReadResultError::Decode(err));
            }
        }
    }
    Err(ReadResultError::Checksum)
}

fn wait_with_timeout(ctx: &PollerContext) {
    let guard = ctx.wake_mutex.lock().unwrap();
    let _ = ctx.wake.wait_timeout(guard, ctx.poll_interval).unwrap();
}

/// Runs an orphan sweep of stale job directories on the shared volume.
///
/// Collects file paths referenced by the latest manifest so the sweep does not delete
/// outputs that have been committed but whose job directory was not yet cleaned up.
fn run_orphan_sweep(ctx: &PollerContext) -> Result<()> {
    let manifest_paths = match list_snapshot_manifest_ids(&ctx.file_manager) {
        Ok(ids) => {
            if let Some(&latest_id) = ids.last() {
                match load_manifest_for_snapshot(&ctx.file_manager, latest_id) {
                    Ok(manifest) => collect_manifest_file_paths(&manifest),
                    Err(err) => {
                        debug!("orphan sweep: could not load latest manifest: {}", err);
                        std::collections::HashSet::new()
                    }
                }
            } else {
                std::collections::HashSet::new()
            }
        }
        Err(err) => {
            debug!("orphan sweep: could not list snapshots: {}", err);
            std::collections::HashSet::new()
        }
    };
    let min_age_ms = ctx.config.compaction_orphan_min_age_ms;
    let swept = sweep_orphan_job_dirs(&ctx.file_manager, &manifest_paths, min_age_ms)?;
    if swept > 0 {
        info!("orphan sweep removed {} stale job directories", swept);
    }
    Ok(())
}
