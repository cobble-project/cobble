//! Resilient remote compaction worker.
//!
//! Wraps a [`RemoteCompactionWorker`] with a [`LocalCompactionWorker`] fallback so the DB stays
//! open and writable when the remote compactor is down. Each compaction first tries remote; on a
//! transient failure (connect refused, timeout, I/O) it either runs the compaction locally
//! (`FallbackLocal`, the default) or abandons the attempt (`Skip`). Permanent failures (protocol
//! incompatible, unsupported merge operator, malformed schema) are never masked by a local
//! fallback — they surface so a misconfiguration is visible.
//!
//! The remote path is connectionless per attempt: every compaction opens a fresh TCP connection, so
//! a recovered compactor is picked up automatically on the next compaction without any reconnection
//! logic. The pending-compaction slot is released exactly once per attempt: on remote success the
//! execute core releases it; on transient failure the fallback/skip path owns it; on permanent
//! failure this worker releases it and marks the lifecycle errored so the failure surfaces (the
//! compaction result is fire-and-forget, so without `mark_error` a permanent failure would be
//! silently swallowed).
use super::remote::{
    RemoteCompactionOutcome, RemoteCompactionWorker, classify_remote_failure,
    execute_compaction_request,
};
use super::{CompactionResult, CompactionWorker, LocalCompactionWorker};
use crate::RemoteCompactionFailureMode;
use crate::data_file::DataFileType;
use crate::db_status::DbLifecycle;
use crate::error::{Error, Result};
use crate::iterator::SortedRun;
use crate::lsm::LSMTree;
use log::{info, warn};
use std::sync::Arc;
use tokio::task::JoinHandle;

pub(crate) struct ResilientRemoteCompactionWorker {
    remote: Arc<RemoteCompactionWorker>,
    local: Arc<LocalCompactionWorker>,
    failure_mode: RemoteCompactionFailureMode,
    db_lifecycle: Arc<DbLifecycle>,
}

impl ResilientRemoteCompactionWorker {
    pub(crate) fn new(
        remote: RemoteCompactionWorker,
        local: LocalCompactionWorker,
        failure_mode: RemoteCompactionFailureMode,
        db_lifecycle: Arc<DbLifecycle>,
    ) -> Self {
        Self {
            remote: Arc::new(remote),
            local: Arc::new(local),
            failure_mode,
            db_lifecycle,
        }
    }
}

impl CompactionWorker for ResilientRemoteCompactionWorker {
    fn submit_runs(
        &self,
        lsm_tree_idx: usize,
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
        data_file_type: DataFileType,
        ttl_provider: Arc<crate::ttl::TTLProvider>,
    ) -> Option<JoinHandle<Result<CompactionResult>>> {
        if sorted_runs.is_empty() {
            return None;
        }
        let Some(handle) = self.remote.runtime_handle() else {
            warn!("resilient remote compaction worker is shutdown, cannot submit new tasks");
            return None;
        };
        let remote = Arc::clone(&self.remote);
        let local = Arc::clone(&self.local);
        let failure_mode = self.failure_mode;
        let db_lifecycle = Arc::clone(&self.db_lifecycle);
        // The execute core needs stable references to the file manager and lsm tree; capture them
        // by cloning the Arc/Weak held by the remote worker.
        let file_manager = Arc::clone(remote.file_manager());
        let lsm_tree_weak = remote.lsm_tree().clone();
        let address = remote.address().to_string();
        let remote_timeout = remote.remote_timeout();
        Some(handle.spawn_blocking(move || {
            let lsm_tree = match lsm_tree_weak.upgrade() {
                Some(tree) => tree,
                None => {
                    // LSM tree dropped; nothing to release (the tree owns the pending slot).
                    return Err(Error::IoError(
                        "lsm tree dropped during compaction".to_string(),
                    ));
                }
            };
            // Build the remote request, which lazily fetches the compactor's capabilities. A
            // failure here (compactor down during capability fetch, or an unsupported operator) is
            // classified and handled below.
            let request = remote.build_request(
                lsm_tree_idx,
                &sorted_runs,
                output_level,
                data_file_type,
                ttl_provider.clone(),
            );
            let request = match request {
                Ok(request) => request,
                Err(err) => {
                    return handle_remote_build_failure(
                        &remote,
                        &local,
                        &lsm_tree,
                        &db_lifecycle,
                        failure_mode,
                        err,
                        lsm_tree_idx,
                        sorted_runs,
                        output_level,
                        data_file_type,
                        ttl_provider,
                    );
                }
            };
            // Execute the request. On success the execute core releases pending and applies the
            // edit; on failure pending is untouched and we own it.
            match execute_compaction_request(
                &address,
                request,
                &sorted_runs,
                lsm_tree_idx,
                &file_manager,
                &lsm_tree,
                remote_timeout,
            ) {
                RemoteCompactionOutcome::Succeeded(result) => Ok(result),
                RemoteCompactionOutcome::Failed(failure) => handle_remote_execution_failure(
                    &remote,
                    &local,
                    &lsm_tree,
                    &db_lifecycle,
                    failure_mode,
                    failure,
                    lsm_tree_idx,
                    sorted_runs,
                    output_level,
                    data_file_type,
                    ttl_provider,
                ),
            }
        }))
    }

    fn shutdown(&self) {
        self.remote.shutdown();
        self.local.shutdown();
    }
}

/// Handles a failure that occurred while building the remote request (e.g. capability fetch
/// failed because the compactor is down, or an unsupported merge operator).
///
/// The error is classified: transient (compactor unreachable) is handled per `failure_mode`;
/// permanent (unsupported operator) never falls back. The pending slot is released on permanent
/// failure and on skip; on fallback the local worker's callback releases it. A permanent failure
/// is surfaced to the DB by marking the lifecycle errored, so a subsequent `put`/`get` observes
/// it — mirroring the local executor's behavior on compaction failure.
#[allow(clippy::too_many_arguments)]
fn handle_remote_build_failure(
    remote: &Arc<RemoteCompactionWorker>,
    local: &Arc<LocalCompactionWorker>,
    lsm_tree: &Arc<LSMTree>,
    db_lifecycle: &Arc<DbLifecycle>,
    failure_mode: RemoteCompactionFailureMode,
    err: Error,
    lsm_tree_idx: usize,
    sorted_runs: Vec<SortedRun>,
    output_level: u8,
    data_file_type: DataFileType,
    ttl_provider: Arc<crate::ttl::TTLProvider>,
) -> Result<CompactionResult> {
    match classify_remote_failure(err) {
        super::remote::RemoteCompactionFailure::Transient(transient) => {
            warn!(
                "remote compaction build failed (transient), applying failure mode {:?}: {}",
                failure_mode, transient
            );
            apply_failure_mode(
                local,
                lsm_tree,
                failure_mode,
                lsm_tree_idx,
                sorted_runs,
                output_level,
                data_file_type,
                ttl_provider,
            )
        }
        super::remote::RemoteCompactionFailure::Permanent(permanent) => {
            warn!(
                "remote compaction build failed (permanent), not falling back: {}",
                permanent
            );
            // A permanent build failure (e.g. unsupported operator) means the cached capabilities
            // do not match the compactor's actual support; invalidate so the next attempt re-fetches
            // in case the compactor was upgraded/replaced.
            remote.invalidate_supported_merge_operator_ids();
            let _ = lsm_tree.on_compaction_complete(lsm_tree_idx);
            db_lifecycle.mark_error(permanent.clone());
            Err(permanent)
        }
    }
}

/// Handles a failure that occurred while executing the remote request.
#[allow(clippy::too_many_arguments)]
fn handle_remote_execution_failure(
    remote: &Arc<RemoteCompactionWorker>,
    local: &Arc<LocalCompactionWorker>,
    lsm_tree: &Arc<LSMTree>,
    db_lifecycle: &Arc<DbLifecycle>,
    failure_mode: RemoteCompactionFailureMode,
    failure: super::remote::RemoteCompactionFailure,
    lsm_tree_idx: usize,
    sorted_runs: Vec<SortedRun>,
    output_level: u8,
    data_file_type: DataFileType,
    ttl_provider: Arc<crate::ttl::TTLProvider>,
) -> Result<CompactionResult> {
    match failure {
        super::remote::RemoteCompactionFailure::Transient(transient) => {
            warn!(
                "remote compaction failed (transient), applying failure mode {:?}: {}",
                failure_mode, transient
            );
            apply_failure_mode(
                local,
                lsm_tree,
                failure_mode,
                lsm_tree_idx,
                sorted_runs,
                output_level,
                data_file_type,
                ttl_provider,
            )
        }
        super::remote::RemoteCompactionFailure::Permanent(permanent) => {
            warn!(
                "remote compaction failed (permanent), not falling back: {}",
                permanent
            );
            // A permanent execution failure (protocol mismatch, malformed schema) may indicate the
            // compactor's capabilities or protocol version changed; invalidate the cached
            // capabilities so the next attempt re-fetches from the (possibly upgraded) compactor.
            remote.invalidate_supported_merge_operator_ids();
            let _ = lsm_tree.on_compaction_complete(lsm_tree_idx);
            db_lifecycle.mark_error(permanent.clone());
            Err(permanent)
        }
    }
}

/// Applies the configured failure mode after a transient remote failure. The pending slot is still
/// held at this point.
#[allow(clippy::too_many_arguments)]
fn apply_failure_mode(
    local: &Arc<LocalCompactionWorker>,
    lsm_tree: &Arc<LSMTree>,
    failure_mode: RemoteCompactionFailureMode,
    lsm_tree_idx: usize,
    sorted_runs: Vec<SortedRun>,
    output_level: u8,
    data_file_type: DataFileType,
    ttl_provider: Arc<crate::ttl::TTLProvider>,
) -> Result<CompactionResult> {
    match failure_mode {
        RemoteCompactionFailureMode::FallbackLocal => {
            info!(
                "falling back to local compaction for tree {} (remote compactor unavailable)",
                lsm_tree_idx
            );
            run_local_fallback_blocking(
                local,
                lsm_tree,
                lsm_tree_idx,
                sorted_runs,
                output_level,
                data_file_type,
                ttl_provider,
            )
        }
        RemoteCompactionFailureMode::Skip => {
            info!(
                "skipping compaction for tree {} (remote compactor unavailable, failure_mode=skip)",
                lsm_tree_idx
            );
            // Release the pending slot and return an empty result. The DB stays healthy; the next
            // flush/write that re-triggers compaction will retry remote.
            let _ = lsm_tree.on_compaction_complete(lsm_tree_idx);
            Ok(empty_compaction_result(lsm_tree_idx))
        }
    }
}

/// Runs the local fallback, blocking until it completes. The local worker's completion callback
/// releases the pending slot and applies the edit.
#[allow(clippy::too_many_arguments)]
fn run_local_fallback_blocking(
    local: &Arc<LocalCompactionWorker>,
    lsm_tree: &Arc<LSMTree>,
    lsm_tree_idx: usize,
    sorted_runs: Vec<SortedRun>,
    output_level: u8,
    data_file_type: DataFileType,
    ttl_provider: Arc<crate::ttl::TTLProvider>,
) -> Result<CompactionResult> {
    let handle = local.submit_runs(
        lsm_tree_idx,
        sorted_runs,
        output_level,
        data_file_type,
        ttl_provider,
    );
    let Some(handle) = handle else {
        let _ = lsm_tree.on_compaction_complete(lsm_tree_idx);
        return Err(Error::IoError(
            "local compaction fallback declined to submit".to_string(),
        ));
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::IoError(e.to_string()))?;
    // block_on yields Result<Result<CompactionResult, Error>, JoinError>; flatten both layers.
    match runtime.block_on(handle) {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => {
            // The local compaction itself failed. The local executor already marked the lifecycle
            // errored, but its on_complete callback only runs on success, so the pending slot is
            // still held — release it here.
            let _ = lsm_tree.on_compaction_complete(lsm_tree_idx);
            Err(err)
        }
        Err(join_err) => {
            // The local task panicked or was cancelled. Release pending and surface the error.
            let _ = lsm_tree.on_compaction_complete(lsm_tree_idx);
            Err(Error::IoError(format!(
                "local compaction fallback task failed: {}",
                join_err
            )))
        }
    }
}

fn empty_compaction_result(lsm_tree_idx: usize) -> CompactionResult {
    use crate::lsm::VersionEdit;
    CompactionResult::new(
        lsm_tree_idx,
        Vec::new(),
        VersionEdit {
            level_edits: Vec::new(),
        },
        None,
        Vec::new(),
    )
}
