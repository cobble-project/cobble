//! Apply logic for dedicated compaction results.
//!
//! This module implements the writer-side application of a dedicated compaction result to the
//! in-memory LSM state, followed by snapshot proof, runtime publication when enabled, and
//! result cleanup.
//!
//! The apply flow (see the dedicated compactor technical plan):
//! 1. Validate the result (operation type, complete fingerprints, output files exist/readable,
//!    output paths in job namespace, no duplicate file ids, key ranges in tree scope, vlog
//!    deltas valid).
//! 2. Operation-specific status judgment: Pending / AppliedInMemory / Conflict.
//! 3. If Pending: allocate canonical file ids for Rewrite outputs, register them readonly,
//!    resolve real input Arcs from the current LSM, apply VersionEdit + VlogEdit.
//! 4. If freshly applied, run a snapshot barrier (flush + materialize with callback). On retry,
//!    reuse an existing manifest only when it contains positive evidence for the operation;
//!    otherwise create a new snapshot. Verify the resulting manifest in either case.
//! 5. When runtime manifests are enabled, publish the applied DbState after the snapshot proof.
//! 6. Once durability is proven: make outputs owned, delete the result.
//! 7. If Conflict: clean up uncommitted outputs, delete the result.
use crate::compaction::dedicated::{
    DedicatedCompactionOperation, DedicatedCompactionResult, DedicatedDataFile, cleanup_job_dir,
    dedicated_compaction_job_output_prefix, delete_dedicated_compaction_result,
};
use crate::compaction::dedicated_poller::PollerContext;
use crate::data_file::{DataFile, DataFileType};
use crate::db_state::LSMTreeScope;
use crate::error::{Error, Result};
use crate::file::{FileManager, TrackedFileId};
use crate::lsm::{LevelEdit, VersionEdit};
use crate::snapshot::manifest::load_manifest_for_snapshot;
use crate::snapshot::{SnapshotCallback, SnapshotManifestInfo};
use crate::vlog::VlogEdit;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

/// The outcome of attempting to apply a dedicated compaction result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExternalCompactionApplyResult {
    /// The result was applied to the in-memory LSM state and committed via a new manifest.
    /// This covers both the fresh-apply path (Pending -> apply_edit -> commit) and the
    /// re-apply path (AppliedInMemory -> commit_and_verify confirms or creates the manifest).
    Applied,
    /// The result conflicts with the current LSM state. Uncommitted outputs are cleaned up
    /// and the result is deleted so the compactor can re-plan.
    Conflict,
    /// The result is terminally invalid (bad version, unknown scope, duplicate ids, bad paths,
    /// invalid vlog deltas, job_id mismatch, etc.). The result and its job directory are
    /// deleted so the compactor can proceed with a new plan.
    TerminalInvalid,
}

/// Entry point called by the poller. Applies (or re-confirms) a dedicated compaction result.
///
/// Returns:
/// - `Ok(Applied)` when the operation was applied (or re-confirmed) and the manifest is
///   committed. The result has been deleted.
/// - `Ok(Conflict)` when the operation conflicts with the current LSM state. Uncommitted
///   outputs are cleaned up and the result is deleted.
/// - `Ok(TerminalInvalid)` for deterministically invalid results (bad structure, bad scope,
///   duplicate ids, bad paths, bad vlog deltas, job_id mismatch). The poller should delete the
///   result and its job directory.
/// - `Err(...)` for failures where retrying is safe and cleanup must **not** happen: transient
///   I/O errors (remote storage unavailable), snapshot materialization failures, manifest read
///   errors, and internal consistency errors (`PreserveAndRetry`) where the LSM may still
///   reference the output files. The poller retries without cleaning up the job directory.
pub(crate) fn apply_external_compaction_result(
    ctx: &PollerContext,
    result: &DedicatedCompactionResult,
    expected_job_id: &str,
) -> Result<ExternalCompactionApplyResult> {
    let owns_existing_suspension = ctx
        .runtime_manifest_publisher
        .as_ref()
        .is_some_and(|publisher| publisher.owns_dedicated_apply_suspension(expected_job_id));

    // Validate that the result's job_id matches the file name's job_id.
    if result.job_id != expected_job_id {
        if owns_existing_suspension {
            return Err(Error::InvalidState(format!(
                "Suspended dedicated compaction job {expected_job_id} changed its payload job id"
            )));
        }
        return Ok(ExternalCompactionApplyResult::TerminalInvalid);
    }

    // Tree indices are not stable across expand/shrink or column-family topology changes.
    // Hold the writer's topology lock from scope validation through the durable snapshot proof,
    // so the exact scope resolved below remains attached to the same logical tree throughout
    // apply and manifest verification. File-level flushes remain independently serialized by
    // DbState/LSM locks and may continue.
    let _topology_guard = ctx.lsm_topology_lock.lock().unwrap();

    // Step 1: validate the result structure. Most validation errors are terminal
    // (structural), but I/O errors (e.g. remote storage temporarily unavailable)
    // should be retried.
    if let Err(err) = validate_result(ctx, result) {
        if owns_existing_suspension || is_transient_error(&err) {
            return Err(err);
        }
        return Ok(ExternalCompactionApplyResult::TerminalInvalid);
    }

    // Serialize against the background runtime publisher before apply_operation can mutate the
    // LSM. The suspension is persistent so a failed attempt cannot expose an unproven edit when
    // its stack unwinds; the next retry resumes from the same state.
    if let Some(publisher) = &ctx.runtime_manifest_publisher {
        publisher.suspend_for_dedicated_apply(expected_job_id)?;
    }

    // Step 2: operation-specific status judgment + apply.
    // Returns the apply outcome and, for Rewrite, the mapping from compactor output path to
    // writer canonical file id (needed for finalize_outputs).
    let (apply_outcome, output_path_to_id) = match apply_operation(ctx, result) {
        Ok(v) => v,
        Err(ApplyError::Terminal(err)) => {
            if owns_existing_suspension {
                return Err(err);
            }
            if let Some(publisher) = &ctx.runtime_manifest_publisher {
                publisher.resume_without_publish(expected_job_id)?;
            }
            debug!(
                "dedicated compaction result {} apply error (terminal): {}",
                result.job_id, err
            );
            return Ok(ExternalCompactionApplyResult::TerminalInvalid);
        }
        Err(ApplyError::PreserveAndRetry(err)) => {
            // Internal consistency error. The LSM may still reference the output files, so
            // we must NOT clean up the job directory. Return Err so the poller retries.
            warn!(
                "dedicated compaction result {} apply error (preserve and retry): {}",
                result.job_id, err
            );
            return Err(err);
        }
    };

    let freshly_applied = matches!(apply_outcome, ApplyOutcome::Applied);
    match apply_outcome {
        ApplyOutcome::Applied | ApplyOutcome::AppliedInMemory => {
            let applied_seq_id = ctx.db_state.load().seq_id;
            // Commit via snapshot barrier (or confirm the manifest already proves it).
            // This is required for BOTH outcomes: Applied needs a fresh manifest;
            // AppliedInMemory may or may not have a committed manifest from a prior attempt.
            // Only commit_and_verify (which reads the manifest from disk) can prove durability.
            commit_and_verify(ctx, result, freshly_applied)?;
            // A runtime manifest may only describe an edit after the snapshot barrier made the
            // edit durable. If this publication fails, preserve the result and readonly outputs
            // so a retry can prove the snapshot again and advance runtime CURRENT later.
            if let Some(publisher) = &ctx.runtime_manifest_publisher {
                publisher.publish_at_least_and_resume(expected_job_id, applied_seq_id)?;
            }
            // Make outputs owned now that the manifest is committed, then delete the result.
            finalize_outputs(ctx, &output_path_to_id)?;
            delete_dedicated_compaction_result(&ctx.file_manager, &result.job_id)?;
            Ok(ExternalCompactionApplyResult::Applied)
        }
        ApplyOutcome::Conflict => {
            if owns_existing_suspension {
                return Err(Error::InvalidState(format!(
                    "Suspended dedicated compaction job {expected_job_id} became conflicting before its durable publication completed"
                )));
            }
            if let Some(publisher) = &ctx.runtime_manifest_publisher {
                publisher.resume_without_publish(expected_job_id)?;
            }
            // Clean up uncommitted outputs and delete result.
            cleanup_uncommitted_outputs(ctx, result)?;
            delete_dedicated_compaction_result(&ctx.file_manager, &result.job_id)?;
            Ok(ExternalCompactionApplyResult::Conflict)
        }
    }
}

/// Internal outcome of the apply step.
///
/// The state machine is intentionally simple:
/// - `Applied`: we just executed `apply_edit` and the in-memory LSM now reflects the operation.
///   The manifest has **not** been proven committed yet.
/// - `AppliedInMemory`: the in-memory LSM already reflects the operation (from a previous
///   attempt that crashed after `apply_edit` but before manifest commit). We do **not** repeat
///   the edit. The manifest has **not** been proven committed yet.
/// - `Conflict`: the operation conflicts with the current LSM state. No edit was applied.
///
/// In both `Applied` and `AppliedInMemory`, the caller must run `commit_and_verify` to ensure
/// the manifest is durable before finalizing outputs and deleting the result. Only
/// `commit_and_verify` (which checks the manifest on disk) can prove the operation is
/// `Committed` - the in-memory LSM state alone cannot, because a crash after `apply_edit` but
/// before manifest write would leave the LSM reflecting the operation with no durable record.
#[derive(Clone, Copy)]
enum ApplyOutcome {
    Applied,
    AppliedInMemory,
    Conflict,
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

fn validate_result(ctx: &PollerContext, result: &DedicatedCompactionResult) -> Result<()> {
    // Validate that the tree scope still exists.
    let tree_idx = find_tree_by_scope(ctx, &result.tree_scope)?;
    let _ = tree_idx; // used in apply

    // Validate input/output file id uniqueness within their sets.
    let mut seen_input_ids = std::collections::HashSet::new();
    for input in result.operation.inputs() {
        if !seen_input_ids.insert(input.file.file_id) {
            return Err(Error::InvalidState(format!(
                "dedicated compaction result {} has duplicate input file id {}",
                result.job_id, input.file.file_id
            )));
        }
    }
    let mut seen_output_ids = std::collections::HashSet::new();
    for output in result.operation.outputs() {
        if !seen_output_ids.insert(output.file_id) {
            return Err(Error::InvalidState(format!(
                "dedicated compaction result {} has duplicate output file id {}",
                result.job_id, output.file_id
            )));
        }
    }

    // Validate output paths belong to the job namespace.
    let expected_prefix = dedicated_compaction_job_output_prefix(&result.job_id);
    for output in result.operation.outputs() {
        // The output path should be volume-absolute and contain the job prefix.
        if !output.path.contains(&expected_prefix) {
            return Err(Error::InvalidState(format!(
                "dedicated compaction result {} output path {} does not belong to job namespace {}",
                result.job_id, output.path, expected_prefix
            )));
        }
        // Note: output file existence is verified later in `prepare_outputs` when we
        // register the file as readonly. We don't check here because the path is
        // volume-absolute (e.g. `file://...`) and the FileManager's metadata APIs
        // expect relative paths.
    }

    // Validate vlog deltas reference existing file sequences (checked against current vlog).
    // We do a lightweight check here; full validation happens during apply.
    if !result.vlog_entry_deltas.is_empty() {
        let db_state = ctx.db_state.load();
        let vlog_files = db_state.vlog_version.files_with_entries();
        let known_seqs: std::collections::HashSet<u32> =
            vlog_files.iter().map(|(seq, _, _)| *seq).collect();
        for (file_seq, delta) in &result.vlog_entry_deltas {
            if *delta == 0 {
                return Err(Error::InvalidState(format!(
                    "dedicated compaction result {} has zero vlog delta for file seq {}",
                    result.job_id, file_seq
                )));
            }
            if !known_seqs.contains(file_seq) {
                return Err(Error::InvalidState(format!(
                    "dedicated compaction result {} vlog delta references unknown file seq {}",
                    result.job_id, file_seq
                )));
            }
        }
    }

    Ok(())
}

/// Finds the current tree index matching the given scope.
fn find_tree_by_scope(ctx: &PollerContext, scope: &LSMTreeScope) -> Result<usize> {
    let db_state = ctx.db_state.load();
    let scopes = db_state.multi_lsm_version.tree_scopes();
    for (idx, s) in scopes.iter().enumerate() {
        if s == scope {
            return Ok(idx);
        }
    }
    Err(Error::InvalidState(format!(
        "dedicated compaction result tree scope {:?} no longer exists",
        scope
    )))
}

fn manifest_tree_levels_by_scope<'a>(
    manifest: &'a crate::snapshot::manifest::ManifestSnapshot,
    scope: &LSMTreeScope,
) -> Result<Option<&'a [crate::manifest_model::ManifestLevel]>> {
    if manifest.tree_scopes.len() != manifest.tree_levels.len() {
        return Err(Error::InvalidState(format!(
            "snapshot {} has {} tree scopes but {} tree level sets",
            manifest.id,
            manifest.tree_scopes.len(),
            manifest.tree_levels.len()
        )));
    }
    Ok(manifest
        .tree_scopes
        .iter()
        .position(|candidate| candidate == scope)
        .map(|tree_idx| manifest.tree_levels[tree_idx].as_slice()))
}

// ---------------------------------------------------------------------------
// Operation apply
// ---------------------------------------------------------------------------

/// Classifies an apply-phase error.
///
/// - `Terminal`: the result is structurally invalid (bad hex, unknown file type, bad paths,
///   duplicate ids, scope mismatch). The poller should delete the result and clean up the job
///   directory.
/// - `PreserveAndRetry`: an internal consistency error where the in-memory LSM state doesn't
///   match expectations, but files may still be referenced by the LSM. The poller must **not**
///   clean up the job directory - it should preserve the files and retry later. This must never
///   be confused with `Terminal`, which would delete files the LSM still references.
#[derive(Debug)]
enum ApplyError {
    Terminal(Error),
    PreserveAndRetry(Error),
}

/// Classifies an error from the apply phase's helper functions (prepare_outputs,
/// resolve_input_arcs, find_tree_by_scope) into a terminal or preserve-and-retry error.
///
/// Storage access errors (`IoError`, `FileSystemError`) are **PreserveAndRetry**: the file or
/// remote storage might become available on retry, and we must not clean up files that may be
/// referenced. All other errors (`InvalidState` from bad hex/scope, etc.) are **Terminal**.
fn classify_apply_error(err: Error) -> ApplyError {
    if is_transient_error(&err) {
        ApplyError::PreserveAndRetry(err)
    } else {
        ApplyError::Terminal(err)
    }
}

fn apply_operation(
    ctx: &PollerContext,
    result: &DedicatedCompactionResult,
) -> std::result::Result<(ApplyOutcome, HashMap<String, u64>), ApplyError> {
    match &result.operation {
        DedicatedCompactionOperation::Rewrite {
            inputs,
            output_level,
            outputs,
        } => apply_rewrite(ctx, result, inputs, *output_level, outputs),
        DedicatedCompactionOperation::TrivialMove {
            input,
            output_level,
        } => {
            let outcome = apply_trivial_move(ctx, result, input, *output_level)?;
            // TrivialMove has no outputs; the map is always empty.
            Ok((outcome, HashMap::new()))
        }
        DedicatedCompactionOperation::Drop { inputs } => {
            let outcome = apply_drop(ctx, result, inputs)?;
            // Drop has no outputs; the map is always empty.
            Ok((outcome, HashMap::new()))
        }
    }
}

/// Builds a mapping from compactor output path to writer canonical file id by finding each
/// output's matching `Arc<DataFile>` in the LSM and using its `file_id` directly.
///
/// This is used for `AppliedInMemory` cases where the outputs are already in the LSM from a
/// previous apply attempt. By taking the canonical ID from the LSM `DataFile` itself (rather
/// than from FileManager tracking), we avoid the risk of a tracking inconsistency causing us
/// to clean up files the LSM still references.
///
/// If an output cannot be found in the LSM, this returns an `Err` (not `TerminalInvalid`) so
/// the poller preserves the files and retries, rather than deleting them.
fn build_path_to_id_from_lsm(
    tree_version: &crate::lsm::LSMTreeVersion,
    outputs: &[DedicatedDataFile],
    file_manager: &Arc<FileManager>,
    job_id: &str,
) -> Result<HashMap<String, u64>> {
    let mut map = HashMap::with_capacity(outputs.len());
    for output in outputs {
        let found = tree_version.levels.iter().find_map(|level| {
            level.files.iter().find_map(|f| {
                if output
                    .matches_data_file_excluding_id(f, file_manager)
                    .unwrap_or(false)
                {
                    Some(f.file_id)
                } else {
                    None
                }
            })
        });
        match found {
            Some(file_id) => {
                map.insert(output.path.clone(), file_id);
            }
            None => {
                // The output is not in the LSM. This is an internal consistency error:
                // classify_rewrite said inputs are gone (implying the operation was applied),
                // but the outputs are not in the LSM. The caller maps this to
                // PreserveAndRetry so the poller preserves files and retries, rather than
                // cleaning up files the LSM may still reference.
                return Err(Error::InvalidState(format!(
                    "dedicated compaction result {}: output path {} not found in LSM \
                     (AppliedInMemory but output missing - consistency error, preserving files)",
                    job_id, output.path
                )));
            }
        }
    }
    Ok(map)
}

/// Applies a Rewrite operation. Returns the apply outcome and the output path->id mapping.
///
/// For `Pending`: the mapping is built inside `prepare_outputs` (before `apply_edit`), so any
/// failure leaves the LSM unmodified.
/// For `AppliedInMemory`: no LSM modification occurs. The mapping is built by finding each
/// output's matching `Arc<DataFile>` in the LSM and using its `file_id` directly. This avoids
/// relying on FileManager tracking (which may be inconsistent) and prevents unsafe cleanup of
/// files the LSM still references.
/// For `Conflict`: no outputs to map (returns empty).
fn apply_rewrite(
    ctx: &PollerContext,
    result: &DedicatedCompactionResult,
    inputs: &[crate::compaction::dedicated::DedicatedCompactionInput],
    output_level: u8,
    outputs: &[DedicatedDataFile],
) -> std::result::Result<(ApplyOutcome, HashMap<String, u64>), ApplyError> {
    let tree_idx = find_tree_by_scope(ctx, &result.tree_scope).map_err(classify_apply_error)?;
    let db_state = ctx.db_state.load();
    let tree_version = db_state.multi_lsm_version.version_of_index(tree_idx);
    let tree_version = &*tree_version;

    // Classify the operation status.
    let status = classify_rewrite(
        tree_version,
        inputs,
        outputs,
        output_level,
        &ctx.file_manager,
    );
    match status {
        OperationStatus::Pending => {
            // Allocate canonical file ids for outputs and register them readonly.
            // prepare_outputs returns the path->id mapping so we have it before apply_edit.
            let (prepared_outputs, path_to_id) =
                prepare_outputs(ctx, outputs, output_level).map_err(classify_apply_error)?;
            // Resolve real input Arcs from the current LSM.
            let input_arcs = resolve_input_arcs(tree_version, inputs, &ctx.file_manager)
                .map_err(classify_apply_error)?;
            // Build VersionEdit.
            let edit = build_rewrite_edit(input_arcs, inputs, &prepared_outputs, output_level);
            // Build VlogEdit.
            let vlog_edit = build_vlog_edit(&result.vlog_entry_deltas);
            // Apply. At this point all outputs are mapped and registered; the LSM edit is safe.
            ctx.lsm_tree.apply_edit(tree_idx, edit, vlog_edit);
            Ok((ApplyOutcome::Applied, path_to_id))
        }
        OperationStatus::AppliedInMemory => {
            // The LSM already reflects this operation. Build the path->id mapping directly
            // from the matching Arc<DataFile> in the LSM, NOT from FileManager tracking.
            // If an output can't be found in the LSM, this is an internal consistency error
            // - we preserve the files (PreserveAndRetry) rather than cleaning up, since the
            // LSM may still reference them.
            let path_to_id =
                build_path_to_id_from_lsm(tree_version, outputs, &ctx.file_manager, &result.job_id)
                    .map_err(ApplyError::PreserveAndRetry)?;
            Ok((ApplyOutcome::AppliedInMemory, path_to_id))
        }
        OperationStatus::Conflict => {
            // No outputs applied; empty mapping.
            Ok((ApplyOutcome::Conflict, HashMap::new()))
        }
    }
}

/// Applies a TrivialMove operation.
///
/// TrivialMove has no new output files (the same file Arc is moved between levels), so the
/// path->id mapping is always empty.
fn apply_trivial_move(
    ctx: &PollerContext,
    result: &DedicatedCompactionResult,
    input: &crate::compaction::dedicated::DedicatedCompactionInput,
    output_level: u8,
) -> std::result::Result<ApplyOutcome, ApplyError> {
    let tree_idx = find_tree_by_scope(ctx, &result.tree_scope).map_err(classify_apply_error)?;
    let db_state = ctx.db_state.load();
    let tree_version = db_state.multi_lsm_version.version_of_index(tree_idx);
    let tree_version = &*tree_version;

    // Find the input in its source level.
    let input_arc = find_file_in_level(tree_version, input.level, &input.file, &ctx.file_manager);
    let target_level_files = tree_version
        .levels
        .iter()
        .find(|l| l.ordinal == output_level)
        .map(|l| &l.files);

    let in_target = |f: &Arc<DataFile>| {
        input
            .file
            .matches_data_file(f, &ctx.file_manager)
            .unwrap_or(false)
    };

    match input_arc {
        Some(arc) => {
            // If the file is also in the target level, this is an abnormal partial state
            // (the file exists in both source and target). This is a Conflict, not
            // AppliedInMemory - otherwise commit_and_verify would wait forever for the
            // manifest to show "absent from source" while the LSM still has it there.
            if let Some(target_files) = target_level_files
                && target_files.iter().any(in_target)
            {
                return Ok(ApplyOutcome::Conflict);
            }
            // Build the trivial move edit: remove from source, add same Arc to target.
            let edit = VersionEdit {
                level_edits: vec![
                    LevelEdit {
                        level: input.level,
                        removed_files: vec![Arc::clone(&arc)],
                        new_files: Vec::new(),
                    },
                    LevelEdit {
                        level: output_level,
                        removed_files: Vec::new(),
                        new_files: vec![arc],
                    },
                ],
            };
            let vlog_edit = build_vlog_edit(&result.vlog_entry_deltas);
            ctx.lsm_tree.apply_edit(tree_idx, edit, vlog_edit);
            Ok(ApplyOutcome::Applied)
        }
        None => {
            // Input not in source level. Check if it's already in the target level.
            if let Some(target_files) = target_level_files
                && target_files.iter().any(in_target)
            {
                // The move is reflected in the LSM (input absent from source, present in
                // target) but the manifest may not be committed yet.
                return Ok(ApplyOutcome::AppliedInMemory);
            }
            Ok(ApplyOutcome::Conflict)
        }
    }
}

/// Applies a Drop operation.
fn apply_drop(
    ctx: &PollerContext,
    result: &DedicatedCompactionResult,
    inputs: &[crate::compaction::dedicated::DedicatedCompactionInput],
) -> std::result::Result<ApplyOutcome, ApplyError> {
    let tree_idx = find_tree_by_scope(ctx, &result.tree_scope).map_err(classify_apply_error)?;
    let db_state = ctx.db_state.load();
    let tree_version = db_state.multi_lsm_version.version_of_index(tree_idx);
    let tree_version = &*tree_version;

    // Check if all inputs are still present (Pending) or all gone (AppliedInMemory).
    let all_present = inputs.iter().all(|input| {
        find_file_in_level(tree_version, input.level, &input.file, &ctx.file_manager).is_some()
    });
    let all_gone = inputs.iter().all(|input| {
        find_file_in_level(tree_version, input.level, &input.file, &ctx.file_manager).is_none()
    });

    if all_present {
        let input_arcs = resolve_input_arcs(tree_version, inputs, &ctx.file_manager)
            .map_err(classify_apply_error)?;
        // Group removals by level.
        let mut level_edits: HashMap<u8, LevelEdit> = HashMap::new();
        for (input, arc) in inputs.iter().zip(input_arcs.iter()) {
            let entry = level_edits.entry(input.level).or_insert_with(|| LevelEdit {
                level: input.level,
                removed_files: Vec::new(),
                new_files: Vec::new(),
            });
            entry.removed_files.push(Arc::clone(arc));
        }
        let edit = VersionEdit {
            level_edits: level_edits.into_values().collect(),
        };
        let vlog_edit = build_vlog_edit(&result.vlog_entry_deltas);
        ctx.lsm_tree.apply_edit(tree_idx, edit, vlog_edit);
        Ok(ApplyOutcome::Applied)
    } else if all_gone {
        // Inputs are gone from the LSM. The drop was either applied in a previous attempt
        // (in-memory) or fully committed. commit_and_verify will check the manifest.
        Ok(ApplyOutcome::AppliedInMemory)
    } else {
        Ok(ApplyOutcome::Conflict)
    }
}

// ---------------------------------------------------------------------------
// Status classification
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
enum OperationStatus {
    Pending,
    /// The in-memory LSM already reflects the operation (inputs gone, outputs present).
    /// The manifest may or may not be committed - `commit_and_verify` must still run.
    AppliedInMemory,
    Conflict,
}

fn classify_rewrite(
    tree_version: &crate::lsm::LSMTreeVersion,
    inputs: &[crate::compaction::dedicated::DedicatedCompactionInput],
    outputs: &[DedicatedDataFile],
    _output_level: u8,
    file_manager: &Arc<FileManager>,
) -> OperationStatus {
    let all_inputs_present = inputs.iter().all(|input| {
        find_file_in_level(tree_version, input.level, &input.file, file_manager).is_some()
    });
    let all_inputs_gone = inputs.iter().all(|input| {
        find_file_in_level(tree_version, input.level, &input.file, file_manager).is_none()
    });

    if all_inputs_present {
        // Check if any outputs are already in the LSM (partial apply = abnormal).
        // Outputs use excluding_id matching because the compactor's output file_id is a
        // process-local id, while the writer's DataFile has a canonical id.
        let any_output_present = outputs.iter().any(|output| {
            tree_version.levels.iter().any(|level| {
                level.files.iter().any(|f| {
                    output
                        .matches_data_file_excluding_id(f, file_manager)
                        .unwrap_or(false)
                })
            })
        });
        if any_output_present && !outputs.is_empty() {
            // Inputs present but some outputs also present - abnormal state.
            return OperationStatus::Conflict;
        }
        OperationStatus::Pending
    } else if all_inputs_gone {
        // Inputs are gone from the LSM. The operation was either applied in a previous
        // attempt (in-memory) or fully committed. We cannot tell from the LSM alone -
        // commit_and_verify will check the manifest. Treat as AppliedInMemory either way.
        OperationStatus::AppliedInMemory
    } else {
        OperationStatus::Conflict
    }
}

// ---------------------------------------------------------------------------
// Helpers: resolve Arcs, prepare outputs, build edits
// ---------------------------------------------------------------------------

/// Finds a file in a level by full 13-field fingerprint match (including path).
fn find_file_in_level(
    tree_version: &crate::lsm::LSMTreeVersion,
    level: u8,
    file: &DedicatedDataFile,
    file_manager: &Arc<FileManager>,
) -> Option<Arc<DataFile>> {
    tree_version
        .levels
        .iter()
        .find(|l| l.ordinal == level)
        .and_then(|l| {
            l.files
                .iter()
                .find(|f| file.matches_data_file(f, file_manager).unwrap_or(false))
        })
        .cloned()
}

/// Resolves real input Arc<DataFile> from the current LSM by fingerprint matching.
fn resolve_input_arcs(
    tree_version: &crate::lsm::LSMTreeVersion,
    inputs: &[crate::compaction::dedicated::DedicatedCompactionInput],
    file_manager: &Arc<FileManager>,
) -> Result<Vec<Arc<DataFile>>> {
    let mut arcs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let arc = find_file_in_level(tree_version, input.level, &input.file, file_manager)
            .ok_or_else(|| {
                Error::InvalidState(format!(
                    "dedicated compaction input file {} not found in level {}",
                    input.file.file_id, input.level
                ))
            })?;
        arcs.push(arc);
    }
    Ok(arcs)
}

/// Allocates canonical file ids for outputs, registers them readonly, and builds DataFile Arcs.
///
/// Each output is registered with a **real** `TrackedFileId::new` (not detached) so that when
/// the file is eventually removed from the LSM, the FileManager's tracking is cleaned up and
/// the physical file is deleted (once `make_data_file_owned` has been called).
///
/// Returns the prepared `DataFile` Arcs **and** a mapping from each output's compactor path to
/// the writer's canonical file id. Building the mapping here (before `apply_edit`) ensures that
/// if any output cannot be registered or resolved, we fail **before** the LSM is modified.
#[allow(clippy::type_complexity)]
fn prepare_outputs(
    ctx: &PollerContext,
    outputs: &[DedicatedDataFile],
    output_level: u8,
) -> Result<(Vec<Arc<DataFile>>, HashMap<String, u64>)> {
    let local_ids = ctx.file_manager.reserve_data_file_ids(outputs.len());
    let mut prepared = Vec::with_capacity(outputs.len());
    let mut path_to_id = HashMap::with_capacity(outputs.len());
    for (output, local_id) in outputs.iter().zip(local_ids.iter()) {
        let local_id = *local_id;
        // Keep the output until the writer adopts it after manifest commit.
        ctx.file_manager
            .register_data_file_pending_adoption(local_id, &output.path)?;
        // Unknown file type is a protocol-level error (corrupt result), not transient I/O.
        // Map it to InvalidState so the poller treats it as terminal and deletes the result.
        let file_type = DataFileType::from_str(&output.file_type).map_err(|e| {
            Error::InvalidState(format!(
                "dedicated compaction output {} has unknown file type '{}': {}",
                local_id, output.file_type, e
            ))
        })?;
        let (start_key, end_key) = output.decode_keys()?;
        // Use a real TrackedFileId (not detached) so the FileManager tracks the file's
        // lifecycle. When the DataFile is dropped from the LSM, the TrackedFileId's Drop
        // impl calls remove_data_file, which removes the TrackedFile from the FileManager.
        // If make_data_file_owned was called (delete_on_drop=true), the TrackedFile's Drop
        // then deletes the physical file.
        let data_file = DataFile::new(
            file_type,
            start_key,
            end_key,
            local_id,
            TrackedFileId::new(&ctx.file_manager, local_id),
            output.schema_id,
            output.size,
            output.bucket_range_start..=output.bucket_range_end,
            output.effective_bucket_range_start..=output.effective_bucket_range_end,
        )
        .with_vlog_offset(output.vlog_file_seq_offset)
        .with_separated_values(output.has_separated_values);
        data_file.set_max_expired_at(output.max_expired_at);
        ctx.file_manager.finalize_data_file(&data_file)?;
        // Set the priority for the output level.
        let _ = ctx.file_manager.set_data_file_priority(
            local_id,
            crate::file::lsm_file_priority_for_level(output_level),
        );
        // Record the mapping now, before apply_edit, so that any failure in prepare_outputs
        // leaves the LSM unmodified.
        path_to_id.insert(output.path.clone(), local_id);
        prepared.push(Arc::new(data_file));
    }
    Ok((prepared, path_to_id))
}

/// Builds a VersionEdit for a Rewrite operation.
fn build_rewrite_edit(
    input_arcs: Vec<Arc<DataFile>>,
    inputs: &[crate::compaction::dedicated::DedicatedCompactionInput],
    outputs: &[Arc<DataFile>],
    output_level: u8,
) -> VersionEdit {
    // Group removals by level.
    let mut level_edits: HashMap<u8, LevelEdit> = HashMap::new();
    for (input, arc) in inputs.iter().zip(input_arcs.iter()) {
        let entry = level_edits.entry(input.level).or_insert_with(|| LevelEdit {
            level: input.level,
            removed_files: Vec::new(),
            new_files: Vec::new(),
        });
        entry.removed_files.push(Arc::clone(arc));
    }
    // Add outputs to the output level.
    let entry = level_edits
        .entry(output_level)
        .or_insert_with(|| LevelEdit {
            level: output_level,
            removed_files: Vec::new(),
            new_files: Vec::new(),
        });
    entry.new_files = outputs.to_vec();
    VersionEdit {
        level_edits: level_edits.into_values().collect(),
    }
}

/// Builds a VlogEdit from entry deltas.
fn build_vlog_edit(deltas: &[(u32, i64)]) -> Option<VlogEdit> {
    if deltas.is_empty() {
        return None;
    }
    let mut vlog_edit = VlogEdit::default();
    for (file_seq, delta) in deltas {
        vlog_edit.add_entry_delta(*file_seq, *delta);
    }
    if vlog_edit.is_empty() {
        None
    } else {
        Some(vlog_edit)
    }
}

// ---------------------------------------------------------------------------
// Manifest commit via snapshot barrier
// ---------------------------------------------------------------------------

fn commit_and_verify(
    ctx: &PollerContext,
    result: &DedicatedCompactionResult,
    freshly_applied: bool,
) -> Result<()> {
    // A result applied in this call always needs a new snapshot proof. On retry/restart, an
    // existing manifest is reusable only when it contains positive evidence: job-unique rewrite
    // outputs or a trivial-move file in its target level. Absence-only operations (Drop and
    // empty-output Rewrite) cannot distinguish a committed removal from a snapshot that predates
    // the input, so they deliberately create another snapshot.
    let has_positive_commit_evidence = operation_has_positive_manifest_evidence(&result.operation);
    if !freshly_applied && has_positive_commit_evidence && is_already_committed(ctx, result)? {
        debug!(
            "dedicated compaction result job={} already committed in manifest",
            result.job_id
        );
        return Ok(());
    }

    // Create a snapshot with a callback to know when materialization succeeds.
    let (tx, rx) = std::sync::mpsc::channel::<Result<SnapshotManifestInfo>>();
    let callback: SnapshotCallback = Arc::new(move |result| {
        let _ = tx.send(result);
    });
    let snapshot_id = ctx
        .memtable_manager
        .create_snapshot(ctx.snapshot_manager.clone(), Some(callback))?;

    // Wait for the materialization callback. This blocks until the snapshot is materialized
    // (or fails). The background materializer invokes the callback exactly once.
    // Use a timeout to avoid hanging forever if the materializer is stuck.
    let manifest_info = rx.recv_timeout(Duration::from_secs(30)).map_err(|_| {
        Error::InvalidState(format!(
            "dedicated compaction snapshot {} materialization timed out after 30s",
            snapshot_id
        ))
    })??;

    debug!(
        "dedicated compaction result job={} committed via snapshot {} manifest={}",
        result.job_id, manifest_info.id, manifest_info.manifest_path
    );

    // Verify the manifest contains the expected state.
    verify_manifest_after_commit(ctx, &manifest_info, result)?;

    Ok(())
}

fn operation_has_positive_manifest_evidence(operation: &DedicatedCompactionOperation) -> bool {
    matches!(
        operation,
        DedicatedCompactionOperation::Rewrite { outputs, .. } if !outputs.is_empty()
    ) || matches!(operation, DedicatedCompactionOperation::TrivialMove { .. })
}

/// Checks if the latest manifest on disk already reflects this operation being applied.
fn is_already_committed(ctx: &PollerContext, result: &DedicatedCompactionResult) -> Result<bool> {
    let snapshot_ids = crate::snapshot::manifest::list_snapshot_manifest_ids(&ctx.file_manager)?;
    let Some(&latest_id) = snapshot_ids.last() else {
        return Ok(false);
    };
    let manifest = load_manifest_for_snapshot(&ctx.file_manager, latest_id)?;

    // Resolve against the manifest's own topology. The latest snapshot may predate an expand or
    // shrink, so a current tree index cannot safely index this older manifest.
    let Some(tree_levels) = manifest_tree_levels_by_scope(&manifest, &result.tree_scope)? else {
        return Ok(false);
    };

    let all_inputs_absent = result.operation.inputs().iter().all(|input| {
        !tree_levels.iter().any(|level| {
            level.ordinal == input.level
                && level
                    .files
                    .iter()
                    .any(|f| input.file.matches_manifest_file(f))
        })
    });

    let all_outputs_present = result.operation.outputs().iter().all(|output| {
        tree_levels.iter().any(|level| {
            level
                .files
                .iter()
                .any(|f| output.matches_manifest_file_excluding_id(f))
        })
    });

    // For Drop (no outputs), inputs absent = committed.
    // For Rewrite, inputs absent + outputs present = committed.
    // For TrivialMove, input absent from source + present in target = committed.
    Ok(match &result.operation {
        DedicatedCompactionOperation::Drop { .. } => all_inputs_absent,
        DedicatedCompactionOperation::Rewrite { outputs, .. } if outputs.is_empty() => {
            all_inputs_absent
        }
        DedicatedCompactionOperation::Rewrite { .. } => all_inputs_absent && all_outputs_present,
        DedicatedCompactionOperation::TrivialMove { output_level, .. } => {
            // Input should be absent from source level and present in target level.
            let input = &result.operation.inputs()[0];
            let absent_from_source = !tree_levels.iter().any(|level| {
                level.ordinal == input.level
                    && level
                        .files
                        .iter()
                        .any(|f| input.file.matches_manifest_file(f))
            });
            let present_in_target = tree_levels.iter().any(|level| {
                level.ordinal == *output_level
                    && level
                        .files
                        .iter()
                        .any(|f| input.file.matches_manifest_file(f))
            });
            absent_from_source && present_in_target
        }
    })
}

/// Verifies that the committed manifest reflects the expected compaction result.
fn verify_manifest_after_commit(
    ctx: &PollerContext,
    manifest_info: &SnapshotManifestInfo,
    result: &DedicatedCompactionResult,
) -> Result<()> {
    let manifest = load_manifest_for_snapshot(&ctx.file_manager, manifest_info.id)?;
    let tree_levels =
        manifest_tree_levels_by_scope(&manifest, &result.tree_scope)?.ok_or_else(|| {
            Error::InvalidState(format!(
                "tree scope {:?} not found in manifest {}",
                result.tree_scope, manifest_info.id
            ))
        })?;

    // Verify inputs are removed.
    for input in result.operation.inputs() {
        let still_present = tree_levels.iter().any(|level| {
            level.ordinal == input.level
                && level
                    .files
                    .iter()
                    .any(|f| input.file.matches_manifest_file(f))
        });
        if still_present {
            return Err(Error::InvalidState(format!(
                "dedicated compaction result {}: input file {} still present in manifest after commit",
                result.job_id, input.file.file_id
            )));
        }
    }

    // Verify outputs are present (for Rewrite with outputs). Outputs use
    // `matches_manifest_file_excluding_id` because the compactor's output file_id is a
    // process-local id, while the manifest stores the writer's canonical id.
    for output in result.operation.outputs() {
        let present = tree_levels.iter().any(|level| {
            level
                .files
                .iter()
                .any(|f| output.matches_manifest_file_excluding_id(f))
        });
        if !present {
            return Err(Error::InvalidState(format!(
                "dedicated compaction result {}: output file not found in manifest after commit",
                result.job_id
            )));
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Finalize and cleanup
// ---------------------------------------------------------------------------

fn finalize_outputs(ctx: &PollerContext, output_path_to_id: &HashMap<String, u64>) -> Result<()> {
    // For Rewrite outputs, make them owned now that the manifest is committed.
    // Every output must be mapped to a canonical file id. If any is missing, the path
    // mapping failed, which is a programming error (not transient).
    for (path, file_id) in output_path_to_id {
        ctx.file_manager
            .make_data_file_owned(*file_id)
            .map_err(|e| {
                Error::InvalidState(format!(
                    "failed to make output owned (path={}, file_id={}): {}",
                    path, file_id, e
                ))
            })?;
    }
    Ok(())
}

fn cleanup_uncommitted_outputs(
    ctx: &PollerContext,
    result: &DedicatedCompactionResult,
) -> Result<()> {
    cleanup_job_dir(&ctx.file_manager, &result.job_id)?;
    info!(
        "cleaned up uncommitted outputs for dedicated compaction job {}",
        result.job_id
    );
    Ok(())
}

/// Classifies an error as transient (retryable) or terminal (delete the result).
///
/// Storage access errors (`IoError`, `FileSystemError`) are transient: the underlying file or
/// remote storage might become available on retry. All other errors (`InvalidState`,
/// `ConfigError`, `ChecksumMismatch`, `FileFormatError`) are structural/protocol-level:
/// retrying won't help, so the poller should delete the poison result.
///
/// Protocol-parsing failures (bad hex keys, unknown file type, bad paths, duplicate ids,
/// scope mismatch) are deliberately produced as `InvalidState` so they fall into the terminal
/// bucket here.
pub(crate) fn is_transient_error(err: &Error) -> bool {
    matches!(err, Error::IoError(_) | Error::FileSystemError(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::dedicated::DedicatedCompactionInput;
    use crate::data_file::{DataFile, DataFileType};
    use crate::file::TrackedFileId;
    use crate::lsm::{LSMTreeVersion, Level};

    /// Builds a `DedicatedDataFile` with the given path and key range (hex-encoded keys).
    fn make_dedicated_file(file_id: u64, path: &str, start: &str, end: &str) -> DedicatedDataFile {
        DedicatedDataFile {
            file_id,
            file_type: "sst".to_string(),
            path: path.to_string(),
            schema_id: 1,
            size: 100,
            start_key: start.to_string(),
            end_key: end.to_string(),
            has_separated_values: false,
            bucket_range_start: 0,
            bucket_range_end: 0,
            effective_bucket_range_start: 0,
            effective_bucket_range_end: 0,
            vlog_file_seq_offset: 0,
            max_expired_at: 0,
        }
    }

    /// Builds a live `DataFile` Arc for LSM construction.
    fn make_live_file(file_manager: &Arc<FileManager>, file_id: u64) -> Arc<DataFile> {
        Arc::new(
            DataFile::new(
                DataFileType::SSTable,
                vec![0u8],
                vec![255u8],
                file_id,
                TrackedFileId::new(file_manager, file_id),
                1,
                100,
                0u16..=0u16,
                0u16..=0u16,
            )
            .with_separated_values(false),
        )
    }

    /// Builds an `LSMTreeVersion` with the given files in the specified level.
    fn make_tree_version(level: u8, files: Vec<Arc<DataFile>>) -> LSMTreeVersion {
        LSMTreeVersion {
            levels: vec![Level {
                ordinal: level,
                tiered: false,
                files,
            }],
        }
    }

    /// When all inputs are gone, classify_rewrite must return AppliedInMemory (not a
    /// "Committed" state), regardless of whether outputs are in the LSM. This ensures
    /// commit_and_verify always runs - the in-memory LSM alone cannot prove manifest durability.
    #[test]
    fn test_classify_rewrite_inputs_gone_is_applied_in_memory() {
        use crate::config::{Config, VolumeDescriptor};
        use crate::metrics_manager::MetricsManager;
        let dir = tempfile::tempdir_in("/tmp").unwrap();
        let base = format!("file://{}", dir.path().display());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(base.clone()),
            ..Config::default()
        };
        let metrics = Arc::new(MetricsManager::new("test"));
        let fm = Arc::new(FileManager::from_config(&config, "test-db", metrics).unwrap());

        // Create real files on disk so registration succeeds.
        let (input_id, _) = fm.create_data_file_with_prefix("data").unwrap();
        let (output_id, _) = fm.create_data_file_with_prefix("data").unwrap();
        let input_path = fm.get_data_file_full_path(input_id).unwrap();
        let output_path = fm.get_data_file_full_path(output_id).unwrap();

        let input_file = make_dedicated_file(input_id, &input_path, "00", "ff");
        let output_file = make_dedicated_file(output_id, &output_path, "00", "ff");
        let inputs = vec![DedicatedCompactionInput {
            level: 0,
            file: input_file,
        }];
        let outputs = vec![output_file];

        // Case 1: inputs gone, outputs present in LSM -> AppliedInMemory (not Committed).
        let live_output = make_live_file(&fm, output_id);
        let tree_version = make_tree_version(1, vec![live_output]);
        let status = classify_rewrite(&tree_version, &inputs, &outputs, 1, &fm);
        assert_eq!(status, OperationStatus::AppliedInMemory);

        // Case 2: inputs gone, outputs NOT in LSM -> still AppliedInMemory (not Conflict).
        let empty_tree = LSMTreeVersion { levels: vec![] };
        let status = classify_rewrite(&empty_tree, &inputs, &outputs, 1, &fm);
        assert_eq!(status, OperationStatus::AppliedInMemory);
    }

    /// When inputs are present and no outputs exist, classify_rewrite must return Pending.
    #[test]
    fn test_classify_rewrite_inputs_present_is_pending() {
        use crate::config::{Config, VolumeDescriptor};
        use crate::metrics_manager::MetricsManager;
        let dir = tempfile::tempdir_in("/tmp").unwrap();
        let base = format!("file://{}", dir.path().display());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(base.clone()),
            ..Config::default()
        };
        let metrics = Arc::new(MetricsManager::new("test"));
        let fm = Arc::new(FileManager::from_config(&config, "test-db", metrics).unwrap());

        // Create a real file so registration and path resolution work.
        let (input_id, _) = fm.create_data_file_with_prefix("data").unwrap();
        let (output_id, _) = fm.create_data_file_with_prefix("data").unwrap();
        let input_path = fm.get_data_file_full_path(input_id).unwrap();
        let output_path = fm.get_data_file_full_path(output_id).unwrap();

        let input_file = make_dedicated_file(input_id, &input_path, "00", "ff");
        let live_input = make_live_file(&fm, input_id);
        let inputs = vec![DedicatedCompactionInput {
            level: 0,
            file: input_file,
        }];
        let outputs = vec![make_dedicated_file(output_id, &output_path, "00", "ff")];
        let tree_version = make_tree_version(0, vec![live_input]);
        let status = classify_rewrite(&tree_version, &inputs, &outputs, 1, &fm);
        assert_eq!(status, OperationStatus::Pending);
    }

    /// classify_apply_error must map storage errors to PreserveAndRetry (not Terminal),
    /// so the poller preserves files instead of cleaning them up.
    #[test]
    fn test_classify_apply_error_storage_is_preserve() {
        let io_err = Error::IoError("disk unavailable".to_string());
        assert!(matches!(
            classify_apply_error(io_err),
            ApplyError::PreserveAndRetry(_)
        ));

        let fs_err = Error::FileSystemError("s3 down".to_string());
        assert!(matches!(
            classify_apply_error(fs_err),
            ApplyError::PreserveAndRetry(_)
        ));
    }

    /// classify_apply_error must map structural errors to Terminal.
    #[test]
    fn test_classify_apply_error_structural_is_terminal() {
        let state_err = Error::InvalidState("bad hex".to_string());
        assert!(matches!(
            classify_apply_error(state_err),
            ApplyError::Terminal(_)
        ));
    }

    /// build_path_to_id_from_lsm must return an error (not silently succeed) when an output
    /// is not found in the LSM. The caller maps this to PreserveAndRetry, ensuring files are
    /// preserved rather than cleaned up.
    #[test]
    fn test_build_path_to_id_from_lsm_missing_output_errors() {
        use crate::config::{Config, VolumeDescriptor};
        use crate::metrics_manager::MetricsManager;
        let dir = tempfile::tempdir_in("/tmp").unwrap();
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", dir.path().display())),
            ..Config::default()
        };
        let metrics = Arc::new(MetricsManager::new("test"));
        let fm = Arc::new(FileManager::from_config(&config, "test-db", metrics).unwrap());

        let output = make_dedicated_file(2, "file:///tmp/test/data/b.sst", "00", "ff");
        let empty_tree = LSMTreeVersion { levels: vec![] };
        let result = build_path_to_id_from_lsm(&empty_tree, &[output], &fm, "job-1");
        assert!(result.is_err(), "missing output must produce an error");
        // The error must NOT be a transient I/O error - it's a structural InvalidState that
        // the caller maps to PreserveAndRetry.
        let err = result.unwrap_err();
        assert!(!is_transient_error(&err));
    }

    #[test]
    fn snapshot_tree_lookup_uses_scope_instead_of_current_index() {
        let first_scope = LSMTreeScope::new(0u16..=1u16, 0);
        let shifted_scope = LSMTreeScope::new(2u16..=3u16, 0);
        let manifest = crate::snapshot::manifest::ManifestSnapshot {
            version: crate::snapshot::manifest::MANIFEST_VERSION_CURRENT,
            id: 7,
            seq_id: 11,
            latest_schema_id: 0,
            data_size_bytes: 0,
            incremental_data_size_bytes: 0,
            bucket_ranges: vec![0u16..=3u16],
            lsm_tree_bucket_ranges: vec![0u16..=1u16, 2u16..=3u16],
            tree_scopes: vec![first_scope, shifted_scope.clone()],
            tree_levels: vec![
                vec![crate::manifest_model::ManifestLevel {
                    ordinal: 1,
                    tiered: false,
                    files: Vec::new(),
                }],
                vec![crate::manifest_model::ManifestLevel {
                    ordinal: 7,
                    tiered: false,
                    files: Vec::new(),
                }],
            ],
            vlog_files: Vec::new(),
            active_memtable_data: Vec::new(),
            truncation_cursors: Vec::new(),
        };

        let levels = manifest_tree_levels_by_scope(&manifest, &shifted_scope)
            .unwrap()
            .unwrap();
        assert_eq!(levels[0].ordinal, 7);
        assert!(
            manifest_tree_levels_by_scope(&manifest, &LSMTreeScope::new(4u16..=5u16, 0))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn absence_only_operations_require_a_new_snapshot_proof() {
        let input = DedicatedCompactionInput {
            level: 0,
            file: make_dedicated_file(1, "file:///tmp/input.sst", "00", "ff"),
        };
        assert!(!operation_has_positive_manifest_evidence(
            &DedicatedCompactionOperation::Drop {
                inputs: vec![input.clone()],
            }
        ));
        assert!(!operation_has_positive_manifest_evidence(
            &DedicatedCompactionOperation::Rewrite {
                inputs: vec![input.clone()],
                output_level: 1,
                outputs: Vec::new(),
            }
        ));
        assert!(operation_has_positive_manifest_evidence(
            &DedicatedCompactionOperation::Rewrite {
                inputs: vec![input.clone()],
                output_level: 1,
                outputs: vec![make_dedicated_file(2, "file:///tmp/output.sst", "00", "ff",)],
            }
        ));
        assert!(operation_has_positive_manifest_evidence(
            &DedicatedCompactionOperation::TrivialMove {
                input,
                output_level: 1,
            }
        ));
    }
}
