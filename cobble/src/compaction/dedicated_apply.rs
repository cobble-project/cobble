//! Apply logic for dedicated compaction results.
//!
//! This module implements the writer-side application of a dedicated compaction result to the
//! in-memory LSM state, followed by manifest commit and result cleanup.
//!
//! The apply flow (see the dedicated compactor technical plan):
//! 1. Validate the result (operation type, complete fingerprints, output files exist/readable,
//!    output paths in job namespace, no duplicate file ids, key ranges in tree scope, vlog
//!    deltas valid).
//! 2. Operation-specific status judgment: Pending / AlreadyAppliedInMemory / Conflict.
//! 3. If Pending: allocate canonical file ids for Rewrite outputs, register them readonly,
//!    resolve real input Arcs from the current LSM, apply VersionEdit + VlogEdit.
//! 4. If Pending or AlreadyAppliedInMemory: check if the latest manifest already proves the
//!    operation committed; if not, run a snapshot barrier (flush + materialize with callback)
//!    and verify the manifest.
//! 5. Once the manifest is proven: make outputs owned, delete the result.
//! 6. If Conflict: clean up uncommitted outputs, delete the result.
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
use log::{debug, info};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

/// The outcome of attempting to apply a dedicated compaction result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExternalCompactionApplyResult {
    /// The result was applied to the in-memory LSM state and committed via a new manifest.
    Applied,
    /// The result was already applied (in-memory and manifest both confirm it). Just needs
    /// cleanup.
    AlreadyApplied,
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
/// - `Ok(Applied/AlreadyApplied/Conflict)` for recognized states.
/// - `Ok(TerminalInvalid)` for deterministically invalid results (bad structure, bad scope,
///   duplicate ids, bad paths, bad vlog deltas, job_id mismatch). The poller should delete the
///   result and its job directory.
/// - `Err(...)` only for transient failures (I/O errors, snapshot materialization failures,
///   manifest read errors) where retrying is safe.
pub(crate) fn apply_external_compaction_result(
    ctx: &PollerContext,
    result: &DedicatedCompactionResult,
    expected_job_id: &str,
) -> Result<ExternalCompactionApplyResult> {
    // Validate that the result's job_id matches the file name's job_id.
    if result.job_id != expected_job_id {
        return Ok(ExternalCompactionApplyResult::TerminalInvalid);
    }

    // Step 1: validate the result structure. Most validation errors are terminal
    // (structural), but I/O errors (e.g. remote storage temporarily unavailable)
    // should be retried.
    if let Err(err) = validate_result(ctx, result) {
        if is_transient_error(&err) {
            return Err(err);
        }
        return Ok(ExternalCompactionApplyResult::TerminalInvalid);
    }

    // Step 2: operation-specific status judgment + apply.
    // Returns the apply outcome and, for Rewrite, the mapping from compactor output path to
    // writer canonical file id (needed for finalize_outputs).
    let (apply_outcome, output_path_to_id) = match apply_operation(ctx, result) {
        Ok(v) => v,
        Err(err) => {
            // Classify the error: I/O errors (file open, registration, remote storage)
            // are transient and should be retried. Structural errors (input not found,
            // hex decode) are terminal.
            if is_transient_error(&err) {
                return Err(err);
            }
            debug!(
                "dedicated compaction result {} apply error (terminal): {}",
                result.job_id, err
            );
            return Ok(ExternalCompactionApplyResult::TerminalInvalid);
        }
    };

    match apply_outcome {
        ApplyOutcome::Applied => {
            // Step 4: commit via snapshot barrier.
            commit_and_verify(ctx, result)?;
            // Step 5: make outputs owned, delete result.
            finalize_outputs(ctx, &output_path_to_id)?;
            delete_dedicated_compaction_result(&ctx.file_manager, &result.job_id)?;
            Ok(ExternalCompactionApplyResult::Applied)
        }
        ApplyOutcome::AlreadyAppliedInMemory => {
            // Manifest may or may not be committed. Check and commit if needed.
            commit_and_verify(ctx, result)?;
            finalize_outputs(ctx, &output_path_to_id)?;
            delete_dedicated_compaction_result(&ctx.file_manager, &result.job_id)?;
            Ok(ExternalCompactionApplyResult::AlreadyApplied)
        }
        ApplyOutcome::AlreadyAppliedAndCommitted => {
            // Already fully committed - just clean up.
            finalize_outputs(ctx, &output_path_to_id)?;
            delete_dedicated_compaction_result(&ctx.file_manager, &result.job_id)?;
            Ok(ExternalCompactionApplyResult::AlreadyApplied)
        }
        ApplyOutcome::Conflict => {
            // Clean up uncommitted outputs and delete result.
            cleanup_uncommitted_outputs(ctx, result)?;
            delete_dedicated_compaction_result(&ctx.file_manager, &result.job_id)?;
            Ok(ExternalCompactionApplyResult::Conflict)
        }
    }
}

/// Internal outcome of the apply step.
enum ApplyOutcome {
    Applied,
    AlreadyAppliedInMemory,
    AlreadyAppliedAndCommitted,
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

// ---------------------------------------------------------------------------
// Operation apply
// ---------------------------------------------------------------------------

fn apply_operation(
    ctx: &PollerContext,
    result: &DedicatedCompactionResult,
) -> Result<(ApplyOutcome, HashMap<String, u64>)> {
    let outcome = match &result.operation {
        DedicatedCompactionOperation::Rewrite {
            inputs,
            output_level,
            outputs,
        } => apply_rewrite(ctx, result, inputs, *output_level, outputs)?,
        DedicatedCompactionOperation::TrivialMove {
            input,
            output_level,
        } => apply_trivial_move(ctx, result, input, *output_level)?,
        DedicatedCompactionOperation::Drop { inputs } => apply_drop(ctx, result, inputs)?,
    };
    // Build the path->id mapping from the result's outputs. For non-Rewrite operations
    // (TrivialMove, Drop) there are no outputs so the map is empty. For Rewrite where we
    // actually allocated ids, the mapping is built inside apply_rewrite and returned via
    // the ApplyOutcome. However, since the outcome is an enum, we reconstruct the map here
    // for the AlreadyApplied cases by matching the output paths against tracked files.
    let path_to_id = build_output_path_to_id_map(ctx, result)?;
    Ok((outcome, path_to_id))
}

/// Builds a mapping from compactor output path to writer canonical file id.
///
/// For a freshly-applied Rewrite, the outputs were just registered with known canonical ids.
/// For AlreadyApplied cases, we match by path against the FileManager's tracked data files.
///
/// **Every** output must be mapped. If any output path cannot be matched, this returns an
/// `InvalidState` error (terminal) so the poller cleans up and deletes the result rather than
/// silently leaving outputs unowned (which would leak files or, worse, delete committed data).
fn build_output_path_to_id_map(
    ctx: &PollerContext,
    result: &DedicatedCompactionResult,
) -> Result<HashMap<String, u64>> {
    let outputs = result.operation.outputs();
    let mut map = HashMap::with_capacity(outputs.len());
    for output in outputs {
        match find_file_id_by_path(&ctx.file_manager, &output.path) {
            Some(file_id) => {
                map.insert(output.path.clone(), file_id);
            }
            None => {
                return Err(Error::InvalidState(format!(
                    "dedicated compaction result {} could not map output path {} to canonical file id",
                    result.job_id, output.path
                )));
            }
        }
    }
    Ok(map)
}

/// Applies a Rewrite operation. Returns the apply outcome.
fn apply_rewrite(
    ctx: &PollerContext,
    result: &DedicatedCompactionResult,
    inputs: &[crate::compaction::dedicated::DedicatedCompactionInput],
    output_level: u8,
    outputs: &[DedicatedDataFile],
) -> Result<ApplyOutcome> {
    let tree_idx = find_tree_by_scope(ctx, &result.tree_scope)?;
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
            let prepared_outputs = prepare_outputs(ctx, outputs, output_level)?;
            // Resolve real input Arcs from the current LSM.
            let input_arcs = resolve_input_arcs(tree_version, inputs, &ctx.file_manager)?;
            // Build VersionEdit.
            let edit = build_rewrite_edit(input_arcs, inputs, &prepared_outputs, output_level);
            // Build VlogEdit.
            let vlog_edit = build_vlog_edit(&result.vlog_entry_deltas);
            // Apply.
            ctx.lsm_tree.apply_edit(tree_idx, edit, vlog_edit);
            Ok(ApplyOutcome::Applied)
        }
        OperationStatus::AlreadyApplied => Ok(ApplyOutcome::AlreadyAppliedAndCommitted),
        OperationStatus::AlreadyAppliedInMemory => Ok(ApplyOutcome::AlreadyAppliedInMemory),
        OperationStatus::Conflict => Ok(ApplyOutcome::Conflict),
    }
}

/// Applies a TrivialMove operation.
fn apply_trivial_move(
    ctx: &PollerContext,
    result: &DedicatedCompactionResult,
    input: &crate::compaction::dedicated::DedicatedCompactionInput,
    output_level: u8,
) -> Result<ApplyOutcome> {
    let tree_idx = find_tree_by_scope(ctx, &result.tree_scope)?;
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

    match input_arc {
        Some(arc) => {
            // Check if it's already in the target level (AlreadyApplied).
            if let Some(target_files) = target_level_files
                && target_files.iter().any(|f| {
                    input
                        .file
                        .matches_data_file(f, &ctx.file_manager)
                        .unwrap_or(false)
                })
            {
                return Ok(ApplyOutcome::AlreadyAppliedAndCommitted);
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
                && target_files.iter().any(|f| {
                    input
                        .file
                        .matches_data_file(f, &ctx.file_manager)
                        .unwrap_or(false)
                })
            {
                return Ok(ApplyOutcome::AlreadyAppliedAndCommitted);
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
) -> Result<ApplyOutcome> {
    let tree_idx = find_tree_by_scope(ctx, &result.tree_scope)?;
    let db_state = ctx.db_state.load();
    let tree_version = db_state.multi_lsm_version.version_of_index(tree_idx);
    let tree_version = &*tree_version;

    // Check if all inputs are still present (Pending) or all gone (AlreadyApplied).
    let all_present = inputs.iter().all(|input| {
        find_file_in_level(tree_version, input.level, &input.file, &ctx.file_manager).is_some()
    });
    let all_gone = inputs.iter().all(|input| {
        find_file_in_level(tree_version, input.level, &input.file, &ctx.file_manager).is_none()
    });

    if all_present {
        let input_arcs = resolve_input_arcs(tree_version, inputs, &ctx.file_manager)?;
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
        Ok(ApplyOutcome::AlreadyAppliedAndCommitted)
    } else {
        Ok(ApplyOutcome::Conflict)
    }
}

// ---------------------------------------------------------------------------
// Status classification
// ---------------------------------------------------------------------------

enum OperationStatus {
    Pending,
    AlreadyApplied,
    AlreadyAppliedInMemory,
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
        // Check if outputs are present in the LSM (AlreadyApplied) or not (in-memory only).
        if outputs.is_empty() {
            // Outputless rewrite: inputs gone = already applied.
            OperationStatus::AlreadyApplied
        } else {
            let all_outputs_present = outputs.iter().all(|output| {
                tree_version.levels.iter().any(|level| {
                    level.files.iter().any(|f| {
                        output
                            .matches_data_file_excluding_id(f, file_manager)
                            .unwrap_or(false)
                    })
                })
            });
            if all_outputs_present {
                OperationStatus::AlreadyApplied
            } else {
                // Inputs gone but outputs not in LSM - was applied in-memory but manifest
                // not yet committed. The in-memory state has moved on.
                OperationStatus::AlreadyAppliedInMemory
            }
        }
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
fn prepare_outputs(
    ctx: &PollerContext,
    outputs: &[DedicatedDataFile],
    output_level: u8,
) -> Result<Vec<Arc<DataFile>>> {
    let local_ids = ctx.file_manager.reserve_data_file_ids(outputs.len());
    let mut prepared = Vec::with_capacity(outputs.len());
    for (output, local_id) in outputs.iter().zip(local_ids.iter()) {
        let local_id = *local_id;
        // Register the output as readonly (delete_on_drop = false) so it survives until
        // make_data_file_owned is called after manifest commit.
        ctx.file_manager
            .register_data_file_readonly(local_id, &output.path)?;
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
        // Set the priority for the output level.
        let _ = ctx.file_manager.set_data_file_priority(
            local_id,
            crate::file::lsm_file_priority_for_level(output_level),
        );
        prepared.push(Arc::new(data_file));
    }
    Ok(prepared)
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

fn commit_and_verify(ctx: &PollerContext, result: &DedicatedCompactionResult) -> Result<()> {
    // Check if the latest manifest already proves this operation committed.
    if is_already_committed(ctx, result)? {
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
    let snapshot = ctx.snapshot_manager.create_snapshot(Some(callback));
    ctx.memtable_manager
        .flush_snapshot(snapshot.id, ctx.snapshot_manager.clone())?;

    // Wait for the materialization callback. This blocks until the snapshot is materialized
    // (or fails). The background materializer invokes the callback exactly once.
    // Use a timeout to avoid hanging forever if the materializer is stuck.
    let manifest_info = rx.recv_timeout(Duration::from_secs(30)).map_err(|_| {
        Error::InvalidState(format!(
            "dedicated compaction snapshot {} materialization timed out after 30s",
            snapshot.id
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

/// Checks if the latest manifest on disk already reflects this operation being applied.
fn is_already_committed(ctx: &PollerContext, result: &DedicatedCompactionResult) -> Result<bool> {
    let snapshot_ids = crate::snapshot::manifest::list_snapshot_manifest_ids(&ctx.file_manager)?;
    let Some(&latest_id) = snapshot_ids.last() else {
        return Ok(false);
    };
    let manifest = load_manifest_for_snapshot(&ctx.file_manager, latest_id)?;

    // Check if all inputs are absent and all outputs are present in the manifest.
    let tree_idx = find_tree_by_scope(ctx, &result.tree_scope)?;
    let tree_levels = manifest.tree_levels.get(tree_idx).ok_or_else(|| {
        Error::InvalidState(format!(
            "tree index {} not found in manifest {}",
            tree_idx, latest_id
        ))
    })?;

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
    let tree_idx = find_tree_by_scope(ctx, &result.tree_scope)?;
    let tree_levels = manifest.tree_levels.get(tree_idx).ok_or_else(|| {
        Error::InvalidState(format!(
            "tree index {} not found in manifest {}",
            tree_idx, manifest_info.id
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

/// Finds the canonical file id assigned to a compactor output by matching its absolute path
/// against the FileManager's tracked data files.
fn find_file_id_by_path(file_manager: &Arc<FileManager>, path: &str) -> Option<u64> {
    // Scan all tracked data files for one whose absolute path matches.
    // This is O(n) but n is small per result (typically 1-10 output files).
    file_manager.find_data_file_by_absolute_path(path)
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
fn is_transient_error(err: &Error) -> bool {
    matches!(err, Error::IoError(_) | Error::FileSystemError(_))
}
