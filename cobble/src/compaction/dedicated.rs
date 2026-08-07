//! Dedicated compaction result protocol.
//!
//! A dedicated compactor is a separate process that compacts SST files and publishes the result
//! as a delta file on the shared volume. The writer process polls for result files, validates
//! them, applies the compaction edit to its in-memory LSM state, commits a new manifest
//! snapshot, and then deletes the result file.
//!
//! The result is **not** a full LSM snapshot - it is a delta describing one compaction
//! operation (rewrite, trivial move, or drop). The manifest written by the writer remains the
//! sole commit record.
//!
//! Key design decisions (see the dedicated compactor technical plan):
//! - The result distinguishes operation types because trivial move must not be represented as
//!   new output files (the writer would wrongly allocate new file ids for them).
//! - Input/output file descriptors carry the complete set of immutable metadata fields (the
//!   same 13 fields as `ManifestFile`) so the writer can detect fingerprint mismatches.
//! - The result is atomically published via temp-file + crc32 trailer + rename.
use crate::db_state::LSMTreeScope;
use crate::error::{Error, Result};
use crate::file::FileManager;
use crate::file::{File, MetadataReader};
use crate::snapshot::manifest::{ManifestFile, from_hex, to_hex};
use log::debug;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Current version of the dedicated compaction result format.
pub(crate) const DEDICATED_COMPACTION_RESULT_VERSION: u32 = 3;

/// Directory (relative to the db base dir) where result files live.
pub(crate) const DEDICATED_COMPACTION_RESULTS_DIR: &str = "compaction/results";

/// Directory (relative to the db base dir) where per-job output files live.
pub(crate) const DEDICATED_COMPACTION_JOBS_DIR: &str = "compaction/jobs";

/// Prefix for result file names.
pub(crate) const DEDICATED_COMPACTION_RESULT_PREFIX: &str = "COMPACTION-";

/// Returns the relative metadata path for a result file: `compaction/results/COMPACTION-<job_id>`.
pub(crate) fn dedicated_compaction_result_name(job_id: &str) -> String {
    format!(
        "{}/{}{}",
        DEDICATED_COMPACTION_RESULTS_DIR, DEDICATED_COMPACTION_RESULT_PREFIX, job_id
    )
}

/// Returns the relative path prefix for a job's output data files:
/// `compaction/jobs/<job_id>/data`.
pub(crate) fn dedicated_compaction_job_output_prefix(job_id: &str) -> String {
    format!("{}/{}/data", DEDICATED_COMPACTION_JOBS_DIR, job_id)
}

/// Parses a job id from a result file name (the basename within `compaction/results/`).
pub(crate) fn parse_dedicated_compaction_job_id(name: &str) -> Option<String> {
    let name = name.rsplit('/').next().unwrap_or(name);
    name.strip_prefix(DEDICATED_COMPACTION_RESULT_PREFIX)
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

/// Complete immutable descriptor for a data file, covering the same fields as `ManifestFile`.
///
/// For inputs this is the writer's canonical file id and path. For outputs the `file_id` is the
/// compactor's process-local id; the writer remaps it to a canonical id on apply.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DedicatedDataFile {
    pub file_id: u64,
    pub file_type: String,
    pub path: String,
    pub schema_id: u64,
    pub size: usize,
    pub start_key: String,
    pub end_key: String,
    pub has_separated_values: bool,
    pub bucket_range_start: u16,
    pub bucket_range_end: u16,
    pub effective_bucket_range_start: u16,
    pub effective_bucket_range_end: u16,
    pub vlog_file_seq_offset: u32,
    #[serde(default)]
    pub max_expired_at: u32,
}

impl From<&ManifestFile> for DedicatedDataFile {
    fn from(file: &ManifestFile) -> Self {
        Self {
            file_id: file.file_id,
            file_type: file.file_type.clone(),
            path: file.path.clone(),
            schema_id: file.schema_id,
            size: file.size,
            start_key: file.start_key.clone(),
            end_key: file.end_key.clone(),
            has_separated_values: file.has_separated_values,
            bucket_range_start: file.bucket_range_start,
            bucket_range_end: file.bucket_range_end,
            effective_bucket_range_start: file.effective_bucket_range_start,
            effective_bucket_range_end: file.effective_bucket_range_end,
            vlog_file_seq_offset: file.vlog_file_seq_offset,
            max_expired_at: file.max_expired_at,
        }
    }
}

impl DedicatedDataFile {
    /// Builds a descriptor from a live `DataFile`, resolving its physical path via the
    /// `FileManager`. Keys are hex-encoded to match the `ManifestFile` representation.
    pub(crate) fn from_data_file(
        file: &crate::data_file::DataFile,
        file_manager: &Arc<FileManager>,
    ) -> Result<Self> {
        let path = file_manager
            .get_data_file_full_path(file.file_id)
            .ok_or_else(|| {
                Error::IoError(format!(
                    "Missing data file path for file_id={}",
                    file.file_id
                ))
            })?;
        Ok(Self::from_data_file_with_path(file, path))
    }

    /// Builds a descriptor from a live `DataFile` with an explicit path.
    pub(crate) fn from_data_file_with_path(
        file: &crate::data_file::DataFile,
        path: String,
    ) -> Self {
        Self {
            file_id: file.file_id,
            file_type: file.file_type.to_string(),
            path,
            schema_id: file.schema_id,
            size: file.size,
            start_key: to_hex(&file.start_key),
            end_key: to_hex(&file.end_key),
            has_separated_values: file.has_separated_values,
            bucket_range_start: *file.bucket_range.start(),
            bucket_range_end: *file.bucket_range.end(),
            effective_bucket_range_start: *file.effective_bucket_range.start(),
            effective_bucket_range_end: *file.effective_bucket_range.end(),
            vlog_file_seq_offset: file.vlog_file_seq_offset,
            max_expired_at: file.max_expired_at(),
        }
    }

    /// Decodes hex-encoded key fields back to bytes, returning all fields needed to construct
    /// or verify a `DataFile`.
    ///
    /// A malformed hex string is a **protocol-level** error (the result file is corrupt), not a
    /// transient I/O error. It is mapped to `InvalidState` so the poller classifies it as
    /// terminal and deletes the poison result instead of retrying forever.
    pub(crate) fn decode_keys(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        let start = from_hex(&self.start_key).map_err(|e| {
            Error::InvalidState(format!(
                "dedicated compaction file {} has invalid start_key hex: {}",
                self.file_id, e
            ))
        })?;
        let end = from_hex(&self.end_key).map_err(|e| {
            Error::InvalidState(format!(
                "dedicated compaction file {} has invalid end_key hex: {}",
                self.file_id, e
            ))
        })?;
        Ok((start, end))
    }

    /// Compares this descriptor against a `ManifestFile` for full fingerprint equality,
    /// **excluding `file_id`**.
    ///
    /// This is used for output verification: the compactor's output `file_id` is a process-local
    /// id, while the manifest stores the writer's canonical id. The remaining 12 fields form a
    /// complete fingerprint that uniquely identifies the file's content and metadata.
    pub(crate) fn matches_manifest_file_excluding_id(&self, file: &ManifestFile) -> bool {
        self.file_type == file.file_type
            && self.path == file.path
            && self.schema_id == file.schema_id
            && self.size == file.size
            && self.start_key == file.start_key
            && self.end_key == file.end_key
            && self.has_separated_values == file.has_separated_values
            && self.bucket_range_start == file.bucket_range_start
            && self.bucket_range_end == file.bucket_range_end
            && self.effective_bucket_range_start == file.effective_bucket_range_start
            && self.effective_bucket_range_end == file.effective_bucket_range_end
            && self.vlog_file_seq_offset == file.vlog_file_seq_offset
            && self.max_expired_at == file.max_expired_at
    }

    /// Compares this descriptor against a `ManifestFile` for full fingerprint equality.
    pub(crate) fn matches_manifest_file(&self, file: &ManifestFile) -> bool {
        self.file_id == file.file_id
            && self.file_type == file.file_type
            && self.path == file.path
            && self.schema_id == file.schema_id
            && self.size == file.size
            && self.start_key == file.start_key
            && self.end_key == file.end_key
            && self.has_separated_values == file.has_separated_values
            && self.bucket_range_start == file.bucket_range_start
            && self.bucket_range_end == file.bucket_range_end
            && self.effective_bucket_range_start == file.effective_bucket_range_start
            && self.effective_bucket_range_end == file.effective_bucket_range_end
            && self.vlog_file_seq_offset == file.vlog_file_seq_offset
            && self.max_expired_at == file.max_expired_at
    }

    /// Compares this descriptor against a live `DataFile` for full fingerprint equality,
    /// **excluding `file_id`**.
    ///
    /// Used for output matching: the compactor's output `file_id` is a process-local id,
    /// while the writer's `DataFile` has a canonical id. The remaining 12 fields (including
    /// path) form a complete fingerprint. The `FileManager` is needed to resolve the
    /// `DataFile`'s absolute path for comparison.
    pub(crate) fn matches_data_file_excluding_id(
        &self,
        file: &crate::data_file::DataFile,
        file_manager: &Arc<FileManager>,
    ) -> Result<bool> {
        let (start_key, end_key) = self.decode_keys()?;
        let file_path = file_manager.get_data_file_full_path(file.file_id);
        let path_matches = match &file_path {
            Some(p) => *p == self.path,
            None => false,
        };
        Ok(path_matches
            && self.file_type == file.file_type.to_string()
            && self.schema_id == file.schema_id
            && self.size == file.size
            && start_key == file.start_key
            && end_key == file.end_key
            && self.has_separated_values == file.has_separated_values
            && self.bucket_range_start == *file.bucket_range.start()
            && self.bucket_range_end == *file.bucket_range.end()
            && self.effective_bucket_range_start == *file.effective_bucket_range.start()
            && self.effective_bucket_range_end == *file.effective_bucket_range.end()
            && self.vlog_file_seq_offset == file.vlog_file_seq_offset
            && self.max_expired_at == file.max_expired_at())
    }

    /// Compares this descriptor against a live `DataFile` for full fingerprint equality (all
    /// 13 fields, including `file_id` and `path`).
    ///
    /// Keys are hex-encoded in the descriptor but raw bytes on the `DataFile`, so this method
    /// decodes the hex before comparing. The `FileManager` is needed to resolve the `DataFile`'s
    /// absolute path for comparison (the descriptor stores the volume-absolute path).
    pub(crate) fn matches_data_file(
        &self,
        file: &crate::data_file::DataFile,
        file_manager: &Arc<FileManager>,
    ) -> Result<bool> {
        let (start_key, end_key) = self.decode_keys()?;
        let file_path = file_manager.get_data_file_full_path(file.file_id);
        let path_matches = match &file_path {
            Some(p) => *p == self.path,
            None => false,
        };
        Ok(path_matches
            && self.file_id == file.file_id
            && self.file_type == file.file_type.to_string()
            && self.schema_id == file.schema_id
            && self.size == file.size
            && start_key == file.start_key
            && end_key == file.end_key
            && self.has_separated_values == file.has_separated_values
            && self.bucket_range_start == *file.bucket_range.start()
            && self.bucket_range_end == *file.bucket_range.end()
            && self.effective_bucket_range_start == *file.effective_bucket_range.start()
            && self.effective_bucket_range_end == *file.effective_bucket_range.end()
            && self.vlog_file_seq_offset == file.vlog_file_seq_offset
            && self.max_expired_at == file.max_expired_at())
    }
}

/// An input file for a dedicated compaction, tagged with its source level.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DedicatedCompactionInput {
    pub level: u8,
    pub file: DedicatedDataFile,
}

/// The kind of compaction operation a result represents.
///
/// `TrivialMove` and `Drop` must be distinct from `Rewrite` so the writer does not allocate
/// new canonical file ids for files that are merely relocated or removed.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum DedicatedCompactionOperation {
    /// A normal rewrite compaction. The writer allocates canonical file ids for `outputs`.
    Rewrite {
        inputs: Vec<DedicatedCompactionInput>,
        output_level: u8,
        outputs: Vec<DedicatedDataFile>,
    },
    /// A trivial move: the same canonical file (same Arc, same file id, same physical file) is
    /// moved from its source level to `output_level`. No new physical file is produced and no
    /// file-id remap occurs.
    TrivialMove {
        input: DedicatedCompactionInput,
        output_level: u8,
    },
    /// A truncated drop: only input removals (and optional vlog deltas) are produced.
    Drop {
        inputs: Vec<DedicatedCompactionInput>,
    },
}

impl DedicatedCompactionOperation {
    /// Returns all input descriptors in this operation.
    pub(crate) fn inputs(&self) -> Vec<&DedicatedCompactionInput> {
        match self {
            DedicatedCompactionOperation::Rewrite { inputs, .. } => inputs.iter().collect(),
            DedicatedCompactionOperation::TrivialMove { input, .. } => vec![input],
            DedicatedCompactionOperation::Drop { inputs } => inputs.iter().collect(),
        }
    }

    /// Returns the output level, if the operation produces placement into a level.
    pub(crate) fn output_level(&self) -> Option<u8> {
        match self {
            DedicatedCompactionOperation::Rewrite { output_level, .. }
            | DedicatedCompactionOperation::TrivialMove { output_level, .. } => Some(*output_level),
            DedicatedCompactionOperation::Drop { .. } => None,
        }
    }

    /// Returns the output file descriptors, if any.
    pub(crate) fn outputs(&self) -> &[DedicatedDataFile] {
        match self {
            DedicatedCompactionOperation::Rewrite { outputs, .. } => outputs,
            _ => &[],
        }
    }
}

/// Durable writer observation used to plan a dedicated compaction result.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum DedicatedCompactionSource {
    Runtime { generation: u64, seq_id: u64 },
    Snapshot { snapshot_id: u64, seq_id: u64 },
}

/// A dedicated compaction result: a delta describing one compaction, published by the
/// compactor process and consumed by the writer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DedicatedCompactionResult {
    pub version: u32,
    pub job_id: String,
    /// The durable layout the compactor used to plan this operation. This is diagnostic only:
    /// the writer applies against its current LSM fingerprints, not a requirement that this
    /// source is still the latest when the result arrives.
    pub source: DedicatedCompactionSource,
    /// Hint only; the writer uses `tree_scope` to locate the current tree.
    pub lsm_tree_idx: usize,
    pub tree_scope: LSMTreeScope,
    pub operation: DedicatedCompactionOperation,
    /// Per-vlog-file entry count deltas (file_seq, delta). Shared across all operation types.
    pub vlog_entry_deltas: Vec<(u32, i64)>,
    pub created_at_ms: u64,
}

impl DedicatedCompactionResult {
    /// Serializes the result as JSON bytes.
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|err| {
            Error::IoError(format!(
                "Failed to encode dedicated compaction result: {}",
                err
            ))
        })
    }

    /// Deserializes a result from JSON bytes.
    ///
    /// Both JSON parse failures and version mismatches are **protocol-level** errors (the result
    /// file is corrupt or from an incompatible version). They are mapped to `InvalidState` so
    /// the poller classifies them as terminal and deletes the poison result instead of retrying
    /// forever.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let result: DedicatedCompactionResult = serde_json::from_slice(bytes).map_err(|err| {
            Error::InvalidState(format!(
                "Failed to decode dedicated compaction result: {}",
                err
            ))
        })?;
        if result.version != DEDICATED_COMPACTION_RESULT_VERSION {
            return Err(Error::InvalidState(format!(
                "Unsupported dedicated compaction result version: {} (expected {})",
                result.version, DEDICATED_COMPACTION_RESULT_VERSION
            )));
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Result file I/O: atomic publish, read, scan, delete
// ---------------------------------------------------------------------------

/// Atomically publishes a result to `compaction/results/COMPACTION-<job_id>`.
///
/// Writes to a temp file with a crc32 trailer, then renames to the final name.
pub(crate) fn publish_dedicated_compaction_result(
    file_manager: &Arc<FileManager>,
    result: &DedicatedCompactionResult,
) -> Result<()> {
    use crate::file::BufferedWriter;
    let name = dedicated_compaction_result_name(&result.job_id);
    let payload = result.encode()?;
    // create_metadata_file returns an AtomicMetadataWriter that writes to a temp path and
    // renames on close, appending a crc32 trailer.
    let writer = file_manager.create_metadata_file(&name)?;
    let mut buffered = BufferedWriter::new(writer, 8192);
    buffered.write(&payload)?;
    buffered.close()?;
    Ok(())
}

/// Reads and decodes a result by job id. Verifies the crc32 trailer.
pub(crate) fn read_dedicated_compaction_result(
    file_manager: &Arc<FileManager>,
    job_id: &str,
) -> Result<DedicatedCompactionResult> {
    let name = dedicated_compaction_result_name(job_id);
    let reader = file_manager.open_metadata_file_reader_untracked(&name)?;
    let bytes = MetadataReader::new(reader).read_all()?;
    DedicatedCompactionResult::decode(bytes.as_ref())
}

/// Lists all job ids whose result files are currently present in `compaction/results/`.
///
/// This scans the underlying filesystem directly (not the in-memory metadata index) so it works
/// across processes.
pub(crate) fn list_dedicated_compaction_result_job_ids(
    file_manager: &Arc<FileManager>,
) -> Result<Vec<String>> {
    let names = file_manager.list_metadata_names(DEDICATED_COMPACTION_RESULTS_DIR)?;
    let mut job_ids: Vec<String> = names
        .into_iter()
        .filter_map(|name| parse_dedicated_compaction_job_id(&name))
        .collect();
    job_ids.sort();
    job_ids.dedup();
    Ok(job_ids)
}

/// Deletes a result file by job id. Works even if the file is not tracked in this process's
/// in-memory metadata index.
pub(crate) fn delete_dedicated_compaction_result(
    file_manager: &Arc<FileManager>,
    job_id: &str,
) -> Result<()> {
    let name = dedicated_compaction_result_name(job_id);
    file_manager.remove_metadata_file(&name)
}

/// Removes all files under a job's output directory, then the job directory itself.
/// Used to clean up after a terminal-invalid result or a conflict.
///
/// Output files are written to data volumes, so they are removed via data-volume-aware APIs.
/// The lease file lives on the metadata volume (see `write_job_lease`), so it is removed
/// separately.
pub(crate) fn cleanup_job_dir(file_manager: &Arc<FileManager>, job_id: &str) -> Result<()> {
    let data_prefix = dedicated_compaction_job_output_prefix(job_id);
    let files = file_manager.list_data_volume_names(&data_prefix)?;
    for file_name in files {
        let path = format!("{}/{}", data_prefix, file_name);
        let _ = file_manager.remove_data_volume_path(&path);
    }
    // Remove the data subdir and the job dir itself on data volumes.
    let _ = file_manager.remove_data_volume_path(&data_prefix);
    let job_dir = format!("{}/{}", DEDICATED_COMPACTION_JOBS_DIR, job_id);
    let _ = file_manager.remove_data_volume_path(&job_dir);
    // Remove the lease file and job directory from the metadata volume.
    let lease_path = format!("{}/{}", job_dir, DEDICATED_COMPACTION_LEASE_FILE);
    let _ = file_manager.remove_metadata_volume_path(&lease_path);
    let _ = file_manager.remove_metadata_volume_path(&job_dir);
    Ok(())
}

/// Returns true if a result file for the given job id exists on the shared volume.
pub(crate) fn dedicated_compaction_result_exists(
    file_manager: &Arc<FileManager>,
    job_id: &str,
) -> Result<bool> {
    let name = dedicated_compaction_result_name(job_id);
    file_manager.metadata_file_exists_untracked(&name)
}

// ---------------------------------------------------------------------------
// Orphan sweep: clean up stale job directories
// ---------------------------------------------------------------------------

/// Name of the lease file written by the compactor into each job directory.
/// The file's last-modified time serves as a heartbeat; the writer's orphan sweep
/// only removes job directories whose lease is older than `min_age_ms`.
pub(crate) const DEDICATED_COMPACTION_LEASE_FILE: &str = "LEASE";

/// Writes a lease file into the job directory on the **metadata volume**, creating the
/// directory if needed. The compactor calls this before starting work and periodically
/// refreshes it. The file's last-modified time serves as a heartbeat for the writer's orphan
/// sweep.
///
/// The lease lives on the metadata volume (not a data volume) because:
/// - The metadata volume is a single, deterministic volume, so the writer's sweep always finds
///   the lease regardless of which data volume the compactor happened to write outputs to.
/// - This avoids the multi-volume problem where a heartbeat refresh could pick a different
///   data volume than the initial write, leaving a stale lease behind.
pub(crate) fn write_job_lease(file_manager: &Arc<FileManager>, job_id: &str) -> Result<()> {
    let lease_path = format!(
        "{}/{}/{}",
        DEDICATED_COMPACTION_JOBS_DIR, job_id, DEDICATED_COMPACTION_LEASE_FILE
    );
    file_manager.write_metadata_volume_file(&lease_path, b"lease")?;
    Ok(())
}

/// Sweeps `compaction/jobs/` for orphaned job directories and removes them.
///
/// A job directory is an orphan if:
/// - It has no corresponding result file in `compaction/results/`.
/// - No file within it is referenced by the latest manifest.
/// - Its lease file is older than `min_age_ms` (to avoid racing with an active compactor).
///
/// This is called periodically by the writer's poller and on startup to reclaim
/// space left by crashed compactor processes.
///
/// Uses data-volume-aware APIs because compaction output files are written to data volumes.
pub(crate) fn sweep_orphan_job_dirs(
    file_manager: &Arc<FileManager>,
    manifest_paths: &std::collections::HashSet<String>,
    min_age_ms: u64,
) -> Result<usize> {
    // Job directories live on data volumes (same volume as the output files).
    let job_dir_names = file_manager.list_data_volume_names(DEDICATED_COMPACTION_JOBS_DIR)?;
    let active_result_job_ids = list_dedicated_compaction_result_job_ids(file_manager)?;
    // The filesystem's `last_modified` returns unix timestamps in **seconds** (see
    // `posix_fs.rs` / `opendal_fs.rs`), so we compute `now` in seconds for the age comparison.
    // `min_age_ms` is in milliseconds; convert it to seconds (rounding up so a sub-second
    // min age still requires at least 1 second of staleness).
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let min_age_secs = min_age_ms.div_ceil(1000);

    let mut swept = 0;
    for job_name in &job_dir_names {
        // job_name is the basename of the job directory.
        let job_id = job_name.rsplit('/').next().unwrap_or(job_name);
        // Skip if there's an active result for this job (compactor finished, writer
        // hasn't consumed it yet).
        if active_result_job_ids.iter().any(|id| id == job_id) {
            continue;
        }
        // Check the lease file's age. If the lease is recent, the compactor is still
        // actively working on this job. The lease lives on the metadata volume (see
        // `write_job_lease`), so we read it from there.
        let lease_path = format!(
            "{}/{}/{}",
            DEDICATED_COMPACTION_JOBS_DIR, job_id, DEDICATED_COMPACTION_LEASE_FILE
        );
        let last_modified = file_manager.metadata_volume_last_modified(&lease_path)?;
        match last_modified {
            Some(ts) => {
                if now_secs.saturating_sub(ts) < min_age_secs {
                    continue; // Lease is still fresh; skip.
                }
            }
            None => {
                // No lease file on the metadata volume. This could be a job directory from
                // an older version that wrote leases to data volumes, or a partially created
                // directory. Fall back to checking the job directory's own mtime on data
                // volumes.
                let job_dir = format!("{}/{}", DEDICATED_COMPACTION_JOBS_DIR, job_id);
                let dir_mtime = file_manager.data_volume_last_modified(&job_dir)?;
                match dir_mtime {
                    Some(ts) if now_secs.saturating_sub(ts) < min_age_secs => continue,
                    None => continue, // Can't determine age; skip.
                    _ => {}
                }
            }
        }
        // Check if any file in this job dir is referenced by the latest manifest.
        // Manifest paths are absolute (volume-prefixed, e.g. file://.../compaction/jobs/.../data/...),
        // so we must resolve the relative paths to absolute for comparison.
        let job_data_prefix = dedicated_compaction_job_output_prefix(job_id);
        let job_files = file_manager.list_data_volume_names(&job_data_prefix)?;
        let referenced = job_files.iter().any(|file_name| {
            let relative_path = format!("{}/{}", job_data_prefix, file_name);
            let absolute_paths = file_manager.data_volume_absolute_paths(&relative_path);
            absolute_paths.iter().any(|p| manifest_paths.contains(p))
        });
        if referenced {
            continue;
        }
        // Delete all files in the job directory, then the directory itself. Output files live
        // on data volumes; the lease file lives on the metadata volume.
        debug!("sweeping orphan job directory {}", job_id);
        for file_name in &job_files {
            let path = format!("{}/{}", job_data_prefix, file_name);
            let _ = file_manager.remove_data_volume_path(&path);
        }
        let _ = file_manager.remove_data_volume_path(&job_data_prefix);
        let job_dir = format!("{}/{}", DEDICATED_COMPACTION_JOBS_DIR, job_id);
        let _ = file_manager.remove_data_volume_path(&job_dir);
        // Also clean up the lease file and job directory on the metadata volume.
        let lease_path = format!("{}/{}", job_dir, DEDICATED_COMPACTION_LEASE_FILE);
        let _ = file_manager.remove_metadata_volume_path(&lease_path);
        let _ = file_manager.remove_metadata_volume_path(&job_dir);
        swept += 1;
    }
    Ok(swept)
}

/// Collects all file paths referenced by a manifest snapshot's tree levels.
pub(crate) fn collect_manifest_file_paths(
    manifest: &crate::snapshot::manifest::ManifestSnapshot,
) -> std::collections::HashSet<String> {
    let mut paths = std::collections::HashSet::new();
    for tree_levels in &manifest.tree_levels {
        for level in tree_levels {
            for file in &level.files {
                paths.insert(file.path.clone());
            }
        }
    }
    paths
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_result_round_trip() {
        let result = DedicatedCompactionResult {
            version: DEDICATED_COMPACTION_RESULT_VERSION,
            job_id: "test-job-123".to_string(),
            source: DedicatedCompactionSource::Runtime {
                generation: 42,
                seq_id: 99,
            },
            lsm_tree_idx: 0,
            tree_scope: LSMTreeScope::new(0u16..=0u16, 0),
            operation: DedicatedCompactionOperation::Rewrite {
                inputs: vec![DedicatedCompactionInput {
                    level: 0,
                    file: DedicatedDataFile {
                        file_id: 1,
                        file_type: "SSTable".to_string(),
                        path: "data/abc.sst".to_string(),
                        schema_id: 1,
                        size: 100,
                        start_key: "00".to_string(),
                        end_key: "ff".to_string(),
                        has_separated_values: false,
                        bucket_range_start: 0,
                        bucket_range_end: 0,
                        effective_bucket_range_start: 0,
                        effective_bucket_range_end: 0,
                        vlog_file_seq_offset: 0,
                        max_expired_at: 0,
                    },
                }],
                output_level: 1,
                outputs: vec![],
            },
            vlog_entry_deltas: vec![(5, -3)],
            created_at_ms: 1234567890,
        };
        let bytes = result.encode().unwrap();
        let decoded = DedicatedCompactionResult::decode(&bytes).unwrap();
        assert_eq!(decoded.job_id, result.job_id);
        assert_eq!(decoded.source, result.source);
        assert_eq!(decoded.operation, result.operation);
        assert_eq!(decoded.vlog_entry_deltas, result.vlog_entry_deltas);
    }

    #[test]
    fn test_result_version_rejected() {
        let bytes = serde_json::json!({
            "version": 999,
            "job_id": "x",
            "source": { "kind": "runtime", "generation": 0, "seq_id": 0 },
            "lsm_tree_idx": 0,
            "tree_scope": { "bucket_range": [0, 0], "column_family_id": 0 },
            "operation": { "Drop": { "inputs": [] } },
            "vlog_entry_deltas": [],
            "created_at_ms": 0,
        })
        .to_string()
        .into_bytes();
        assert!(DedicatedCompactionResult::decode(&bytes).is_err());
    }

    #[test]
    fn test_parse_job_id() {
        assert_eq!(
            parse_dedicated_compaction_job_id("COMPACTION-abc-123"),
            Some("abc-123".to_string())
        );
        assert_eq!(parse_dedicated_compaction_job_id("COMPACTION-"), None);
        assert_eq!(parse_dedicated_compaction_job_id("SNAPSHOT-5"), None);
        assert_eq!(
            parse_dedicated_compaction_job_id("compaction/results/COMPACTION-xyz"),
            Some("xyz".to_string())
        );
    }

    #[test]
    fn test_operation_helpers() {
        let input = DedicatedCompactionInput {
            level: 0,
            file: DedicatedDataFile {
                file_id: 1,
                file_type: "SSTable".to_string(),
                path: "data/a.sst".to_string(),
                schema_id: 1,
                size: 10,
                start_key: "00".to_string(),
                end_key: "ff".to_string(),
                has_separated_values: false,
                bucket_range_start: 0,
                bucket_range_end: 0,
                effective_bucket_range_start: 0,
                effective_bucket_range_end: 0,
                vlog_file_seq_offset: 0,
                max_expired_at: 0,
            },
        };
        let rewrite = DedicatedCompactionOperation::Rewrite {
            inputs: vec![input.clone()],
            output_level: 1,
            outputs: Vec::new(),
        };
        assert_eq!(rewrite.inputs().len(), 1);
        assert_eq!(rewrite.output_level(), Some(1));
        assert!(rewrite.outputs().is_empty());

        let mv = DedicatedCompactionOperation::TrivialMove {
            input: input.clone(),
            output_level: 2,
        };
        assert_eq!(mv.inputs().len(), 1);
        assert_eq!(mv.output_level(), Some(2));
        assert!(mv.outputs().is_empty());

        let drop_op = DedicatedCompactionOperation::Drop {
            inputs: vec![input],
        };
        assert_eq!(drop_op.inputs().len(), 1);
        assert_eq!(drop_op.output_level(), None);
        assert!(drop_op.outputs().is_empty());
    }

    /// Builds a FileManager backed by **separate** metadata and data volumes, each under a
    /// unique tempfile directory. The metadata volume holds leases/manifests; the data volume
    /// holds compaction output files. This mirrors a multi-volume production setup and verifies
    /// the sweep correctly checks the lease on the metadata volume while deleting output files
    /// on the data volume.
    fn build_fm_multi_volume(
        db_id: &str,
    ) -> (Arc<FileManager>, tempfile::TempDir, tempfile::TempDir) {
        use crate::config::{Config, VolumeDescriptor, VolumeUsageKind};
        use crate::metrics_manager::MetricsManager;
        let meta_dir = tempfile::tempdir_in("/tmp").expect("create meta tempdir");
        let data_dir = tempfile::tempdir_in("/tmp").expect("create data tempdir");
        let config = Config {
            volumes: vec![
                VolumeDescriptor::new(
                    format!("file://{}", meta_dir.path().display()),
                    vec![VolumeUsageKind::Meta, VolumeUsageKind::Snapshot],
                ),
                VolumeDescriptor::new(
                    format!("file://{}", data_dir.path().display()),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
            ],
            ..Config::default()
        };
        let metrics = std::sync::Arc::new(MetricsManager::new("sweep-test"));
        let fm = Arc::new(FileManager::from_config(&config, db_id, metrics).unwrap());
        (fm, meta_dir, data_dir)
    }

    /// The orphan sweep must not delete:
    /// - Job directories with a fresh lease (active compactor).
    /// - Job directories whose outputs are referenced by the manifest (committed).
    ///
    /// And must delete:
    /// - Job directories with an expired lease, no result, and no manifest reference (crashed).
    ///
    /// This test uses separate metadata and data volumes to verify the lease (on the metadata
    /// volume) is correctly checked while output files (on the data volume) are correctly
    /// matched against manifest paths and deleted.
    #[test]
    fn test_orphan_sweep_preserves_active_and_committed() {
        let db_id = "sweep-db";
        let (fm, _meta_dir, _data_dir) = build_fm_multi_volume(db_id);

        // --- Active job: fresh lease, no result, not referenced. Should survive. ---
        let active_job = "job-active";
        write_job_lease(&fm, active_job).unwrap();
        // Create a dummy output file under the job's data dir on the data volume.
        fm.create_data_file_with_prefix(&format!(
            "{}/{}/data",
            DEDICATED_COMPACTION_JOBS_DIR, active_job
        ))
        .unwrap();

        // --- Crashed job: stale lease (written then we sleep past min_age), no result,
        //     not referenced. Should be deleted. ---
        let crashed_job = "job-crashed";
        write_job_lease(&fm, crashed_job).unwrap();
        fm.create_data_file_with_prefix(&format!(
            "{}/{}/data",
            DEDICATED_COMPACTION_JOBS_DIR, crashed_job
        ))
        .unwrap();

        // --- Committed job: stale lease, no result, but output is referenced by manifest.
        //     Should survive. ---
        let committed_job = "job-committed";
        write_job_lease(&fm, committed_job).unwrap();
        let (committed_file_id, _writer) = fm
            .create_data_file_with_prefix(&format!(
                "{}/{}/data",
                DEDICATED_COMPACTION_JOBS_DIR, committed_job
            ))
            .unwrap();
        // Build the manifest path set that includes the committed job's output.
        let committed_path = fm
            .get_data_file_full_path(committed_file_id)
            .expect("committed file path");
        let mut manifest_paths = std::collections::HashSet::new();
        manifest_paths.insert(committed_path);

        // The filesystem's `last_modified` returns timestamps in seconds, so the sweep
        // compares age in seconds. Sleep 2 seconds so the crashed and committed leases are
        // stale (>= 1s old with min_age_ms=100 -> min_age_secs=1), then refresh the active
        // job's lease so it's fresh (< 1s old).
        std::thread::sleep(std::time::Duration::from_secs(2));
        write_job_lease(&fm, active_job).unwrap();

        // Sweep with a 100ms min age (rounds up to 1s). The active job's lease is fresh,
        // so it survives. The crashed and committed jobs have stale leases. The committed
        // job survives because its output is referenced by the manifest. Only the crashed
        // job is swept.
        let swept = sweep_orphan_job_dirs(&fm, &manifest_paths, 100).unwrap();

        // Only the crashed job should have been swept.
        assert_eq!(swept, 1, "exactly the crashed job should be swept");

        // Active job dir should still exist (lease is fresh).
        let active_dir = format!("{}/{}", DEDICATED_COMPACTION_JOBS_DIR, active_job);
        assert!(
            fm.data_volume_path_exists(&active_dir).unwrap(),
            "active job directory should survive (fresh lease)"
        );

        // Committed job dir should still exist (output referenced by manifest).
        let committed_dir = format!("{}/{}", DEDICATED_COMPACTION_JOBS_DIR, committed_job);
        assert!(
            fm.data_volume_path_exists(&committed_dir).unwrap(),
            "committed job directory should survive (referenced by manifest)"
        );

        // Crashed job dir should be gone.
        let crashed_dir = format!("{}/{}", DEDICATED_COMPACTION_JOBS_DIR, crashed_job);
        assert!(
            !fm.data_volume_path_exists(&crashed_dir).unwrap(),
            "crashed job directory should be swept"
        );
    }

    #[test]
    fn test_dedicated_data_file_preserves_max_expired_at() {
        let manifest_file = ManifestFile {
            file_id: 10,
            file_type: "sst".to_string(),
            schema_id: 1,
            size: 100,
            start_key: "61".to_string(),
            end_key: "7a".to_string(),
            path: "data/10.sst".to_string(),
            has_separated_values: false,
            bucket_range_start: 0,
            bucket_range_end: 0,
            effective_bucket_range_start: 0,
            effective_bucket_range_end: 0,
            vlog_file_seq_offset: 0,
            max_expired_at: 5000,
        };
        let dedicated: DedicatedDataFile = DedicatedDataFile::from(&manifest_file);
        assert_eq!(dedicated.max_expired_at, 5000);
        // Round-trip through JSON should preserve the value.
        let json = serde_json::to_string(&dedicated).unwrap();
        let decoded: DedicatedDataFile = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.max_expired_at, 5000);
        // Fingerprint match should require max_expired_at equality.
        assert!(dedicated.matches_manifest_file(&manifest_file));
        let mut mismatched = manifest_file.clone();
        mismatched.max_expired_at = 0;
        assert!(!dedicated.matches_manifest_file(&mismatched));
    }

    #[test]
    fn test_dedicated_data_file_backward_compatible_without_max_expired_at() {
        // JSON without max_expired_at (old format) should decode with default 0.
        let json = r#"{
            "file_id": 10,
            "file_type": "sst",
            "path": "data/10.sst",
            "schema_id": 1,
            "size": 100,
            "start_key": "61",
            "end_key": "7a",
            "has_separated_values": false,
            "bucket_range_start": 0,
            "bucket_range_end": 0,
            "effective_bucket_range_start": 0,
            "effective_bucket_range_end": 0,
            "vlog_file_seq_offset": 0
        }"#;
        let decoded: DedicatedDataFile = serde_json::from_str(json).unwrap();
        assert_eq!(decoded.max_expired_at, 0);
        assert_eq!(decoded.file_id, 10);
    }
}
