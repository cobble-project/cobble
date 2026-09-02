//! This module implements a remote compaction worker and server for Cobble.
//! The `RemoteCompactionWorker` sends compaction tasks to a remote server over TCP
//! and the `RemoteCompactionServer` listens for incoming compaction requests
//! executes them using a local `CompactionExecutor`, and returns the results back to the worker.
use super::{CompactionExecutor, CompactionResult, CompactionTask, CompactionWorker};
use crate::cache::{
    BlockCacheKey, BlockCachePreload, ScanHotBlockRegistry, cache_namespace_for_db_id,
};
use crate::data_file::{DataFile, DataFileType};
use crate::db_state::{TruncationCursorId, TruncationCursorMap};
use crate::db_status::DbLifecycle;
use crate::error::{Error, Result};
use crate::file::{FileId, FileManager, TrackedFileId};
use crate::iterator::SortedRun;
use crate::lsm::{LSMTree, LevelEdit, VersionEdit};
use crate::merge_operator::{
    BytesMergeOperator, MergeOperator, MergeOperatorResolver, U32CounterMergeOperator,
    U64CounterMergeOperator, default_merge_operator, merge_operator_by_id,
};
use crate::metrics_manager::MetricsManager;
use crate::parquet::ParquetWriterOptions;
use crate::schema::{Schema, SchemaManager};
use crate::sst::SSTWriterOptions;
use crate::time::ManualTimeProvider;
use crate::ttl::{TTLProvider, TtlConfig};
use crate::util::{build_commit_short_id, build_version_string, init_logging};
use crate::vlog::VlogEdit;
use crate::writer_options::{WriterOptions, WriterOptionsFactory};
use crate::{Config, SstReadMetadataCacheMode};
use bytes::Bytes;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use uuid::Uuid;

const REMOTE_FILE_ID_START: u64 = u64::MAX / 2;
/// Remote-compaction protocol version.
///
/// Version 5 adds an explicit fixed target schema id. A peer that does not
/// understand it could compact with a different schema, so endpoints must be
/// upgraded together.
const REMOTE_COMPACTION_PROTOCOL_VERSION_CURRENT: u32 = 5;
const REMOTE_COMPACTION_PROTOCOL_MIN_COMPATIBLE_VERSION: u32 = 5;
type RemoteCompactionOutput = (Vec<RemoteDataFile>, Vec<(u32, i64)>, Vec<BlockCachePreload>);

/// Checks whether a peer speaking `(peer_version, peer_min_compatible_version)` is compatible
/// with a local endpoint speaking `(local_version, local_min_compatible)`.
///
/// Incompatible when the peer requires something newer than we speak
/// (`peer_min > local_version`) or when we require something newer than the peer speaks
/// (`peer_version < local_min_compatible`). Kept as a free function taking explicit local
/// versions so the protocol-compatibility tests can exercise the full version matrix against
/// arbitrary local/peer pairs.
fn validate_protocol_compatibility(
    role: &str,
    peer_version: u32,
    peer_min_compatible_version: u32,
    local_version: u32,
    local_min_compatible: u32,
) -> Result<()> {
    if peer_min_compatible_version > local_version || peer_version < local_min_compatible {
        return Err(Error::IoError(format!(
            "{} protocol incompatible: peer(version={}, compatible_version={}), local(version={}, compatible_version={})",
            role, peer_version, peer_min_compatible_version, local_version, local_min_compatible
        )));
    }
    Ok(())
}

/// Concurrency limiter for the remote compaction server.
///
/// Limits both the number of concurrently processed requests and the total
/// number of pending (active + queued) requests. When all slots and queue
/// positions are taken, new requests are rejected immediately.
struct RequestLimiter {
    max_concurrent: usize,
    max_total: usize,
    active: Mutex<usize>,
    pending: AtomicUsize,
    slot_available: Condvar,
    shutdown: AtomicBool,
}

impl RequestLimiter {
    fn new(max_concurrent: usize, max_queued: usize) -> Self {
        Self {
            max_concurrent: max_concurrent.max(1),
            max_total: max_concurrent.max(1) + max_queued,
            active: Mutex::new(0),
            pending: AtomicUsize::new(0),
            slot_available: Condvar::new(),
            shutdown: AtomicBool::new(false),
        }
    }

    /// Try to accept a new request. Returns false if the server is overloaded
    /// or shutting down.
    fn try_accept(&self) -> bool {
        if self.shutdown.load(Ordering::Acquire) {
            return false;
        }
        let old = self.pending.fetch_add(1, Ordering::SeqCst);
        if old >= self.max_total {
            self.pending.fetch_sub(1, Ordering::SeqCst);
            false
        } else {
            true
        }
    }

    /// Block until an active processing slot is available. Returns false if
    /// the server shuts down while waiting or the peer disconnects.
    ///
    /// When `stream` is provided, the limiter periodically checks whether the
    /// peer has closed the connection (via `peek`). If the peer disconnected
    /// (e.g. client timeout or Db close), this returns false immediately so
    /// the server does not waste a slot on a dead request.
    fn acquire_slot(&self, stream: Option<&TcpStream>) -> bool {
        let mut active = self.active.lock().unwrap();
        while *active >= self.max_concurrent && !self.shutdown.load(Ordering::Acquire) {
            active = self
                .slot_available
                .wait_timeout(active, Duration::from_millis(200))
                .unwrap()
                .0;
            // Check if the peer disconnected while we were queued.
            if let Some(s) = stream {
                let mut probe = [0u8; 1];
                s.set_nonblocking(true).ok();
                let disconnected = matches!(s.peek(&mut probe), Ok(0));
                s.set_nonblocking(false).ok();
                if disconnected {
                    self.pending.fetch_sub(1, Ordering::SeqCst);
                    return false;
                }
            }
        }
        if self.shutdown.load(Ordering::Acquire) {
            self.pending.fetch_sub(1, Ordering::SeqCst);
            return false;
        }
        *active += 1;
        true
    }

    /// Release an active processing slot.
    fn release_slot(&self) {
        {
            let mut active = self.active.lock().unwrap();
            *active -= 1;
        }
        self.pending.fetch_sub(1, Ordering::SeqCst);
        self.slot_available.notify_one();
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.slot_available.notify_all();
    }
}

/// RAII guard that releases a processing slot when dropped.
struct RequestSlotGuard<'a>(&'a RequestLimiter);

impl Drop for RequestSlotGuard<'_> {
    fn drop(&mut self) {
        self.0.release_slot();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RemoteTtlConfig {
    enabled: bool,
    default_ttl_seconds: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RemoteSstOptions {
    block_size: usize,
    buffer_size: usize,
    num_columns: usize,
    bloom_filter_enabled: bool,
    bloom_bits_per_key: u32,
    partitioned_index: bool,
    #[serde(default)]
    read_metadata_cache_mode: SstReadMetadataCacheMode,
    data_block_restart_interval: usize,
    compression: crate::SstCompressionAlgorithm,
    #[serde(default)]
    block_checksum_enabled: bool,
}

impl RemoteSstOptions {
    fn from_sst_options(options: &SSTWriterOptions) -> Self {
        Self {
            block_size: options.block_size,
            buffer_size: options.buffer_size,
            num_columns: options.num_columns,
            bloom_filter_enabled: options.bloom_filter_enabled,
            bloom_bits_per_key: options.bloom_bits_per_key,
            partitioned_index: options.partitioned_index,
            read_metadata_cache_mode: options.read_metadata_cache_mode,
            data_block_restart_interval: options.data_block_restart_interval,
            compression: options.compression,
            block_checksum_enabled: options.block_checksum_enabled,
        }
    }

    fn into_sst_options(self) -> SSTWriterOptions {
        SSTWriterOptions {
            metrics: None,
            block_size: self.block_size,
            buffer_size: self.buffer_size,
            num_columns: self.num_columns,
            bloom_filter_enabled: self.bloom_filter_enabled,
            bloom_bits_per_key: self.bloom_bits_per_key,
            partitioned_index: self.partitioned_index,
            read_metadata_cache_mode: self.read_metadata_cache_mode,
            data_block_restart_interval: self.data_block_restart_interval,
            compression: self.compression,
            value_has_ttl: true,
            block_checksum_enabled: self.block_checksum_enabled,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RemoteParquetOptions {
    row_group_size_bytes: usize,
    buffer_size: usize,
    num_columns: usize,
}

impl RemoteParquetOptions {
    fn from_parquet_options(options: &ParquetWriterOptions, num_columns: usize) -> Self {
        Self {
            row_group_size_bytes: options.row_group_size_bytes,
            buffer_size: options.buffer_size,
            num_columns,
        }
    }

    fn into_parquet_options(self) -> ParquetWriterOptions {
        ParquetWriterOptions {
            row_group_size_bytes: self.row_group_size_bytes,
            buffer_size: self.buffer_size,
            num_columns: self.num_columns,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "options", rename_all = "snake_case")]
enum RemoteWriterOptions {
    Sst(RemoteSstOptions),
    Parquet(RemoteParquetOptions),
}

impl RemoteWriterOptions {
    fn from_writer_options(options: &WriterOptions, num_columns: usize) -> Self {
        match options {
            WriterOptions::Sst(sst_options) => {
                Self::Sst(RemoteSstOptions::from_sst_options(sst_options))
            }
            WriterOptions::Parquet(parquet_options) => Self::Parquet(
                RemoteParquetOptions::from_parquet_options(parquet_options, num_columns),
            ),
        }
    }

    fn data_file_type(&self) -> DataFileType {
        match self {
            Self::Sst(_) => DataFileType::SSTable,
            Self::Parquet(_) => DataFileType::Parquet,
        }
    }

    fn num_columns(&self) -> usize {
        match self {
            Self::Sst(options) => options.num_columns,
            Self::Parquet(options) => options.num_columns,
        }
    }

    fn into_writer_options(self, metrics_manager: &MetricsManager) -> WriterOptions {
        match self {
            Self::Sst(options) => {
                let mut sst_options = options.into_sst_options();
                sst_options.metrics =
                    Some(metrics_manager.sst_writer_metrics(sst_options.compression));
                WriterOptions::Sst(sst_options)
            }
            Self::Parquet(options) => WriterOptions::Parquet(options.into_parquet_options()),
        }
    }
}

/// A struct representing a data file in the remote compaction protocol.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RemoteDataFile {
    file_id: FileId,
    file_type: String,
    full_path: String,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    schema_id: u64,
    size: usize,
    has_separated_values: bool,
    bucket_range_start: u16,
    bucket_range_end: u16,
    effective_bucket_range_start: u16,
    effective_bucket_range_end: u16,
    vlog_file_seq_offset: u32,
    meta_bytes: Option<Vec<u8>>,
    #[serde(default)]
    max_expired_at: u32,
}

#[derive(Clone, Copy)]
enum RemoteReplicaUse {
    ReadonlyView,
    PendingAdoption,
}

impl RemoteDataFile {
    fn from_data_file(file: &DataFile, full_path: String) -> Self {
        Self {
            file_id: file.file_id,
            file_type: file.file_type.to_string(),
            full_path,
            start_key: file.start_key.clone(),
            end_key: file.end_key.clone(),
            schema_id: file.schema_id,
            size: file.size,
            has_separated_values: file.has_separated_values,
            bucket_range_start: *file.bucket_range.start(),
            bucket_range_end: *file.bucket_range.end(),
            effective_bucket_range_start: *file.effective_bucket_range.start(),
            effective_bucket_range_end: *file.effective_bucket_range.end(),
            vlog_file_seq_offset: file.vlog_file_seq_offset,
            meta_bytes: file.meta_bytes().map(|bytes| bytes.to_vec()),
            max_expired_at: file.max_expired_at(),
        }
    }

    fn from_data_file_with_manager(file: &DataFile, file_manager: &FileManager) -> Result<Self> {
        let full_path = file_manager
            .get_data_file_full_path(file.file_id)
            .ok_or_else(|| {
                Error::IoError(format!(
                    "Missing data file path for file_id={}",
                    file.file_id
                ))
            })?;
        Ok(Self::from_data_file(file, full_path))
    }

    fn into_data_file(
        self,
        file_manager: &Arc<FileManager>,
        file_id: FileId,
        replica_use: RemoteReplicaUse,
    ) -> Result<Arc<DataFile>> {
        let file_type = DataFileType::from_str(&self.file_type).map_err(Error::IoError)?;
        let path = self.full_path;
        match replica_use {
            RemoteReplicaUse::ReadonlyView => {
                file_manager.register_data_file_readonly(file_id, &path)?;
            }
            RemoteReplicaUse::PendingAdoption => {
                file_manager.register_data_file_pending_adoption(file_id, &path)?;
            }
        }
        let data_file = DataFile::new(
            file_type,
            self.start_key,
            self.end_key,
            file_id,
            TrackedFileId::new(file_manager, file_id),
            self.schema_id,
            self.size,
            self.bucket_range_start..=self.bucket_range_end,
            self.effective_bucket_range_start..=self.effective_bucket_range_end,
        )
        .with_vlog_offset(self.vlog_file_seq_offset)
        .with_separated_values(self.has_separated_values);
        data_file.set_max_expired_at(self.max_expired_at);
        file_manager.finalize_data_file(&data_file)?;
        if let Some(bytes) = self.meta_bytes.map(Bytes::from) {
            data_file.set_meta_bytes(bytes);
        }
        Ok(Arc::new(data_file))
    }
}

/// A struct representing a sorted run in the remote compaction protocol.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RemoteSortedRun {
    level: u8,
    files: Vec<RemoteDataFile>,
}

impl RemoteSortedRun {
    fn from_sorted_run(run: &SortedRun, file_manager: &FileManager) -> Result<Self> {
        let files = run
            .files()
            .iter()
            .map(|file| RemoteDataFile::from_data_file_with_manager(file.as_ref(), file_manager))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            level: run.level(),
            files,
        })
    }
}

/// A struct representing the request for a remote compaction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RemoteCompactionRequest {
    version: u32,
    compatible_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<u64>,
    db_id: String,
    lsm_tree_idx: usize,
    topology_epoch: u64,
    tree_scope: crate::db_state::LSMTreeScope,
    column_family_id: u8,
    output_level: u8,
    target_schema_id: u64,
    writer_options: RemoteWriterOptions,
    ttl_config: RemoteTtlConfig,
    ttl_now_seconds: u32,
    runs: Vec<RemoteSortedRun>,
    merge_operator_ids: Vec<String>,
    merge_operator_metadata: Vec<Option<serde_json::Value>>,
    truncation_cursors: Vec<RemoteTruncationCursor>,
    /// Schema definitions the compactor needs to decode input SST files and stamp output.
    ///
    /// Carries exactly the schema versions referenced by the input files (version 0 excluded),
    /// serialized as `SchemaFile`. This lets the compactor reconstruct the writer's schema registry
    /// from the request alone, without reading the shared volume — so remote compaction works even
    /// when triggered by a flush before the first checkpoint persists schemas. Schemas remain
    /// persisted to the volume only at checkpoint time, as before.
    ///
    /// Includes the fixed compaction target plus every input-referenced version needed to decode
    /// the selected files. The server must use `target_schema_id`, not infer a target from these
    /// definitions' highest version.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    schemas: Vec<crate::schema::SchemaFile>,
    /// Snapshot of local scan-hot input block keys.
    ///
    /// The remote server seeds its `ScanHotBlockRegistry` from this list, so its
    /// compaction input iterators can produce the same output preload requests as
    /// local compaction. The client remaps returned file ids before async loading.
    scan_hot_block_keys: Vec<BlockCacheKey>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RemoteTruncationCursor {
    bucket: u16,
    column_family_id: u8,
    key: Vec<u8>,
}

impl RemoteTruncationCursor {
    fn from_map(cursors: &TruncationCursorMap) -> Vec<Self> {
        cursors
            .iter()
            .map(|(id, key)| Self {
                bucket: id.bucket,
                column_family_id: id.column_family_id,
                key: key.clone(),
            })
            .collect()
    }

    fn into_map(cursors: Vec<Self>) -> TruncationCursorMap {
        cursors
            .into_iter()
            .map(|cursor| {
                (
                    TruncationCursorId::new(cursor.bucket, cursor.column_family_id),
                    cursor.key,
                )
            })
            .collect()
    }
}

impl RemoteCompactionRequest {
    fn validate_protocol_compatibility(&self) -> Result<()> {
        validate_protocol_compatibility(
            "request",
            self.version,
            self.compatible_version,
            REMOTE_COMPACTION_PROTOCOL_VERSION_CURRENT,
            REMOTE_COMPACTION_PROTOCOL_MIN_COMPATIBLE_VERSION,
        )
    }
}

impl fmt::Display for RemoteCompactionRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let file_count: usize = self.runs.iter().map(|run| run.files.len()).sum();
        write!(
            f,
            "id={} db_id={} tree_idx={} cf_id={} output_level={} data_file_type={} runs={} files={}",
            self.request_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "unassigned".to_string()),
            self.db_id,
            self.lsm_tree_idx,
            self.column_family_id,
            self.output_level,
            self.writer_options.data_file_type(),
            self.runs.len(),
            file_count
        )
    }
}

/// Classification of a server-side compaction error, sent back to the client so it can decide
/// whether to fall back to local (transient) or surface the failure (permanent) without parsing
/// the error message string.
///
/// `#[serde(default)]` on the response field keeps this backward compatible: an older server that
/// does not send `error_kind` deserializes as `None`, which the client treats as transient (the
/// pre-typing behavior). An older client simply ignores the unknown field.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RemoteCompactionErrorKind {
    /// Recoverable: compactor overload/shutdown, transient I/O, cancelled task. The client may
    /// fall back to local (or skip) and retry remote next time.
    Transient,
    /// Deterministic: malformed carried schema, unsupported/unknown merge operator, bad config,
    /// invalid request shape, protocol mismatch. Falling back would mask a misconfiguration, so
    /// the client must surface it and mark the DB errored.
    Permanent,
}

/// Classifies a server-side `Error` produced while handling a compaction request. Mirrors the
/// client-side `classify_remote_failure`: deterministic config/state/format errors are permanent;
/// I/O, filesystem, and cancellation errors are transient.
fn classify_server_error(err: &Error) -> RemoteCompactionErrorKind {
    match err {
        Error::ConfigError(_)
        | Error::InvalidState(_)
        | Error::FileFormatError(_)
        | Error::InputError(_) => RemoteCompactionErrorKind::Permanent,
        Error::IoError(_)
        | Error::FileSystemError(_)
        | Error::ChecksumMismatch(_)
        | Error::CancelledError(_)
        | Error::MemtableFull { .. }
        | Error::CoordinationError(_)
        | Error::UrlParseError(_) => RemoteCompactionErrorKind::Transient,
    }
}

/// A struct representing the response from a remote compaction request.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RemoteCompactionResponse {
    version: u32,
    compatible_version: u32,
    topology_epoch: u64,
    tree_scope: crate::db_state::LSMTreeScope,
    output_files: Vec<RemoteDataFile>,
    vlog_entry_deltas: Vec<(u32, i64)>,
    /// Output block preload requests produced by the server-side compaction writer.
    ///
    /// These keys use remote output file ids. The client remaps them to locally
    /// reserved file ids before submitting them to the dedicated preload worker.
    preload_block_keys: Vec<BlockCachePreload>,
    error: Option<String>,
    /// When `error` is `Some`, classifies it as transient or permanent so the client does not have
    /// to parse the error string. `#[serde(default)]` for backward compatibility with older
    /// servers (treated as transient by the client).
    #[serde(default)]
    error_kind: Option<RemoteCompactionErrorKind>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
enum RemoteCompactionCommand {
    Execute(Box<RemoteCompactionRequest>),
    SupportedMergeOperators,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
enum RemoteCompactionReply {
    Execute(RemoteCompactionResponse),
    SupportedMergeOperators(Vec<String>),
    Error(String),
}

impl RemoteCompactionResponse {
    fn ok(
        topology_epoch: u64,
        tree_scope: crate::db_state::LSMTreeScope,
        output_files: Vec<RemoteDataFile>,
        vlog_entry_deltas: Vec<(u32, i64)>,
        preload_block_keys: Vec<BlockCachePreload>,
    ) -> Self {
        Self {
            version: REMOTE_COMPACTION_PROTOCOL_VERSION_CURRENT,
            compatible_version: REMOTE_COMPACTION_PROTOCOL_MIN_COMPATIBLE_VERSION,
            topology_epoch,
            tree_scope,
            output_files,
            vlog_entry_deltas,
            preload_block_keys,
            error: None,
            error_kind: None,
        }
    }

    /// Builds an error response. `kind` classifies the error so the client can decide fallback
    /// without parsing the message string.
    fn err(message: impl Into<String>, kind: RemoteCompactionErrorKind) -> Self {
        Self {
            version: REMOTE_COMPACTION_PROTOCOL_VERSION_CURRENT,
            compatible_version: REMOTE_COMPACTION_PROTOCOL_MIN_COMPATIBLE_VERSION,
            topology_epoch: 0,
            tree_scope: crate::db_state::LSMTreeScope::new(0..=0, 0),
            output_files: Vec::new(),
            vlog_entry_deltas: Vec::new(),
            preload_block_keys: Vec::new(),
            error: Some(message.into()),
            error_kind: Some(kind),
        }
    }

    fn validate_protocol_compatibility(&self) -> Result<()> {
        validate_protocol_compatibility(
            "response",
            self.version,
            self.compatible_version,
            REMOTE_COMPACTION_PROTOCOL_VERSION_CURRENT,
            REMOTE_COMPACTION_PROTOCOL_MIN_COMPATIBLE_VERSION,
        )
    }
}

impl fmt::Display for RemoteCompactionResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.error {
            Some(err) => write!(f, "error={}", err),
            None => write!(f, "output_files={}", self.output_files.len()),
        }
    }
}

/// Classification of a remote compaction failure, used by the resilient wrapper to decide whether
/// to fall back to local compaction or give up.
///
/// Transient failures (compactor down, connect refused, timeout, connection reset, I/O) are
/// recoverable: the DB should fall back to local (or skip) and retry remote next time. Permanent
/// failures (protocol incompatible, unsupported merge operator, malformed schema, config error)
/// are deterministic: falling back would silently mask a misconfiguration, so they must surface.
pub(crate) enum RemoteCompactionFailure {
    Transient(Error),
    Permanent(Error),
}

impl RemoteCompactionFailure {
    fn into_error(self) -> Error {
        match self {
            RemoteCompactionFailure::Transient(e) | RemoteCompactionFailure::Permanent(e) => e,
        }
    }
}

/// Classifies an error produced by the remote compaction path.
///
/// `Error::ConfigError` (unsupported merge operator, bad config) and `Error::InvalidState`
/// (malformed carried schema, protocol/shape violations) are permanent. `Error::IoError` is
/// ambiguous — the remote layer wraps both wire I/O (transient) and server-reported/protocol
/// errors as `IoError(String)`. Because the structured permanent checks
/// (`validate_protocol_compatibility`, `ensure_supported_merge_operator_ids`) are performed before
/// the request is sent, an `IoError` reaching this point is overwhelmingly a transient
/// connect/timeout/IO failure, so it is classified transient. This keeps a recoverable compactor
/// outage from aborting the DB; the explicit permanent paths are still caught earlier.
pub(crate) fn classify_remote_failure(err: Error) -> RemoteCompactionFailure {
    match err {
        Error::ConfigError(_) | Error::InvalidState(_) => RemoteCompactionFailure::Permanent(err),
        other => RemoteCompactionFailure::Transient(other),
    }
}

/// Outcome of attempting a single remote compaction request, carrying the transient/permanent
/// classification so the resilient wrapper can decide fallback without re-parsing error strings.
pub(crate) enum RemoteCompactionOutcome {
    Succeeded(CompactionResult),
    Failed(RemoteCompactionFailure),
}

impl RemoteCompactionOutcome {
    pub(crate) fn succeeded(result: CompactionResult) -> Self {
        RemoteCompactionOutcome::Succeeded(result)
    }

    pub(crate) fn failed_transient(err: Error) -> Self {
        RemoteCompactionOutcome::Failed(RemoteCompactionFailure::Transient(err))
    }

    pub(crate) fn failed_permanent(err: Error) -> Self {
        RemoteCompactionOutcome::Failed(RemoteCompactionFailure::Permanent(err))
    }
}

/// A compaction worker that sends compaction tasks to a remote server.
pub(crate) struct RemoteCompactionWorker {
    address: String,
    file_manager: Arc<FileManager>,
    lsm_tree: Weak<LSMTree>,
    config: Config,
    ttl_config: TtlConfig,
    runtime: Mutex<Option<Runtime>>,
    tasks: Arc<super::BlockingTaskTracker>,
    remote_timeout: Duration,
    metrics_manager: Arc<MetricsManager>,
    schema_manager: Arc<SchemaManager>,
    /// Merge operator ids advertised by the remote compactor, fetched lazily on the first
    /// compaction that needs them. `None` means "not yet fetched / invalidated"; a transient
    /// connection failure during fetch is surfaced to the caller (and handled by the resilient
    /// wrapper) rather than failing `Db::open`. A successful fetch is cached so subsequent
    /// compactions skip the capability round-trip.
    supported_merge_operator_ids: Mutex<Option<HashSet<String>>>,
}

impl RemoteCompactionWorker {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        address: String,
        file_manager: Arc<FileManager>,
        lsm_tree: Weak<LSMTree>,
        config: Config,
        ttl_config: TtlConfig,
        remote_timeout: Duration,
        metrics_manager: Arc<MetricsManager>,
        schema_manager: Arc<SchemaManager>,
    ) -> Result<Self> {
        let runtime =
            super::build_compaction_runtime("cobble-remote-compaction", config.compaction_threads)?;
        info!(
            "Cobble remote compactor ({}, Rev:{}) configured for addr: {}.",
            build_version_string(),
            build_commit_short_id(),
            address
        );
        Ok(Self {
            address,
            file_manager,
            lsm_tree,
            config,
            ttl_config,
            runtime: Mutex::new(Some(runtime)),
            tasks: Arc::new(super::BlockingTaskTracker::new()),
            remote_timeout,
            metrics_manager,
            schema_manager,
            supported_merge_operator_ids: Mutex::new(None),
        })
    }

    pub(crate) fn spawn_blocking<F, T>(&self, task: F) -> Option<tokio::task::JoinHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let guard = self.runtime.lock().unwrap();
        self.tasks.spawn(guard.as_ref()?, task)
    }

    pub(crate) fn compaction_metrics(&self) -> Arc<super::CompactionTaskMetrics> {
        self.metrics_manager.compaction_metrics()
    }

    /// Lazily fetches and caches the remote compactor's supported merge operator ids, then checks
    /// that every operator required by `required_ids` is supported.
    ///
    /// The fetch is best-effort: a connection/IO failure (compactor down) is returned as an
    /// `Error::IoError` so the resilient wrapper can treat it as transient and fall back. A
    /// successful fetch is cached in `supported_merge_operator_ids` so subsequent compactions skip
    /// the round-trip. An unsupported operator is a permanent `Error::ConfigError`.
    fn ensure_supported_merge_operator_ids(&self, required_ids: &[String]) -> Result<()> {
        if required_ids.is_empty() {
            return Ok(());
        }
        let mut cache = self.supported_merge_operator_ids.lock().unwrap();
        if cache.is_none() {
            let ids = fetch_supported_merge_operator_ids(&self.address, self.remote_timeout)?
                .into_iter()
                .collect::<HashSet<_>>();
            *cache = Some(ids);
        }
        let supported = cache.as_ref().unwrap();
        for merge_operator_id in required_ids {
            if !supported.contains(merge_operator_id) {
                return Err(Error::ConfigError(format!(
                    "remote compactor {} does not support merge operator '{}'",
                    self.address, merge_operator_id
                )));
            }
        }
        Ok(())
    }

    /// Invalidates the cached capability so the next compaction re-fetches it. Called when a
    /// permanent protocol/capability mismatch is observed, in case the compactor was upgraded or
    /// replaced and its advertised merge operators changed.
    pub(crate) fn invalidate_supported_merge_operator_ids(&self) {
        *self.supported_merge_operator_ids.lock().unwrap() = None;
    }

    /// The remote compactor address this worker is configured for.
    pub(crate) fn address(&self) -> &str {
        &self.address
    }

    pub(crate) fn file_manager(&self) -> &Arc<FileManager> {
        &self.file_manager
    }

    pub(crate) fn lsm_tree(&self) -> &Weak<LSMTree> {
        &self.lsm_tree
    }

    pub(crate) fn remote_timeout(&self) -> Duration {
        self.remote_timeout
    }

    pub(crate) fn build_request(
        &self,
        lsm_tree_idx: usize,
        sorted_runs: &[SortedRun],
        output_level: u8,
        target_schema_id: u64,
        data_file_type: DataFileType,
        ttl_provider: Arc<TTLProvider>,
    ) -> Result<RemoteCompactionRequest> {
        let lsm_tree = self.lsm_tree.upgrade().ok_or_else(|| {
            Error::IoError("lsm tree dropped during remote compaction".to_string())
        })?;
        let state = lsm_tree.db_state().load();
        let truncation_cursors = state.truncation_cursors_snapshot();
        let tree_scope = state
            .multi_lsm_version
            .tree_scope_of_tree(lsm_tree_idx)
            .ok_or_else(|| {
                Error::InvalidState(format!(
                    "missing tree scope for remote compaction tree {}",
                    lsm_tree_idx
                ))
            })?;
        let runs = sorted_runs
            .iter()
            .map(|run| RemoteSortedRun::from_sorted_run(run, &self.file_manager))
            .collect::<Result<Vec<_>>>()?;
        let schema = self.schema_manager.schema(target_schema_id)?;
        let num_columns = schema
            .num_columns_in_family(tree_scope.column_family_id)
            .unwrap_or_else(|| schema.num_columns());
        let writer_options =
            super::build_writer_options(&self.config, output_level, data_file_type, num_columns)?;
        let scan_hot_block_keys = if lsm_tree.block_cache().is_some() {
            lsm_tree.scan_hot_blocks().snapshot_keys()
        } else {
            Vec::new()
        };
        let merge_operator_ids =
            schema.operator_ids_for_column_family_id(tree_scope.column_family_id);
        // Lazily fetch (and cache) the compactor's advertised merge operator ids. Fetching here —
        // on the first compaction that needs it, rather than in `new()` — keeps `Db::open` from
        // failing when the compactor is down. A fetch failure is returned as-is so the resilient
        // wrapper can classify and handle it (fallback/skip). Once fetched the result is cached.
        self.ensure_supported_merge_operator_ids(&merge_operator_ids)?;
        let merge_operator_metadata = schema
            .column_metadata_for_column_family_id(tree_scope.column_family_id)
            .to_vec();
        // Collect the fixed target and every schema version referenced by an input file. The
        // compactor registers these from the request so it can decode input SST files stamped
        // with non-zero schema ids and materialize output using `target_schema_id`. Version 0 is
        // excluded: it has no real column-family layout (the writer starts from a default schema
        // and evolves from there), and the compactor already reconstructs a version-0 fallback
        // from the request's resolved merge operators. Schemas are carried in the request itself
        // rather than read back from the shared volume, so remote compaction works regardless of
        // when the writer last persisted schemas to disk.
        //
        // The target is carried even when no selected input uses it, preventing an asynchronous
        // remote task from silently changing schema when the writer advances after planning.
        let mut schema_ids: BTreeSet<u64> = BTreeSet::new();
        if target_schema_id > 0 {
            schema_ids.insert(target_schema_id);
        }
        for run in sorted_runs {
            for file in run.files() {
                if file.schema_id > 0 {
                    schema_ids.insert(file.schema_id);
                }
            }
        }
        let schemas = schema_ids
            .into_iter()
            .map(|schema_id| {
                let schema = self.schema_manager.schema(schema_id)?;
                Ok(crate::schema::schema_to_file(schema.as_ref()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(RemoteCompactionRequest {
            version: REMOTE_COMPACTION_PROTOCOL_VERSION_CURRENT,
            compatible_version: REMOTE_COMPACTION_PROTOCOL_MIN_COMPATIBLE_VERSION,
            request_id: None,
            db_id: self.metrics_manager.db_id().to_string(),
            lsm_tree_idx,
            topology_epoch: state.topology_epoch,
            tree_scope: tree_scope.clone(),
            column_family_id: tree_scope.column_family_id,
            output_level,
            target_schema_id,
            writer_options: RemoteWriterOptions::from_writer_options(&writer_options, num_columns),
            ttl_config: RemoteTtlConfig {
                enabled: self.ttl_config.enabled,
                default_ttl_seconds: self.ttl_config.default_ttl_seconds,
            },
            ttl_now_seconds: ttl_provider.now_seconds(),
            runs,
            merge_operator_ids,
            merge_operator_metadata,
            truncation_cursors: RemoteTruncationCursor::from_map(&truncation_cursors),
            scan_hot_block_keys,
            schemas,
        })
    }
}

impl CompactionWorker for RemoteCompactionWorker {
    fn submit_runs(
        &self,
        lsm_tree_idx: usize,
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
        target_schema_id: u64,
        data_file_type: DataFileType,
        ttl_provider: Arc<TTLProvider>,
    ) -> Option<tokio::task::JoinHandle<Result<CompactionResult>>> {
        if sorted_runs.is_empty() {
            return None;
        }
        let request = match self.build_request(
            lsm_tree_idx,
            &sorted_runs,
            output_level,
            target_schema_id,
            data_file_type,
            ttl_provider,
        ) {
            Ok(request) => request,
            Err(err) => {
                let lsm_tree = self.lsm_tree.clone();
                return self.spawn_blocking(move || {
                    if let Some(lsm_tree) = lsm_tree.upgrade() {
                        let _ = lsm_tree.on_compaction_complete(lsm_tree_idx);
                    }
                    Err(err)
                });
            }
        };
        // Execute via the shared "execute request and apply" core. On success it releases the
        // pending slot and applies the edit; on failure it returns the (classified) error without
        // touching pending, and this direct path releases pending here. The resilient wrapper uses
        // the same core but takes over pending handling to implement fallback/skip.
        let worker_lsm_tree = self.lsm_tree.clone();
        let file_manager = Arc::clone(&self.file_manager);
        let address = self.address.clone();
        let remote_timeout = self.remote_timeout;
        let compaction_metrics = self.metrics_manager.compaction_metrics();
        self.spawn_blocking(move || {
            let result = (|| -> Result<CompactionResult> {
                let lsm_tree = worker_lsm_tree.upgrade().ok_or_else(|| {
                    Error::IoError("lsm tree dropped during compaction".to_string())
                })?;
                match execute_compaction_request(
                    &address,
                    request,
                    &sorted_runs,
                    lsm_tree_idx,
                    &file_manager,
                    &lsm_tree,
                    remote_timeout,
                    &compaction_metrics,
                ) {
                    RemoteCompactionOutcome::Succeeded(result) => Ok(result),
                    RemoteCompactionOutcome::Failed(failure) => Err(failure.into_error()),
                }
            })();
            if result.is_err()
                && let Some(lsm_tree) = worker_lsm_tree.upgrade()
            {
                let _ = lsm_tree.on_compaction_complete(lsm_tree_idx);
            }
            result
        })
    }

    fn shutdown(&self) {
        info!(
            "cobble=remote compaction worker shutdown version={} build_commit={}",
            build_version_string(),
            build_commit_short_id()
        );
        self.tasks.close_and_wait();
        if let Some(runtime) = self.runtime.lock().unwrap().take() {
            drop(runtime);
        }
    }
}

/// A server that listens for remote compaction requests and executes them.
pub struct RemoteCompactionServer {
    config: Config,
    runtime: Arc<Runtime>,
    executor: Arc<CompactionExecutor>,
    request_id: Arc<AtomicU64>,
    metrics_manager: Arc<MetricsManager>,
    merge_operator_map: Arc<Mutex<HashMap<String, Arc<dyn MergeOperator>>>>,
    merge_operator_resolver: Arc<Mutex<Option<Arc<dyn MergeOperatorResolver>>>>,
    resolvable_operator_ids: Arc<Mutex<HashSet<String>>>,
    limiter: Arc<RequestLimiter>,
    shutdown: Arc<AtomicBool>,
}

impl RemoteCompactionServer {
    pub fn new(config: Config) -> Result<Self> {
        let compaction_config = super::build_compaction_config(&config, 1)?;
        let runtime = Arc::new(super::build_compaction_runtime(
            "cobble-compaction",
            compaction_config.max_threads,
        )?);
        let executor = CompactionExecutor::new_with_runtime(
            compaction_config,
            runtime.clone(),
            Arc::new(DbLifecycle::new_open()),
        )?;
        let metrics_manager = Arc::new(MetricsManager::new(Uuid::new_v4().to_string()));
        let mut merge_operator_map: HashMap<String, Arc<dyn MergeOperator>> = HashMap::new();
        for operator in [
            Arc::new(BytesMergeOperator) as Arc<dyn MergeOperator>,
            Arc::new(U32CounterMergeOperator) as Arc<dyn MergeOperator>,
            Arc::new(U64CounterMergeOperator) as Arc<dyn MergeOperator>,
        ] {
            merge_operator_map.insert(operator.id(), operator);
        }
        let limiter = Arc::new(RequestLimiter::new(
            config.compaction_server_max_concurrent,
            config.compaction_server_max_queued,
        ));
        let shutdown = Arc::new(AtomicBool::new(false));
        Ok(Self {
            config,
            runtime,
            executor: Arc::new(executor),
            request_id: Arc::new(AtomicU64::new(1)),
            metrics_manager,
            merge_operator_map: Arc::new(Mutex::new(merge_operator_map)),
            merge_operator_resolver: Arc::new(Mutex::new(None)),
            resolvable_operator_ids: Arc::new(Mutex::new(HashSet::new())),
            limiter,
            shutdown,
        })
    }

    pub fn register_merge_operator(&self, operator: Arc<dyn MergeOperator>) {
        self.merge_operator_map
            .lock()
            .unwrap()
            .insert(operator.id(), operator);
    }

    pub fn set_merge_operator_resolver(
        &self,
        resolver: Arc<dyn MergeOperatorResolver>,
        resolvable_ids: Vec<String>,
    ) {
        *self.merge_operator_resolver.lock().unwrap() = Some(resolver);
        let mut ids = self.resolvable_operator_ids.lock().unwrap();
        for id in resolvable_ids {
            ids.insert(id);
        }
    }

    pub fn supported_merge_operator_ids(&self) -> Vec<String> {
        let mut ids: HashSet<String> = self
            .merge_operator_map
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        ids.extend(self.resolvable_operator_ids.lock().unwrap().iter().cloned());
        let mut sorted: Vec<String> = ids.into_iter().collect();
        sorted.sort();
        sorted
    }

    pub fn serve(&self, address: &str) -> Result<()> {
        init_logging(&self.config);
        let listener = TcpListener::bind(address).map_err(|e| Error::IoError(e.to_string()))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| Error::IoError(e.to_string()))?;
        info!(
            "cobble=remote compaction server start version={} build_commit={} addr={} max_concurrent={} max_queued={}",
            build_version_string(),
            build_commit_short_id(),
            listener.local_addr().unwrap(),
            self.limiter.max_concurrent,
            self.limiter.max_total - self.limiter.max_concurrent,
        );
        while !self.shutdown.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false).ok();
                    if let Err(err) = self.handle_connection(stream) {
                        warn!("Handle connection error: {}", err);
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(err) => {
                    warn!("Accept connection error: {}", err);
                }
            }
        }
        info!("cobble=remote compaction server stopped");
        Ok(())
    }

    pub fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        if !self.limiter.try_accept() {
            let _ = write_message(
                &mut stream,
                &RemoteCompactionReply::Error("server overloaded, queue full".into()),
            );
            return Ok(());
        }
        let config = self.config.clone();
        let executor = Arc::clone(&self.executor);
        let request_id_counter = Arc::clone(&self.request_id);
        let metrics_manager = Arc::clone(&self.metrics_manager);
        let merge_operator_map = Arc::clone(&self.merge_operator_map);
        let merge_operator_resolver = self.merge_operator_resolver.lock().unwrap().clone();
        let resolvable_ids = self.resolvable_operator_ids.lock().unwrap().clone();
        let limiter = Arc::clone(&self.limiter);
        self.runtime.spawn_blocking(move || {
            if !limiter.acquire_slot(Some(&stream)) {
                let _ = write_message(
                    &mut stream,
                    &RemoteCompactionReply::Error("server shutting down".into()),
                );
                return;
            }
            let _guard = RequestSlotGuard(&limiter);
            let command: Result<RemoteCompactionCommand> = read_message(&mut stream);
            if let Err(err) = &command {
                warn!("Read request error: {}", err);
                let response = RemoteCompactionReply::Error(format!("Invalid request: {}", err));
                let _ = write_message(&mut stream, &response);
                return;
            }
            let response = match command.unwrap() {
                RemoteCompactionCommand::SupportedMergeOperators => {
                    let mut ids: HashSet<String> =
                        merge_operator_map.lock().unwrap().keys().cloned().collect();
                    ids.extend(resolvable_ids.iter().cloned());
                    let mut sorted: Vec<String> = ids.into_iter().collect();
                    sorted.sort();
                    RemoteCompactionReply::SupportedMergeOperators(sorted)
                }
                RemoteCompactionCommand::Execute(mut request) => {
                    if let Err(err) = request.validate_protocol_compatibility() {
                        warn!("Reject incompatible request: {}", err);
                        // Protocol mismatch is deterministic — a version skew that will not fix
                        // itself on retry — so classify it permanent to prevent the client from
                        // masking it with a local fallback.
                        let response = RemoteCompactionResponse::err(
                            err.to_string(),
                            RemoteCompactionErrorKind::Permanent,
                        );
                        let _ =
                            write_message(&mut stream, &RemoteCompactionReply::Execute(response));
                        return;
                    }
                    let request_id = request_id_counter.fetch_add(1, Ordering::SeqCst);
                    request.request_id = Some(request_id);
                    let topology_epoch = request.topology_epoch;
                    let tree_scope = request.tree_scope.clone();
                    info!("Received request: {}", request);
                    let response = match Self::handle_request_with(
                        &config,
                        executor.as_ref(),
                        Arc::clone(&metrics_manager),
                        Arc::clone(&merge_operator_map),
                        merge_operator_resolver.clone(),
                        *request,
                    ) {
                        Ok((files, vlog_entry_deltas, preload_block_keys)) => {
                            RemoteCompactionResponse::ok(
                                topology_epoch,
                                tree_scope,
                                files,
                                vlog_entry_deltas,
                                preload_block_keys,
                            )
                        }
                        // Classify the server-side error so the client can distinguish a
                        // recoverable execution failure (transient) from a deterministic
                        // schema/config/format error (permanent) without parsing the message.
                        Err(err) => {
                            let kind = classify_server_error(&err);
                            RemoteCompactionResponse::err(err.to_string(), kind)
                        }
                    };
                    info!("Request={} complete with response={}", request_id, response);
                    RemoteCompactionReply::Execute(response)
                }
            };
            let _ = write_message(&mut stream, &response);
        });
        Ok(())
    }

    /// Shut down the server, stopping the accept loop and cancelling in-flight
    /// requests. Clients connected to this server will observe connection
    /// failures.
    pub fn close(&self) {
        info!("cobble=remote compaction server closing");
        self.shutdown.store(true, Ordering::Release);
        self.limiter.shutdown();
    }

    fn handle_request_with(
        config: &Config,
        executor: &CompactionExecutor,
        metrics_manager: Arc<MetricsManager>,
        merge_operator_map: Arc<Mutex<HashMap<String, Arc<dyn MergeOperator>>>>,
        merge_operator_resolver: Option<Arc<dyn MergeOperatorResolver>>,
        request: RemoteCompactionRequest,
    ) -> Result<RemoteCompactionOutput> {
        let file_manager = Self::file_manager_for_with(config, &request.db_id, &metrics_manager)?;
        let data_file_type = request.writer_options.data_file_type();
        let num_columns = request.writer_options.num_columns();
        let column_family_id = request.column_family_id;
        let merge_operator_metadata = request.merge_operator_metadata.clone();
        let writer_options = request.writer_options.into_writer_options(&metrics_manager);
        let file_builder_factory = super::make_data_file_builder_factory(writer_options.clone());
        let writer_options_factory = WriterOptionsFactory::from(&writer_options);
        let sorted_runs = request
            .runs
            .into_iter()
            .map(|run| {
                let files = run
                    .files
                    .into_iter()
                    .map(|file| {
                        let file_id = file.file_id;
                        file.into_data_file(&file_manager, file_id, RemoteReplicaUse::ReadonlyView)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(SortedRun::new(run.level, files))
            })
            .collect::<Result<Vec<_>>>()?;
        let ttl_provider = Arc::new(TTLProvider::new(
            &TtlConfig {
                enabled: request.ttl_config.enabled,
                default_ttl_seconds: request.ttl_config.default_ttl_seconds,
            },
            Arc::new(ManualTimeProvider::new(request.ttl_now_seconds)),
        ));
        let compaction_metrics = metrics_manager.compaction_metrics();
        let sst_metrics = metrics_manager.sst_iterator_metrics();
        let schema_resolver = merge_operator_resolver.clone();
        let request_schemas = request.schemas.clone();
        let merge_operators = Self::resolve_merge_operators(
            Arc::clone(&merge_operator_map),
            merge_operator_resolver,
            &request.merge_operator_ids,
            &request.merge_operator_metadata,
            num_columns,
        )?;
        let schema_manager = Self::build_schema_manager(
            column_family_id,
            &merge_operator_metadata,
            merge_operators,
            schema_resolver.as_ref(),
            num_columns,
            &request_schemas,
        )?;
        let cache_namespace = cache_namespace_for_db_id(&request.db_id);
        let scan_hot_blocks = ScanHotBlockRegistry::from_keys(request.scan_hot_block_keys);
        let truncation_cursors = RemoteTruncationCursor::into_map(request.truncation_cursors);
        let task = CompactionTask::new(
            compaction_metrics,
            sst_metrics,
            request.lsm_tree_idx,
            sorted_runs,
            request.output_level,
            Arc::clone(&file_manager),
            Arc::clone(&file_builder_factory),
            data_file_type,
            ttl_provider,
            schema_manager,
        )
        .with_writer_options_factory(writer_options_factory)
        .with_target_schema_id(request.target_schema_id)
        .with_column_family(request.column_family_id, num_columns)
        .with_truncation_cursors(truncation_cursors)
        .with_scan_hot_blocks(cache_namespace, scan_hot_blocks)
        .with_readonly_outputs();
        let result = executor.execute_blocking(task, None);
        if let Err(e) = &result {
            warn!("Execution error: {}", e);
        }
        let result = result?;
        let output_files = result
            .new_files()
            .iter()
            .map(|file| RemoteDataFile::from_data_file_with_manager(file, &file_manager))
            .collect::<Result<Vec<_>>>()?;
        let vlog_entry_deltas = result
            .vlog_edit()
            .map(|edit| edit.entry_deltas())
            .unwrap_or_default();
        Ok((
            output_files,
            vlog_entry_deltas,
            result.preload_block_keys().to_vec(),
        ))
    }

    fn resolve_merge_operators(
        merge_operator_map: Arc<Mutex<HashMap<String, Arc<dyn MergeOperator>>>>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
        request_ids: &[String],
        request_metadata: &[Option<serde_json::Value>],
        num_columns: usize,
    ) -> Result<Vec<Arc<dyn MergeOperator>>> {
        let ids: Vec<String> = if request_ids.is_empty() {
            vec![default_merge_operator().id(); num_columns]
        } else if request_ids.len() >= num_columns {
            request_ids[..num_columns].to_vec()
        } else {
            let mut ids = request_ids.to_vec();
            ids.resize(num_columns, default_merge_operator().id());
            ids
        };
        let map = merge_operator_map.lock().unwrap();
        let mut operators = Vec::with_capacity(num_columns);
        for (idx, id) in ids.iter().enumerate() {
            let metadata = request_metadata.get(idx).and_then(|m| m.as_ref());
            // Try resolver with metadata first, then fall back to the static map
            if let Some(ref resolver) = resolver
                && let Some(op) = resolver.resolve(id, metadata)
            {
                operators.push(op);
                continue;
            }
            if let Some(op) = map.get(id.as_str()).cloned() {
                operators.push(op);
                continue;
            }
            // Final fallback: merge_operator_by_id (built-in operators)
            operators.push(merge_operator_by_id(id, metadata, None)?);
        }
        Ok(operators)
    }

    fn file_manager_for_with(
        process_config: &Config,
        db_id: &str,
        metrics_manager: &Arc<MetricsManager>,
    ) -> Result<Arc<FileManager>> {
        let bootstrap_file_manager =
            FileManager::from_config(process_config, db_id, Arc::clone(metrics_manager))?;
        let config = crate::properties::load_compactor_config(
            &bootstrap_file_manager,
            db_id,
            process_config,
        )?;
        let file_manager = FileManager::from_config(&config, db_id, Arc::clone(metrics_manager))?;
        file_manager.set_next_file_id(REMOTE_FILE_ID_START);
        Ok(Arc::new(file_manager))
    }

    /// Builds the schema manager for a compaction request.
    ///
    /// The writer carries the schema versions referenced by the input files inside the request
    /// itself (`schemas`), so the compactor can decode input SST files stamped with non-zero
    /// schema ids — without reading the shared volume. This keeps schemas persisted to the volume
    /// only at checkpoint time (as before) and makes remote compaction independent of the writer's
    /// volume directory layout.
    ///
    /// Only input-referenced versions are registered, so the compaction target (`latest_schema`)
    /// is the highest version actually present in the input. Output is stamped with that version,
    /// which avoids spurious schema evolution: when every input file shares the writer's latest
    /// version the target equals it and no evolution runs; when input files predate the latest
    /// version the target stays at the input version and evolution is not attempted.
    ///
    /// A version-0 fallback schema (carrying the request's resolved merge operators) is always
    /// registered first so that the existing version-0 protocol path keeps working when no schemas
    /// are carried (e.g. an older writer). Each carried schema is then registered; a schema that
    /// fails to decode fails the request immediately rather than silently falling back, since a
    /// missing schema would only surface later inside the compaction executor as an opaque
    /// "Missing schema version N" error.
    fn build_schema_manager(
        column_family_id: u8,
        merge_operator_metadata: &[Option<serde_json::Value>],
        merge_operators: Vec<Arc<dyn MergeOperator>>,
        merge_operator_resolver: Option<&Arc<dyn MergeOperatorResolver>>,
        num_columns: usize,
        schemas: &[crate::schema::SchemaFile],
    ) -> Result<Arc<SchemaManager>> {
        let schema_manager = Arc::new(SchemaManager::from_schemas(
            vec![Schema::new_for_column_family(
                0,
                column_family_id,
                merge_operators,
                merge_operator_metadata.to_vec(),
                crate::schema::ColumnFamilyOptions::default(),
            )],
            num_columns,
            None,
        ));
        for schema_file in schemas {
            if schema_file.id == 0 {
                // Version 0 is already registered as the fallback above.
                continue;
            }
            schema_manager
                .register_schema_from_def(schema_file, merge_operator_resolver)
                .map_err(|err| {
                    Error::InvalidState(format!(
                        "remote compactor failed to register carried schema version {}: {}",
                        schema_file.id, err
                    ))
                })?;
        }
        Ok(schema_manager)
    }
}

fn build_version_edit(
    sorted_runs: &[SortedRun],
    output_level: u8,
    output_files: Vec<Arc<DataFile>>,
) -> VersionEdit {
    let mut level_edits: BTreeMap<u8, LevelEdit> = BTreeMap::new();
    for run in sorted_runs {
        let entry = level_edits.entry(run.level()).or_insert_with(|| LevelEdit {
            level: run.level(),
            removed_files: Vec::new(),
            new_files: Vec::new(),
        });
        entry.removed_files.extend(run.files().iter().cloned());
    }
    let entry = level_edits
        .entry(output_level)
        .or_insert_with(|| LevelEdit {
            level: output_level,
            removed_files: Vec::new(),
            new_files: Vec::new(),
        });
    entry.new_files = output_files;
    VersionEdit {
        level_edits: level_edits.into_values().collect(),
    }
}

fn remap_preload_file_ids(
    mut preloads: Vec<BlockCachePreload>,
    remote_to_local_file_ids: &HashMap<FileId, FileId>,
) -> Vec<BlockCachePreload> {
    for preload in &mut preloads {
        if let Some(local_id) = remote_to_local_file_ids.get(&preload.key.file_id) {
            preload.key.file_id = *local_id;
        }
    }
    preloads
}

fn read_message<T: for<'de> Deserialize<'de>>(stream: &mut TcpStream) -> Result<T> {
    let mut len_bytes = [0u8; 4];
    stream
        .read_exact(&mut len_bytes)
        .map_err(|e| Error::IoError(e.to_string()))?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .map_err(|e| Error::IoError(e.to_string()))?;
    serde_json::from_slice(&buf).map_err(|e| Error::IoError(e.to_string()))
}

fn write_message<T: Serialize>(stream: &mut TcpStream, message: &T) -> Result<()> {
    let payload = serde_json::to_vec(message).map_err(|e| Error::IoError(e.to_string()))?;
    let len = payload.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .map_err(|e| Error::IoError(e.to_string()))?;
    stream
        .write_all(&payload)
        .map_err(|e| Error::IoError(e.to_string()))?;
    Ok(())
}

fn send_command_to(
    address: &str,
    command: RemoteCompactionCommand,
    timeout: Duration,
) -> Result<RemoteCompactionReply> {
    let start = Instant::now();
    let addr: SocketAddr = address
        .parse()
        .map_err(|e: std::net::AddrParseError| Error::IoError(e.to_string()))?;
    let mut stream =
        TcpStream::connect_timeout(&addr, timeout).map_err(|e| Error::IoError(e.to_string()))?;
    let remaining = timeout.checked_sub(start.elapsed()).ok_or_else(|| {
        Error::IoError("remote compaction request timed out during connect".to_string())
    })?;
    stream
        .set_read_timeout(Some(remaining))
        .map_err(|e| Error::IoError(e.to_string()))?;
    stream
        .set_write_timeout(Some(remaining))
        .map_err(|e| Error::IoError(e.to_string()))?;
    write_message(&mut stream, &command)?;
    read_message(&mut stream)
}

fn fetch_supported_merge_operator_ids(address: &str, timeout: Duration) -> Result<Vec<String>> {
    match send_command_to(
        address,
        RemoteCompactionCommand::SupportedMergeOperators,
        timeout,
    )? {
        RemoteCompactionReply::SupportedMergeOperators(ids) => Ok(ids),
        RemoteCompactionReply::Error(error) => Err(Error::IoError(error)),
        RemoteCompactionReply::Execute(response) => Err(Error::IoError(format!(
            "unexpected execute response while requesting capabilities: {}",
            response
        ))),
    }
}

fn send_compaction_request_to(
    address: &str,
    request: RemoteCompactionRequest,
    timeout: Duration,
) -> Result<RemoteCompactionResponse> {
    match send_command_to(
        address,
        RemoteCompactionCommand::Execute(Box::new(request)),
        timeout,
    )? {
        RemoteCompactionReply::Execute(response) => {
            response.validate_protocol_compatibility()?;
            Ok(response)
        }
        RemoteCompactionReply::Error(error) => Err(Error::IoError(error)),
        RemoteCompactionReply::SupportedMergeOperators(ids) => Err(Error::IoError(format!(
            "unexpected capability response while executing compaction: {:?}",
            ids
        ))),
    }
}

/// Sends a remote compaction request, remaps the response, and applies the resulting version/vlog/
/// preload edits to the LSM tree.
///
/// This is the reusable "execute request and apply" core shared by the direct `RemoteCompactionWorker`
/// path and the resilient wrapper. It owns the success path's `on_compaction_complete` +
/// `apply_edit` (so the pending slot is released exactly once on success), but does **not** touch
/// pending on error — the caller releases pending on failure. This separation lets the wrapper try
/// remote and then, on a transient failure, hand the compaction to the local worker without
/// double-releasing pending.
///
/// The outcome carries a transient/permanent classification:
/// - protocol incompatibility (`validate_protocol_compatibility`) is **permanent** — surfaced as
///   `Error::InvalidState` so a version mismatch is never masked by a local fallback;
/// - connect/timeout/I/O failures and server-reported errors are **transient**.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_compaction_request(
    address: &str,
    request: RemoteCompactionRequest,
    sorted_runs: &[SortedRun],
    lsm_tree_idx: usize,
    file_manager: &Arc<FileManager>,
    lsm_tree: &Arc<LSMTree>,
    remote_timeout: Duration,
    compaction_metrics: &super::CompactionTaskMetrics,
) -> RemoteCompactionOutcome {
    let output_level = request.output_level;
    let topology_epoch = request.topology_epoch;
    let tree_scope = request.tree_scope.clone();
    let response = match send_compaction_request_to(address, request, remote_timeout) {
        Ok(response) => response,
        Err(Error::IoError(msg)) if msg.contains("protocol incompatible") => {
            // validate_protocol_compatibility wraps the mismatch in IoError; re-classify as a
            // permanent InvalidState so the resilient wrapper does not mask a version mismatch.
            return RemoteCompactionOutcome::failed_permanent(Error::InvalidState(msg));
        }
        Err(err) => return RemoteCompactionOutcome::failed_transient(err),
    };
    if let Some(error) = response.error {
        // The server classifies its errors via `error_kind` so we do not have to parse the message.
        // Permanent errors (malformed schema, unsupported operator, bad config, protocol mismatch)
        // must surface — the resilient wrapper marks the DB errored and never falls back to local,
        // which would silently mask a deterministic misconfiguration. Transient errors (overload,
        // I/O, cancellation) fall back per the configured failure mode. An older server that omits
        // `error_kind` deserializes as `None` and is treated as transient (the pre-typing behavior).
        return match response.error_kind {
            Some(RemoteCompactionErrorKind::Permanent) => {
                RemoteCompactionOutcome::failed_permanent(Error::InvalidState(error))
            }
            _ => RemoteCompactionOutcome::failed_transient(Error::IoError(error)),
        };
    }
    if response.topology_epoch != topology_epoch || response.tree_scope != tree_scope {
        cleanup_unregistered_remote_outputs(file_manager, &response.output_files);
        return RemoteCompactionOutcome::failed_permanent(Error::InvalidState(
            "remote compaction response topology token does not match request".to_string(),
        ));
    }
    let current = lsm_tree.db_state().load();
    if current.topology_epoch != topology_epoch
        || current
            .multi_lsm_version
            .tree_scope_of_tree(lsm_tree_idx)
            .as_ref()
            != Some(&tree_scope)
    {
        cleanup_unregistered_remote_outputs(file_manager, &response.output_files);
        return RemoteCompactionOutcome::failed_transient(Error::CancelledError(
            "remote compaction response became stale before apply".to_string(),
        ));
    }
    let result = (|| -> Result<CompactionResult> {
        let input_bytes = sorted_runs
            .iter()
            .flat_map(|run| run.files().iter())
            .fold(0u64, |total, file| total.saturating_add(file.size as u64));
        let remote_output_paths = response
            .output_files
            .iter()
            .map(|file| file.full_path.clone())
            .collect::<Vec<_>>();
        let output_ids = file_manager.reserve_data_file_ids(response.output_files.len());
        let mut pending_outputs = PendingRemoteOutputCleanup::new(
            Arc::clone(file_manager),
            remote_output_paths,
            output_ids.clone(),
        );
        let remote_to_local_file_ids = response
            .output_files
            .iter()
            .zip(output_ids.iter().copied())
            .map(|(file, local_id)| (file.file_id, local_id))
            .collect::<HashMap<_, _>>();
        let preload_block_keys =
            remap_preload_file_ids(response.preload_block_keys, &remote_to_local_file_ids);
        let output_files = response
            .output_files
            .into_iter()
            .zip(output_ids)
            .map(|(file, file_id)| {
                file.into_data_file(file_manager, file_id, RemoteReplicaUse::PendingAdoption)
            })
            .collect::<Result<Vec<_>>>()?;
        let output_bytes = output_files
            .iter()
            .fold(0u64, |total, file| total.saturating_add(file.size as u64));
        if let Err(err) = file_manager.trigger_offload_if_needed() {
            warn!("remote compaction check-in offload trigger failed: {}", err);
        }
        let edit = build_version_edit(sorted_runs, output_level, output_files.clone());
        let vlog_edit = {
            let edit = VlogEdit::from_entry_deltas(response.vlog_entry_deltas);
            (!edit.is_empty()).then_some(edit)
        };
        // Ownership promotion may fail, so complete it before the LSM can reference these files.
        for file in &output_files {
            file_manager.adopt_data_file(file.file_id)?;
        }
        // Success path: release the pending slot and apply the edit exactly once.
        if lsm_tree
            .apply_compaction_result(lsm_tree_idx, edit.clone(), vlog_edit.clone())
            .is_some()
        {
            pending_outputs.disarm();
            lsm_tree
                .submit_block_cache_preload(Arc::clone(file_manager), preload_block_keys.clone());
            compaction_metrics.record_read_bytes(input_bytes);
            compaction_metrics.record_write_bytes(output_bytes);
            compaction_metrics.record_completed();
        } else {
            return Err(Error::CancelledError(
                "remote compaction response became stale during apply".to_string(),
            ));
        }
        Ok(CompactionResult::new(
            lsm_tree_idx,
            output_files,
            edit,
            vlog_edit,
            preload_block_keys,
        ))
    })();
    match result {
        Ok(result) => RemoteCompactionOutcome::succeeded(result),
        // Failures while remapping/applying the (already-validated) response are transient I/O
        // issues. The pending slot has not been released on this path, so the caller still owns it.
        Err(err) => RemoteCompactionOutcome::failed_transient(err),
    }
}

fn cleanup_unregistered_remote_outputs(
    file_manager: &Arc<FileManager>,
    outputs: &[RemoteDataFile],
) {
    for output in outputs {
        let _ = file_manager.remove_data_file_at_path(&output.full_path);
    }
}

struct PendingRemoteOutputCleanup {
    file_manager: Arc<FileManager>,
    paths: Vec<String>,
    file_ids: Vec<FileId>,
    armed: bool,
}

impl PendingRemoteOutputCleanup {
    fn new(file_manager: Arc<FileManager>, paths: Vec<String>, file_ids: Vec<FileId>) -> Self {
        Self {
            file_manager,
            paths,
            file_ids,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingRemoteOutputCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        for file_id in &self.file_ids {
            let _ = self.file_manager.remove_data_file(*file_id);
        }
        for path in &self.paths {
            let _ = self.file_manager.remove_data_file_at_path(path);
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/compaction/remote.rs"]
mod tests;
