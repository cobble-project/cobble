//! This module implements a remote compaction worker and server for Cobble.
//! The `RemoteCompactionWorker` sends compaction tasks to a remote server over TCP
//! and the `RemoteCompactionServer` listens for incoming compaction requests
//! executes them using a local `CompactionExecutor`, and returns the results back to the worker.
use super::{CompactionExecutor, CompactionResult, CompactionTask, CompactionWorker};
use crate::Config;
use crate::data_file::{DataFile, DataFileType};
use crate::db_status::DbLifecycle;
use crate::error::{Error, Result};
use crate::file::{DataVolume, FileId, FileManager, FileManagerOptions, TrackedFileId};
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
use bytes::Bytes;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
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
const REMOTE_COMPACTION_PROTOCOL_VERSION_CURRENT: u32 = 1;
const REMOTE_COMPACTION_PROTOCOL_MIN_COMPATIBLE_VERSION: u32 = 1;
type RemoteCompactionOutput = (Vec<RemoteDataFile>, Vec<(u32, i64)>);

fn validate_protocol_compatibility(
    role: &str,
    peer_version: u32,
    peer_min_compatible_version: u32,
) -> Result<()> {
    let local_version = REMOTE_COMPACTION_PROTOCOL_VERSION_CURRENT;
    let local_min_compatible = REMOTE_COMPACTION_PROTOCOL_MIN_COMPATIBLE_VERSION;
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
    compression: crate::SstCompressionAlgorithm,
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
            compression: options.compression,
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
            compression: self.compression,
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
        readonly: bool,
    ) -> Result<Arc<DataFile>> {
        let file_type = DataFileType::from_str(&self.file_type).map_err(Error::IoError)?;
        let path = self.full_path;
        if readonly {
            file_manager.register_data_file_readonly(file_id, &path)?;
        } else {
            file_manager.register_data_file(file_id, &path)?;
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
struct RemoteCompactionRequest {
    version: u32,
    compatible_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<u64>,
    db_id: String,
    lsm_tree_idx: usize,
    column_family_id: u8,
    output_level: u8,
    writer_options: RemoteWriterOptions,
    ttl_config: RemoteTtlConfig,
    ttl_now_seconds: u32,
    runs: Vec<RemoteSortedRun>,
    merge_operator_ids: Vec<String>,
    merge_operator_metadata: Vec<Option<serde_json::Value>>,
}

impl RemoteCompactionRequest {
    fn validate_protocol_compatibility(&self) -> Result<()> {
        validate_protocol_compatibility("request", self.version, self.compatible_version)
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

/// A struct representing the response from a remote compaction request.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RemoteCompactionResponse {
    version: u32,
    compatible_version: u32,
    output_files: Vec<RemoteDataFile>,
    vlog_entry_deltas: Vec<(u32, i64)>,
    error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
enum RemoteCompactionCommand {
    Execute(RemoteCompactionRequest),
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
    fn ok(output_files: Vec<RemoteDataFile>, vlog_entry_deltas: Vec<(u32, i64)>) -> Self {
        Self {
            version: REMOTE_COMPACTION_PROTOCOL_VERSION_CURRENT,
            compatible_version: REMOTE_COMPACTION_PROTOCOL_MIN_COMPATIBLE_VERSION,
            output_files,
            vlog_entry_deltas,
            error: None,
        }
    }

    fn err(message: impl Into<String>) -> Self {
        Self {
            version: REMOTE_COMPACTION_PROTOCOL_VERSION_CURRENT,
            compatible_version: REMOTE_COMPACTION_PROTOCOL_MIN_COMPATIBLE_VERSION,
            output_files: Vec::new(),
            vlog_entry_deltas: Vec::new(),
            error: Some(message.into()),
        }
    }

    fn validate_protocol_compatibility(&self) -> Result<()> {
        validate_protocol_compatibility("response", self.version, self.compatible_version)
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

/// A compaction worker that sends compaction tasks to a remote server.
pub(crate) struct RemoteCompactionWorker {
    address: String,
    file_manager: Arc<FileManager>,
    lsm_tree: Weak<LSMTree>,
    config: Config,
    ttl_config: TtlConfig,
    runtime: Mutex<Option<Runtime>>,
    remote_timeout: Duration,
    metrics_manager: Arc<MetricsManager>,
    schema_manager: Arc<SchemaManager>,
    supported_merge_operator_ids: HashSet<String>,
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
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("cobble-remote-compaction")
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| Error::IoError(e.to_string()))?;
        let supported_merge_operator_ids =
            fetch_supported_merge_operator_ids(&address, remote_timeout)?
                .into_iter()
                .collect();
        info!(
            "Cobble remote compactor ({}, Rev:{}) start at addr: {}.",
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
            remote_timeout,
            metrics_manager,
            schema_manager,
            supported_merge_operator_ids,
        })
    }

    fn runtime_handle(&self) -> Option<tokio::runtime::Handle> {
        let guard = self.runtime.lock().unwrap();
        guard.as_ref().map(|runtime| runtime.handle().clone())
    }

    fn build_request(
        &self,
        lsm_tree_idx: usize,
        sorted_runs: &[SortedRun],
        output_level: u8,
        data_file_type: DataFileType,
        ttl_provider: Arc<TTLProvider>,
    ) -> Result<RemoteCompactionRequest> {
        let lsm_tree = self.lsm_tree.upgrade().ok_or_else(|| {
            Error::IoError("lsm tree dropped during remote compaction".to_string())
        })?;
        let tree_scope = lsm_tree.tree_scope_of_tree(lsm_tree_idx).ok_or_else(|| {
            Error::InvalidState(format!(
                "missing tree scope for remote compaction tree {}",
                lsm_tree_idx
            ))
        })?;
        let runs = sorted_runs
            .iter()
            .map(|run| RemoteSortedRun::from_sorted_run(run, &self.file_manager))
            .collect::<Result<Vec<_>>>()?;
        let schema = self.schema_manager.latest_schema();
        let num_columns = schema
            .num_columns_in_family(tree_scope.column_family_id)
            .unwrap_or_else(|| schema.num_columns());
        let writer_options =
            super::build_writer_options(&self.config, output_level, data_file_type, num_columns)?;
        let merge_operator_ids =
            schema.operator_ids_for_column_family_id(tree_scope.column_family_id);
        let merge_operator_metadata = schema
            .column_metadata_for_column_family_id(tree_scope.column_family_id)
            .to_vec();
        for merge_operator_id in &merge_operator_ids {
            if !self
                .supported_merge_operator_ids
                .contains(merge_operator_id)
            {
                return Err(Error::ConfigError(format!(
                    "remote compactor {} does not support merge operator '{}'",
                    self.address, merge_operator_id
                )));
            }
        }
        Ok(RemoteCompactionRequest {
            version: REMOTE_COMPACTION_PROTOCOL_VERSION_CURRENT,
            compatible_version: REMOTE_COMPACTION_PROTOCOL_MIN_COMPATIBLE_VERSION,
            request_id: None,
            db_id: self.metrics_manager.db_id().to_string(),
            lsm_tree_idx,
            column_family_id: tree_scope.column_family_id,
            output_level,
            writer_options: RemoteWriterOptions::from_writer_options(&writer_options, num_columns),
            ttl_config: RemoteTtlConfig {
                enabled: self.ttl_config.enabled,
                default_ttl_seconds: self.ttl_config.default_ttl_seconds,
            },
            ttl_now_seconds: ttl_provider.now_seconds(),
            runs,
            merge_operator_ids,
            merge_operator_metadata,
        })
    }
}

impl CompactionWorker for RemoteCompactionWorker {
    fn submit_runs(
        &self,
        lsm_tree_idx: usize,
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
        data_file_type: DataFileType,
        ttl_provider: Arc<TTLProvider>,
    ) -> Option<tokio::task::JoinHandle<Result<CompactionResult>>> {
        if sorted_runs.is_empty() {
            return None;
        }
        let Some(handle) = self.runtime_handle() else {
            warn!("remote compaction worker is shutdown, cannot submit new tasks");
            return None;
        };
        let request = match self.build_request(
            lsm_tree_idx,
            &sorted_runs,
            output_level,
            data_file_type,
            ttl_provider,
        ) {
            Ok(request) => request,
            Err(err) => {
                let lsm_tree = self.lsm_tree.clone();
                return Some(handle.spawn_blocking(move || {
                    if let Some(lsm_tree) = lsm_tree.upgrade() {
                        let _ = lsm_tree.on_compaction_complete(lsm_tree_idx);
                    }
                    Err(err)
                }));
            }
        };
        let lsm_tree = self.lsm_tree.clone();
        let file_manager = Arc::clone(&self.file_manager);
        let address = self.address.clone();
        let remote_timeout = self.remote_timeout;
        Some(handle.spawn_blocking(move || {
            let result = (|| -> Result<CompactionResult> {
                let response = send_compaction_request_to(&address, request, remote_timeout)?;
                let Some(lsm_tree) = lsm_tree.upgrade() else {
                    return Err(Error::IoError(
                        "lsm tree dropped during compaction".to_string(),
                    ));
                };
                if let Some(error) = response.error {
                    return Err(Error::IoError(error));
                }
                let output_ids = file_manager.reserve_data_file_ids(response.output_files.len());
                let output_files = response
                    .output_files
                    .into_iter()
                    .zip(output_ids)
                    .map(|(file, file_id)| file.into_data_file(&file_manager, file_id, false))
                    .collect::<Result<Vec<_>>>()?;
                if let Err(err) = file_manager.trigger_offload_if_needed() {
                    warn!("remote compaction check-in offload trigger failed: {}", err);
                }
                let edit = build_version_edit(&sorted_runs, output_level, output_files.clone());
                let vlog_edit = {
                    let edit = VlogEdit::from_entry_deltas(response.vlog_entry_deltas);
                    (!edit.is_empty()).then_some(edit)
                };
                let apply_tree_idx = lsm_tree.on_compaction_complete(lsm_tree_idx);
                if let Some(apply_tree_idx) = apply_tree_idx {
                    lsm_tree.apply_edit(apply_tree_idx, edit.clone(), vlog_edit.clone());
                }
                Ok(CompactionResult::new(
                    lsm_tree_idx,
                    output_files,
                    edit,
                    vlog_edit,
                ))
            })();
            if result.is_err()
                && let Some(lsm_tree) = lsm_tree.upgrade()
            {
                let _ = lsm_tree.on_compaction_complete(lsm_tree_idx);
            }
            result
        }))
    }

    fn shutdown(&self) {
        info!(
            "cobble=remote compaction worker shutdown version={} build_commit={}",
            build_version_string(),
            build_commit_short_id()
        );
        if let Some(runtime) = self.runtime.lock().unwrap().take() {
            runtime.shutdown_timeout(Duration::from_millis(500));
        }
    }
}

/// A server that listens for remote compaction requests and executes them.
pub struct RemoteCompactionServer {
    config: Config,
    runtime: Arc<Runtime>,
    executor: Arc<CompactionExecutor>,
    data_volumes: Arc<Vec<DataVolume>>,
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
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .thread_name("cobble-compaction")
                .worker_threads(compaction_config.max_threads.max(1))
                .enable_all()
                .build()
                .map_err(|e| Error::IoError(e.to_string()))?,
        );
        let executor = CompactionExecutor::new_with_runtime(
            compaction_config,
            runtime.clone(),
            Arc::new(DbLifecycle::new_open()),
        )?;
        let data_volumes = FileManager::data_volumes_from_config(&config)?;
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
            data_volumes: Arc::new(data_volumes),
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
        let data_volumes = Arc::clone(&self.data_volumes);
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
                        let response = RemoteCompactionResponse::err(err.to_string());
                        let _ =
                            write_message(&mut stream, &RemoteCompactionReply::Execute(response));
                        return;
                    }
                    let request_id = request_id_counter.fetch_add(1, Ordering::SeqCst);
                    request.request_id = Some(request_id);
                    info!("Received request: {}", request);
                    let response = match Self::handle_request_with(
                        &config,
                        executor.as_ref(),
                        data_volumes.clone(),
                        Arc::clone(&metrics_manager),
                        Arc::clone(&merge_operator_map),
                        merge_operator_resolver.clone(),
                        request,
                    ) {
                        Ok((files, vlog_entry_deltas)) => {
                            RemoteCompactionResponse::ok(files, vlog_entry_deltas)
                        }
                        Err(err) => RemoteCompactionResponse::err(err.to_string()),
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
        data_volumes: Arc<Vec<DataVolume>>,
        metrics_manager: Arc<MetricsManager>,
        merge_operator_map: Arc<Mutex<HashMap<String, Arc<dyn MergeOperator>>>>,
        merge_operator_resolver: Option<Arc<dyn MergeOperatorResolver>>,
        request: RemoteCompactionRequest,
    ) -> Result<RemoteCompactionOutput> {
        let file_manager =
            Self::file_manager_for_with(config, &data_volumes, &request.db_id, &metrics_manager)?;
        let data_file_type = request.writer_options.data_file_type();
        let num_columns = request.writer_options.num_columns();
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
                        file.into_data_file(&file_manager, file_id, true)
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
        let merge_operators = Self::resolve_merge_operators(
            Arc::clone(&merge_operator_map),
            merge_operator_resolver,
            &request.merge_operator_ids,
            &request.merge_operator_metadata,
            num_columns,
        )?;
        let schema_manager = Arc::new(SchemaManager::from_schemas(
            vec![Schema::new_for_column_family(
                0,
                request.column_family_id,
                merge_operators,
                request.merge_operator_metadata.clone(),
            )],
            num_columns,
        ));
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
        .with_column_family(request.column_family_id, num_columns)
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
        Ok((output_files, vlog_entry_deltas))
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
        config: &Config,
        data_volumes: &Arc<Vec<DataVolume>>,
        db_id: &str,
        metrics_manager: &Arc<MetricsManager>,
    ) -> Result<Arc<FileManager>> {
        let options = FileManagerOptions {
            base_dir: db_id.to_string(),
            base_file_size: config.base_file_size_bytes()?,
            ..FileManagerOptions::default()
        };
        let file_manager =
            FileManager::new(data_volumes.to_vec(), options, Arc::clone(metrics_manager))?;
        file_manager.set_next_file_id(REMOTE_FILE_ID_START);
        Ok(Arc::new(file_manager))
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
    match send_command_to(address, RemoteCompactionCommand::Execute(request), timeout)? {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VolumeDescriptor;
    use crate::compaction::{build_sst_writer_options, make_data_file_builder_factory};
    use crate::db_state::{DbState, DbStateHandle, LSMTreeScope, MultiLSMTreeVersion};
    use crate::lsm::{LSMTree, LSMTreeVersion, Level};
    use crate::parquet::ParquetIterator;
    use crate::sst::row_codec::{decode_value, encode_key, encode_value};
    use crate::r#type::{Column, Key, KvValue, Value, ValueType};
    use crate::writer_options::WriterOptions;
    use serial_test::serial;
    use size::Size;
    use std::collections::{HashMap, VecDeque};
    use std::sync::Arc;

    fn cleanup_test_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn make_typed_value_bytes(value_type: ValueType, data: &[u8], num_columns: usize) -> Vec<u8> {
        let value = Value::new(vec![Some(Column::new(value_type, data.to_vec()))]);
        encode_value(&value, num_columns).to_vec()
    }

    fn make_test_key(raw_key: &[u8]) -> Vec<u8> {
        encode_key(&Key::new(0, raw_key.to_vec())).to_vec()
    }

    fn make_test_key_in_family(column_family_id: u8, raw_key: &[u8]) -> Vec<u8> {
        encode_key(&Key::new_with_column_family(
            0,
            column_family_id,
            raw_key.to_vec(),
        ))
        .to_vec()
    }

    fn create_test_sst(
        file_manager: &Arc<FileManager>,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        options: SSTWriterOptions,
    ) -> Result<Arc<DataFile>> {
        let (file_id, writer_file) = file_manager.create_data_file_with_offload()?;
        let mut writer = crate::sst::SSTWriter::new(writer_file, options);

        for (key, value) in entries {
            writer.add(&key, &value)?;
        }

        let (first_key, last_key, file_size, footer_bytes) = writer.finish_with_range()?;
        let bucket_range = DataFile::bucket_range_from_keys(&first_key, &last_key);
        let data_file = DataFile::new(
            DataFileType::SSTable,
            first_key,
            last_key,
            file_id,
            TrackedFileId::new(file_manager, file_id),
            0,
            file_size,
            bucket_range.clone(),
            bucket_range,
        );
        data_file.set_meta_bytes(footer_bytes);
        Ok(Arc::new(data_file))
    }

    fn create_test_parquet(
        file_manager: &Arc<FileManager>,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Arc<DataFile>> {
        let (file_id, writer_file) = file_manager.create_data_file_with_offload()?;
        let factory =
            make_data_file_builder_factory(WriterOptions::Parquet(ParquetWriterOptions {
                row_group_size_bytes: 256 * 1024,
                buffer_size: 8192,
                num_columns: 1,
            }));
        let mut writer = factory(Box::new(writer_file));
        for (key, value) in entries {
            writer.add(&key, &KvValue::Encoded(Bytes::from(value)))?;
        }
        let (first_key, last_key, file_size, footer_bytes) = writer.finish()?;
        let bucket_range = DataFile::bucket_range_from_keys(&first_key, &last_key);
        let data_file = DataFile::new(
            DataFileType::Parquet,
            first_key,
            last_key,
            file_id,
            TrackedFileId::new(file_manager, file_id),
            0,
            file_size,
            bucket_range.clone(),
            bucket_range,
        );
        data_file.set_meta_bytes(footer_bytes);
        Ok(Arc::new(data_file))
    }

    #[test]
    #[serial(file)]
    fn test_remote_compaction_roundtrip_multiple_files() {
        let root = "/tmp/remote_compaction_roundtrip";
        cleanup_test_root(root);
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            base_file_size: Size::from_const(128),
            sst_bloom_filter_enabled: true,
            compaction_threads: 2,
            ..Config::default()
        };
        let db_id = "remote-compaction-roundtrip".to_string();
        let metrics_manager = Arc::new(MetricsManager::new(&db_id));
        let file_manager = Arc::new(
            FileManager::from_config(&config, &db_id, Arc::clone(&metrics_manager)).unwrap(),
        );
        let mut sst_options = build_sst_writer_options(&config, 0, config.num_columns);
        sst_options.metrics = Some(metrics_manager.sst_writer_metrics(sst_options.compression));
        let value_payload = vec![b'x'; 128];
        let num_columns = sst_options.num_columns;

        let entries_a = (0..40)
            .map(|idx| {
                let key = format!("a{:03}", idx).into_bytes();
                let value = make_typed_value_bytes(ValueType::Put, &value_payload, num_columns);
                (key, value)
            })
            .collect::<Vec<_>>();
        let entries_b = (0..40)
            .map(|idx| {
                let key = format!("b{:03}", idx).into_bytes();
                let value = make_typed_value_bytes(ValueType::Put, &value_payload, num_columns);
                (key, value)
            })
            .collect::<Vec<_>>();

        let file_a = create_test_sst(&file_manager, entries_a, sst_options.clone()).unwrap();
        let file_b = create_test_sst(&file_manager, entries_b, sst_options.clone()).unwrap();

        let lsm_version = LSMTreeVersion {
            levels: vec![
                Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![Arc::clone(&file_a), Arc::clone(&file_b)],
                },
                Level {
                    ordinal: 1,
                    tiered: false,
                    files: Vec::new(),
                },
            ],
        };
        let db_state = Arc::new(DbStateHandle::new());
        db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version: MultiLSMTreeVersion::new(lsm_version),
            vlog_version: crate::vlog::VlogVersion::new(),
            active: None,
            immutables: VecDeque::new(),
            suggested_base_snapshot_id: None,
        });
        let lsm_tree = Arc::new(LSMTree::with_state(
            Arc::clone(&db_state),
            Arc::clone(&metrics_manager),
        ));

        let remote_timeout = Duration::from_millis(config.compaction_remote_timeout_ms);
        let server = Arc::new(RemoteCompactionServer::new(config.clone()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server_thread = {
            let server = Arc::clone(&server);
            std::thread::spawn(move || {
                for _ in 0..2 {
                    if let Ok((stream, _)) = listener.accept() {
                        server.handle_connection(stream).unwrap();
                    }
                }
            })
        };
        let schema_manager = Arc::new(SchemaManager::new(config.num_columns));

        let worker = RemoteCompactionWorker::new(
            addr.to_string(),
            Arc::clone(&file_manager),
            Arc::downgrade(&lsm_tree),
            config.clone(),
            TtlConfig {
                enabled: false,
                default_ttl_seconds: None,
            },
            remote_timeout,
            Arc::clone(&metrics_manager),
            Arc::clone(&schema_manager),
        )
        .unwrap();

        let runs = vec![
            SortedRun::new(0, vec![file_a]),
            SortedRun::new(0, vec![file_b]),
        ];
        let handle = worker
            .submit_runs(
                0,
                runs,
                1,
                DataFileType::SSTable,
                Arc::new(TTLProvider::disabled()),
            )
            .expect("compaction handle");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(handle).unwrap().unwrap();
        let _ = server_thread.join();

        let level0 = lsm_tree.level_files(0);
        let level1 = lsm_tree.level_files(1);
        assert!(level0.is_empty());
        assert!(level1.len() > 1);
        assert!(
            level1
                .iter()
                .all(|file| file.file_id < REMOTE_FILE_ID_START)
        );
        assert!(
            level1
                .iter()
                .all(|file| file.file_type == DataFileType::SSTable)
        );

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_remote_compaction_with_u64_counter_merge_operator_in_non_default_family() {
        let root = "/tmp/remote_compaction_u64_counter";
        cleanup_test_root(root);
        let column_family_id = 1;
        let num_columns = 1;
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            base_file_size: Size::from_const(128),
            sst_bloom_filter_enabled: true,
            compaction_threads: 2,
            num_columns: 2,
            ..Config::default()
        };
        let db_id = "remote-compaction-u64-counter".to_string();
        let metrics_manager = Arc::new(MetricsManager::new(&db_id));
        let file_manager = Arc::new(
            FileManager::from_config(&config, &db_id, Arc::clone(&metrics_manager)).unwrap(),
        );
        let mut sst_options = build_sst_writer_options(&config, 0, num_columns);
        sst_options.metrics = Some(metrics_manager.sst_writer_metrics(sst_options.compression));

        let schema_manager = Arc::new(SchemaManager::new(config.num_columns));
        let mut schema_builder = schema_manager.builder();
        schema_builder
            .add_column(0, None, None, Some("metrics".to_string()))
            .unwrap();
        schema_builder
            .set_column_operator(
                Some("metrics".to_string()),
                0,
                Arc::new(U64CounterMergeOperator),
            )
            .unwrap();
        let _ = schema_builder.commit();

        let mut expected = HashMap::new();
        let entries_old = (0..20u64)
            .map(|idx| {
                let key =
                    make_test_key_in_family(column_family_id, format!("k{:03}", idx).as_bytes());
                let base = idx + 1;
                let delta = 10u64;
                expected.insert(key.clone(), base + delta);
                (
                    key,
                    make_typed_value_bytes(ValueType::Put, &base.to_le_bytes(), num_columns),
                )
            })
            .collect::<Vec<_>>();
        let entries_new = (0..20u64)
            .map(|idx| {
                let key =
                    make_test_key_in_family(column_family_id, format!("k{:03}", idx).as_bytes());
                let delta = 10u64;
                (
                    key,
                    make_typed_value_bytes(ValueType::Merge, &delta.to_le_bytes(), num_columns),
                )
            })
            .collect::<Vec<_>>();

        let older_file = create_test_sst(&file_manager, entries_old, sst_options.clone()).unwrap();
        let newer_file = create_test_sst(&file_manager, entries_new, sst_options.clone()).unwrap();

        let lsm_version = LSMTreeVersion {
            levels: vec![
                Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![Arc::clone(&newer_file), Arc::clone(&older_file)],
                },
                Level {
                    ordinal: 1,
                    tiered: false,
                    files: Vec::new(),
                },
            ],
        };
        let db_state = Arc::new(DbStateHandle::new());
        db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version: MultiLSMTreeVersion::from_scopes_with_tree_versions(
                1,
                &[LSMTreeScope::new(0u16..=0u16, column_family_id)],
                vec![Arc::new(lsm_version)],
            )
            .unwrap(),
            vlog_version: crate::vlog::VlogVersion::new(),
            active: None,
            immutables: VecDeque::new(),
            suggested_base_snapshot_id: None,
        });
        let lsm_tree = Arc::new(LSMTree::with_state(
            Arc::clone(&db_state),
            Arc::clone(&metrics_manager),
        ));

        let remote_timeout = Duration::from_millis(config.compaction_remote_timeout_ms);
        let server = Arc::new(RemoteCompactionServer::new(config.clone()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server_thread = {
            let server = Arc::clone(&server);
            std::thread::spawn(move || {
                for _ in 0..2 {
                    if let Ok((stream, _)) = listener.accept() {
                        server.handle_connection(stream).unwrap();
                    }
                }
            })
        };

        let worker = RemoteCompactionWorker::new(
            addr.to_string(),
            Arc::clone(&file_manager),
            Arc::downgrade(&lsm_tree),
            config.clone(),
            TtlConfig {
                enabled: false,
                default_ttl_seconds: None,
            },
            remote_timeout,
            Arc::clone(&metrics_manager),
            Arc::clone(&schema_manager),
        )
        .unwrap();

        let runs = vec![
            SortedRun::new(0, vec![newer_file]),
            SortedRun::new(0, vec![older_file]),
        ];
        let handle = worker
            .submit_runs(
                0,
                runs,
                1,
                DataFileType::SSTable,
                Arc::new(TTLProvider::disabled()),
            )
            .expect("compaction handle");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(handle).unwrap().unwrap();
        let _ = server_thread.join();

        let mut actual = HashMap::new();
        for file in lsm_tree.level_files(1) {
            let reader = file_manager.open_data_file_reader(file.file_id).unwrap();
            let mut iter = crate::sst::SSTIterator::with_cache_and_file(
                Box::new(reader),
                file.as_ref(),
                crate::sst::SSTIteratorOptions {
                    num_columns,
                    bloom_filter_enabled: true,
                    ..Default::default()
                },
                None,
            )
            .unwrap();
            iter.seek_to_first().unwrap();
            while iter.valid() {
                let (key, mut value) = iter.current().unwrap().unwrap();
                let decoded = decode_value(&mut value, num_columns).unwrap();
                let bytes = decoded.columns()[0].as_ref().unwrap().data();
                let merged = u64::from_le_bytes(bytes.as_ref().try_into().unwrap());
                actual.insert(key.to_vec(), merged);
                iter.next().unwrap();
            }
        }
        assert_eq!(actual, expected);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_remote_compaction_roundtrip_parquet_output() {
        let root = "/tmp/remote_compaction_roundtrip_parquet";
        cleanup_test_root(root);
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            base_file_size: Size::from_const(128),
            sst_bloom_filter_enabled: true,
            compaction_threads: 2,
            ..Config::default()
        };
        let db_id = "remote-compaction-roundtrip-parquet".to_string();
        let metrics_manager = Arc::new(MetricsManager::new(&db_id));
        let file_manager = Arc::new(
            FileManager::from_config(&config, &db_id, Arc::clone(&metrics_manager)).unwrap(),
        );
        let num_columns = config.num_columns;
        let entries_a = (0..40)
            .map(|idx| {
                let key = format!("a{:03}", idx).into_bytes();
                let value = make_typed_value_bytes(ValueType::Put, b"va", num_columns);
                (key, value)
            })
            .collect::<Vec<_>>();
        let entries_b = (0..40)
            .map(|idx| {
                let key = format!("b{:03}", idx).into_bytes();
                let value = make_typed_value_bytes(ValueType::Put, b"vb", num_columns);
                (key, value)
            })
            .collect::<Vec<_>>();
        let file_a = create_test_parquet(&file_manager, entries_a).unwrap();
        let file_b = create_test_parquet(&file_manager, entries_b).unwrap();

        let lsm_version = LSMTreeVersion {
            levels: vec![
                Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![Arc::clone(&file_a), Arc::clone(&file_b)],
                },
                Level {
                    ordinal: 1,
                    tiered: false,
                    files: Vec::new(),
                },
            ],
        };
        let db_state = Arc::new(DbStateHandle::new());
        db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version: MultiLSMTreeVersion::new(lsm_version),
            vlog_version: crate::vlog::VlogVersion::new(),
            active: None,
            immutables: VecDeque::new(),
            suggested_base_snapshot_id: None,
        });
        let lsm_tree = Arc::new(LSMTree::with_state(
            Arc::clone(&db_state),
            Arc::clone(&metrics_manager),
        ));

        let remote_timeout = Duration::from_millis(config.compaction_remote_timeout_ms);
        let server = Arc::new(RemoteCompactionServer::new(config.clone()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server_thread = {
            let server = Arc::clone(&server);
            std::thread::spawn(move || {
                for _ in 0..2 {
                    if let Ok((stream, _)) = listener.accept() {
                        server.handle_connection(stream).unwrap();
                    }
                }
            })
        };
        let schema_manager = Arc::new(SchemaManager::new(config.num_columns));
        let worker = RemoteCompactionWorker::new(
            addr.to_string(),
            Arc::clone(&file_manager),
            Arc::downgrade(&lsm_tree),
            config.clone(),
            TtlConfig {
                enabled: false,
                default_ttl_seconds: None,
            },
            remote_timeout,
            Arc::clone(&metrics_manager),
            Arc::clone(&schema_manager),
        )
        .unwrap();

        let runs = vec![
            SortedRun::new(0, vec![file_a]),
            SortedRun::new(0, vec![file_b]),
        ];
        let handle = worker
            .submit_runs(
                0,
                runs,
                1,
                DataFileType::Parquet,
                Arc::new(TTLProvider::disabled()),
            )
            .expect("compaction handle");
        let runtime = Runtime::new().unwrap();
        runtime.block_on(handle).unwrap().unwrap();
        let _ = server_thread.join();

        let level1 = lsm_tree.level_files(1);
        assert!(!level1.is_empty());
        assert!(
            level1
                .iter()
                .all(|file| file.file_type == DataFileType::Parquet)
        );
        for file in level1 {
            let reader = file_manager.open_data_file_reader(file.file_id).unwrap();
            let mut iter =
                ParquetIterator::from_data_file(Box::new(reader), file.as_ref(), None).unwrap();
            iter.seek_to_first().unwrap();
            assert!(iter.valid());
        }
        cleanup_test_root(root);
    }

    #[test]
    fn test_request_limiter_basic() {
        let limiter = RequestLimiter::new(2, 1); // max 2 concurrent, 1 queued
        // Accept 3 requests (2 active + 1 queued)
        assert!(limiter.try_accept());
        assert!(limiter.try_accept());
        assert!(limiter.try_accept());
        // 4th should be rejected (over capacity)
        assert!(!limiter.try_accept());
    }

    #[test]
    fn test_request_limiter_acquire_and_release() {
        let limiter = Arc::new(RequestLimiter::new(1, 1));
        // First request: accept and acquire
        assert!(limiter.try_accept());
        assert!(limiter.acquire_slot(None));
        // Second: accept (queued) and acquire blocks, so release first
        assert!(limiter.try_accept());
        // Third: rejected
        assert!(!limiter.try_accept());
        // Release first slot
        limiter.release_slot();
        // Now second can acquire
        assert!(limiter.acquire_slot(None));
        limiter.release_slot();
    }

    #[test]
    fn test_request_limiter_shutdown_unblocks() {
        let limiter = Arc::new(RequestLimiter::new(1, 2));
        // Fill up the active slot
        assert!(limiter.try_accept());
        assert!(limiter.acquire_slot(None));
        // Queue a second request
        assert!(limiter.try_accept());
        let limiter2 = Arc::clone(&limiter);
        let handle = std::thread::spawn(move || limiter2.acquire_slot(None));
        std::thread::sleep(Duration::from_millis(50));
        // Shutdown should unblock the waiting thread
        limiter.shutdown();
        let acquired = handle.join().unwrap();
        assert!(!acquired, "should return false on shutdown");
        // After shutdown, try_accept should fail
        assert!(!limiter.try_accept());
    }

    #[test]
    #[serial(file)]
    fn test_server_rejects_when_overloaded() {
        let root = "/tmp/remote_compaction_overload";
        cleanup_test_root(root);
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            compaction_threads: 1,
            compaction_server_max_concurrent: 1,
            compaction_server_max_queued: 0,
            ..Config::default()
        };
        let server = Arc::new(RemoteCompactionServer::new(config).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();

        // Occupy the only active slot with a slow request that blocks the server
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let barrier_clone = barrier.clone();
        let server_clone = Arc::clone(&server);
        let accept_handle = std::thread::spawn(move || {
            // Accept connections in a loop
            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false).ok();
                        let _ = server_clone.handle_connection(stream);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
                // After accepting first connection, signal
                barrier_clone.wait();
            }
        });

        // First connection — will occupy the active slot
        let slow_handle = std::thread::spawn({
            let addr_str = addr.to_string();
            move || {
                let mut stream = TcpStream::connect(&addr_str).expect("connect should succeed");
                stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
                stream.set_write_timeout(Some(Duration::from_secs(5))).ok();
                write_message(
                    &mut stream,
                    &RemoteCompactionCommand::SupportedMergeOperators,
                )
                .unwrap();
                // This will succeed because the server processes it
                let reply: Result<RemoteCompactionReply> = read_message(&mut stream);
                reply
            }
        });

        // Wait until first connection is accepted
        barrier.wait();
        std::thread::sleep(Duration::from_millis(200));

        // Second connection should be rejected (max_concurrent=1, max_queued=0)
        let mut stream2 = TcpStream::connect(addr.to_string()).unwrap();
        stream2.set_read_timeout(Some(Duration::from_secs(2))).ok();
        stream2.set_write_timeout(Some(Duration::from_secs(2))).ok();
        write_message(
            &mut stream2,
            &RemoteCompactionCommand::SupportedMergeOperators,
        )
        .unwrap();
        let reply: Result<RemoteCompactionReply> = read_message(&mut stream2);

        // First connection completes fine
        let first_reply = slow_handle.join().unwrap();
        assert!(
            matches!(
                first_reply,
                Ok(RemoteCompactionReply::SupportedMergeOperators(_))
            ),
            "first request should succeed"
        );

        // Second connection should get an overload error
        match reply {
            Ok(RemoteCompactionReply::Error(msg)) => {
                assert!(
                    msg.contains("overloaded") || msg.contains("queue full"),
                    "error should mention overload, got: {}",
                    msg
                );
            }
            other => {
                // It's also acceptable if the connection was reset/closed
                // This can happen if the server rejects before reading the full message
                if let Ok(ref r) = other {
                    panic!("expected error reply, got: {:?}", r);
                }
            }
        }

        drop(accept_handle);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_server_close_detected_by_client() {
        let root = "/tmp/remote_compaction_close";
        cleanup_test_root(root);
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            compaction_threads: 1,
            ..Config::default()
        };
        let server = Arc::new(RemoteCompactionServer::new(config).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        // Server thread using serve()
        let server_clone = Arc::clone(&server);
        let addr_str = addr.to_string();
        let server_thread = std::thread::spawn(move || {
            let _ = server_clone.serve(&addr_str);
        });
        std::thread::sleep(Duration::from_millis(100));

        // Verify server works
        let ids =
            fetch_supported_merge_operator_ids(&addr.to_string(), Duration::from_secs(5)).unwrap();
        assert!(!ids.is_empty());

        // Close the server
        server.close();
        server_thread.join().unwrap();

        // After close, client should fail to connect
        let result = fetch_supported_merge_operator_ids(&addr.to_string(), Duration::from_secs(1));
        assert!(result.is_err(), "should fail after server close");

        cleanup_test_root(root);
    }

    #[test]
    fn test_client_timeout_on_connect() {
        // Connect to a non-routable address — should timeout
        let result = send_command_to(
            "192.0.2.1:9999", // RFC 5737 TEST-NET, non-routable
            RemoteCompactionCommand::SupportedMergeOperators,
            Duration::from_millis(200),
        );
        assert!(result.is_err(), "should timeout on non-routable address");
    }
}
