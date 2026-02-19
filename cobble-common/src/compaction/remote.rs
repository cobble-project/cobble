//! This module implements a remote compaction worker and server for Cobble.
//! The `RemoteCompactionWorker` sends compaction tasks to a remote server over TCP
//! and the `RemoteCompactionServer` listens for incoming compaction requests
//! executes them using a local `CompactionExecutor`, and returns the results back to the worker.
use super::{CompactionExecutor, CompactionResult, CompactionTask, CompactionWorker};
use crate::Config;
use crate::data_file::{DataFile, DataFileType};
use crate::error::{Error, Result};
use crate::file::{DataVolume, FileId, FileManager, FileManagerOptions, TrackedFileId};
use crate::iterator::SortedRun;
use crate::lsm::{LSMTree, LevelEdit, VersionEdit};
use crate::metrics_manager::MetricsManager;
use crate::sst::SSTWriterOptions;
use crate::time::ManualTimeProvider;
use crate::ttl::{TTLProvider, TtlConfig};
use crate::util::init_logging;
use bytes::Bytes;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::runtime::Runtime;
use uuid::Uuid;

const REMOTE_FILE_ID_START: u64 = u64::MAX / 2;

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

/// A struct representing a data file in the remote compaction protocol.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RemoteDataFile {
    file_id: FileId,
    file_type: String,
    full_path: String,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    seq: u64,
    size: usize,
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
            seq: file.seq,
            size: file.size,
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
            file_manager.register_data_file_readonly(file_id, path)?;
        } else {
            file_manager.register_data_file(file_id, path)?;
        }
        let data_file = DataFile {
            file_type,
            start_key: self.start_key,
            end_key: self.end_key,
            file_id,
            tracked_id: TrackedFileId::new(file_manager, file_id),
            seq: self.seq,
            size: self.size,
            meta_bytes: Default::default(),
        };
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_id: Option<u64>,
    db_id: String,
    output_level: u8,
    data_file_type: String,
    sst_options: RemoteSstOptions,
    ttl_config: RemoteTtlConfig,
    ttl_now_seconds: u32,
    runs: Vec<RemoteSortedRun>,
}

impl fmt::Display for RemoteCompactionRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let file_count: usize = self.runs.iter().map(|run| run.files.len()).sum();
        write!(
            f,
            "id={} db_id={} output_level={} data_file_type={} runs={} files={}",
            self.request_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "unassigned".to_string()),
            self.db_id,
            self.output_level,
            self.data_file_type,
            self.runs.len(),
            file_count
        )
    }
}

/// A struct representing the response from a remote compaction request.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RemoteCompactionResponse {
    output_files: Vec<RemoteDataFile>,
    error: Option<String>,
}

impl RemoteCompactionResponse {
    fn ok(output_files: Vec<RemoteDataFile>) -> Self {
        Self {
            output_files,
            error: None,
        }
    }

    fn err(message: impl Into<String>) -> Self {
        Self {
            output_files: Vec::new(),
            error: Some(message.into()),
        }
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
}

impl RemoteCompactionWorker {
    pub(crate) fn new(
        address: String,
        file_manager: Arc<FileManager>,
        lsm_tree: Weak<LSMTree>,
        config: Config,
        ttl_config: TtlConfig,
        remote_timeout: Duration,
        metrics_manager: Arc<MetricsManager>,
    ) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("cobble-remote-compaction")
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| Error::IoError(e.to_string()))?;
        Ok(Self {
            address,
            file_manager,
            lsm_tree,
            config,
            ttl_config,
            runtime: Mutex::new(Some(runtime)),
            remote_timeout,
            metrics_manager,
        })
    }

    fn runtime_handle(&self) -> Option<tokio::runtime::Handle> {
        let guard = self.runtime.lock().unwrap();
        guard.as_ref().map(|runtime| runtime.handle().clone())
    }

    fn build_request(
        &self,
        sorted_runs: &[SortedRun],
        output_level: u8,
        data_file_type: DataFileType,
        ttl_provider: Arc<TTLProvider>,
    ) -> Result<RemoteCompactionRequest> {
        let runs = sorted_runs
            .iter()
            .map(|run| RemoteSortedRun::from_sorted_run(run, &self.file_manager))
            .collect::<Result<Vec<_>>>()?;
        let sst_options = super::build_sst_writer_options(&self.config, output_level);
        Ok(RemoteCompactionRequest {
            request_id: None,
            db_id: self.metrics_manager.db_id().to_string(),
            output_level,
            data_file_type: data_file_type.to_string(),
            sst_options: RemoteSstOptions::from_sst_options(&sst_options),
            ttl_config: RemoteTtlConfig {
                enabled: self.ttl_config.enabled,
                default_ttl_seconds: self.ttl_config.default_ttl_seconds,
            },
            ttl_now_seconds: ttl_provider.now_seconds(),
            runs,
        })
    }
}

impl CompactionWorker for RemoteCompactionWorker {
    fn submit_runs(
        &self,
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
        let request =
            match self.build_request(&sorted_runs, output_level, data_file_type, ttl_provider) {
                Ok(request) => request,
                Err(err) => {
                    let lsm_tree = self.lsm_tree.clone();
                    return Some(handle.spawn_blocking(move || {
                        if let Some(lsm_tree) = lsm_tree.upgrade() {
                            lsm_tree.on_compaction_complete();
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
                let response = send_request_to(&address, request, remote_timeout)?;
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
                let edit = build_version_edit(&sorted_runs, output_level, output_files.clone());
                lsm_tree.on_compaction_complete();
                lsm_tree.apply_edit(edit.clone());
                Ok(CompactionResult::new(output_files, edit))
            })();
            if result.is_err()
                && let Some(lsm_tree) = lsm_tree.upgrade()
            {
                lsm_tree.on_compaction_complete();
            }
            result
        }))
    }

    fn shutdown(&self) {
        info!("remote compaction worker shutdown");
        if let Some(runtime) = self.runtime.lock().unwrap().take() {
            runtime.shutdown_timeout(Duration::from_secs(5));
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
}

impl RemoteCompactionServer {
    pub fn new(config: Config) -> Result<Self> {
        let compaction_config = super::build_compaction_config(&config);
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .thread_name("cobble-compaction")
                .worker_threads(compaction_config.max_threads.max(1))
                .enable_all()
                .build()
                .map_err(|e| Error::IoError(e.to_string()))?,
        );
        let executor = CompactionExecutor::new_with_runtime(compaction_config, runtime.clone())?;
        let data_volumes = FileManager::data_volumes_from_config(&config)?;
        let metrics_manager = Arc::new(MetricsManager::new(Uuid::new_v4().to_string()));
        Ok(Self {
            config,
            runtime,
            executor: Arc::new(executor),
            data_volumes: Arc::new(data_volumes),
            request_id: Arc::new(AtomicU64::new(1)),
            metrics_manager,
        })
    }

    pub fn serve(&self, address: &str) -> Result<()> {
        init_logging(&self.config);
        let listener = TcpListener::bind(address).map_err(|e| Error::IoError(e.to_string()))?;
        info!(
            "remote compaction server listening on {}",
            listener.local_addr().unwrap()
        );
        for stream in listener.incoming() {
            let stream = match stream {
                Ok(stream) => stream,
                Err(err) => {
                    warn!("Accept connection error: {}", err);
                    continue;
                }
            };
            if let Err(err) = self.handle_connection(stream) {
                warn!("Handle connection error: {}", err);
            }
        }
        Ok(())
    }

    fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        let config = self.config.clone();
        let executor = Arc::clone(&self.executor);
        let data_volumes = Arc::clone(&self.data_volumes);
        let request_id_counter = Arc::clone(&self.request_id);
        let metrics_manager = Arc::clone(&self.metrics_manager);
        self.runtime.spawn_blocking(move || {
            let request: Result<RemoteCompactionRequest> = read_message(&mut stream);
            if let Err(err) = &request {
                warn!("Read request error: {}", err);
                let response = RemoteCompactionResponse::err(format!("Invalid request: {}", err));
                let _ = write_message(&mut stream, &response);
                return;
            }
            let mut request = request.unwrap();
            let request_id = request_id_counter.fetch_add(1, Ordering::SeqCst);
            request.request_id = Some(request_id);
            info!("Received request: {}", request);
            let response = match Self::handle_request_with(
                &config,
                executor.as_ref(),
                data_volumes.clone(),
                Arc::clone(&metrics_manager),
                request,
            ) {
                Ok(files) => RemoteCompactionResponse::ok(files),
                Err(err) => RemoteCompactionResponse::err(err.to_string()),
            };
            info!("Request={} complete with response={}", request_id, response);
            let _ = write_message(&mut stream, &response);
        });
        Ok(())
    }

    fn handle_request_with(
        config: &Config,
        executor: &CompactionExecutor,
        data_volumes: Arc<Vec<DataVolume>>,
        metrics_manager: Arc<MetricsManager>,
        request: RemoteCompactionRequest,
    ) -> Result<Vec<RemoteDataFile>> {
        let file_manager =
            Self::file_manager_for_with(config, &data_volumes, &request.db_id, &metrics_manager)?;
        let data_file_type =
            DataFileType::from_str(&request.data_file_type).map_err(Error::IoError)?;
        let mut sst_options = request.sst_options.into_sst_options();
        sst_options.metrics = Some(metrics_manager.sst_writer_metrics(sst_options.compression));
        let file_builder_factory = super::make_sst_builder_factory(sst_options);
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
        let task = CompactionTask::new(
            compaction_metrics,
            sst_metrics,
            sorted_runs,
            request.output_level,
            Arc::clone(&file_manager),
            Arc::clone(&file_builder_factory),
            data_file_type,
            ttl_provider,
        )
        .with_readonly_outputs();
        let result = executor.execute_blocking(task, None);
        if let Err(e) = &result {
            warn!("Execution error: {}", e);
        }
        result?
            .new_files()
            .iter()
            .map(|file| RemoteDataFile::from_data_file_with_manager(file, &file_manager))
            .collect::<Result<Vec<_>>>()
    }

    fn file_manager_for_with(
        config: &Config,
        data_volumes: &Arc<Vec<DataVolume>>,
        db_id: &str,
        metrics_manager: &Arc<MetricsManager>,
    ) -> Result<Arc<FileManager>> {
        let options = FileManagerOptions {
            base_dir: db_id.to_string(),
            base_file_size: config.base_file_size,
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

/// Send a remote compaction request to the specified address and wait for the response.
fn send_request_to(
    address: &str,
    request: RemoteCompactionRequest,
    timeout: Duration,
) -> Result<RemoteCompactionResponse> {
    let mut stream = TcpStream::connect(address).map_err(|e| Error::IoError(e.to_string()))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| Error::IoError(e.to_string()))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| Error::IoError(e.to_string()))?;
    // serialize the request and send it to the server
    write_message(&mut stream, &request)?;
    // read the response and deserialize it
    read_message(&mut stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VolumeDescriptor;
    use crate::compaction::build_sst_writer_options;
    use crate::db_state::{DbState, DbStateHandle};
    use crate::lsm::{LSMTree, LSMTreeVersion, Level};
    use crate::sst::row_codec::encode_value;
    use crate::r#type::{Column, Value, ValueType};
    use serial_test::serial;
    use std::collections::VecDeque;
    use std::sync::Arc;

    fn cleanup_test_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn make_value_bytes(data: &[u8], num_columns: usize) -> Vec<u8> {
        let value = Value::new(vec![Some(Column::new(ValueType::Put, data.to_vec()))]);
        encode_value(&value, num_columns).to_vec()
    }

    fn create_test_sst(
        file_manager: &Arc<FileManager>,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        options: SSTWriterOptions,
    ) -> Result<Arc<DataFile>> {
        let (file_id, writer_file) = file_manager.create_data_file()?;
        let mut writer = crate::sst::SSTWriter::new(writer_file, options);

        for (key, value) in entries {
            writer.add(&key, &value)?;
        }

        let (first_key, last_key, file_size, footer_bytes) = writer.finish_with_range()?;
        let data_file = DataFile {
            file_type: DataFileType::SSTable,
            start_key: first_key,
            end_key: last_key,
            file_id,
            tracked_id: TrackedFileId::new(file_manager, file_id),
            seq: 0,
            size: file_size,
            meta_bytes: Default::default(),
        };
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
            base_file_size: 128,
            sst_bloom_filter_enabled: true,
            compaction_threads: 2,
            ..Config::default()
        };
        let db_id = "remote-compaction-roundtrip".to_string();
        let metrics_manager = Arc::new(MetricsManager::new(db_id.clone()));
        let file_manager = Arc::new(
            FileManager::from_config(&config, &db_id, Arc::clone(&metrics_manager)).unwrap(),
        );
        let mut sst_options = build_sst_writer_options(&config, 0);
        sst_options.metrics = Some(metrics_manager.sst_writer_metrics(sst_options.compression));
        let value_payload = vec![b'x'; 128];
        let num_columns = sst_options.num_columns;

        let entries_a = (0..40)
            .map(|idx| {
                let key = format!("a{:03}", idx).into_bytes();
                let value = make_value_bytes(&value_payload, num_columns);
                (key, value)
            })
            .collect::<Vec<_>>();
        let entries_b = (0..40)
            .map(|idx| {
                let key = format!("b{:03}", idx).into_bytes();
                let value = make_value_bytes(&value_payload, num_columns);
                (key, value)
            })
            .collect::<Vec<_>>();

        let file_a = create_test_sst(&file_manager, entries_a, sst_options.clone()).unwrap();
        let file_b = create_test_sst(&file_manager, entries_b, sst_options.clone()).unwrap();

        let db_state = Arc::new(DbStateHandle::new());
        db_state.store(DbState {
            seq_id: 0,
            lsm_version: LSMTreeVersion {
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
            },
            vlog_version: crate::vlog::VlogVersion::new(),
            active: None,
            immutables: VecDeque::new(),
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
                if let Ok((stream, _)) = listener.accept() {
                    server.handle_connection(stream).unwrap();
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
        )
        .unwrap();

        let runs = vec![
            SortedRun::new(0, vec![file_a]),
            SortedRun::new(0, vec![file_b]),
        ];
        let handle = worker
            .submit_runs(
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
}
