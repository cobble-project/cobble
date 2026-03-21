use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, mpsc};
use std::thread::JoinHandle;

use crate::config::MemtableType;
use crate::data_file::{DataFile, DataFileType};
use crate::db_state::{DbState, DbStateHandle, MultiLSMTreeVersion};
use crate::db_status::DbLifecycle;
use crate::error::Error::InvalidState;
use crate::error::{Error, Result};
use crate::file::{File, FileManager, TrackedFileId};
use crate::format::{FileBuilder, FileBuilderFactory};
use crate::iterator::{DeduplicatingIterator, KvIterator, SchemaEvolvingIterator};
use crate::lsm::LSMTree;
use crate::memtable::vlog::{MemtableVlogRecorder, rewrite_ref_value_for_memtable};
use crate::memtable::{HashMemtable, VecMemtable};
use crate::memtable::{Memtable, MemtableImpl, MemtableKvIter, MemtableReclaimer};
use crate::metrics_manager::MetricsManager;
use crate::parquet::{ParquetWriter, ParquetWriterOptions};
use crate::paths::snapshot_active_data_relative_path;
use crate::schema::{Schema, SchemaManager};
use crate::snapshot::{ActiveMemtableSnapshotData, SnapshotManager};
use crate::sst::{SSTWriter, SSTWriterOptions};
use crate::r#type::{RefKey, RefValue};
use crate::vlog::{VlogEdit, VlogMergeCollector, VlogPointer, VlogStore};
use crate::writer_options::WriterOptions;
use log::{debug, trace, warn};
use metrics::{Counter, counter};
use uuid::Uuid;

type DynKvIterator = Box<dyn for<'a> KvIterator<'a>>;

#[derive(Clone)]
pub(crate) struct MemtableFlushResult {
    pub(crate) data_files_by_tree: Vec<(usize, Arc<DataFile>)>,
    vlog_edit: Option<VlogEdit>,
}

pub(crate) struct MemtableManagerOptions {
    pub(crate) memtable_capacity: usize,
    pub(crate) buffer_count: usize,
    pub(crate) writer_options: WriterOptions,
    pub(crate) file_builder_factory: Option<Arc<FileBuilderFactory>>,
    pub(crate) num_columns: usize,
    pub(crate) memtable_type: MemtableType,
    pub(crate) write_stall_limit: usize,
    pub(crate) auto_snapshot_manager: Option<SnapshotManager>,
    pub(crate) metrics_manager: Option<Arc<MetricsManager>>,
    pub(crate) vlog_store: Option<Arc<VlogStore>>,
    pub(crate) schema_manager: Option<Arc<SchemaManager>>,
    pub(crate) active_memtable_incremental_snapshot_ratio: f64,
    pub(crate) db_lifecycle: Option<Arc<DbLifecycle>>,
}

#[derive(Clone)]
pub(crate) struct MemtableManagerMetrics {
    flushes_total: Counter,
    flush_bytes_total: Counter,
    write_stall_waits_total: Counter,
}

impl MemtableManagerMetrics {
    pub(crate) fn new(db_id: &str) -> Self {
        let db_id = db_id.to_string();
        Self {
            flushes_total: counter!("memtable_flushes_total", "db_id" => db_id.clone()),
            flush_bytes_total: counter!("memtable_flush_bytes_total", "db_id" => db_id.clone()),
            write_stall_waits_total: counter!("write_stall_waits_total", "db_id" => db_id),
        }
    }
}

impl Default for MemtableManagerOptions {
    fn default() -> Self {
        Self {
            memtable_capacity: 1024 * 1024,
            buffer_count: 2,
            writer_options: WriterOptions::Sst(SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            }),
            file_builder_factory: None,
            num_columns: 1,
            memtable_type: MemtableType::Hash,
            write_stall_limit: 8,
            auto_snapshot_manager: None,
            metrics_manager: None,
            vlog_store: None,
            schema_manager: None,
            active_memtable_incremental_snapshot_ratio: 0.0,
            db_lifecycle: None,
        }
    }
}

pub(crate) struct MemtableManager {
    state: Arc<Mutex<MemtableManagerState>>,
    buffer_ready: Arc<Condvar>,
    flush_done: Arc<Condvar>,
    file_manager: Arc<FileManager>,
    file_builder_factory: Arc<FileBuilderFactory>,
    writer_options: WriterOptions,
    lsm_tree: Arc<LSMTree>,
    db_state: Arc<DbStateHandle>,
    db_lifecycle: Arc<DbLifecycle>,
    vlog_store: Arc<VlogStore>,
    schema_manager: Arc<SchemaManager>,
    memtable_capacity: usize,
    total_budget: i64,
    memtable_type: MemtableType,
    reclaimer: MemtableReclaimer,
    write_stall_limit: usize,
    flush_tx: Mutex<Option<mpsc::Sender<FlushJob>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    auto_snapshot_manager: Option<SnapshotManager>,
    active_memtable_incremental_snapshot_ratio: f64,
    metrics: MemtableManagerMetrics,
}

struct MemtableManagerState {
    budget: i64,
    allow_make_active_buffer_in_reclaimer: bool,
    in_flight: usize,
    restore_in_progress: bool,
    flush_results: Vec<Result<MemtableFlushResult>>,
}

struct FlushJob {
    memtable_id: Option<Uuid>,
    memtable: Option<Arc<MemtableImpl>>,
    schema: Option<Arc<Schema>>,
    vlog_recorder: Option<Arc<MemtableVlogRecorder>>,
    snapshot: Option<SnapshotCompletion>,
    active_memtable_snapshot: Option<ActiveMemtableSnapshotJob>,
}

struct ActiveMemtableSnapshotJob {
    active: Arc<Mutex<ActiveMemtable>>,
}

struct ActiveMemtableSnapshotWriteResult {
    active_data: Vec<ActiveMemtableSnapshotData>,
}

pub(crate) struct SnapshotCompletion {
    pub(crate) snapshot_id: u64,
    pub(crate) manager: SnapshotManager,
}

pub(crate) struct ActiveMemtable {
    id: Uuid,
    schema: Arc<Schema>,
    memtable: Option<MemtableImpl>,
    vlog_recorder: Option<MemtableVlogRecorder>,
}

#[derive(Clone, Copy)]
struct ActiveMemtableCheckpoint {
    blob_cursor: usize,
    recorder_checkpoint: Option<u32>,
}

enum MemtableScanSource {
    Active(Arc<Mutex<ActiveMemtable>>),
    Immutable(Arc<MemtableImpl>),
}

struct MemtableScanIterator {
    source: MemtableScanSource,
    seek_target: Option<Bytes>,
    next_offset: usize,
    current_key: Option<Bytes>,
    current_value: Option<Bytes>,
}

impl MemtableScanIterator {
    fn for_active(active: Arc<Mutex<ActiveMemtable>>) -> Self {
        Self {
            source: MemtableScanSource::Active(active),
            seek_target: None,
            next_offset: 0,
            current_key: None,
            current_value: None,
        }
    }

    fn for_immutable(memtable: Arc<MemtableImpl>) -> Self {
        Self {
            source: MemtableScanSource::Immutable(memtable),
            seek_target: None,
            next_offset: 0,
            current_key: None,
            current_value: None,
        }
    }

    fn read_entry_from_iter(
        iter: &mut MemtableKvIter<'_>,
        seek_target: Option<&[u8]>,
        offset: usize,
    ) -> Result<Option<(Bytes, Bytes)>> {
        if let Some(target) = seek_target {
            iter.seek(target)?;
        } else {
            iter.seek_to_first()?;
        }
        for _ in 0..=offset {
            if !iter.next()? {
                return Ok(None);
            }
        }
        iter.current()
    }

    fn read_entry_at_offset(&self, offset: usize) -> Result<Option<(Bytes, Bytes)>> {
        let seek_target = self.seek_target.as_deref();
        match &self.source {
            MemtableScanSource::Active(active) => {
                let active = active.lock().unwrap();
                let Some(memtable) = active.memtable.as_ref() else {
                    return Ok(None);
                };
                let mut iter = memtable.iter();
                Self::read_entry_from_iter(&mut iter, seek_target, offset)
            }
            MemtableScanSource::Immutable(memtable) => {
                let mut iter = memtable.iter();
                Self::read_entry_from_iter(&mut iter, seek_target, offset)
            }
        }
    }

    fn prime_current(&mut self) -> Result<()> {
        if let Some((key, value)) = self.read_entry_at_offset(0)? {
            self.current_key = Some(key);
            self.current_value = Some(value);
            self.next_offset = 1;
        } else {
            self.current_key = None;
            self.current_value = None;
            self.next_offset = 0;
        }
        Ok(())
    }
}

impl<'a> KvIterator<'a> for MemtableScanIterator {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.seek_target = Some(Bytes::copy_from_slice(target));
        self.prime_current()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.seek_target = None;
        self.prime_current()
    }

    fn next(&mut self) -> Result<bool> {
        if let Some((key, value)) = self.read_entry_at_offset(self.next_offset)? {
            self.current_key = Some(key);
            self.current_value = Some(value);
            self.next_offset += 1;
            Ok(true)
        } else {
            self.current_key = None;
            self.current_value = None;
            Ok(false)
        }
    }

    fn valid(&self) -> bool {
        self.current_key.is_some() && self.current_value.is_some()
    }

    fn key(&self) -> Result<Option<Bytes>> {
        Ok(self.current_key.clone())
    }

    fn key_slice(&self) -> Result<Option<&[u8]>> {
        Ok(self.current_key.as_deref())
    }

    fn value(&self) -> Result<Option<Bytes>> {
        Ok(self.current_value.clone())
    }

    fn value_slice(&self) -> Result<Option<&[u8]>> {
        Ok(self.current_value.as_deref())
    }
}

#[derive(Clone)]
pub(crate) struct ImmutableMemtable {
    pub(crate) id: Uuid,
    schema: Arc<Schema>,
    memtable: Arc<MemtableImpl>,
    vlog_recorder: Option<Arc<MemtableVlogRecorder>>,
}

impl ActiveMemtable {
    fn checkpoint(&self) -> ActiveMemtableCheckpoint {
        let memtable = self.memtable.as_ref().expect("active memtable exists");
        ActiveMemtableCheckpoint {
            blob_cursor: memtable.blob_cursor_checkpoint(),
            recorder_checkpoint: self
                .vlog_recorder
                .as_ref()
                .map(MemtableVlogRecorder::checkpoint),
        }
    }

    fn restore_checkpoint(&mut self, checkpoint: ActiveMemtableCheckpoint) {
        let memtable = self.memtable.as_mut().expect("active memtable exists");
        memtable.rollback_blob_cursor(checkpoint.blob_cursor);
        if let Some(recorder_checkpoint) = checkpoint.recorder_checkpoint {
            if let Some(recorder) = self.vlog_recorder.as_mut() {
                recorder.rollback(recorder_checkpoint);
            }
        } else {
            self.vlog_recorder = None;
        }
    }
}

impl MemtableManager {
    pub(crate) fn new(
        file_manager: Arc<FileManager>,
        lsm_tree: Arc<LSMTree>,
        options: MemtableManagerOptions,
    ) -> Result<Self> {
        let mut options = options;
        if options.buffer_count == 0 {
            return Err(Error::IoError(
                "buffer_count must be greater than 0".to_string(),
            ));
        }
        let total_budget =
            (options.buffer_count as u64).saturating_mul(options.memtable_capacity as u64);
        let total_budget = total_budget.min(i64::MAX as u64) as i64;
        let state = MemtableManagerState {
            budget: total_budget,
            allow_make_active_buffer_in_reclaimer: true,
            in_flight: 0,
            restore_in_progress: false,
            flush_results: Vec::new(),
        };
        let state = Arc::new(Mutex::new(state));
        let buffer_ready = Arc::new(Condvar::new());
        let flush_done = Arc::new(Condvar::new());
        let db_state = lsm_tree.db_state();
        if let Some(manager) = &options.metrics_manager
            && let WriterOptions::Sst(sst_options) = &mut options.writer_options
            && sst_options.metrics.is_none()
        {
            sst_options.metrics = Some(manager.sst_writer_metrics(sst_options.compression));
        }
        let metrics = options
            .metrics_manager
            .as_ref()
            .map(|manager| manager.memtable_metrics())
            .unwrap_or_else(|| MemtableManagerMetrics::new("unknown"));
        let schema_manager = options
            .schema_manager
            .unwrap_or_else(|| Arc::new(SchemaManager::new(options.num_columns)));
        let db_lifecycle = options
            .db_lifecycle
            .unwrap_or_else(|| Arc::new(DbLifecycle::new_open()));
        let reclaimer = Self::make_reclaimer(
            Arc::clone(&state),
            Arc::clone(&buffer_ready),
            Arc::clone(&db_state),
            Arc::clone(&schema_manager),
            options.memtable_capacity,
            options.memtable_type,
        );
        let file_builder_factory = options.file_builder_factory.unwrap_or_else(|| {
            Arc::new(make_data_file_builder_factory(
                options.writer_options.clone(),
            ))
        });
        let vlog_store = options.vlog_store.unwrap_or_else(|| {
            Arc::new(VlogStore::new(
                Arc::clone(&file_manager),
                usize::MAX,
                usize::MAX,
            ))
        });
        // Initialize the flush worker
        let (worker, flush_tx) = Self::init_flush_worker(
            Arc::clone(&state),
            Arc::clone(&flush_done),
            Arc::clone(&file_manager),
            Arc::clone(&file_builder_factory),
            options.writer_options.clone(),
            Arc::clone(&lsm_tree),
            lsm_tree.ttl_provider(),
            Arc::clone(&db_lifecycle),
            Arc::clone(&vlog_store),
            metrics.clone(),
        )?;
        Ok(Self {
            state,
            buffer_ready,
            flush_done,
            file_manager,
            file_builder_factory,
            writer_options: options.writer_options,
            lsm_tree,
            db_state,
            db_lifecycle,
            vlog_store,
            schema_manager,
            memtable_capacity: options.memtable_capacity,
            total_budget,
            memtable_type: options.memtable_type,
            reclaimer,
            write_stall_limit: options.write_stall_limit,
            flush_tx: Mutex::new(Some(flush_tx)),
            worker: Mutex::new(Some(worker)),
            auto_snapshot_manager: options.auto_snapshot_manager,
            active_memtable_incremental_snapshot_ratio: options
                .active_memtable_incremental_snapshot_ratio
                .clamp(0.0, 1.0),
            metrics,
        })
    }

    pub(crate) fn db_state(&self) -> Arc<DbStateHandle> {
        Arc::clone(&self.db_state)
    }

    pub(crate) fn open(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        Self::make_active_buffer(
            &mut state,
            &self.db_state,
            &self.schema_manager,
            self.memtable_capacity,
            self.memtable_type,
            &self.reclaimer,
            self.total_budget,
            false,
        );
        if self.db_state.load().active.is_none() {
            return Err(InvalidState(
                "failed to allocate initial active memtable".to_string(),
            ));
        }
        self.buffer_ready.notify_all();
        Ok(())
    }

    /// Initializes the flush worker and returns the worker handle and flush sender.
    #[allow(clippy::too_many_arguments)]
    fn init_flush_worker(
        state: Arc<Mutex<MemtableManagerState>>,
        flush_done: Arc<Condvar>,
        file_manager: Arc<FileManager>,
        file_builder_factory: Arc<FileBuilderFactory>,
        writer_options: WriterOptions,
        lsm_tree: Arc<LSMTree>,
        ttl_provider: Arc<crate::ttl::TTLProvider>,
        db_lifecycle: Arc<DbLifecycle>,
        vlog_store: Arc<VlogStore>,
        metrics: MemtableManagerMetrics,
    ) -> Result<(JoinHandle<()>, mpsc::Sender<FlushJob>)> {
        let (flush_tx, flush_rx) = mpsc::channel::<FlushJob>();
        let state_clone = Arc::clone(&state);
        let flush_done_clone = Arc::clone(&flush_done);
        let file_manager_clone = Arc::clone(&file_manager);
        let file_builder_factory_clone = Arc::clone(&file_builder_factory);
        let writer_options_clone = writer_options.clone();
        let lsm_tree_clone = Arc::clone(&lsm_tree);
        let db_state_clone = lsm_tree_clone.db_state();
        let ttl_provider_clone = Arc::clone(&ttl_provider);
        let vlog_store_clone = Arc::clone(&vlog_store);
        let handle = std::thread::Builder::new()
            .name("cobble-flush".to_string())
            .spawn(move || {
                while let Ok(job) = flush_rx.recv() {
                    if db_lifecycle.ensure_open().is_err() {
                        let mut state = state_clone.lock().unwrap();
                        state.in_flight = state.in_flight.saturating_sub(1);
                        flush_done_clone.notify_all();
                        continue;
                    }
                    if let Some(memtable) = job.memtable {
                        trace!(
                            "memtable flush start memtable_id={}",
                            job.memtable_id
                                .as_ref()
                                .map(Uuid::to_string)
                                .unwrap_or_else(|| "none".to_string())
                        );
                        let keep_memtable_alive = Arc::clone(&memtable);
                        let multi_lsm_version = db_state_clone.load().multi_lsm_version.clone();
                        let result = flush_memtable(
                            memtable.as_ref(),
                            job.schema.expect("flush job schema exists"),
                            job.vlog_recorder,
                            Arc::clone(&file_manager_clone),
                            Arc::clone(&file_builder_factory_clone),
                            Arc::clone(&ttl_provider_clone),
                            Arc::clone(&vlog_store_clone),
                            multi_lsm_version,
                            0,
                            writer_options_clone.data_file_type(),
                        );
                        let mut state = state_clone.lock().unwrap();
                        state.in_flight = state.in_flight.saturating_sub(1);
                        match result {
                            Ok(res) => {
                                let memtable_id =
                                    job.memtable_id.expect("flush job memtable_id exists");
                                debug!(
                                    "memtable flush complete memtable_id={} files={}",
                                    memtable_id,
                                    res.data_files_by_tree.len()
                                );
                                metrics.flushes_total.increment(1);
                                let flushed_bytes: u64 = res
                                    .data_files_by_tree
                                    .iter()
                                    .map(|(_, file)| file.size as u64)
                                    .sum();
                                metrics.flush_bytes_total.increment(flushed_bytes);
                                let vlog_edit = res.vlog_edit.clone();
                                let snapshot = match lsm_tree_clone.add_level0_files(
                                    memtable_id,
                                    res.data_files_by_tree.clone(),
                                    vlog_edit,
                                ) {
                                    Ok(snapshot) => snapshot,
                                    Err(err) => {
                                        db_lifecycle.mark_error(err.clone());
                                        state.flush_results.push(Err(err));
                                        flush_done_clone.notify_all();
                                        drop(state);
                                        drop(keep_memtable_alive);
                                        continue;
                                    }
                                };
                                state.flush_results.push(Ok(res));
                                flush_done_clone.notify_all();
                                drop(state);
                                Self::finish_and_materialize_snapshot(
                                    &job.snapshot,
                                    &snapshot,
                                    Vec::new(),
                                    &db_state_clone,
                                );
                            }
                            Err(err) => {
                                db_lifecycle.mark_error(err.clone());
                                state.flush_results.push(Err(err));
                                flush_done_clone.notify_all();
                            }
                        }
                        drop(keep_memtable_alive);
                    } else if let Some(active_snapshot_job) = job.active_memtable_snapshot {
                        let snapshot = db_state_clone.load();
                        let snapshot_result = match job.snapshot.as_ref() {
                            Some(snapshot_job) => Self::write_active_memtable_snapshot_data(
                                snapshot_job.snapshot_id,
                                snapshot.suggested_base_snapshot_id,
                                &active_snapshot_job.active,
                                &snapshot_job.manager,
                                &file_manager_clone,
                            ),
                            None => Ok(ActiveMemtableSnapshotWriteResult {
                                active_data: Vec::new(),
                            }),
                        };
                        let snapshot_result = match snapshot_result {
                            Ok(result) => result,
                            Err(err) => {
                                db_lifecycle.mark_error(err.clone());
                                let mut state = state_clone.lock().unwrap();
                                state.in_flight = state.in_flight.saturating_sub(1);
                                flush_done_clone.notify_all();
                                drop(state);
                                continue;
                            }
                        };
                        let ActiveMemtableSnapshotWriteResult { active_data } = snapshot_result;
                        let mut state = state_clone.lock().unwrap();
                        state.in_flight = state.in_flight.saturating_sub(1);
                        flush_done_clone.notify_all();
                        drop(state);
                        Self::finish_and_materialize_snapshot(
                            &job.snapshot,
                            &snapshot,
                            active_data,
                            &db_state_clone,
                        );
                    } else {
                        let snapshot = db_state_clone.load();
                        let mut state = state_clone.lock().unwrap();
                        state.in_flight = state.in_flight.saturating_sub(1);
                        flush_done_clone.notify_all();
                        drop(state);
                        Self::finish_and_materialize_snapshot(
                            &job.snapshot,
                            &snapshot,
                            Vec::new(),
                            &db_state_clone,
                        );
                    }
                }
            })
            .map_err(|e| Error::IoError(format!("Failed to start flush worker: {}", e)))?;
        Ok((handle, flush_tx))
    }

    fn finish_and_materialize_snapshot(
        snapshot_completion: &Option<SnapshotCompletion>,
        snapshot: &Arc<DbState>,
        active_data: Vec<ActiveMemtableSnapshotData>,
        db_state: &Arc<DbStateHandle>,
    ) {
        if let Some(snapshot_job) = snapshot_completion {
            snapshot_job.manager.finish_snapshot(
                snapshot_job.snapshot_id,
                snapshot,
                active_data,
                db_state.as_ref(),
            );
            let _ = snapshot_job
                .manager
                .schedule_materialize(snapshot_job.snapshot_id);
            let _ = snapshot_job.manager.process_retention();
        }
    }

    fn write_active_memtable_snapshot_data(
        snapshot_id: u64,
        base_snapshot_id: Option<u64>,
        active: &Arc<Mutex<ActiveMemtable>>,
        snapshot_manager: &SnapshotManager,
        file_manager: &Arc<FileManager>,
    ) -> Result<ActiveMemtableSnapshotWriteResult> {
        let active_guard = active.lock().unwrap();
        let memtable_id = active_guard.id.to_string();
        let memtable = active_guard
            .memtable
            .as_ref()
            .ok_or_else(|| InvalidState("Active memtable missing".to_string()))?;
        let memtable_type = match memtable {
            MemtableImpl::Hash(_) => MemtableType::Hash,
            MemtableImpl::Vec(_) => MemtableType::Vec,
        };
        let mut segments = snapshot_manager.active_memtable_snapshot_segments(
            base_snapshot_id,
            memtable_type,
            &memtable_id,
        );
        let data_end_offset = memtable.data_offset();
        let inherited_data_end = segments.last().map_or(0, |segment| segment.end_offset);
        let inherited_data_end = usize::try_from(inherited_data_end).unwrap_or(usize::MAX);
        let data_start_offset = if inherited_data_end <= data_end_offset {
            inherited_data_end
        } else {
            segments.clear();
            0
        };
        let mut vlog_file_seq = None;
        let mut vlog_start_offset = 0u32;
        let mut vlog_end_offset = 0u32;
        if let Some(recorder) = active_guard.vlog_recorder.as_ref()
            && recorder.has_entries()
        {
            vlog_file_seq = Some(recorder.file_seq());
            vlog_end_offset = recorder.checkpoint();
            vlog_start_offset = segments
                .iter()
                .rev()
                .find_map(|segment| {
                    (segment.vlog_file_seq == vlog_file_seq).then_some(segment.vlog_end_offset)
                })
                .unwrap_or(0);
            if vlog_start_offset > vlog_end_offset {
                vlog_start_offset = 0;
            }
        }
        if data_start_offset < data_end_offset || vlog_start_offset < vlog_end_offset {
            let path = snapshot_active_data_relative_path(snapshot_id);
            let mut writer = file_manager.create_metadata_file(&path)?;
            let mut data_bytes_written = 0usize;
            if data_start_offset < data_end_offset {
                data_bytes_written = memtable.write_data_since(data_start_offset, &mut writer)?;
            }
            if vlog_start_offset < vlog_end_offset {
                let recorder = active_guard
                    .vlog_recorder
                    .as_ref()
                    .expect("vlog recorder exists when offsets advance");
                let _ = recorder.write_data_since(memtable, vlog_start_offset, &mut writer)?;
            }
            writer.close()?;
            segments.push(ActiveMemtableSnapshotData {
                path,
                memtable_type,
                memtable_id: memtable_id.clone(),
                start_offset: data_start_offset as u64,
                end_offset: data_end_offset as u64,
                vlog_file_seq,
                vlog_start_offset,
                vlog_end_offset,
                vlog_data_file_offset: data_bytes_written as u64,
            });
        }
        Ok(ActiveMemtableSnapshotWriteResult {
            active_data: segments,
        })
    }

    /// Makes an active memtable from a free buffer if none exists.
    /// Assumes the caller holds the lock on the state.
    #[allow(clippy::too_many_arguments)]
    fn make_active_buffer(
        state: &mut MemtableManagerState,
        db_state: &Arc<DbStateHandle>,
        schema_manager: &Arc<SchemaManager>,
        memtable_capacity: usize,
        memtable_type: MemtableType,
        reclaimer: &MemtableReclaimer,
        total_budget: i64,
        allow_overcommit: bool,
    ) {
        let _guard = db_state.lock();
        let snapshot = db_state.load();
        let budget_need = (memtable_capacity as u64).min(i64::MAX as u64) as i64;
        if snapshot.active.is_none() {
            let has_budget = state.budget >= budget_need;
            let can_overcommit =
                allow_overcommit && budget_need > total_budget && state.in_flight == 0;
            if !has_budget && !can_overcommit {
                return;
            }
            state.budget -= budget_need;
            let memtable_id = Uuid::new_v4();
            let schema = schema_manager.latest_schema();
            let memtable = match memtable_type {
                MemtableType::Hash => {
                    let buffer = vec![0u8; memtable_capacity];
                    MemtableImpl::Hash(HashMemtable::with_buffer_and_reclaimer(
                        buffer,
                        reclaimer.clone(),
                    ))
                }
                MemtableType::Vec => MemtableImpl::Vec(VecMemtable::with_capacity_and_reclaimer(
                    memtable_capacity,
                    reclaimer.clone(),
                )),
            };
            let active = Arc::new(Mutex::new(ActiveMemtable {
                id: memtable_id,
                schema,
                memtable: Some(memtable),
                vlog_recorder: None,
            }));
            db_state.cas_mutate(snapshot.seq_id, |db_state, snapshot| {
                Some(DbState {
                    seq_id: db_state.allocate_seq_id(),
                    bucket_ranges: snapshot.bucket_ranges.clone(),
                    multi_lsm_version: snapshot.multi_lsm_version.clone(),
                    vlog_version: snapshot.vlog_version.clone(),
                    active: Some(Arc::clone(&active)),
                    immutables: snapshot.immutables.clone(),
                    suggested_base_snapshot_id: snapshot.suggested_base_snapshot_id,
                })
            });
        }
    }

    /// Creates a memtable reclaimer closure.
    /// The reclaimer adds the returned buffer size back to the free budget
    /// and attempts to create a new active memtable if none exists.
    fn make_reclaimer(
        state: Arc<Mutex<MemtableManagerState>>,
        buffer_ready: Arc<Condvar>,
        db_state: Arc<DbStateHandle>,
        schema_manager: Arc<SchemaManager>,
        memtable_capacity: usize,
        memtable_type: MemtableType,
    ) -> MemtableReclaimer {
        let state = Arc::downgrade(&state);
        let buffer_ready = Arc::downgrade(&buffer_ready);
        let db_state = Arc::downgrade(&db_state);
        let schema_manager = Arc::downgrade(&schema_manager);
        Arc::new(move |returned| {
            // upgrade weak references
            let Some(state) = state.upgrade() else {
                return;
            };
            let Some(buffer_ready) = buffer_ready.upgrade() else {
                return;
            };
            let Some(db_state) = db_state.upgrade() else {
                return;
            };
            let Some(schema_manager) = schema_manager.upgrade() else {
                return;
            };
            let mut guard = state.lock().unwrap();
            let returned_budget = returned.min(i64::MAX as u64) as i64;
            guard.budget = guard.budget.saturating_add(returned_budget);
            // reuse the same reclaimer since it captures nothing by reference
            let reclaimer = Self::make_reclaimer(
                Arc::clone(&state),
                Arc::clone(&buffer_ready),
                Arc::clone(&db_state),
                Arc::clone(&schema_manager),
                memtable_capacity,
                memtable_type,
            );
            // try to make a new active buffer
            if guard.allow_make_active_buffer_in_reclaimer {
                Self::make_active_buffer(
                    &mut guard,
                    &db_state,
                    &schema_manager,
                    memtable_capacity,
                    memtable_type,
                    &reclaimer,
                    (memtable_capacity as u64).min(i64::MAX as u64) as i64,
                    false,
                );
            }
            // notify waiters
            buffer_ready.notify_all();
        })
    }

    /// Puts a key-value pair into the active memtable using reference types to avoid extra copy.
    pub(crate) fn put(&self, key: &RefKey<'_>, value: &RefValue<'_>) -> Result<()> {
        loop {
            // Wait for an active memtable to be available.
            if self.db_state.load().active.is_none() {
                let mut state = self.state.lock().unwrap();
                while self.db_state.load().active.is_none() {
                    state = self.buffer_ready.wait(state).unwrap();
                }
                drop(state);
            }
            let active = self
                .db_state
                .load()
                .active
                .clone()
                .expect("active memtable exists");
            let mut active = active.lock().unwrap();
            let latest_schema = self.schema_manager.latest_schema();
            if active.schema.version() != latest_schema.version() {
                let is_empty = active
                    .memtable
                    .as_ref()
                    .map(|memtable| memtable.is_empty())
                    .unwrap_or(true);
                if is_empty {
                    active.schema = latest_schema;
                } else {
                    drop(active);
                    let _ = self.flush_active()?;
                    continue;
                }
            }
            let num_columns = active.schema.num_columns();
            if active.memtable.is_none() {
                drop(active);
                continue;
            }
            // Steps:
            // 1) checkpoint active memtable state (blob arena + vlog recorder offsets)
            // 2) rewrite large columns to vlog pointers
            // 3) stream the final encoded value into memtable storage
            // 4) restore checkpoint on any error/full condition
            let checkpoint = active.checkpoint();
            let rewrite_result = {
                let ActiveMemtable {
                    memtable,
                    vlog_recorder,
                    ..
                } = &mut *active;
                let memtable = memtable.as_mut().expect("active memtable exists");
                rewrite_ref_value_for_memtable(
                    value,
                    &self.vlog_store,
                    memtable,
                    vlog_recorder,
                    num_columns,
                )
            };
            let rewrite_plan = match rewrite_result {
                Ok(rewritten) => rewritten,
                Err(err) => {
                    self.handle_memtable_put_error(&err, active, checkpoint, key, value)?;
                    continue;
                }
            };
            let put_result = {
                let memtable = active.memtable.as_mut().expect("active memtable exists");
                if let Some(plan) = rewrite_plan.as_ref() {
                    memtable.put_ref_rewritten(key, plan, num_columns)
                } else {
                    memtable.put_ref(key, value, num_columns)
                }
            };
            match put_result {
                Ok(()) => return Ok(()),
                Err(err) => {
                    self.handle_memtable_put_error(&err, active, checkpoint, key, value)?;
                }
            }
        }
    }

    fn allocate_one_key_value_special_vec_memtable_as_active(
        &self,
        key: &RefKey<'_>,
        value: &RefValue<'_>,
    ) -> Result<bool> {
        let num_columns = self.schema_manager.current_num_columns();
        let capacity = VecMemtable::estimate_capacity_for_ref(
            key,
            value,
            num_columns,
            self.vlog_store.as_ref(),
        );
        {
            // Disallow other reclaimer calls to make active buffer
            // while we are trying to allocate a special memtable.
            let mut state = self.state.lock().unwrap();
            state.allow_make_active_buffer_in_reclaimer = false;
        }
        {
            // Release the current active memtable if exists to free up budget.
            let _guard = self.db_state.lock();
            let snapshot = self.db_state.load();
            let Some(active) = snapshot.active.as_ref() else {
                let mut state = self.state.lock().unwrap();
                state.allow_make_active_buffer_in_reclaimer = true;
                return Ok(false);
            };
            let mut active = active.lock().unwrap();
            let old_memtable = active.memtable.take();
            active.vlog_recorder = None;
            drop(active);
            self.db_state
                .cas_mutate(snapshot.seq_id, |db_state, snapshot| {
                    Some(DbState {
                        seq_id: db_state.allocate_seq_id(),
                        bucket_ranges: snapshot.bucket_ranges.clone(),
                        multi_lsm_version: snapshot.multi_lsm_version.clone(),
                        vlog_version: snapshot.vlog_version.clone(),
                        active: None,
                        immutables: snapshot.immutables.clone(),
                        suggested_base_snapshot_id: snapshot.suggested_base_snapshot_id,
                    })
                });
            drop(old_memtable);
        }
        // Try to make a new active buffer with just enough capacity for the single key-value pair.
        let mut state = self.state.lock().unwrap();
        loop {
            Self::make_active_buffer(
                &mut state,
                &self.db_state,
                &self.schema_manager,
                capacity,
                MemtableType::Vec,
                &self.reclaimer,
                self.total_budget,
                true,
            );
            if self.db_state.load().active.is_some() {
                break;
            }
            if state.in_flight == 0 {
                state.allow_make_active_buffer_in_reclaimer = true;
                return Err(InvalidState(
                    "failed to allocate special active vec memtable".to_string(),
                ));
            }
            state = self.buffer_ready.wait(state).unwrap();
        }
        // Allow reclaimer to make active buffer again for future flushes.
        state.allow_make_active_buffer_in_reclaimer = true;
        drop(state);
        Ok(true)
    }

    fn handle_memtable_put_error(
        &self,
        err: &Error,
        mut active: MutexGuard<ActiveMemtable>,
        checkpoint: ActiveMemtableCheckpoint,
        key: &RefKey<'_>,
        value: &RefValue<'_>,
    ) -> Result<()> {
        match err {
            Error::MemtableFull {
                needed: _needed,
                remaining: _remaining,
            } => {
                active.restore_checkpoint(checkpoint);
                if active
                    .memtable
                    .as_ref()
                    .expect("active memtable exists")
                    .is_empty()
                {
                    drop(active);
                    self.allocate_one_key_value_special_vec_memtable_as_active(key, value)?;
                } else {
                    // flush active memtable and retry
                    drop(active);
                    self.flush_active()?;
                }
            }
            _ => {
                active.restore_checkpoint(checkpoint);
                return Err(err.clone());
            }
        }
        Ok(())
    }

    /// Gets all values associated with the given key.
    /// The provided closure `f` is called for each value. This allows
    /// processing values without returning byte copy.
    ///
    pub(crate) fn get_all<F>(&self, key: &[u8], f: F) -> Result<()>
    where
        F: FnMut(&[u8], &Schema) -> Result<()>,
    {
        let snapshot = self.db_state.load();
        self.get_all_with_snapshot(snapshot, key, f)
    }

    /// Same as [`get_all`] but uses the provided `DbState` snapshot to ensure a consistent view.
    pub(crate) fn get_all_with_snapshot<F>(
        &self,
        snapshot: Arc<DbState>,
        key: &[u8],
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &Schema) -> Result<()>,
    {
        if let Some(active) = &snapshot.active {
            let active = active.lock().unwrap();
            let memtable = active.memtable.as_ref().expect("active memtable exists");
            for value in memtable.get_all(key) {
                f(value, active.schema.as_ref())?;
            }
            drop(active);
        }
        for immutable in snapshot.immutables.iter().rev() {
            for value in immutable.memtable.get_all(key) {
                f(value, immutable.schema.as_ref())?;
            }
        }
        Ok(())
    }

    pub(crate) fn scan_memtable_iterators_with_snapshot(
        &self,
        snapshot: Arc<DbState>,
        target_schema: Arc<Schema>,
    ) -> Result<Vec<DynKvIterator>> {
        let mut iterators: Vec<DynKvIterator> = Vec::new();
        if let Some(active) = &snapshot.active {
            let source_schema = {
                let active_guard = active.lock().unwrap();
                Arc::clone(&active_guard.schema)
            };
            let iter = MemtableScanIterator::for_active(Arc::clone(active));
            let iter: DynKvIterator = if source_schema.version() == target_schema.version() {
                Box::new(iter)
            } else {
                Box::new(SchemaEvolvingIterator::new(
                    iter,
                    Arc::clone(&source_schema),
                    Arc::clone(&target_schema),
                    Arc::clone(&self.schema_manager),
                ))
            };
            iterators.push(iter);
        }
        for immutable in snapshot.immutables.iter().rev() {
            let iter = MemtableScanIterator::for_immutable(Arc::clone(&immutable.memtable));
            let iter: DynKvIterator = if immutable.schema.version() == target_schema.version() {
                Box::new(iter)
            } else {
                Box::new(SchemaEvolvingIterator::new(
                    iter,
                    Arc::clone(&immutable.schema),
                    Arc::clone(&target_schema),
                    Arc::clone(&self.schema_manager),
                ))
            };
            iterators.push(iter);
        }
        Ok(iterators)
    }

    pub(crate) fn read_vlog_pointer_with_snapshot(
        &self,
        snapshot: Arc<DbState>,
        pointer: VlogPointer,
    ) -> Result<Option<Bytes>> {
        if let Some(active) = &snapshot.active {
            let active = active.lock().unwrap();
            if let (Some(memtable), Some(recorder)) =
                (active.memtable.as_ref(), active.vlog_recorder.as_ref())
                && let Some(value) = recorder.read_pointer(memtable, pointer)?
            {
                return Ok(Some(value));
            }
        }
        for immutable in snapshot.immutables.iter().rev() {
            if let Some(recorder) = immutable.vlog_recorder.as_ref()
                && let Some(value) = recorder.read_pointer(immutable.memtable.as_ref(), pointer)?
            {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    pub(crate) fn flush_active(&self) -> Result<Option<Uuid>> {
        let auto_snapshot = self
            .auto_snapshot_manager
            .as_ref()
            .map(|manager| SnapshotCompletion {
                snapshot_id: manager.create_snapshot(None).id,
                manager: manager.clone(),
            });
        self.flush_active_internal(auto_snapshot)
    }

    pub(crate) fn flush_snapshot(&self, snapshot_id: u64, manager: SnapshotManager) -> Result<()> {
        let snapshot = SnapshotCompletion {
            snapshot_id,
            manager,
        };
        let _ = self.flush_active_internal(Some(snapshot))?;
        Ok(())
    }

    fn flush_active_internal(&self, snapshot: Option<SnapshotCompletion>) -> Result<Option<Uuid>> {
        self.db_lifecycle.ensure_open()?;
        let mut state = self.state.lock().unwrap();
        while state.restore_in_progress {
            state = self.flush_done.wait(state).unwrap();
        }
        let mut guard = self.db_state.lock();
        guard = self.wait_for_write_stall_under_guard(guard);
        let snapshot_state = self.db_state.load();
        let mut to_flush = None;
        let mut active_memtable_snapshot = None;
        let mut flushed_id = None;
        if snapshot.is_some()
            && let Some(active) = snapshot_state.active.as_ref()
        {
            let inner_active = active.lock().unwrap();
            if let Some(memtable) = inner_active.memtable.as_ref()
                && !memtable.is_empty()
                && self.should_use_active_incremental_snapshot(memtable)
            {
                active_memtable_snapshot = Some(ActiveMemtableSnapshotJob {
                    active: Arc::clone(active),
                });
            }
        }
        if active_memtable_snapshot.is_none() {
            self.db_state
                .cas_mutate(snapshot_state.seq_id, |db_state, snapshot_state| {
                    let active = snapshot_state.active.clone()?;
                    let mut inner_active = active.lock().unwrap();
                    let memtable = match inner_active.memtable.as_ref() {
                        Some(memtable) => memtable,
                        None => {
                            return None;
                        }
                    };
                    if memtable.is_empty() {
                        return None;
                    }
                    let active_memtable = inner_active
                        .memtable
                        .take()
                        .expect("active memtable exists");
                    let active_vlog_recorder = inner_active.vlog_recorder.take().map(Arc::new);
                    let mut immutables = snapshot_state.immutables.clone();
                    let new_immutable = ImmutableMemtable {
                        id: inner_active.id,
                        schema: Arc::clone(&inner_active.schema),
                        memtable: Arc::new(active_memtable),
                        vlog_recorder: active_vlog_recorder,
                    };
                    to_flush = Some(new_immutable.clone());
                    immutables.push_back(new_immutable);

                    Some(DbState {
                        seq_id: db_state.allocate_seq_id(),
                        bucket_ranges: snapshot_state.bucket_ranges.clone(),
                        multi_lsm_version: snapshot_state.multi_lsm_version.clone(),
                        vlog_version: snapshot_state.vlog_version.clone(),
                        active: None,
                        immutables,
                        suggested_base_snapshot_id: snapshot_state.suggested_base_snapshot_id,
                    })
                });
        }
        drop(guard);
        let job = if let Some(to_flush) = to_flush {
            Self::make_active_buffer(
                &mut state,
                &self.db_state,
                &self.schema_manager,
                self.memtable_capacity,
                self.memtable_type,
                &self.reclaimer,
                self.total_budget,
                false,
            );
            flushed_id = Some(to_flush.id);
            state.in_flight += 1;
            FlushJob {
                memtable_id: Some(to_flush.id),
                memtable: Some(to_flush.memtable),
                schema: Some(to_flush.schema),
                vlog_recorder: to_flush.vlog_recorder,
                snapshot,
                active_memtable_snapshot: None,
            }
        } else if let Some(active_memtable_snapshot) = active_memtable_snapshot {
            state.in_flight += 1;
            FlushJob {
                memtable_id: None,
                memtable: None,
                schema: None,
                vlog_recorder: None,
                snapshot,
                active_memtable_snapshot: Some(active_memtable_snapshot),
            }
        } else if snapshot.is_some() {
            state.in_flight += 1;
            FlushJob {
                memtable_id: None,
                memtable: None,
                schema: None,
                vlog_recorder: None,
                snapshot,
                active_memtable_snapshot: None,
            }
        } else {
            drop(state);
            return Ok(flushed_id);
        };
        drop(state);
        let sender = self.flush_tx.lock().unwrap();
        if let Some(sender) = sender.as_ref() {
            if sender.send(job).is_err() {
                panic!("failed to spawn flush task: {:?}", sender);
            }
        } else {
            warn!("failed to spawn flush task, flush channel closed");
        }
        Ok(flushed_id)
    }

    fn should_use_active_incremental_snapshot(&self, memtable: &MemtableImpl) -> bool {
        if self.active_memtable_incremental_snapshot_ratio <= 0.0 || self.memtable_capacity == 0 {
            return false;
        }
        let used = self
            .memtable_capacity
            .saturating_sub(memtable.remaining_capacity());
        let usage_ratio = (used as f64) / (self.memtable_capacity as f64);
        usage_ratio < self.active_memtable_incremental_snapshot_ratio
    }

    #[cfg(test)]
    pub(crate) fn wait_for_flushes(&self) -> Vec<Result<MemtableFlushResult>> {
        let mut state = self.state.lock().unwrap();
        while state.in_flight > 0 {
            state = self.flush_done.wait(state).unwrap();
        }
        std::mem::take(&mut state.flush_results)
    }

    pub(crate) fn restore_active_memtable_snapshot_to_l0(
        &self,
        source_file_manager: &Arc<FileManager>,
        segments: &[ActiveMemtableSnapshotData],
        vlog_file_seq_offset: u32,
    ) -> Result<bool> {
        if segments.is_empty() {
            return Ok(false);
        }
        {
            let mut state = self.state.lock().unwrap();
            while state.restore_in_progress || state.in_flight > 0 {
                state = self.flush_done.wait(state).unwrap();
            }
            state.restore_in_progress = true;
        }
        let result = (|| {
            let (memtable, vlog_recorder) =
                decode_active_snapshot_segments_into_vec_memtable(source_file_manager, segments)?;
            if memtable.is_empty() {
                return Ok(false);
            }
            let multi_lsm_version = self.db_state.load().multi_lsm_version.clone();
            let result = flush_memtable(
                &memtable,
                self.schema_manager.latest_schema(),
                vlog_recorder.map(Arc::new),
                Arc::clone(&self.file_manager),
                Arc::clone(&self.file_builder_factory),
                self.lsm_tree.ttl_provider(),
                Arc::clone(&self.vlog_store),
                multi_lsm_version,
                vlog_file_seq_offset,
                self.writer_options.data_file_type(),
            )?;
            self.metrics.flushes_total.increment(1);
            let flushed_bytes: u64 = result
                .data_files_by_tree
                .iter()
                .map(|(_, file)| file.size as u64)
                .sum();
            self.metrics.flush_bytes_total.increment(flushed_bytes);
            self.lsm_tree.add_level0_files(
                Uuid::new_v4(),
                result.data_files_by_tree.clone(),
                result.vlog_edit,
            )?;
            Ok(true)
        })();
        let mut state = self.state.lock().unwrap();
        state.restore_in_progress = false;
        self.flush_done.notify_all();
        drop(state);
        result
    }

    pub(crate) fn close(&self) -> Result<()> {
        {
            let mut tx = self.flush_tx.lock().unwrap();
            tx.take();
        }
        let worker = self.worker.lock().unwrap().take();
        if let Some(worker) = worker {
            let _ = worker.join();
        }
        Ok(())
    }

    fn wait_for_write_stall_under_guard<'a>(
        &self,
        mut guard: std::sync::MutexGuard<'a, ()>,
    ) -> std::sync::MutexGuard<'a, ()> {
        while Self::should_write_stall_with_snapshot(&self.db_state.load(), self.write_stall_limit)
        {
            self.metrics.write_stall_waits_total.increment(1);
            guard = self.db_state.wait_for_change(guard);
        }
        guard
    }

    fn should_write_stall_with_snapshot(snapshot: &DbState, write_stall_limit: usize) -> bool {
        let immutables = snapshot.immutables.len();
        let max_level0 = (0..snapshot.multi_lsm_version.tree_count())
            .map(|tree_idx| {
                snapshot
                    .multi_lsm_version
                    .version_of_index(tree_idx)
                    .levels
                    .iter()
                    .find(|level| level.ordinal == 0)
                    .map(|level| level.files.len())
                    .unwrap_or(0)
            })
            .max()
            .unwrap_or(0);
        immutables + max_level0 > write_stall_limit
    }
}

impl Drop for MemtableManager {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Flush a sealed memtable to L0 SST files.
///
/// Flush path: (1) write separated blob payloads to VLOG if the memtable
/// has a vlog recorder, (2) iterate memtable entries in sorted order,
/// (3) route each key to the correct LSM tree by bucket, (4) write per-tree
/// L0 SST files via FileBuilder, (5) return data files and vlog edit for
/// atomic LSM version update.
#[allow(clippy::too_many_arguments)]
fn flush_memtable(
    memtable: &impl Memtable,
    schema: Arc<Schema>,
    vlog_recorder: Option<Arc<MemtableVlogRecorder>>,
    file_manager: Arc<FileManager>,
    file_builder_factory: Arc<FileBuilderFactory>,
    ttl_provider: Arc<crate::ttl::TTLProvider>,
    vlog_store: Arc<VlogStore>,
    multi_lsm_version: MultiLSMTreeVersion,
    vlog_file_seq_offset: u32,
    data_file_type: DataFileType,
) -> Result<MemtableFlushResult> {
    // Step 1: If there is a vlog recorder with entries, flush it to the vlog store and get the resulting edit.
    let mut vlog_edit = None;
    if let Some(recorder) = vlog_recorder
        && recorder.has_entries()
    {
        let target_file_seq = recorder
            .file_seq()
            .checked_add(vlog_file_seq_offset)
            .ok_or_else(|| {
                Error::IoError(format!(
                    "VLOG file seq overflow: {} + {}",
                    recorder.file_seq(),
                    vlog_file_seq_offset
                ))
            })?;
        let (mut writer, mut edit) = vlog_store.create_writer_for_seq(target_file_seq)?;
        recorder.flush_to_writer(memtable, &mut writer)?;
        edit.add_entry_delta(target_file_seq, recorder.entry_count() as i64);
        writer.close()?;
        vlog_edit = Some(edit);
    }
    // Step 2: Create data files on-demand per tree and write entries by key bucket.
    let mut builders: BTreeMap<usize, (u64, Box<dyn FileBuilder>, u16, u16)> = BTreeMap::new();
    // Try to handle merges during flush if vlog edits are present
    let merge_collector = vlog_edit.as_ref().map(|_| VlogMergeCollector::shared(true));
    let merge_callback = merge_collector.as_ref().map(VlogMergeCollector::callback);
    let num_columns = schema.num_columns();
    let mut dedup_iter = DeduplicatingIterator::new(
        PrimedIterator::new(memtable.iter()),
        num_columns,
        ttl_provider,
        merge_callback,
        Arc::clone(&schema),
    );
    dedup_iter.seek_to_first()?;
    while dedup_iter.valid() {
        if let Some(collector) = merge_collector.as_ref() {
            collector.borrow_mut().check_error()?;
        }
        if let (Some(key), Some(value)) = (dedup_iter.key_slice()?, dedup_iter.value_slice()?) {
            if key.len() < 2 {
                return Err(Error::InvalidState(
                    "encoded key missing bucket prefix".to_string(),
                ));
            }
            let bucket = u16::from_le_bytes([key[0], key[1]]);
            let tree_idx = multi_lsm_version
                .tree_index_for_bucket(bucket)
                .ok_or_else(|| {
                    Error::InvalidState(format!("bucket {} not mapped to an LSM tree", bucket))
                })?;
            if let std::collections::btree_map::Entry::Vacant(entry) = builders.entry(tree_idx) {
                let (file_id, writer) = file_manager.create_data_file_with_offload()?;
                entry.insert((
                    file_id,
                    (file_builder_factory)(Box::new(writer)),
                    bucket,
                    bucket,
                ));
            }
            let (_, builder, min_bucket, max_bucket) = builders
                .get_mut(&tree_idx)
                .expect("builder should exist for tree");
            builder.add(key, value)?;
            *min_bucket = (*min_bucket).min(bucket);
            *max_bucket = (*max_bucket).max(bucket);
        }
        dedup_iter.next()?;
    }
    if let Some(collector) = merge_collector.as_ref() {
        collector.borrow_mut().check_error()?;
    }
    if let (Some(edit), Some(collector)) = (&mut vlog_edit, merge_collector.as_ref()) {
        for (file_seq, delta) in collector.borrow().removed_entry_deltas() {
            let shifted_file_seq = file_seq.checked_add(vlog_file_seq_offset).ok_or_else(|| {
                InvalidState(format!(
                    "VLOG file seq overflow: {} + {}",
                    file_seq, vlog_file_seq_offset
                ))
            })?;
            edit.add_entry_delta(shifted_file_seq, delta);
        }
    }
    // Step 3: Finish data files and construct the resulting `MemtableFlushResult`.
    let has_separated_values = merge_collector
        .as_ref()
        .is_some_and(|collector| collector.borrow().has_separated_values());
    let mut data_files_by_tree = Vec::with_capacity(builders.len());
    for (tree_idx, (file_id, builder, min_bucket, max_bucket)) in builders {
        let (start_key, end_key, file_size, footer_bytes) = builder.finish()?;
        let bucket_range = min_bucket..=max_bucket;
        let data_file = DataFile::new(
            data_file_type,
            start_key,
            end_key,
            file_id,
            TrackedFileId::new(&file_manager, file_id),
            schema.version(),
            file_size,
            bucket_range.clone(),
            bucket_range,
        )
        .with_vlog_offset(vlog_file_seq_offset)
        .with_separated_values(has_separated_values);
        data_file.set_meta_bytes(footer_bytes);
        data_files_by_tree.push((tree_idx, Arc::new(data_file)));
    }
    if data_files_by_tree.is_empty() {
        return Err(Error::InvalidState(
            "flush produced no sst entries".to_string(),
        ));
    }
    Ok(MemtableFlushResult {
        data_files_by_tree,
        vlog_edit,
    })
}

fn decode_active_snapshot_segments_into_vec_memtable(
    file_manager: &Arc<FileManager>,
    segments: &[ActiveMemtableSnapshotData],
) -> Result<(VecMemtable, Option<MemtableVlogRecorder>)> {
    if segments.is_empty() {
        return Ok((VecMemtable::with_capacity(1), None));
    }
    let first = &segments[0];
    let mut expected_data_start = 0u64;
    let mut required_capacity = 0u64;
    for segment in segments {
        if segment.memtable_type != first.memtable_type || segment.memtable_id != first.memtable_id
        {
            return Err(Error::InvalidState(
                "active memtable snapshot segment metadata mismatch".to_string(),
            ));
        }
        if segment.end_offset < segment.start_offset || segment.start_offset != expected_data_start
        {
            return Err(Error::InvalidState(
                "active memtable snapshot data offsets are discontinuous".to_string(),
            ));
        }
        if segment.vlog_end_offset < segment.vlog_start_offset {
            return Err(Error::InvalidState(
                "active memtable snapshot vlog offsets are invalid".to_string(),
            ));
        }
        expected_data_start = segment.end_offset;
        required_capacity = required_capacity
            .checked_add(segment.end_offset - segment.start_offset)
            .and_then(|v| {
                v.checked_add((segment.vlog_end_offset - segment.vlog_start_offset) as u64)
            })
            .ok_or_else(|| {
                Error::IoError("active memtable snapshot capacity overflow".to_string())
            })?;
    }
    let capacity = usize::try_from(required_capacity.max(1))
        .map_err(|_| Error::IoError("active memtable snapshot too large".to_string()))?;
    let mut memtable = VecMemtable::with_capacity(capacity);
    let mut recorder: Option<MemtableVlogRecorder> = None;

    for segment in segments {
        let reader = file_manager.open_metadata_file_reader_untracked(&segment.path)?;
        let bytes = reader.read_at(0, reader.size())?;
        let segment_bytes = bytes.as_ref();
        let vlog_data_file_offset = usize::try_from(segment.vlog_data_file_offset)
            .map_err(|_| Error::InvalidState("invalid vlog_data_file_offset".to_string()))?;
        if vlog_data_file_offset > segment_bytes.len() {
            return Err(Error::InvalidState(format!(
                "vlog_data_file_offset out of range: {} > {}",
                vlog_data_file_offset,
                segment_bytes.len()
            )));
        }
        let kv_bytes = &segment_bytes[..vlog_data_file_offset];
        let vlog_bytes = &segment_bytes[vlog_data_file_offset..];
        let expected_kv_len = usize::try_from(segment.end_offset - segment.start_offset)
            .map_err(|_| Error::IoError("active memtable kv length overflow".to_string()))?;
        if kv_bytes.len() != expected_kv_len {
            return Err(Error::InvalidState(format!(
                "active memtable kv bytes mismatch: {} != {}",
                kv_bytes.len(),
                expected_kv_len
            )));
        }
        let expected_vlog_len =
            usize::try_from((segment.vlog_end_offset - segment.vlog_start_offset) as u64)
                .map_err(|_| Error::IoError("active memtable vlog length overflow".to_string()))?;
        if vlog_bytes.len() != expected_vlog_len {
            return Err(Error::InvalidState(format!(
                "active memtable vlog bytes mismatch: {} != {}",
                vlog_bytes.len(),
                expected_vlog_len
            )));
        }
        let mut kv_pos = 0usize;
        while kv_pos < kv_bytes.len() {
            if kv_pos + 8 > kv_bytes.len() {
                return Err(Error::InvalidState(
                    "truncated active memtable kv entry header".to_string(),
                ));
            }
            let key_len = match segment.memtable_type {
                MemtableType::Hash => {
                    u32::from_be_bytes(kv_bytes[kv_pos..kv_pos + 4].try_into().unwrap()) as usize
                }
                MemtableType::Vec => {
                    u32::from_le_bytes(kv_bytes[kv_pos..kv_pos + 4].try_into().unwrap()) as usize
                }
            };
            let value_len = match segment.memtable_type {
                MemtableType::Hash => {
                    u32::from_be_bytes(kv_bytes[kv_pos + 4..kv_pos + 8].try_into().unwrap())
                        as usize
                }
                MemtableType::Vec => {
                    u32::from_le_bytes(kv_bytes[kv_pos + 4..kv_pos + 8].try_into().unwrap())
                        as usize
                }
            };
            let entry_end = kv_pos
                .checked_add(8)
                .and_then(|v| v.checked_add(key_len))
                .and_then(|v| v.checked_add(value_len))
                .ok_or_else(|| Error::IoError("active memtable kv entry overflow".to_string()))?;
            if entry_end > kv_bytes.len() {
                return Err(Error::InvalidState(
                    "truncated active memtable kv entry body".to_string(),
                ));
            }
            let key_start = kv_pos + 8;
            let value_start = key_start + key_len;
            memtable.put(
                &kv_bytes[key_start..value_start],
                &kv_bytes[value_start..entry_end],
            )?;
            kv_pos = entry_end;
        }
        if expected_vlog_len > 0 {
            let file_seq = segment.vlog_file_seq.ok_or_else(|| {
                Error::InvalidState("missing vlog_file_seq for vlog snapshot data".to_string())
            })?;
            let recorder_ref = recorder.get_or_insert_with(|| MemtableVlogRecorder::new(file_seq));
            if recorder_ref.file_seq() != file_seq {
                return Err(Error::InvalidState(
                    "active memtable vlog file seq mismatch".to_string(),
                ));
            }
            if recorder_ref.checkpoint() != segment.vlog_start_offset {
                return Err(Error::InvalidState(format!(
                    "active memtable vlog start offset mismatch: {} != {}",
                    recorder_ref.checkpoint(),
                    segment.vlog_start_offset
                )));
            }
            let mut vlog_pos = 0usize;
            while vlog_pos < vlog_bytes.len() {
                if vlog_pos + 4 > vlog_bytes.len() {
                    return Err(Error::InvalidState(
                        "truncated active memtable vlog record header".to_string(),
                    ));
                }
                let payload_len =
                    u32::from_le_bytes(vlog_bytes[vlog_pos..vlog_pos + 4].try_into().unwrap())
                        as usize;
                vlog_pos += 4;
                let payload_end = vlog_pos.checked_add(payload_len).ok_or_else(|| {
                    Error::IoError("active memtable vlog entry overflow".to_string())
                })?;
                if payload_end > vlog_bytes.len() {
                    return Err(Error::InvalidState(
                        "truncated active memtable vlog record body".to_string(),
                    ));
                }
                let payload = &vlog_bytes[vlog_pos..payload_end];
                let _ = recorder_ref.append_value(&mut memtable, payload)?;
                vlog_pos = payload_end;
            }
            if recorder_ref.checkpoint() != segment.vlog_end_offset {
                return Err(Error::InvalidState(format!(
                    "active memtable vlog end offset mismatch: {} != {}",
                    recorder_ref.checkpoint(),
                    segment.vlog_end_offset
                )));
            }
        }
    }

    Ok((memtable, recorder))
}

struct PrimedIterator<I> {
    inner: I,
}

impl<I> PrimedIterator<I> {
    fn new(inner: I) -> Self {
        Self { inner }
    }
}

impl<'a, I> KvIterator<'a> for PrimedIterator<I>
where
    I: KvIterator<'a>,
{
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.inner.seek(target)?;
        let _ = self.inner.next()?;
        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.inner.seek_to_first()?;
        let _ = self.inner.next()?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool> {
        self.inner.next()
    }

    fn valid(&self) -> bool {
        self.inner.valid()
    }

    fn key(&self) -> Result<Option<Bytes>> {
        self.inner.key()
    }

    fn key_slice(&self) -> Result<Option<&[u8]>> {
        self.inner.key_slice()
    }

    fn value(&self) -> Result<Option<Bytes>> {
        self.inner.value()
    }

    fn value_slice(&self) -> Result<Option<&[u8]>> {
        self.inner.value_slice()
    }
}

fn make_sst_builder_factory(options: SSTWriterOptions) -> FileBuilderFactory {
    Box::new(move |writer| {
        Box::new(SSTWriter::new(
            writer,
            SSTWriterOptions {
                metrics: options.metrics.clone(),
                block_size: options.block_size,
                buffer_size: options.buffer_size,
                num_columns: options.num_columns,
                bloom_filter_enabled: options.bloom_filter_enabled,
                bloom_bits_per_key: options.bloom_bits_per_key,
                partitioned_index: options.partitioned_index,
                compression: options.compression,
            },
        )) as Box<dyn FileBuilder>
    })
}

fn make_data_file_builder_factory(writer_options: WriterOptions) -> FileBuilderFactory {
    match writer_options {
        WriterOptions::Sst(sst_options) => make_sst_builder_factory(sst_options),
        WriterOptions::Parquet(parquet_options) => Box::new(move |writer| {
            let parquet_options = parquet_options.clone();
            Box::new(
                ParquetWriter::with_options(
                    writer,
                    ParquetWriterOptions {
                        row_group_size_bytes: parquet_options.row_group_size_bytes,
                        buffer_size: parquet_options.buffer_size,
                        num_columns: parquet_options.num_columns,
                    },
                )
                .expect("failed to create parquet writer"),
            ) as Box<dyn FileBuilder>
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MemtableType;
    use crate::file::{FileManager, FileSystemRegistry};
    use crate::sst::row_codec::decode_value;
    use crate::sst::{SSTIterator, SSTIteratorOptions, SSTWriterOptions};
    use crate::r#type::ValueType;
    use crate::r#type::{RefColumn, RefKey, RefValue};
    use crate::vlog::VlogStore;

    fn cleanup_test_root() {
        let _ = std::fs::remove_dir_all("/tmp/memtable_manager_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_memtable_flush_deduplicates() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test".to_string())
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
        let file_manager =
            Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
        let lsm_tree = Arc::new(LSMTree::with_state(
            Arc::new(DbStateHandle::new()),
            Arc::clone(&metrics_manager),
        ));
        let manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 256,
                buffer_count: 2,
                writer_options: WriterOptions::Sst(SSTWriterOptions {
                    bloom_filter_enabled: true,
                    ..SSTWriterOptions::default()
                }),
                file_builder_factory: None,
                num_columns: 1,
                write_stall_limit: 8,
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();
        manager.open().unwrap();

        let num_columns = 1;
        let key_a = RefKey::new(0, b"a");
        let key_b = RefKey::new(0, b"b");
        let old = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"old"))]);
        let new = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"new"))]);
        let v1 = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v1"))]);

        manager.put(&key_a, &old).unwrap();
        manager.put(&key_a, &new).unwrap();
        manager.put(&key_b, &v1).unwrap();

        manager.flush_active().unwrap();
        let results = manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        let data_file = results[0].as_ref().unwrap().data_files_by_tree[0].1.clone();
        let level0_files = lsm_tree.level_files(0);
        assert_eq!(level0_files.len(), 1);
        assert_eq!(level0_files[0].file_id, data_file.file_id);
        let reader = file_manager
            .open_data_file_reader(data_file.file_id)
            .unwrap();
        let mut iter = SSTIterator::with_cache_and_file(
            Box::new(reader),
            data_file.as_ref(),
            SSTIteratorOptions {
                bloom_filter_enabled: true,
                ..SSTIteratorOptions::default()
            },
            None,
        )
        .unwrap();
        iter.seek_to_first().unwrap();
        let mut entries = Vec::new();
        while iter.valid() {
            let (key, mut value) = iter.current().unwrap().unwrap();
            let decoded = decode_value(&mut value, num_columns).unwrap();
            let raw = decoded
                .columns()
                .get(0)
                .and_then(|col| col.as_ref())
                .map(|col| Bytes::copy_from_slice(col.data()))
                .unwrap_or_else(Bytes::new);
            entries.push((key, raw));
            iter.next().unwrap();
        }
        assert_eq!(
            entries,
            vec![
                (Bytes::from_static(b"\0\0a"), Bytes::from("new")),
                (Bytes::from_static(b"\0\0b"), Bytes::from("v1"))
            ]
        );
        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_memtable_flush_with_separated_value() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test".to_string())
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
        let file_manager =
            Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
        let lsm_tree = Arc::new(LSMTree::with_state(
            Arc::new(DbStateHandle::new()),
            Arc::clone(&metrics_manager),
        ));
        let vlog_store = Arc::new(VlogStore::new(Arc::clone(&file_manager), 64, 8));
        let manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 256,
                buffer_count: 2,
                file_builder_factory: None,
                num_columns: 1,
                write_stall_limit: 8,
                vlog_store: Some(vlog_store),
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();
        manager.open().unwrap();

        let key = RefKey::new(0, b"k1");
        let value = RefValue::new(vec![Some(RefColumn::new(
            ValueType::Put,
            b"value-larger-than-threshold",
        ))]);
        manager.put(&key, &value).unwrap();

        manager.flush_active().unwrap();
        let results = manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        let flush_result = results[0].as_ref().unwrap();
        assert!(flush_result.vlog_edit.is_some());

        let data_file = Arc::clone(&flush_result.data_files_by_tree[0].1);
        let reader = file_manager
            .open_data_file_reader(data_file.file_id)
            .unwrap();
        let mut iter = SSTIterator::with_cache_and_file(
            Box::new(reader),
            data_file.as_ref(),
            SSTIteratorOptions::default(),
            None,
        )
        .unwrap();
        iter.seek_to_first().unwrap();
        let (_, mut value) = iter.current().unwrap().unwrap();
        let decoded = decode_value(&mut value, 1).unwrap();
        let column = decoded.columns()[0].as_ref().unwrap();
        assert_eq!(column.value_type, ValueType::PutSeparated);
        assert_eq!(column.data().len(), 8);

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_memtable_flush_splits_l0_files_by_bucket_tree() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test".to_string())
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
        let file_manager =
            Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
        let lsm_tree = Arc::new(LSMTree::with_state(
            Arc::new(DbStateHandle::new()),
            Arc::clone(&metrics_manager),
        ));
        lsm_tree
            .db_state()
            .configure_multi_lsm(2, &[0u16..=0u16, 1u16..=1u16])
            .unwrap();
        let manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 256,
                buffer_count: 2,
                file_builder_factory: None,
                num_columns: 1,
                write_stall_limit: 8,
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();
        manager.open().unwrap();

        let key_bucket0 = RefKey::new(0, b"k0");
        let key_bucket1 = RefKey::new(1, b"k1");
        let v0 = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v0"))]);
        let v1 = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v1"))]);
        manager.put(&key_bucket0, &v0).unwrap();
        manager.put(&key_bucket1, &v1).unwrap();

        manager.flush_active().unwrap();
        let results = manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        let flush_result = results[0].as_ref().unwrap();
        assert_eq!(flush_result.data_files_by_tree.len(), 2);
        assert_eq!(lsm_tree.level_files_in_tree(0, 0).len(), 1);
        assert_eq!(lsm_tree.level_files_in_tree(1, 0).len(), 1);
        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_memtable_schema_change_triggers_flush_and_preserves_flush_schema() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test".to_string())
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
        let file_manager =
            Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
        let lsm_tree = Arc::new(LSMTree::with_state(
            Arc::new(DbStateHandle::new()),
            Arc::clone(&metrics_manager),
        ));
        let schema_manager = Arc::new(SchemaManager::new(1));
        let manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 256,
                buffer_count: 2,
                file_builder_factory: None,
                num_columns: 1,
                write_stall_limit: 8,
                schema_manager: Some(Arc::clone(&schema_manager)),
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();
        manager.open().unwrap();

        let key1 = RefKey::new(0, b"k1");
        let value1 = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v1"))]);
        manager.put(&key1, &value1).unwrap();

        let mut builder = schema_manager.builder();
        builder.add_column(1, None, None).unwrap();
        let _ = builder.commit();

        let key2 = RefKey::new(0, b"k2");
        let value2 = RefValue::new(vec![
            Some(RefColumn::new(ValueType::Put, b"v2")),
            Some(RefColumn::new(ValueType::Put, b"v2c1")),
        ]);
        manager.put(&key2, &value2).unwrap();

        let results = manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].as_ref().unwrap().data_files_by_tree[0]
                .1
                .schema_id,
            0
        );

        manager.flush_active().unwrap();
        let results = manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].as_ref().unwrap().data_files_by_tree[0]
                .1
                .schema_id,
            1
        );

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_memtable_reuses_buffer_after_flush() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test".to_string())
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
        let file_manager =
            Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
        let lsm_tree = Arc::new(crate::lsm::LSMTree::with_state(
            Arc::new(crate::db_state::DbStateHandle::new()),
            Arc::clone(&metrics_manager),
        ));
        let manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 256,
                buffer_count: 2,
                writer_options: WriterOptions::Sst(SSTWriterOptions {
                    bloom_filter_enabled: true,
                    ..SSTWriterOptions::default()
                }),
                file_builder_factory: None,
                num_columns: 1,
                write_stall_limit: 8,
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();
        manager.open().unwrap();

        let key1 = RefKey::new(0, b"k1");
        let v1 = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v1"))]);
        manager.put(&key1, &v1).unwrap();
        manager.flush_active().unwrap();
        let results = manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        assert_eq!(lsm_tree.level_files(0).len(), 1);

        let key2 = RefKey::new(0, b"k2");
        let v2 = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v2"))]);
        manager.put(&key2, &v2).unwrap();
        manager.flush_active().unwrap();
        let results = manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        assert_eq!(lsm_tree.level_files(0).len(), 2);
        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_vec_memtable_triggers_flush_on_full() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test".to_string())
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
        let file_manager =
            Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
        let lsm_tree = Arc::new(crate::lsm::LSMTree::with_state(
            Arc::new(crate::db_state::DbStateHandle::new()),
            Arc::clone(&metrics_manager),
        ));
        let manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 192,
                buffer_count: 2,
                memtable_type: MemtableType::Vec,
                writer_options: WriterOptions::Sst(SSTWriterOptions {
                    bloom_filter_enabled: true,
                    ..SSTWriterOptions::default()
                }),
                file_builder_factory: None,
                num_columns: 1,
                write_stall_limit: 8,
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();
        manager.open().unwrap();

        let key1 = RefKey::new(0, b"k1");
        let key2 = RefKey::new(0, b"k2");
        let large_value = vec![b'v'; 96];
        let value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, &large_value))]);
        manager.put(&key1, &value).unwrap();
        manager.put(&key2, &value).unwrap();

        let results = manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        assert_eq!(lsm_tree.level_files(0).len(), 1);
        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_oversized_put_ref_uses_special_vec_memtable_and_can_overcommit_budget() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test".to_string())
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
        let file_manager =
            Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
        let lsm_tree = Arc::new(crate::lsm::LSMTree::with_state(
            Arc::new(crate::db_state::DbStateHandle::new()),
            Arc::clone(&metrics_manager),
        ));
        let manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 128,
                buffer_count: 1,
                memtable_type: MemtableType::Hash,
                file_builder_factory: None,
                num_columns: 1,
                write_stall_limit: 8,
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();
        manager.open().unwrap();

        let key = RefKey::new(0, b"k1");
        let big_value = vec![b'x'; 1024];
        let value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, &big_value))]);
        manager.put(&key, &value).unwrap();
        manager.flush_active().unwrap();

        let results = manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        assert_eq!(lsm_tree.level_files(0).len(), 1);

        let state = manager.state.lock().unwrap();
        assert_eq!(state.in_flight, 0);
        assert_eq!(state.budget, 0);
        cleanup_test_root();
    }
}
