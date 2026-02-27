use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, mpsc};
use std::thread::JoinHandle;

use crate::config::MemtableType;
use crate::data_file::{DataFile, DataFileType};
use crate::db_state::{DbState, DbStateHandle};
use crate::error::Error::InvalidState;
use crate::error::{Error, Result};
use crate::file::{FileManager, TrackedFileId};
use crate::format::{FileBuilder, FileBuilderFactory};
use crate::iterator::{DeduplicatingIterator, KvIterator};
use crate::lsm::LSMTree;
use crate::memtable::vlog::{MemtableVlogRecorder, rewrite_ref_value_for_memtable};
use crate::memtable::{HashMemtable, VecMemtable};
use crate::memtable::{Memtable, MemtableImpl, MemtableKvIter, MemtableReclaimer};
use crate::metrics_manager::MetricsManager;
use crate::schema::SchemaManager;
use crate::snapshot::SnapshotManager;
use crate::sst::{SSTWriter, SSTWriterOptions};
use crate::r#type::{RefKey, RefValue};
use crate::vlog::{VlogEdit, VlogMergeCollector, VlogPointer, VlogStore};
use log::{debug, trace, warn};
use metrics::{Counter, counter};

type DynKvIterator = Box<dyn for<'a> KvIterator<'a>>;

#[derive(Clone)]
pub(crate) struct MemtableFlushResult {
    pub(crate) data_file: Arc<DataFile>,
    pub(crate) seq: u64,
    vlog_edit: Option<VlogEdit>,
}

pub(crate) struct MemtableManagerOptions {
    pub(crate) initial_seq: u64,
    pub(crate) memtable_capacity: usize,
    pub(crate) buffer_count: usize,
    pub(crate) sst_options: SSTWriterOptions,
    pub(crate) file_builder_factory: Option<Arc<FileBuilderFactory>>,
    pub(crate) num_columns: usize,
    pub(crate) memtable_type: MemtableType,
    pub(crate) write_stall_limit: usize,
    pub(crate) auto_snapshot_manager: Option<SnapshotManager>,
    pub(crate) metrics_manager: Option<Arc<MetricsManager>>,
    pub(crate) vlog_store: Option<Arc<VlogStore>>,
    pub(crate) schema_manager: Option<Arc<SchemaManager>>,
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
            initial_seq: 0,
            memtable_capacity: 1024 * 1024,
            buffer_count: 2,
            sst_options: SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            },
            file_builder_factory: None,
            num_columns: 1,
            memtable_type: MemtableType::Hash,
            write_stall_limit: 8,
            auto_snapshot_manager: None,
            metrics_manager: None,
            vlog_store: None,
            schema_manager: None,
        }
    }
}

pub(crate) struct MemtableManager {
    state: Arc<Mutex<MemtableManagerState>>,
    buffer_ready: Arc<Condvar>,
    flush_done: Arc<Condvar>,
    file_manager: Arc<FileManager>,
    file_builder_factory: Arc<FileBuilderFactory>,
    num_columns: usize,
    lsm_tree: Arc<LSMTree>,
    db_state: Arc<DbStateHandle>,
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
    metrics: MemtableManagerMetrics,
}

struct MemtableManagerState {
    budget: i64,
    allow_make_active_buffer_in_reclaimer: bool,
    in_flight: usize,
    next_seq: u64,
    flush_results: BTreeMap<u64, Result<MemtableFlushResult>>,
}

struct FlushJob {
    seq: u64,
    memtable: Option<Arc<MemtableImpl>>,
    vlog_recorder: Option<Arc<MemtableVlogRecorder>>,
    snapshot: Option<SnapshotCompletion>,
}

pub(crate) struct SnapshotCompletion {
    pub(crate) snapshot_id: u64,
    pub(crate) manager: SnapshotManager,
}

pub(crate) struct ActiveMemtable {
    seq: u64,
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
    pub(crate) seq: u64,
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
            next_seq: options.initial_seq,
            flush_results: BTreeMap::new(),
        };
        let state = Arc::new(Mutex::new(state));
        let buffer_ready = Arc::new(Condvar::new());
        let flush_done = Arc::new(Condvar::new());
        let db_state = lsm_tree.db_state();
        if let Some(manager) = &options.metrics_manager
            && options.sst_options.metrics.is_none()
        {
            options.sst_options.metrics =
                Some(manager.sst_writer_metrics(options.sst_options.compression));
        }
        let metrics = options
            .metrics_manager
            .as_ref()
            .map(|manager| manager.memtable_metrics())
            .unwrap_or_else(|| MemtableManagerMetrics::new("unknown"));
        let reclaimer = Self::make_reclaimer(
            Arc::clone(&state),
            Arc::clone(&buffer_ready),
            Arc::clone(&db_state),
            options.memtable_capacity,
            options.memtable_type,
        );
        let file_builder_factory = options
            .file_builder_factory
            .unwrap_or_else(|| Arc::new(make_sst_builder_factory(options.sst_options.clone())));
        let vlog_store = options.vlog_store.unwrap_or_else(|| {
            Arc::new(VlogStore::new(
                Arc::clone(&file_manager),
                usize::MAX,
                usize::MAX,
            ))
        });
        let schema_manager = options
            .schema_manager
            .unwrap_or_else(|| Arc::new(SchemaManager::new(options.num_columns)));
        {
            // init the first active buffer
            let mut state_guard = state.lock().unwrap();
            Self::make_active_buffer(
                &mut state_guard,
                &db_state,
                options.memtable_capacity,
                options.memtable_type,
                &reclaimer,
                total_budget,
                false,
            );
        }
        // Initialize the flush worker
        let (worker, flush_tx) = Self::init_flush_worker(
            Arc::clone(&state),
            Arc::clone(&flush_done),
            Arc::clone(&file_manager),
            Arc::clone(&file_builder_factory),
            options.num_columns,
            Arc::clone(&lsm_tree),
            lsm_tree.ttl_provider(),
            Arc::clone(&vlog_store),
            Arc::clone(&schema_manager),
            metrics.clone(),
        )?;
        Ok(Self {
            state,
            buffer_ready,
            flush_done,
            file_manager,
            file_builder_factory,
            num_columns: options.num_columns,
            lsm_tree,
            db_state,
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
            metrics,
        })
    }

    pub(crate) fn db_state(&self) -> Arc<DbStateHandle> {
        Arc::clone(&self.db_state)
    }

    /// Initializes the flush worker and returns the worker handle and flush sender.
    #[allow(clippy::too_many_arguments)]
    fn init_flush_worker(
        state: Arc<Mutex<MemtableManagerState>>,
        flush_done: Arc<Condvar>,
        file_manager: Arc<FileManager>,
        file_builder_factory: Arc<FileBuilderFactory>,
        num_columns: usize,
        lsm_tree: Arc<LSMTree>,
        ttl_provider: Arc<crate::ttl::TTLProvider>,
        vlog_store: Arc<VlogStore>,
        schema_manager: Arc<SchemaManager>,
        metrics: MemtableManagerMetrics,
    ) -> Result<(JoinHandle<()>, mpsc::Sender<FlushJob>)> {
        let (flush_tx, flush_rx) = mpsc::channel::<FlushJob>();
        let state_clone = Arc::clone(&state);
        let flush_done_clone = Arc::clone(&flush_done);
        let file_manager_clone = Arc::clone(&file_manager);
        let file_builder_factory_clone = Arc::clone(&file_builder_factory);
        let lsm_tree_clone = Arc::clone(&lsm_tree);
        let db_state_clone = lsm_tree_clone.db_state();
        let ttl_provider_clone = Arc::clone(&ttl_provider);
        let vlog_store_clone = Arc::clone(&vlog_store);
        let schema_manager_clone = Arc::clone(&schema_manager);
        let handle = std::thread::Builder::new()
            .name("cobble-flush".to_string())
            .spawn(move || {
                while let Ok(job) = flush_rx.recv() {
                    if let Some(memtable) = job.memtable {
                        trace!("memtable flush start seq={}", job.seq);
                        let keep_memtable_alive = Arc::clone(&memtable);
                        let result = flush_memtable(
                            job.seq,
                            memtable.as_ref(),
                            job.vlog_recorder,
                            Arc::clone(&file_manager_clone),
                            Arc::clone(&file_builder_factory_clone),
                            num_columns,
                            Arc::clone(&ttl_provider_clone),
                            Arc::clone(&vlog_store_clone),
                            Arc::clone(&schema_manager_clone),
                        );
                        let mut state = state_clone.lock().unwrap();
                        state.in_flight = state.in_flight.saturating_sub(1);
                        match result {
                            Ok(res) => {
                                debug!(
                                    "memtable flush complete seq={} file_id={} size={}",
                                    res.seq, res.data_file.file_id, res.data_file.size
                                );
                                metrics.flushes_total.increment(1);
                                metrics
                                    .flush_bytes_total
                                    .increment(res.data_file.size as u64);
                                let vlog_edit = res.vlog_edit.clone();
                                let snapshot = lsm_tree_clone.add_level0_files(
                                    res.seq,
                                    vec![Arc::clone(&res.data_file)],
                                    vlog_edit,
                                );
                                state.flush_results.insert(res.seq, Ok(res));
                                flush_done_clone.notify_all();
                                drop(state);
                                Self::finish_and_materialize_snapshot(
                                    &job.snapshot,
                                    &snapshot,
                                    &db_state_clone,
                                );
                            }
                            Err(err) => {
                                panic!("memtable flush failed seq={} err={}", job.seq, err);
                            }
                        }
                        drop(keep_memtable_alive);
                    } else {
                        let snapshot = db_state_clone.load();
                        let mut state = state_clone.lock().unwrap();
                        state.in_flight = state.in_flight.saturating_sub(1);
                        flush_done_clone.notify_all();
                        drop(state);
                        Self::finish_and_materialize_snapshot(
                            &job.snapshot,
                            &snapshot,
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
        db_state: &Arc<DbStateHandle>,
    ) {
        if let Some(snapshot_job) = snapshot_completion {
            snapshot_job.manager.finish_snapshot(
                snapshot_job.snapshot_id,
                snapshot,
                db_state.as_ref(),
            );
            let _ = snapshot_job
                .manager
                .schedule_materialize(snapshot_job.snapshot_id);
            let _ = snapshot_job.manager.process_retention();
        }
    }

    /// Makes an active memtable from a free buffer if none exists.
    /// Assumes the caller holds the lock on the state.
    fn make_active_buffer(
        state: &mut MemtableManagerState,
        db_state: &Arc<DbStateHandle>,
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
            let seq = state.next_seq;
            state.next_seq += 1;
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
                seq,
                memtable: Some(memtable),
                vlog_recorder: None,
            }));
            db_state.cas_mutate(snapshot.seq_id, |db_state, snapshot| {
                Some(DbState {
                    seq_id: db_state.allocate_seq_id(),
                    lsm_version: snapshot.lsm_version.clone(),
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
        memtable_capacity: usize,
        memtable_type: MemtableType,
    ) -> MemtableReclaimer {
        let state = Arc::downgrade(&state);
        let buffer_ready = Arc::downgrade(&buffer_ready);
        let db_state = Arc::downgrade(&db_state);
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
            let mut guard = state.lock().unwrap();
            let returned_budget = returned.min(i64::MAX as u64) as i64;
            guard.budget = guard.budget.saturating_add(returned_budget);
            // reuse the same reclaimer since it captures nothing by reference
            let reclaimer = Self::make_reclaimer(
                Arc::clone(&state),
                Arc::clone(&buffer_ready),
                Arc::clone(&db_state),
                memtable_capacity,
                memtable_type,
            );
            // try to make a new active buffer
            if guard.allow_make_active_buffer_in_reclaimer {
                Self::make_active_buffer(
                    &mut guard,
                    &db_state,
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
                    self.num_columns,
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
                    memtable.put_ref_rewritten(key, plan, self.num_columns)
                } else {
                    memtable.put_ref(key, value, self.num_columns)
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
        let capacity = VecMemtable::estimate_capacity_for_ref(
            key,
            value,
            self.num_columns,
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
                        lsm_version: snapshot.lsm_version.clone(),
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
    /// Returns the minimum sequence number among the memtables searched.
    pub(crate) fn get_all<F>(&self, key: &[u8], f: F) -> Result<Option<u64>>
    where
        F: FnMut(&[u8]) -> Result<()>,
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
    ) -> Result<Option<u64>>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let mut min_seq = u64::MAX;
        if let Some(active) = &snapshot.active {
            let active = active.lock().unwrap();
            let memtable = active.memtable.as_ref().expect("active memtable exists");
            min_seq = active.seq;
            for value in memtable.get_all(key) {
                f(value)?;
            }
            drop(active);
        }
        min_seq = min_seq.min(
            snapshot
                .immutables
                .front()
                .map(|m| m.seq)
                .unwrap_or(min_seq),
        );
        for immutable in snapshot.immutables.iter().rev() {
            for value in immutable.memtable.get_all(key) {
                f(value)?;
            }
        }
        Ok(Some(min_seq))
    }

    pub(crate) fn scan_memtable_iterators_with_snapshot(
        &self,
        snapshot: Arc<DbState>,
    ) -> Result<Vec<DynKvIterator>> {
        let mut iterators: Vec<DynKvIterator> = Vec::new();
        if let Some(active) = &snapshot.active {
            iterators.push(Box::new(MemtableScanIterator::for_active(Arc::clone(
                active,
            ))));
        }
        for immutable in snapshot.immutables.iter().rev() {
            iterators.push(Box::new(MemtableScanIterator::for_immutable(Arc::clone(
                &immutable.memtable,
            ))));
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

    pub(crate) fn flush_active(&self) -> Result<Option<u64>> {
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

    fn flush_active_internal(&self, snapshot: Option<SnapshotCompletion>) -> Result<Option<u64>> {
        let mut state = self.state.lock().unwrap();
        let mut guard = self.db_state.lock();
        guard = self.wait_for_write_stall_under_guard(guard);
        let snapshot_state = self.db_state.load();
        let mut to_flush = None;
        let mut flushed_seq = None;
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
                    seq: inner_active.seq,
                    memtable: Arc::new(active_memtable),
                    vlog_recorder: active_vlog_recorder,
                };
                flushed_seq = Some(new_immutable.seq);
                to_flush = Some(new_immutable.clone());
                immutables.push_back(new_immutable);

                Some(DbState {
                    seq_id: db_state.allocate_seq_id(),
                    lsm_version: snapshot_state.lsm_version.clone(),
                    vlog_version: snapshot_state.vlog_version.clone(),
                    active: None,
                    immutables,
                    suggested_base_snapshot_id: snapshot_state.suggested_base_snapshot_id,
                })
            });
        drop(guard);
        let job = if let Some(to_flush) = to_flush {
            Self::make_active_buffer(
                &mut state,
                &self.db_state,
                self.memtable_capacity,
                self.memtable_type,
                &self.reclaimer,
                self.total_budget,
                false,
            );
            state.in_flight += 1;
            FlushJob {
                seq: to_flush.seq,
                memtable: Some(to_flush.memtable),
                vlog_recorder: to_flush.vlog_recorder,
                snapshot,
            }
        } else if snapshot.is_some() {
            state.in_flight += 1;
            FlushJob {
                seq: 0,
                memtable: None,
                vlog_recorder: None,
                snapshot,
            }
        } else {
            drop(state);
            return Ok(flushed_seq);
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
        Ok(flushed_seq)
    }

    #[cfg(test)]
    pub(crate) fn wait_for_flushes(&self) -> Vec<Result<MemtableFlushResult>> {
        let mut state = self.state.lock().unwrap();
        while state.in_flight > 0 {
            state = self.flush_done.wait(state).unwrap();
        }
        std::mem::take(&mut state.flush_results)
            .into_values()
            .collect()
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
        let level0 = snapshot
            .lsm_version
            .levels
            .iter()
            .find(|level| level.ordinal == 0)
            .map(|level| level.files.len())
            .unwrap_or(0);
        immutables + level0 > write_stall_limit
    }
}

impl Drop for MemtableManager {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[allow(clippy::too_many_arguments)]
fn flush_memtable(
    seq: u64,
    memtable: &impl Memtable,
    vlog_recorder: Option<Arc<MemtableVlogRecorder>>,
    file_manager: Arc<FileManager>,
    file_builder_factory: Arc<FileBuilderFactory>,
    num_columns: usize,
    ttl_provider: Arc<crate::ttl::TTLProvider>,
    vlog_store: Arc<VlogStore>,
    schema_manager: Arc<SchemaManager>,
) -> Result<MemtableFlushResult> {
    // Step 1: If there is a vlog recorder with entries, flush it to the vlog store and get the resulting edit.
    let mut vlog_edit = None;
    if let Some(recorder) = vlog_recorder
        && recorder.has_entries()
    {
        let (mut writer, mut edit) = vlog_store.create_writer_for_seq(recorder.file_seq())?;
        recorder.flush_to_writer(memtable, &mut writer)?;
        edit.add_entry_delta(recorder.file_seq(), recorder.entry_count() as i64);
        writer.close()?;
        vlog_edit = Some(edit);
    }
    // Step 2: Create a new data file and write all entries from the memtable into it using a deduplicating iterator.
    let (file_id, writer) = file_manager.create_data_file()?;
    let mut builder = (file_builder_factory)(Box::new(writer));
    // Try to handle merges during flush if vlog edits are present
    let merge_collector = vlog_edit.as_ref().map(|_| VlogMergeCollector::shared(true));
    let merge_callback = merge_collector.as_ref().map(VlogMergeCollector::callback);
    let schema = schema_manager.latest_schema();
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
            builder.add(key, value)?;
        }
        dedup_iter.next()?;
    }
    if let Some(collector) = merge_collector.as_ref() {
        collector.borrow_mut().check_error()?;
    }
    if let (Some(edit), Some(collector)) = (&mut vlog_edit, merge_collector.as_ref()) {
        for (file_seq, delta) in collector.borrow().removed_entry_deltas() {
            edit.add_entry_delta(file_seq, delta);
        }
    }
    // Step 3: Finish the data file and construct the resulting `MemtableFlushResult`.
    let (start_key, end_key, file_size, footer_bytes) = builder.finish()?;
    let has_separated_values = merge_collector
        .as_ref()
        .is_some_and(|collector| collector.borrow().has_separated_values());
    let data_file = DataFile {
        file_type: DataFileType::SSTable,
        start_key,
        end_key,
        file_id,
        tracked_id: TrackedFileId::new(&file_manager, file_id),
        size: file_size,
        seq,
        schema_id: schema.version(),
        has_separated_values,
        meta_bytes: Default::default(),
    };
    data_file.set_meta_bytes(footer_bytes);
    Ok(MemtableFlushResult {
        data_file: Arc::new(data_file),
        seq,
        vlog_edit,
    })
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
        let metrics_manager = Arc::new(MetricsManager::new("memtable-test".to_string()));
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
                initial_seq: 0,
                memtable_capacity: 256,
                buffer_count: 2,
                sst_options: SSTWriterOptions {
                    bloom_filter_enabled: true,
                    ..SSTWriterOptions::default()
                },
                file_builder_factory: None,
                num_columns: 1,
                write_stall_limit: 8,
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();

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
        assert_eq!(results[0].as_ref().unwrap().seq, 0);
        let data_file = results[0].as_ref().unwrap().data_file.clone();
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
        let metrics_manager = Arc::new(MetricsManager::new("memtable-test".to_string()));
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
                initial_seq: 0,
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

        let data_file = Arc::clone(&flush_result.data_file);
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
    fn test_memtable_reuses_buffer_after_flush() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test".to_string())
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("memtable-test".to_string()));
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
                initial_seq: 0,
                memtable_capacity: 256,
                buffer_count: 2,
                sst_options: SSTWriterOptions {
                    bloom_filter_enabled: true,
                    ..SSTWriterOptions::default()
                },
                file_builder_factory: None,
                num_columns: 1,
                write_stall_limit: 8,
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();

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
        let metrics_manager = Arc::new(MetricsManager::new("memtable-test".to_string()));
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
                initial_seq: 0,
                memtable_capacity: 192,
                buffer_count: 2,
                memtable_type: MemtableType::Vec,
                sst_options: SSTWriterOptions {
                    bloom_filter_enabled: true,
                    ..SSTWriterOptions::default()
                },
                file_builder_factory: None,
                num_columns: 1,
                write_stall_limit: 8,
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();

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
        let metrics_manager = Arc::new(MetricsManager::new("memtable-test".to_string()));
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
                initial_seq: 0,
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
