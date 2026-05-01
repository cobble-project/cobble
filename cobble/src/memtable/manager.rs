use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, mpsc};
use std::thread::JoinHandle;

use crate::config::MemtableType;
use crate::data_file::DataFile;
use crate::db_state::{DbState, DbStateHandle, LSMTreeScope, MultiLSMTreeVersion};
use crate::db_status::DbLifecycle;
use crate::error::Error::InvalidState;
use crate::error::{Error, Result};
use crate::file::{File, FileManager, MetadataReader, TrackedFileId};
use crate::format::{FileBuilder, FileBuilderFactory};
use crate::iterator::{
    ColumnMaskingIterator, DeduplicatingIterator, KvIterator, SchemaEvolvingIterator,
};
use crate::lsm::LSMTree;
use crate::memtable::vlog::{MemtableVlogRecorder, rewrite_ref_value_for_memtable};
use crate::memtable::{HashMemtable, SkiplistMemtable, VecMemtable};
use crate::memtable::{Memtable, MemtableImpl, MemtableReclaimer};
use crate::metrics_manager::MetricsManager;
use crate::parquet::{ParquetWriter, ParquetWriterOptions};
use crate::paths::snapshot_active_data_relative_path;
use crate::schema::{Schema, SchemaManager};
use crate::snapshot::{ActiveMemtableSnapshotData, SnapshotManager};
use crate::sst::{SSTWriter, SSTWriterOptions};
use crate::r#type::{KvValue, RefKey, RefValue, ValueType, key_bucket, key_column_family};
use crate::vlog::{VlogEdit, VlogMergeCollector, VlogPointer, VlogStore};
use crate::writer_options::{WriterOptions, WriterOptionsFactory};
use log::{debug, trace, warn};
use metrics::{Counter, counter};
use std::time::Duration;
use uuid::Uuid;

type DynKvIterator = Box<dyn for<'a> KvIterator<'a>>;
const CLOSE_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(crate) struct MemtableFlushResult {
    pub(crate) data_files_by_scope: Vec<(LSMTreeScope, Arc<DataFile>)>,
    vlog_edit: Option<VlogEdit>,
}

pub(crate) struct MemtableManagerOptions {
    pub(crate) memtable_capacity: usize,
    pub(crate) buffer_count: usize,
    pub(crate) writer_options: WriterOptions,
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
    writer_options_factory: WriterOptionsFactory,
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

struct FlushTreeBuilder {
    scope: LSMTreeScope,
    file_id: u64,
    builder: Box<dyn FileBuilder>,
    min_bucket: u16,
    max_bucket: u16,
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

#[derive(Clone)]
enum MemtableScanSource {
    Active(Arc<Mutex<ActiveMemtable>>),
    Immutable(Arc<MemtableImpl>),
}

struct SkiplistScanCursor {
    source: MemtableScanSource,
    end_bound_exclusive: Option<Bytes>,
    next_node: Option<u32>,
    current_key: Option<Bytes>,
    current_value: Option<Bytes>,
}

impl SkiplistScanCursor {
    fn new(source: MemtableScanSource, end_bound_exclusive: Option<Bytes>) -> Self {
        Self {
            source,
            end_bound_exclusive,
            next_node: None,
            current_key: None,
            current_value: None,
        }
    }

    fn read_skiplist_node(&self, node: u32) -> Result<Option<(Bytes, Bytes, Option<u32>)>> {
        let read_from_skiplist = |memtable: &SkiplistMemtable| {
            memtable.node_entry(node).map(|(key, value)| {
                (
                    Bytes::copy_from_slice(key),
                    Bytes::copy_from_slice(value),
                    memtable.next_node_offset(node),
                )
            })
        };
        match &self.source {
            MemtableScanSource::Active(active) => {
                let active = active.lock().unwrap();
                let Some(memtable) = active.memtable.as_ref() else {
                    return Ok(None);
                };
                match memtable {
                    MemtableImpl::Skiplist(skiplist_memtable) => {
                        Ok(read_from_skiplist(skiplist_memtable))
                    }
                    _ => Err(InvalidState(
                        "skiplist iterator expected skiplist active memtable".to_string(),
                    )),
                }
            }
            MemtableScanSource::Immutable(memtable) => match memtable.as_ref() {
                MemtableImpl::Skiplist(skiplist_memtable) => {
                    Ok(read_from_skiplist(skiplist_memtable))
                }
                _ => Err(InvalidState(
                    "skiplist iterator expected skiplist immutable memtable".to_string(),
                )),
            },
        }
    }

    fn skiplist_start_node(&self, seek_target: Option<&[u8]>) -> Result<Option<u32>> {
        let find_start = |memtable: &SkiplistMemtable| {
            if let Some(target) = seek_target {
                memtable.lower_bound_node_offset(target)
            } else {
                memtable.first_node_offset()
            }
        };
        match &self.source {
            MemtableScanSource::Active(active) => {
                let active = active.lock().unwrap();
                let Some(memtable) = active.memtable.as_ref() else {
                    return Ok(None);
                };
                match memtable {
                    MemtableImpl::Skiplist(skiplist_memtable) => Ok(find_start(skiplist_memtable)),
                    _ => Err(InvalidState(
                        "skiplist iterator expected skiplist active memtable".to_string(),
                    )),
                }
            }
            MemtableScanSource::Immutable(memtable) => match memtable.as_ref() {
                MemtableImpl::Skiplist(skiplist_memtable) => Ok(find_start(skiplist_memtable)),
                _ => Err(InvalidState(
                    "skiplist iterator expected skiplist immutable memtable".to_string(),
                )),
            },
        }
    }

    fn advance(&mut self) -> Result<bool> {
        let Some(node) = self.next_node else {
            self.current_key = None;
            self.current_value = None;
            return Ok(false);
        };
        let Some((key, value, next_node)) = self.read_skiplist_node(node)? else {
            self.next_node = None;
            self.current_key = None;
            self.current_value = None;
            return Ok(false);
        };
        self.next_node = next_node;
        if let Some(end) = self.end_bound_exclusive.as_deref()
            && key.as_ref() >= end
        {
            self.next_node = None;
            self.current_key = None;
            self.current_value = None;
            return Ok(false);
        }
        self.current_key = Some(key);
        self.current_value = Some(value);
        Ok(true)
    }

    fn seek_inner(&mut self, target: Option<&[u8]>) -> Result<()> {
        self.next_node = self.skiplist_start_node(target)?;
        let _ = self.advance()?;
        Ok(())
    }
}

impl<'a> KvIterator<'a> for SkiplistScanCursor {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.seek_inner(Some(target))
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.seek_inner(None)
    }

    fn next(&mut self) -> Result<bool> {
        self.advance()
    }

    fn valid(&self) -> bool {
        self.current_key.is_some() && self.current_value.is_some()
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        Ok(self.current_key.as_deref())
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        Ok(self.current_key.take())
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        Ok(self.current_value.take().map(KvValue::Encoded))
    }
}

struct MemtableScanIterator {
    source: MemtableScanSource,
    /// Scan lower bound already encoded in DB key space.
    start_bound_inclusive: Option<Bytes>,
    /// Scan upper bound already encoded in DB key space.
    end_bound_exclusive: Option<Bytes>,
    /// Optional projected-row visibility filter used only on the collect path.
    /// When present, collected rows that are provably invisible for the projection
    /// (for example all selected columns are empty/Delete) do not count toward `max_rows`.
    row_filter: Option<MemtableRowFilter>,
    /// The caller-requested visible-row limit for this memtable layer.
    /// This is only meaningful on paths that materialize collected entries eagerly.
    max_rows: Option<usize>,
    /// Shared allowance flowing from newer collected memtables to older collected memtables.
    /// Each invisible shadowing row found in an upper collected layer increments this allowance,
    /// so lower collected layers fetch extra candidates before the final DbIterator dedup pass.
    shadow_allowance: Option<Arc<AtomicUsize>>,
    /// Marks the first collected memtable iterator in the chain so it can reset the shared
    /// allowance before a seek starts walking newer-to-older collected layers.
    shadow_allowance_root: bool,
    entries: Vec<(Bytes, Bytes)>,
    next_index: usize,
    skiplist_iter: Option<SkiplistScanCursor>,
    current_key: Option<Bytes>,
    current_value: Option<Bytes>,
}

#[derive(Clone)]
struct MemtableRowFilter {
    num_columns: usize,
    selected_columns: Option<Vec<usize>>,
}

fn build_memtable_row_filter(
    source_schema: &Arc<Schema>,
    target_schema_version: u64,
    column_family_id: u8,
    selected_columns: Option<&[usize]>,
) -> Option<MemtableRowFilter> {
    if source_schema.version() != target_schema_version {
        return None;
    }
    Some(MemtableRowFilter {
        num_columns: source_schema
            .num_columns_in_family(column_family_id)
            .unwrap_or(0),
        selected_columns: selected_columns.map(|columns| columns.to_vec()),
    })
}

impl MemtableScanIterator {
    fn for_active(
        active: Arc<Mutex<ActiveMemtable>>,
        start_bound_inclusive: Option<Bytes>,
        end_bound_exclusive: Option<Bytes>,
        row_filter: Option<MemtableRowFilter>,
        max_rows: Option<usize>,
        shadow_allowance: Option<Arc<AtomicUsize>>,
        shadow_allowance_root: bool,
    ) -> Self {
        Self {
            source: MemtableScanSource::Active(active),
            start_bound_inclusive,
            end_bound_exclusive,
            row_filter,
            max_rows,
            shadow_allowance,
            shadow_allowance_root,
            entries: Vec::new(),
            next_index: 0,
            skiplist_iter: None,
            current_key: None,
            current_value: None,
        }
    }

    fn for_immutable(
        memtable: Arc<MemtableImpl>,
        start_bound_inclusive: Option<Bytes>,
        end_bound_exclusive: Option<Bytes>,
        row_filter: Option<MemtableRowFilter>,
        max_rows: Option<usize>,
        shadow_allowance: Option<Arc<AtomicUsize>>,
        shadow_allowance_root: bool,
    ) -> Self {
        Self {
            source: MemtableScanSource::Immutable(memtable),
            start_bound_inclusive,
            end_bound_exclusive,
            row_filter,
            max_rows,
            shadow_allowance,
            shadow_allowance_root,
            entries: Vec::new(),
            next_index: 0,
            skiplist_iter: None,
            current_key: None,
            current_value: None,
        }
    }

    fn reload_collected_entries(
        &mut self,
        start_bound_inclusive: Option<&[u8]>,
        end_bound_exclusive: Option<&[u8]>,
    ) -> Result<()> {
        if self.shadow_allowance_root
            && let Some(shadow_allowance) = self.shadow_allowance.as_ref()
        {
            shadow_allowance.store(0, Ordering::Relaxed);
        }
        let effective_max_rows = if self.row_filter.is_some() {
            self.max_rows
        } else {
            None
        };
        self.skiplist_iter = None;
        self.entries = match &self.source {
            MemtableScanSource::Active(active) => {
                let active = active.lock().unwrap();
                let Some(memtable) = active.memtable.as_ref() else {
                    self.entries.clear();
                    self.current_key = None;
                    self.current_value = None;
                    self.next_index = 0;
                    return Ok(());
                };
                collect_limited_entries_from_memtable(
                    memtable,
                    start_bound_inclusive,
                    end_bound_exclusive,
                    self.row_filter.as_ref(),
                    effective_max_rows,
                    self.shadow_allowance.as_ref(),
                )?
                .0
            }
            MemtableScanSource::Immutable(memtable) => {
                collect_limited_entries_from_memtable(
                    memtable.as_ref(),
                    start_bound_inclusive,
                    end_bound_exclusive,
                    self.row_filter.as_ref(),
                    effective_max_rows,
                    self.shadow_allowance.as_ref(),
                )?
                .0
            }
        };
        self.prime_current();
        Ok(())
    }

    fn reload_skiplist_stream(&mut self, start_bound_inclusive: Option<&[u8]>) -> Result<()> {
        self.entries.clear();
        self.next_index = 0;
        self.current_key = None;
        self.current_value = None;
        let mut skiplist_iter =
            SkiplistScanCursor::new(self.source.clone(), self.end_bound_exclusive.clone());
        if let Some(target) = start_bound_inclusive {
            skiplist_iter.seek(target)?;
        } else {
            skiplist_iter.seek_to_first()?;
        }
        self.skiplist_iter = Some(skiplist_iter);
        Ok(())
    }

    fn reload_entries(&mut self, seek_target: Option<&[u8]>) -> Result<()> {
        let effective_start = match (self.start_bound_inclusive.as_ref(), seek_target) {
            (Some(bound), Some(target)) => {
                if bound.as_ref() >= target {
                    Some(bound.clone())
                } else {
                    Some(Bytes::copy_from_slice(target))
                }
            }
            (Some(bound), None) => Some(bound.clone()),
            (None, Some(target)) => Some(Bytes::copy_from_slice(target)),
            (None, None) => None,
        };
        let start_bound_inclusive = effective_start.as_deref();
        let end_bound_exclusive = self.end_bound_exclusive.clone();
        let end_bound_exclusive_ref = end_bound_exclusive.as_deref();
        let source_is_immutable_skiplist = matches!(
            &self.source,
            MemtableScanSource::Immutable(memtable)
                if matches!(memtable.as_ref(), MemtableImpl::Skiplist(_))
        );
        if source_is_immutable_skiplist {
            self.reload_skiplist_stream(start_bound_inclusive)
        } else {
            self.reload_collected_entries(start_bound_inclusive, end_bound_exclusive_ref)
        }
    }

    fn prime_current(&mut self) {
        if let Some((key, value)) = self.entries.first().cloned() {
            self.current_key = Some(key);
            self.current_value = Some(value);
            self.next_index = 1;
        } else {
            self.current_key = None;
            self.current_value = None;
            self.next_index = 0;
        }
    }
}

fn row_counts_toward_max_rows(
    encoded_value: &Bytes,
    row_filter: &MemtableRowFilter,
) -> Result<bool> {
    let decoded = KvValue::Encoded(encoded_value.clone()).into_decoded(row_filter.num_columns)?;
    let has_visible_column = if let Some(selected_columns) = row_filter.selected_columns.as_ref() {
        selected_columns.iter().any(|&index| {
            decoded.columns().get(index).is_some_and(|column| {
                column
                    .as_ref()
                    .is_some_and(|column| column.value_type() != &ValueType::Delete)
            })
        })
    } else {
        decoded.columns().iter().any(|column| {
            column
                .as_ref()
                .is_some_and(|column| column.value_type() != &ValueType::Delete)
        })
    };
    Ok(has_visible_column)
}

fn collect_limited_entries_from_iter<'a, I>(
    iter: &mut I,
    seek_target: Option<&[u8]>,
    end_bound_exclusive: Option<&[u8]>,
    row_filter: Option<&MemtableRowFilter>,
    max_rows: Option<usize>,
    shadow_allowance: Option<&Arc<AtomicUsize>>,
) -> Result<(Vec<(Bytes, Bytes)>, usize)>
where
    I: KvIterator<'a>,
{
    if let Some(target) = seek_target {
        iter.seek(target)?;
    } else {
        iter.seek_to_first()?;
    }
    let mut entries = Vec::new();
    let mut visible_rows = 0usize;
    let mut filtered_shadow_rows = 0usize;
    // The local max_rows budget starts from the caller's requested visible-row cap,
    // then inherits any extra allowance already earned by newer collected memtables.
    // That inherited allowance compensates for newer invisible shadowing rows that can
    // suppress same-key values in this older collected layer during the final dedup pass.
    let effective_max_rows = max_rows.map(|max_rows| {
        max_rows + shadow_allowance.map_or(0, |allowance| allowance.load(Ordering::Relaxed))
    });
    while iter.next()? {
        if let Some((key, kv_value)) = iter.take_current()? {
            if let Some(end) = end_bound_exclusive
                && key.as_ref() >= end
            {
                break;
            }
            let encoded_value = kv_value.unwrap_encoded();
            let counts_toward_limit = if let Some(row_filter) = row_filter {
                row_counts_toward_max_rows(&encoded_value, row_filter)?
            } else {
                true
            };
            entries.push((key, encoded_value));
            if counts_toward_limit {
                visible_rows += 1;
            } else {
                filtered_shadow_rows += 1;
                // This row is already provably invisible for the projected read, but it can still
                // shadow an older same-key value. Propagate one extra slot to lower collected
                // memtables so they can fetch the next candidate instead of being truncated early.
                if let Some(shadow_allowance) = shadow_allowance {
                    shadow_allowance.fetch_add(1, Ordering::Relaxed);
                }
            }
            if let Some(max_rows) = effective_max_rows
                && visible_rows >= max_rows
            {
                break;
            }
        }
    }
    Ok((entries, filtered_shadow_rows))
}

fn collect_limited_entries_from_memtable(
    memtable: &MemtableImpl,
    seek_target: Option<&[u8]>,
    end_bound_exclusive: Option<&[u8]>,
    row_filter: Option<&MemtableRowFilter>,
    max_rows: Option<usize>,
    shadow_allowance: Option<&Arc<AtomicUsize>>,
) -> Result<(Vec<(Bytes, Bytes)>, usize)> {
    match memtable {
        MemtableImpl::Hash(hash_memtable) => {
            let mut iter = hash_memtable.iter_with_bounds(seek_target, end_bound_exclusive);
            collect_limited_entries_from_iter(
                &mut iter,
                seek_target,
                end_bound_exclusive,
                row_filter,
                max_rows,
                shadow_allowance,
            )
        }
        _ => {
            let mut iter = memtable.iter();
            collect_limited_entries_from_iter(
                &mut iter,
                seek_target,
                end_bound_exclusive,
                row_filter,
                max_rows,
                shadow_allowance,
            )
        }
    }
}

impl<'a> KvIterator<'a> for MemtableScanIterator {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.reload_entries(Some(target))
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.reload_entries(None)
    }

    fn next(&mut self) -> Result<bool> {
        if let Some(iter) = self.skiplist_iter.as_mut() {
            return iter.next();
        }
        if let Some((key, value)) = self.entries.get(self.next_index).cloned() {
            self.current_key = Some(key);
            self.current_value = Some(value);
            self.next_index += 1;
            Ok(true)
        } else {
            self.current_key = None;
            self.current_value = None;
            Ok(false)
        }
    }

    fn valid(&self) -> bool {
        if let Some(iter) = self.skiplist_iter.as_ref() {
            return iter.valid();
        }
        self.current_key.is_some() && self.current_value.is_some()
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        if let Some(iter) = self.skiplist_iter.as_ref() {
            return iter.key();
        }
        Ok(self.current_key.as_deref())
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        if let Some(iter) = self.skiplist_iter.as_mut() {
            return iter.take_key();
        }
        Ok(self.current_key.take())
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        if let Some(iter) = self.skiplist_iter.as_mut() {
            return iter.take_value();
        }
        Ok(self.current_value.take().map(KvValue::Encoded))
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
    pub(crate) fn force_close(&self) {
        self.flush_tx.lock().unwrap().take();
        let _ = self.worker.lock().unwrap().take();
    }

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
        let vlog_store = options.vlog_store.unwrap_or_else(|| {
            Arc::new(VlogStore::new(
                Arc::clone(&file_manager),
                usize::MAX,
                usize::MAX,
            ))
        });
        let writer_options_factory = WriterOptionsFactory::from(&options.writer_options);
        // Initialize the flush worker
        let (worker, flush_tx) = Self::init_flush_worker(
            Arc::clone(&state),
            Arc::clone(&flush_done),
            Arc::clone(&file_manager),
            writer_options_factory.clone(),
            Arc::clone(&lsm_tree),
            lsm_tree.ttl_provider(),
            Arc::clone(&db_lifecycle),
            Arc::clone(&vlog_store),
            metrics.clone(),
        )?;
        // Wake up writer wait loops when the DB enters error/closing state.
        db_lifecycle.register_error_notifier(&buffer_ready);
        db_lifecycle.register_error_notifier(db_state.changed_condvar());
        Ok(Self {
            state,
            buffer_ready,
            flush_done,
            file_manager,
            writer_options_factory,
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
        writer_options_factory: WriterOptionsFactory,
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
        let lsm_tree_clone = Arc::clone(&lsm_tree);
        let db_state_clone = lsm_tree_clone.db_state();
        let ttl_provider_clone = Arc::clone(&ttl_provider);
        let vlog_store_clone = Arc::clone(&vlog_store);
        let handle = std::thread::Builder::new()
            .name("cobble-flush".to_string())
            .spawn(move || {
                while let Ok(job) = flush_rx.recv() {
                    if db_lifecycle.ensure_open().is_err() {
                        // Remove immutable from state so the memtable can be
                        // fully dropped, triggering the reclaimer → buffer_ready.
                        if let Some(id) = job.memtable_id {
                            db_state_clone.remove_immutable(id);
                        }
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
                            writer_options_factory.clone(),
                            Arc::clone(&ttl_provider_clone),
                            Arc::clone(&vlog_store_clone),
                            multi_lsm_version,
                            0,
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
                                    res.data_files_by_scope.len()
                                );
                                metrics.flushes_total.increment(1);
                                let flushed_bytes: u64 = res
                                    .data_files_by_scope
                                    .iter()
                                    .map(|(_, file)| file.size as u64)
                                    .sum();
                                metrics.flush_bytes_total.increment(flushed_bytes);
                                let vlog_edit = res.vlog_edit.clone();
                                let snapshot = match lsm_tree_clone.add_level0_files(
                                    memtable_id,
                                    res.data_files_by_scope.clone(),
                                    vlog_edit,
                                ) {
                                    Ok(snapshot) => snapshot,
                                    Err(err) => {
                                        db_lifecycle.mark_error(err.clone());
                                        state.flush_results.push(Err(err));
                                        flush_done_clone.notify_all();
                                        drop(state);
                                        // Remove immutable from state so the
                                        // memtable drop triggers the reclaimer.
                                        db_state_clone.remove_immutable(memtable_id);
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
                                drop(state);
                                // Remove immutable from state so the
                                // memtable drop triggers the reclaimer.
                                if let Some(id) = job.memtable_id {
                                    db_state_clone.remove_immutable(id);
                                }
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
            MemtableImpl::Skiplist(_) => MemtableType::Skiplist,
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
                MemtableType::Skiplist => {
                    let buffer = vec![0u8; memtable_capacity];
                    MemtableImpl::Skiplist(SkiplistMemtable::with_buffer_and_reclaimer(
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
                    self.db_lifecycle.ensure_open()?;
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
            let num_columns = active
                .schema
                .num_columns_in_family(key.column_family())
                .unwrap_or(0);
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
        let latest_schema = self.schema_manager.latest_schema();
        let num_columns = latest_schema
            .num_columns_in_family(key.column_family())
            .unwrap_or_else(|| self.schema_manager.current_num_columns());
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
            self.db_lifecycle.ensure_open()?;
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_memtable_iterators_with_snapshot(
        &self,
        snapshot: Arc<DbState>,
        target_schema: Arc<Schema>,
        column_family_id: u8,
        selected_columns: Option<&[usize]>,
        start_bound_inclusive: Option<Bytes>,
        end_bound_exclusive: Option<Bytes>,
        max_rows: Option<usize>,
    ) -> Result<Vec<DynKvIterator>> {
        let mut iterators: Vec<DynKvIterator> = Vec::new();
        let target_num_columns = target_schema
            .num_columns_in_family(column_family_id)
            .unwrap_or(0);
        let shared_shadow_allowance = max_rows.map(|_| Arc::new(AtomicUsize::new(0)));
        let mut first_collecting_iterator = true;
        let mut deeper_collected_limits_allowed = true;
        if let Some(active) = &snapshot.active {
            let source_schema = {
                let active_guard = active.lock().unwrap();
                Arc::clone(&active_guard.schema)
            };
            let row_filter = build_memtable_row_filter(
                &source_schema,
                target_schema.version(),
                column_family_id,
                selected_columns,
            );
            if max_rows.is_some() && row_filter.is_none() {
                deeper_collected_limits_allowed = false;
            }
            let shadow_allowance = row_filter
                .as_ref()
                .and_then(|_| shared_shadow_allowance.as_ref().map(Arc::clone));
            let shadow_allowance_root = shadow_allowance.is_some() && first_collecting_iterator;
            if shadow_allowance.is_some() {
                first_collecting_iterator = false;
            }
            let iter: DynKvIterator = Box::new(MemtableScanIterator::for_active(
                Arc::clone(active),
                start_bound_inclusive.clone(),
                end_bound_exclusive.clone(),
                row_filter,
                max_rows,
                shadow_allowance,
                shadow_allowance_root,
            ));
            let iter: DynKvIterator = if source_schema.version() == target_schema.version() {
                iter
            } else {
                Box::new(SchemaEvolvingIterator::new(
                    iter,
                    Arc::clone(&source_schema),
                    Arc::clone(&target_schema),
                    Arc::clone(&self.schema_manager),
                ))
            };
            let iter: DynKvIterator = if let Some(columns) = selected_columns {
                Box::new(ColumnMaskingIterator::new(
                    iter,
                    target_num_columns,
                    columns,
                ))
            } else {
                iter
            };
            iterators.push(iter);
        }
        for immutable in snapshot.immutables.iter().rev() {
            let skiplist_streaming =
                matches!(immutable.memtable.as_ref(), MemtableImpl::Skiplist(_));
            let row_filter = if deeper_collected_limits_allowed && !skiplist_streaming {
                build_memtable_row_filter(
                    &immutable.schema,
                    target_schema.version(),
                    column_family_id,
                    selected_columns,
                )
            } else {
                None
            };
            let row_filter_missing = row_filter.is_none();
            let shadow_allowance = if skiplist_streaming {
                None
            } else {
                row_filter
                    .as_ref()
                    .and_then(|_| shared_shadow_allowance.as_ref().map(Arc::clone))
            };
            let shadow_allowance_root = shadow_allowance.is_some() && first_collecting_iterator;
            if shadow_allowance.is_some() {
                first_collecting_iterator = false;
            }
            let iter: DynKvIterator = Box::new(MemtableScanIterator::for_immutable(
                Arc::clone(&immutable.memtable),
                start_bound_inclusive.clone(),
                end_bound_exclusive.clone(),
                row_filter,
                if deeper_collected_limits_allowed && !skiplist_streaming {
                    max_rows
                } else {
                    None
                },
                shadow_allowance,
                shadow_allowance_root,
            ));
            let iter: DynKvIterator = if immutable.schema.version() == target_schema.version() {
                iter
            } else {
                Box::new(SchemaEvolvingIterator::new(
                    iter,
                    Arc::clone(&immutable.schema),
                    Arc::clone(&target_schema),
                    Arc::clone(&self.schema_manager),
                ))
            };
            let iter: DynKvIterator = if let Some(columns) = selected_columns {
                Box::new(ColumnMaskingIterator::new(
                    iter,
                    target_num_columns,
                    columns,
                ))
            } else {
                iter
            };
            iterators.push(iter);
            if skiplist_streaming || (max_rows.is_some() && row_filter_missing) {
                deeper_collected_limits_allowed = false;
            }
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
        guard = self.wait_for_write_stall_under_guard(guard)?;
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
                decode_active_snapshot_segments_into_memtable(source_file_manager, segments)?;
            if memtable.is_empty() {
                return Ok(false);
            }
            let multi_lsm_version = self.db_state.load().multi_lsm_version.clone();
            let result = flush_memtable(
                &memtable,
                self.schema_manager.latest_schema(),
                vlog_recorder.map(Arc::new),
                Arc::clone(&self.file_manager),
                self.writer_options_factory.clone(),
                self.lsm_tree.ttl_provider(),
                Arc::clone(&self.vlog_store),
                multi_lsm_version,
                vlog_file_seq_offset,
            )?;
            self.metrics.flushes_total.increment(1);
            let flushed_bytes: u64 = result
                .data_files_by_scope
                .iter()
                .map(|(_, file)| file.size as u64)
                .sum();
            self.metrics.flush_bytes_total.increment(flushed_bytes);
            self.lsm_tree.add_level0_files(
                Uuid::new_v4(),
                result.data_files_by_scope.clone(),
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
        if let Some(err) = self.db_lifecycle.error() {
            self.force_close();
            return Err(err);
        }
        {
            let mut tx = self.flush_tx.lock().unwrap();
            tx.take();
        }
        let (state, _) = self
            .flush_done
            .wait_timeout_while(self.state.lock().unwrap(), CLOSE_WAIT_TIMEOUT, |state| {
                state.in_flight > 0
            })
            .unwrap();
        if let Some(err) = self.db_lifecycle.error() {
            drop(state);
            self.force_close();
            return Err(err);
        }
        if state.in_flight > 0 {
            drop(state);
            self.force_close();
            return Err(Error::IoError(
                "Timed out waiting for memtable flush worker to finish during close".to_string(),
            ));
        }
        let flush_err = state
            .flush_results
            .iter()
            .find_map(|result| result.as_ref().err().cloned());
        drop(state);
        let worker = self.worker.lock().unwrap().take();
        if let Some(worker) = worker {
            worker.join().map_err(|_| {
                Error::IoError("Memtable flush worker panicked during close".to_string())
            })?;
        }
        if let Some(err) = flush_err {
            self.db_lifecycle.mark_error(err.clone());
            self.force_close();
            return Err(err);
        }
        Ok(())
    }

    fn wait_for_write_stall_under_guard<'a>(
        &self,
        mut guard: std::sync::MutexGuard<'a, ()>,
    ) -> Result<std::sync::MutexGuard<'a, ()>> {
        while Self::should_write_stall_with_snapshot(&self.db_state.load(), self.write_stall_limit)
        {
            self.db_lifecycle.ensure_open()?;
            self.metrics.write_stall_waits_total.increment(1);
            guard = self.db_state.wait_for_change(guard);
        }
        Ok(guard)
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
    writer_options_factory: WriterOptionsFactory,
    ttl_provider: Arc<crate::ttl::TTLProvider>,
    vlog_store: Arc<VlogStore>,
    multi_lsm_version: MultiLSMTreeVersion,
    vlog_file_seq_offset: u32,
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
    let mut builders: BTreeMap<usize, FlushTreeBuilder> = BTreeMap::new();
    // Try to handle merges during flush if vlog edits are present
    let merge_collector = vlog_edit.as_ref().map(|_| VlogMergeCollector::shared(true));
    let merge_callback = merge_collector.as_ref().map(VlogMergeCollector::callback);
    let mut dedup_iter = DeduplicatingIterator::new(
        PrimedIterator::new(memtable.iter()),
        None,
        ttl_provider,
        merge_callback,
        Arc::clone(&schema),
    );
    dedup_iter.seek_to_first()?;
    while dedup_iter.valid() {
        if let Some(collector) = merge_collector.as_ref() {
            collector.borrow_mut().check_error()?;
        }
        if let Some((key, kv_value)) = dedup_iter.take_current()? {
            let bucket = key_bucket(&key)
                .ok_or_else(|| InvalidState("encoded key missing bucket/cf prefix".to_string()))?;
            let column_family_id = key_column_family(&key)
                .ok_or_else(|| InvalidState("encoded key missing bucket/cf prefix".to_string()))?;
            let tree_idx = multi_lsm_version
                .tree_index_for_bucket_and_column_family(bucket, column_family_id)
                .ok_or_else(|| {
                    InvalidState(format!(
                        "bucket {} and column_family {} not mapped to an LSM tree",
                        bucket, column_family_id
                    ))
                })?;
            if let std::collections::btree_map::Entry::Vacant(entry) = builders.entry(tree_idx) {
                let tree_scope =
                    multi_lsm_version
                        .tree_scope_of_tree(tree_idx)
                        .ok_or_else(|| {
                            InvalidState(format!(
                                "missing tree scope for memtable flush tree {}",
                                tree_idx
                            ))
                        })?;
                let runtime_num_columns = schema
                    .num_columns_in_family(tree_scope.column_family_id)
                    .unwrap_or_else(|| schema.num_columns());
                let (file_id, writer) = file_manager.create_data_file_with_offload()?;
                entry.insert(FlushTreeBuilder {
                    scope: tree_scope,
                    file_id,
                    builder: make_data_file_builder_factory(
                        writer_options_factory.build(runtime_num_columns),
                    )(Box::new(writer)),
                    min_bucket: bucket,
                    max_bucket: bucket,
                });
            }
            let builder_state = builders
                .get_mut(&tree_idx)
                .expect("builder should exist for tree");
            builder_state.builder.add(&key, &kv_value)?;
            builder_state.min_bucket = builder_state.min_bucket.min(bucket);
            builder_state.max_bucket = builder_state.max_bucket.max(bucket);
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
    let mut data_files_by_scope = Vec::with_capacity(builders.len());
    let data_file_type = writer_options_factory.data_file_type();
    for (
        _,
        FlushTreeBuilder {
            scope: tree_scope,
            file_id,
            builder,
            min_bucket,
            max_bucket,
        },
    ) in builders
    {
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
        data_files_by_scope.push((tree_scope, Arc::new(data_file)));
    }
    if data_files_by_scope.is_empty() {
        return Err(Error::InvalidState(
            "flush produced no sst entries".to_string(),
        ));
    }
    Ok(MemtableFlushResult {
        data_files_by_scope,
        vlog_edit,
    })
}

fn decode_active_snapshot_segments_into_memtable(
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
        let segment_payload = MetadataReader::new(reader).read_all()?;
        let segment_bytes = segment_payload.as_ref();
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
                MemtableType::Skiplist | MemtableType::Vec => {
                    u32::from_le_bytes(kv_bytes[kv_pos..kv_pos + 4].try_into().unwrap()) as usize
                }
            };
            let value_len = match segment.memtable_type {
                MemtableType::Hash => {
                    u32::from_be_bytes(kv_bytes[kv_pos + 4..kv_pos + 8].try_into().unwrap())
                        as usize
                }
                MemtableType::Skiplist | MemtableType::Vec => {
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

    fn key(&self) -> Result<Option<&[u8]>> {
        self.inner.key()
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        self.inner.take_key()
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        self.inner.take_value()
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
                data_block_restart_interval: options.data_block_restart_interval,
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
    use crate::db_iter::{DbIterator, DbIteratorOptions};
    use crate::db_state::{DbState, DbStateHandle, MultiLSMTreeVersion};
    use crate::file::{FileManager, FileSystemRegistry};
    use crate::lsm::LSMTreeVersion;
    use crate::sst::row_codec::{decode_value, encode_key_ref_into};
    use crate::sst::{SSTIterator, SSTIteratorOptions, SSTWriterOptions};
    use crate::r#type::ValueType;
    use crate::r#type::{RefColumn, RefKey, RefValue};
    use crate::vlog::VlogStore;
    use bytes::{Bytes, BytesMut};
    use std::collections::VecDeque;
    use uuid::Uuid;

    fn cleanup_test_root() {
        let _ = std::fs::remove_dir_all("/tmp/memtable_manager_test");
    }

    fn empty_lsm_versions(len: usize) -> Vec<Arc<LSMTreeVersion>> {
        let mut v: Vec<Arc<LSMTreeVersion>> = Vec::with_capacity(len);
        (0..len).for_each(|_| v.push(Arc::new(LSMTreeVersion { levels: vec![] })));
        v
    }

    fn build_test_memtable(
        memtable_type: MemtableType,
        entries: &[(&[u8], ValueType, &[u8])],
    ) -> MemtableImpl {
        let mut memtable = match memtable_type {
            MemtableType::Hash => MemtableImpl::Hash(HashMemtable::with_capacity(4096)),
            MemtableType::Skiplist => MemtableImpl::Skiplist(SkiplistMemtable::with_capacity(4096)),
            MemtableType::Vec => MemtableImpl::Vec(VecMemtable::with_capacity(4096)),
        };
        for (key_bytes, value_type, value_bytes) in entries {
            let key = RefKey::new(0, key_bytes);
            let value = RefValue::new(vec![Some(RefColumn::new(*value_type, value_bytes))]);
            memtable.put_ref(&key, &value, 1).unwrap();
        }
        memtable
    }

    fn encode_scan_key(key: &[u8]) -> Bytes {
        let mut encoded = BytesMut::with_capacity(3 + key.len());
        encode_key_ref_into(&RefKey::new(0, key), &mut encoded);
        encoded.freeze()
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_scan_max_rows_shadow_allowance_disables_deeper_collected_limits_after_skiplist() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test")
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("memtable-shadow-allowance-test"));
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
                memtable_capacity: 4096,
                buffer_count: 2,
                num_columns: 1,
                write_stall_limit: 8,
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();
        manager.open().unwrap();

        let schema = manager.schema_manager.latest_schema();
        let active = Arc::new(Mutex::new(ActiveMemtable {
            id: Uuid::new_v4(),
            schema: Arc::clone(&schema),
            memtable: Some(build_test_memtable(
                MemtableType::Hash,
                &[
                    (b"a".as_slice(), ValueType::Delete, b""),
                    (b"c".as_slice(), ValueType::Delete, b""),
                    (b"e".as_slice(), ValueType::Delete, b""),
                ],
            )),
            vlog_recorder: None,
        }));
        let immutables = VecDeque::from(vec![
            ImmutableMemtable {
                id: Uuid::new_v4(),
                schema: Arc::clone(&schema),
                memtable: Arc::new(build_test_memtable(
                    MemtableType::Hash,
                    &[
                        (b"a".as_slice(), ValueType::Put, b"va"),
                        (b"b".as_slice(), ValueType::Put, b"vb"),
                        (b"c".as_slice(), ValueType::Put, b"vc"),
                        (b"d".as_slice(), ValueType::Put, b"vd"),
                        (b"e".as_slice(), ValueType::Put, b"ve"),
                        (b"f".as_slice(), ValueType::Put, b"vf"),
                    ],
                )),
                vlog_recorder: None,
            },
            ImmutableMemtable {
                id: Uuid::new_v4(),
                schema: Arc::clone(&schema),
                memtable: Arc::new(build_test_memtable(
                    MemtableType::Skiplist,
                    &[
                        (b"b".as_slice(), ValueType::Delete, b""),
                        (b"d".as_slice(), ValueType::Delete, b""),
                    ],
                )),
                vlog_recorder: None,
            },
        ]);
        let snapshot = Arc::new(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version: MultiLSMTreeVersion::new(LSMTreeVersion { levels: Vec::new() }),
            vlog_version: crate::vlog::VlogVersion::new(),
            active: Some(active),
            immutables,
            suggested_base_snapshot_id: None,
        });

        let start_key = encode_scan_key(b"");
        let end_key = encode_scan_key(b"\xff");
        let memtable_iters = manager
            .scan_memtable_iterators_with_snapshot(
                Arc::clone(&snapshot),
                Arc::clone(&schema),
                0,
                Some(&[0]),
                Some(start_key.clone()),
                Some(end_key.clone()),
                Some(1),
            )
            .unwrap();
        let mut iter = DbIterator::new(
            memtable_iters,
            Vec::new(),
            DbIteratorOptions {
                end_bound: Some((end_key.clone(), false)),
                max_rows: Some(1),
                snapshot,
                memtable_manager: Some(&manager),
                access_guard: None,
                vlog_store: Arc::clone(&manager.vlog_store),
                ttl_provider: manager.lsm_tree.ttl_provider(),
                schema,
                column_family_id: 0,
            },
        );
        iter.seek(start_key.as_ref()).unwrap();

        let rows: Vec<(Bytes, Vec<Option<Bytes>>)> = iter.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, Bytes::from_static(b"f"));
        assert_eq!(rows[0].1[0].as_deref(), Some(b"vf".as_slice()));

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_memtable_flush_deduplicates() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test")
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
        let data_file = results[0].as_ref().unwrap().data_files_by_scope[0]
            .1
            .clone();
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
                .first()
                .and_then(|col| col.as_ref())
                .map(|col| Bytes::copy_from_slice(col.data()))
                .unwrap_or_default();
            entries.push((key, raw));
            iter.next().unwrap();
        }
        assert_eq!(
            entries,
            vec![
                (Bytes::from_static(b"\0\0\0a"), Bytes::from("new")),
                (Bytes::from_static(b"\0\0\0b"), Bytes::from("v1"))
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
            .get_or_register("file:///tmp/memtable_manager_test")
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

        let data_file = Arc::clone(&flush_result.data_files_by_scope[0].1);
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
            .get_or_register("file:///tmp/memtable_manager_test")
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
        assert_eq!(flush_result.data_files_by_scope.len(), 2);
        assert_eq!(lsm_tree.level_files_in_tree(0, 0).len(), 1);
        assert_eq!(lsm_tree.level_files_in_tree(1, 0).len(), 1);
        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_memtable_flush_checkin_stays_in_matching_column_family_scope() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test")
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("memtable-test"));
        let file_manager =
            Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
        let db_state = Arc::new(DbStateHandle::new());
        let scopes = vec![
            LSMTreeScope::new(0u16..=0u16, 0),
            LSMTreeScope::new(0u16..=0u16, 1),
        ];
        let multi_lsm_version = MultiLSMTreeVersion::from_scopes_with_tree_versions(
            1,
            &scopes,
            empty_lsm_versions(scopes.len()),
        )
        .unwrap();
        db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version,
            vlog_version: crate::vlog::VlogVersion::new(),
            active: None,
            immutables: std::collections::VecDeque::new(),
            suggested_base_snapshot_id: None,
        });
        let lsm_tree = Arc::new(LSMTree::with_state(
            Arc::clone(&db_state),
            Arc::clone(&metrics_manager),
        ));
        let schema_manager = Arc::new(SchemaManager::new(1));
        let mut builder = schema_manager.builder();
        builder
            .add_column(0, None, None, Some("metrics".to_string()))
            .unwrap();
        let schema = builder.commit();
        let metrics_cf_id = schema.resolve_column_family_id(Some("metrics")).unwrap();
        let manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 256,
                buffer_count: 2,
                num_columns: 1,
                write_stall_limit: 8,
                schema_manager: Some(Arc::clone(&schema_manager)),
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();
        manager.open().unwrap();

        let default_key = RefKey::new(0, b"default");
        let metrics_key = RefKey::new_with_column_family(0, metrics_cf_id, b"metric");
        let value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v"))]);
        manager.put(&default_key, &value).unwrap();
        manager.put(&metrics_key, &value).unwrap();

        manager.flush_active().unwrap();
        let results = manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        let flush_result = results[0].as_ref().unwrap();
        assert_eq!(flush_result.data_files_by_scope.len(), 2);

        let mut flushed_scopes = flush_result
            .data_files_by_scope
            .iter()
            .map(|(scope, _)| scope.clone())
            .collect::<Vec<_>>();
        flushed_scopes.sort_by_key(|scope| (scope.column_family_id, *scope.bucket_range.start()));
        assert_eq!(
            flushed_scopes,
            vec![
                LSMTreeScope::new(0u16..=0u16, 0),
                LSMTreeScope::new(0u16..=0u16, metrics_cf_id),
            ]
        );
        assert_eq!(lsm_tree.level_files_in_tree(0, 0).len(), 1);
        assert_eq!(lsm_tree.level_files_in_tree(1, 0).len(), 1);
        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_restore_active_memtable_snapshot_to_l0_stays_in_matching_column_family_scope() {
        let source_root = "/tmp/memtable_manager_restore_source";
        let target_root = "/tmp/memtable_manager_restore_target";
        let _ = std::fs::remove_dir_all(source_root);
        let _ = std::fs::remove_dir_all(target_root);

        let registry = FileSystemRegistry::new();
        let source_fs = registry
            .get_or_register(format!("file://{}", source_root))
            .unwrap();
        let target_fs = registry
            .get_or_register(format!("file://{}", target_root))
            .unwrap();
        let source_metrics = Arc::new(MetricsManager::new("memtable-restore-source"));
        let target_metrics = Arc::new(MetricsManager::new("memtable-restore-target"));
        let source_file_manager =
            Arc::new(FileManager::with_defaults(source_fs, Arc::clone(&source_metrics)).unwrap());
        let target_file_manager =
            Arc::new(FileManager::with_defaults(target_fs, Arc::clone(&target_metrics)).unwrap());
        let schema_manager = Arc::new(SchemaManager::new(1));
        let mut schema_builder = schema_manager.builder();
        schema_builder
            .add_column(0, None, None, Some("metrics".to_string()))
            .unwrap();
        let schema = schema_builder.commit();
        let metrics_cf_id = schema.resolve_column_family_id(Some("metrics")).unwrap();
        let scopes = vec![
            LSMTreeScope::new(0u16..=0u16, 0),
            LSMTreeScope::new(0u16..=0u16, metrics_cf_id),
        ];
        let multi_lsm_version = MultiLSMTreeVersion::from_scopes_with_tree_versions(
            1,
            &scopes,
            empty_lsm_versions(scopes.len()),
        )
        .unwrap();

        let source_db_state = Arc::new(DbStateHandle::new());
        source_db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version: multi_lsm_version.clone(),
            vlog_version: crate::vlog::VlogVersion::new(),
            active: None,
            immutables: std::collections::VecDeque::new(),
            suggested_base_snapshot_id: None,
        });
        let source_lsm_tree = Arc::new(LSMTree::with_state(
            Arc::clone(&source_db_state),
            Arc::clone(&source_metrics),
        ));
        let source_manager = MemtableManager::new(
            Arc::clone(&source_file_manager),
            Arc::clone(&source_lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 256,
                buffer_count: 2,
                num_columns: 1,
                write_stall_limit: 8,
                schema_manager: Some(Arc::clone(&schema_manager)),
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();
        source_manager.open().unwrap();

        let default_key = RefKey::new(0, b"default");
        let metrics_key = RefKey::new_with_column_family(0, metrics_cf_id, b"metric");
        let value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"v"))]);
        source_manager.put(&default_key, &value).unwrap();
        source_manager.put(&metrics_key, &value).unwrap();

        let active = source_manager
            .db_state
            .load()
            .active
            .clone()
            .expect("source active memtable should exist");
        let snapshot_manager = SnapshotManager::new(
            Arc::clone(&source_file_manager),
            Arc::clone(&schema_manager),
            Arc::new(DbLifecycle::new_open()),
            None,
            vec![0u16..=0u16],
        );
        let snapshot_write = MemtableManager::write_active_memtable_snapshot_data(
            1,
            None,
            &active,
            &snapshot_manager,
            &source_file_manager,
        )
        .unwrap();

        let target_db_state = Arc::new(DbStateHandle::new());
        target_db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version,
            vlog_version: crate::vlog::VlogVersion::new(),
            active: None,
            immutables: std::collections::VecDeque::new(),
            suggested_base_snapshot_id: None,
        });
        let target_lsm_tree = Arc::new(LSMTree::with_state(
            Arc::clone(&target_db_state),
            Arc::clone(&target_metrics),
        ));
        let target_vlog_store = Arc::new(VlogStore::new(Arc::clone(&target_file_manager), 64, 8));
        let target_manager = MemtableManager::new(
            Arc::clone(&target_file_manager),
            Arc::clone(&target_lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 256,
                buffer_count: 2,
                num_columns: 1,
                write_stall_limit: 8,
                schema_manager: Some(Arc::clone(&schema_manager)),
                vlog_store: Some(target_vlog_store),
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();
        target_manager.open().unwrap();

        let restored = target_manager
            .restore_active_memtable_snapshot_to_l0(
                &source_file_manager,
                &snapshot_write.active_data,
                0,
            )
            .unwrap();

        assert!(restored);
        assert_eq!(target_lsm_tree.level_files_in_tree(0, 0).len(), 1);
        assert_eq!(target_lsm_tree.level_files_in_tree(1, 0).len(), 1);

        let _ = std::fs::remove_dir_all(source_root);
        let _ = std::fs::remove_dir_all(target_root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_restore_active_memtable_snapshot_to_l0_preserves_shared_vlog_across_scopes() {
        let source_root = "/tmp/memtable_manager_restore_vlog_source";
        let target_root = "/tmp/memtable_manager_restore_vlog_target";
        let _ = std::fs::remove_dir_all(source_root);
        let _ = std::fs::remove_dir_all(target_root);

        let registry = FileSystemRegistry::new();
        let source_fs = registry
            .get_or_register(format!("file://{}", source_root))
            .unwrap();
        let target_fs = registry
            .get_or_register(format!("file://{}", target_root))
            .unwrap();
        let source_metrics = Arc::new(MetricsManager::new("memtable-restore-vlog-source"));
        let target_metrics = Arc::new(MetricsManager::new("memtable-restore-vlog-target"));
        let source_file_manager =
            Arc::new(FileManager::with_defaults(source_fs, Arc::clone(&source_metrics)).unwrap());
        let target_file_manager =
            Arc::new(FileManager::with_defaults(target_fs, Arc::clone(&target_metrics)).unwrap());
        let schema_manager = Arc::new(SchemaManager::new(1));
        let mut schema_builder = schema_manager.builder();
        schema_builder
            .add_column(0, None, None, Some("metrics".to_string()))
            .unwrap();
        let schema = schema_builder.commit();
        let metrics_cf_id = schema.resolve_column_family_id(Some("metrics")).unwrap();
        let scopes = vec![
            LSMTreeScope::new(0u16..=0u16, 0),
            LSMTreeScope::new(0u16..=0u16, metrics_cf_id),
        ];
        let multi_lsm_version = MultiLSMTreeVersion::from_scopes_with_tree_versions(
            1,
            &scopes,
            empty_lsm_versions(scopes.len()),
        )
        .unwrap();

        let source_db_state = Arc::new(DbStateHandle::new());
        source_db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version: multi_lsm_version.clone(),
            vlog_version: crate::vlog::VlogVersion::new(),
            active: None,
            immutables: std::collections::VecDeque::new(),
            suggested_base_snapshot_id: None,
        });
        let source_lsm_tree = Arc::new(LSMTree::with_state(
            Arc::clone(&source_db_state),
            Arc::clone(&source_metrics),
        ));
        let source_vlog_store = Arc::new(VlogStore::new(Arc::clone(&source_file_manager), 64, 8));
        let source_manager = MemtableManager::new(
            Arc::clone(&source_file_manager),
            Arc::clone(&source_lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 256,
                buffer_count: 2,
                num_columns: 1,
                write_stall_limit: 8,
                schema_manager: Some(Arc::clone(&schema_manager)),
                vlog_store: Some(source_vlog_store),
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();
        source_manager.open().unwrap();

        let long_value = RefValue::new(vec![Some(RefColumn::new(
            ValueType::Put,
            b"value-larger-than-threshold",
        ))]);
        let default_key = RefKey::new(0, b"default");
        let metrics_key = RefKey::new_with_column_family(0, metrics_cf_id, b"metric");
        source_manager.put(&default_key, &long_value).unwrap();
        source_manager.put(&metrics_key, &long_value).unwrap();

        let active = source_manager
            .db_state
            .load()
            .active
            .clone()
            .expect("source active memtable should exist");
        let snapshot_manager = SnapshotManager::new(
            Arc::clone(&source_file_manager),
            Arc::clone(&schema_manager),
            Arc::new(DbLifecycle::new_open()),
            None,
            vec![0u16..=0u16],
        );
        let snapshot_write = MemtableManager::write_active_memtable_snapshot_data(
            1,
            None,
            &active,
            &snapshot_manager,
            &source_file_manager,
        )
        .unwrap();

        let target_db_state = Arc::new(DbStateHandle::new());
        target_db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version,
            vlog_version: crate::vlog::VlogVersion::new(),
            active: None,
            immutables: std::collections::VecDeque::new(),
            suggested_base_snapshot_id: None,
        });
        let target_lsm_tree = Arc::new(LSMTree::with_state(
            Arc::clone(&target_db_state),
            Arc::clone(&target_metrics),
        ));
        let target_vlog_store = Arc::new(VlogStore::new(Arc::clone(&target_file_manager), 64, 8));
        let target_manager = MemtableManager::new(
            Arc::clone(&target_file_manager),
            Arc::clone(&target_lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 256,
                buffer_count: 2,
                num_columns: 1,
                write_stall_limit: 8,
                schema_manager: Some(Arc::clone(&schema_manager)),
                vlog_store: Some(target_vlog_store),
                ..MemtableManagerOptions::default()
            },
        )
        .unwrap();
        target_manager.open().unwrap();

        let restored = target_manager
            .restore_active_memtable_snapshot_to_l0(
                &source_file_manager,
                &snapshot_write.active_data,
                0,
            )
            .unwrap();

        assert!(restored);
        let tree0_files = target_lsm_tree.level_files_in_tree(0, 0);
        let tree1_files = target_lsm_tree.level_files_in_tree(1, 0);
        assert_eq!(tree0_files.len(), 1);
        assert_eq!(tree1_files.len(), 1);
        assert!(tree0_files[0].has_separated_values());
        assert!(tree1_files[0].has_separated_values());
        assert_eq!(tree0_files[0].vlog_file_seq_offset, 0);
        assert_eq!(tree1_files[0].vlog_file_seq_offset, 0);

        let files_with_entries = target_db_state.load().vlog_version.files_with_entries();
        assert_eq!(files_with_entries.len(), 1);
        assert_eq!(files_with_entries[0].2, 2);

        let _ = std::fs::remove_dir_all(source_root);
        let _ = std::fs::remove_dir_all(target_root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_memtable_schema_change_triggers_flush_and_preserves_flush_schema() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test")
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
        builder.add_column(1, None, None, None).unwrap();
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
            results[0].as_ref().unwrap().data_files_by_scope[0]
                .1
                .schema_id,
            0
        );

        manager.flush_active().unwrap();
        let results = manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        let second_file = results[0].as_ref().unwrap().data_files_by_scope[0]
            .1
            .clone();
        assert_eq!(second_file.schema_id, 1);
        let reader = file_manager
            .open_data_file_reader(second_file.file_id)
            .unwrap();
        let mut iter = SSTIterator::with_cache_and_file(
            Box::new(reader),
            second_file.as_ref(),
            SSTIteratorOptions {
                num_columns: 2,
                ..Default::default()
            },
            None,
        )
        .unwrap();
        iter.seek_to_first().unwrap();
        let (_, mut value) = iter.current().unwrap().unwrap();
        let decoded = decode_value(&mut value, 2).unwrap();
        assert_eq!(decoded.columns().len(), 2);
        assert_eq!(
            decoded.columns()[0].as_ref().unwrap().data().as_ref(),
            b"v2"
        );
        assert_eq!(
            decoded.columns()[1].as_ref().unwrap().data().as_ref(),
            b"v2c1"
        );

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_memtable_reuses_buffer_after_flush() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test")
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
            .get_or_register("file:///tmp/memtable_manager_test")
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
            .get_or_register("file:///tmp/memtable_manager_test")
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

    #[test]
    #[serial_test::serial(file)]
    fn test_skiplist_memtable_triggers_flush_on_full() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test")
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
                memtable_capacity: 224,
                buffer_count: 2,
                memtable_type: MemtableType::Skiplist,
                writer_options: WriterOptions::Sst(SSTWriterOptions {
                    bloom_filter_enabled: true,
                    ..SSTWriterOptions::default()
                }),
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
}
