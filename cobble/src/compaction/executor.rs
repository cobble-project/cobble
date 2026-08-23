//! Compaction executor for running compaction tasks.
//!
//! This module provides the core compaction functionality including:
//! - `CompactionTask`: Parameters for a single compaction operation
//! - `CompactionResult`: Output of a compaction operation
//! - `CompactionExecutor`: Manages compaction execution in a thread pool

use crate::cache::{
    BlockCache, BlockCachePreload, ScanHotBlockRegistry, WriterHotBlockCache,
    bucket_scoped_cache_namespace,
};
use crate::compaction::CompactionConfig;
use crate::data_file::{DataFile, DataFileType};
use crate::db_state::{TruncationCursorId, TruncationCursorMap};
use crate::db_status::DbLifecycle;
use crate::error::Result;
use crate::file::{FileManager, ReadAheadBufferedReader, TrackedFileId, read_ahead_runtime};
use crate::format::{FileBuildResult, FileBuilder, FileBuilderFactory};
use crate::iterator::{
    BucketFilterIterator, DeduplicatingIterator, KvIterator, MergingIterator,
    SchemaEvolvingIterator, SortedRun, VlogSeqOffsetIterator,
};
use crate::lsm::{LevelEdit, VersionEdit};
use crate::schema::{DEFAULT_COLUMN_FAMILY_ID, SchemaManager};
use crate::sst::{SSTIteratorMetrics, SSTIteratorOptions, SSTWriter};
use crate::r#type::{ENCODED_KEY_PREFIX_BYTES, key_bucket, key_column_family};
use crate::vlog::{VlogEdit, VlogMergeCollector};
use crate::writer_options::{WriterOptions, WriterOptionsFactory};
use log::trace;
use metrics::{Counter, counter};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Condvar, Mutex};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

type CompactionCompleteCallback =
    Arc<dyn Fn(usize, VersionEdit, Option<VlogEdit>, Vec<BlockCachePreload>) + Send + Sync>;

pub(crate) fn build_compaction_runtime(thread_name: &str, max_threads: usize) -> Result<Runtime> {
    let max_threads = max_threads.max(1);
    // Compaction runs through `spawn_blocking`; Tokio's `worker_threads` setting alone does not
    // limit that pool. Cap both pools so `compaction_threads` is the actual concurrency bound.
    tokio::runtime::Builder::new_multi_thread()
        .thread_name(thread_name)
        .worker_threads(max_threads)
        .max_blocking_threads(max_threads)
        .enable_all()
        .build()
        .map_err(|e| crate::error::Error::IoError(e.to_string()))
}

/// Tracks compaction work accepted by a runtime so shutdown can drain it before LSM teardown.
///
/// Tokio may keep queued blocking tasks beyond a timeout-based runtime shutdown. Those tasks own
/// output files and invoke completion callbacks, so letting them outlive the LSM can lose an edit
/// or race resource cleanup.
pub(crate) struct BlockingTaskTracker {
    state: Mutex<BlockingTaskState>,
    idle: Condvar,
}

struct BlockingTaskState {
    accepting: bool,
    pending: usize,
}

impl BlockingTaskTracker {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(BlockingTaskState {
                accepting: true,
                pending: 0,
            }),
            idle: Condvar::new(),
        }
    }

    pub(crate) fn spawn<F, T>(self: &Arc<Self>, runtime: &Runtime, task: F) -> Option<JoinHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let mut state = self.state.lock().unwrap();
        if !state.accepting {
            return None;
        }
        state.pending += 1;
        drop(state);

        let tracker = Arc::clone(self);
        Some(runtime.spawn_blocking(move || {
            let _guard = BlockingTaskGuard(tracker);
            task()
        }))
    }

    pub(crate) fn close_and_wait(&self) {
        let mut state = self.state.lock().unwrap();
        // Close admission before waiting so the pending count can only move toward zero.
        state.accepting = false;
        while state.pending != 0 {
            state = self.idle.wait(state).unwrap();
        }
    }
}

impl Default for BlockingTaskTracker {
    fn default() -> Self {
        Self::new()
    }
}

struct BlockingTaskGuard(Arc<BlockingTaskTracker>);

impl Drop for BlockingTaskGuard {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.pending -= 1;
        if state.pending == 0 {
            self.0.idle.notify_all();
        }
    }
}

fn single_bucket_in_range(range: &std::ops::RangeInclusive<u16>) -> Option<u16> {
    let start = *range.start();
    (start == *range.end()).then_some(start)
}

/// A compaction task describes the input and output parameters for a compaction.
pub struct CompactionTask {
    lsm_tree_idx: usize,
    metrics: Arc<CompactionTaskMetrics>,
    sst_metrics: Arc<SSTIteratorMetrics>,
    /// The sorted runs to compact.
    sorted_runs: Vec<SortedRun>,
    output_level: u8,
    /// The file manager to use for reading/writing files.
    file_manager: Arc<FileManager>,
    /// Factory function for creating FileBuilder instances.
    file_builder_factory: Arc<FileBuilderFactory>,
    writer_options_factory: Option<WriterOptionsFactory>,
    /// The data file type for output files.
    data_file_type: DataFileType,
    /// TTL provider for compaction to determine if entries are expired and can be dropped.
    ttl_provider: Arc<crate::ttl::TTLProvider>,
    /// Whether to create output files in read-only mode.
    /// This is used for remote compaction workers where we want to write files.
    output_files_readonly: bool,
    schema_manager: Arc<SchemaManager>,
    column_family_id: u8,
    num_columns: usize,
    truncation_cursors: TruncationCursorMap,
    cache_namespace: u64,
    scan_hot_blocks: Option<Arc<ScanHotBlockRegistry>>,
    /// Optional output path prefix (relative to the file manager base dir) for output files.
    /// When set, output files are written under `{base_dir}/{prefix}/` instead of
    /// `{base_dir}/data/`. Used by the dedicated compactor to isolate per-job outputs.
    output_path_prefix: Option<String>,
}

#[derive(Clone)]
pub(crate) struct CompactionTaskMetrics {
    completed_total: Counter,
    read_bytes_total: Counter,
    write_bytes_total: Counter,
}

impl CompactionTaskMetrics {
    pub(crate) fn new(db_id: &str) -> Self {
        let db_id = db_id.to_string();
        Self {
            completed_total: counter!("compactions_total", "db_id" => db_id.clone()),
            read_bytes_total: counter!("compaction_read_bytes_total", "db_id" => db_id.clone()),
            write_bytes_total: counter!("compaction_write_bytes_total", "db_id" => db_id),
        }
    }

    pub(crate) fn record_completed(&self) {
        self.completed_total.increment(1);
    }

    pub(crate) fn record_read_bytes(&self, bytes: u64) {
        self.read_bytes_total.increment(bytes);
    }

    pub(crate) fn record_write_bytes(&self, bytes: u64) {
        self.write_bytes_total.increment(bytes);
    }
}

impl CompactionTask {
    /// Creates a new compaction task.
    ///
    /// # Arguments
    /// * `sorted_runs` - The sorted runs to merge together
    /// * `file_manager` - The file manager for reading input files and writing output files
    /// * `file_builder_factory` - Factory function for creating FileBuilder instances
    /// * `data_file_type` - The data file type for output files
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metrics: Arc<CompactionTaskMetrics>,
        sst_metrics: Arc<SSTIteratorMetrics>,
        lsm_tree_idx: usize,
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
        file_manager: Arc<FileManager>,
        file_builder_factory: Arc<FileBuilderFactory>,
        data_file_type: DataFileType,
        ttl_provider: Arc<crate::ttl::TTLProvider>,
        schema_manager: Arc<SchemaManager>,
    ) -> Self {
        let default_num_columns = schema_manager.current_num_columns();
        Self {
            lsm_tree_idx,
            metrics,
            sst_metrics,
            sorted_runs,
            output_level,
            file_manager,
            file_builder_factory,
            writer_options_factory: None,
            data_file_type,
            ttl_provider,
            output_files_readonly: false,
            schema_manager,
            column_family_id: DEFAULT_COLUMN_FAMILY_ID,
            num_columns: default_num_columns,
            truncation_cursors: TruncationCursorMap::new(),
            cache_namespace: 0,
            scan_hot_blocks: None,
            output_path_prefix: None,
        }
    }

    pub fn with_readonly_outputs(mut self) -> Self {
        self.output_files_readonly = true;
        self
    }

    pub(crate) fn with_writer_options_factory(
        mut self,
        writer_options_factory: WriterOptionsFactory,
    ) -> Self {
        self.writer_options_factory = Some(writer_options_factory);
        self
    }

    pub(crate) fn with_column_family(mut self, column_family_id: u8, num_columns: usize) -> Self {
        self.column_family_id = column_family_id;
        self.num_columns = num_columns;
        self
    }

    pub(crate) fn with_truncation_cursors(
        mut self,
        truncation_cursors: TruncationCursorMap,
    ) -> Self {
        self.truncation_cursors = truncation_cursors;
        self
    }

    pub(crate) fn with_scan_hot_block_cache(
        mut self,
        block_cache: Option<BlockCache>,
        cache_namespace: u64,
        scan_hot_blocks: Arc<ScanHotBlockRegistry>,
    ) -> Self {
        if block_cache.is_some() {
            self.cache_namespace = cache_namespace;
            self.scan_hot_blocks = Some(scan_hot_blocks);
        }
        self
    }

    pub(crate) fn with_scan_hot_blocks(
        mut self,
        cache_namespace: u64,
        scan_hot_blocks: Arc<ScanHotBlockRegistry>,
    ) -> Self {
        self.cache_namespace = cache_namespace;
        self.scan_hot_blocks = Some(scan_hot_blocks);
        self
    }

    /// Sets a prefix for output file paths (relative to the file manager base dir).
    /// When set, output files are written under `{base_dir}/{prefix}/` instead of
    /// `{base_dir}/data/`. Used by the dedicated compactor to isolate per-job outputs.
    pub fn with_output_path_prefix(mut self, prefix: String) -> Self {
        self.output_path_prefix = Some(prefix);
        self
    }

    /// Returns the sorted runs in this task.
    pub fn sorted_runs(&self) -> &[SortedRun] {
        &self.sorted_runs
    }

    pub fn output_level(&self) -> u8 {
        self.output_level
    }

    pub fn ttl_provider(&self) -> Arc<crate::ttl::TTLProvider> {
        Arc::clone(&self.ttl_provider)
    }
}

/// The result of a compaction operation.
pub struct CompactionResult {
    lsm_tree_idx: usize,
    /// New files created by the compaction.
    /// Files are sorted by their key ranges (first key of each file is sorted).
    new_files: Vec<Arc<DataFile>>,
    edit: VersionEdit,
    vlog_edit: Option<VlogEdit>,
    /// Output SST data blocks that should be warmed in the local block cache.
    ///
    /// See the full hot-block handoff in `cache::ScanHotBlockRegistry`:
    /// scan iterators register current/next input block keys, compaction input
    /// iterators observe those keys, SST writers record output block preload
    /// requests here, and local/remote completion paths asynchronously load them.
    preload_block_keys: Vec<BlockCachePreload>,
}

impl CompactionResult {
    /// Creates a new compaction result.
    pub fn new(
        lsm_tree_idx: usize,
        new_files: Vec<Arc<DataFile>>,
        edit: VersionEdit,
        vlog_edit: Option<VlogEdit>,
        preload_block_keys: Vec<BlockCachePreload>,
    ) -> Self {
        Self {
            lsm_tree_idx,
            new_files,
            edit,
            vlog_edit,
            preload_block_keys,
        }
    }

    /// Returns the new files created by compaction.
    /// Files are sorted by their key ranges.
    pub fn new_files(&self) -> &[Arc<DataFile>] {
        &self.new_files
    }

    pub fn edit(&self) -> &VersionEdit {
        &self.edit
    }

    pub fn vlog_edit(&self) -> Option<&VlogEdit> {
        self.vlog_edit.as_ref()
    }

    pub(crate) fn preload_block_keys(&self) -> &[BlockCachePreload] {
        &self.preload_block_keys
    }
}

/// Executor for running compaction tasks.
///
/// The executor uses tokio's runtime for async task execution in a thread pool.
pub struct CompactionExecutor {
    runtime: Option<Arc<Runtime>>,
    tasks: Arc<BlockingTaskTracker>,
    options: CompactionConfig,
    db_lifecycle: Arc<DbLifecycle>,
}

impl CompactionExecutor {
    /// Creates a new compaction executor with the given options and its own runtime.
    pub fn new(options: CompactionConfig, db_lifecycle: Arc<DbLifecycle>) -> Result<Self> {
        Self::new_with_runtime(
            options,
            Arc::new(build_compaction_runtime(
                "cobble-compaction",
                options.max_threads,
            )?),
            db_lifecycle,
        )
    }

    /// Creates a new compaction executor with the given options and thread count.
    pub fn new_with_runtime(
        options: CompactionConfig,
        runtime: Arc<Runtime>,
        db_lifecycle: Arc<DbLifecycle>,
    ) -> Result<Self> {
        Ok(Self {
            runtime: Some(runtime),
            tasks: Arc::new(BlockingTaskTracker::new()),
            options,
            db_lifecycle,
        })
    }

    /// Blocks on a JoinHandle using the executor's internal runtime.
    ///
    /// # Panics
    /// Panics if the executor was created without a runtime.
    pub fn block_on_handle<T>(
        &self,
        handle: tokio::task::JoinHandle<T>,
    ) -> std::result::Result<T, tokio::task::JoinError> {
        let runtime = self.runtime.as_ref().expect("Executor has no runtime.");
        runtime.block_on(handle)
    }

    /// Creates a new compaction executor with the given options without its own runtime.
    /// Use this when running in an existing tokio runtime.
    pub fn new_without_runtime(options: CompactionConfig, db_lifecycle: Arc<DbLifecycle>) -> Self {
        Self {
            runtime: None,
            tasks: Arc::new(BlockingTaskTracker::new()),
            options,
            db_lifecycle,
        }
    }

    /// Creates a new compaction executor with default options.
    pub fn with_defaults(db_lifecycle: Arc<DbLifecycle>) -> Result<Self> {
        Self::new(CompactionConfig::default(), db_lifecycle)
    }

    /// Executes a compaction task asynchronously using the executor's internal runtime.
    ///
    /// The compaction process:
    /// 1. Creates iterators for all input sorted runs
    /// 2. Merges them using MergingIterator
    /// 3. Deduplicates entries using DeduplicatingIterator
    /// 4. Writes output to new SST files, starting a new file when target_file_size is exceeded
    /// 5. Returns the list of new files with their key ranges sorted
    ///
    /// # Panics
    /// Panics if the executor was created without a runtime.
    pub fn execute(
        &self,
        task: CompactionTask,
        on_complete: Option<CompactionCompleteCallback>,
    ) -> tokio::task::JoinHandle<Result<CompactionResult>> {
        let runtime = self.runtime.as_ref().expect("Executor has no runtime.");
        let options = self.options;
        let db_lifecycle = Arc::clone(&self.db_lifecycle);

        self.tasks
            .spawn(runtime, move || {
                let result = Self::run_compaction(task, options);
                match result {
                    Ok(result) => {
                        if let Some(callback) = on_complete {
                            callback(
                                result.lsm_tree_idx,
                                result.edit.clone(),
                                result.vlog_edit.clone(),
                                result.preload_block_keys.clone(),
                            );
                        }
                        Ok(result)
                    }
                    Err(err) => {
                        db_lifecycle.mark_error(err.clone());
                        Err(err)
                    }
                }
            })
            .expect("Executor has been shutdown.")
    }

    /// Executes a compaction task synchronously and blocks until completion.
    pub fn execute_blocking(
        &self,
        task: CompactionTask,
        on_complete: Option<CompactionCompleteCallback>,
    ) -> Result<CompactionResult> {
        let db_lifecycle = Arc::clone(&self.db_lifecycle);
        let result = Self::run_compaction(task, self.options);
        let result = match result {
            Ok(result) => result,
            Err(err) => {
                db_lifecycle.mark_error(err.clone());
                return Err(err);
            }
        };
        if let Some(callback) = on_complete {
            callback(
                result.lsm_tree_idx,
                result.edit.clone(),
                result.vlog_edit.clone(),
                result.preload_block_keys.clone(),
            );
        }
        Ok(result)
    }

    pub fn shutdown(&mut self) {
        self.tasks.close_and_wait();
        if let Some(runtime) = self.runtime.take()
            && let Ok(runtime) = Arc::try_unwrap(runtime)
        {
            drop(runtime);
        }
    }

    fn run_compaction(task: CompactionTask, options: CompactionConfig) -> Result<CompactionResult> {
        let mut all_iters: Vec<Box<dyn for<'a> KvIterator<'a>>> = Vec::new();
        let mut read_bytes = 0u64;
        let use_read_ahead = options.read_ahead_enabled;
        let target_schema = task.schema_manager.latest_schema();
        let column_family_id = task.column_family_id;
        let num_columns = target_schema
            .num_columns_in_family(column_family_id)
            .unwrap_or(task.num_columns);
        let value_has_ttl = target_schema.value_has_ttl_in_family(column_family_id);
        let hot_observed_cursor = task
            .scan_hot_blocks
            .as_ref()
            .map(|registry| Arc::new(AtomicU64::new(registry.observed_count())));
        let preload_block_keys = task
            .scan_hot_blocks
            .as_ref()
            .map(|_| Arc::new(Mutex::new(Vec::<BlockCachePreload>::new())));
        for run in &task.sorted_runs {
            for file in run.files() {
                read_bytes = read_bytes.saturating_add(file.size as u64);
            }
            let file_manager = Arc::clone(&task.file_manager);
            let sst_metrics = Arc::clone(&task.sst_metrics);
            let schema_manager = Arc::clone(&task.schema_manager);
            let target_schema = Arc::clone(&target_schema);
            let scan_hot_blocks = task.scan_hot_blocks.as_ref().map(Arc::clone);
            let base_cache_namespace = task.cache_namespace;
            let pin_metadata = options
                .pinned_metadata_max_level
                .is_some_and(|max_level| run.level() <= max_level);
            let run_iter = run.iter(move |file| {
                let source_schema = schema_manager.schema(file.schema_id)?;
                let source_num_columns = source_schema
                    .num_columns_in_family(column_family_id)
                    .unwrap_or_else(|| source_schema.num_columns());
                let reader = file_manager.open_data_file_reader(file.file_id)?;
                let reader: Box<dyn crate::file::RandomAccessFile> = if use_read_ahead {
                    Box::new(ReadAheadBufferedReader::new(
                        reader,
                        options.read_buffer_size,
                        read_ahead_runtime(),
                    ))
                } else {
                    Box::new(reader)
                };
                let base_iter: Box<dyn for<'a> KvIterator<'a>> = match file.file_type {
                    DataFileType::SSTable => {
                        let sst_options = SSTIteratorOptions {
                            metrics: Some(Arc::clone(&sst_metrics)),
                            num_columns: source_num_columns,
                            bloom_filter_enabled: options.bloom_filter_enabled,
                            read_metadata_cache_mode: options.read_metadata_cache_mode,
                            pin_metadata,
                            pin_metadata_partitions: options.pinned_metadata_partitions_enabled,
                            cache_namespace: single_bucket_in_range(&file.effective_bucket_range)
                                .map(|bucket| {
                                    bucket_scoped_cache_namespace(base_cache_namespace, bucket)
                                })
                                .unwrap_or(base_cache_namespace),
                            // This is the compaction-reader half of the hot-block handoff
                            // documented on `ScanHotBlockRegistry`. It does not change
                            // compaction selection; it only increments the registry counter
                            // when this input iterator reads a block that an active scan
                            // marked as current/next.
                            hot_block_registry: scan_hot_blocks.as_ref().map(Arc::clone),
                            observe_hot_blocks: scan_hot_blocks.is_some(),
                            ..SSTIteratorOptions::default()
                        };
                        let iter = crate::sst::SSTIterator::with_cache_and_file(
                            reader,
                            file,
                            sst_options,
                            None,
                        )?;
                        if file.needs_bucket_filter() {
                            Box::new(BucketFilterIterator::new(
                                iter,
                                file.effective_bucket_range.clone(),
                            ))
                        } else {
                            Box::new(iter)
                        }
                    }
                    DataFileType::Parquet => {
                        let cache_namespace = single_bucket_in_range(&file.effective_bucket_range)
                            .map(|bucket| {
                                bucket_scoped_cache_namespace(base_cache_namespace, bucket)
                            })
                            .unwrap_or(base_cache_namespace);
                        let iter = crate::parquet::ParquetIterator::from_data_file_with_options(
                            reader,
                            file,
                            None,
                            None,
                            crate::parquet::ParquetIteratorOptions {
                                cache_namespace,
                                hot_block_registry: scan_hot_blocks.as_ref().map(Arc::clone),
                                observe_hot_blocks: scan_hot_blocks.is_some(),
                                ..Default::default()
                            },
                        )?;
                        if file.needs_bucket_filter() {
                            Box::new(BucketFilterIterator::new(
                                iter,
                                file.effective_bucket_range.clone(),
                            ))
                        } else {
                            Box::new(iter)
                        }
                    }
                };
                let iter: Box<dyn for<'a> KvIterator<'a>> =
                    if file.schema_id == target_schema.version() {
                        base_iter
                    } else {
                        Box::new(SchemaEvolvingIterator::new(
                            base_iter,
                            Arc::clone(&source_schema),
                            Arc::clone(&target_schema),
                            Arc::clone(&schema_manager),
                            column_family_id,
                        ))
                    };
                if file.vlog_file_seq_offset == 0 {
                    Ok(iter)
                } else {
                    Ok(Box::new(VlogSeqOffsetIterator::new(
                        iter,
                        num_columns,
                        file.vlog_file_seq_offset,
                    )))
                }
            });
            all_iters.push(Box::new(run_iter));
        }
        task.metrics.record_read_bytes(read_bytes);

        // Create merging iterator
        let merging_iter = MergingIterator::new(all_iters);
        let input_has_separated_values = task
            .sorted_runs
            .iter()
            .flat_map(|run| run.files().iter())
            .any(|file| file.has_separated_values());
        let merge_collector = input_has_separated_values.then(|| VlogMergeCollector::shared(true));
        let merge_callback = merge_collector.as_ref().map(VlogMergeCollector::callback);
        let expired_callback = merge_collector
            .as_ref()
            .map(VlogMergeCollector::expired_value_callback);

        // Create deduplicating iterator
        let mut dedup_iter = DeduplicatingIterator::new_for_sst_build(
            merging_iter,
            Some(num_columns),
            task.ttl_provider(),
            merge_callback,
            expired_callback,
            Arc::clone(&target_schema),
        );
        dedup_iter.seek_to_first()?;

        // Collect output files
        let mut output_files: Vec<Arc<DataFile>> = Vec::new();
        let mut written_bytes = 0u64;

        // Process entries and write to output files using the FileBuilder trait
        let mut current_builder: Option<Box<dyn FileBuilder>> = None;
        let mut current_file_id: Option<u64> = None;

        while dedup_iter.valid() {
            if let Some(collector) = merge_collector.as_ref() {
                collector.borrow_mut().check_error()?;
            }
            let (key, kv_value) = match dedup_iter.take_current()? {
                Some(kv) => kv,
                None => break,
            };
            if key_is_truncated_by_cursor_map(&task.truncation_cursors, &key) {
                if let Some(collector) = merge_collector.as_ref() {
                    let value = kv_value.into_decoded(num_columns)?;
                    collector
                        .borrow_mut()
                        .collect_removed_entries_from_value(&value)?;
                    collector.borrow_mut().check_error()?;
                }
                dedup_iter.next()?;
                continue;
            }

            // Check if we need to start a new file
            if current_builder.is_none() {
                let (file_id, writer) = if let Some(prefix) = &task.output_path_prefix {
                    task.file_manager.create_data_file_with_prefix(prefix)?
                } else if task.output_files_readonly {
                    task.file_manager.create_data_file()?
                } else {
                    task.file_manager.create_data_file_with_offload()?
                };
                current_file_id = Some(file_id);
                current_builder = Some(
                    if let Some(writer_options_factory) = task.writer_options_factory.as_ref() {
                        match writer_options_factory.build(num_columns, value_has_ttl) {
                            WriterOptions::Sst(options) => {
                                // The writer side consumes observations from the same registry.
                                // When the compaction reader above touches a scan-hot input block,
                                // `SSTWriter` records the output block key in `preload_block_keys`;
                                // the completion path later loads those keys asynchronously.
                                let hot_block_cache = task
                                    .scan_hot_blocks
                                    .as_ref()
                                    .zip(hot_observed_cursor.as_ref())
                                    .zip(preload_block_keys.as_ref())
                                    .map(|((hot_blocks, observed_cursor), preloads)| {
                                        WriterHotBlockCache {
                                            hot_blocks: Arc::clone(hot_blocks),
                                            observed_cursor: Arc::clone(observed_cursor),
                                            preloads: Arc::clone(preloads),
                                        }
                                    });
                                let writer: Box<dyn crate::file::SequentialWriteFile> =
                                    Box::new(writer);
                                Box::new(SSTWriter::new_with_hot_block_cache(
                                    writer,
                                    options,
                                    hot_block_cache,
                                    Some(task.cache_namespace),
                                    Some(file_id),
                                )) as Box<dyn FileBuilder>
                            }
                            WriterOptions::Parquet(options) => {
                                let hot_block_cache = task
                                    .scan_hot_blocks
                                    .as_ref()
                                    .zip(hot_observed_cursor.as_ref())
                                    .zip(preload_block_keys.as_ref())
                                    .map(|((hot_blocks, observed_cursor), preloads)| {
                                        WriterHotBlockCache {
                                            hot_blocks: Arc::clone(hot_blocks),
                                            observed_cursor: Arc::clone(observed_cursor),
                                            preloads: Arc::clone(preloads),
                                        }
                                    });
                                Box::new(crate::parquet::ParquetWriter::with_options_and_hot_block_cache(
                                    writer,
                                    options,
                                    hot_block_cache,
                                    Some(task.cache_namespace),
                                    Some(file_id),
                                )?) as Box<dyn FileBuilder>
                            }
                        }
                    } else {
                        (task.file_builder_factory)(Box::new(writer))
                    },
                );
            }

            // Add entry to current file
            if let Some(ref mut builder) = current_builder {
                builder.add(&key, &kv_value)?;

                // Check if we should close this file and start a new one
                if builder.offset() >= options.target_file_size {
                    let file_id = current_file_id.take().unwrap();
                    let builder = current_builder.take().unwrap();
                    let FileBuildResult {
                        first_key,
                        last_key,
                        file_size,
                        meta_bytes,
                        sst_read_metadata,
                        max_expired_at,
                    } = builder.finish()?;
                    let bucket_range = DataFile::bucket_range_from_keys(&first_key, &last_key);
                    trace!(
                        "compaction output file level={} file_id={} size={}",
                        task.output_level, file_id, file_size
                    );

                    let data_file = DataFile::new(
                        task.data_file_type,
                        first_key,
                        last_key,
                        file_id,
                        TrackedFileId::new(&task.file_manager, file_id),
                        target_schema.version(),
                        file_size,
                        bucket_range.clone(),
                        bucket_range,
                    )
                    .with_separated_values(
                        merge_collector
                            .as_ref()
                            .is_some_and(|collector| collector.borrow().has_separated_values()),
                    );
                    data_file.set_meta_bytes(meta_bytes);
                    data_file.set_max_expired_at(max_expired_at);
                    task.file_manager.finalize_data_file(&data_file)?;
                    if let Some(metadata) = sst_read_metadata {
                        data_file.set_sst_read_metadata(metadata);
                    }
                    output_files.push(Arc::new(data_file));
                    written_bytes = written_bytes.saturating_add(file_size as u64);
                    if let Some(collector) = &merge_collector {
                        collector.borrow_mut().reset_has_separated_values();
                    }
                }
            }

            dedup_iter.next()?;
        }
        if let Some(collector) = merge_collector.as_ref() {
            collector.borrow_mut().check_error()?;
        }

        // Finish any remaining file
        if let Some(builder) = current_builder
            && !builder.is_empty()
        {
            let file_id = current_file_id.take().unwrap();
            let FileBuildResult {
                first_key,
                last_key,
                file_size,
                meta_bytes,
                sst_read_metadata,
                max_expired_at,
            } = builder.finish()?;
            let bucket_range = DataFile::bucket_range_from_keys(&first_key, &last_key);
            trace!(
                "compaction output file level={} file_id={} size={}",
                task.output_level, file_id, file_size
            );

            let data_file = DataFile::new(
                task.data_file_type,
                first_key,
                last_key,
                file_id,
                TrackedFileId::new(&task.file_manager, file_id),
                target_schema.version(),
                file_size,
                bucket_range.clone(),
                bucket_range,
            )
            .with_separated_values(
                merge_collector
                    .as_ref()
                    .is_some_and(|collector| collector.borrow().has_separated_values()),
            );
            data_file.set_meta_bytes(meta_bytes);
            data_file.set_max_expired_at(max_expired_at);
            task.file_manager.finalize_data_file(&data_file)?;
            if let Some(metadata) = sst_read_metadata {
                data_file.set_sst_read_metadata(metadata);
            }
            output_files.push(Arc::new(data_file));
            written_bytes = written_bytes.saturating_add(file_size as u64);
        }
        task.metrics.record_write_bytes(written_bytes);

        // Create version edits
        let mut level_edits: std::collections::BTreeMap<u8, LevelEdit> =
            std::collections::BTreeMap::new();
        for run in &task.sorted_runs {
            let entry = level_edits.entry(run.level()).or_insert_with(|| LevelEdit {
                level: run.level(),
                removed_files: Vec::new(),
                new_files: Vec::new(),
            });
            entry.removed_files.extend(run.files().iter().cloned());
        }
        let entry = level_edits
            .entry(task.output_level)
            .or_insert_with(|| LevelEdit {
                level: task.output_level,
                removed_files: Vec::new(),
                new_files: Vec::new(),
            });
        entry.new_files = output_files.clone();

        let edit = VersionEdit {
            level_edits: level_edits.into_values().collect(),
        };
        let mut vlog_edit = VlogEdit::default();
        for (file_seq, delta) in merge_collector
            .as_ref()
            .map(|collector| collector.borrow().removed_entry_deltas())
            .unwrap_or_default()
        {
            vlog_edit.add_entry_delta(file_seq, delta);
        }
        let vlog_edit = (!vlog_edit.is_empty()).then_some(vlog_edit);
        let output_bytes = output_files.iter().map(|file| file.size).sum::<usize>();
        trace!(
            "compaction complete output_level={} input_files={} input_bytes={} output_files={} output_bytes={}",
            task.output_level,
            task.sorted_runs
                .iter()
                .map(|run| run.files().len())
                .sum::<usize>(),
            task.sorted_runs
                .iter()
                .flat_map(|run| run.files().iter())
                .map(|file| file.size)
                .sum::<usize>(),
            output_files.len(),
            output_bytes
        );
        if task.output_files_readonly {
            // If files were created in read-only mode, we need to mark them as read-only
            // after writing is complete so they can be opened by other processes.
            for file in &output_files {
                task.file_manager.publish_data_file_transfer(file.file_id)?;
            }
        }
        task.metrics.record_completed();
        Ok(CompactionResult::new(
            task.lsm_tree_idx,
            output_files,
            edit,
            vlog_edit,
            preload_block_keys
                .map(|keys| keys.lock().unwrap().clone())
                .unwrap_or_default(),
        ))
    }
}

fn key_is_truncated_by_cursor_map(cursors: &TruncationCursorMap, encoded_key: &[u8]) -> bool {
    if cursors.is_empty() || encoded_key.len() < ENCODED_KEY_PREFIX_BYTES {
        return false;
    }
    let Some(bucket) = key_bucket(encoded_key) else {
        return false;
    };
    let Some(column_family_id) = key_column_family(encoded_key) else {
        return false;
    };
    cursors
        .get(&TruncationCursorId::new(bucket, column_family_id))
        .is_some_and(|cursor| &encoded_key[ENCODED_KEY_PREFIX_BYTES..] <= cursor.as_slice())
}

#[cfg(test)]
#[path = "../../tests/unit/compaction/executor.rs"]
mod tests;
