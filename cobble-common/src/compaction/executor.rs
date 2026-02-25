//! Compaction executor for running compaction tasks.
//!
//! This module provides the core compaction functionality including:
//! - `CompactionTask`: Parameters for a single compaction operation
//! - `CompactionResult`: Output of a compaction operation
//! - `CompactionExecutor`: Manages compaction execution in a thread pool

use crate::compaction::CompactionConfig;
use crate::data_file::{DataFile, DataFileType};
use crate::error::Result;
use crate::file::{FileManager, ReadAheadBufferedReader, TrackedFileId};
use crate::format::{FileBuilder, FileBuilderFactory};
use crate::iterator::{DeduplicatingIterator, KvIterator, MergingIterator, SortedRun};
use crate::lsm::{LevelEdit, VersionEdit};
use crate::sst::{SSTIteratorMetrics, SSTIteratorOptions};
use crate::vlog::{VlogEdit, VlogMergeCollector};
use log::trace;
use metrics::{Counter, counter};
use std::sync::Arc;
use tokio::runtime::Runtime;

type CompactionCompleteCallback = Arc<dyn Fn(VersionEdit, Option<VlogEdit>) + Send + Sync>;

/// A compaction task describes the input and output parameters for a compaction.
pub struct CompactionTask {
    metrics: Arc<CompactionTaskMetrics>,
    sst_metrics: Arc<SSTIteratorMetrics>,
    /// The sorted runs to compact.
    sorted_runs: Vec<SortedRun>,
    output_level: u8,
    /// The file manager to use for reading/writing files.
    file_manager: Arc<FileManager>,
    /// Factory function for creating FileBuilder instances.
    file_builder_factory: Arc<FileBuilderFactory>,
    /// The data file type for output files.
    data_file_type: DataFileType,
    /// TTL provider for compaction to determine if entries are expired and can be dropped.
    ttl_provider: Arc<crate::ttl::TTLProvider>,
    /// Whether to create output files in read-only mode.
    /// This is used for remote compaction workers where we want to write files.
    output_files_readonly: bool,
}

#[derive(Clone)]
pub(crate) struct CompactionTaskMetrics {
    read_bytes_total: Counter,
    write_bytes_total: Counter,
}

impl CompactionTaskMetrics {
    pub(crate) fn new(db_id: &str) -> Self {
        let db_id = db_id.to_string();
        Self {
            read_bytes_total: counter!("compaction_read_bytes_total", "db_id" => db_id.clone()),
            write_bytes_total: counter!("compaction_write_bytes_total", "db_id" => db_id),
        }
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
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
        file_manager: Arc<FileManager>,
        file_builder_factory: Arc<FileBuilderFactory>,
        data_file_type: DataFileType,
        ttl_provider: Arc<crate::ttl::TTLProvider>,
    ) -> Self {
        Self {
            metrics,
            sst_metrics,
            sorted_runs,
            output_level,
            file_manager,
            file_builder_factory,
            data_file_type,
            ttl_provider,
            output_files_readonly: false,
        }
    }

    pub fn with_readonly_outputs(mut self) -> Self {
        self.output_files_readonly = true;
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
    /// New files created by the compaction.
    /// Files are sorted by their key ranges (first key of each file is sorted).
    new_files: Vec<Arc<DataFile>>,
    edit: VersionEdit,
    vlog_edit: Option<VlogEdit>,
}

impl CompactionResult {
    /// Creates a new compaction result.
    pub fn new(
        new_files: Vec<Arc<DataFile>>,
        edit: VersionEdit,
        vlog_edit: Option<VlogEdit>,
    ) -> Self {
        Self {
            new_files,
            edit,
            vlog_edit,
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
}

/// Executor for running compaction tasks.
///
/// The executor uses tokio's runtime for async task execution in a thread pool.
pub struct CompactionExecutor {
    runtime: Option<Arc<Runtime>>,
    options: CompactionConfig,
}

impl CompactionExecutor {
    /// Creates a new compaction executor with the given options and its own runtime.
    pub fn new(options: CompactionConfig) -> Result<Self> {
        Self::new_with_runtime(
            options,
            Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .thread_name("cobble-compaction")
                    .worker_threads(options.max_threads.max(1))
                    .enable_all()
                    .build()
                    .map_err(|e| crate::error::Error::IoError(e.to_string()))?,
            ),
        )
    }

    /// Creates a new compaction executor with the given options and thread count.
    pub fn new_with_runtime(options: CompactionConfig, runtime: Arc<Runtime>) -> Result<Self> {
        Ok(Self {
            runtime: Some(runtime),
            options,
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
    pub fn new_without_runtime(options: CompactionConfig) -> Self {
        Self {
            runtime: None,
            options,
        }
    }

    /// Creates a new compaction executor with default options.
    pub fn with_defaults() -> Result<Self> {
        Self::new(CompactionConfig::default())
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

        runtime.spawn_blocking(move || {
            let result = Self::run_compaction(task, options)?;
            if let Some(callback) = on_complete {
                callback(result.edit.clone(), result.vlog_edit.clone());
            }
            Ok(result)
        })
    }

    /// Executes a compaction task synchronously and blocks until completion.
    pub fn execute_blocking(
        &self,
        task: CompactionTask,
        on_complete: Option<CompactionCompleteCallback>,
    ) -> Result<CompactionResult> {
        let result = Self::run_compaction(task, self.options)?;
        if let Some(callback) = on_complete {
            callback(result.edit.clone(), result.vlog_edit.clone());
        }
        Ok(result)
    }

    pub fn shutdown(&mut self) {
        if let Some(runtime) = self.runtime.take()
            && let Ok(runtime) = Arc::try_unwrap(runtime)
        {
            runtime.shutdown_timeout(std::time::Duration::from_secs(5));
        }
    }

    fn run_compaction(task: CompactionTask, options: CompactionConfig) -> Result<CompactionResult> {
        let mut all_iters: Vec<Box<dyn for<'a> KvIterator<'a>>> = Vec::new();
        let mut read_bytes = 0u64;
        let use_read_ahead =
            options.read_ahead_enabled && tokio::runtime::Handle::try_current().is_ok();
        let sst_options = SSTIteratorOptions {
            metrics: Some(Arc::clone(&task.sst_metrics)),
            num_columns: options.num_columns,
            bloom_filter_enabled: options.bloom_filter_enabled,
            ..SSTIteratorOptions::default()
        };
        for run in &task.sorted_runs {
            for file in run.files() {
                read_bytes = read_bytes.saturating_add(file.size as u64);
            }
            let file_manager = Arc::clone(&task.file_manager);
            let sst_options = sst_options.clone();
            let run_iter = run.iter(move |file| {
                let reader = file_manager.open_data_file_reader(file.file_id)?;
                let reader: Box<dyn crate::file::RandomAccessFile> = if use_read_ahead {
                    Box::new(ReadAheadBufferedReader::new(
                        reader,
                        options.read_buffer_size,
                    ))
                } else {
                    Box::new(reader)
                };
                let iter = crate::sst::SSTIterator::with_cache_and_file(
                    reader,
                    file,
                    sst_options.clone(),
                    None,
                )?;
                Ok(Box::new(iter) as Box<dyn for<'a> KvIterator<'a>>)
            });
            all_iters.push(Box::new(run_iter));
        }
        task.metrics.read_bytes_total.increment(read_bytes);

        let max_seq = task
            .sorted_runs
            .iter()
            .flat_map(|run| run.files().iter())
            .map(|file| file.seq)
            .max()
            .unwrap_or(0);

        // Create merging iterator
        let merging_iter = MergingIterator::new(all_iters);
        let input_has_separated_values = task
            .sorted_runs
            .iter()
            .flat_map(|run| run.files().iter())
            .any(|file| file.has_separated_values());
        let merge_collector = input_has_separated_values.then(|| VlogMergeCollector::shared(true));
        let merge_callback = merge_collector.as_ref().map(VlogMergeCollector::callback);

        // Create deduplicating iterator
        let mut dedup_iter = DeduplicatingIterator::new(
            merging_iter,
            options.num_columns,
            task.ttl_provider(),
            merge_callback,
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
            let (key, value) = match dedup_iter.current_slice()? {
                Some(kv) => kv,
                None => break,
            };

            // Check if we need to start a new file
            if current_builder.is_none() {
                let (file_id, writer) = task.file_manager.create_data_file()?;
                current_file_id = Some(file_id);
                current_builder = Some((task.file_builder_factory)(Box::new(writer)));
            }

            // Add entry to current file
            if let Some(ref mut builder) = current_builder {
                builder.add(key, value)?;

                // Check if we should close this file and start a new one
                if builder.offset() >= options.target_file_size {
                    let file_id = current_file_id.take().unwrap();
                    let builder = current_builder.take().unwrap();
                    let (first_key, last_key, file_size, footer_bytes) = builder.finish()?;
                    trace!(
                        "compaction output file level={} file_id={} size={}",
                        task.output_level, file_id, file_size
                    );

                    let data_file = DataFile {
                        file_type: task.data_file_type,
                        start_key: first_key,
                        end_key: last_key,
                        file_id,
                        tracked_id: TrackedFileId::new(&task.file_manager, file_id),
                        seq: max_seq,
                        size: file_size,
                        has_separated_values: merge_collector
                            .as_ref()
                            .is_some_and(|collector| collector.borrow().has_separated_values()),
                        meta_bytes: Default::default(),
                    };
                    data_file.set_meta_bytes(footer_bytes);
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
            let (first_key, last_key, file_size, footer_bytes) = builder.finish()?;
            trace!(
                "compaction output file level={} file_id={} size={}",
                task.output_level, file_id, file_size
            );

            let data_file = DataFile {
                file_type: task.data_file_type,
                start_key: first_key,
                end_key: last_key,
                file_id,
                tracked_id: TrackedFileId::new(&task.file_manager, file_id),
                seq: max_seq,
                size: file_size,
                has_separated_values: merge_collector
                    .as_ref()
                    .is_some_and(|collector| collector.borrow().has_separated_values()),
                meta_bytes: Default::default(),
            };
            data_file.set_meta_bytes(footer_bytes);
            output_files.push(Arc::new(data_file));
            written_bytes = written_bytes.saturating_add(file_size as u64);
        }
        task.metrics.write_bytes_total.increment(written_bytes);

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
                task.file_manager.make_data_file_readonly(file.file_id)?;
            }
        }
        Ok(CompactionResult::new(output_files, edit, vlog_edit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::{FileSystemRegistry, TrackedFileId};
    use crate::metrics_manager::MetricsManager;
    use crate::sst::row_codec::encode_value;
    use crate::sst::{SSTWriter, SSTWriterOptions};
    use crate::r#type::Value;
    use crate::r#type::{Column, ValueType, decode_merge_separated_array};

    fn make_value_bytes(data: &[u8], num_columns: usize) -> Vec<u8> {
        let value = Value::new(vec![Some(Column::new(ValueType::Put, data.to_vec()))]);
        encode_value(&value, num_columns).to_vec()
    }

    fn make_typed_value_bytes(value_type: ValueType, data: &[u8], num_columns: usize) -> Vec<u8> {
        let value = Value::new(vec![Some(Column::new(value_type, data.to_vec()))]);
        encode_value(&value, num_columns).to_vec()
    }

    fn cleanup_test_dir(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn create_test_sst(
        file_manager: &Arc<FileManager>,
        entries: Vec<(&[u8], &[u8])>,
    ) -> Result<Arc<DataFile>> {
        let (file_id, writer_file) = file_manager.create_data_file()?;
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            },
        );

        for (key, value) in entries {
            writer.add(key, value)?;
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
            has_separated_values: true,
            meta_bytes: Default::default(),
        };
        data_file.set_meta_bytes(footer_bytes);
        Ok(Arc::new(data_file))
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_compaction_basic() {
        let test_dir = "/tmp/compaction_basic_test";
        cleanup_test_dir(test_dir);

        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", test_dir))
            .unwrap();

        let metrics_manager = Arc::new(MetricsManager::new("compaction-test".to_string()));
        let file_manager = Arc::new(
            FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
        );

        let num_columns = 1;

        // Create first SST file with entries a, c, e
        let file1 = create_test_sst(
            &file_manager,
            vec![
                (b"a", &make_value_bytes(b"v1", num_columns)),
                (b"c", &make_value_bytes(b"v3", num_columns)),
                (b"e", &make_value_bytes(b"v5", num_columns)),
            ],
        )
        .unwrap();

        // Create second SST file with entries b, d, f
        let file2 = create_test_sst(
            &file_manager,
            vec![
                (b"b", &make_value_bytes(b"v2", num_columns)),
                (b"d", &make_value_bytes(b"v4", num_columns)),
                (b"f", &make_value_bytes(b"v6", num_columns)),
            ],
        )
        .unwrap();

        // Create sorted runs
        let run1 = SortedRun::new(0, vec![file1]);
        let run2 = SortedRun::new(1, vec![file2]);

        let options = CompactionConfig {
            num_columns,
            target_file_size: 1024 * 1024, // 1MB - all entries fit in one file
            bloom_filter_enabled: true,
            bloom_bits_per_key: 10,
            ..Default::default()
        };

        // Create and execute compaction
        let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions {
            metrics: None,
            block_size: options.block_size,
            buffer_size: options.buffer_size,
            num_columns: options.num_columns,
            bloom_filter_enabled: options.bloom_filter_enabled,
            bloom_bits_per_key: options.bloom_bits_per_key,
            partitioned_index: options.partitioned_index,
            compression: crate::SstCompressionAlgorithm::None,
        });
        let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
        let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
        let task = CompactionTask::new(
            compaction_metrics,
            sst_metrics,
            vec![run1, run2],
            1,
            Arc::clone(&file_manager),
            factory,
            DataFileType::SSTable,
            Arc::new(crate::ttl::TTLProvider::disabled()),
        );

        let executor = CompactionExecutor::new(options).unwrap();

        let result = executor.execute_blocking(task, None).unwrap();
        assert_eq!(result.edit().level_edits.len(), 2);
        assert!(
            result
                .edit()
                .level_edits
                .iter()
                .any(|edit| edit.new_files.len() == 1)
        );

        // Verify output
        assert!(!result.new_files().is_empty());

        // Verify first file has correct key range
        let first_file = &result.new_files()[0];
        assert_eq!(first_file.start_key, b"a");
        assert_eq!(first_file.end_key, b"f");

        // Verify file exists and is readable
        let reader = file_manager
            .open_data_file_reader(first_file.file_id)
            .unwrap();
        let mut iter = crate::sst::SSTIterator::with_cache_and_file(
            Box::new(reader),
            first_file,
            crate::sst::SSTIteratorOptions {
                bloom_filter_enabled: true,
                ..crate::sst::SSTIteratorOptions::default()
            },
            None,
        )
        .unwrap();
        iter.seek_to_first().unwrap();

        // Verify entries are merged and sorted
        let mut keys = vec![];
        while iter.valid() {
            let (key, _) = iter.current().unwrap().unwrap();
            keys.push(key.to_vec());
            iter.next().unwrap();
        }

        assert_eq!(
            keys,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
                b"f".to_vec()
            ]
        );

        cleanup_test_dir(test_dir);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_compaction_with_duplicates() {
        let test_dir = "/tmp/compaction_duplicates_test";
        cleanup_test_dir(test_dir);

        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", test_dir))
            .unwrap();

        let metrics_manager = Arc::new(MetricsManager::new("compaction-test".to_string()));
        let file_manager = Arc::new(
            FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
        );

        let num_columns = 1;

        // Create first SST file (newer) with entries a, b
        let file1 = create_test_sst(
            &file_manager,
            vec![
                (b"a", &make_value_bytes(b"new_a", num_columns)),
                (b"b", &make_value_bytes(b"new_b", num_columns)),
            ],
        )
        .unwrap();

        // Create second SST file (older) with entries a, b, c
        let file2 = create_test_sst(
            &file_manager,
            vec![
                (b"a", &make_value_bytes(b"old_a", num_columns)),
                (b"b", &make_value_bytes(b"old_b", num_columns)),
                (b"c", &make_value_bytes(b"old_c", num_columns)),
            ],
        )
        .unwrap();

        // Create sorted runs (first run is newer)
        let run1 = SortedRun::new(0, vec![file1]);
        let run2 = SortedRun::new(0, vec![file2]);

        let options = CompactionConfig {
            num_columns,
            bloom_filter_enabled: true,
            bloom_bits_per_key: 10,
            ..Default::default()
        };

        // Create and execute compaction
        let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions {
            metrics: None,
            block_size: options.block_size,
            buffer_size: options.buffer_size,
            num_columns: options.num_columns,
            bloom_filter_enabled: options.bloom_filter_enabled,
            bloom_bits_per_key: options.bloom_bits_per_key,
            partitioned_index: options.partitioned_index,
            compression: crate::SstCompressionAlgorithm::None,
        });
        let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
        let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
        let task = CompactionTask::new(
            compaction_metrics,
            sst_metrics,
            vec![run1, run2],
            1,
            Arc::clone(&file_manager),
            factory,
            DataFileType::SSTable,
            Arc::new(crate::ttl::TTLProvider::disabled()),
        );

        let executor = CompactionExecutor::new(options).unwrap();

        let result = executor.execute_blocking(task, None).unwrap();
        assert_eq!(result.edit().level_edits.len(), 2);
        assert!(
            result
                .edit()
                .level_edits
                .iter()
                .any(|edit| edit.new_files.len() == 1)
        );

        // Verify output
        assert_eq!(result.new_files().len(), 1);

        // Read and verify merged entries
        let reader = file_manager
            .open_data_file_reader(result.new_files()[0].file_id)
            .unwrap();
        let mut iter = crate::sst::SSTIterator::with_cache_and_file(
            Box::new(reader),
            result.new_files()[0].as_ref(),
            crate::sst::SSTIteratorOptions {
                bloom_filter_enabled: true,
                num_columns,
                ..Default::default()
            },
            None,
        )
        .unwrap();

        iter.seek_to_first().unwrap();

        // Key "a" - newer value should win
        assert!(iter.valid());
        let (key, mut value) = iter.current().unwrap().unwrap();
        assert_eq!(&key[..], b"a");
        let decoded = crate::sst::row_codec::decode_value(&mut value, num_columns).unwrap();
        assert_eq!(
            decoded.columns()[0].as_ref().unwrap().data().as_ref(),
            b"new_a"
        );

        // Key "b" - newer value should win
        iter.next().unwrap();
        assert!(iter.valid());
        let (key, mut value) = iter.current().unwrap().unwrap();
        assert_eq!(&key[..], b"b");
        let decoded = crate::sst::row_codec::decode_value(&mut value, num_columns).unwrap();
        assert_eq!(
            decoded.columns()[0].as_ref().unwrap().data().as_ref(),
            b"new_b"
        );

        // Key "c" - only in older file
        iter.next().unwrap();
        assert!(iter.valid());
        let (key, mut value) = iter.current().unwrap().unwrap();
        assert_eq!(&key[..], b"c");
        let decoded = crate::sst::row_codec::decode_value(&mut value, num_columns).unwrap();
        assert_eq!(
            decoded.columns()[0].as_ref().unwrap().data().as_ref(),
            b"old_c"
        );

        cleanup_test_dir(test_dir);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_compaction_lazy_merge_with_separated_values() {
        let test_dir = "/tmp/compaction_separated_merge_test";
        cleanup_test_dir(test_dir);

        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", test_dir))
            .unwrap();

        let metrics_manager = Arc::new(MetricsManager::new("compaction-test".to_string()));
        let file_manager = Arc::new(
            FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
        );

        let num_columns = 1;
        let old_put_separated = [0x11u8; 8];
        let new_merge_separated_a = [0x22u8; 8];
        let new_merge_separated_b = [0x33u8; 8];

        // Newer run contains merge-separated values.
        let file1 = create_test_sst(
            &file_manager,
            vec![
                (
                    b"a",
                    &make_typed_value_bytes(
                        ValueType::MergeSeparated,
                        &new_merge_separated_a,
                        num_columns,
                    ),
                ),
                (
                    b"b",
                    &make_typed_value_bytes(
                        ValueType::MergeSeparated,
                        &new_merge_separated_b,
                        num_columns,
                    ),
                ),
            ],
        )
        .unwrap();

        // Older run contains one separated base and one inline base.
        let file2 = create_test_sst(
            &file_manager,
            vec![
                (
                    b"a",
                    &make_typed_value_bytes(
                        ValueType::PutSeparated,
                        &old_put_separated,
                        num_columns,
                    ),
                ),
                (
                    b"b",
                    &make_typed_value_bytes(ValueType::Put, b"base_b", num_columns),
                ),
            ],
        )
        .unwrap();

        let run1 = SortedRun::new(0, vec![file1]);
        let run2 = SortedRun::new(0, vec![file2]);

        let options = CompactionConfig {
            num_columns,
            bloom_filter_enabled: true,
            bloom_bits_per_key: 10,
            ..Default::default()
        };
        let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions {
            metrics: None,
            block_size: options.block_size,
            buffer_size: options.buffer_size,
            num_columns: options.num_columns,
            bloom_filter_enabled: options.bloom_filter_enabled,
            bloom_bits_per_key: options.bloom_bits_per_key,
            partitioned_index: options.partitioned_index,
            compression: crate::SstCompressionAlgorithm::None,
        });
        let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
        let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
        let task = CompactionTask::new(
            compaction_metrics,
            sst_metrics,
            vec![run1, run2],
            1,
            Arc::clone(&file_manager),
            factory,
            DataFileType::SSTable,
            Arc::new(crate::ttl::TTLProvider::disabled()),
        );
        let executor = CompactionExecutor::new(options).unwrap();
        let result = executor.execute_blocking(task, None).unwrap();

        assert_eq!(result.new_files().len(), 1);
        let reader = file_manager
            .open_data_file_reader(result.new_files()[0].file_id)
            .unwrap();
        let mut iter = crate::sst::SSTIterator::with_cache_and_file(
            Box::new(reader),
            result.new_files()[0].as_ref(),
            crate::sst::SSTIteratorOptions {
                bloom_filter_enabled: true,
                num_columns,
                ..Default::default()
            },
            None,
        )
        .unwrap();

        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        let (key, mut value) = iter.current().unwrap().unwrap();
        assert_eq!(&key[..], b"a");
        let decoded = crate::sst::row_codec::decode_value(&mut value, num_columns).unwrap();
        let column = decoded.columns()[0].as_ref().unwrap();
        assert_eq!(column.value_type, ValueType::MergeSeparatedArray);
        let merged_items = decode_merge_separated_array(column.data()).unwrap();
        assert_eq!(merged_items.len(), 2);
        assert_eq!(merged_items[0].value_type, ValueType::PutSeparated);
        assert_eq!(merged_items[0].data().as_ref(), old_put_separated);
        assert_eq!(merged_items[1].value_type, ValueType::MergeSeparated);
        assert_eq!(merged_items[1].data().as_ref(), new_merge_separated_a);

        iter.next().unwrap();
        assert!(iter.valid());
        let (key, mut value) = iter.current().unwrap().unwrap();
        assert_eq!(&key[..], b"b");
        let decoded = crate::sst::row_codec::decode_value(&mut value, num_columns).unwrap();
        let column = decoded.columns()[0].as_ref().unwrap();
        assert_eq!(column.value_type, ValueType::MergeSeparatedArray);
        let merged_items = decode_merge_separated_array(column.data()).unwrap();
        assert_eq!(merged_items.len(), 2);
        assert_eq!(merged_items[0].value_type, ValueType::Put);
        assert_eq!(merged_items[0].data(), b"base_b");
        assert_eq!(merged_items[1].value_type, ValueType::MergeSeparated);
        assert_eq!(merged_items[1].data().as_ref(), new_merge_separated_b);

        cleanup_test_dir(test_dir);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_compaction_multiple_output_files() {
        let test_dir = "/tmp/compaction_multi_output_test";
        cleanup_test_dir(test_dir);

        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", test_dir))
            .unwrap();

        let metrics_manager = Arc::new(MetricsManager::new("compaction-test".to_string()));
        let file_manager = Arc::new(
            FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
        );

        let num_columns = 1;

        // Create a large SST file
        let mut entries = vec![];
        for i in 0..100 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}_with_some_extra_padding_data", i);
            entries.push((
                key.into_bytes(),
                make_value_bytes(value.as_bytes(), num_columns),
            ));
        }

        let (file_id, writer_file) = file_manager.create_data_file().unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            },
        );

        for (key, value) in &entries {
            writer.add(key, value).unwrap();
        }
        let (first_key, last_key, file_size, footer_bytes) = writer.finish_with_range().unwrap();

        let file = DataFile {
            file_type: DataFileType::SSTable,
            start_key: first_key,
            end_key: last_key,
            file_id,
            tracked_id: TrackedFileId::new(&file_manager, file_id),
            seq: 0,
            size: file_size,
            has_separated_values: false,
            meta_bytes: Default::default(),
        };
        file.set_meta_bytes(footer_bytes);
        let file = Arc::new(file);

        let run = SortedRun::new(0, vec![file]);

        let options = CompactionConfig {
            num_columns,
            target_file_size: 500, // Very small to force multiple files
            bloom_filter_enabled: true,
            bloom_bits_per_key: 10,
            ..Default::default()
        };

        // Create compaction with very small target file size to force multiple output files
        let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions {
            metrics: None,
            block_size: options.block_size,
            buffer_size: options.buffer_size,
            num_columns: options.num_columns,
            bloom_filter_enabled: options.bloom_filter_enabled,
            bloom_bits_per_key: options.bloom_bits_per_key,
            partitioned_index: options.partitioned_index,
            compression: crate::SstCompressionAlgorithm::None,
        });
        let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
        let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
        let task = CompactionTask::new(
            compaction_metrics,
            sst_metrics,
            vec![run],
            1,
            Arc::clone(&file_manager),
            factory,
            DataFileType::SSTable,
            Arc::new(crate::ttl::TTLProvider::disabled()),
        );

        let executor = CompactionExecutor::new(options).unwrap();

        let result = executor.execute_blocking(task, None).unwrap();
        assert_eq!(result.edit().level_edits.len(), 2);
        assert!(
            result
                .edit()
                .level_edits
                .iter()
                .any(|edit| edit.new_files.len() > 1)
        );

        // Should have multiple output files
        assert!(result.new_files().len() > 1);

        // Verify files are sorted by key range
        for i in 1..result.new_files().len() {
            let prev_file = &result.new_files()[i - 1];
            let curr_file = &result.new_files()[i];
            assert!(
                prev_file.end_key < curr_file.start_key,
                "Files should have non-overlapping, sorted key ranges"
            );
        }

        cleanup_test_dir(test_dir);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_compaction_empty_input() {
        let test_dir = "/tmp/compaction_empty_test";
        cleanup_test_dir(test_dir);

        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", test_dir))
            .unwrap();

        let metrics_manager = Arc::new(MetricsManager::new("compaction-test".to_string()));
        let file_manager = Arc::new(
            FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
        );

        let options = CompactionConfig::default();

        // Create compaction with no sorted runs
        let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions {
            metrics: None,
            block_size: options.block_size,
            buffer_size: options.buffer_size,
            num_columns: options.num_columns,
            bloom_filter_enabled: options.bloom_filter_enabled,
            bloom_bits_per_key: options.bloom_bits_per_key,
            partitioned_index: options.partitioned_index,
            compression: crate::SstCompressionAlgorithm::None,
        });
        let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
        let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
        let task = CompactionTask::new(
            compaction_metrics,
            sst_metrics,
            vec![],
            1,
            Arc::clone(&file_manager),
            factory,
            DataFileType::SSTable,
            Arc::new(crate::ttl::TTLProvider::disabled()),
        );

        let executor = CompactionExecutor::with_defaults().unwrap();
        let result = executor.execute_blocking(task, None).unwrap();
        assert_eq!(result.edit().level_edits.len(), 1);
        assert!(result.edit().level_edits[0].new_files.is_empty());

        // Should have no output files
        assert!(result.new_files().is_empty());

        cleanup_test_dir(test_dir);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_compaction_tracks_vlog_entry_deletions_for_shadowed_values() {
        let test_dir = "/tmp/compaction_vlog_entry_delta_test";
        cleanup_test_dir(test_dir);

        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", test_dir))
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("compaction-test".to_string()));
        let file_manager = Arc::new(
            FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager)).unwrap(),
        );
        let num_columns = 1;
        let pointer = crate::vlog::VlogPointer::new(9, 0);
        let older = create_test_sst(
            &file_manager,
            vec![(
                b"k",
                &make_typed_value_bytes(ValueType::PutSeparated, &pointer.to_bytes(), num_columns),
            )],
        )
        .unwrap();
        let newer = create_test_sst(
            &file_manager,
            vec![(b"k", &make_value_bytes(b"inline", num_columns))],
        )
        .unwrap();

        let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions::default());
        let compaction_metrics = Arc::new(CompactionTaskMetrics::new("test"));
        let sst_metrics = Arc::new(crate::sst::SSTIteratorMetrics::new("test"));
        let task = CompactionTask::new(
            compaction_metrics,
            sst_metrics,
            vec![
                SortedRun::new(0, vec![newer]),
                SortedRun::new(1, vec![older]),
            ],
            1,
            Arc::clone(&file_manager),
            factory,
            DataFileType::SSTable,
            Arc::new(crate::ttl::TTLProvider::disabled()),
        );
        let executor = CompactionExecutor::with_defaults().unwrap();
        let result = executor.execute_blocking(task, None).unwrap();
        let deltas: std::collections::HashMap<u32, i64> = result
            .vlog_edit()
            .unwrap()
            .entry_deltas()
            .into_iter()
            .collect();
        assert_eq!(deltas.get(&9).copied(), Some(-1));
        cleanup_test_dir(test_dir);
    }
}
