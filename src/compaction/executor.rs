//! Compaction executor for running compaction tasks.
//!
//! This module provides the core compaction functionality including:
//! - `CompactionTask`: Parameters for a single compaction operation
//! - `CompactionResult`: Output of a compaction operation
//! - `CompactionExecutor`: Manages compaction execution in a thread pool

use crate::compaction::{FileBuilder, FileBuilderFactory};
use crate::data_file::{DataFile, DataFileType};
use crate::error::Result;
use crate::file::FileManager;
use crate::iterator::{DeduplicatingIterator, KvIterator, MergingIterator, SortedRun};
use crate::sst::SSTIteratorOptions;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Options for the compaction executor.
#[derive(Clone)]
pub struct CompactionOptions {
    /// Block size for SST files.
    pub block_size: usize,
    /// Buffer size for file writes.
    pub buffer_size: usize,
    /// Number of columns in the value schema.
    pub num_columns: usize,
    /// Target file size for output SST files.
    /// Compaction will start a new file when this size is exceeded.
    pub target_file_size: usize,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self {
            block_size: 4096,
            buffer_size: 8192,
            num_columns: 1,
            target_file_size: 64 * 1024 * 1024, // 64 MB
        }
    }
}

/// A compaction task describes the input and output parameters for a compaction.
pub struct CompactionTask {
    /// The sorted runs to compact.
    sorted_runs: Vec<SortedRun>,
    /// The file manager to use for reading/writing files.
    file_manager: Arc<FileManager>,
    /// Factory function for creating FileBuilder instances.
    file_builder_factory: Arc<FileBuilderFactory>,
    /// The data file type for output files.
    data_file_type: DataFileType,
}

impl CompactionTask {
    /// Creates a new compaction task.
    ///
    /// # Arguments
    /// * `sorted_runs` - The sorted runs to merge together
    /// * `file_manager` - The file manager for reading input files and writing output files
    /// * `file_builder_factory` - Factory function for creating FileBuilder instances
    /// * `data_file_type` - The data file type for output files
    pub fn new(
        sorted_runs: Vec<SortedRun>,
        file_manager: Arc<FileManager>,
        file_builder_factory: Arc<FileBuilderFactory>,
        data_file_type: DataFileType,
    ) -> Self {
        Self {
            sorted_runs,
            file_manager,
            file_builder_factory,
            data_file_type,
        }
    }

    /// Returns the sorted runs in this task.
    pub fn sorted_runs(&self) -> &[SortedRun] {
        &self.sorted_runs
    }
}

/// The result of a compaction operation.
pub struct CompactionResult {
    /// New files created by the compaction.
    /// Files are sorted by their key ranges (first key of each file is sorted).
    new_files: Vec<Arc<DataFile>>,
}

impl CompactionResult {
    /// Creates a new compaction result.
    pub fn new(new_files: Vec<Arc<DataFile>>) -> Self {
        Self { new_files }
    }

    /// Returns the new files created by compaction.
    /// Files are sorted by their key ranges.
    pub fn new_files(&self) -> &[Arc<DataFile>] {
        &self.new_files
    }
}

/// Executor for running compaction tasks.
///
/// The executor uses tokio's runtime for async task execution in a thread pool.
pub struct CompactionExecutor {
    runtime: Option<Runtime>,
    options: CompactionOptions,
}

impl CompactionExecutor {
    /// Creates a new compaction executor with the given options and its own runtime.
    pub fn new(options: CompactionOptions) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .map_err(|e| crate::error::Error::IoError(e.to_string()))?;

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
    pub fn new_without_runtime(options: CompactionOptions) -> Self {
        Self {
            runtime: None,
            options,
        }
    }

    /// Creates a new compaction executor with default options.
    pub fn with_defaults() -> Result<Self> {
        Self::new(CompactionOptions::default())
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
    ) -> tokio::task::JoinHandle<Result<CompactionResult>> {
        let runtime = self.runtime.as_ref().expect("Executor has no runtime.");
        let options = self.options.clone();

        runtime.spawn(async move { Self::run_compaction(task, options) })
    }

    /// Executes a compaction task synchronously and blocks until completion.
    pub fn execute_blocking(&self, task: CompactionTask) -> Result<CompactionResult> {
        Self::run_compaction(task, self.options.clone())
    }

    fn run_compaction(
        task: CompactionTask,
        options: CompactionOptions,
    ) -> Result<CompactionResult> {
        // Create iterators for all files in all sorted runs
        // We iterate files from all sorted runs, with earlier runs (newer data) coming first
        let sst_options = SSTIteratorOptions {
            num_columns: options.num_columns,
            ..SSTIteratorOptions::default()
        };

        let mut all_iters: Vec<Box<dyn KvIterator>> = Vec::new();
        for run in &task.sorted_runs {
            for file in run.files() {
                let reader = task.file_manager.open_data_file_reader(file.file_id)?;
                let iter = crate::sst::SSTIterator::new(reader, sst_options.clone())?;
                all_iters.push(Box::new(iter));
            }
        }

        // Create merging iterator
        let merging_iter = MergingIterator::new(all_iters);

        // Create deduplicating iterator
        let mut dedup_iter = DeduplicatingIterator::new(merging_iter, options.num_columns);
        dedup_iter.seek_to_first()?;

        // Collect output files
        let mut output_files: Vec<Arc<DataFile>> = Vec::new();

        // Process entries and write to output files using the FileBuilder trait
        let mut current_builder: Option<Box<dyn FileBuilder>> = None;
        let mut current_file_id: Option<u64> = None;

        while dedup_iter.valid() {
            let (key, value) = match dedup_iter.current()? {
                Some(kv) => kv,
                None => break,
            };

            // Check if we need to start a new file
            if current_builder.is_none() {
                let (file_id, writer) = task.file_manager.create_data_file()?;
                current_file_id = Some(file_id);
                current_builder = Some((task.file_builder_factory)(writer));
            }

            // Add entry to current file
            if let Some(ref mut builder) = current_builder {
                builder.add(&key, &value)?;

                // Check if we should close this file and start a new one
                if builder.offset() >= options.target_file_size {
                    let file_id = current_file_id.take().unwrap();
                    let builder = current_builder.take().unwrap();
                    let (first_key, last_key, file_size) = builder.finish()?;

                    output_files.push(Arc::new(DataFile {
                        file_type: task.data_file_type,
                        start_key: first_key,
                        end_key: last_key,
                        file_id,
                        size: file_size,
                    }));
                }
            }

            dedup_iter.next()?;
        }

        // Finish any remaining file
        if let Some(builder) = current_builder
            && !builder.is_empty()
        {
            let file_id = current_file_id.take().unwrap();
            let (first_key, last_key, file_size) = builder.finish()?;

            output_files.push(Arc::new(DataFile {
                file_type: task.data_file_type,
                start_key: first_key,
                end_key: last_key,
                file_id,
                size: file_size,
            }));
        }

        Ok(CompactionResult::new(output_files))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::{FileManagerOptions, FileSystemRegistry};
    use crate::sst::row_codec::encode_value;
    use crate::sst::{SSTWriter, SSTWriterOptions};
    use crate::r#type::Value;
    use crate::r#type::{Column, ValueType};

    fn make_value_bytes(data: &[u8], num_columns: usize) -> Vec<u8> {
        let value = Value::new(vec![Some(Column::new(ValueType::Put, data.to_vec()))]);
        encode_value(&value, num_columns).to_vec()
    }

    fn cleanup_test_dir(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    /// Creates an SST file builder factory for tests.
    fn make_sst_builder_factory(options: CompactionOptions) -> Arc<FileBuilderFactory> {
        Arc::new(Box::new(move |writer| {
            Box::new(SSTWriter::new(
                writer,
                SSTWriterOptions {
                    block_size: options.block_size,
                    buffer_size: options.buffer_size,
                    num_columns: options.num_columns,
                },
            )) as Box<dyn FileBuilder>
        }))
    }

    fn create_test_sst(
        file_manager: &FileManager,
        entries: Vec<(&[u8], &[u8])>,
    ) -> Result<Arc<DataFile>> {
        let (file_id, writer_file) = file_manager.create_data_file()?;
        let mut writer = SSTWriter::new(writer_file, SSTWriterOptions::default());

        for (key, value) in entries {
            writer.add(key, value)?;
        }

        let (first_key, last_key, file_size) = writer.finish_with_range()?;

        Ok(Arc::new(DataFile {
            file_type: DataFileType::SSTable,
            start_key: first_key,
            end_key: last_key,
            file_id,
            size: file_size,
        }))
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

        let file_manager =
            Arc::new(FileManager::new(Arc::clone(&fs), FileManagerOptions::default()).unwrap());

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
        let run1 = SortedRun::new(vec![file1]);
        let run2 = SortedRun::new(vec![file2]);

        let options = CompactionOptions {
            num_columns,
            target_file_size: 1024 * 1024, // 1MB - all entries fit in one file
            ..Default::default()
        };

        // Create and execute compaction
        let task = CompactionTask::new(
            vec![run1, run2],
            Arc::clone(&file_manager),
            make_sst_builder_factory(options.clone()),
            DataFileType::SSTable,
        );

        let executor = CompactionExecutor::new(options).unwrap();

        let result = executor.execute_blocking(task).unwrap();

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
        let mut iter =
            crate::sst::SSTIterator::new(reader, crate::sst::SSTIteratorOptions::default())
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

        let file_manager =
            Arc::new(FileManager::new(Arc::clone(&fs), FileManagerOptions::default()).unwrap());

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
        let run1 = SortedRun::new(vec![file1]);
        let run2 = SortedRun::new(vec![file2]);

        let options = CompactionOptions {
            num_columns,
            ..Default::default()
        };

        // Create and execute compaction
        let task = CompactionTask::new(
            vec![run1, run2],
            Arc::clone(&file_manager),
            make_sst_builder_factory(options.clone()),
            DataFileType::SSTable,
        );

        let executor = CompactionExecutor::new(options).unwrap();

        let result = executor.execute_blocking(task).unwrap();

        // Verify output
        assert_eq!(result.new_files().len(), 1);

        // Read and verify merged entries
        let reader = file_manager
            .open_data_file_reader(result.new_files()[0].file_id)
            .unwrap();
        let mut iter = crate::sst::SSTIterator::new(
            reader,
            crate::sst::SSTIteratorOptions {
                num_columns,
                ..Default::default()
            },
        )
        .unwrap();

        iter.seek_to_first().unwrap();

        // Key "a" - newer value should win
        assert!(iter.valid());
        let (key, value) = iter.current().unwrap().unwrap();
        assert_eq!(&key[..], b"a");
        let decoded = crate::sst::row_codec::decode_value(&value, num_columns).unwrap();
        assert_eq!(decoded.columns()[0].as_ref().unwrap().data(), b"new_a");

        // Key "b" - newer value should win
        iter.next().unwrap();
        assert!(iter.valid());
        let (key, value) = iter.current().unwrap().unwrap();
        assert_eq!(&key[..], b"b");
        let decoded = crate::sst::row_codec::decode_value(&value, num_columns).unwrap();
        assert_eq!(decoded.columns()[0].as_ref().unwrap().data(), b"new_b");

        // Key "c" - only in older file
        iter.next().unwrap();
        assert!(iter.valid());
        let (key, value) = iter.current().unwrap().unwrap();
        assert_eq!(&key[..], b"c");
        let decoded = crate::sst::row_codec::decode_value(&value, num_columns).unwrap();
        assert_eq!(decoded.columns()[0].as_ref().unwrap().data(), b"old_c");

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

        let file_manager =
            Arc::new(FileManager::new(Arc::clone(&fs), FileManagerOptions::default()).unwrap());

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
        let mut writer = SSTWriter::new(writer_file, SSTWriterOptions::default());

        for (key, value) in &entries {
            writer.add(key, value).unwrap();
        }
        let (first_key, last_key, file_size) = writer.finish_with_range().unwrap();

        let file = Arc::new(DataFile {
            file_type: DataFileType::SSTable,
            start_key: first_key,
            end_key: last_key,
            file_id,
            size: file_size,
        });

        let run = SortedRun::new(vec![file]);

        let options = CompactionOptions {
            num_columns,
            target_file_size: 500, // Very small to force multiple files
            ..Default::default()
        };

        // Create compaction with very small target file size to force multiple output files
        let task = CompactionTask::new(
            vec![run],
            Arc::clone(&file_manager),
            make_sst_builder_factory(options.clone()),
            DataFileType::SSTable,
        );

        let executor = CompactionExecutor::new(options).unwrap();

        let result = executor.execute_blocking(task).unwrap();

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

        let file_manager =
            Arc::new(FileManager::new(Arc::clone(&fs), FileManagerOptions::default()).unwrap());

        let options = CompactionOptions::default();

        // Create compaction with no sorted runs
        let task = CompactionTask::new(
            vec![],
            Arc::clone(&file_manager),
            make_sst_builder_factory(options),
            DataFileType::SSTable,
        );

        let executor = CompactionExecutor::with_defaults().unwrap();
        let result = executor.execute_blocking(task).unwrap();

        // Should have no output files
        assert!(result.new_files().is_empty());

        cleanup_test_dir(test_dir);
    }
}
