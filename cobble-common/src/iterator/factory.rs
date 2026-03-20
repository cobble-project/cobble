//! Factory for creating iterators from data files based on their type.

use crate::block_cache::BlockCache;
use crate::data_file::{DataFile, DataFileType};
use crate::error::Result;
use crate::file::FileManager;
use crate::iterator::{BucketFilterIterator, KvIterator};
use crate::parquet::ParquetIterator;
use crate::sst::{SSTIterator, SSTIteratorOptions};
use std::sync::Arc;

/// Options for creating iterators from data files.
#[derive(Default)]
pub struct IteratorFactoryOptions {
    /// Options for SST iterators.
    pub sst_options: SSTIteratorOptions,
    pub block_cache: Option<BlockCache>,
}

/// Creates an iterator for a data file based on its type.
///
/// This function provides a mapping from `DataFileType` to the appropriate
/// iterator implementation. It can be extended to support additional file
/// types in the future.
///
/// # Arguments
/// * `file` - The data file to create an iterator for
/// * `file_manager` - The file manager to use for reading the file
/// * `options` - Options for iterator creation
///
/// # Returns
/// A boxed iterator that implements the `KvIterator` trait
pub fn create_iterator(
    file: &DataFile,
    file_manager: &Arc<FileManager>,
    options: &IteratorFactoryOptions,
) -> Result<Box<dyn for<'a> KvIterator<'a>>> {
    match file.file_type {
        DataFileType::SSTable => {
            let reader = file_manager.open_data_file_reader(file.file_id)?;
            let iter = SSTIterator::with_cache_and_file(
                Box::new(reader),
                file,
                options.sst_options.clone(),
                options.block_cache.clone(),
            )?;
            if file.needs_bucket_filter() {
                Ok(Box::new(BucketFilterIterator::new(
                    iter,
                    file.effective_bucket_range.clone(),
                )))
            } else {
                Ok(Box::new(iter))
            }
        }
        DataFileType::Parquet => {
            let reader = file_manager.open_data_file_reader(file.file_id)?;
            let iter = ParquetIterator::from_data_file(
                Box::new(reader),
                file,
                options.block_cache.clone(),
            )?;
            if file.needs_bucket_filter() {
                Ok(Box::new(BucketFilterIterator::new(
                    iter,
                    file.effective_bucket_range.clone(),
                )))
            } else {
                Ok(Box::new(iter))
            }
        }
    }
}

/// A helper function to create an iterator factory closure for use with `SortedRun`.
///
/// This returns a closure that can be passed to `SortedRun::iter()` to create
/// iterators for each file in the run.
///
/// # Arguments
/// * `file_manager` - The file manager to use for reading files
/// * `options` - Options for iterator creation
///
/// # Returns
/// A closure that creates iterators for data files
pub fn make_iterator_factory(
    file_manager: Arc<FileManager>,
    options: IteratorFactoryOptions,
) -> impl Fn(&DataFile) -> Result<Box<dyn for<'a> KvIterator<'a>>> {
    move |file: &DataFile| create_iterator(file, &file_manager, &options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_file::{DataFile, DataFileType};
    use crate::file::{FileManager, FileSystemRegistry, TrackedFileId};
    use crate::metrics_manager::MetricsManager;
    use crate::parquet::ParquetWriter;

    fn cleanup_test_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_create_iterator_parquet_with_bucket_filter() {
        cleanup_test_root("/tmp/iterator_factory_parquet_filter_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/iterator_factory_parquet_filter_test")
            .unwrap();
        let metrics = Arc::new(MetricsManager::new("iterator-factory-parquet-test"));
        let file_manager = Arc::new(FileManager::with_defaults(fs, metrics).unwrap());

        let (file_id, writer_file) = file_manager.create_data_file().unwrap();
        let mut writer = ParquetWriter::new(writer_file).unwrap();
        writer.add(&[1, 0, b'a'], b"v1").unwrap();
        writer.add(&[2, 0, b'b'], b"v2").unwrap();
        let (start_key, end_key, file_size, meta) = writer.finish().unwrap();

        let data_file = DataFile::new(
            DataFileType::Parquet,
            start_key,
            end_key,
            file_id,
            TrackedFileId::new(&file_manager, file_id),
            0,
            file_size,
            1..=2,
            2..=2,
        );
        data_file.set_meta_bytes(meta);

        let options = IteratorFactoryOptions::default();
        let mut iter = create_iterator(&data_file, &file_manager, &options).unwrap();
        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        let key = iter.key().unwrap().unwrap();
        assert_eq!(key.as_ref(), &[2, 0, b'b']);
        let value = iter.value().unwrap().unwrap();
        assert_eq!(value.as_ref(), b"v2");
        assert!(!iter.next().unwrap());

        cleanup_test_root("/tmp/iterator_factory_parquet_filter_test");
    }
}
