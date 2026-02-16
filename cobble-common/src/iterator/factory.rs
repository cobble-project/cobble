//! Factory for creating iterators from data files based on their type.

use crate::data_file::{DataFile, DataFileType};
use crate::error::Result;
use crate::file::FileManager;
use crate::iterator::KvIterator;
use crate::sst::block_cache::BlockCache;
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
            Ok(Box::new(iter))
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
