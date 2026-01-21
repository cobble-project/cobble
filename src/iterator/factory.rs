//! Factory for creating iterators from data files based on their type.

use crate::data_file::{DataFile, DataFileType};
use crate::error::Result;
use crate::file::FileSystem;
use crate::iterator::KvIterator;
use crate::sst::{SSTIterator, SSTIteratorOptions};
use std::sync::Arc;

/// Options for creating iterators from data files.
#[derive(Default)]
pub struct IteratorFactoryOptions {
    /// Options for SST iterators.
    pub sst_options: SSTIteratorOptions,
}

/// Creates an iterator for a data file based on its type.
///
/// This function provides a mapping from `DataFileType` to the appropriate
/// iterator implementation. It can be extended to support additional file
/// types in the future.
///
/// # Arguments
/// * `file` - The data file to create an iterator for
/// * `fs` - The file system to use for reading the file
/// * `options` - Options for iterator creation
///
/// # Returns
/// A boxed iterator that implements the `KvIterator` trait
pub fn create_iterator(
    file: &DataFile,
    fs: &Arc<dyn FileSystem>,
    options: &IteratorFactoryOptions,
) -> Result<Box<dyn KvIterator>> {
    match file.file_type {
        DataFileType::SSTable => {
            let reader = fs.open_read(&file.path)?;
            let iter = SSTIterator::new(reader, options.sst_options.clone())?;
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
/// * `fs` - The file system to use for reading files
/// * `options` - Options for iterator creation
///
/// # Returns
/// A closure that creates iterators for data files
pub fn make_iterator_factory(
    fs: Arc<dyn FileSystem>,
    options: IteratorFactoryOptions,
) -> impl Fn(&DataFile) -> Result<Box<dyn KvIterator>> {
    move |file: &DataFile| create_iterator(file, &fs, &options)
}
