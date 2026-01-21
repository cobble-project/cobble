//! FileBuilder trait for compaction output file builders.
//!
//! This module provides the `FileBuilder` trait that abstracts the interface
//! for building output files during compaction. Different file formats
//! (SST, etc.) can implement this trait.

use crate::error::Result;
use crate::file::SequentialWriteFile;

/// A trait for building output files during compaction.
///
/// This trait provides a common interface for different file formats
/// to be used in the compaction process. Implementations handle the
/// specifics of their file format while providing a unified API.
pub trait FileBuilder {
    /// Adds a key-value pair to the file.
    ///
    /// Keys must be added in sorted order.
    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Finishes building the file and returns (first_key, last_key, file_size).
    ///
    /// After calling this method, the builder is consumed and the file
    /// is ready to be read. The returned file_size is the total size of
    /// the file including all headers, data, indexes, and footers.
    fn finish(self: Box<Self>) -> Result<(Vec<u8>, Vec<u8>, usize)>;

    /// Returns the current offset (bytes written) in the file.
    fn offset(&self) -> usize;

    /// Returns true if no keys have been added yet.
    fn is_empty(&self) -> bool;
}

/// A factory function type for creating FileBuilder instances.
///
/// The factory takes a file writer and returns a boxed FileBuilder.
/// This allows the compaction process to be independent of the specific
/// file format being used.
pub type FileBuilderFactory =
    Box<dyn Fn(Box<dyn SequentialWriteFile>) -> Box<dyn FileBuilder> + Send + Sync>;
