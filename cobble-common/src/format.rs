//! File format utilities and shared writer abstractions.

use crate::error::Result;
use crate::file::SequentialWriteFile;
use crate::r#type::KvValue;
use bytes::Bytes;

/// A trait for building output files (SST, etc.).
///
/// This trait provides a common interface for different file formats
/// to be used in flush/compaction processes.
pub trait FileBuilder {
    /// Adds a key-value pair to the file.
    ///
    /// Keys must be added in sorted order.
    fn add(&mut self, key: &[u8], value: &KvValue) -> Result<()>;

    /// Finishes building the file and returns (first_key, last_key, file_size, footer_bytes).
    fn finish(self: Box<Self>) -> Result<(Vec<u8>, Vec<u8>, usize, Bytes)>;

    /// Returns the current offset (bytes written) in the file.
    fn offset(&self) -> usize;

    /// Returns true if no keys have been added yet.
    fn is_empty(&self) -> bool;
}

/// A factory function type for creating FileBuilder instances.
pub type FileBuilderFactory =
    Box<dyn Fn(Box<dyn SequentialWriteFile>) -> Box<dyn FileBuilder> + Send + Sync>;
