//! FileBuilder trait for compaction output file builders.
//!
//! This module provides the `FileBuilder` trait that abstracts the interface
//! for building output files during compaction. Different file formats
//! (SST, etc.) can implement this trait.

use crate::error::Result;

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

    /// Finishes building the file and returns (first_key, last_key).
    ///
    /// After calling this method, the builder is consumed and the file
    /// is ready to be read.
    fn finish(self: Box<Self>) -> Result<(Vec<u8>, Vec<u8>)>;

    /// Returns the current offset (bytes written) in the file.
    fn offset(&self) -> usize;

    /// Returns true if no keys have been added yet.
    fn is_empty(&self) -> bool;
}
