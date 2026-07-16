//! File format utilities and shared writer abstractions.

use crate::error::Result;
use crate::file::SequentialWriteFile;
use crate::sst::SstReadMetadata;
use crate::r#type::KvValue;
use bytes::Bytes;
use std::sync::Arc;

/// Metadata returned after finishing one immutable data file.
pub(crate) struct FileBuildResult {
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
    pub(crate) file_size: usize,
    pub(crate) meta_bytes: Bytes,
    pub(crate) sst_read_metadata: Option<Arc<SstReadMetadata>>,
}

impl FileBuildResult {
    pub(crate) fn new(
        first_key: Vec<u8>,
        last_key: Vec<u8>,
        file_size: usize,
        meta_bytes: Bytes,
    ) -> Self {
        Self {
            first_key,
            last_key,
            file_size,
            meta_bytes,
            sst_read_metadata: None,
        }
    }

    pub(crate) fn with_sst_read_metadata(mut self, metadata: SstReadMetadata) -> Self {
        self.sst_read_metadata = Some(Arc::new(metadata));
        self
    }
}

/// A trait for building output files (SST, etc.).
///
/// This trait provides a common interface for different file formats
/// to be used in flush/compaction processes.
pub(crate) trait FileBuilder {
    /// Adds a key-value pair to the file.
    ///
    /// Keys must be added in sorted order.
    fn add(&mut self, key: &[u8], value: &KvValue) -> Result<()>;

    /// Finishes building the file and returns its immutable metadata.
    fn finish(self: Box<Self>) -> Result<FileBuildResult>;

    /// Returns the current offset (bytes written) in the file.
    fn offset(&self) -> usize;

    /// Returns true if no keys have been added yet.
    fn is_empty(&self) -> bool;
}

/// A factory function type for creating FileBuilder instances.
pub(crate) type FileBuilderFactory =
    Box<dyn Fn(Box<dyn SequentialWriteFile>) -> Box<dyn FileBuilder> + Send + Sync>;
