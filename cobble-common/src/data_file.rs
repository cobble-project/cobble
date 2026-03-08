use crate::file::{FileId, TrackedFileId};
use crate::r#type::key_bucket;
use bytes::Bytes;
use std::fmt;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataFileType {
    SSTable,
}

impl DataFileType {
    pub fn as_str(self) -> &'static str {
        match self {
            DataFileType::SSTable => "sst",
        }
    }
}

impl fmt::Display for DataFileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DataFileType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "sst" => Ok(DataFileType::SSTable),
            _ => Err(format!("Unknown data file type: {}", s)),
        }
    }
}

pub struct DataFile {
    pub file_type: DataFileType,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    /// Unique file identifier assigned by the FileManager.
    pub file_id: FileId,
    /// Handle for removing the file when LSM drops references.
    pub(crate) tracked_id: Arc<TrackedFileId>,
    /// Maximum sequence id for data contained in this file.
    pub seq: u64,
    /// Schema id used when this data file was written.
    pub schema_id: u64,
    /// Size of the file in bytes.
    pub size: usize,
    /// Full bucket range covered by keys physically present in this file.
    pub bucket_range: RangeInclusive<u16>,
    /// Bucket range visible to the owning LSM tree.
    pub effective_bucket_range: RangeInclusive<u16>,
    /// Per-file offset applied to VLOG file seq pointers for this SST.
    pub vlog_file_seq_offset: u32,
    /// Whether this file contains separated value columns/pointers.
    pub has_separated_values: bool,
    /// Optional cached meta bytes to avoid re-reading from disk.
    pub meta_bytes: OnceLock<Bytes>,
}

impl DataFile {
    pub(crate) fn bucket_range_from_keys(start_key: &[u8], end_key: &[u8]) -> RangeInclusive<u16> {
        let start = key_bucket(start_key).unwrap_or(0);
        let end = key_bucket(end_key).unwrap_or(u16::MAX);
        let (start, end) = if start <= end {
            (start, end)
        } else {
            (end, start)
        };
        start..=end
    }

    pub(crate) fn with_effective_bucket_range(&self, range: RangeInclusive<u16>) -> Self {
        let data_file = Self {
            file_type: self.file_type,
            start_key: self.start_key.clone(),
            end_key: self.end_key.clone(),
            file_id: self.file_id,
            tracked_id: Arc::clone(&self.tracked_id),
            seq: self.seq,
            schema_id: self.schema_id,
            size: self.size,
            bucket_range: self.bucket_range.clone(),
            effective_bucket_range: range,
            vlog_file_seq_offset: self.vlog_file_seq_offset,
            has_separated_values: self.has_separated_values,
            meta_bytes: Default::default(),
        };
        if let Some(meta_bytes) = self.meta_bytes() {
            data_file.set_meta_bytes(meta_bytes);
        }
        data_file
    }

    pub(crate) fn needs_bucket_filter(&self) -> bool {
        self.effective_bucket_range.start() > self.bucket_range.start()
            || self.effective_bucket_range.end() < self.bucket_range.end()
    }

    pub fn meta_bytes(&self) -> Option<Bytes> {
        self.meta_bytes.get().cloned()
    }

    pub fn set_meta_bytes(&self, bytes: Bytes) {
        let _ = self.meta_bytes.set(bytes);
    }

    pub fn has_separated_values(&self) -> bool {
        self.has_separated_values
    }
}

pub(crate) fn intersect_bucket_ranges(
    left: &RangeInclusive<u16>,
    right: &RangeInclusive<u16>,
) -> Option<RangeInclusive<u16>> {
    let start = (*left.start()).max(*right.start());
    let end = (*left.end()).min(*right.end());
    (start <= end).then_some(start..=end)
}
