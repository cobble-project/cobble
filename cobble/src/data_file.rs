use crate::file::{FileId, TrackedFileId};
use crate::r#type::key_bucket;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
pub enum DataFileType {
    #[default]
    #[serde(rename = "sst")]
    SSTable,
    #[serde(rename = "parquet")]
    Parquet,
}

impl DataFileType {
    pub fn as_str(self) -> &'static str {
        match self {
            DataFileType::SSTable => "sst",
            DataFileType::Parquet => "parquet",
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
            "parquet" => Ok(DataFileType::Parquet),
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
    /// Optional snapshot-side tracked file id used to reuse uploaded files.
    pub snapshot_data_file: Mutex<Option<Arc<TrackedFileId>>>,
    /// Optional cached meta bytes to avoid re-reading from disk.
    pub meta_bytes: OnceLock<Bytes>,
}

impl DataFile {
    /// Create a new DataFile with the given tracked_id and required fields.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        file_type: DataFileType,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        file_id: FileId,
        tracked_id: Arc<TrackedFileId>,
        schema_id: u64,
        size: usize,
        bucket_range: RangeInclusive<u16>,
        effective_bucket_range: RangeInclusive<u16>,
    ) -> Self {
        Self {
            file_type,
            start_key,
            end_key,
            file_id,
            tracked_id,
            schema_id,
            size,
            bucket_range,
            effective_bucket_range,
            vlog_file_seq_offset: 0,
            has_separated_values: false,
            snapshot_data_file: Default::default(),
            meta_bytes: Default::default(),
        }
    }

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
            schema_id: self.schema_id,
            size: self.size,
            bucket_range: self.bucket_range.clone(),
            effective_bucket_range: range,
            vlog_file_seq_offset: self.vlog_file_seq_offset,
            has_separated_values: self.has_separated_values,
            snapshot_data_file: Mutex::new(
                self.snapshot_data_file
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            ),
            meta_bytes: Default::default(),
        };
        data_file.copy_meta_from(self);
        data_file
    }

    /// Create a detached DataFile (for snapshots / read-only use).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_detached(
        file_type: DataFileType,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        file_id: FileId,
        schema_id: u64,
        size: usize,
        bucket_range: RangeInclusive<u16>,
        effective_bucket_range: RangeInclusive<u16>,
    ) -> Self {
        Self::new(
            file_type,
            start_key,
            end_key,
            file_id,
            TrackedFileId::detached(file_id),
            schema_id,
            size,
            bucket_range,
            effective_bucket_range,
        )
    }

    /// Builder-style setter for vlog_file_seq_offset.
    pub(crate) fn with_vlog_offset(mut self, offset: u32) -> Self {
        self.vlog_file_seq_offset = offset;
        self
    }

    /// Builder-style setter for has_separated_values.
    pub(crate) fn with_separated_values(mut self, separated: bool) -> Self {
        self.has_separated_values = separated;
        self
    }

    /// Copy meta_bytes from another DataFile if present.
    pub(crate) fn copy_meta_from(&self, source: &DataFile) {
        if let Some(meta_bytes) = source.meta_bytes() {
            self.set_meta_bytes(meta_bytes);
        }
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

    pub fn snapshot_data_file_id(&self) -> Option<FileId> {
        self.snapshot_data_file
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|tracked_id| tracked_id.file_id())
    }

    pub fn set_snapshot_data_file(&self, tracked_id: Arc<TrackedFileId>) {
        let mut guard = self
            .snapshot_data_file
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = Some(tracked_id);
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
