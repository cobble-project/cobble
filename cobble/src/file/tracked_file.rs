//! File handles that keep files live and account for their eventual deletion.

use super::file_manager::FileManager;
use super::volume::DataVolume;
use crate::error::{Error, Result};
use crate::file::file_system::FileSystem;
use crate::file::files::{File, RandomAccessFile, SequentialWriteFile};
use crate::file::logical_file::LogicalFile;
use crate::storage_url::append_storage_path;
use bytes::Bytes;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

pub(crate) const VLOG_FILE_PRIORITY: u8 = 10;
const DEFAULT_TRACKED_FILE_PRIORITY: u8 = u8::MAX;

#[inline(always)]
pub(crate) fn lsm_file_priority_for_level(level: u8) -> u8 {
    u8::MAX
        .saturating_sub(level * 5 + 5)
        .max(VLOG_FILE_PRIORITY + 1)
}

/// A unique identifier for data files managed by the FileManager.
pub type FileId = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum PhysicalDeletePolicy {
    ManagedDelete,
    Retained,
}

/// Information about a tracked file.
pub struct TrackedFile {
    /// The path to the file relative to the file system root.
    pub(crate) path: String,
    /// The underlying file system (needed for deletion on drop).
    pub(crate) fs: Arc<dyn FileSystem>,
    /// Optional volume usage and deletion owner.
    pub(crate) volume: Option<Arc<DataVolume>>,
    /// Whether this file's bytes belong to this DB's managed volume usage.
    accounted_on_volume: bool,
    /// Bytes tracked for this file.
    pub(crate) size_bytes: AtomicU64,
    /// Which process is responsible for physically deleting the file.
    physical_delete_policy: AtomicU8,
    /// Count of explicit references to this file (e.g., from snapshots).
    pub(crate) explicit_refs: AtomicU32,
    /// File priority used by primary-volume offload policy.
    pub(crate) priority: AtomicU8,
}

impl TrackedFile {
    pub(crate) fn fs(&self) -> &Arc<dyn FileSystem> {
        &self.fs
    }
    /// Creates a tracked file whose final drop deletes the physical file.
    pub(crate) fn managed(
        path: String,
        fs: Arc<dyn FileSystem>,
        volume: Option<Arc<DataVolume>>,
    ) -> Self {
        Self {
            path,
            fs,
            volume,
            accounted_on_volume: true,
            size_bytes: AtomicU64::new(0),
            physical_delete_policy: AtomicU8::new(PhysicalDeletePolicy::ManagedDelete as u8),
            explicit_refs: AtomicU32::new(0),
            priority: AtomicU8::new(DEFAULT_TRACKED_FILE_PRIORITY),
        }
    }

    /// Creates a tracked file retained by another lifecycle owner.
    pub(crate) fn retained(
        path: String,
        fs: Arc<dyn FileSystem>,
        volume: Option<Arc<DataVolume>>,
    ) -> Self {
        Self {
            path,
            fs,
            volume,
            accounted_on_volume: true,
            size_bytes: AtomicU64::new(0),
            physical_delete_policy: AtomicU8::new(PhysicalDeletePolicy::Retained as u8),
            explicit_refs: AtomicU32::new(0),
            priority: AtomicU8::new(DEFAULT_TRACKED_FILE_PRIORITY),
        }
    }

    /// Creates a view of another database's file. The view must neither delete nor consume this
    /// database's volume quota.
    pub(crate) fn external_view(
        path: String,
        fs: Arc<dyn FileSystem>,
        volume: Option<Arc<DataVolume>>,
    ) -> Self {
        Self {
            path,
            fs,
            volume,
            accounted_on_volume: false,
            size_bytes: AtomicU64::new(0),
            physical_delete_policy: AtomicU8::new(PhysicalDeletePolicy::Retained as u8),
            explicit_refs: AtomicU32::new(0),
            priority: AtomicU8::new(DEFAULT_TRACKED_FILE_PRIORITY),
        }
    }

    pub(super) fn set_physical_delete_policy(&self, policy: PhysicalDeletePolicy) {
        self.physical_delete_policy
            .store(policy as u8, Ordering::SeqCst);
    }

    pub(super) fn physical_delete_policy(&self) -> PhysicalDeletePolicy {
        match self.physical_delete_policy.load(Ordering::SeqCst) {
            value if value == PhysicalDeletePolicy::ManagedDelete as u8 => {
                PhysicalDeletePolicy::ManagedDelete
            }
            _ => PhysicalDeletePolicy::Retained,
        }
    }

    /// Returns the path to the file.
    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub fn absolute_path(&self) -> String {
        let Some(volume) = &self.volume else {
            return self.path.clone();
        };
        let Some(base_dir) = volume.base_dir() else {
            return self.path.clone();
        };
        append_storage_path(base_dir, &self.path)
    }

    pub fn reference(&self) {
        self.explicit_refs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dereference(&self) {
        self.explicit_refs.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn update_size_bytes(&self, delta: u64) {
        if delta == 0 {
            return;
        }
        self.size_bytes.fetch_add(delta, Ordering::SeqCst);
        if self.accounted_on_volume
            && let Some(volume) = &self.volume
        {
            volume.add_usage(delta);
        }
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes.load(Ordering::SeqCst)
    }

    pub(crate) fn set_priority(&self, priority: u8) {
        self.priority.store(priority, Ordering::SeqCst);
    }

    pub(crate) fn priority(&self) -> u8 {
        self.priority.load(Ordering::SeqCst)
    }

    pub(crate) fn is_snapshot_persistable(&self) -> bool {
        self.volume
            .as_ref()
            .is_some_and(|volume| volume.snapshot_persistable)
    }
}

/// Handle that keeps a data file id tracked by the FileManager.
pub struct TrackedFileId {
    file_id: FileId,
    file_manager: Weak<FileManager>,
    logical_file: Option<Arc<crate::file::logical_file::LogicalFile>>,
}

impl TrackedFileId {
    pub fn new(file_manager: &Arc<FileManager>, file_id: FileId) -> Arc<Self> {
        Arc::new(Self {
            file_id,
            file_manager: Arc::downgrade(file_manager),
            logical_file: file_manager.get_logical_file(file_id),
        })
    }

    pub fn file_id(&self) -> FileId {
        self.file_id
    }

    pub fn untracked(file_id: FileId) -> Arc<Self> {
        Arc::new(Self {
            file_id,
            file_manager: Weak::new(),
            logical_file: None,
        })
    }

    /// Retains a runtime file without unregistering it when this read-only view is dropped.
    pub(crate) fn retained(logical_file: Arc<LogicalFile>) -> Arc<Self> {
        Arc::new(Self {
            file_id: logical_file.file_id,
            file_manager: Weak::new(),
            logical_file: Some(logical_file),
        })
    }

    pub(crate) fn set_priority(&self, priority: u8) -> Result<()> {
        let Some(file_manager) = self.file_manager.upgrade() else {
            return Ok(());
        };
        file_manager.set_data_file_priority(self.file_id, priority)
    }

    pub(crate) fn logical_file(&self) -> Option<Arc<crate::file::logical_file::LogicalFile>> {
        self.logical_file.clone()
    }
}

impl Drop for TrackedFileId {
    fn drop(&mut self) {
        if let Some(file_manager) = self.file_manager.upgrade() {
            let _ = file_manager.remove_data_file(self.file_id);
        }
    }
}

impl Drop for TrackedFile {
    fn drop(&mut self) {
        if self.physical_delete_policy() == PhysicalDeletePolicy::ManagedDelete
            && self.explicit_refs.load(Ordering::SeqCst) == 0
        {
            if let Some(volume) = &self.volume {
                volume
                    .deletions
                    .submit(Arc::clone(&self.fs), self.path.clone());
            } else {
                let _ = self.fs.delete_async(&self.path);
            }
        }
        if self.accounted_on_volume
            && let Some(volume) = &self.volume
        {
            let size = self.size_bytes.load(Ordering::SeqCst);
            if size > 0 {
                volume.subtract_usage(size);
            }
        }
    }
}

/// A wrapper around a RandomAccessFile that holds a reference to the TrackedFile.
/// This ensures the TrackedFile is not dropped while the file is in use.
pub struct TrackedReader {
    inner: Arc<dyn RandomAccessFile>,
    _tracked: Arc<TrackedFile>,
}

impl TrackedReader {
    /// Creates a new TrackedReader.
    pub fn new(inner: Arc<dyn RandomAccessFile>, tracked: Arc<TrackedFile>) -> Self {
        Self {
            inner,
            _tracked: tracked,
        }
    }
}

impl File for TrackedReader {
    fn close(&mut self) -> Result<(), Error> {
        Arc::get_mut(&mut self.inner).map_or(Ok(()), File::close)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

impl RandomAccessFile for TrackedReader {
    fn prefers_read_ahead(&self) -> bool {
        self.inner.prefers_read_ahead()
    }

    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error> {
        self.inner.read_at(offset, size)
    }

    fn read_at_async(
        self: Arc<Self>,
        offset: usize,
        size: usize,
        runtime: &tokio::runtime::Handle,
    ) -> tokio::task::JoinHandle<Result<Bytes, Error>> {
        Arc::clone(&self.inner).read_at_async(offset, size, runtime)
    }
}

/// A wrapper around a SequentialWriteFile that holds a reference to the TrackedFile.
/// This ensures the TrackedFile is not dropped while the file is being written.
pub struct TrackedWriter {
    inner: Box<dyn SequentialWriteFile>,
    tracked: Arc<TrackedFile>,
    logical_file: Option<Arc<LogicalFile>>,
}

impl TrackedWriter {
    /// Creates a new TrackedWriter.
    pub fn new(inner: Box<dyn SequentialWriteFile>, tracked: Arc<TrackedFile>) -> Self {
        Self {
            inner,
            tracked,
            logical_file: None,
        }
    }

    pub(super) fn new_for_logical(
        inner: Box<dyn SequentialWriteFile>,
        tracked: Arc<TrackedFile>,
        logical_file: Arc<LogicalFile>,
    ) -> Self {
        Self {
            inner,
            tracked,
            logical_file: Some(logical_file),
        }
    }
}

impl File for TrackedWriter {
    fn close(&mut self) -> Result<(), Error> {
        self.inner.close()?;
        if let Some(logical) = &self.logical_file {
            logical.finish_staging_replica();
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

impl SequentialWriteFile for TrackedWriter {
    fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        let written = self.inner.write(data)?;
        self.tracked.update_size_bytes(written as u64);
        Ok(written)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/file/tracked_file.rs"]
mod tests;
