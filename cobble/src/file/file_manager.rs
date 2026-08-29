//! File manager for managing files in a KV storage engine.
//!
//! The FileManager handles two types of files:
//! - **Metadata files**: Identified by special filenames or IDs (e.g., manifest, WAL)
//! - **Data files**: Identified by automatically generated file IDs (e.g., SST files)
//!
//! The FileManager is responsible for:
//! - Tracking all alive files in the engine
//! - Keeping files open and reusing file readers
//! - Closing and deleting files when no longer needed
//! - Creating files for writing and assigning file IDs

use crate::Config;
use crate::config::{PrimaryVolumeOffloadPolicyKind, VolumeUsageKind};
use crate::error::{Error, Result};
use crate::file::file_system::{FileSystem, FileSystemRegistry};
use crate::file::files::{File, RandomAccessFile, SequentialWriteFile};
use crate::file::logical_file::{
    FileCommitState, LogicalFile, ReplicaId, ReplicaLifecycle, ReplicaOrigin,
};
use crate::file::metadata_io::MetadataWriter;
use crate::file::offload::OffloadRuntime;
use crate::lru::LruCache;
use crate::metrics_manager::MetricsManager;
use crate::snapshot::SnapshotLifecycleState;
use crate::util::normalize_storage_path_to_url;
use bytes::Bytes;
use dashmap::DashMap;
use metrics::{Counter, Gauge, counter, gauge};
use rand::random;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use uuid::Uuid;

const DATA_DIR: &str = "data";
const SNAPSHOT_DIR: &str = "snapshot";
const SCHEMA_DIR: &str = "schema";
const DEFAULT_BASE_FILE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_READER_CACHE_CAPACITY: usize = 512;
const SNAPSHOT_COPY_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const VLOG_FILE_PRIORITY: u8 = 10;
const DEFAULT_TRACKED_FILE_PRIORITY: u8 = u8::MAX;

type DurableReplicaRoutePublisher = Arc<dyn Fn() -> Result<()> + Send + Sync>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum PhysicalDeletePolicy {
    ManagedDelete,
    Retained,
}

/// Reader-cache identity for one concrete replica of a logical file.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ReplicaKey {
    file_id: FileId,
    replica_id: crate::file::logical_file::ReplicaId,
}

#[inline(always)]
pub(crate) fn lsm_file_priority_for_level(level: u8) -> u8 {
    u8::MAX
        .saturating_sub(level * 5 + 5)
        .max(VLOG_FILE_PRIORITY + 1)
}

struct CachedRandomAccessFile {
    inner: Arc<dyn RandomAccessFile>,
}

impl CachedRandomAccessFile {
    fn new(inner: Arc<dyn RandomAccessFile>) -> Self {
        Self { inner }
    }
}

impl File for CachedRandomAccessFile {
    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

impl RandomAccessFile for CachedRandomAccessFile {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VolumePriority {
    High,
    Medium,
    Low,
}

impl VolumePriority {
    pub(crate) fn rank(self) -> u8 {
        match self {
            VolumePriority::High => 3,
            VolumePriority::Medium => 2,
            VolumePriority::Low => 1,
        }
    }
}

struct VolumeUsage {
    used_bytes: AtomicU64,
}

impl Clone for VolumeUsage {
    fn clone(&self) -> Self {
        Self {
            used_bytes: AtomicU64::new(self.used_bytes.load(Ordering::SeqCst)),
        }
    }
}

pub(crate) struct DataVolume {
    pub(crate) fs: Arc<dyn FileSystem>,
    pub(crate) base_dir: Option<String>,
    pub(crate) size_limit: Option<u64>,
    pub(crate) used_bytes: Arc<AtomicU64>,
    storage_file_bytes: Option<Gauge>,
    pub(crate) projected_offload_bytes: AtomicU64,
    pub(crate) priority: VolumePriority,
    pub(crate) supports_primary_data: bool,
    pub(crate) supports_meta: bool,
    pub(crate) snapshot_persistable: bool,
    pub(crate) readonly_source: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PrimaryResidualFile {
    pub(crate) file_name: String,
    pub(crate) absolute_path: String,
    pub(crate) size_bytes: u64,
}

impl Clone for DataVolume {
    fn clone(&self) -> Self {
        Self {
            fs: Arc::clone(&self.fs),
            base_dir: self.base_dir.clone(),
            size_limit: self.size_limit,
            used_bytes: Arc::clone(&self.used_bytes),
            storage_file_bytes: self.storage_file_bytes.clone(),
            projected_offload_bytes: AtomicU64::new(
                self.projected_offload_bytes.load(Ordering::SeqCst),
            ),
            priority: self.priority,
            supports_primary_data: self.supports_primary_data,
            supports_meta: self.supports_meta,
            snapshot_persistable: self.snapshot_persistable,
            readonly_source: self.readonly_source,
        }
    }
}

impl DataVolume {
    pub(crate) fn base_dir(&self) -> Option<&str> {
        self.base_dir.as_deref()
    }

    pub(crate) fn add_usage(&self, bytes: u64) {
        let mut current = self.used_bytes.load(Ordering::SeqCst);
        loop {
            let next = current.saturating_add(bytes);
            match self.used_bytes.compare_exchange(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    if let Some(gauge) = &self.storage_file_bytes {
                        gauge.set(next as f64);
                    }
                    break;
                }
                Err(updated) => current = updated,
            }
        }
    }

    pub(crate) fn add_projected_offload_bytes(&self, bytes: u64) {
        self.projected_offload_bytes
            .fetch_add(bytes, Ordering::SeqCst);
    }

    pub(crate) fn subtract_projected_offload_bytes(&self, bytes: u64) {
        let mut current = self.projected_offload_bytes.load(Ordering::SeqCst);
        loop {
            let next = current.saturating_sub(bytes);
            match self.projected_offload_bytes.compare_exchange(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(updated) => current = updated,
            }
        }
    }

    pub(crate) fn projected_offload_bytes(&self) -> u64 {
        self.projected_offload_bytes.load(Ordering::SeqCst)
    }

    pub(crate) fn subtract_usage(&self, bytes: u64) {
        let mut current = self.used_bytes.load(Ordering::SeqCst);
        loop {
            // Use saturating_sub to avoid underflow, but ensure it doesn't go negative.
            let next = current.saturating_sub(bytes);
            match self.used_bytes.compare_exchange(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    if let Some(gauge) = &self.storage_file_bytes {
                        gauge.set(next as f64);
                    }
                    break;
                }
                Err(updated) => current = updated,
            }
        }
    }

    pub(crate) fn is_full(&self, base_file_size: u64) -> bool {
        let Some(limit) = self.size_limit else {
            return false;
        };
        let threshold = limit.saturating_sub(base_file_size);
        let used = self.used_bytes.load(Ordering::SeqCst);
        used >= threshold
    }

    pub(crate) fn is_write_stopped(&self, write_stop_watermark: f64) -> bool {
        self.usage_ratio()
            .map(|ratio| ratio >= write_stop_watermark)
            .unwrap_or(false)
    }

    pub(crate) fn is_write_stopped_with_expected(
        &self,
        write_stop_watermark: f64,
        expected_write_bytes: u64,
    ) -> bool {
        let Some(limit) = self.size_limit else {
            return false;
        };
        if limit == 0 {
            return true;
        }
        let used = self.used_bytes.load(Ordering::SeqCst);
        let projected = used.saturating_add(expected_write_bytes);
        (projected as f64 / limit as f64) >= write_stop_watermark
    }

    pub(crate) fn usage_ratio(&self) -> Option<f64> {
        let limit = self.size_limit?;
        if limit == 0 {
            return Some(1.0);
        }
        let used = self.used_bytes.load(Ordering::SeqCst);
        Some((used as f64 / limit as f64).min(1.0))
    }

    pub(crate) fn fs(&self) -> &Arc<dyn FileSystem> {
        &self.fs
    }
}

/// A unique identifier for data files managed by the FileManager.
pub type FileId = u64;

pub(crate) trait SnapshotCopyResourceRegistry: Send + Sync {
    fn register_temp_copied_replica(&self, logical: Arc<LogicalFile>, replica_id: ReplicaId);
}

pub(crate) trait RestoreCopyResourceRegistry: Send + Sync {
    fn register_temp_restored_copy(&self, file_id: FileId);
}

/// Configuration options for the FileManager.
pub struct FileManagerOptions {
    /// Base directory for file storage (relative to the file system root).
    pub base_dir: String,
    /// File extension for data files (e.g., "sst").
    pub data_file_extension: String,
    /// Base SST file size used for volume threshold calculations.
    pub base_file_size: usize,
    /// Usage ratio watermark for stopping new writes on a primary volume.
    pub primary_volume_write_stop_watermark: f64,
    /// Usage ratio watermark for triggering background offload from a primary volume.
    pub primary_volume_offload_trigger_watermark: f64,
    /// Usage ratio below which higher-priority primary volumes are backfilled.
    pub primary_volume_backfill_trigger_watermark: f64,
    /// Maximum number of background file transfers executed concurrently.
    pub file_transfer_concurrency: usize,
    /// Offload policy for selecting candidate files.
    pub primary_volume_offload_policy: PrimaryVolumeOffloadPolicyKind,
    /// Whether VLog files newly created or copied into primary use the lowest-priority tier.
    pub vlog_low_priority_primary_enabled: bool,
}

/// Placement policy for a file that is becoming locally primary.
///
/// Ordinary SSTs retain the normal highest-priority-first behavior. VLog files newly created or
/// copied into primary may opt into the lowest primary tier through [`FileManagerOptions`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PrimaryDataPlacement {
    Standard,
    Vlog,
}

impl Default for FileManagerOptions {
    fn default() -> Self {
        Self {
            base_dir: "".to_string(),
            data_file_extension: "sst".to_string(),
            base_file_size: DEFAULT_BASE_FILE_SIZE,
            primary_volume_write_stop_watermark: 0.95,
            primary_volume_offload_trigger_watermark: 0.85,
            primary_volume_backfill_trigger_watermark: 0.40,
            file_transfer_concurrency: 4,
            primary_volume_offload_policy: PrimaryVolumeOffloadPolicyKind::Priority,
            vlog_low_priority_primary_enabled: false,
        }
    }
}

/// Information about a tracked file.
pub struct TrackedFile {
    /// The path to the file relative to the file system root.
    pub(crate) path: String,
    /// The underlying file system (needed for deletion on drop).
    pub(crate) fs: Arc<dyn FileSystem>,
    /// Optional volume usage tracker.
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

    pub(crate) fn set_physical_delete_policy(&self, policy: PhysicalDeletePolicy) {
        self.physical_delete_policy
            .store(policy as u8, Ordering::SeqCst);
    }

    pub(crate) fn physical_delete_policy(&self) -> PhysicalDeletePolicy {
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
        format!("{}/{}", base_dir, self.path)
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
            // Attempt to delete the file, ignore errors
            let _ = self.fs.delete_async(&self.path);
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

    fn new_for_logical(
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

pub struct AtomicMetadataWriter {
    temp_path: String,
    final_name: String,
    final_path: String,
    writer: Option<MetadataWriter<TrackedWriter>>,
    fs: Arc<dyn FileSystem>,
    metadata_files: Arc<DashMap<String, Arc<TrackedFile>>>,
    metadata_files_gauge: Gauge,
    volume: Option<Arc<DataVolume>>,
}

impl AtomicMetadataWriter {
    #[allow(clippy::too_many_arguments)]
    fn new(
        temp_path: String,
        final_name: String,
        final_path: String,
        writer: TrackedWriter,
        fs: Arc<dyn FileSystem>,
        metadata_files: Arc<DashMap<String, Arc<TrackedFile>>>,
        metadata_files_gauge: Gauge,
        volume: Option<Arc<DataVolume>>,
    ) -> Self {
        Self {
            temp_path,
            final_name,
            final_path,
            writer: Some(MetadataWriter::new(writer)),
            fs,
            metadata_files,
            metadata_files_gauge,
            volume,
        }
    }

    fn finalize(&mut self) -> Result<()> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };
        let mut writer = writer;
        writer.close()?;
        let size = self.fs.open_read(&self.temp_path)?.size() as u64;
        self.fs.rename(&self.temp_path, &self.final_path)?;
        let tracked = Arc::new(TrackedFile::retained(
            self.final_path.clone(),
            Arc::clone(&self.fs),
            self.volume.clone(),
        ));
        tracked.update_size_bytes(size);
        self.metadata_files
            .insert(self.final_name.clone(), Arc::clone(&tracked));
        self.metadata_files_gauge
            .set(self.metadata_files.len() as f64);
        Ok(())
    }
}

impl File for AtomicMetadataWriter {
    fn close(&mut self) -> Result<(), Error> {
        self.finalize()
    }

    fn size(&self) -> usize {
        self.writer.as_ref().map(|w| w.size()).unwrap_or(0)
    }
}

impl SequentialWriteFile for AtomicMetadataWriter {
    fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        match self.writer.as_mut() {
            Some(writer) => writer.write(data),
            None => Err(Error::IoError("Atomic writer already closed".to_string())),
        }
    }
}

#[derive(Clone)]
pub(crate) struct FileManagerMetrics {
    data_files_tracked: Gauge,
    metadata_files_tracked: Gauge,
    offload_jobs_scheduled_total: Counter,
    offload_jobs_completed_total: Counter,
    offload_jobs_failed_total: Counter,
    offload_jobs_noop_total: Counter,
    offload_bytes_moved_total: Counter,
    offload_promotions_total: Counter,
}

impl FileManagerMetrics {
    pub(crate) fn new(db_id: &str) -> Self {
        let db_id = db_id.to_string();
        Self {
            data_files_tracked: gauge!("data_files_tracked", "db_id" => db_id.clone()),
            metadata_files_tracked: gauge!("metadata_files_tracked", "db_id" => db_id.clone()),
            offload_jobs_scheduled_total: counter!(
                "offload_jobs_scheduled_total",
                "db_id" => db_id.clone()
            ),
            offload_jobs_completed_total: counter!(
                "offload_jobs_completed_total",
                "db_id" => db_id.clone()
            ),
            offload_jobs_failed_total: counter!(
                "offload_jobs_failed_total",
                "db_id" => db_id.clone()
            ),
            offload_jobs_noop_total: counter!("offload_jobs_noop_total", "db_id" => db_id.clone()),
            offload_bytes_moved_total: counter!("offload_bytes_moved_total", "db_id" => db_id.clone()),
            offload_promotions_total: counter!("offload_promotions_total", "db_id" => db_id),
        }
    }
}

/// File manager for managing files in a KV storage engine.
///
/// The FileManager is responsible for managing both metadata files and data files.
/// It provides file ID assignment, reader caching, and file lifecycle management.
pub struct FileManager {
    pub(crate) metrics: FileManagerMetrics,
    /// The metadata volume for metadata files.
    pub(crate) meta_volume: Arc<DataVolume>,
    /// Ordered data volumes by priority (high to low).
    pub(crate) data_volumes: Vec<Arc<DataVolume>>,
    /// Configuration options.
    pub(crate) options: FileManagerOptions,
    /// Counter for generating unique file IDs.
    pub(crate) next_file_id: AtomicU64,
    /// Map of logical IDs to their physical replicas and lifecycle.
    pub(crate) logical_files: DashMap<FileId, Arc<LogicalFile>>,
    /// Map of filename to tracked file information for metadata files.
    pub(crate) metadata_files: Arc<DashMap<String, Arc<TrackedFile>>>,
    /// LRU cache for open random access readers, keyed by physical replica.
    pub(crate) reader_cache: Mutex<LruCache<ReplicaKey, Arc<dyn RandomAccessFile>>>,
    pub(crate) offload_runtime: Arc<OffloadRuntime>,
    durable_replica_route_publisher: OnceLock<DurableReplicaRoutePublisher>,
}

impl FileManager {
    /// Limits all background data transfers, including primary tiering and snapshot copies.
    pub(crate) fn transfer_semaphore(&self) -> Arc<tokio::sync::Semaphore> {
        self.offload_runtime.transfer_semaphore()
    }

    /// Installs the single callback that makes durable replica-route changes visible to dedicated
    /// compaction. Callers must capture the publisher weakly to avoid a reference cycle.
    pub(crate) fn install_durable_replica_route_publisher(
        &self,
        publisher: DurableReplicaRoutePublisher,
    ) {
        assert!(
            self.durable_replica_route_publisher.set(publisher).is_ok(),
            "replica route publisher already installed"
        );
    }

    pub(crate) fn publish_durable_replica_route(&self) -> Result<()> {
        let publisher = self.durable_replica_route_publisher.get().cloned();
        publisher.map_or(Ok(()), |publisher| publisher())
    }

    fn is_volume_write_stopped(&self, volume: &Arc<DataVolume>, expected_write_bytes: u64) -> bool {
        volume.is_write_stopped_with_expected(
            self.options.primary_volume_write_stop_watermark,
            expected_write_bytes,
        )
    }

    fn sort_data_volumes(mut volumes: Vec<Arc<DataVolume>>) -> Vec<Arc<DataVolume>> {
        volumes.sort_by_key(|volume| std::cmp::Reverse(volume.priority.rank()));
        volumes
    }

    fn ensure_volume_dirs(fs: &Arc<dyn FileSystem>, options: &FileManagerOptions) -> Result<()> {
        if !options.base_dir.is_empty() && !fs.exists(&options.base_dir)? {
            fs.create_dir(&options.base_dir)?;
        }
        let data_dir = if options.base_dir.is_empty() {
            DATA_DIR.to_string()
        } else {
            format!("{}/{}", options.base_dir, DATA_DIR)
        };
        if !fs.exists(&data_dir)? {
            fs.create_dir(&data_dir)?;
        }
        let snapshot_dir = if options.base_dir.is_empty() {
            SNAPSHOT_DIR.to_string()
        } else {
            format!("{}/{}", options.base_dir, SNAPSHOT_DIR)
        };
        if !fs.exists(&snapshot_dir)? {
            fs.create_dir(&snapshot_dir)?;
        }
        let schema_dir = if options.base_dir.is_empty() {
            SCHEMA_DIR.to_string()
        } else {
            format!("{}/{}", options.base_dir, SCHEMA_DIR)
        };
        if !fs.exists(&schema_dir)? {
            fs.create_dir(&schema_dir)?;
        }
        Ok(())
    }

    fn select_data_volume(&self, expected_write_bytes: Option<u64>) -> Result<&Arc<DataVolume>> {
        let base_file_size = self.options.base_file_size as u64;
        let expected_write_bytes = expected_write_bytes.unwrap_or(base_file_size);
        let mut selected: Option<&Arc<DataVolume>> = None;
        let mut candidates: Vec<&Arc<DataVolume>> = Vec::with_capacity(self.data_volumes.len());
        for volume in &self.data_volumes {
            if !volume.supports_primary_data {
                continue;
            }
            if !candidates.is_empty() && volume.priority.rank() < candidates[0].priority.rank() {
                break;
            }
            if self.is_volume_write_stopped(volume, expected_write_bytes) {
                continue;
            }
            if volume.is_full(base_file_size.max(expected_write_bytes)) {
                continue;
            }
            candidates.push(volume);
        }
        if candidates.len() == 1 {
            selected = Some(candidates[0]);
        } else if !candidates.is_empty() {
            // Randomly select among candidates of the same priority
            let idx = random::<usize>() % candidates.len();
            selected = Some(candidates[idx]);
        }
        selected.ok_or_else(|| Error::IoError("All primary data volumes are full".to_string()))
    }

    /// Selects only the configured lowest-priority primary tier for a new value-log file.
    ///
    /// The priority tier is determined before capacity filtering. This deliberately does not
    /// fall back to a higher-priority volume when every volume in the lowest tier is full or
    /// write-stopped: callers that opt into direct VLOG placement expect the file to stay there.
    fn select_lowest_priority_data_volume(
        &self,
        expected_write_bytes: Option<u64>,
    ) -> Result<&Arc<DataVolume>> {
        let base_file_size = self.options.base_file_size as u64;
        let expected_write_bytes = expected_write_bytes.unwrap_or(base_file_size);
        let Some(lowest_rank) = self
            .data_volumes
            .iter()
            .filter(|volume| volume.supports_primary_data && !volume.readonly_source)
            .map(|volume| volume.priority.rank())
            .min()
        else {
            return Err(Error::IoError(
                "No primary data volume is configured for value-log files".to_string(),
            ));
        };

        let candidates: Vec<&Arc<DataVolume>> = self
            .data_volumes
            .iter()
            .filter(|volume| {
                volume.supports_primary_data
                    && !volume.readonly_source
                    && volume.priority.rank() == lowest_rank
                    && !self.is_volume_write_stopped(volume, expected_write_bytes)
                    && !volume.is_full(base_file_size.max(expected_write_bytes))
            })
            .collect();
        match candidates.len() {
            0 => Err(Error::IoError(
                "All lowest-priority primary data volumes are full or write-stopped for value-log files"
                    .to_string(),
            )),
            1 => Ok(candidates[0]),
            _ => Ok(candidates[random::<usize>() % candidates.len()]),
        }
    }

    pub(crate) fn uses_lowest_primary_tier(&self, placement: PrimaryDataPlacement) -> bool {
        placement == PrimaryDataPlacement::Vlog && self.options.vlog_low_priority_primary_enabled
    }

    pub(crate) fn lowest_writable_primary_rank(&self) -> Option<u8> {
        self.data_volumes
            .iter()
            .filter(|volume| volume.supports_primary_data && !volume.readonly_source)
            .map(|volume| volume.priority.rank())
            .min()
    }

    fn select_primary_data_volume_for_placement(
        &self,
        placement: PrimaryDataPlacement,
        expected_write_bytes: Option<u64>,
    ) -> Result<&Arc<DataVolume>> {
        if self.uses_lowest_primary_tier(placement) {
            self.select_lowest_priority_data_volume(expected_write_bytes)
        } else {
            self.select_data_volume(expected_write_bytes)
        }
    }

    fn test_existence_for_path(&self, path: &str) -> Result<&Arc<DataVolume>> {
        for volume in &self.data_volumes {
            if volume.fs().exists(path)? {
                return Ok(volume);
            }
        }
        Err(Error::IoError(format!(
            "Data file not found in configured volumes: {}",
            path
        )))
    }

    fn trim_volume_base_dir<'a>(&self, path: &'a str, base_dir: &str) -> &'a str {
        let base_dir = base_dir.trim_end_matches('/');
        let Some(stripped) = path.strip_prefix(base_dir) else {
            return path;
        };
        stripped.trim_start_matches('/')
    }

    /// Resolves a file path to the corresponding data volume and relative path.
    /// This is used when registering existing files to determine which volume they belong to.
    fn resolve_volume_path(&self, path: &str) -> Result<(Arc<DataVolume>, String)> {
        let normalized = normalize_storage_path_to_url(path)?;
        for volume in &self.data_volumes {
            let Some(base_dir) = volume.base_dir() else {
                continue;
            };
            if normalized.starts_with(base_dir) {
                let relative = self.trim_volume_base_dir(&normalized, base_dir);
                return Ok((Arc::clone(volume), relative.to_string()));
            }
        }
        let volume = self.test_existence_for_path(path)?;
        Ok((Arc::clone(volume), path.to_string()))
    }
    fn choose_meta_volume(volumes: &[Arc<DataVolume>]) -> Result<Arc<DataVolume>> {
        if let Some(meta_volume) = volumes.iter().find(|volume| volume.supports_meta) {
            return Ok(Arc::clone(meta_volume));
        }

        if let Some(snapshot_volume) = volumes.iter().find(|volume| volume.snapshot_persistable) {
            return Ok(Arc::clone(snapshot_volume));
        }

        Err(Error::ConfigError(
            "No volume configured for snapshot persistence".to_string(),
        ))
    }

    /// Creates a new FileManager with the given data volumes and options.
    ///
    /// This will create the data and snapshot directories if they don't exist.
    pub fn new(
        data_volumes: Vec<DataVolume>,
        options: FileManagerOptions,
        metrics_manager: Arc<MetricsManager>,
    ) -> Result<Self> {
        if !(0.0..=1.0).contains(&options.primary_volume_write_stop_watermark)
            || !(0.0..=1.0).contains(&options.primary_volume_offload_trigger_watermark)
        {
            return Err(Error::ConfigError(
                "primary volume watermarks must be in [0.0, 1.0]".to_string(),
            ));
        }
        if options.primary_volume_offload_trigger_watermark
            > options.primary_volume_write_stop_watermark
        {
            return Err(Error::ConfigError(
                "primary_volume_offload_trigger_watermark must be <= primary_volume_write_stop_watermark"
                    .to_string(),
            ));
        }
        if !(0.0..=0.80).contains(&options.primary_volume_backfill_trigger_watermark) {
            return Err(Error::ConfigError(
                "primary_volume_backfill_trigger_watermark must be in [0.0, 0.80]".to_string(),
            ));
        }
        if options.file_transfer_concurrency == 0 {
            return Err(Error::ConfigError(
                "file_transfer_concurrency must be greater than zero".to_string(),
            ));
        }
        if data_volumes.is_empty() {
            return Err(Error::ConfigError(
                "No data volumes configured for FileManager".to_string(),
            ));
        }
        if !data_volumes
            .iter()
            .any(|volume| volume.supports_primary_data)
        {
            return Err(Error::ConfigError(
                "No volume configured for primary data storage".to_string(),
            ));
        }
        let data_volumes = data_volumes.into_iter().map(Arc::new).collect::<Vec<_>>();
        let meta_volume = Self::choose_meta_volume(&data_volumes)?;
        let data_volumes = Self::sort_data_volumes(data_volumes);
        for volume in &data_volumes {
            if volume.readonly_source {
                continue;
            }
            Self::ensure_volume_dirs(volume.fs(), &options)?;
        }
        let offload_runtime = Arc::new(OffloadRuntime::new_with_policy_kind(
            &data_volumes,
            options.primary_volume_offload_policy,
            options.file_transfer_concurrency,
        ));
        Ok(Self {
            metrics: metrics_manager.file_manager_metrics(),
            meta_volume,
            data_volumes,
            options,
            next_file_id: AtomicU64::new(1), // Start from 1, 0 is reserved
            logical_files: DashMap::new(),
            metadata_files: Arc::new(DashMap::new()),
            reader_cache: Mutex::new(LruCache::new(DEFAULT_READER_CACHE_CAPACITY)),
            offload_runtime,
            durable_replica_route_publisher: OnceLock::new(),
        })
    }

    /// Creates a new FileManager with default options.
    pub fn with_defaults(
        fs: Arc<dyn FileSystem>,
        metrics_manager: Arc<MetricsManager>,
    ) -> Result<Self> {
        let volume = DataVolume {
            fs,
            base_dir: None,
            size_limit: None,
            used_bytes: Arc::new(AtomicU64::new(0)),
            storage_file_bytes: None,
            projected_offload_bytes: AtomicU64::new(0),
            priority: VolumePriority::High,
            supports_primary_data: true,
            supports_meta: true,
            snapshot_persistable: true,
            readonly_source: false,
        };
        Self::new(vec![volume], FileManagerOptions::default(), metrics_manager)
    }

    pub fn from_config(
        config: &Config,
        db_id: &str,
        metrics_manager: Arc<MetricsManager>,
    ) -> Result<Self> {
        let data_volumes = Self::data_volumes_from_config_with_metrics(config, Some(db_id))?;
        let options = FileManagerOptions {
            base_dir: db_id.to_string(),
            base_file_size: config.base_file_size_bytes()?,
            primary_volume_write_stop_watermark: config.primary_volume_write_stop_watermark,
            primary_volume_offload_trigger_watermark: config
                .primary_volume_offload_trigger_watermark,
            primary_volume_backfill_trigger_watermark: config
                .primary_volume_backfill_trigger_watermark,
            file_transfer_concurrency: config.file_transfer_concurrency,
            primary_volume_offload_policy: config.primary_volume_offload_policy,
            vlog_low_priority_primary_enabled: config.vlog_low_priority_primary_enabled,
            ..FileManagerOptions::default()
        };
        Self::new(data_volumes, options, metrics_manager)
    }

    pub(crate) fn data_volumes_from_config(config: &Config) -> Result<Vec<DataVolume>> {
        Self::data_volumes_from_config_with_metrics(config, None)
    }

    fn data_volumes_from_config_with_metrics(
        config: &Config,
        db_id: Option<&str>,
    ) -> Result<Vec<DataVolume>> {
        let registry = FileSystemRegistry::new();
        let volumes = if config.volumes.is_empty() {
            return Err(Error::ConfigError("No volumes configured".to_string()));
        } else {
            config.volumes.clone()
        };
        let has_explicit_snapshot_volume = volumes
            .iter()
            .any(|volume| volume.supports(VolumeUsageKind::Snapshot));
        let mut data_volumes = Vec::new();
        for (volume_index, volume) in volumes.iter().enumerate() {
            let readonly_source = volume.supports(VolumeUsageKind::Readonly);
            if readonly_source {
                let has_other_kinds = volume.supports(VolumeUsageKind::Meta)
                    || volume.supports(VolumeUsageKind::PrimaryDataPriorityHigh)
                    || volume.supports(VolumeUsageKind::PrimaryDataPriorityMedium)
                    || volume.supports(VolumeUsageKind::PrimaryDataPriorityLow)
                    || volume.supports(VolumeUsageKind::Snapshot)
                    || volume.supports(VolumeUsageKind::Cache)
                    || volume.supports(VolumeUsageKind::Wal);
                if has_other_kinds {
                    return Err(Error::ConfigError(format!(
                        "Volume {} uses readonly and other kinds; readonly must be exclusive",
                        volume.base_dir
                    )));
                }
            }
            let mut priority = None;
            if volume.supports(VolumeUsageKind::PrimaryDataPriorityHigh) {
                priority = Some(VolumePriority::High);
            }
            if priority.is_none() && volume.supports(VolumeUsageKind::PrimaryDataPriorityMedium) {
                priority = Some(VolumePriority::Medium);
            }
            if priority.is_none() && volume.supports(VolumeUsageKind::PrimaryDataPriorityLow) {
                priority = Some(VolumePriority::Low);
            }
            let supports_primary_data = priority.is_some();
            let supports_meta = volume.supports(VolumeUsageKind::Meta);
            let supports_snapshot = if has_explicit_snapshot_volume {
                volume.supports(VolumeUsageKind::Snapshot)
            } else {
                supports_primary_data || volume.supports(VolumeUsageKind::Snapshot)
            };
            if supports_primary_data || supports_snapshot || supports_meta || readonly_source {
                let fs = registry.get_or_register_volume(volume)?;
                let normalized_base_dir = normalize_storage_path_to_url(&volume.base_dir)?;
                data_volumes.push(DataVolume {
                    fs,
                    base_dir: Some(normalized_base_dir),
                    size_limit: volume.size_limit_bytes()?,
                    used_bytes: Arc::new(AtomicU64::new(0)),
                    storage_file_bytes: db_id.map(|db_id| {
                        gauge!(
                            "storage_file_bytes",
                            "db_id" => db_id.to_string(),
                            "volume" => volume_index.to_string()
                        )
                    }),
                    projected_offload_bytes: AtomicU64::new(0),
                    priority: priority.unwrap_or(VolumePriority::Low),
                    supports_primary_data,
                    supports_meta,
                    snapshot_persistable: supports_snapshot,
                    readonly_source,
                });
            }
        }
        if data_volumes.is_empty() {
            return Err(Error::ConfigError(
                "No volume configured for primary data storage".to_string(),
            ));
        }
        if !data_volumes
            .iter()
            .any(|volume| volume.snapshot_persistable)
        {
            return Err(Error::ConfigError(
                "No volume configured for snapshot persistence".to_string(),
            ));
        }
        Ok(data_volumes)
    }

    /// Sets the starting file ID counter.
    ///
    /// This is useful when recovering from a previous state where some file IDs
    /// were already assigned.
    pub fn set_next_file_id(&self, id: FileId) {
        self.next_file_id.store(id, Ordering::SeqCst);
    }

    /// Returns the next file ID that will be assigned.
    pub fn peek_next_file_id(&self) -> FileId {
        self.next_file_id.load(Ordering::SeqCst)
    }

    /// Reserves a contiguous range of file IDs without creating files.
    pub(crate) fn reserve_data_file_ids(&self, count: usize) -> Vec<FileId> {
        if count == 0 {
            return Vec::new();
        }
        let start = self.next_file_id.fetch_add(count as u64, Ordering::SeqCst);
        (start..start + count as u64).collect()
    }

    /// Generates a new unique file ID.
    fn allocate_file_id(&self) -> FileId {
        self.next_file_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Generates the path for a data file with the given ID.
    pub(crate) fn data_file_path(&self, _file_id: FileId) -> String {
        format!(
            "{}/{}/{}.{}",
            self.options.base_dir,
            DATA_DIR,
            Uuid::new_v4(),
            self.options.data_file_extension
        )
    }

    pub(crate) fn data_file_path_with_name(&self, name: &str) -> String {
        format!("{}/{}/{}", self.options.base_dir, DATA_DIR, name)
    }

    /// Best-effort shallow scan of built-in POSIX primary data directories.
    pub(crate) fn scan_primary_residual_files(&self) -> Vec<PrimaryResidualFile> {
        let mut files = Vec::new();
        let data_dir = if self.options.base_dir.is_empty() {
            DATA_DIR.to_string()
        } else {
            format!("{}/{}", self.options.base_dir, DATA_DIR)
        };
        for volume in &self.data_volumes {
            if !volume.supports_primary_data || !volume.fs().is_posix() {
                continue;
            }
            let names = match volume.fs().list(&data_dir) {
                Ok(names) => names,
                Err(err) => {
                    log::warn!(
                        "failed to scan primary residual directory {}: {}",
                        data_dir,
                        err
                    );
                    continue;
                }
            };
            for file_name in names {
                let relative_path = self.data_file_path_with_name(&file_name);
                let Some(size_bytes) = volume.fs().file_size(&relative_path).ok().flatten() else {
                    continue;
                };
                let absolute_path = volume
                    .base_dir()
                    .map(|base_dir| format!("{}/{}", base_dir.trim_end_matches('/'), relative_path))
                    .unwrap_or_else(|| relative_path.clone());
                files.push(PrimaryResidualFile {
                    file_name,
                    absolute_path,
                    size_bytes,
                });
            }
        }
        files
    }

    /// Generates the path for a metadata file with the given name.
    fn metadata_file_path(&self, name: &str) -> String {
        if self.options.base_dir.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", self.options.base_dir, name)
        }
    }

    // =========================================================================
    // Data file operations
    // =========================================================================

    /// Creates a new data file for writing and returns the assigned file ID
    /// along with a writer wrapped with the TrackedFile reference.
    ///
    /// The file is tracked by the FileManager and can be opened for reading
    /// later using `open_data_file_reader`.
    pub fn create_data_file(&self) -> Result<(FileId, TrackedWriter)> {
        let file_id = self.allocate_file_id();
        let volume = self.select_data_volume(None)?;
        let writer = self.create_data_file_writer_on_volume(file_id, volume)?;
        Ok((file_id, writer))
    }

    /// Creates a value-log data file using its configured placement policy.
    ///
    /// Every creation first runs the normal primary-tiering check. With direct low-priority
    /// placement disabled, this keeps the historical route through the highest-priority tier.
    pub(crate) fn create_vlog_data_file(self: &Arc<Self>) -> Result<(FileId, TrackedWriter)> {
        if !self.options.vlog_low_priority_primary_enabled {
            return self.create_data_file_with_offload();
        }
        self.trigger_offload_if_needed()?;
        let file_id = self.allocate_file_id();
        let volume =
            self.select_primary_data_volume_for_placement(PrimaryDataPlacement::Vlog, None)?;
        let writer = self.create_data_file_writer_on_volume(file_id, volume)?;
        Ok((file_id, writer))
    }

    /// Creates a new data file under a custom path prefix (relative to the base dir) instead
    /// of the default `data/` directory. The prefix directory is created if needed.
    pub fn create_data_file_with_prefix(
        &self,
        path_prefix: &str,
    ) -> Result<(FileId, TrackedWriter)> {
        let file_id = self.allocate_file_id();
        let volume = self.select_data_volume(None)?;
        let dir = if self.options.base_dir.is_empty() {
            path_prefix.to_string()
        } else {
            format!("{}/{}", self.options.base_dir, path_prefix)
        };
        // Ensure the prefix directory exists on the selected volume's fs.
        if !volume.fs().exists(&dir)? {
            volume.fs().create_dir(&dir)?;
        }
        let path = format!(
            "{}/{}.{}",
            dir,
            Uuid::new_v4(),
            self.options.data_file_extension
        );
        let tracked = Arc::new(TrackedFile::managed(
            path,
            Arc::clone(volume.fs()),
            Some(Arc::clone(volume)),
        ));
        self.register_logical_file(
            file_id,
            Arc::clone(&tracked),
            ReplicaLifecycle::Staging,
            FileCommitState::Uncommitted,
            ReplicaOrigin::Owned,
        );
        self.report_data_files_gauge();
        let writer = volume.fs().open_write(tracked.path())?;
        let logical = self.get_logical_file(file_id).unwrap();
        Ok((
            file_id,
            TrackedWriter::new_for_logical(writer, tracked, logical),
        ))
    }

    /// Creates a new data file with a specific file ID.
    ///
    /// This is useful when recovering files or when the ID is known in advance.
    /// Returns an error if the file ID is already in use.
    pub fn create_data_file_with_id(
        &self,
        file_id: FileId,
        expected_write_bytes: Option<u64>,
    ) -> Result<TrackedWriter> {
        if self.logical_files.contains_key(&file_id) {
            return Err(Error::IoError(format!(
                "File ID {} is already in use",
                file_id
            )));
        }

        let volume = self.select_data_volume(expected_write_bytes)?;
        let writer = self.create_data_file_writer_on_volume(file_id, volume)?;

        // Update next_file_id if necessary
        let mut current = self.next_file_id.load(Ordering::SeqCst);
        while file_id >= current {
            match self.next_file_id.compare_exchange(
                current,
                file_id + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }

        Ok(writer)
    }

    fn create_data_file_writer_on_volume(
        &self,
        file_id: FileId,
        volume: &Arc<DataVolume>,
    ) -> Result<TrackedWriter> {
        let tracked = Arc::new(TrackedFile::managed(
            self.data_file_path(file_id),
            Arc::clone(volume.fs()),
            Some(Arc::clone(volume)),
        ));
        self.register_logical_file(
            file_id,
            Arc::clone(&tracked),
            ReplicaLifecycle::Staging,
            FileCommitState::Uncommitted,
            ReplicaOrigin::Owned,
        );
        self.report_data_files_gauge();
        let writer = volume.fs().open_write(tracked.path())?;
        let logical = self.get_logical_file(file_id).unwrap();
        Ok(TrackedWriter::new_for_logical(writer, tracked, logical))
    }

    /// Registers an existing data file with the FileManager.
    ///
    /// This is useful when recovering files from disk or when files were
    /// created externally. The file is tracked but no reader is opened.
    pub fn register_data_file(&self, file_id: FileId, path: &str) -> Result<()> {
        if self.logical_files.contains_key(&file_id) {
            return Ok(());
        }
        let (volume, relative_path) = self.resolve_volume_path(path)?;
        if volume.readonly_source {
            return Err(Error::InvalidState(format!(
                "Cannot register readonly source {path} as owned; use an explicit external registration"
            )));
        }
        let fs = Arc::clone(volume.fs());
        let tracked = Arc::new(TrackedFile::managed(
            relative_path,
            Arc::clone(&fs),
            Some(Arc::clone(&volume)),
        ));
        let size = fs
            .open_read(tracked.path())
            .map(|reader| reader.size())
            .unwrap_or(0);
        tracked.update_size_bytes(size as u64);

        // Ensure a logical file entry mirrors this tracked file for multi-replica APIs.
        let lifecycle = ReplicaLifecycle::OwnedReady;
        let origin = ReplicaOrigin::Owned;
        self.register_logical_file(
            file_id,
            tracked,
            lifecycle,
            FileCommitState::Committed,
            origin,
        );
        self.report_data_files_gauge();

        // Update next_file_id if necessary
        let mut current = self.next_file_id.load(Ordering::SeqCst);
        while file_id >= current {
            match self.next_file_id.compare_exchange(
                current,
                file_id + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }

        Ok(())
    }

    /// Registers a verified, locally owned output before the writer publishes it in a durable
    /// layout. It is readable immediately, but remains uncommitted until that barrier.
    pub(crate) fn register_uncommitted_data_file(&self, file_id: FileId, path: &str) -> Result<()> {
        self.register_existing_data_file(
            file_id,
            path,
            ReplicaLifecycle::OwnedReady,
            FileCommitState::Uncommitted,
            PhysicalDeletePolicy::ManagedDelete,
            ReplicaOrigin::Owned,
        )
    }

    fn register_logical_file(
        &self,
        file_id: FileId,
        tracked: Arc<TrackedFile>,
        lifecycle: ReplicaLifecycle,
        commit_state: FileCommitState,
        origin: ReplicaOrigin,
    ) {
        self.logical_files.entry(file_id).or_insert_with(|| {
            Arc::new(LogicalFile::new(
                file_id,
                tracked,
                lifecycle,
                commit_state,
                origin,
            ))
        });
    }

    /// Attaches the logical file for `file_id` (if any) to the given [`DataFile`], so that
    /// multi-replica APIs can be reached from the data file and initializes immutable metadata.
    /// Call this only after all `DataFile` builder fields, including `max_expired_at`, are set.
    /// This makes a finished local output readable, but does not make an uncommitted output
    /// durable; existing manifest/snapshot barriers continue to decide that transition.
    pub(crate) fn finalize_data_file(&self, data_file: &crate::data_file::DataFile) -> Result<()> {
        let file_id = data_file.file_id;
        let logical = self
            .logical_files
            .get(&file_id)
            .map(|e| Arc::clone(e.value()))
            .ok_or_else(|| Error::IoError(format!("Logical file {} is not tracked", file_id)))?;
        logical.initialize_metadata(data_file.build_immutable_metadata())?;
        logical.finish_staging_replica();
        data_file.attach_logical_file(logical);
        Ok(())
    }

    /// Returns the logical file for `file_id`, if registered.
    pub(crate) fn get_logical_file(&self, file_id: FileId) -> Option<Arc<LogicalFile>> {
        self.logical_files
            .get(&file_id)
            .map(|e| Arc::clone(e.value()))
    }

    pub(crate) fn commit_logical_files<I>(&self, file_ids: I)
    where
        I: IntoIterator<Item = FileId>,
    {
        for file_id in file_ids {
            if let Some(logical) = self.get_logical_file(file_id) {
                logical.set_commit_state(FileCommitState::Committed);
            }
        }
    }

    pub(crate) fn copy_reader_to_tracked_writer(
        &self,
        source: &dyn RandomAccessFile,
        writer: &mut TrackedWriter,
    ) -> Result<()> {
        self.copy_reader_to_tracked_writer_with_cancel_and_progress(
            source,
            writer,
            None,
            &mut |_| {},
        )
    }

    pub(crate) fn copy_reader_to_tracked_writer_with_cancel(
        &self,
        source: &dyn RandomAccessFile,
        writer: &mut TrackedWriter,
        lifecycle_state: Option<&AtomicU8>,
    ) -> Result<()> {
        self.copy_reader_to_tracked_writer_with_cancel_and_progress(
            source,
            writer,
            lifecycle_state,
            &mut |_| {},
        )
    }

    pub(crate) fn copy_reader_to_tracked_writer_with_progress(
        &self,
        source: &dyn RandomAccessFile,
        writer: &mut TrackedWriter,
        progress: &mut dyn FnMut(u64),
    ) -> Result<()> {
        self.copy_reader_to_tracked_writer_with_cancel_and_progress(source, writer, None, progress)
    }

    fn copy_reader_to_tracked_writer_with_cancel_and_progress(
        &self,
        source: &dyn RandomAccessFile,
        writer: &mut TrackedWriter,
        lifecycle_state: Option<&AtomicU8>,
        progress: &mut dyn FnMut(u64),
    ) -> Result<()> {
        let source_size = source.size();
        let mut offset = 0usize;
        while offset < source_size {
            if lifecycle_state.is_some_and(SnapshotLifecycleState::is_cancelled_raw) {
                return Err(Error::CancelledError(
                    "Snapshot upload cancelled while copying data files".to_string(),
                ));
            }
            let chunk = SNAPSHOT_COPY_CHUNK_BYTES.min(source_size - offset);
            let bytes = source.read_at(offset, chunk)?;
            let written = writer.write(bytes.as_ref())?;
            progress(written as u64);
            offset += bytes.len();
        }
        if lifecycle_state.is_some_and(SnapshotLifecycleState::is_cancelled_raw) {
            return Err(Error::CancelledError(
                "Snapshot upload cancelled while copying data files".to_string(),
            ));
        }
        writer.close()?;
        Ok(())
    }

    /// Registers an existing data file for restore operations.
    ///
    /// A primary residual registered by the resume scan is preferred without copying. Otherwise,
    /// the requested placement is resolved before deciding whether the manifest source can be
    /// retained or must be copied.
    ///
    /// Returns `true` when the selected replica is pending adoption by the restore caller.
    pub(crate) fn register_data_file_for_restore(
        &self,
        file_id: FileId,
        path: &str,
        source_origin: ReplicaOrigin,
        placement: PrimaryDataPlacement,
        estimated_size_bytes: Option<u64>,
        resource_registry: Option<Arc<dyn RestoreCopyResourceRegistry + Send + Sync>>,
    ) -> Result<bool> {
        if self.select_primary_residual_replica(file_id, placement) {
            return Ok(true);
        }
        let (source_volume, source_relative_path) = self.resolve_volume_path(path)?;
        let mut source_reader = None;
        let expected_write_bytes = if let Some(size) = estimated_size_bytes {
            size
        } else {
            let reader = source_volume.fs().open_read(&source_relative_path)?;
            let size = reader.size() as u64;
            source_reader = Some(reader);
            size
        };
        let target_volume =
            self.select_primary_data_volume_for_placement(placement, Some(expected_write_bytes))?;
        let selected_target_rank = target_volume.priority.rank();
        if source_volume.supports_primary_data
            && source_volume.priority.rank() == selected_target_rank
        {
            self.register_restore_source_replica(file_id, path, source_origin)?;
            return Ok(false);
        }

        let source_reader = match source_reader {
            Some(reader) => reader,
            None => source_volume.fs().open_read(&source_relative_path)?,
        };
        if source_volume.snapshot_persistable {
            self.register_data_file_readonly_with_origin(file_id, path, source_origin)?;
        }
        let (mut writer, target_tracked) =
            self.create_transfer_data_file_writer_on_volume(target_volume, path)?;
        if let Err(err) = self.copy_reader_to_tracked_writer(source_reader.as_ref(), &mut writer) {
            if source_volume.snapshot_persistable {
                let _ = self.remove_data_file(file_id);
            }
            return Err(err);
        }
        if let Some(logical) = self.get_logical_file(file_id) {
            let source = logical.preferred_replica_any().ok_or_else(|| {
                Error::InvalidState(format!("Logical file {} has no replica", file_id))
            })?;
            if !logical.retain_and_select_replica_if(
                &source.tracked,
                logical.add_replica_with_origin(
                    Arc::clone(&target_tracked),
                    ReplicaLifecycle::OwnedReady,
                    ReplicaOrigin::Owned,
                ),
            ) {
                return Err(Error::InvalidState(format!(
                    "Logical file {} changed while restoring",
                    file_id
                )));
            }
            let replacement_id = logical
                .preferred_replica_any()
                .expect("newly selected replica is present")
                .replica_id;
            logical.select_durable_and_preferred(replacement_id);
        } else {
            self.register_logical_file(
                file_id,
                Arc::clone(&target_tracked),
                ReplicaLifecycle::OwnedReady,
                FileCommitState::Committed,
                ReplicaOrigin::Owned,
            );
        }

        if let Some(registry) = resource_registry {
            registry.register_temp_restored_copy(file_id);
        }
        Ok(false)
    }

    fn register_restore_source_replica(
        &self,
        file_id: FileId,
        path: &str,
        origin: ReplicaOrigin,
    ) -> Result<()> {
        if matches!(origin, ReplicaOrigin::Owned) {
            self.register_data_file(file_id, path)
        } else {
            self.register_data_file_readonly_with_origin(file_id, path, origin)
        }
    }

    /// Publishes a compactor-produced replica for transfer without making it locally owned.
    pub(crate) fn publish_data_file_transfer(&self, file_id: FileId) -> Result<()> {
        let logical = self.get_logical_file(file_id).ok_or_else(|| {
            Error::IoError(format!(
                "Logical file {} is not tracked by FileManager",
                file_id
            ))
        })?;
        logical
            .preferred_replica_any()
            .ok_or_else(|| Error::InvalidState(format!("Logical file {} has no replica", file_id)))?
            .tracked
            .set_physical_delete_policy(PhysicalDeletePolicy::Retained);
        logical.set_preferred_lifecycle(ReplicaLifecycle::PublishedTransfer);
        Ok(())
    }

    /// Adopts a previously published replica into the local owned lifecycle.
    pub(crate) fn adopt_data_file(&self, file_id: FileId) -> Result<()> {
        let logical = self.get_logical_file(file_id).ok_or_else(|| {
            Error::IoError(format!(
                "Logical file {} is not tracked by FileManager",
                file_id
            ))
        })?;
        logical
            .preferred_replica_any()
            .ok_or_else(|| Error::InvalidState(format!("Logical file {} has no replica", file_id)))?
            .tracked
            .set_physical_delete_policy(PhysicalDeletePolicy::ManagedDelete);
        logical.set_preferred_lifecycle(ReplicaLifecycle::OwnedReady);
        Ok(())
    }

    pub(crate) fn set_data_file_priority(&self, file_id: FileId, priority: u8) -> Result<()> {
        let logical = self.get_logical_file(file_id).ok_or_else(|| {
            Error::IoError(format!(
                "Logical file {} is not tracked by FileManager",
                file_id
            ))
        })?;
        logical
            .preferred_replica_any()
            .ok_or_else(|| Error::InvalidState(format!("Logical file {} has no replica", file_id)))?
            .tracked
            .set_priority(priority);
        Ok(())
    }

    /// Opens a data file for reading.
    ///
    /// Returns a TrackedReader that holds a reference to the TrackedFile.
    /// The file will not be deleted while the reader is in use.
    pub fn open_data_file_reader(&self, file_id: FileId) -> Result<TrackedReader> {
        let replica = self.preferred_readable_replica(file_id)?;
        let key = ReplicaKey {
            file_id,
            replica_id: replica.replica_id,
        };
        let cached = {
            let mut cache = self
                .reader_cache
                .lock()
                .map_err(|_| Error::IoError("Reader cache lock poisoned".to_string()))?;
            cache.get(&key).map(Arc::clone)
        };
        let reader = if let Some(reader) = cached {
            reader
        } else {
            let reader = replica.tracked.fs().open_read(replica.tracked.path())?;
            let reader: Arc<dyn RandomAccessFile> = Arc::from(reader);
            let mut cache = self
                .reader_cache
                .lock()
                .map_err(|_| Error::IoError("Reader cache lock poisoned".to_string()))?;
            cache.insert(key, Arc::clone(&reader));
            reader
        };
        let reader: Arc<dyn RandomAccessFile> = Arc::new(CachedRandomAccessFile::new(reader));
        Ok(TrackedReader::new(reader, Arc::clone(&replica.tracked)))
    }

    /// Returns the tracked data file reference.
    pub(crate) fn data_file_ref(&self, file_id: FileId) -> Result<Arc<TrackedFile>> {
        let replica = self.preferred_readable_replica(file_id)?;
        replica.tracked.reference();
        Ok(Arc::clone(&replica.tracked))
    }

    pub(crate) fn data_file_ref_at_path(
        &self,
        file_id: FileId,
        absolute_path: &str,
        lifecycle: ReplicaLifecycle,
    ) -> Result<Arc<TrackedFile>> {
        self.data_file_ref_at_path_with_origin(
            file_id,
            absolute_path,
            lifecycle,
            ReplicaOrigin::Owned,
        )
    }

    pub(crate) fn data_file_ref_at_path_with_origin(
        &self,
        file_id: FileId,
        absolute_path: &str,
        lifecycle: ReplicaLifecycle,
        origin: ReplicaOrigin,
    ) -> Result<Arc<TrackedFile>> {
        let logical = self
            .get_logical_file(file_id)
            .ok_or_else(|| Error::IoError(format!("Logical file {} is not tracked", file_id)))?;
        if let Some(replica) = logical.replica_at_absolute_path(absolute_path) {
            if replica.origin() != origin {
                return Err(Error::InvalidState(format!(
                    "Logical file {file_id} replica at {absolute_path} has a different origin"
                )));
            }
            replica.tracked.reference();
            if lifecycle == ReplicaLifecycle::OwnedReady && matches!(origin, ReplicaOrigin::Owned) {
                replica
                    .tracked
                    .set_physical_delete_policy(PhysicalDeletePolicy::ManagedDelete);
                logical.set_replica_lifecycle(replica.replica_id, lifecycle);
            }
            return Ok(Arc::clone(&replica.tracked));
        }
        let (volume, relative_path) = self.resolve_volume_path(absolute_path)?;
        if !volume.fs().exists(&relative_path)? {
            return Err(Error::IoError(format!(
                "Manifest file is missing: {}",
                absolute_path
            )));
        }
        let tracked = Arc::new(
            if lifecycle == ReplicaLifecycle::OwnedReady && matches!(origin, ReplicaOrigin::Owned) {
                TrackedFile::managed(
                    relative_path,
                    Arc::clone(volume.fs()),
                    Some(Arc::clone(&volume)),
                )
            } else if matches!(origin, ReplicaOrigin::Owned) {
                TrackedFile::retained(
                    relative_path,
                    Arc::clone(volume.fs()),
                    Some(Arc::clone(&volume)),
                )
            } else {
                TrackedFile::external_view(
                    relative_path,
                    Arc::clone(volume.fs()),
                    Some(Arc::clone(&volume)),
                )
            },
        );
        tracked.update_size_bytes(volume.fs().open_read(tracked.path())?.size() as u64);
        logical.add_replica_with_origin(Arc::clone(&tracked), lifecycle, origin);
        tracked.reference();
        Ok(tracked)
    }

    pub(crate) fn register_external_persistent_replica(
        &self,
        file_id: FileId,
        path: &str,
        source_id: String,
    ) -> Result<Arc<TrackedFile>> {
        self.register_external_replica(
            file_id,
            path,
            ReplicaOrigin::ExternalPersistent { source_id },
        )
    }

    pub(crate) fn register_external_leased_replica(
        &self,
        file_id: FileId,
        path: &str,
        export_id: String,
    ) -> Result<Arc<TrackedFile>> {
        self.register_external_replica(file_id, path, ReplicaOrigin::ExternalLeased { export_id })
    }

    fn register_external_replica(
        &self,
        file_id: FileId,
        path: &str,
        origin: ReplicaOrigin,
    ) -> Result<Arc<TrackedFile>> {
        if !self.has_data_file(file_id) {
            self.register_existing_data_file(
                file_id,
                path,
                ReplicaLifecycle::ExternalReference,
                FileCommitState::Committed,
                PhysicalDeletePolicy::Retained,
                origin,
            )?;
            return self
                .preferred_tracked_file(file_id)
                .ok_or_else(|| Error::IoError(format!("Logical file {file_id} is not tracked")));
        }
        let logical = self
            .get_logical_file(file_id)
            .expect("logical file must exist after has_data_file");
        if let Some(replica) = logical.replica_at_absolute_path(path)
            && replica.origin() == origin
        {
            return Ok(Arc::clone(&replica.tracked));
        }
        let (volume, relative_path) = self.resolve_volume_path(path)?;
        if !volume.fs().exists(&relative_path)? {
            return Err(Error::IoError(format!("External file is missing: {path}")));
        }
        let tracked = Arc::new(TrackedFile::external_view(
            relative_path,
            Arc::clone(volume.fs()),
            Some(Arc::clone(&volume)),
        ));
        tracked.update_size_bytes(volume.fs().open_read(tracked.path())?.size() as u64);
        logical.add_replica_with_origin(
            Arc::clone(&tracked),
            ReplicaLifecycle::ExternalReference,
            origin,
        );
        Ok(tracked)
    }

    pub(crate) fn preferred_replica_origin(&self, file_id: FileId) -> Option<ReplicaOrigin> {
        self.get_logical_file(file_id)
            .and_then(|logical| logical.preferred_replica_any())
            .map(|replica| replica.origin())
    }

    pub(crate) fn data_file_ref_with_origin(
        &self,
        file_id: FileId,
    ) -> Result<(Arc<TrackedFile>, ReplicaOrigin)> {
        let replica = self
            .get_logical_file(file_id)
            .and_then(|logical| logical.preferred_replica_any())
            .ok_or_else(|| Error::IoError(format!("Data file {file_id} is not tracked")))?;
        replica.tracked.reference();
        Ok((Arc::clone(&replica.tracked), replica.origin()))
    }

    pub(crate) fn durable_data_file_ref_with_origin(
        &self,
        file_id: FileId,
    ) -> Result<(Arc<TrackedFile>, ReplicaOrigin)> {
        let logical = self
            .get_logical_file(file_id)
            .ok_or_else(|| Error::IoError(format!("Data file {file_id} is not tracked")))?;
        let replica = logical.durable_replica().ok_or_else(|| {
            Error::InvalidState(format!("Logical file {file_id} has no durable replica"))
        })?;
        replica.tracked.reference();
        Ok((Arc::clone(&replica.tracked), replica.origin()))
    }

    pub(crate) fn durable_data_file_path_with_origin(
        &self,
        file_id: FileId,
    ) -> Option<(String, ReplicaOrigin)> {
        self.get_logical_file(file_id)
            .and_then(|logical| logical.durable_replica())
            .map(|replica| (replica.tracked.absolute_path(), replica.origin()))
    }

    fn preferred_readable_replica(
        &self,
        file_id: FileId,
    ) -> Result<Arc<crate::file::logical_file::PhysicalReplica>> {
        let logical = self.get_logical_file(file_id).ok_or_else(|| {
            Error::IoError(format!(
                "Logical file {} is not tracked by FileManager",
                file_id
            ))
        })?;
        logical.preferred_replica().ok_or_else(|| {
            Error::InvalidState(format!(
                "Logical file {} has no readable preferred replica",
                file_id
            ))
        })
    }

    pub(crate) fn preferred_tracked_file(&self, file_id: FileId) -> Option<Arc<TrackedFile>> {
        self.get_logical_file(file_id)
            .and_then(|logical| logical.preferred_replica_any())
            .map(|replica| Arc::clone(&replica.tracked))
    }

    pub(crate) fn has_snapshot_volume(&self) -> bool {
        self.data_volumes
            .iter()
            .any(|volume| volume.snapshot_persistable)
    }

    pub(crate) fn is_data_file_on_primary_volume(&self, file_id: FileId) -> bool {
        let Ok(replica) = self.preferred_readable_replica(file_id) else {
            return false;
        };
        let Some(volume) = &replica.tracked.volume else {
            return false;
        };
        volume.supports_primary_data
    }

    pub(crate) fn is_data_file_on_snapshot_volume(&self, file_id: FileId) -> bool {
        let Some(logical) = self.get_logical_file(file_id) else {
            return false;
        };
        self.data_volumes
            .iter()
            .filter(|volume| volume.snapshot_persistable)
            .any(|volume| logical.replica_on_volume(volume).is_some())
    }

    fn select_snapshot_persistable_volume(&self) -> Result<&Arc<DataVolume>> {
        let base_file_size = self.options.base_file_size as u64;
        let snapshot_only_non_full = self
            .data_volumes
            .iter()
            .filter(|volume| volume.snapshot_persistable && !volume.supports_primary_data)
            .find(|volume| !volume.is_full(base_file_size));
        if let Some(volume) = snapshot_only_non_full {
            return Ok(volume);
        }
        let snapshot_only_any = self
            .data_volumes
            .iter()
            .find(|volume| volume.snapshot_persistable && !volume.supports_primary_data);
        if let Some(volume) = snapshot_only_any {
            return Ok(volume);
        }
        let shared_non_full = self
            .data_volumes
            .iter()
            .filter(|volume| volume.snapshot_persistable && volume.supports_primary_data)
            .find(|volume| !volume.is_full(base_file_size));
        if let Some(volume) = shared_non_full {
            return Ok(volume);
        }
        self.data_volumes
            .iter()
            .find(|volume| volume.snapshot_persistable && volume.supports_primary_data)
            .ok_or_else(|| {
                Error::ConfigError("Snapshot persistence volume is not configured".to_string())
            })
    }

    /// Returns an existing snapshot-persistable replica or adds a copied physical replica under
    /// the same logical file id. The returned tracked file is the exact replica a snapshot pins.
    pub(crate) fn snapshot_replica_for_tracked_file(
        &self,
        source_file_id: FileId,
        source_tracked: &Arc<TrackedFile>,
        logical: Option<&Arc<LogicalFile>>,
        resource_registry: Option<Arc<dyn SnapshotCopyResourceRegistry + Send + Sync>>,
        lifecycle_state: Option<&AtomicU8>,
    ) -> Result<Arc<TrackedFile>> {
        self.snapshot_replica_with_origin(
            source_file_id,
            source_tracked,
            ReplicaOrigin::Owned,
            logical,
            resource_registry,
            lifecycle_state,
        )
        .map(|(tracked, _)| tracked)
    }

    pub(crate) fn snapshot_replica_with_origin(
        &self,
        source_file_id: FileId,
        source_tracked: &Arc<TrackedFile>,
        source_origin: ReplicaOrigin,
        logical: Option<&Arc<LogicalFile>>,
        resource_registry: Option<Arc<dyn SnapshotCopyResourceRegistry + Send + Sync>>,
        lifecycle_state: Option<&AtomicU8>,
    ) -> Result<(Arc<TrackedFile>, ReplicaOrigin)> {
        if !matches!(source_origin, ReplicaOrigin::Owned) {
            return Ok((Arc::clone(source_tracked), source_origin));
        }
        if source_tracked.is_snapshot_persistable() {
            return Ok((Arc::clone(source_tracked), ReplicaOrigin::Owned));
        }
        let logical = logical.ok_or_else(|| {
            Error::InvalidState(format!(
                "Snapshot did not retain logical file {}",
                source_file_id
            ))
        })?;
        if let Some(replica) = self
            .data_volumes
            .iter()
            .filter(|volume| volume.snapshot_persistable)
            .find_map(|volume| logical.replica_on_volume(volume))
        {
            return Ok((Arc::clone(&replica.tracked), replica.origin()));
        }
        let snapshot_volume = self.select_snapshot_persistable_volume()?;
        let source_reader = source_tracked.fs().open_read(source_tracked.path())?;
        let source_priority = source_tracked.priority();
        let (mut writer, target) =
            self.create_snapshot_replica_writer_on_volume(snapshot_volume, source_tracked)?;

        let copy_result = self.copy_reader_to_tracked_writer_with_cancel(
            source_reader.as_ref(),
            &mut writer,
            lifecycle_state,
        );

        copy_result?;
        target.set_priority(source_priority);
        let replica_id = logical.add_replica(Arc::clone(&target), ReplicaLifecycle::Staging);
        if let Some(registry) = resource_registry {
            registry.register_temp_copied_replica(Arc::clone(logical), replica_id);
        }
        Ok((target, ReplicaOrigin::Owned))
    }

    /// Returns the path for a data file.
    pub fn get_data_file_path(&self, file_id: FileId) -> Option<String> {
        self.get_logical_file(file_id)
            .and_then(|logical| logical.preferred_replica_any())
            .map(|replica| replica.tracked.path().to_string())
    }

    /// Returns the full path for a data file, including the volume base directory if known.
    pub fn get_data_file_full_path(&self, file_id: FileId) -> Option<String> {
        self.get_logical_file(file_id)
            .and_then(|logical| logical.preferred_replica_any())
            .map(|replica| replica.tracked.absolute_path())
    }

    /// Finds a tracked data file by its absolute (volume-prefixed) path.
    ///
    /// This is used by the dedicated compaction apply path to map a compactor output path
    /// (e.g. `file://.../compaction/jobs/<id>/data/<uuid>.sst`) back to the canonical file id
    /// the writer assigned when registering the output.
    pub(crate) fn find_data_file_by_absolute_path(&self, absolute_path: &str) -> Option<FileId> {
        self.logical_files
            .iter()
            .find(|entry| {
                entry
                    .preferred_replica_any()
                    .is_some_and(|replica| replica.tracked.absolute_path() == absolute_path)
            })
            .map(|entry| *entry.key())
    }

    /// Returns the path for a metadata file.
    pub fn get_metadata_file_path(&self, name: &str) -> Option<String> {
        self.metadata_files.get(name).map(|f| f.path().to_string())
    }

    /// Returns the full path for a metadata file, including the volume base directory if known.
    pub fn get_metadata_file_full_path(&self, name: &str) -> Option<String> {
        self.metadata_files.get(name).map(|f| f.absolute_path())
    }

    /// Returns the expected path for a metadata file, even if not tracked yet.
    pub fn metadata_path(&self, name: &str) -> String {
        self.metadata_file_path(name)
    }

    /// Checks if a data file is tracked by the FileManager.
    pub fn has_data_file(&self, file_id: FileId) -> bool {
        self.logical_files.contains_key(&file_id)
    }

    pub(crate) fn select_new_replica_retaining_source_if(
        &self,
        file_id: FileId,
        expected: &Arc<TrackedFile>,
        replacement: Arc<TrackedFile>,
    ) -> Option<ReplicaId> {
        let logical = self.get_logical_file(file_id)?;
        let source_id = logical.add_and_select_replica_if(
            expected,
            replacement,
            ReplicaLifecycle::OwnedReady,
        )?;
        let replacement_id = logical.preferred_replica_any()?.replica_id;
        logical
            .select_durable_and_preferred(replacement_id)
            .then_some(source_id)
    }

    pub(crate) fn retire_replica(&self, file_id: FileId, replica_id: ReplicaId) {
        self.remove_replica(file_id, replica_id);
    }

    pub(crate) fn select_existing_replica_retaining_source_if(
        &self,
        file_id: FileId,
        expected: &Arc<TrackedFile>,
        replica_id: ReplicaId,
    ) -> Option<ReplicaId> {
        let logical = self.get_logical_file(file_id)?;
        let source_id = logical.preferred_replica_any()?.replica_id;
        if !logical.retain_and_select_replica_if(expected, replica_id) {
            return None;
        }
        logical
            .select_durable_and_preferred(replica_id)
            .then_some(source_id)
    }

    pub(crate) fn request_referenced_persistent_caches(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
    ) -> Result<()> {
        for file_id in file_ids {
            let Some(logical) = self.get_logical_file(file_id) else {
                continue;
            };
            if matches!(
                logical.durable_replica().map(|replica| replica.origin()),
                Some(ReplicaOrigin::ExternalPersistent { .. })
            ) {
                logical.set_persistent_cache_requested(true);
            }
        }
        Ok(())
    }

    pub(crate) fn evict_preferred_persistent_cache(&self, file_id: FileId) -> Result<bool> {
        let Some(logical) = self.get_logical_file(file_id) else {
            return Ok(false);
        };
        let state = logical.replica_state_snapshot();
        let (Some(preferred_id), Some(durable_id)) =
            (state.preferred_replica_id, state.durable_replica_id)
        else {
            return Ok(false);
        };
        if preferred_id == durable_id
            || !logical.persistent_cache_requested()
            || !matches!(
                logical
                    .preferred_replica_any()
                    .map(|replica| replica.origin()),
                Some(ReplicaOrigin::Owned)
            )
            || !matches!(
                logical.durable_replica().map(|replica| replica.origin()),
                Some(ReplicaOrigin::ExternalPersistent { .. })
            )
        {
            return Ok(false);
        }
        if !logical.select_preferred_read_replica(durable_id) {
            return Ok(false);
        }
        self.remove_replica(file_id, preferred_id);
        Ok(true)
    }

    pub(crate) fn remove_replica(&self, file_id: FileId, replica_id: ReplicaId) {
        let Some(logical) = self.get_logical_file(file_id) else {
            return;
        };
        if logical.remove_replica(replica_id).is_some()
            && let Ok(mut cache) = self.reader_cache.lock()
        {
            cache.remove(&ReplicaKey {
                file_id,
                replica_id,
            });
        }
    }

    /// Removes a data file from tracking.
    pub(crate) fn remove_data_file(&self, file_id: FileId) -> Result<()> {
        let Some((_, logical)) = self.logical_files.remove(&file_id) else {
            return Ok(());
        };
        if let Ok(mut cache) = self.reader_cache.lock() {
            for replica_id in logical.replica_ids() {
                cache.remove(&ReplicaKey {
                    file_id,
                    replica_id,
                });
            }
        }
        self.report_data_files_gauge();
        Ok(())
    }

    /// Returns the number of tracked data files.
    pub fn data_file_count(&self) -> usize {
        self.logical_files.len()
    }

    /// Returns all tracked data file IDs.
    pub fn data_file_ids(&self) -> Vec<FileId> {
        self.logical_files
            .iter()
            .map(|entry| *entry.key())
            .collect()
    }

    // =========================================================================
    // Metadata file operations
    // =========================================================================

    /// Creates a new metadata file for writing.
    ///
    /// Metadata files are identified by their name rather than a numeric ID.
    pub fn create_metadata_file(&self, name: &str) -> Result<AtomicMetadataWriter> {
        let final_path = self.metadata_file_path(name);
        let temp_path = format!("{}.tmp-{}", final_path, Uuid::new_v4());
        let writer = self.meta_volume.fs().open_write(&temp_path)?;

        let tracked = Arc::new(TrackedFile::managed(
            temp_path.clone(),
            Arc::clone(self.meta_volume.fs()),
            Some(Arc::clone(&self.meta_volume)),
        ));
        let tracked_writer = TrackedWriter::new(writer, tracked);
        Ok(AtomicMetadataWriter::new(
            temp_path,
            name.to_string(),
            final_path,
            tracked_writer,
            Arc::clone(self.meta_volume.fs()),
            Arc::clone(&self.metadata_files),
            self.metrics.metadata_files_tracked.clone(),
            Some(Arc::clone(&self.meta_volume)),
        ))
    }

    /// Atomically writes a plain, untracked metadata file without the checksum envelope used by
    /// manifests. Use this for human-readable metadata formats such as TOML.
    pub(crate) fn write_plain_metadata_file_atomic(
        &self,
        name: &str,
        content: &[u8],
    ) -> Result<()> {
        let final_path = self.metadata_file_path(name);
        let temp_path = format!("{}.tmp-{}", final_path, Uuid::new_v4());
        let mut writer = self.meta_volume.fs().open_write(&temp_path)?;
        let write_result = (|| {
            let mut written = 0;
            while written < content.len() {
                let count = writer.write(&content[written..])?;
                if count == 0 {
                    return Err(Error::IoError(format!(
                        "write returned zero bytes for metadata file {name}"
                    )));
                }
                written += count;
            }
            writer.close()?;
            self.meta_volume.fs().rename(&temp_path, &final_path)
        })();
        drop(writer);
        if write_result.is_err() {
            let _ = self.meta_volume.fs().delete(&temp_path);
        }
        write_result
    }

    /// Registers an existing metadata file with the FileManager.
    pub fn register_metadata_file(&self, name: &str, path: &str) -> Result<()> {
        // Verify the file exists
        if !self.meta_volume.fs().exists(path)? {
            return Err(Error::IoError(format!(
                "Metadata file {} does not exist at path: {}",
                name, path
            )));
        }

        // Track the file if not already tracked
        if !self.metadata_files.contains_key(name) {
            let tracked = Arc::new(TrackedFile::retained(
                path.to_string(),
                Arc::clone(self.meta_volume.fs()),
                Some(Arc::clone(&self.meta_volume)),
            ));
            self.metadata_files.insert(name.to_string(), tracked);
            self.report_metadata_files_gauge();
        }

        Ok(())
    }

    /// Opens a metadata file for reading.
    ///
    /// Returns a TrackedReader that holds a reference to the TrackedFile.
    pub fn open_metadata_file_reader(&self, name: &str) -> Result<TrackedReader> {
        // Get the tracked file to read the path
        let tracked = self.metadata_files.get(name).ok_or_else(|| {
            Error::IoError(format!(
                "Metadata file {} is not tracked by FileManager",
                name
            ))
        })?;

        let reader = self.meta_volume.fs().open_read(tracked.path())?;
        Ok(TrackedReader::new(reader.into(), Arc::clone(&tracked)))
    }

    /// Opens a metadata file for reading without tracking it.
    pub fn open_metadata_file_reader_untracked(
        &self,
        name: &str,
    ) -> Result<Box<dyn RandomAccessFile>> {
        let path = self.metadata_file_path(name);
        self.meta_volume.fs().open_read(&path)
    }

    pub(crate) fn open_metadata_file_reader_at_path(
        &self,
        path: &str,
    ) -> Result<Box<dyn RandomAccessFile>> {
        let (volume, relative_path) = self.resolve_volume_path(path)?;
        volume.fs().open_read(&relative_path)
    }

    pub(crate) fn data_file_size_at_path(&self, path: &str) -> Result<u64> {
        let (volume, relative_path) = self.resolve_volume_path(path)?;
        Ok(volume.fs().open_read(&relative_path)?.size() as u64)
    }

    /// Registers an existing data file without deleting it on drop.
    pub fn register_data_file_readonly(&self, file_id: FileId, path: &str) -> Result<()> {
        self.register_data_file_readonly_with_origin(
            file_id,
            path,
            ReplicaOrigin::ExternalPersistent {
                source_id: path.to_string(),
            },
        )
    }

    pub(crate) fn register_data_file_readonly_with_origin(
        &self,
        file_id: FileId,
        path: &str,
        origin: ReplicaOrigin,
    ) -> Result<()> {
        self.register_existing_data_file(
            file_id,
            path,
            ReplicaLifecycle::ReadonlyView,
            FileCommitState::Committed,
            PhysicalDeletePolicy::Retained,
            origin,
        )
    }

    /// Registers a compactor-produced file that is readable but cannot be deleted until the
    /// writer has committed its manifest entry.
    pub(crate) fn register_data_file_pending_adoption(
        &self,
        file_id: FileId,
        path: &str,
    ) -> Result<()> {
        self.register_existing_data_file(
            file_id,
            path,
            ReplicaLifecycle::PendingAdoption,
            FileCommitState::Uncommitted,
            PhysicalDeletePolicy::Retained,
            ReplicaOrigin::Owned,
        )
    }

    /// Registers one scanned primary residual as a non-owning replica until resume succeeds.
    pub(crate) fn register_primary_residual_replica(
        &self,
        file_id: FileId,
        path: &str,
        size_bytes: u64,
    ) -> Result<()> {
        let (volume, relative_path) = self.resolve_volume_path(path)?;
        if let Some(logical) = self.get_logical_file(file_id) {
            if logical.replica_at_absolute_path(path).is_some() {
                return Ok(());
            }
            let tracked = Arc::new(TrackedFile::retained(
                relative_path,
                Arc::clone(volume.fs()),
                Some(volume),
            ));
            tracked.update_size_bytes(size_bytes);
            logical.add_replica(tracked, ReplicaLifecycle::PendingAdoption);
            return Ok(());
        }

        let tracked = Arc::new(TrackedFile::retained(
            relative_path,
            Arc::clone(volume.fs()),
            Some(volume),
        ));
        tracked.update_size_bytes(size_bytes);
        self.register_logical_file(
            file_id,
            tracked,
            ReplicaLifecycle::PendingAdoption,
            FileCommitState::Uncommitted,
            ReplicaOrigin::Owned,
        );
        self.report_data_files_gauge();
        Ok(())
    }

    fn select_primary_residual_replica(
        &self,
        file_id: FileId,
        placement: PrimaryDataPlacement,
    ) -> bool {
        let Some(logical) = self.get_logical_file(file_id) else {
            return false;
        };
        let mut candidates = logical
            .replica_state_snapshot()
            .replicas
            .into_iter()
            .filter(|replica| {
                replica.lifecycle() == ReplicaLifecycle::PendingAdoption
                    && replica
                        .tracked
                        .volume
                        .as_ref()
                        .is_some_and(|volume| volume.supports_primary_data)
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|replica| {
            replica
                .tracked
                .volume
                .as_ref()
                .map(|volume| volume.priority.rank())
                .unwrap_or(0)
        });
        let selected = if self.uses_lowest_primary_tier(placement) {
            candidates.first()
        } else {
            candidates.last()
        };
        selected.is_some_and(|replica| logical.select_durable_and_preferred(replica.replica_id))
    }

    /// Completes ownership transfer for every scanned replica of one restored logical file.
    pub(crate) fn adopt_primary_residual_replicas(&self, file_id: FileId) {
        let Some(logical) = self.get_logical_file(file_id) else {
            return;
        };
        for replica in logical.replica_state_snapshot().replicas {
            if replica.lifecycle() != ReplicaLifecycle::PendingAdoption {
                continue;
            }
            replica
                .tracked
                .set_physical_delete_policy(PhysicalDeletePolicy::ManagedDelete);
            logical.set_replica_lifecycle(replica.replica_id, ReplicaLifecycle::OwnedReady);
        }
    }

    fn register_existing_data_file(
        &self,
        file_id: FileId,
        path: &str,
        lifecycle: ReplicaLifecycle,
        commit_state: FileCommitState,
        physical_delete_policy: PhysicalDeletePolicy,
        origin: ReplicaOrigin,
    ) -> Result<()> {
        if self.logical_files.contains_key(&file_id) {
            return Ok(());
        }
        let (volume, relative_path) = self.resolve_volume_path(path)?;
        let fs = Arc::clone(volume.fs());
        let tracked = Arc::new(match (physical_delete_policy, &origin) {
            (
                _,
                ReplicaOrigin::ExternalLeased { .. } | ReplicaOrigin::ExternalPersistent { .. },
            ) => TrackedFile::external_view(
                relative_path,
                Arc::clone(&fs),
                Some(Arc::clone(&volume)),
            ),
            (PhysicalDeletePolicy::ManagedDelete, _) => {
                TrackedFile::managed(relative_path, Arc::clone(&fs), Some(Arc::clone(&volume)))
            }
            (PhysicalDeletePolicy::Retained, _) => {
                TrackedFile::retained(relative_path, Arc::clone(&fs), Some(Arc::clone(&volume)))
            }
        });
        let size = fs
            .open_read(tracked.path())
            .map(|reader| reader.size())
            .unwrap_or(0);
        tracked.update_size_bytes(size as u64);
        self.register_logical_file(file_id, tracked, lifecycle, commit_state, origin);
        self.report_data_files_gauge();
        Ok(())
    }

    /// Checks if a metadata file exists.
    pub fn has_metadata_file(&self, name: &str) -> bool {
        self.metadata_files.contains_key(name)
    }

    /// Marks a metadata file for deletion. The file will be deleted when all
    /// references to it (readers/writers) are dropped.
    pub fn mark_metadata_file_for_deletion(&self, name: &str, remove_from_tracking: bool) {
        if let Some(tracked) = self.metadata_files.get(name) {
            tracked.set_physical_delete_policy(PhysicalDeletePolicy::ManagedDelete);
        }
        if remove_from_tracking {
            self.metadata_files.remove(name);
            self.report_metadata_files_gauge();
        }
    }

    /// Removes a metadata file from tracking and optionally deletes it from disk.
    pub fn remove_metadata_file(&self, name: &str) -> Result<()> {
        if let Some((_, tracked)) = self.metadata_files.remove(name) {
            if Arc::strong_count(&tracked) > 1 {
                tracked.set_physical_delete_policy(PhysicalDeletePolicy::ManagedDelete);
            } else {
                self.meta_volume.fs().delete(tracked.path())?;
            }
            self.report_metadata_files_gauge();
            return Ok(());
        }
        let path = self.metadata_file_path(name);
        if self.meta_volume.fs().exists(&path)? {
            self.meta_volume.fs().delete(&path)?;
        }
        Ok(())
    }

    fn report_data_files_gauge(&self) {
        self.metrics
            .data_files_tracked
            .set(self.logical_files.len() as f64);
    }

    fn report_metadata_files_gauge(&self) {
        self.metrics
            .metadata_files_tracked
            .set(self.metadata_files.len() as f64);
    }

    pub(crate) fn record_offload_scheduled(&self) {
        self.metrics.offload_jobs_scheduled_total.increment(1);
    }

    pub(crate) fn record_offload_completed_copy(&self, bytes: u64) {
        self.metrics.offload_jobs_completed_total.increment(1);
        self.metrics.offload_bytes_moved_total.increment(bytes);
    }

    pub(crate) fn record_offload_completed_promotion(&self) {
        self.metrics.offload_jobs_completed_total.increment(1);
        self.metrics.offload_promotions_total.increment(1);
    }

    pub(crate) fn record_offload_noop(&self) {
        self.metrics.offload_jobs_noop_total.increment(1);
    }

    pub(crate) fn record_offload_failed(&self) {
        self.metrics.offload_jobs_failed_total.increment(1);
    }

    /// Returns the number of tracked metadata files.
    pub fn metadata_file_count(&self) -> usize {
        self.metadata_files.len()
    }

    /// Returns all tracked metadata file names.
    pub fn metadata_file_names(&self) -> Vec<String> {
        self.metadata_files
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Lists metadata file names under this DB's snapshot directory.
    pub fn list_snapshot_metadata_names(&self) -> Result<Vec<String>> {
        let snapshot_dir = if self.options.base_dir.is_empty() {
            SNAPSHOT_DIR.to_string()
        } else {
            format!("{}/{}", self.options.base_dir, SNAPSHOT_DIR)
        };
        self.meta_volume.fs().list(&snapshot_dir)
    }

    /// Lists metadata file names under an arbitrary directory relative to the DB base dir.
    ///
    /// Unlike `list_snapshot_metadata_names` this scans any subdirectory and bypasses the
    /// in-memory `metadata_files` index, so it works across processes (e.g. a writer
    /// discovering result files published by a separate compactor process).
    pub fn list_metadata_names(&self, relative_dir: &str) -> Result<Vec<String>> {
        let dir = if self.options.base_dir.is_empty() {
            relative_dir.to_string()
        } else {
            format!("{}/{}", self.options.base_dir, relative_dir)
        };
        // Ensure the directory exists; list on a missing dir is treated as empty.
        match self.meta_volume.fs().list(&dir) {
            Ok(names) => Ok(names),
            Err(Error::IoError(_)) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    /// Lists file names under a directory relative to the DB base dir, using **data volumes**
    /// rather than the metadata volume. This is necessary for cleaning up compaction output
    /// files that were written to data volumes (which may differ from the metadata volume in
    /// multi-volume setups).
    ///
    /// Returns the union of files found across all data volumes.
    pub(crate) fn list_data_volume_names(&self, relative_dir: &str) -> Result<Vec<String>> {
        let dir = if self.options.base_dir.is_empty() {
            relative_dir.to_string()
        } else {
            format!("{}/{}", self.options.base_dir, relative_dir)
        };
        let mut all_names = Vec::new();
        for volume in &self.data_volumes {
            match volume.fs().list(&dir) {
                Ok(names) => all_names.extend(names),
                Err(Error::IoError(_)) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(all_names)
    }

    /// Resolves a relative path (relative to the DB base dir) to the set of absolute
    /// (volume-prefixed) paths across all data volumes. Used by the orphan sweep to compare
    /// against manifest paths (which are stored as absolute volume-prefixed paths produced by
    /// `TrackedFile::absolute_path`, i.e. `{volume_base_dir}/{options.base_dir}/{relative_path}`).
    pub(crate) fn data_volume_absolute_paths(&self, relative_path: &str) -> Vec<String> {
        // The manifest path is `{volume_base_dir}/{options.base_dir}/{relative_path}` because
        // `TrackedFile.path` already includes `options.base_dir` (the db_id) as a prefix, and
        // `absolute_path` prepends the volume base_dir. We must replicate that structure here.
        let full_relative = if self.options.base_dir.is_empty() {
            relative_path.to_string()
        } else {
            format!("{}/{}", self.options.base_dir, relative_path)
        };
        self.data_volumes
            .iter()
            .filter_map(|volume| {
                volume
                    .base_dir()
                    .map(|bd| format!("{}/{}", bd, full_relative))
            })
            .collect()
    }

    /// Deletes a file by its path relative to the DB base dir from **all data volumes**.
    /// Used to clean up compaction output files that may live on any data volume.
    pub(crate) fn remove_data_volume_path(&self, relative_path: &str) -> Result<()> {
        let path = if self.options.base_dir.is_empty() {
            relative_path.to_string()
        } else {
            format!("{}/{}", self.options.base_dir, relative_path)
        };
        for volume in &self.data_volumes {
            if volume.fs().exists(&path)? {
                volume.fs().delete(&path)?;
            }
        }
        Ok(())
    }

    /// Removes one data file addressed by a volume-qualified path.
    pub(crate) fn remove_data_file_at_path(&self, path: &str) -> Result<()> {
        let (volume, relative_path) = self.resolve_volume_path(path)?;
        if volume.fs().exists(&relative_path)? {
            volume.fs().delete(&relative_path)?;
        }
        Ok(())
    }

    /// Checks if a file exists on any data volume by its path relative to the DB base dir.
    pub(crate) fn data_volume_path_exists(&self, relative_path: &str) -> Result<bool> {
        let path = if self.options.base_dir.is_empty() {
            relative_path.to_string()
        } else {
            format!("{}/{}", self.options.base_dir, relative_path)
        };
        for volume in &self.data_volumes {
            if volume.fs().exists(&path)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Returns the last-modified time (unix seconds) of a path on any data volume.
    pub(crate) fn data_volume_last_modified(&self, relative_path: &str) -> Result<Option<u64>> {
        let path = if self.options.base_dir.is_empty() {
            relative_path.to_string()
        } else {
            format!("{}/{}", self.options.base_dir, relative_path)
        };
        for volume in &self.data_volumes {
            if let Some(ts) = volume.fs().last_modified(&path)? {
                return Ok(Some(ts));
            }
        }
        Ok(None)
    }

    /// Returns true if a metadata file exists on disk, bypassing the in-memory index.
    pub fn metadata_file_exists_untracked(&self, name: &str) -> Result<bool> {
        let path = self.metadata_file_path(name);
        self.meta_volume.fs().exists(&path)
    }

    /// Returns the last-modified time (unix millis) of a metadata file on disk, bypassing
    /// the in-memory index. Returns `Ok(None)` if the file does not exist.
    pub(crate) fn last_modified_untracked(&self, name: &str) -> Result<Option<u64>> {
        let path = self.metadata_file_path(name);
        self.meta_volume.fs().last_modified(&path)
    }

    /// Ensures a directory (relative to the DB base dir) exists on the metadata volume,
    /// creating it and any missing parents if needed.
    pub(crate) fn ensure_metadata_dir(&self, relative_dir: &str) -> Result<()> {
        let dir = if self.options.base_dir.is_empty() {
            relative_dir.to_string()
        } else {
            format!("{}/{}", self.options.base_dir, relative_dir)
        };
        // Create parent directories first.
        let mut current = String::new();
        for part in dir.split('/') {
            if current.is_empty() {
                current = part.to_string();
            } else {
                current = format!("{}/{}", current, part);
            }
            if !self.meta_volume.fs().exists(&current)? {
                self.meta_volume.fs().create_dir(&current)?;
            }
        }
        Ok(())
    }

    /// Writes a small untracked file to the **metadata volume** at a path relative to the DB
    /// base dir, creating parent directories as needed.
    ///
    /// This is used for compaction job lease files. The metadata volume is a single,
    /// deterministic volume (unlike data volumes which may be multiple and randomly selected),
    /// so both the writer and compactor always agree on where the lease lives. This avoids
    /// heartbeat files jumping between volumes and the writer's sweep missing a fresh lease.
    pub(crate) fn write_metadata_volume_file(
        &self,
        relative_path: &str,
        content: &[u8],
    ) -> Result<()> {
        let path = if self.options.base_dir.is_empty() {
            relative_path.to_string()
        } else {
            format!("{}/{}", self.options.base_dir, relative_path)
        };
        // Create parent directories.
        if let Some(parent) = std::path::Path::new(&path).parent() {
            let parent_str = parent.to_string_lossy().to_string();
            if !parent_str.is_empty() {
                let mut current = String::new();
                for part in parent_str.split('/') {
                    if current.is_empty() {
                        current = part.to_string();
                    } else {
                        current = format!("{}/{}", current, part);
                    }
                    if !self.meta_volume.fs().exists(&current)? {
                        self.meta_volume.fs().create_dir(&current)?;
                    }
                }
            }
        }
        let mut writer = self.meta_volume.fs().open_write(&path)?;
        writer.write(content)?;
        writer.close()?;
        Ok(())
    }

    /// Returns the last-modified time (unix seconds) of a file on the **metadata volume** at a
    /// path relative to the DB base dir. Returns `Ok(None)` if the file does not exist.
    ///
    /// Used by the orphan sweep to check the age of compaction job lease files.
    pub(crate) fn metadata_volume_last_modified(&self, relative_path: &str) -> Result<Option<u64>> {
        let path = if self.options.base_dir.is_empty() {
            relative_path.to_string()
        } else {
            format!("{}/{}", self.options.base_dir, relative_path)
        };
        self.meta_volume.fs().last_modified(&path)
    }

    /// Deletes a file (or empty directory) on the **metadata volume** at a path relative to the
    /// DB base dir. No-op if the path does not exist.
    ///
    /// Used to clean up compaction job lease files and job directories from the metadata volume.
    pub(crate) fn remove_metadata_volume_path(&self, relative_path: &str) -> Result<()> {
        let path = if self.options.base_dir.is_empty() {
            relative_path.to_string()
        } else {
            format!("{}/{}", self.options.base_dir, relative_path)
        };
        if self.meta_volume.fs().exists(&path)? {
            self.meta_volume.fs().delete(&path)?;
        }
        Ok(())
    }
}

impl Drop for FileManager {
    fn drop(&mut self) {
        self.stop_offload_worker();
    }
}

#[cfg(test)]
pub(crate) use tests::test_utils;

#[cfg(test)]
#[path = "../../tests/unit/file/file_manager.rs"]
pub(crate) mod tests;
