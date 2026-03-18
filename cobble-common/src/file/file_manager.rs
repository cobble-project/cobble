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
use crate::file::offload::OffloadRuntime;
use crate::lru::LruCache;
use crate::metrics_manager::MetricsManager;
use crate::util::normalize_storage_path_to_url;
use bytes::Bytes;
use dashmap::DashMap;
use metrics::{Counter, Gauge, counter, gauge};
use rand::random;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use uuid::Uuid;

const DATA_DIR: &str = "data";
const SNAPSHOT_DIR: &str = "snapshot";
const SCHEMA_DIR: &str = "schema";
const DEFAULT_BASE_FILE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_READER_CACHE_CAPACITY: usize = 512;
const SNAPSHOT_COPY_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const VLOG_FILE_PRIORITY: u8 = 10;
const DEFAULT_TRACKED_FILE_PRIORITY: u8 = u8::MAX;

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
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error> {
        self.inner.read_at(offset, size)
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
    pub(crate) used_bytes: AtomicU64,
    pub(crate) projected_offload_bytes: AtomicU64,
    pub(crate) priority: VolumePriority,
    pub(crate) supports_primary_data: bool,
    pub(crate) supports_meta: bool,
    pub(crate) snapshot_persistable: bool,
    pub(crate) readonly_source: bool,
}

impl Clone for DataVolume {
    fn clone(&self) -> Self {
        Self {
            fs: Arc::clone(&self.fs),
            base_dir: self.base_dir.clone(),
            size_limit: self.size_limit,
            used_bytes: AtomicU64::new(self.used_bytes.load(Ordering::SeqCst)),
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
        self.used_bytes.fetch_add(bytes, Ordering::SeqCst);
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
                Ok(_) => break,
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
    fn register_temp_copied_file(&self, file_id: FileId);
}

pub(crate) trait RestoreCopyResourceRegistry: Send + Sync {
    fn register_temp_restored_copy(&self, file_id: FileId);
}

pub(crate) struct RestoredDataFileRegistration {
    pub(crate) snapshot_link_file_id: Option<FileId>,
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
    /// Offload policy for selecting candidate files.
    pub primary_volume_offload_policy: PrimaryVolumeOffloadPolicyKind,
}

impl Default for FileManagerOptions {
    fn default() -> Self {
        Self {
            base_dir: "".to_string(),
            data_file_extension: "sst".to_string(),
            base_file_size: DEFAULT_BASE_FILE_SIZE,
            primary_volume_write_stop_watermark: 0.95,
            primary_volume_offload_trigger_watermark: 0.85,
            primary_volume_offload_policy: PrimaryVolumeOffloadPolicyKind::Priority,
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
    /// Bytes tracked for this file.
    pub(crate) size_bytes: AtomicU64,
    /// Whether to delete the file when this TrackedFile is dropped.
    pub(crate) delete_on_drop: AtomicBool,
    /// Count of explicit references to this file (e.g., from snapshots).
    pub(crate) explicit_refs: AtomicU32,
    /// File priority used by primary-volume offload policy.
    pub(crate) priority: AtomicU8,
}

impl TrackedFile {
    pub(crate) fn fs(&self) -> &Arc<dyn FileSystem> {
        &self.fs
    }
    /// Creates a new TrackedFile.
    pub(crate) fn new(
        path: String,
        fs: Arc<dyn FileSystem>,
        volume: Option<Arc<DataVolume>>,
    ) -> Self {
        Self {
            path,
            fs,
            volume,
            size_bytes: AtomicU64::new(0),
            delete_on_drop: AtomicBool::new(true),
            explicit_refs: AtomicU32::new(0),
            priority: AtomicU8::new(DEFAULT_TRACKED_FILE_PRIORITY),
        }
    }

    /// Creates a new TrackedFile that never deletes on drop.
    pub(crate) fn readonly(
        path: String,
        fs: Arc<dyn FileSystem>,
        volume: Option<Arc<DataVolume>>,
    ) -> Self {
        Self {
            path,
            fs,
            volume,
            size_bytes: AtomicU64::new(0),
            delete_on_drop: AtomicBool::new(false),
            explicit_refs: AtomicU32::new(0),
            priority: AtomicU8::new(DEFAULT_TRACKED_FILE_PRIORITY),
        }
    }

    /// Marks the file for deletion when this TrackedFile is dropped.
    pub(crate) fn mark_for_deletion(&self) {
        self.delete_on_drop.store(true, Ordering::SeqCst);
    }

    pub(crate) fn mark_for_retention(&self) {
        self.delete_on_drop.store(false, Ordering::SeqCst);
    }

    /// Returns true if the file is marked for deletion on drop.
    pub(crate) fn is_marked_for_deletion(&self) -> bool {
        self.delete_on_drop.load(Ordering::SeqCst)
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
        if let Some(volume) = &self.volume {
            volume.add_usage(delta);
        }
    }

    pub(crate) fn set_priority(&self, priority: u8) {
        self.priority.store(priority, Ordering::SeqCst);
    }

    pub(crate) fn priority(&self) -> u8 {
        self.priority.load(Ordering::SeqCst)
    }
}

/// Handle that keeps a data file id tracked by the FileManager.
pub struct TrackedFileId {
    file_id: FileId,
    file_manager: Weak<FileManager>,
}

impl TrackedFileId {
    pub fn new(file_manager: &Arc<FileManager>, file_id: FileId) -> Arc<Self> {
        Arc::new(Self {
            file_id,
            file_manager: Arc::downgrade(file_manager),
        })
    }

    pub fn file_id(&self) -> FileId {
        self.file_id
    }

    pub fn detached(file_id: FileId) -> Arc<Self> {
        Arc::new(Self {
            file_id,
            file_manager: Weak::new(),
        })
    }

    pub(crate) fn set_priority(&self, priority: u8) -> Result<()> {
        let Some(file_manager) = self.file_manager.upgrade() else {
            return Ok(());
        };
        file_manager.set_data_file_priority(self.file_id, priority)
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
        if self.delete_on_drop.load(Ordering::SeqCst)
            && self.explicit_refs.load(Ordering::SeqCst) == 0
        {
            // Attempt to delete the file, ignore errors
            let _ = self.fs.delete_async(&self.path);
        }
        if let Some(volume) = &self.volume {
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
    inner: Box<dyn RandomAccessFile>,
    _tracked: Arc<TrackedFile>,
}

impl TrackedReader {
    /// Creates a new TrackedReader.
    pub fn new(inner: Box<dyn RandomAccessFile>, tracked: Arc<TrackedFile>) -> Self {
        Self {
            inner,
            _tracked: tracked,
        }
    }
}

impl File for TrackedReader {
    fn close(&mut self) -> Result<(), Error> {
        self.inner.close()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

impl RandomAccessFile for TrackedReader {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error> {
        self.inner.read_at(offset, size)
    }
}

/// A wrapper around a SequentialWriteFile that holds a reference to the TrackedFile.
/// This ensures the TrackedFile is not dropped while the file is being written.
pub struct TrackedWriter {
    inner: Box<dyn SequentialWriteFile>,
    tracked: Arc<TrackedFile>,
}

impl TrackedWriter {
    /// Creates a new TrackedWriter.
    pub fn new(inner: Box<dyn SequentialWriteFile>, tracked: Arc<TrackedFile>) -> Self {
        Self { inner, tracked }
    }
}

impl File for TrackedWriter {
    fn close(&mut self) -> Result<(), Error> {
        self.inner.close()
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
    writer: Option<TrackedWriter>,
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
            writer: Some(writer),
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
        self.fs.rename(&self.temp_path, &self.final_path)?;
        let tracked = Arc::new(TrackedFile::new(
            self.final_path.clone(),
            Arc::clone(&self.fs),
            self.volume.clone(),
        ));
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
    /// Map of file ID to tracked file information for data files.
    pub(crate) data_files: DashMap<FileId, Arc<TrackedFile>>,
    /// Lazy hint mapping from primary data file id to equivalent snapshot replica file id.
    snapshot_replica_hints: DashMap<FileId, FileId>,
    /// Map of filename to tracked file information for metadata files.
    pub(crate) metadata_files: Arc<DashMap<String, Arc<TrackedFile>>>,
    /// LRU cache for open random access readers.
    pub(crate) reader_cache: Mutex<LruCache<FileId, Arc<dyn RandomAccessFile>>>,
    pub(crate) offload_runtime: Arc<OffloadRuntime>,
}

impl FileManager {
    fn is_volume_write_stopped(&self, volume: &Arc<DataVolume>) -> bool {
        volume.is_write_stopped(self.options.primary_volume_write_stop_watermark)
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

    fn select_data_volume(&self) -> Result<&Arc<DataVolume>> {
        let base_file_size = self.options.base_file_size as u64;
        let mut selected: Option<&Arc<DataVolume>> = None;
        let mut candidates: Vec<&Arc<DataVolume>> = Vec::with_capacity(self.data_volumes.len());
        for volume in &self.data_volumes {
            if !volume.supports_primary_data {
                continue;
            }
            if !candidates.is_empty() && volume.priority.rank() < candidates[0].priority.rank() {
                break;
            }
            if self.is_volume_write_stopped(volume) {
                continue;
            }
            if volume.is_full(base_file_size) {
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
        ));
        Ok(Self {
            metrics: metrics_manager.file_manager_metrics(),
            meta_volume,
            data_volumes,
            options,
            next_file_id: AtomicU64::new(1), // Start from 1, 0 is reserved
            data_files: DashMap::new(),
            snapshot_replica_hints: DashMap::new(),
            metadata_files: Arc::new(DashMap::new()),
            reader_cache: Mutex::new(LruCache::new(DEFAULT_READER_CACHE_CAPACITY)),
            offload_runtime,
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
            used_bytes: AtomicU64::new(0),
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
        let data_volumes = Self::data_volumes_from_config(config)?;
        let options = FileManagerOptions {
            base_dir: db_id.to_string(),
            base_file_size: config.base_file_size,
            primary_volume_write_stop_watermark: config.primary_volume_write_stop_watermark,
            primary_volume_offload_trigger_watermark: config
                .primary_volume_offload_trigger_watermark,
            primary_volume_offload_policy: config.primary_volume_offload_policy,
            ..FileManagerOptions::default()
        };
        Self::new(data_volumes, options, metrics_manager)
    }

    pub(crate) fn data_volumes_from_config(config: &Config) -> Result<Vec<DataVolume>> {
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
        for volume in &volumes {
            let readonly_source = volume.supports(VolumeUsageKind::Readonly);
            if readonly_source {
                let has_other_kinds = volume.supports(VolumeUsageKind::Meta)
                    || volume.supports(VolumeUsageKind::PrimaryDataPriorityHigh)
                    || volume.supports(VolumeUsageKind::PrimaryDataPriorityMedium)
                    || volume.supports(VolumeUsageKind::PrimaryDataPriorityLow)
                    || volume.supports(VolumeUsageKind::Snapshot)
                    || volume.supports(VolumeUsageKind::Cache);
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
                    size_limit: volume.size_limit,
                    used_bytes: AtomicU64::new(0),
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
        let volume = self.select_data_volume()?;
        let writer = self.create_data_file_writer_on_volume(file_id, volume)?;
        Ok((file_id, writer))
    }

    /// Creates a new data file with a specific file ID.
    ///
    /// This is useful when recovering files or when the ID is known in advance.
    /// Returns an error if the file ID is already in use.
    pub fn create_data_file_with_id(&self, file_id: FileId) -> Result<TrackedWriter> {
        if self.data_files.contains_key(&file_id) {
            return Err(Error::IoError(format!(
                "File ID {} is already in use",
                file_id
            )));
        }

        let volume = self.select_data_volume()?;
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
        let tracked = Arc::new(TrackedFile::new(
            self.data_file_path(file_id),
            Arc::clone(volume.fs()),
            Some(Arc::clone(volume)),
        ));
        self.data_files.insert(file_id, Arc::clone(&tracked));
        self.report_data_files_gauge();
        let writer = volume.fs().open_write(tracked.path())?;
        Ok(TrackedWriter::new(writer, tracked))
    }

    /// Registers an existing data file with the FileManager.
    ///
    /// This is useful when recovering files from disk or when files were
    /// created externally. The file is tracked but no reader is opened.
    pub fn register_data_file(&self, file_id: FileId, path: &str) -> Result<()> {
        let (volume, relative_path) = self.resolve_volume_path(path)?;
        let fs = Arc::clone(volume.fs());

        // Track the file if not already tracked
        {
            let tracked = self.data_files.entry(file_id).or_insert_with(|| {
                if volume.readonly_source {
                    Arc::new(TrackedFile::readonly(
                        relative_path.clone(),
                        Arc::clone(&fs),
                        Some(Arc::clone(&volume)),
                    ))
                } else {
                    Arc::new(TrackedFile::new(
                        relative_path.clone(),
                        Arc::clone(&fs),
                        Some(Arc::clone(&volume)),
                    ))
                }
            });
            let size = fs
                .open_read(tracked.path())
                .map(|reader| reader.size())
                .unwrap_or(0);
            tracked.update_size_bytes(size as u64);
        }
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

    pub(crate) fn copy_reader_to_tracked_writer(
        &self,
        source: &dyn RandomAccessFile,
        writer: &mut TrackedWriter,
    ) -> Result<()> {
        let source_size = source.size();
        let mut offset = 0usize;
        while offset < source_size {
            let chunk = SNAPSHOT_COPY_CHUNK_BYTES.min(source_size - offset);
            let bytes = source.read_at(offset, chunk)?;
            writer.write(bytes.as_ref())?;
            offset += bytes.len();
        }
        writer.close()?;
        Ok(())
    }

    /// Registers an existing data file for restore operations.
    /// This will copy the file if it's on a snapshot volume, or link it if possible.
    pub(crate) fn register_data_file_for_restore(
        &self,
        file_id: FileId,
        path: &str,
        resource_registry: Option<Arc<dyn RestoreCopyResourceRegistry + Send + Sync>>,
    ) -> Result<RestoredDataFileRegistration> {
        let (source_volume, source_relative_path) = self.resolve_volume_path(path)?;
        if source_volume.snapshot_persistable && source_volume.supports_primary_data {
            self.register_data_file_readonly(file_id, path)?;
            return Ok(RestoredDataFileRegistration {
                snapshot_link_file_id: Some(file_id),
            });
        }

        if source_volume.supports_primary_data {
            self.register_data_file_readonly(file_id, path)?;
            return Ok(RestoredDataFileRegistration {
                snapshot_link_file_id: None,
            });
        }

        let snapshot_link_file_id = if source_volume.snapshot_persistable {
            let snapshot_file_id = self.allocate_file_id();
            self.register_data_file_readonly(snapshot_file_id, path)?;
            Some(snapshot_file_id)
        } else {
            None
        };

        let source_reader = source_volume.fs().open_read(&source_relative_path)?;
        let mut writer = self.create_data_file_with_id(file_id)?;
        if let Err(err) = self.copy_reader_to_tracked_writer(source_reader.as_ref(), &mut writer) {
            let _ = self.remove_data_file(file_id);
            if let Some(snapshot_file_id) = snapshot_link_file_id {
                let _ = self.remove_data_file(snapshot_file_id);
            }
            return Err(err);
        }

        if let Some(registry) = resource_registry {
            registry.register_temp_restored_copy(file_id);
        }

        Ok(RestoredDataFileRegistration {
            snapshot_link_file_id,
        })
    }

    /// Marks a data file as read-only, preventing it from being deleted on drop.
    pub(crate) fn make_data_file_readonly(&self, file_id: FileId) -> Result<()> {
        let tracked = self
            .data_files
            .get(&file_id)
            .ok_or_else(|| Error::IoError(format!("Data file {} is not tracked", file_id)))?;
        tracked.mark_for_retention();
        Ok(())
    }

    pub(crate) fn make_data_file_owned(&self, file_id: FileId) -> Result<()> {
        let tracked = self
            .data_files
            .get(&file_id)
            .ok_or_else(|| Error::IoError(format!("Data file {} is not tracked", file_id)))?;
        tracked.mark_for_deletion();
        Ok(())
    }

    pub(crate) fn set_data_file_priority(&self, file_id: FileId, priority: u8) -> Result<()> {
        let tracked = self
            .data_files
            .get(&file_id)
            .ok_or_else(|| Error::IoError(format!("Data file {} is not tracked", file_id)))?;
        tracked.set_priority(priority);
        Ok(())
    }

    pub(crate) fn register_snapshot_replica_hint(
        &self,
        source_file_id: FileId,
        replica_file_id: FileId,
    ) {
        if source_file_id == replica_file_id {
            return;
        }
        self.snapshot_replica_hints
            .insert(source_file_id, replica_file_id);
    }

    pub(crate) fn snapshot_replica_hint_file_id(&self, source_file_id: FileId) -> Option<FileId> {
        self.snapshot_replica_hints
            .get(&source_file_id)
            .map(|entry| *entry.value())
    }

    /// Opens a data file for reading.
    ///
    /// Returns a TrackedReader that holds a reference to the TrackedFile.
    /// The file will not be deleted while the reader is in use.
    pub fn open_data_file_reader(&self, file_id: FileId) -> Result<TrackedReader> {
        // Get the tracked file
        let tracked = self.data_files.get(&file_id).ok_or_else(|| {
            Error::IoError(format!(
                "Data file {} is not tracked by FileManager",
                file_id
            ))
        })?;
        let cached = {
            let mut cache = self
                .reader_cache
                .lock()
                .map_err(|_| Error::IoError("Reader cache lock poisoned".to_string()))?;
            cache.get(&file_id).map(Arc::clone)
        };
        let reader = if let Some(reader) = cached {
            reader
        } else {
            let reader = tracked.fs().open_read(tracked.path())?;
            let reader: Arc<dyn RandomAccessFile> = Arc::from(reader);
            let mut cache = self
                .reader_cache
                .lock()
                .map_err(|_| Error::IoError("Reader cache lock poisoned".to_string()))?;
            cache.insert(file_id, Arc::clone(&reader));
            reader
        };
        let reader = Box::new(CachedRandomAccessFile::new(reader));
        Ok(TrackedReader::new(reader, Arc::clone(&tracked)))
    }

    /// Returns the tracked data file reference.
    pub(crate) fn data_file_ref(&self, file_id: FileId) -> Result<Arc<TrackedFile>> {
        let tracked = self.data_files.get(&file_id).ok_or_else(|| {
            Error::IoError(format!(
                "Data file {} is not tracked by FileManager",
                file_id
            ))
        })?;
        tracked.reference();
        Ok(Arc::clone(&tracked))
    }

    pub(crate) fn has_snapshot_volume(&self) -> bool {
        self.data_volumes
            .iter()
            .any(|volume| volume.snapshot_persistable)
    }

    pub(crate) fn is_data_file_persistable_for_snapshot(&self, file_id: FileId) -> bool {
        let Some(tracked) = self.data_files.get(&file_id) else {
            return false;
        };
        let Some(tracked_volume) = &tracked.volume else {
            return false;
        };
        tracked_volume.snapshot_persistable
    }

    pub(crate) fn is_data_file_on_primary_volume(&self, file_id: FileId) -> bool {
        let Some(tracked) = self.data_files.get(&file_id) else {
            return false;
        };
        let Some(volume) = &tracked.volume else {
            return false;
        };
        volume.supports_primary_data
    }

    pub(crate) fn is_data_file_on_snapshot_volume(&self, file_id: FileId) -> bool {
        let Some(tracked) = self.data_files.get(&file_id) else {
            return false;
        };
        let Some(volume) = &tracked.volume else {
            return false;
        };
        volume.snapshot_persistable
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

    #[cfg(test)]
    pub(crate) fn copy_data_file_to_snapshot_volume(
        &self,
        source_file_id: FileId,
    ) -> Result<FileId> {
        self.copy_data_file_to_snapshot_volume_with_result(source_file_id, None)
            .map(|(file_id, _)| file_id)
    }

    /// Copies a data file to a snapshot-persistable volume if it's not already on one.
    ///
    /// Returns the file ID of the snapshot-persistable copy (which may be the same as the source)
    /// and a boolean indicating whether a copy was actually made.
    pub(crate) fn copy_data_file_to_snapshot_volume_with_result(
        &self,
        source_file_id: FileId,
        resource_registry: Option<Arc<dyn SnapshotCopyResourceRegistry + Send + Sync>>,
    ) -> Result<(FileId, bool)> {
        if self.is_data_file_persistable_for_snapshot(source_file_id) {
            return Ok((source_file_id, false));
        }
        let snapshot_volume = self.select_snapshot_persistable_volume()?;
        let source_tracked = self.data_files.get(&source_file_id).ok_or_else(|| {
            Error::IoError(format!(
                "Data file {} is not tracked by FileManager",
                source_file_id
            ))
        })?;
        let source_reader = source_tracked.fs().open_read(source_tracked.path())?;
        let target_file_id = self.allocate_file_id();
        let mut writer = self.create_data_file_writer_on_volume(target_file_id, snapshot_volume)?;

        let copy_result = self.copy_reader_to_tracked_writer(source_reader.as_ref(), &mut writer);

        if let Err(err) = copy_result {
            let _ = self.remove_data_file(target_file_id);
            return Err(err);
        }
        writer.tracked.set_priority(source_tracked.priority());
        if let Some(registry) = resource_registry {
            registry.register_temp_copied_file(target_file_id);
        }
        Ok((target_file_id, true))
    }

    /// Returns the path for a data file.
    pub fn get_data_file_path(&self, file_id: FileId) -> Option<String> {
        self.data_files.get(&file_id).map(|f| f.path().to_string())
    }

    /// Returns the full path for a data file, including the volume base directory if known.
    pub fn get_data_file_full_path(&self, file_id: FileId) -> Option<String> {
        self.data_files.get(&file_id).map(|f| f.absolute_path())
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
        self.data_files.contains_key(&file_id)
    }

    /// Removes a data file from tracking.
    pub(crate) fn remove_data_file(&self, file_id: FileId) -> Result<()> {
        self.snapshot_replica_hints.remove(&file_id);
        let Some(_) = self.data_files.remove(&file_id) else {
            return Ok(());
        };
        if let Ok(mut cache) = self.reader_cache.lock() {
            cache.remove(&file_id);
        }
        self.report_data_files_gauge();
        Ok(())
    }

    /// Returns the number of tracked data files.
    pub fn data_file_count(&self) -> usize {
        self.data_files.len()
    }

    /// Returns all tracked data file IDs.
    pub fn data_file_ids(&self) -> Vec<FileId> {
        self.data_files.iter().map(|entry| *entry.key()).collect()
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

        let tracked = Arc::new(TrackedFile::new(
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
            let tracked = Arc::new(TrackedFile::new(
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
        Ok(TrackedReader::new(reader, Arc::clone(&tracked)))
    }

    /// Opens a metadata file for reading without tracking it.
    pub fn open_metadata_file_reader_untracked(
        &self,
        name: &str,
    ) -> Result<Box<dyn RandomAccessFile>> {
        let path = self.metadata_file_path(name);
        self.meta_volume.fs().open_read(&path)
    }

    /// Registers an existing data file without deleting it on drop.
    pub fn register_data_file_readonly(&self, file_id: FileId, path: &str) -> Result<()> {
        let (volume, relative_path) = self.resolve_volume_path(path)?;
        let fs = Arc::clone(volume.fs());
        let tracked = Arc::new(TrackedFile::readonly(
            relative_path,
            Arc::clone(&fs),
            Some(Arc::clone(&volume)),
        ));
        let size = fs
            .open_read(tracked.path())
            .map(|reader| reader.size())
            .unwrap_or(0);
        tracked.update_size_bytes(size as u64);
        {
            self.data_files.entry(file_id).or_insert_with(|| tracked);
        }
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
            tracked.mark_for_deletion();
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
                tracked.mark_for_deletion();
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
            .set(self.data_files.len() as f64);
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
}

impl Drop for FileManager {
    fn drop(&mut self) {
        self.stop_offload_worker();
    }
}

pub(crate) mod test_utils {
    use crate::file::file_system::FileSystem;
    use std::sync::Arc;

    pub(crate) fn wait_for_file_deletion(fs: &Arc<dyn FileSystem>, path: &str) {
        for _ in 0..50 {
            if !fs.exists(path).unwrap() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::file::FileSystemRegistry;
    use crate::file::files::File;
    use crate::metrics_manager::MetricsManager;

    static TEST_ROOT: &str = "file:///tmp/file_manager_test";

    fn cleanup_test_root() {
        let _ = std::fs::remove_dir_all("/tmp/file_manager_test");
    }

    fn create_test_file_manager() -> (Arc<dyn FileSystem>, FileManager) {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(TEST_ROOT.to_string()).unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-test"));
        let fm = FileManager::with_defaults(Arc::clone(&fs), metrics_manager).unwrap();
        (fs, fm)
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_create_data_file() {
        let (_fs, fm) = create_test_file_manager();

        // Create a data file
        let (file_id, mut writer) = fm.create_data_file().unwrap();
        assert_eq!(file_id, 1);
        assert!(fm.has_data_file(file_id));

        // Write some data
        writer.write(b"test data").unwrap();
        writer.close().unwrap();

        // Verify we can read it back
        let reader = fm.open_data_file_reader(file_id).unwrap();
        let data = reader.read_at(0, 9).unwrap();
        assert_eq!(&data[..], b"test data");

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_multiple_data_files() {
        let (_fs, fm) = create_test_file_manager();

        // Create multiple files
        let (id1, mut w1) = fm.create_data_file().unwrap();
        let (id2, mut w2) = fm.create_data_file().unwrap();
        let (id3, mut w3) = fm.create_data_file().unwrap();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
        assert_eq!(fm.data_file_count(), 3);

        // Write different data to each
        w1.write(b"file1").unwrap();
        w2.write(b"file2").unwrap();
        w3.write(b"file3").unwrap();
        w1.close().unwrap();
        w2.close().unwrap();
        w3.close().unwrap();

        // Read back and verify
        let r1 = fm.open_data_file_reader(id1).unwrap();
        let r2 = fm.open_data_file_reader(id2).unwrap();
        let r3 = fm.open_data_file_reader(id3).unwrap();

        assert_eq!(&r1.read_at(0, 5).unwrap()[..], b"file1");
        assert_eq!(&r2.read_at(0, 5).unwrap()[..], b"file2");
        assert_eq!(&r3.read_at(0, 5).unwrap()[..], b"file3");

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_remove_data_file() {
        let (fs, fm) = create_test_file_manager();

        // Create and write a file
        let (file_id, mut writer) = fm.create_data_file().unwrap();
        writer.write(b"data").unwrap();
        writer.close().unwrap();
        drop(writer); // Drop the writer to release the Arc reference

        let path = fm.get_data_file_path(file_id).unwrap();
        assert!(fs.exists(&path).unwrap());

        // Remove with delete
        fm.remove_data_file(file_id).unwrap();
        assert!(!fm.has_data_file(file_id));
        test_utils::wait_for_file_deletion(&fs, &path);
        assert!(!fs.exists(&path).unwrap());

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_remove_data_file_with_snapshot_ref() {
        let (fs, fm) = create_test_file_manager();

        let (file_id, mut writer) = fm.create_data_file().unwrap();
        writer.write(b"data").unwrap();
        writer.close().unwrap();
        drop(writer);

        let tracked = fm.data_file_ref(file_id).unwrap();
        let path = fm.get_data_file_path(file_id).unwrap();

        fm.remove_data_file(file_id).unwrap();
        assert!(!fm.has_data_file(file_id));
        assert!(fs.exists(&path).unwrap());

        tracked.dereference();
        assert!(fs.exists(&path).unwrap());
        drop(tracked);
        test_utils::wait_for_file_deletion(&fs, &path);
        assert!(!fs.exists(&path).unwrap());

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_metadata_files() {
        let (_fs, fm) = create_test_file_manager();

        // Create a metadata file
        let mut writer = fm.create_metadata_file("manifest").unwrap();
        writer.write(b"manifest data").unwrap();
        writer.close().unwrap();

        assert!(fm.has_metadata_file("manifest"));
        assert_eq!(fm.metadata_file_count(), 1);

        // Read it back
        let reader = fm.open_metadata_file_reader("manifest").unwrap();
        let data = reader.read_at(0, 13).unwrap();
        assert_eq!(&data[..], b"manifest data");

        // Remove it
        fm.remove_metadata_file("manifest").unwrap();
        assert!(!fm.has_metadata_file("manifest"));

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_set_next_file_id() {
        let (_fs, fm) = create_test_file_manager();

        // Set a custom starting ID
        fm.set_next_file_id(100);
        assert_eq!(fm.peek_next_file_id(), 100);

        let (file_id, mut writer) = fm.create_data_file().unwrap();
        assert_eq!(file_id, 100);
        writer.close().unwrap();

        let (file_id2, mut writer2) = fm.create_data_file().unwrap();
        assert_eq!(file_id2, 101);
        writer2.close().unwrap();

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_create_with_specific_id() {
        let (_fs, fm) = create_test_file_manager();

        // Create file with specific ID
        let mut writer = fm.create_data_file_with_id(50).unwrap();
        writer.write(b"data50").unwrap();
        writer.close().unwrap();

        assert!(fm.has_data_file(50));
        assert_eq!(fm.peek_next_file_id(), 51);

        // Should fail if ID already exists
        let result = fm.create_data_file_with_id(50);
        assert!(result.is_err());

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_register_existing_file() {
        let (fs, fm) = create_test_file_manager();

        // Create a file directly on the filesystem
        let path = "data/existing_file.sst";
        let mut writer = fs.open_write(path).unwrap();
        writer.write(b"existing").unwrap();
        writer.close().unwrap();

        // Register it with path
        fm.register_data_file(999, path).unwrap();
        assert!(fm.has_data_file(999));
        assert_eq!(fm.peek_next_file_id(), 1000);

        // Can read it
        let reader = fm.open_data_file_reader(999).unwrap();
        let data = reader.read_at(0, 8).unwrap();
        assert_eq!(&data[..], b"existing");

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_data_file_ids() {
        let (_fs, fm) = create_test_file_manager();

        let (id1, mut w1) = fm.create_data_file().unwrap();
        let (id2, mut w2) = fm.create_data_file().unwrap();
        w1.close().unwrap();
        w2.close().unwrap();

        let mut ids = fm.data_file_ids();
        ids.sort();
        assert_eq!(ids, vec![id1, id2]);

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_metadata_file_names() {
        let (_fs, fm) = create_test_file_manager();

        let mut w1 = fm.create_metadata_file("manifest").unwrap();
        let mut w2 = fm.create_metadata_file("wal").unwrap();
        w1.close().unwrap();
        w2.close().unwrap();

        let mut names = fm.metadata_file_names();
        names.sort();
        assert_eq!(names, vec!["manifest", "wal"]);

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_volume_priority_selection() {
        let root = "/tmp/file_manager_volume_priority";
        let _ = std::fs::remove_dir_all(root);
        let registry = FileSystemRegistry::new();
        let high_fs = registry
            .get_or_register(format!("file://{}/high", root))
            .unwrap();
        let low_fs = registry
            .get_or_register(format!("file://{}/low", root))
            .unwrap();
        let high_volume = DataVolume {
            fs: Arc::clone(&high_fs),
            base_dir: Some(format!("{}/high", root)),
            size_limit: Some(128),
            used_bytes: AtomicU64::new(0),
            projected_offload_bytes: AtomicU64::new(0),
            priority: VolumePriority::High,
            supports_primary_data: true,
            supports_meta: false,
            snapshot_persistable: true,
            readonly_source: false,
        };
        let low_volume = DataVolume {
            fs: Arc::clone(&low_fs),
            base_dir: Some(format!("{}/low", root)),
            size_limit: None,
            used_bytes: AtomicU64::new(0),
            projected_offload_bytes: AtomicU64::new(0),
            priority: VolumePriority::Low,
            supports_primary_data: true,
            supports_meta: false,
            snapshot_persistable: false,
            readonly_source: false,
        };
        let mut options = FileManagerOptions::default();
        options.base_dir = "db".to_string();
        options.base_file_size = 64;
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-test"));
        let fm = FileManager::new(vec![high_volume, low_volume], options, metrics_manager).unwrap();

        let (file_id1, mut writer1) = fm.create_data_file().unwrap();
        writer1.write(&vec![b'a'; 80]).unwrap();
        writer1.close().unwrap();
        let path1 = fm.get_data_file_path(file_id1).unwrap();
        assert!(high_fs.exists(&path1).unwrap());

        let (file_id2, mut writer2) = fm.create_data_file().unwrap();
        writer2.write(&vec![b'b'; 8]).unwrap();
        writer2.close().unwrap();
        let path2 = fm.get_data_file_path(file_id2).unwrap();
        assert!(low_fs.exists(&path2).unwrap());

        let manifest = "manifest";
        let mut meta_writer = fm.create_metadata_file(manifest).unwrap();
        meta_writer.write(b"meta").unwrap();
        meta_writer.close().unwrap();
        let meta_path = fm.get_metadata_file_path(manifest).unwrap();
        assert!(high_fs.exists(&meta_path).unwrap());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_snapshot_volume_for_metadata_and_copy() {
        let root = "/tmp/file_manager_snapshot_volume";
        let _ = std::fs::remove_dir_all(root);
        let primary_url = format!("file://{}/primary", root);
        let snapshot_url = format!("file://{}/snapshot", root);
        let registry = FileSystemRegistry::new();
        let primary_fs = registry.get_or_register(primary_url.clone()).unwrap();
        let snapshot_fs = registry.get_or_register(snapshot_url.clone()).unwrap();
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    primary_url,
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    snapshot_url,
                    vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-snapshot"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();

        let (source_file_id, mut source_writer) = fm.create_data_file().unwrap();
        source_writer.write(b"source-bytes").unwrap();
        source_writer.close().unwrap();
        let source_path = fm.get_data_file_path(source_file_id).unwrap();
        assert!(primary_fs.exists(&source_path).unwrap());
        assert!(!fm.is_data_file_persistable_for_snapshot(source_file_id));

        let mut metadata_writer = fm.create_metadata_file("snapshot/MANIFEST").unwrap();
        metadata_writer.write(b"manifest").unwrap();
        metadata_writer.close().unwrap();
        let metadata_path = fm.get_metadata_file_path("snapshot/MANIFEST").unwrap();
        assert!(snapshot_fs.exists(&metadata_path).unwrap());
        assert!(!primary_fs.exists(&metadata_path).unwrap());

        let copied_file_id = fm
            .copy_data_file_to_snapshot_volume(source_file_id)
            .unwrap();
        assert_ne!(copied_file_id, source_file_id);
        assert!(fm.is_data_file_persistable_for_snapshot(copied_file_id));
        let copied_path = fm.get_data_file_path(copied_file_id).unwrap();
        assert!(snapshot_fs.exists(&copied_path).unwrap());
        let copied_reader = fm.open_data_file_reader(copied_file_id).unwrap();
        assert_eq!(&copied_reader.read_at(0, 12).unwrap()[..], b"source-bytes");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_meta_volume_uses_first_snapshot_only_volume() {
        let root = "/tmp/file_manager_snapshot_meta_first";
        let _ = std::fs::remove_dir_all(root);
        let primary_url = format!("file://{}/primary", root);
        let snapshot_a_url = format!("file://{}/snapshot-a", root);
        let snapshot_b_url = format!("file://{}/snapshot-b", root);
        let registry = FileSystemRegistry::new();
        let snapshot_a_fs = registry.get_or_register(snapshot_a_url.clone()).unwrap();
        let snapshot_b_fs = registry.get_or_register(snapshot_b_url.clone()).unwrap();
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    primary_url,
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(snapshot_a_url, vec![VolumeUsageKind::Snapshot]),
                crate::VolumeDescriptor::new(snapshot_b_url, vec![VolumeUsageKind::Snapshot]),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-snapshot-meta-first"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let mut metadata_writer = fm.create_metadata_file("snapshot/MANIFEST").unwrap();
        metadata_writer.write(b"manifest").unwrap();
        metadata_writer.close().unwrap();
        let metadata_path = fm.get_metadata_file_path("snapshot/MANIFEST").unwrap();
        assert!(snapshot_a_fs.exists(&metadata_path).unwrap());
        assert!(!snapshot_b_fs.exists(&metadata_path).unwrap());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_meta_volume_prefers_meta_kind_over_snapshot_kind() {
        let root = "/tmp/file_manager_meta_kind_preferred";
        let _ = std::fs::remove_dir_all(root);
        let primary_url = format!("file://{}/primary", root);
        let snapshot_url = format!("file://{}/snapshot", root);
        let meta_url = format!("file://{}/meta", root);
        let registry = FileSystemRegistry::new();
        let snapshot_fs = registry.get_or_register(snapshot_url.clone()).unwrap();
        let meta_fs = registry.get_or_register(meta_url.clone()).unwrap();
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    primary_url,
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(snapshot_url, vec![VolumeUsageKind::Snapshot]),
                crate::VolumeDescriptor::new(meta_url, vec![VolumeUsageKind::Meta]),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-meta-kind"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();

        let mut metadata_writer = fm.create_metadata_file("snapshot/MANIFEST").unwrap();
        metadata_writer.write(b"manifest").unwrap();
        metadata_writer.close().unwrap();
        let metadata_path = fm.get_metadata_file_path("snapshot/MANIFEST").unwrap();
        assert!(meta_fs.exists(&metadata_path).unwrap());
        assert!(!snapshot_fs.exists(&metadata_path).unwrap());

        let (source_file_id, mut source_writer) = fm.create_data_file().unwrap();
        source_writer.write(b"source-bytes").unwrap();
        source_writer.close().unwrap();
        let copied_file_id = fm
            .copy_data_file_to_snapshot_volume(source_file_id)
            .unwrap();
        let copied_path = fm.get_data_file_path(copied_file_id).unwrap();
        assert!(snapshot_fs.exists(&copied_path).unwrap());
        assert!(!meta_fs.exists(&copied_path).unwrap());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_meta_volume_uses_first_meta_volume() {
        let root = "/tmp/file_manager_meta_first_meta_volume";
        let _ = std::fs::remove_dir_all(root);
        let primary_url = format!("file://{}/primary", root);
        let meta_a_url = format!("file://{}/meta-a", root);
        let meta_b_url = format!("file://{}/meta-b", root);
        let registry = FileSystemRegistry::new();
        let meta_a_fs = registry.get_or_register(meta_a_url.clone()).unwrap();
        let meta_b_fs = registry.get_or_register(meta_b_url.clone()).unwrap();
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    primary_url,
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(meta_a_url, vec![VolumeUsageKind::Meta]),
                crate::VolumeDescriptor::new(meta_b_url, vec![VolumeUsageKind::Meta]),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-meta-first-meta"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();

        let mut metadata_writer = fm.create_metadata_file("snapshot/MANIFEST").unwrap();
        metadata_writer.write(b"manifest").unwrap();
        metadata_writer.close().unwrap();
        let metadata_path = fm.get_metadata_file_path("snapshot/MANIFEST").unwrap();
        assert!(meta_a_fs.exists(&metadata_path).unwrap());
        assert!(!meta_b_fs.exists(&metadata_path).unwrap());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_snapshot_copy_creates_snapshot_file() {
        let root = "/tmp/file_manager_snapshot_copy_reuse";
        let _ = std::fs::remove_dir_all(root);
        let primary_url = format!("file://{}/primary", root);
        let snapshot_url = format!("file://{}/snapshot", root);
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    primary_url,
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(snapshot_url, vec![VolumeUsageKind::Snapshot]),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-snapshot-copy-reuse"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();

        let (source_file_id, mut source_writer) = fm.create_data_file().unwrap();
        source_writer.write(b"source-bytes").unwrap();
        source_writer.close().unwrap();

        let copied_file_id = fm
            .copy_data_file_to_snapshot_volume(source_file_id)
            .unwrap();
        let copied_file_id_again = fm
            .copy_data_file_to_snapshot_volume(source_file_id)
            .unwrap();
        assert_ne!(copied_file_id_again, copied_file_id);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_meta_volume_uses_first_snapshot_persistable_when_no_meta() {
        let root = "/tmp/file_manager_snapshot_meta_shared_priority";
        let _ = std::fs::remove_dir_all(root);
        let high_url = format!("file://{}/high", root);
        let low_url = format!("file://{}/low", root);
        let registry = FileSystemRegistry::new();
        let high_fs = registry.get_or_register(high_url.clone()).unwrap();
        let low_fs = registry.get_or_register(low_url.clone()).unwrap();
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    low_url,
                    vec![
                        VolumeUsageKind::Snapshot,
                        VolumeUsageKind::PrimaryDataPriorityLow,
                    ],
                ),
                crate::VolumeDescriptor::new(
                    high_url,
                    vec![
                        VolumeUsageKind::Snapshot,
                        VolumeUsageKind::PrimaryDataPriorityHigh,
                    ],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-snapshot-meta-first"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let mut metadata_writer = fm.create_metadata_file("snapshot/MANIFEST").unwrap();
        metadata_writer.write(b"manifest").unwrap();
        metadata_writer.close().unwrap();
        let metadata_path = fm.get_metadata_file_path("snapshot/MANIFEST").unwrap();
        assert!(low_fs.exists(&metadata_path).unwrap());
        assert!(!high_fs.exists(&metadata_path).unwrap());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_file_manager_rejects_readonly_volume_with_other_kinds() {
        let root = "/tmp/file_manager_readonly_kinds";
        let _ = std::fs::remove_dir_all(root);
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}/primary", root),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}/bad", root),
                    vec![VolumeUsageKind::Readonly, VolumeUsageKind::Snapshot],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}/snapshot", root),
                    vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
                ),
            ],
            ..Config::default()
        };
        let err = match FileManager::data_volumes_from_config(&config) {
            Ok(_) => panic!("expected readonly exclusivity error"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("readonly must be exclusive"),
            "unexpected error: {}",
            err
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_register_data_file_for_restore_from_readonly_snapshot_dir_does_not_set_snapshot_link() {
        let root = "/tmp/file_manager_restore_readonly_snapshot_dir";
        let primary_root = format!("{}/primary", root);
        let snapshot_root = format!("{}/snapshot", root);
        let readonly_root = format!("{}/readonly", root);
        let _ = std::fs::remove_dir_all(root);
        let registry = FileSystemRegistry::new();
        let readonly_fs = registry
            .get_or_register(format!("file://{}", readonly_root))
            .unwrap();
        readonly_fs.create_dir("db").unwrap();
        readonly_fs.create_dir("db/snapshot").unwrap();
        let source_path = "db/snapshot/source.sst";
        let mut source_writer = readonly_fs.open_write(source_path).unwrap();
        source_writer.write(b"restore-source").unwrap();
        source_writer.close().unwrap();

        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}", primary_root),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}", snapshot_root),
                    vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}", readonly_root),
                    vec![VolumeUsageKind::Readonly],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-restore-readonly"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let source_full_path = format!("file://{}/{}", readonly_root, source_path);
        let restored = fm
            .register_data_file_for_restore(42, &source_full_path, None)
            .unwrap();
        assert!(fm.is_data_file_on_primary_volume(42));
        let restored_reader = fm.open_data_file_reader(42).unwrap();
        assert_eq!(
            &restored_reader.read_at(0, "restore-source".len()).unwrap()[..],
            b"restore-source"
        );
        assert_eq!(restored.snapshot_link_file_id, None);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_register_data_file_for_restore_sets_snapshot_link_on_snapshot_volume_any_path() {
        let root = "/tmp/file_manager_restore_snapshot_volume_any_path";
        let primary_root = format!("{}/primary", root);
        let snapshot_root = format!("{}/snapshot", root);
        let _ = std::fs::remove_dir_all(root);
        let registry = FileSystemRegistry::new();
        let snapshot_fs = registry
            .get_or_register(format!("file://{}", snapshot_root))
            .unwrap();
        snapshot_fs.create_dir("db").unwrap();
        snapshot_fs.create_dir("db/data").unwrap();
        let source_path = "db/data/source.sst";
        let mut source_writer = snapshot_fs.open_write(source_path).unwrap();
        source_writer.write(b"restore-source").unwrap();
        source_writer.close().unwrap();

        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}", primary_root),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}", snapshot_root),
                    vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-restore-snapshot-any"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let source_full_path = format!("file://{}/{}", snapshot_root, source_path);
        let restored = fm
            .register_data_file_for_restore(66, &source_full_path, None)
            .unwrap();
        assert!(fm.is_data_file_on_primary_volume(66));
        let snapshot_link_file_id = restored
            .snapshot_link_file_id
            .expect("snapshot link should be set");
        assert!(fm.is_data_file_on_snapshot_volume(snapshot_link_file_id));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_register_data_file_for_restore_shared_snapshot_volume_is_readonly() {
        let root = "/tmp/file_manager_restore_shared_snapshot_volume";
        let shared_root = format!("{}/shared", root);
        let _ = std::fs::remove_dir_all(root);
        std::fs::create_dir_all(format!("{}/db/data", shared_root)).unwrap();
        let source_local_path = format!("{}/db/data/source.sst", shared_root);
        std::fs::write(&source_local_path, b"restore-source").unwrap();

        let config = Config {
            volumes: vec![crate::VolumeDescriptor::new(
                format!("file://{}", shared_root),
                vec![
                    VolumeUsageKind::PrimaryDataPriorityHigh,
                    VolumeUsageKind::Snapshot,
                    VolumeUsageKind::Meta,
                ],
            )],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-restore-shared-snapshot"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let source_full_path = format!("file://{}/db/data/source.sst", shared_root);
        let restored = fm
            .register_data_file_for_restore(88, &source_full_path, None)
            .unwrap();
        assert_eq!(restored.snapshot_link_file_id, Some(88));
        fm.remove_data_file(88).unwrap();
        assert!(std::path::Path::new(&source_local_path).exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_register_data_file_for_restore_clears_snapshot_link_for_non_snapshot_dir() {
        let root = "/tmp/file_manager_restore_readonly_data_dir";
        let primary_root = format!("{}/primary", root);
        let snapshot_root = format!("{}/snapshot", root);
        let readonly_root = format!("{}/readonly", root);
        let _ = std::fs::remove_dir_all(root);
        let registry = FileSystemRegistry::new();
        let readonly_fs = registry
            .get_or_register(format!("file://{}", readonly_root))
            .unwrap();
        readonly_fs.create_dir("db").unwrap();
        readonly_fs.create_dir("db/data").unwrap();
        let source_path = "db/data/source.sst";
        let mut source_writer = readonly_fs.open_write(source_path).unwrap();
        source_writer.write(b"restore-source").unwrap();
        source_writer.close().unwrap();

        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}", primary_root),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}", snapshot_root),
                    vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}", readonly_root),
                    vec![VolumeUsageKind::Readonly],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-restore-readonly-data"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let source_full_path = format!("file://{}/{}", readonly_root, source_path);
        let restored = fm
            .register_data_file_for_restore(77, &source_full_path, None)
            .unwrap();
        assert!(fm.is_data_file_on_primary_volume(77));
        assert_eq!(restored.snapshot_link_file_id, None);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_register_data_file_for_restore_primary_volume_keeps_source_unowned() {
        let root = "/tmp/file_manager_restore_primary_source_unowned";
        let primary_root = format!("{}/primary", root);
        let snapshot_root = format!("{}/snapshot", root);
        let _ = std::fs::remove_dir_all(root);
        std::fs::create_dir_all(format!("{}/db/data", primary_root)).unwrap();
        let source_local_path = format!("{}/db/data/source.sst", primary_root);
        std::fs::write(&source_local_path, b"restore-source").unwrap();

        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}", primary_root),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}", snapshot_root),
                    vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-restore-primary-unowned"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let source_full_path = format!("file://{}/db/data/source.sst", primary_root);
        let restored = fm
            .register_data_file_for_restore(120, &source_full_path, None)
            .unwrap();
        assert_eq!(restored.snapshot_link_file_id, None);
        fm.remove_data_file(120).unwrap();
        assert!(std::path::Path::new(&source_local_path).exists());
        let _ = std::fs::remove_dir_all(root);
    }
}
