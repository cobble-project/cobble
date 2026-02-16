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
use crate::config::VolumeUsageKind;
use crate::error::{Error, Result};
use crate::file::file_system::{FileSystem, FileSystemRegistry};
use crate::file::files::{File, RandomAccessFile, SequentialWriteFile};
use crate::lru::LruCache;
use crate::metrics_manager::MetricsManager;
use bytes::Bytes;
use dashmap::DashMap;
use metrics::{Gauge, gauge};
use rand::random;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use uuid::Uuid;

const DATA_DIR: &str = "data";
const SNAPSHOT_DIR: &str = "snapshot";
const DEFAULT_BASE_FILE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_READER_CACHE_CAPACITY: usize = 512;

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
enum VolumePriority {
    High,
    Medium,
    Low,
}

impl VolumePriority {
    fn rank(self) -> u8 {
        match self {
            VolumePriority::High => 3,
            VolumePriority::Medium => 2,
            VolumePriority::Low => 1,
        }
    }
}

struct VolumeState {
    fs: Arc<dyn FileSystem>,
    base_dir: Option<String>,
    size_limit: Option<u64>,
    used_bytes: AtomicU64,
}

impl VolumeState {
    fn new(fs: Arc<dyn FileSystem>, size_limit: Option<u64>, base_dir: Option<String>) -> Self {
        let base_dir = base_dir.map(|dir| dir.trim_end_matches('/').to_string());
        Self {
            fs,
            base_dir,
            size_limit,
            used_bytes: AtomicU64::new(0),
        }
    }

    fn base_dir(&self) -> Option<&str> {
        self.base_dir.as_deref()
    }

    fn add_usage(&self, bytes: u64) {
        self.used_bytes.fetch_add(bytes, Ordering::SeqCst);
    }

    fn subtract_usage(&self, bytes: u64) {
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

    fn is_full(&self, base_file_size: u64) -> bool {
        let Some(limit) = self.size_limit else {
            return false;
        };
        let threshold = limit.saturating_sub(base_file_size);
        let used = self.used_bytes.load(Ordering::SeqCst);
        used >= threshold
    }
}

#[derive(Clone)]
pub(crate) struct DataVolume {
    state: Arc<VolumeState>,
    priority: VolumePriority,
}

/// A unique identifier for data files managed by the FileManager.
pub type FileId = u64;

/// Configuration options for the FileManager.
pub struct FileManagerOptions {
    /// Base directory for file storage (relative to the file system root).
    pub base_dir: String,
    /// File extension for data files (e.g., "sst").
    pub data_file_extension: String,
    /// Base SST file size used for volume threshold calculations.
    pub base_file_size: usize,
}

impl Default for FileManagerOptions {
    fn default() -> Self {
        Self {
            base_dir: "".to_string(),
            data_file_extension: "sst".to_string(),
            base_file_size: DEFAULT_BASE_FILE_SIZE,
        }
    }
}

/// Information about a tracked file.
pub struct TrackedFile {
    /// The path to the file relative to the file system root.
    path: String,
    /// The underlying file system (needed for deletion on drop).
    fs: Arc<dyn FileSystem>,
    /// Optional volume usage tracker.
    volume: Option<Arc<VolumeState>>,
    /// Bytes tracked for this file.
    size_bytes: AtomicU64,
    /// Whether to delete the file when this TrackedFile is dropped.
    delete_on_drop: AtomicBool,
    /// Count of explicit references to this file (e.g., from snapshots).
    explicit_refs: AtomicU32,
}

impl TrackedFile {
    fn fs(&self) -> &Arc<dyn FileSystem> {
        &self.fs
    }
    /// Creates a new TrackedFile.
    fn new(path: String, fs: Arc<dyn FileSystem>, volume: Option<Arc<VolumeState>>) -> Self {
        Self {
            path,
            fs,
            volume,
            size_bytes: AtomicU64::new(0),
            delete_on_drop: AtomicBool::new(true),
            explicit_refs: AtomicU32::new(0),
        }
    }

    /// Creates a new TrackedFile that never deletes on drop.
    fn readonly(path: String, fs: Arc<dyn FileSystem>, volume: Option<Arc<VolumeState>>) -> Self {
        Self {
            path,
            fs,
            volume,
            size_bytes: AtomicU64::new(0),
            delete_on_drop: AtomicBool::new(false),
            explicit_refs: AtomicU32::new(0),
        }
    }

    /// Marks the file for deletion when this TrackedFile is dropped.
    pub fn mark_for_deletion(&self) {
        self.delete_on_drop.store(true, Ordering::SeqCst);
    }

    pub fn mark_for_retention(&self) {
        self.delete_on_drop.store(false, Ordering::SeqCst);
    }

    /// Returns true if the file is marked for deletion on drop.
    pub fn is_marked_for_deletion(&self) -> bool {
        self.delete_on_drop.load(Ordering::SeqCst)
    }

    /// Returns the path to the file.
    pub fn path(&self) -> &str {
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

    pub fn detached(file_id: FileId) -> Arc<Self> {
        Arc::new(Self {
            file_id,
            file_manager: Weak::new(),
        })
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
    volume: Option<Arc<VolumeState>>,
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
        volume: Option<Arc<VolumeState>>,
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
}

impl FileManagerMetrics {
    pub(crate) fn new(db_id: &str) -> Self {
        let db_id = db_id.to_string();
        Self {
            data_files_tracked: gauge!("data_files_tracked", "db_id" => db_id.clone()),
            metadata_files_tracked: gauge!("metadata_files_tracked", "db_id" => db_id),
        }
    }
}

/// File manager for managing files in a KV storage engine.
///
/// The FileManager is responsible for managing both metadata files and data files.
/// It provides file ID assignment, reader caching, and file lifecycle management.
pub struct FileManager {
    metrics: FileManagerMetrics,
    /// The metadata volume for metadata files.
    meta_volume: DataVolume,
    /// Ordered data volumes by priority (high to low).
    data_volumes: Vec<DataVolume>,
    /// Configuration options.
    options: FileManagerOptions,
    /// Counter for generating unique file IDs.
    next_file_id: AtomicU64,
    /// Map of file ID to tracked file information for data files.
    data_files: DashMap<FileId, Arc<TrackedFile>>,
    /// Map of filename to tracked file information for metadata files.
    metadata_files: Arc<DashMap<String, Arc<TrackedFile>>>,
    /// LRU cache for open random access readers.
    reader_cache: Mutex<LruCache<FileId, Arc<dyn RandomAccessFile>>>,
}

impl FileManager {
    fn sort_data_volumes(mut volumes: Vec<DataVolume>) -> Vec<DataVolume> {
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
        Ok(())
    }

    fn select_data_volume(&self) -> Result<&DataVolume> {
        let base_file_size = self.options.base_file_size as u64;
        let mut selected: Option<&DataVolume> = None;
        let mut candidates: Vec<&DataVolume> = Vec::with_capacity(self.data_volumes.len());
        for volume in &self.data_volumes {
            if !candidates.is_empty() && volume.priority.rank() < candidates[0].priority.rank() {
                break;
            }
            if volume.state.is_full(base_file_size) {
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

    fn test_existence_for_path(&self, path: &str) -> Result<&DataVolume> {
        for volume in &self.data_volumes {
            if volume.state.fs.exists(path)? {
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
    fn resolve_volume_path<'a>(&self, path: &'a str) -> Result<(&DataVolume, &'a str)> {
        for volume in &self.data_volumes {
            let Some(base_dir) = volume.state.base_dir() else {
                continue;
            };
            if path.starts_with(base_dir) {
                let relative = self.trim_volume_base_dir(path, base_dir);
                return Ok((volume, relative));
            }
        }
        let volume = self.test_existence_for_path(path)?;
        Ok((volume, path))
    }
    /// Creates a new FileManager with the given data volumes and options.
    ///
    /// This will create the data and snapshot directories if they don't exist.
    pub fn new(
        data_volumes: Vec<DataVolume>,
        options: FileManagerOptions,
        metrics_manager: Arc<MetricsManager>,
    ) -> Result<Self> {
        if data_volumes.is_empty() {
            return Err(Error::ConfigError(
                "No data volumes configured for FileManager".to_string(),
            ));
        }
        let data_volumes = Self::sort_data_volumes(data_volumes);
        for volume in &data_volumes {
            Self::ensure_volume_dirs(&volume.state.fs, &options)?;
        }
        let meta_volume = DataVolume {
            state: Arc::clone(&data_volumes[0].state),
            priority: data_volumes[0].priority,
        };
        Ok(Self {
            metrics: metrics_manager.file_manager_metrics(),
            meta_volume,
            data_volumes,
            options,
            next_file_id: AtomicU64::new(1), // Start from 1, 0 is reserved
            data_files: DashMap::new(),
            metadata_files: Arc::new(DashMap::new()),
            reader_cache: Mutex::new(LruCache::new(DEFAULT_READER_CACHE_CAPACITY)),
        })
    }

    /// Creates a new FileManager with default options.
    pub fn with_defaults(
        fs: Arc<dyn FileSystem>,
        metrics_manager: Arc<MetricsManager>,
    ) -> Result<Self> {
        let volume = DataVolume {
            state: Arc::new(VolumeState::new(fs, None, None)),
            priority: VolumePriority::High,
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
        let mut data_volumes = Vec::new();
        for volume in &volumes {
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
            if let Some(priority) = priority {
                let fs = registry.get_or_register_volume(volume)?;
                data_volumes.push(DataVolume {
                    state: Arc::new(VolumeState::new(
                        fs,
                        volume.size_limit,
                        Some(volume.base_dir.clone()),
                    )),
                    priority,
                });
            }
        }
        if data_volumes.is_empty() {
            return Err(Error::ConfigError(
                "No volume configured for primary data storage".to_string(),
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
        let path = self.data_file_path(file_id);

        // Track the file
        let volume = self.select_data_volume()?;
        let tracked = Arc::new(TrackedFile::new(
            path,
            Arc::clone(&volume.state.fs),
            Some(Arc::clone(&volume.state)),
        ));
        self.data_files.insert(file_id, Arc::clone(&tracked));
        self.report_data_files_gauge();
        let writer = volume.state.fs.open_write(tracked.path())?;

        Ok((file_id, TrackedWriter::new(writer, tracked)))
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

        // Track the file
        let volume = self.select_data_volume()?;
        let tracked = Arc::new(TrackedFile::new(
            self.data_file_path(file_id),
            Arc::clone(&volume.state.fs),
            Some(Arc::clone(&volume.state)),
        ));
        self.data_files.insert(file_id, Arc::clone(&tracked));
        self.report_data_files_gauge();
        let writer = volume.state.fs.open_write(tracked.path())?;

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

        Ok(TrackedWriter::new(writer, tracked))
    }

    /// Registers an existing data file with the FileManager.
    ///
    /// This is useful when recovering files from disk or when files were
    /// created externally. The file is tracked but no reader is opened.
    pub fn register_data_file(&self, file_id: FileId, path: String) -> Result<()> {
        let (volume, relative_path) = self.resolve_volume_path(&path)?;
        let fs = Arc::clone(&volume.state.fs);

        // Track the file if not already tracked
        {
            let tracked = self.data_files.entry(file_id).or_insert_with(|| {
                Arc::new(TrackedFile::new(
                    relative_path.to_string(),
                    Arc::clone(&fs),
                    Some(Arc::clone(&volume.state)),
                ))
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

    /// Marks a data file as read-only, preventing it from being deleted on drop.
    pub(crate) fn make_data_file_readonly(&self, file_id: FileId) -> Result<()> {
        let tracked = self
            .data_files
            .get(&file_id)
            .ok_or_else(|| Error::IoError(format!("Data file {} is not tracked", file_id)))?;
        tracked.mark_for_retention();
        Ok(())
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
        self.data_files.remove(&file_id);
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
        let writer = self.meta_volume.state.fs.open_write(&temp_path)?;

        let tracked = Arc::new(TrackedFile::new(
            temp_path.clone(),
            Arc::clone(&self.meta_volume.state.fs),
            Some(Arc::clone(&self.meta_volume.state)),
        ));
        let tracked_writer = TrackedWriter::new(writer, tracked);
        Ok(AtomicMetadataWriter::new(
            temp_path,
            name.to_string(),
            final_path,
            tracked_writer,
            Arc::clone(&self.meta_volume.state.fs),
            Arc::clone(&self.metadata_files),
            self.metrics.metadata_files_tracked.clone(),
            Some(Arc::clone(&self.meta_volume.state)),
        ))
    }

    /// Registers an existing metadata file with the FileManager.
    pub fn register_metadata_file(&self, name: &str, path: String) -> Result<()> {
        // Verify the file exists
        if !self.meta_volume.state.fs.exists(&path)? {
            return Err(Error::IoError(format!(
                "Metadata file {} does not exist at path: {}",
                name, path
            )));
        }

        // Track the file if not already tracked
        if !self.metadata_files.contains_key(name) {
            let tracked = Arc::new(TrackedFile::new(
                path,
                Arc::clone(&self.meta_volume.state.fs),
                Some(Arc::clone(&self.meta_volume.state)),
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

        let reader = self.meta_volume.state.fs.open_read(tracked.path())?;
        Ok(TrackedReader::new(reader, Arc::clone(&tracked)))
    }

    /// Opens a metadata file for reading without tracking it.
    pub fn open_metadata_file_reader_untracked(
        &self,
        name: &str,
    ) -> Result<Box<dyn RandomAccessFile>> {
        let path = self.metadata_file_path(name);
        self.meta_volume.state.fs.open_read(&path)
    }

    /// Registers an existing data file without deleting it on drop.
    pub fn register_data_file_readonly(&self, file_id: FileId, path: String) -> Result<()> {
        let (volume, relative_path) = self.resolve_volume_path(&path)?;
        let fs = Arc::clone(&volume.state.fs);
        let tracked = Arc::new(TrackedFile::readonly(
            relative_path.to_string(),
            Arc::clone(&fs),
            Some(Arc::clone(&volume.state)),
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
                self.meta_volume.state.fs.delete(tracked.path())?;
            }
            self.report_metadata_files_gauge();
            return Ok(());
        }
        let path = self.metadata_file_path(name);
        if self.meta_volume.state.fs.exists(&path)? {
            self.meta_volume.state.fs.delete(&path)?;
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
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-test".to_string()));
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
        fm.register_data_file(999, path.to_string()).unwrap();
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
            state: Arc::new(VolumeState::new(
                Arc::clone(&high_fs),
                Some(128),
                Some(format!("{}/high", root)),
            )),
            priority: VolumePriority::High,
        };
        let low_volume = DataVolume {
            state: Arc::new(VolumeState::new(
                Arc::clone(&low_fs),
                None,
                Some(format!("{}/low", root)),
            )),
            priority: VolumePriority::Low,
        };
        let mut options = FileManagerOptions::default();
        options.base_dir = "db".to_string();
        options.base_file_size = 64;
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-test".to_string()));
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
}
