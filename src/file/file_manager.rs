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

use crate::error::{Error, Result};
use crate::file::file_system::FileSystem;
use crate::file::files::{File, RandomAccessFile, SequentialWriteFile};
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use uuid::Uuid;

/// A unique identifier for data files managed by the FileManager.
pub type FileId = u64;

/// Configuration options for the FileManager.
pub struct FileManagerOptions {
    /// Directory path for data files (relative to the file system root).
    pub data_dir: String,
    /// Directory path for metadata files (relative to the file system root).
    pub metadata_dir: String,
    /// File extension for data files (e.g., "sst").
    pub data_file_extension: String,
}

impl Default for FileManagerOptions {
    fn default() -> Self {
        Self {
            data_dir: "data".to_string(),
            metadata_dir: "meta".to_string(),
            data_file_extension: "sst".to_string(),
        }
    }
}

/// Information about a tracked file.
pub struct TrackedFile {
    /// The path to the file relative to the file system root.
    path: String,
    /// The underlying file system (needed for deletion on drop).
    fs: Arc<dyn FileSystem>,
    /// Whether to delete the file when this TrackedFile is dropped.
    delete_on_drop: AtomicBool,
    /// Count of explicit references to this file (e.g., from snapshots).
    explicit_refs: AtomicU32,
}

impl TrackedFile {
    /// Creates a new TrackedFile.
    fn new(path: String, fs: Arc<dyn FileSystem>) -> Self {
        Self {
            path,
            fs,
            delete_on_drop: AtomicBool::new(true),
            explicit_refs: AtomicU32::new(0),
        }
    }

    /// Marks the file for deletion when this TrackedFile is dropped.
    pub fn mark_for_deletion(&self) {
        self.delete_on_drop.store(true, Ordering::SeqCst);
    }

    /// Returns true if the file is marked for deletion on drop.
    pub fn is_marked_for_deletion(&self) -> bool {
        self.delete_on_drop.load(Ordering::SeqCst)
    }

    /// Returns the path to the file.
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn reference(&self) {
        self.explicit_refs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dereference(&self) {
        self.explicit_refs.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for TrackedFile {
    fn drop(&mut self) {
        if self.delete_on_drop.load(Ordering::SeqCst)
            && self.explicit_refs.load(Ordering::SeqCst) == 0
        {
            // Attempt to delete the file, ignore errors
            let _ = self.fs.delete(&self.path);
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
    _tracked: Arc<TrackedFile>,
}

impl TrackedWriter {
    /// Creates a new TrackedWriter.
    pub fn new(inner: Box<dyn SequentialWriteFile>, tracked: Arc<TrackedFile>) -> Self {
        Self {
            inner,
            _tracked: tracked,
        }
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
        self.inner.write(data)
    }
}

pub struct AtomicMetadataWriter {
    temp_path: String,
    final_name: String,
    final_path: String,
    writer: Option<TrackedWriter>,
    fs: Arc<dyn FileSystem>,
    metadata_files: Arc<DashMap<String, Arc<TrackedFile>>>,
}

impl AtomicMetadataWriter {
    fn new(
        temp_path: String,
        final_name: String,
        final_path: String,
        writer: TrackedWriter,
        fs: Arc<dyn FileSystem>,
        metadata_files: Arc<DashMap<String, Arc<TrackedFile>>>,
    ) -> Self {
        Self {
            temp_path,
            final_name,
            final_path,
            writer: Some(writer),
            fs,
            metadata_files,
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
        ));
        self.metadata_files
            .insert(self.final_name.clone(), Arc::clone(&tracked));
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

/// File manager for managing files in a KV storage engine.
///
/// The FileManager is responsible for managing both metadata files and data files.
/// It provides file ID assignment, reader caching, and file lifecycle management.
pub struct FileManager {
    /// The underlying file system.
    fs: Arc<dyn FileSystem>,
    /// Configuration options.
    options: FileManagerOptions,
    /// Counter for generating unique file IDs.
    next_file_id: AtomicU64,
    /// Map of file ID to tracked file information for data files.
    data_files: DashMap<FileId, Arc<TrackedFile>>,
    /// Map of filename to tracked file information for metadata files.
    metadata_files: Arc<DashMap<String, Arc<TrackedFile>>>,
}

impl FileManager {
    /// Creates a new FileManager with the given file system and options.
    ///
    /// This will create the data and metadata directories if they don't exist.
    pub fn new(fs: Arc<dyn FileSystem>, options: FileManagerOptions) -> Result<Self> {
        // Create directories if they don't exist
        if !fs.exists(&options.data_dir)? {
            fs.create_dir(&options.data_dir)?;
        }
        if !fs.exists(&options.metadata_dir)? {
            fs.create_dir(&options.metadata_dir)?;
        }

        Ok(Self {
            fs,
            options,
            next_file_id: AtomicU64::new(1), // Start from 1, 0 is reserved
            data_files: DashMap::new(),
            metadata_files: Arc::new(DashMap::new()),
        })
    }

    /// Creates a new FileManager with default options.
    pub fn with_defaults(fs: Arc<dyn FileSystem>) -> Result<Self> {
        Self::new(fs, FileManagerOptions::default())
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

    /// Generates a new unique file ID.
    fn allocate_file_id(&self) -> FileId {
        self.next_file_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Generates the path for a data file with the given ID.
    fn data_file_path(&self, file_id: FileId) -> String {
        format!(
            "{}/{}.{}",
            self.options.data_dir, file_id, self.options.data_file_extension
        )
    }

    /// Generates the path for a metadata file with the given name.
    fn metadata_file_path(&self, name: &str) -> String {
        format!("{}/{}", self.options.metadata_dir, name)
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

        let writer = self.fs.open_write(&path)?;

        // Track the file
        let tracked = Arc::new(TrackedFile::new(path, Arc::clone(&self.fs)));
        self.data_files.insert(file_id, Arc::clone(&tracked));

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

        let path = self.data_file_path(file_id);
        let writer = self.fs.open_write(&path)?;

        // Track the file
        let tracked = Arc::new(TrackedFile::new(path, Arc::clone(&self.fs)));
        self.data_files.insert(file_id, Arc::clone(&tracked));

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
        // Verify the file exists
        if !self.fs.exists(&path)? {
            return Err(Error::IoError(format!(
                "Data file {} does not exist at path: {}",
                file_id, path
            )));
        }

        // Track the file if not already tracked
        if !self.data_files.contains_key(&file_id) {
            let tracked = Arc::new(TrackedFile::new(path, Arc::clone(&self.fs)));
            self.data_files.insert(file_id, tracked);
        }

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

        let reader = self.fs.open_read(tracked.path())?;
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

    /// Returns the path for a metadata file.
    pub fn get_metadata_file_path(&self, name: &str) -> Option<String> {
        self.metadata_files.get(name).map(|f| f.path().to_string())
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
    pub fn remove_data_file(&self, file_id: FileId) -> Result<()> {
        self.data_files.remove(&file_id);
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
        let writer = self.fs.open_write(&temp_path)?;

        let tracked = Arc::new(TrackedFile::new(temp_path.clone(), Arc::clone(&self.fs)));
        let tracked_writer = TrackedWriter::new(writer, tracked);
        Ok(AtomicMetadataWriter::new(
            temp_path,
            name.to_string(),
            final_path,
            tracked_writer,
            Arc::clone(&self.fs),
            Arc::clone(&self.metadata_files),
        ))
    }

    /// Registers an existing metadata file with the FileManager.
    pub fn register_metadata_file(&self, name: &str, path: String) -> Result<()> {
        // Verify the file exists
        if !self.fs.exists(&path)? {
            return Err(Error::IoError(format!(
                "Metadata file {} does not exist at path: {}",
                name, path
            )));
        }

        // Track the file if not already tracked
        if !self.metadata_files.contains_key(name) {
            let tracked = Arc::new(TrackedFile::new(path, Arc::clone(&self.fs)));
            self.metadata_files.insert(name.to_string(), tracked);
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

        let reader = self.fs.open_read(tracked.path())?;
        Ok(TrackedReader::new(reader, Arc::clone(&tracked)))
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
        }
    }

    /// Removes a metadata file from tracking and optionally deletes it from disk.
    pub fn remove_metadata_file(&self, name: &str) -> Result<()> {
        if let Some((_, tracked)) = self.metadata_files.remove(name) {
            if Arc::strong_count(&tracked) > 1 {
                tracked.mark_for_deletion();
            } else {
                self.fs.delete(tracked.path())?;
            }
            return Ok(());
        }
        let path = self.metadata_file_path(name);
        if self.fs.exists(&path)? {
            self.fs.delete(&path)?;
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FileSystemRegistry;
    use crate::file::files::File;

    static TEST_ROOT: &str = "file:///tmp/file_manager_test";

    fn cleanup_test_root() {
        let _ = std::fs::remove_dir_all("/tmp/file_manager_test");
    }

    fn create_test_file_manager() -> (Arc<dyn FileSystem>, FileManager) {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(TEST_ROOT.to_string()).unwrap();
        let fm = FileManager::with_defaults(Arc::clone(&fs)).unwrap();
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
}
