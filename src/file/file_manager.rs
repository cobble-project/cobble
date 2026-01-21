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
use crate::file::files::{RandomAccessFile, SequentialWriteFile};
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
struct TrackedFile {
    /// The path to the file relative to the file system root.
    path: String,
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
    data_files: DashMap<FileId, TrackedFile>,
    /// Map of filename to tracked file information for metadata files.
    metadata_files: DashMap<String, TrackedFile>,
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
            metadata_files: DashMap::new(),
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
    /// along with a writer.
    ///
    /// The file is tracked by the FileManager and can be opened for reading
    /// later using `open_data_file_reader`.
    pub fn create_data_file(&self) -> Result<(FileId, Box<dyn SequentialWriteFile>)> {
        let file_id = self.allocate_file_id();
        let path = self.data_file_path(file_id);

        let writer = self.fs.open_write(&path)?;

        // Track the file
        self.data_files.insert(file_id, TrackedFile { path });

        Ok((file_id, writer))
    }

    /// Creates a new data file with a specific file ID.
    ///
    /// This is useful when recovering files or when the ID is known in advance.
    /// Returns an error if the file ID is already in use.
    pub fn create_data_file_with_id(
        &self,
        file_id: FileId,
    ) -> Result<Box<dyn SequentialWriteFile>> {
        if self.data_files.contains_key(&file_id) {
            return Err(Error::IoError(format!(
                "File ID {} is already in use",
                file_id
            )));
        }

        let path = self.data_file_path(file_id);
        let writer = self.fs.open_write(&path)?;

        // Track the file
        self.data_files.insert(file_id, TrackedFile { path });

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
            self.data_files.insert(file_id, TrackedFile { path });
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
    /// A new reader is created each time this method is called.
    pub fn open_data_file_reader(&self, file_id: FileId) -> Result<Box<dyn RandomAccessFile>> {
        // Get the tracked file to read the path
        let tracked = self.data_files.get(&file_id).ok_or_else(|| {
            Error::IoError(format!(
                "Data file {} is not tracked by FileManager",
                file_id
            ))
        })?;

        self.fs.open_read(&tracked.path)
    }

    /// Returns the path for a data file.
    pub fn get_data_file_path(&self, file_id: FileId) -> Option<String> {
        self.data_files.get(&file_id).map(|f| f.path.clone())
    }

    /// Checks if a data file is tracked by the FileManager.
    pub fn has_data_file(&self, file_id: FileId) -> bool {
        self.data_files.contains_key(&file_id)
    }

    /// Removes a data file from tracking and optionally deletes it from disk.
    ///
    /// If `delete_from_disk` is true, the file is also deleted from the file system.
    pub fn remove_data_file(&self, file_id: FileId, delete_from_disk: bool) -> Result<()> {
        if let Some((_, tracked)) = self.data_files.remove(&file_id)
            && delete_from_disk
        {
            self.fs.delete(&tracked.path)?;
        }
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
    pub fn create_metadata_file(&self, name: &str) -> Result<Box<dyn SequentialWriteFile>> {
        let path = self.metadata_file_path(name);
        let writer = self.fs.open_write(&path)?;

        // Track the file
        self.metadata_files
            .insert(name.to_string(), TrackedFile { path });

        Ok(writer)
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
            self.metadata_files
                .insert(name.to_string(), TrackedFile { path });
        }

        Ok(())
    }

    /// Opens a metadata file for reading.
    pub fn open_metadata_file_reader(&self, name: &str) -> Result<Box<dyn RandomAccessFile>> {
        // Get the tracked file to read the path
        let tracked = self.metadata_files.get(name).ok_or_else(|| {
            Error::IoError(format!(
                "Metadata file {} is not tracked by FileManager",
                name
            ))
        })?;

        self.fs.open_read(&tracked.path)
    }

    /// Checks if a metadata file exists.
    pub fn has_metadata_file(&self, name: &str) -> bool {
        self.metadata_files.contains_key(name)
    }

    /// Removes a metadata file from tracking and optionally deletes it from disk.
    pub fn remove_metadata_file(&self, name: &str, delete_from_disk: bool) -> Result<()> {
        if let Some((_, tracked)) = self.metadata_files.remove(name)
            && delete_from_disk
        {
            self.fs.delete(&tracked.path)?;
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

        let path = fm.get_data_file_path(file_id).unwrap();
        assert!(fs.exists(&path).unwrap());

        // Remove with delete
        fm.remove_data_file(file_id, true).unwrap();
        assert!(!fm.has_data_file(file_id));
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
        fm.remove_metadata_file("manifest", true).unwrap();
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
