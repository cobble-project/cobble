use crate::error::{Error, Result};
use crate::file::file_system::FileSystem;
use crate::file::files::{File, RandomAccessFile, SequentialWriteFile};
use bytes::Bytes;
use positioned_io::ReadAt;
use std::collections::HashMap;
use std::fs::{File as StdFile, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock, mpsc};
use std::time::UNIX_EPOCH;
use url::Url;

pub struct PosixFileSystem {
    root: PathBuf,
}

impl PosixFileSystem {
    fn resolve_path(&self, path: &str) -> PathBuf {
        let path = path.trim_start_matches('/');
        self.root.join(path)
    }
}

impl FileSystem for PosixFileSystem {
    fn init(
        _url: &Url,
        _access_id: Option<String>,
        _access_key: Option<String>,
        _custom_options: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        Ok(Self {
            root: Path::new(_url.path()).to_path_buf(),
        })
    }

    fn create_dir(&self, path: &str) -> Result<()> {
        let resolved = self.resolve_path(path);
        std::fs::create_dir_all(&resolved).map_err(|e| {
            Error::IoError(format!(
                "Failed to create dir {}: {}",
                resolved.display(),
                e
            ))
        })
    }

    fn exists(&self, path: &str) -> Result<bool> {
        let resolved = self.resolve_path(path);
        Ok(resolved.exists())
    }

    fn delete(&self, path: &str) -> Result<()> {
        let resolved = self.resolve_path(path);
        if !resolved.exists() {
            return Ok(());
        }
        if resolved.is_dir() {
            std::fs::remove_dir_all(&resolved).map_err(|e| {
                Error::IoError(format!("Failed to delete {}: {}", resolved.display(), e))
            })
        } else {
            std::fs::remove_file(&resolved).map_err(|e| {
                Error::IoError(format!("Failed to delete {}: {}", resolved.display(), e))
            })
        }
    }

    fn delete_async(&self, path: &str) -> Result<()> {
        let resolved = self.resolve_path(path);
        // Use a background worker to perform the delete operation to avoid blocking the caller
        // thread, especially when deleting large directories. If the worker is not available,
        // fallback to synchronous delete.
        static DELETE_WORKER: OnceLock<Mutex<Option<mpsc::Sender<PathBuf>>>> = OnceLock::new();
        let sender = {
            let lock = DELETE_WORKER.get_or_init(|| Mutex::new(None));
            let mut guard = lock
                .lock()
                .map_err(|_| Error::IoError("Delete worker lock poisoned".to_string()))?;
            // Spawn the worker thread if it hasn't been spawned yet.
            if guard.is_none() {
                let (tx, rx) = mpsc::channel::<PathBuf>();
                std::thread::Builder::new()
                    .name("posix-fs-delete".to_string())
                    .spawn(move || {
                        for path in rx {
                            let _ = if path.is_dir() {
                                std::fs::remove_dir_all(&path)
                            } else {
                                std::fs::remove_file(&path)
                            };
                        }
                    })
                    .map_err(|e| {
                        Error::IoError(format!("Failed to spawn posix delete worker: {}", e))
                    })?;
                *guard = Some(tx);
            }
            guard.as_ref().cloned()
        };
        if let Some(sender) = sender {
            if let Err(err) = sender.send(resolved) {
                let resolved = err.0;
                let _ = if resolved.is_dir() {
                    std::fs::remove_dir_all(&resolved)
                } else {
                    std::fs::remove_file(&resolved)
                };
            }
        } else {
            let _ = if resolved.is_dir() {
                std::fs::remove_dir_all(&resolved)
            } else {
                std::fs::remove_file(&resolved)
            };
        }
        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        let from_path = self.resolve_path(from);
        let to_path = self.resolve_path(to);
        if let Some(parent) = to_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        std::fs::rename(&from_path, &to_path)
            .map_err(|e| Error::IoError(format!("Failed to rename {} to {}: {}", from, to, e)))
    }

    fn list(&self, path: &str) -> Result<Vec<String>> {
        let resolved = self.resolve_path(path);
        if !resolved.exists() {
            return Ok(Vec::new());
        }
        let mut items = Vec::new();
        for entry in std::fs::read_dir(&resolved)
            .map_err(|e| Error::IoError(format!("Failed to list {}: {}", resolved.display(), e)))?
        {
            let entry = entry.map_err(|e| {
                Error::IoError(format!(
                    "Failed to read entry in {}: {}",
                    resolved.display(),
                    e
                ))
            })?;
            let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) else {
                continue;
            };
            items.push(name);
        }
        Ok(items)
    }

    fn open_read(&self, path: &str) -> Result<Box<dyn RandomAccessFile>> {
        let resolved = self.resolve_path(path);
        let file = StdFile::open(&resolved)
            .map_err(|e| Error::IoError(format!("Failed to open {}: {}", resolved.display(), e)))?;
        let size = file
            .metadata()
            .map_err(|e| Error::IoError(format!("Failed to stat {}: {}", resolved.display(), e)))?
            .len() as usize;
        Ok(Box::new(PosixRandomAccessFile { file, size }))
    }

    fn open_write(&self, path: &str) -> Result<Box<dyn SequentialWriteFile>> {
        let resolved = self.resolve_path(path);
        if let Some(parent) = resolved.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                Error::IoError(format!(
                    "Failed to create parent dir {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&resolved)
            .map_err(|e| Error::IoError(format!("Failed to open {}: {}", resolved.display(), e)))?;
        Ok(Box::new(PosixSequentialWriteFile { file, size: 0 }))
    }

    fn last_modified(&self, path: &str) -> Result<Option<u64>> {
        let resolved = self.resolve_path(path);
        if !resolved.exists() {
            return Ok(None);
        }
        let metadata = resolved
            .metadata()
            .map_err(|e| Error::IoError(format!("Failed to stat {}: {}", resolved.display(), e)))?;
        let modified = metadata.modified().map_err(|e| {
            Error::IoError(format!(
                "Failed to read mtime for {}: {}",
                resolved.display(),
                e
            ))
        })?;
        Ok(modified
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs()))
    }
}

struct PosixRandomAccessFile {
    file: StdFile,
    size: usize,
}

impl File for PosixRandomAccessFile {
    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn size(&self) -> usize {
        self.size
    }
}

impl RandomAccessFile for PosixRandomAccessFile {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error> {
        if size == 0 {
            return Ok(Bytes::new());
        }
        let mut buffer = vec![0u8; size];
        self.file
            .read_exact_at(offset as u64, &mut buffer)
            .map_err(|e| {
                Error::IoError(format!(
                    "Failed to read at offset {} size {}: {}",
                    offset, size, e
                ))
            })?;
        Ok(Bytes::from(buffer))
    }
}

struct PosixSequentialWriteFile {
    file: StdFile,
    size: usize,
}

impl File for PosixSequentialWriteFile {
    fn close(&mut self) -> Result<(), Error> {
        self.file
            .sync_all()
            .map_err(|e| Error::IoError(format!("Failed to sync file: {}", e)))
    }

    fn size(&self) -> usize {
        self.size
    }
}

impl SequentialWriteFile for PosixSequentialWriteFile {
    fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        let written = self
            .file
            .write(data)
            .map_err(|e| Error::IoError(format!("Failed to write data: {}", e)))?;
        self.size = self.size.saturating_add(written);
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FileSystem;

    static TEST_ROOT: &str = "file:///tmp/posix_fs_test";

    fn cleanup_test_root() {
        let _ = std::fs::remove_dir_all("/tmp/posix_fs_test");
    }

    #[test]
    #[serial_test::serial(file)]
    #[cfg(unix)]
    fn test_posix_fs_basic() {
        cleanup_test_root();
        let fs = PosixFileSystem::init(&Url::parse(TEST_ROOT).unwrap(), None, None, None).unwrap();
        assert!(!fs.exists("example").unwrap());
        fs.create_dir("example").unwrap();
        assert!(fs.exists("example").unwrap());
        fs.rename("example", "renamed").unwrap();
        assert!(!fs.exists("example").unwrap());
        assert!(fs.exists("renamed").unwrap());
        fs.delete("renamed").unwrap();
        assert!(!fs.exists("renamed").unwrap());
        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    #[cfg(unix)]
    fn test_posix_fs_read_write_and_mtime() {
        cleanup_test_root();
        let fs = PosixFileSystem::init(&Url::parse(TEST_ROOT).unwrap(), None, None, None).unwrap();
        assert_eq!(fs.last_modified("example").unwrap(), None);
        let data = b"Hello, Cobble!";
        {
            let mut writer = fs.open_write("example").unwrap();
            let written = writer.write(data).unwrap();
            assert_eq!(written, data.len());
            writer.close().unwrap();
        }
        assert!(fs.last_modified("example").unwrap().is_some());
        {
            let reader = fs.open_read("example").unwrap();
            let read = reader.read_at(0, data.len()).unwrap();
            assert_eq!(&read[..], data);
        }
        fs.delete("example").unwrap();
        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    #[cfg(unix)]
    fn test_posix_fs_list() {
        cleanup_test_root();
        let fs = PosixFileSystem::init(&Url::parse(TEST_ROOT).unwrap(), None, None, None).unwrap();
        fs.create_dir("list/subdir").unwrap();
        {
            let mut writer = fs.open_write("list/a.txt").unwrap();
            writer.write(b"a").unwrap();
            writer.close().unwrap();
        }
        {
            let mut writer = fs.open_write("list/b.txt").unwrap();
            writer.write(b"b").unwrap();
            writer.close().unwrap();
        }

        let mut listed = fs.list("list").unwrap();
        listed.sort();
        assert_eq!(
            listed,
            vec![
                "a.txt".to_string(),
                "b.txt".to_string(),
                "subdir".to_string()
            ]
        );
        assert!(fs.list("missing").unwrap().is_empty());
        cleanup_test_root();
    }
}
