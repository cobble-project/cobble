use super::*;
use crate::Error;
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

struct NoopFileSystem;

impl FileSystem for NoopFileSystem {
    fn init(
        _url: &Url,
        _access_id: Option<String>,
        _access_key: Option<String>,
        _custom_options: Option<HashMap<String, String>>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self)
    }

    fn create_dir(&self, _path: &str) -> Result<()> {
        Ok(())
    }

    fn exists(&self, _path: &str) -> Result<bool> {
        Ok(true)
    }

    fn delete(&self, _path: &str) -> Result<()> {
        Ok(())
    }

    fn delete_async(&self, _path: &str) -> Result<()> {
        Ok(())
    }

    fn rename(&self, _from: &str, _to: &str) -> Result<()> {
        Ok(())
    }

    fn list(&self, _path: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    fn open_read(&self, _path: &str) -> Result<Box<dyn RandomAccessFile>> {
        Ok(Box::new(NoopRandomAccessFile))
    }

    fn open_write(&self, _path: &str) -> Result<Box<dyn SequentialWriteFile>> {
        Ok(Box::new(NoopSequentialWriteFile))
    }

    fn last_modified(&self, _path: &str) -> Result<Option<u64>> {
        Ok(None)
    }
}

struct NoopRandomAccessFile;

impl crate::file::File for NoopRandomAccessFile {
    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn size(&self) -> usize {
        0
    }
}

impl RandomAccessFile for NoopRandomAccessFile {
    fn read_at(&self, _offset: usize, _size: usize) -> Result<Bytes, Error> {
        Ok(Bytes::new())
    }
}

struct NoopSequentialWriteFile;

impl crate::file::File for NoopSequentialWriteFile {
    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn size(&self) -> usize {
        0
    }
}

impl SequentialWriteFile for NoopSequentialWriteFile {
    fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        Ok(data.len())
    }
}

struct NoopProcessRegistry {
    calls: AtomicUsize,
}

impl ProcessFileSystemRegistry for NoopProcessRegistry {
    fn try_init(&self, _request: &ProcessFileSystemRequest) -> Result<Option<Arc<dyn FileSystem>>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(Some(Arc::new(NoopFileSystem)))
    }
}

#[test]
fn test_filesystem_registry() {
    let registry = FileSystemRegistry::new();
    let fs1 = registry.get_or_register("file:///tmp/checkpoint");
    assert!(fs1.is_ok());
    let fs2 = registry.get_or_register("file:///tmp/checkpoint");
    assert!(fs2.is_ok());
    assert!(Arc::ptr_eq(&fs1.unwrap(), &fs2.unwrap()));
}

#[test]
fn test_filesystem_registry_normalizes_absolute_local_path() {
    let registry = FileSystemRegistry::new();
    let path = std::env::temp_dir().join("cobble-filesystem-registry-normalize");
    let path_str = path.to_string_lossy().to_string();
    let url = url::Url::from_file_path(PathBuf::from(&path_str))
        .expect("absolute local path should convert to file URL")
        .to_string();
    let from_path = registry.get_or_register(&path_str).unwrap();
    let from_url = registry.get_or_register(&url).unwrap();
    assert!(Arc::ptr_eq(&from_path, &from_url));
}

#[test]
fn test_filesystem_registry_falls_back_to_process_registry_on_builtin_failure() {
    clear_process_custom_file_system_registry();
    let process_registry = Arc::new(NoopProcessRegistry {
        calls: AtomicUsize::new(0),
    });
    register_process_custom_file_system_registry(process_registry.clone());
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register("mockfs:///tmp/cobble-fallback");
    assert!(fs.is_ok());
    assert_eq!(process_registry.calls.load(Ordering::Relaxed), 1);
    clear_process_custom_file_system_registry();
}
