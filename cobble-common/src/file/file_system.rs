use crate::VolumeDescriptor;
use crate::error::Result;
use crate::file::files::{RandomAccessFile, SequentialWriteFile};
use crate::file::opendal_fs::OpendalFileSystem;
use dashmap::DashMap;
use std::sync::Arc;
use url::Url;

pub trait FileSystem: Send + Sync {
    fn init(url: &Url, access_id: Option<String>, access_key: Option<String>) -> Result<Self>
    where
        Self: Sized;

    fn create_dir(&self, path: &str) -> Result<()>;

    fn exists(&self, path: &str) -> Result<bool>;

    fn delete(&self, path: &str) -> Result<()>;

    fn delete_async(&self, path: &str) -> Result<()>;

    fn rename(&self, from: &str, to: &str) -> Result<()>;

    fn open_read(&self, path: &str) -> Result<Box<dyn RandomAccessFile>>;

    fn open_write(&self, path: &str) -> Result<Box<dyn SequentialWriteFile>>;

    fn last_modified(&self, path: &str) -> Result<Option<u64>>;
}

pub struct FileSystemRegistry {
    registered: DashMap<String, Arc<dyn FileSystem>>,
}

impl FileSystemRegistry {
    pub fn new() -> Self {
        Self {
            registered: DashMap::new(),
        }
    }

    pub fn get_or_register(&self, name: String) -> Result<Arc<dyn FileSystem>> {
        let url = Url::parse(name.as_str())?;
        let name = url.to_string();
        if let Some(fs) = self.registered.get(&name) {
            return Ok(Arc::clone(&fs));
        }

        let fs: Arc<dyn FileSystem> = Arc::new(OpendalFileSystem::init(&url, None, None)?);
        self.registered.insert(name.clone(), Arc::clone(&fs));
        Ok(fs)
    }

    pub fn get_or_register_volume(&self, volume: &VolumeDescriptor) -> Result<Arc<dyn FileSystem>> {
        let url = Url::parse(volume.base_dir.as_str())?;
        let name = url.to_string();
        if let Some(fs) = self.registered.get(&name) {
            return Ok(Arc::clone(&fs));
        }

        let fs: Arc<dyn FileSystem> = Arc::new(OpendalFileSystem::init(
            &url,
            volume.access_id.clone(),
            volume.secret_key.clone(),
        )?);
        self.registered.insert(name.clone(), Arc::clone(&fs));
        Ok(fs)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_filesystem_registry() {
        let registry = FileSystemRegistry::new();
        let fs1 = registry.get_or_register("file:///tmp/checkpoint".to_string());
        assert!(fs1.is_ok());
        let fs2 = registry.get_or_register("file:///tmp/checkpoint".to_string());
        assert!(fs2.is_ok());
        assert!(Arc::ptr_eq(&fs1.unwrap(), &fs2.unwrap()));
    }
}
