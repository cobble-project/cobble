use crate::VolumeDescriptor;
use crate::error::Result;
use crate::file::files::{RandomAccessFile, SequentialWriteFile};
use crate::file::opendal_fs::OpendalFileSystem;
#[cfg(unix)]
use crate::file::posix_fs::PosixFileSystem;
use crate::util::normalize_storage_path_to_url;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

pub trait FileSystem: Send + Sync {
    fn init(
        url: &Url,
        access_id: Option<String>,
        access_key: Option<String>,
        custom_options: Option<HashMap<String, String>>,
    ) -> Result<Self>
    where
        Self: Sized;

    fn create_dir(&self, path: &str) -> Result<()>;

    fn exists(&self, path: &str) -> Result<bool>;

    fn delete(&self, path: &str) -> Result<()>;

    fn delete_async(&self, path: &str) -> Result<()>;

    fn rename(&self, from: &str, to: &str) -> Result<()>;

    fn list(&self, path: &str) -> Result<Vec<String>>;

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

    pub fn get_or_register(&self, name: impl AsRef<str>) -> Result<Arc<dyn FileSystem>> {
        let normalized = normalize_storage_path_to_url(name.as_ref())?;
        let url = Url::parse(&normalized)?;
        let name = url.to_string();
        if let Some(fs) = self.registered.get(&name) {
            return Ok(Arc::clone(&fs));
        }

        #[cfg(unix)]
        if url.scheme().eq_ignore_ascii_case("file") {
            let fs: Arc<dyn FileSystem> = Arc::new(PosixFileSystem::init(&url, None, None, None)?);
            self.registered.insert(name.clone(), Arc::clone(&fs));
            return Ok(fs);
        }

        let fs: Arc<dyn FileSystem> = Arc::new(OpendalFileSystem::init(&url, None, None, None)?);
        self.registered.insert(name.clone(), Arc::clone(&fs));
        Ok(fs)
    }

    pub fn get_or_register_volume(&self, volume: &VolumeDescriptor) -> Result<Arc<dyn FileSystem>> {
        let normalized = normalize_storage_path_to_url(&volume.base_dir)?;
        let url = Url::parse(&normalized)?;
        let name = url.to_string();
        if let Some(fs) = self.registered.get(&name) {
            return Ok(Arc::clone(&fs));
        }

        #[cfg(unix)]
        if url.scheme().eq_ignore_ascii_case("file") {
            let fs: Arc<dyn FileSystem> = Arc::new(PosixFileSystem::init(
                &url,
                volume.access_id.clone(),
                volume.secret_key.clone(),
                volume.custom_options.clone(),
            )?);
            self.registered.insert(name.clone(), Arc::clone(&fs));
            return Ok(fs);
        }

        let fs: Arc<dyn FileSystem> = Arc::new(OpendalFileSystem::init(
            &url,
            volume.access_id.clone(),
            volume.secret_key.clone(),
            volume.custom_options.clone(),
        )?);
        self.registered.insert(name.clone(), Arc::clone(&fs));
        Ok(fs)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::PathBuf;
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
}
