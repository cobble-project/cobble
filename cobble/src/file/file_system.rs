use crate::VolumeDescriptor;
use crate::error::{Error, Result};
use crate::file::files::{RandomAccessFile, SequentialWriteFile};
use crate::file::opendal_fs::OpendalFileSystem;
#[cfg(unix)]
use crate::file::posix_fs::PosixFileSystem;
use crate::util::normalize_storage_path_to_url;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
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

#[derive(Clone, Debug)]
pub struct ProcessFileSystemRequest {
    pub base_dir: String,
    pub normalized_base_dir: Option<String>,
    pub access_id: Option<String>,
    pub secret_key: Option<String>,
    pub custom_options: Option<HashMap<String, String>>,
    pub builtin_error: Error,
}

pub trait ProcessFileSystemRegistry: Send + Sync {
    fn try_init(&self, request: &ProcessFileSystemRequest) -> Result<Option<Arc<dyn FileSystem>>>;
}

static PROCESS_CUSTOM_FILE_SYSTEM_REGISTRY: OnceLock<
    RwLock<Option<Arc<dyn ProcessFileSystemRegistry>>>,
> = OnceLock::new();

pub fn register_process_custom_file_system_registry(registry: Arc<dyn ProcessFileSystemRegistry>) {
    let lock = PROCESS_CUSTOM_FILE_SYSTEM_REGISTRY.get_or_init(|| RwLock::new(None));
    if let Ok(mut guard) = lock.write() {
        *guard = Some(registry);
    }
}

pub fn clear_process_custom_file_system_registry() {
    let Some(lock) = PROCESS_CUSTOM_FILE_SYSTEM_REGISTRY.get() else {
        return;
    };
    if let Ok(mut guard) = lock.write() {
        *guard = None;
    }
}

fn get_process_custom_file_system_registry() -> Option<Arc<dyn ProcessFileSystemRegistry>> {
    PROCESS_CUSTOM_FILE_SYSTEM_REGISTRY
        .get()
        .and_then(|lock| lock.read().ok().and_then(|guard| guard.clone()))
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
        let volume = VolumeDescriptor {
            base_dir: name.as_ref().to_string(),
            ..VolumeDescriptor::default()
        };
        self.get_or_register_volume(&volume)
    }

    pub fn get_or_register_volume(&self, volume: &VolumeDescriptor) -> Result<Arc<dyn FileSystem>> {
        let base_dir = volume.base_dir.clone();
        let normalized = match normalize_storage_path_to_url(&base_dir) {
            Ok(v) => v,
            Err(err) => {
                return self.try_custom_registry(ProcessFileSystemRequest {
                    base_dir: base_dir.clone(),
                    normalized_base_dir: None,
                    access_id: volume.access_id.clone(),
                    secret_key: volume.secret_key.clone(),
                    custom_options: volume.custom_options.clone(),
                    builtin_error: err,
                });
            }
        };
        let url = match Url::parse(&normalized) {
            Ok(v) => v,
            Err(err) => {
                return self.try_custom_registry(ProcessFileSystemRequest {
                    base_dir: base_dir.clone(),
                    normalized_base_dir: Some(normalized),
                    access_id: volume.access_id.clone(),
                    secret_key: volume.secret_key.clone(),
                    custom_options: volume.custom_options.clone(),
                    builtin_error: Error::from(err),
                });
            }
        };
        let cache_key = url.to_string();
        if let Some(fs) = self.registered.get(&cache_key) {
            return Ok(Arc::clone(&fs));
        }
        let custom_registry = get_process_custom_file_system_registry();
        let builtin_result = self.init_builtin_file_system(
            &url,
            volume.access_id.clone(),
            volume.secret_key.clone(),
            volume.custom_options.clone(),
        );
        match builtin_result {
            Ok(fs) => {
                let probe = fs.exists("");
                if custom_registry.is_none() || probe.is_ok() {
                    self.registered.insert(cache_key, Arc::clone(&fs));
                    return Ok(fs);
                }
                let probe_err = probe.err().unwrap_or_else(|| {
                    Error::FileSystemError("builtin file system access probe failed".to_string())
                });
                self.try_custom_registry_with(
                    custom_registry,
                    cache_key,
                    ProcessFileSystemRequest {
                        base_dir,
                        normalized_base_dir: Some(normalized),
                        access_id: volume.access_id.clone(),
                        secret_key: volume.secret_key.clone(),
                        custom_options: volume.custom_options.clone(),
                        builtin_error: probe_err,
                    },
                )
            }
            Err(err) => self.try_custom_registry_with(
                custom_registry,
                cache_key,
                ProcessFileSystemRequest {
                    base_dir,
                    normalized_base_dir: Some(normalized),
                    access_id: volume.access_id.clone(),
                    secret_key: volume.secret_key.clone(),
                    custom_options: volume.custom_options.clone(),
                    builtin_error: err,
                },
            ),
        }
    }

    fn init_builtin_file_system(
        &self,
        url: &Url,
        access_id: Option<String>,
        secret_key: Option<String>,
        custom_options: Option<HashMap<String, String>>,
    ) -> Result<Arc<dyn FileSystem>> {
        #[cfg(unix)]
        if url.scheme().eq_ignore_ascii_case("file") {
            let fs: Arc<dyn FileSystem> = Arc::new(PosixFileSystem::init(
                url,
                access_id,
                secret_key,
                custom_options,
            )?);
            return Ok(fs);
        }
        let fs: Arc<dyn FileSystem> = Arc::new(OpendalFileSystem::init(
            url,
            access_id,
            secret_key,
            custom_options,
        )?);
        Ok(fs)
    }

    fn try_custom_registry(
        &self,
        request: ProcessFileSystemRequest,
    ) -> Result<Arc<dyn FileSystem>> {
        let cache_key = request
            .normalized_base_dir
            .clone()
            .unwrap_or_else(|| request.base_dir.clone());
        self.try_custom_registry_with(
            get_process_custom_file_system_registry(),
            cache_key,
            request,
        )
    }

    fn try_custom_registry_with(
        &self,
        registry: Option<Arc<dyn ProcessFileSystemRegistry>>,
        cache_key: String,
        request: ProcessFileSystemRequest,
    ) -> Result<Arc<dyn FileSystem>> {
        let Some(registry) = registry else {
            return Err(request.builtin_error);
        };
        let maybe_fs = registry.try_init(&request)?;
        let Some(fs) = maybe_fs else {
            return Err(request.builtin_error);
        };
        self.registered.insert(cache_key, Arc::clone(&fs));
        Ok(fs)
    }
}

#[cfg(test)]
mod test {
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
        fn try_init(
            &self,
            _request: &ProcessFileSystemRequest,
        ) -> Result<Option<Arc<dyn FileSystem>>> {
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
}
