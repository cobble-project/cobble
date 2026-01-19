use crate::error::{Error, Result};
use crate::file::FileHandle;
use crate::file::file_system::FileSystem;
use crate::file::files::{RandomAccessFile, SequentialWriteFile};
use crate::file::opendal_file::{OpendalRandomAccessFile, OpendalSequentialWriteFile};
use opendal::{Operator, Scheme};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

pub struct OpendalFileSystem {
    pub(crate) op: Operator,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl FileSystem for OpendalFileSystem {
    fn init(url: &Url) -> Result<Self> {
        let scheme = Scheme::from_str(match url.scheme().to_lowercase().as_str() {
            "file" => "fs",
            other => other,
        })
        .map_err(|e| Error::FileSystemError(format!("Unsupported scheme: {}", e)))?;
        // other options from url
        let mut options: HashMap<String, String> = HashMap::new();
        match scheme {
            Scheme::Fs => {
                // For local fs, we only need to set the root path
                options.insert("root".to_string(), url.path().to_string());
            }
            _ => {
                /* other schemes will be handled below */
                let host = url.host_str().unwrap_or("");
                let access_id = url.username();
                let access_key = url.password().unwrap_or("");
                let bucket = url.path().trim_start_matches('/');
                options.insert("host".to_string(), host.to_string());
                options.insert("access_id".to_string(), access_id.to_string());
                options.insert("access_key".to_string(), access_key.to_string());
                options.insert("bucket".to_string(), bucket.to_string());
                options.insert("root".to_string(), url.path().to_string());
            }
        }
        let op = Operator::via_iter(scheme, options).map_err(|e| {
            Error::FileSystemError(format!("Failed to create opendal operator: {}", e))
        })?;
        // Here we would create a concrete implementation of FileSystem using `op`
        Ok(OpendalFileSystem {
            op,
            runtime: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    Error::FileSystemError(format!("Failed to create tokio runtime: {}", e))
                })?
                .into(),
        })
    }

    fn create_dir(&self, path: &str) -> Result<()> {
        let path = if path.ends_with('/') {
            path
        } else {
            &format!("{}/", path)
        };
        self.runtime
            .block_on(async { self.op.create_dir(path).await })
            .map_err(|e| Error::IoError(format!("Failed to create directory {}: {}", path, e)))
    }

    fn exists(&self, path: &str) -> Result<bool> {
        self.runtime
            .block_on(async { self.op.exists(path).await })
            .map_err(|e| Error::IoError(format!("Failed to create directory {}: {}", path, e)))
    }

    fn delete(&self, path: &str) -> Result<()> {
        self.runtime
            .block_on(async { self.op.delete(path).await })
            .map_err(|e| Error::IoError(format!("Failed to create directory {}: {}", path, e)))
    }

    fn open_read(&self, path: &str) -> Result<Box<dyn RandomAccessFile>> {
        self.runtime.block_on(async {
            // Get file metadata to retrieve size
            let metadata = self.op.stat(path).await.map_err(|e| {
                Error::IoError(format!("Failed to get metadata for {}: {}", path, e))
            })?;
            
            let file_size = metadata.content_length();
            
            let reader = self.op.reader(path).await;
            match reader {
                Ok(r) => Ok(Box::new(OpendalRandomAccessFile {
                    handle: FileHandle { 
                        id: 1, 
                        size: file_size,
                    },
                    reader: r,
                    runtime: Arc::clone(&self.runtime),
                }) as Box<dyn RandomAccessFile>),
                Err(e) => Err(Error::IoError(format!(
                    "Failed to create reader for {}: {}",
                    path, e
                ))),
            }
        })
    }

    fn open_write(&self, path: &str) -> Result<Box<dyn SequentialWriteFile>> {
        self.runtime.block_on(async {
            let writer = self.op.writer(path).await;
            match writer {
                Ok(w) => Ok(Box::new(OpendalSequentialWriteFile {
                    handle: FileHandle { 
                        id: 1,
                        size: 0,  // New file starts at size 0
                    },
                    writer: w,
                    runtime: Arc::clone(&self.runtime),
                }) as Box<dyn SequentialWriteFile>),
                Err(e) => Err(Error::IoError(format!(
                    "Failed to create writer for {}: {}",
                    path, e
                ))),
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    static TEST_ROOT: &str = "file:///tmp/checkpoint";

    fn cleanup_test_root() {
        let _ = std::fs::remove_dir_all("/tmp/checkpoint");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_opendal_fs_basic() {
        cleanup_test_root();
        let fs = OpendalFileSystem::init(&Url::parse(TEST_ROOT).unwrap());
        assert!(fs.is_ok());
        let fs = fs.unwrap();
        assert!(!fs.exists("example").unwrap());
        assert!(fs.create_dir("example").is_ok());
        assert!(fs.exists("example").unwrap());
        assert!(fs.delete("example").is_ok());
        assert!(!fs.exists("example").unwrap());
        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_opendal_read_write() {
        cleanup_test_root();
        let fs = OpendalFileSystem::init(&Url::parse(TEST_ROOT).unwrap());
        assert!(fs.is_ok());
        let fs = fs.unwrap();
        assert!(!fs.exists("example").unwrap());
        let data = b"Hello, Stella!";
        {
            let mut writer = fs.open_write("example").unwrap();
            let written = writer.write(data).unwrap();
            assert_eq!(written, data.len());
            writer.close().unwrap();
        }
        {
            let reader = fs.open_read("example").unwrap();
            let read = reader.read_at(0, data.len()).unwrap();
            assert_eq!(&read[..], data);
        }
        cleanup_test_root();
    }
}
