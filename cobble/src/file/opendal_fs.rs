use crate::error::{Error, Result};
use crate::file::file_system::FileSystem;
use crate::file::files::{RandomAccessFile, SequentialWriteFile};
use crate::file::opendal_file::{OpendalRandomAccessFile, OpendalSequentialWriteFile};
use opendal::layers::RetryLayer;
use opendal::{ErrorKind, Operator, Scheme};
use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

pub struct OpendalFileSystem {
    pub(crate) op: Operator,
    runtime: Arc<tokio::runtime::Runtime>,
}

fn normalize_root(path: &str) -> String {
    if path.is_empty() || path == "/" {
        return "/".to_string();
    }
    let rooted = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    };
    if rooted.len() > 1 {
        rooted.trim_end_matches('/').to_string()
    } else {
        rooted
    }
}

fn split_bucket_and_root_from_path(path: &str) -> Option<(String, String)> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    let mut parts = trimmed.splitn(2, '/');
    let bucket = parts.next()?.to_string();
    let root = match parts.next() {
        Some(rest) if !rest.is_empty() => normalize_root(rest),
        _ => "/".to_string(),
    };
    Some((bucket, root))
}

fn host_looks_like_endpoint(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost") || host.parse::<IpAddr>().is_ok()
}

fn resolve_s3_bucket_root_endpoint(
    url: &Url,
    query_options: &mut HashMap<String, String>,
) -> Result<(String, String, Option<String>)> {
    let endpoint_scheme = query_options
        .remove("endpoint_scheme")
        .unwrap_or_else(|| "https".to_string());
    let endpoint_from_query = query_options.remove("endpoint");

    let host = url.host_str().unwrap_or("");
    let path_bucket_root = split_bucket_and_root_from_path(url.path());
    let host_is_endpoint =
        !host.is_empty() && (url.port().is_some() || host_looks_like_endpoint(host));

    let default_bucket = if host_is_endpoint {
        path_bucket_root.as_ref().map(|(bucket, _)| bucket.clone())
    } else if !host.is_empty() {
        Some(host.to_string())
    } else {
        path_bucket_root.as_ref().map(|(bucket, _)| bucket.clone())
    };
    let bucket = query_options
        .remove("bucket")
        .or(default_bucket)
        .ok_or_else(|| {
            Error::ConfigError(format!(
                "S3 base_dir '{}' must include a bucket (for example: s3://my-bucket/prefix or s3://127.0.0.1:9000/my-bucket/prefix?endpoint_scheme=http)",
                url
            ))
        })?;

    let default_root = if host_is_endpoint {
        path_bucket_root
            .as_ref()
            .map(|(_, root)| root.clone())
            .unwrap_or_else(|| "/".to_string())
    } else {
        normalize_root(url.path())
    };
    let root = normalize_root(&query_options.remove("root").unwrap_or(default_root));

    let endpoint = endpoint_from_query.or_else(|| {
        if !host_is_endpoint {
            return None;
        }
        let port = url.port().map(|p| format!(":{p}")).unwrap_or_default();
        Some(format!("{endpoint_scheme}://{host}{port}"))
    });

    Ok((bucket, root, endpoint))
}

impl FileSystem for OpendalFileSystem {
    fn init(
        url: &Url,
        access_id: Option<String>,
        access_key: Option<String>,
        custom_options: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        let scheme = Scheme::from_str(match url.scheme().to_lowercase().as_str() {
            "file" => "fs",
            other => other,
        })
        .map_err(|e| {
            Error::FileSystemError(format!(
                "Unsupported scheme '{}': {}. Enable the matching cobble storage feature (for example: storage-s3, storage-oss, storage-cos, storage-alluxio).",
                url.scheme(),
                e
            ))
        })?;
        let mut query_options: HashMap<String, String> = url.query_pairs().into_owned().collect();
        let mut options: HashMap<String, String> = HashMap::new();
        match scheme {
            Scheme::Fs => {
                // For local fs, we only need to set the root path
                options.insert("root".to_string(), url.path().to_string());
            }
            Scheme::S3 => {
                let (bucket, root, endpoint) =
                    resolve_s3_bucket_root_endpoint(url, &mut query_options)?;
                options.insert("bucket".to_string(), bucket);
                options.insert("root".to_string(), root);
                if let Some(endpoint) = endpoint {
                    options.insert("endpoint".to_string(), endpoint);
                    if !query_options.contains_key("enable_virtual_host_style") {
                        options
                            .insert("enable_virtual_host_style".to_string(), "false".to_string());
                    }
                }

                let access_key_id = access_id
                    .or_else(|| query_options.remove("access_key_id"))
                    .or_else(|| query_options.remove("access_id"));
                let secret_access_key = access_key
                    .or_else(|| query_options.remove("secret_access_key"))
                    .or_else(|| query_options.remove("access_key"));
                if let Some(access_key_id) = access_key_id
                    && !access_key_id.is_empty()
                {
                    options.insert("access_key_id".to_string(), access_key_id);
                }
                if let Some(secret_access_key) = secret_access_key
                    && !secret_access_key.is_empty()
                {
                    options.insert("secret_access_key".to_string(), secret_access_key);
                }
            }
            _ => {
                // Other schemes: preserve previous host/path-based behavior.
                let host = url.host_str().unwrap_or("");
                let access_id = access_id.unwrap_or_else(|| url.username().to_string());
                let access_key =
                    access_key.unwrap_or_else(|| url.password().unwrap_or("").to_string());
                let bucket = url.path().trim_start_matches('/');
                options.insert("host".to_string(), host.to_string());
                if !access_id.is_empty() {
                    options.insert("access_key_id".to_string(), access_id);
                }
                if !access_key.is_empty() {
                    options.insert("secret_access_key".to_string(), access_key);
                }
                if !bucket.is_empty() {
                    options.insert("bucket".to_string(), bucket.to_string());
                }
                options.insert("root".to_string(), normalize_root(url.path()));
            }
        }
        options.extend(query_options);
        if let Some(custom_options) = custom_options {
            options.extend(custom_options);
        }
        let op = Operator::via_iter(scheme, options)
            .map_err(|e| {
                Error::FileSystemError(format!("Failed to create opendal operator: {}", e))
            })?
            .layer(
                RetryLayer::new()
                    .with_jitter()
                    .with_min_delay(Duration::from_millis(50))
                    .with_max_delay(Duration::from_secs(2))
                    .with_max_times(6),
            );
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

    fn delete_async(&self, path: &str) -> Result<()> {
        let path = path.to_string();
        let op = self.op.clone();
        self.runtime.spawn(async move {
            let _ = op.delete(&path).await;
        });
        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.runtime.block_on(async {
            match self.op.rename(from, to).await {
                Ok(()) => Ok(()),
                Err(rename_err) if rename_err.kind() == ErrorKind::Unsupported => {
                    self.op.copy(from, to).await.map_err(|copy_err| {
                        Error::IoError(format!(
                            "Failed to copy {} to {} during rename fallback: {}",
                            from, to, copy_err
                        ))
                    })?;
                    self.op.delete(from).await.map_err(|delete_err| {
                        Error::IoError(format!(
                            "Failed to delete {} after rename fallback copy to {}: {}",
                            from, to, delete_err
                        ))
                    })?;
                    Ok(())
                }
                Err(rename_err) => Err(Error::IoError(format!(
                    "Failed to rename {} to {}: {}",
                    from, to, rename_err
                ))),
            }
        })
    }

    fn list(&self, path: &str) -> Result<Vec<String>> {
        let path = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{}/", path)
        };
        let entries = self
            .runtime
            .block_on(async { self.op.list(&path).await })
            .map_err(|e| Error::IoError(format!("Failed to list {}: {}", path, e)))?;
        Ok(entries
            .into_iter()
            .filter_map(|entry| {
                let name = entry.name().trim_end_matches('/');
                (!name.is_empty()).then(|| name.to_string())
            })
            .collect())
    }

    fn open_read(&self, path: &str) -> Result<Box<dyn RandomAccessFile>> {
        self.runtime.block_on(async {
            // Get file metadata to retrieve size
            let metadata = self.op.stat(path).await.map_err(|e| {
                Error::IoError(format!("Failed to get metadata for {}: {}", path, e))
            })?;

            let file_size = metadata.content_length() as usize;

            let reader = self.op.reader(path).await;
            match reader {
                Ok(r) => Ok(Box::new(OpendalRandomAccessFile {
                    size: file_size,
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
                    size: 0, // New file starts at size 0
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

    fn last_modified(&self, path: &str) -> Result<Option<u64>> {
        self.runtime
            .block_on(async { self.op.stat(path).await })
            .map(|meta| meta.last_modified().map(|ts| ts.timestamp() as u64))
            .map_err(|e| Error::IoError(format!("Failed to stat {}: {}", path, e)))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    static TEST_ROOT: &str = "file:///tmp/checkpoint";

    fn cleanup_test_root() {
        let _ = std::fs::remove_dir_all("/tmp/checkpoint");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_opendal_fs_basic() {
        cleanup_test_root();
        let fs = OpendalFileSystem::init(&Url::parse(TEST_ROOT).unwrap(), None, None, None);
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
        let fs = OpendalFileSystem::init(&Url::parse(TEST_ROOT).unwrap(), None, None, None);
        assert!(fs.is_ok());
        let fs = fs.unwrap();
        assert!(!fs.exists("example").unwrap());
        let data = b"Hello, Cobble!";
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

    #[test]
    #[serial_test::serial(file)]
    fn test_opendal_fs_list() {
        cleanup_test_root();
        let fs =
            OpendalFileSystem::init(&Url::parse(TEST_ROOT).unwrap(), None, None, None).unwrap();
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

        let mut listed: Vec<String> = fs
            .list("list")
            .unwrap()
            .into_iter()
            .map(|name| name.trim_start_matches("list/").to_string())
            .collect();
        listed.sort();
        assert!(listed.contains(&"a.txt".to_string()));
        assert!(listed.contains(&"b.txt".to_string()));
        assert!(listed.contains(&"subdir".to_string()));
        cleanup_test_root();
    }

    #[test]
    fn test_resolve_s3_bucket_root_endpoint_aws_style() {
        let url = Url::parse("s3://my-bucket/data/prefix").unwrap();
        let mut query = HashMap::new();
        let (bucket, root, endpoint) = resolve_s3_bucket_root_endpoint(&url, &mut query).unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(root, "/data/prefix");
        assert!(endpoint.is_none());
    }

    #[test]
    fn test_resolve_s3_bucket_root_endpoint_local_endpoint_style() {
        let url = Url::parse(
            "s3://127.0.0.1:9000/cobble-test/prefix?endpoint_scheme=http&region=us-east-1",
        )
        .unwrap();
        let mut query: HashMap<String, String> = url.query_pairs().into_owned().collect();
        let (bucket, root, endpoint) = resolve_s3_bucket_root_endpoint(&url, &mut query).unwrap();
        assert_eq!(bucket, "cobble-test");
        assert_eq!(root, "/prefix");
        assert_eq!(endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(query.get("region").map(String::as_str), Some("us-east-1"));
    }
}
