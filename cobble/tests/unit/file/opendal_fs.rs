use super::*;
use ::opendal::raw::{
    OpCopier, OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite,
    RpCreateDir, RpPresign, RpRead, RpRename, RpStat, Service, ServiceInfo, oio,
};
use ::opendal::{Buffer, BytesRange, Capability, ErrorKind, OperationContext, Operator};
use std::collections::HashMap;
use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEST_ROOT: &str = "file:///tmp/checkpoint";

fn cleanup_test_root() {
    let _ = std::fs::remove_dir_all("/tmp/checkpoint");
}

#[test]
fn test_local_fs_root_uses_native_platform_path() {
    let path = std::env::temp_dir().join("cobble local fs root");
    let url = Url::from_file_path(&path).expect("temporary directory must form a file URL");
    let root = local_fs_root(&url).expect("file URL must convert back to a local root");
    assert_eq!(std::path::PathBuf::from(root), path);
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
    assert_eq!(fs.file_size("example").unwrap(), Some(data.len() as u64));
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
    let fs = OpendalFileSystem::init(&Url::parse(TEST_ROOT).unwrap(), None, None, None).unwrap();
    fs.create_dir("list/subdir").unwrap();
    assert_eq!(fs.file_size("list/subdir").unwrap(), None);
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
    let url =
        Url::parse("s3://127.0.0.1:9000/cobble-test/prefix?endpoint_scheme=http&region=us-east-1")
            .unwrap();
    let mut query: HashMap<String, String> = url.query_pairs().into_owned().collect();
    let (bucket, root, endpoint) = resolve_s3_bucket_root_endpoint(&url, &mut query).unwrap();
    assert_eq!(bucket, "cobble-test");
    assert_eq!(root, "/prefix");
    assert_eq!(endpoint.as_deref(), Some("http://127.0.0.1:9000"));
    assert_eq!(query.get("region").map(String::as_str), Some("us-east-1"));
}

#[test]
fn test_resolve_goosefs_master_addr_from_host_and_port() {
    let url = Url::parse("goosefs://10.0.0.1:9200/cobble-data").unwrap();
    let mut query = HashMap::new();
    let (master_addr, root) = resolve_goosefs_master_addr_root(&url, &mut query);
    assert_eq!(master_addr.as_deref(), Some("10.0.0.1:9200"));
    assert_eq!(root, "/cobble-data");
}

#[test]
fn test_resolve_goosefs_master_addr_default_port() {
    let url = Url::parse("goosefs://10.0.0.1/data").unwrap();
    let mut query = HashMap::new();
    let (master_addr, root) = resolve_goosefs_master_addr_root(&url, &mut query);
    assert_eq!(master_addr.as_deref(), Some("10.0.0.1:9200"));
    assert_eq!(root, "/data");
}

#[test]
fn test_resolve_goosefs_master_addr_ha_query_override() {
    let url =
        Url::parse("goosefs://ignored-host/?master_addr=10.0.0.1:9200,10.0.0.2:9200&root=/shared")
            .unwrap();
    let mut query: HashMap<String, String> = url.query_pairs().into_owned().collect();
    let (master_addr, root) = resolve_goosefs_master_addr_root(&url, &mut query);
    assert_eq!(master_addr.as_deref(), Some("10.0.0.1:9200,10.0.0.2:9200"));
    assert_eq!(root, "/shared");
}

#[derive(Debug, Clone)]
struct HangingThenReadyService {
    reads: Arc<AtomicUsize>,
}

fn unsupported<T>(op: &str) -> ::opendal::Result<T> {
    Err(::opendal::Error::new(
        ErrorKind::Unsupported,
        format!("{op} is not supported"),
    ))
}

impl Service for HangingThenReadyService {
    type Reader = HangingThenReadyReader;
    type Writer = ();
    type Lister = ();
    type Deleter = ();
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("mock")
    }

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> ::opendal::Result<RpCreateDir> {
        unsupported("create_dir")
    }

    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> ::opendal::Result<RpStat> {
        unsupported("stat")
    }

    fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> ::opendal::Result<Self::Reader> {
        Ok(HangingThenReadyReader {
            reads: Arc::clone(&self.reads),
        })
    }

    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> ::opendal::Result<Self::Writer> {
        unsupported("write")
    }

    fn delete(&self, _: &OperationContext) -> ::opendal::Result<Self::Deleter> {
        unsupported("delete")
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> ::opendal::Result<Self::Lister> {
        unsupported("list")
    }

    fn copy(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpCopy,
        _: OpCopier,
    ) -> ::opendal::Result<Self::Copier> {
        unsupported("copy")
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> ::opendal::Result<RpRename> {
        unsupported("rename")
    }

    async fn presign(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpPresign,
    ) -> ::opendal::Result<RpPresign> {
        unsupported("presign")
    }
}

#[derive(Debug, Clone)]
struct HangingThenReadyReader {
    reads: Arc<AtomicUsize>,
}

struct HangingThenReadyStream {
    reads: Arc<AtomicUsize>,
}

impl HangingThenReadyStream {
    async fn next_buffer(&self) -> ::opendal::Result<Buffer> {
        match self.reads.fetch_add(1, Ordering::SeqCst) {
            0 => pending::<::opendal::Result<Buffer>>().await,
            1 => Ok(Buffer::from("recovered")),
            _ => Ok(Buffer::new()),
        }
    }
}

impl oio::ReadStream for HangingThenReadyStream {
    async fn read(&mut self) -> ::opendal::Result<Buffer> {
        self.next_buffer().await
    }
}

impl oio::Read for HangingThenReadyReader {
    async fn open(
        &self,
        _: BytesRange,
    ) -> ::opendal::Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        Ok((
            RpRead::default(),
            Box::new(HangingThenReadyStream {
                reads: Arc::clone(&self.reads),
            }),
        ))
    }

    async fn read(&self, _: BytesRange) -> ::opendal::Result<(RpRead, Buffer)> {
        let stream = HangingThenReadyStream {
            reads: Arc::clone(&self.reads),
        };
        Ok((RpRead::default(), stream.next_buffer().await?))
    }
}

#[tokio::test]
async fn remote_timeout_is_retried_after_hanging_io() {
    let reads = Arc::new(AtomicUsize::new(0));
    let op = layer_remote_operator(
        Operator::from_parts(
            OperationContext::default(),
            Arc::new(HangingThenReadyService {
                reads: Arc::clone(&reads),
            }),
        ),
        Duration::from_millis(20),
        Duration::from_millis(20),
    );

    let value = op.read("hanging").await.expect("retry after timeout");
    assert_eq!(value.to_bytes().as_ref(), b"recovered");
    assert!(
        reads.load(Ordering::SeqCst) >= 2,
        "timeout should cancel the hanging read and retry"
    );
}
