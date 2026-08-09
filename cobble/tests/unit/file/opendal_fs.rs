use super::*;
use ::opendal::raw::{Access, AccessorInfo, OpRead, RpRead, oio};
use ::opendal::{Buffer, Capability, OperatorBuilder};
use std::collections::HashMap;
use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};

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
    let fs = OpendalFileSystem::init(&Url::parse(TEST_ROOT).unwrap(), None, None, None).unwrap();
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

#[derive(Debug, Clone)]
struct HangingThenReadyAccessor {
    reads: Arc<AtomicUsize>,
}

impl Access for HangingThenReadyAccessor {
    type Reader = HangingThenReadyReader;
    type Writer = ();
    type Lister = ();
    type Deleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let info = AccessorInfo::default();
        info.set_native_capability(Capability {
            read: true,
            ..Default::default()
        });
        info.into()
    }

    async fn read(&self, _: &str, _: OpRead) -> ::opendal::Result<(RpRead, Self::Reader)> {
        Ok((
            RpRead::new(),
            HangingThenReadyReader {
                reads: Arc::clone(&self.reads),
            },
        ))
    }
}

#[derive(Debug, Clone)]
struct HangingThenReadyReader {
    reads: Arc<AtomicUsize>,
}

impl oio::Read for HangingThenReadyReader {
    async fn read(&mut self) -> ::opendal::Result<Buffer> {
        match self.reads.fetch_add(1, Ordering::SeqCst) {
            0 => pending::<::opendal::Result<Buffer>>().await,
            1 => Ok(Buffer::from("recovered")),
            _ => Ok(Buffer::new()),
        }
    }
}

#[tokio::test]
async fn remote_timeout_is_retried_after_hanging_io() {
    let reads = Arc::new(AtomicUsize::new(0));
    let op = layer_remote_operator(
        OperatorBuilder::new(HangingThenReadyAccessor {
            reads: Arc::clone(&reads),
        })
        .finish(),
        Duration::from_millis(20),
        Duration::from_millis(20),
    );

    let value = op.read("hanging").await.expect("retry after timeout");
    assert_eq!(value.to_bytes().as_ref(), b"recovered");
    assert_eq!(reads.load(Ordering::SeqCst), 3);
}
