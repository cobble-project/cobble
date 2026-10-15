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
#[cfg(windows)]
fn test_windows_local_fs_fast_copy_uses_hard_link_on_the_same_volume() {
    let root = tempfile::tempdir().unwrap();
    let source_root = root.path().join("source");
    let destination_root = root.path().join("destination");
    std::fs::create_dir_all(&source_root).unwrap();
    std::fs::create_dir_all(&destination_root).unwrap();
    let source_url = Url::from_directory_path(&source_root).unwrap();
    let destination_url = Url::from_directory_path(&destination_root).unwrap();
    let source_fs =
        OpendalFileSystem::init(&source_url, None, None, None).expect("source filesystem");
    let destination_fs = OpendalFileSystem::init(&destination_url, None, None, None)
        .expect("destination filesystem");
    source_fs.create_dir("data").unwrap();
    destination_fs.create_dir("data").unwrap();
    let mut writer = source_fs.open_write("data/file.sst").unwrap();
    writer.write(b"fast-copy").unwrap();
    writer.close().unwrap();

    let destination = crate::file::FastCopyDestination::new(&destination_fs, "data/file.sst");
    assert!(source_fs.can_fast_copy_to("data/file.sst", &destination));
    source_fs
        .fast_copy_to("data/file.sst", &destination)
        .unwrap();

    std::fs::write(source_root.join("data/file.sst"), b"updated").unwrap();
    assert_eq!(
        std::fs::read(destination_root.join("data/file.sst")).unwrap(),
        b"updated"
    );
}

#[test]
#[serial_test::serial(file)]
fn test_opendal_fs_basic() {
    cleanup_test_root();
    let fs = OpendalFileSystem::init(&Url::parse(TEST_ROOT).unwrap(), None, None, None);
    assert!(fs.is_ok());
    let fs = fs.unwrap();
    assert_eq!(fs.last_modified("absent").unwrap(), None);
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
    fs.create_dir("list/list").unwrap();
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
    assert_eq!(listed.iter().filter(|name| *name == "list").count(), 1);
    cleanup_test_root();
}

#[test]
fn test_hdfs_url_options_preserve_authority_root_and_user() {
    let url = Url::parse("hdfs://alice@[::1]:19000/data/prefix/").unwrap();
    let options = hdfs_url_options(&url);
    assert_eq!(options.get("name_node").unwrap(), "hdfs://[::1]:19000");
    assert_eq!(options.get("root").unwrap(), "/data/prefix");
    assert_eq!(options.get("user").unwrap(), "alice");
}

#[test]
fn test_opendal_options_service_url_mapping() {
    for (url, expected) in [
        ("fs:///data/prefix/", vec![("root", "/data/prefix/")]),
        (
            "s3://my-bucket/data/prefix/",
            vec![("bucket", "my-bucket"), ("root", "/data/prefix")],
        ),
        (
            "s3://[::1]/my-bucket/prefix?endpoint_scheme=http",
            vec![
                ("bucket", "my-bucket"),
                ("root", "/prefix"),
                ("endpoint", "http://[::1]"),
            ],
        ),
        (
            "hdfs://alice@[::1]:19000/data/prefix/",
            vec![
                ("name_node", "hdfs://[::1]:19000"),
                ("root", "/data/prefix"),
                ("user", "alice"),
            ],
        ),
        (
            "goosefs://[::1]:9201/data/prefix/",
            vec![("master_addr", "[::1]:9201"), ("root", "/data/prefix")],
        ),
        (
            "oss://alice:secret@my-bucket/data/prefix/",
            vec![
                ("bucket", "my-bucket"),
                ("root", "/data/prefix"),
                ("access_key_id", "alice"),
                ("access_key_secret", "secret"),
            ],
        ),
        (
            "cos://alice:secret@my-bucket/data/prefix/",
            vec![
                ("bucket", "my-bucket"),
                ("root", "/data/prefix"),
                ("secret_id", "alice"),
                ("secret_key", "secret"),
            ],
        ),
        (
            "ftp://alice:secret@[::1]:2121/data/prefix/",
            vec![
                ("endpoint", "ftp://[::1]:2121"),
                ("root", "/data/prefix"),
                ("user", "alice"),
                ("password", "secret"),
            ],
        ),
        (
            "sftp://alice@[::1]:2222/data/prefix/?key=/keys/id_ed25519",
            vec![
                ("endpoint", "ssh://[::1]:2222"),
                ("root", "/data/prefix"),
                ("user", "alice"),
                ("key", "/keys/id_ed25519"),
            ],
        ),
    ] {
        let (_, options) = opendal_options(&Url::parse(url).unwrap(), None, None, None).unwrap();
        for (key, value) in expected {
            assert_eq!(
                options.get(key).map(String::as_str),
                Some(value),
                "{url}: {key}"
            );
        }
        assert!(!options.contains_key("host"), "{url}");
        if !url.starts_with("s3:") {
            assert!(!options.contains_key("secret_access_key"), "{url}");
        }
        if !["s3:", "oss:", "cos:"]
            .iter()
            .any(|scheme| url.starts_with(scheme))
        {
            assert!(!options.contains_key("bucket"), "{url}");
        }
    }
}

#[test]
fn test_opendal_options_override_precedence() {
    for (scheme, authority_key, user_key, secret_key) in [
        ("s3", "bucket", "access_key_id", Some("secret_access_key")),
        ("oss", "bucket", "access_key_id", Some("access_key_secret")),
        ("cos", "bucket", "secret_id", Some("secret_key")),
        ("ftp", "endpoint", "user", Some("password")),
        ("sftp", "endpoint", "user", None),
        ("hdfs", "name_node", "user", None),
        ("goosefs", "master_addr", "auth_username", None),
    ] {
        let url = Url::parse(&format!("{scheme}://url-user@url-host/data?{authority_key}=query-authority&{user_key}=query-user&root=/query-root")).unwrap();
        let custom = HashMap::from([
            (authority_key.to_string(), "custom-authority".to_string()),
            (user_key.to_string(), "custom-user".to_string()),
            ("root".to_string(), "/custom-root".to_string()),
        ]);
        let field_secret = secret_key.map(|_| "field-secret".to_string());
        let (_, query) = opendal_options(
            &url,
            Some("field-user".to_string()),
            field_secret.clone(),
            None,
        )
        .unwrap();
        assert_eq!(
            query.get(authority_key).unwrap(),
            "query-authority",
            "{scheme}"
        );
        assert_eq!(query.get(user_key).unwrap(), "query-user", "{scheme}");
        assert_eq!(query.get("root").unwrap(), "/query-root", "{scheme}");
        if let Some(secret_key) = secret_key {
            assert_eq!(query.get(secret_key).unwrap(), "field-secret", "{scheme}");
            let mut credential_url = url.clone();
            credential_url
                .query_pairs_mut()
                .append_pair(secret_key, "query-secret");
            let (_, query) = opendal_options(
                &credential_url,
                None,
                Some("field-secret".to_string()),
                None,
            )
            .unwrap();
            assert_eq!(query.get(secret_key).unwrap(), "query-secret", "{scheme}");
            let (_, custom) = opendal_options(
                &credential_url,
                None,
                Some("field-secret".to_string()),
                Some(HashMap::from([(
                    secret_key.to_string(),
                    "custom-secret".to_string(),
                )])),
            )
            .unwrap();
            assert_eq!(custom.get(secret_key).unwrap(), "custom-secret", "{scheme}");
        }
        let (_, options) = opendal_options(
            &url,
            Some("field-user".to_string()),
            field_secret,
            Some(custom),
        )
        .unwrap();
        assert_eq!(
            options.get(authority_key).unwrap(),
            "custom-authority",
            "{scheme}"
        );
        assert_eq!(options.get(user_key).unwrap(), "custom-user", "{scheme}");
        assert_eq!(options.get("root").unwrap(), "/custom-root", "{scheme}");
    }
    // Required S3 options can come entirely from custom_options, before validation.
    let (_, options) = opendal_options(
        &Url::parse("s3:///").unwrap(),
        None,
        None,
        Some(HashMap::from([(
            "bucket".to_string(),
            "custom-bucket".to_string(),
        )])),
    )
    .unwrap();
    assert_eq!(options.get("bucket").unwrap(), "custom-bucket");
}

#[test]
fn test_opendal_options_credential_and_scheme_boundaries() {
    for (url, secret) in [
        ("sftp://alice:password@host/data", None),
        ("sftp://alice@host/data", Some("password".to_string())),
        ("sftp://alice@host/data?password=secret", None),
    ] {
        assert!(matches!(
            opendal_options(&Url::parse(url).unwrap(), None, secret, None),
            Err(Error::ConfigError(_))
        ));
    }
    for (scheme, id_key, secret_key) in [
        ("oss", "access_key_id", "access_key_secret"),
        ("cos", "secret_id", "secret_key"),
        ("ftp", "user", "password"),
    ] {
        let url = Url::parse(&format!("{scheme}://al%40ice:p%40ss+word@host/data")).unwrap();
        let (_, options) = opendal_options(&url, None, None, None).unwrap();
        assert_eq!(options.get(id_key).unwrap(), "al@ice");
        assert_eq!(options.get(secret_key).unwrap(), "p@ss+word");
    }
    let (_, options) = opendal_options(
        &Url::parse("sftp://al%40ice@host/data").unwrap(),
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(options.get("user").unwrap(), "al@ice");
    let (_, options) = opendal_options(
        &Url::parse("unknown://host/path?explicit=value").unwrap(),
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        options,
        HashMap::from([("explicit".to_string(), "value".to_string())])
    );
}

#[test]
#[cfg(any(
    feature = "storage-oss",
    feature = "storage-cos",
    feature = "storage-ftp",
    feature = "storage-sftp"
))]
fn test_opendal_service_url_operator_construction() {
    let cases = [
        #[cfg(feature = "storage-oss")]
        "oss://my-bucket/data?endpoint=https://oss.example.com&access_key_id=id&access_key_secret=secret",
        #[cfg(feature = "storage-cos")]
        "cos://my-bucket/data?endpoint=https://cos.example.com&secret_id=id&secret_key=secret",
        #[cfg(feature = "storage-ftp")]
        "ftp://alice:secret@localhost:2121/data",
        #[cfg(feature = "storage-sftp")]
        "sftp://alice@localhost:2222/data?key=/keys/id_ed25519",
    ];
    for url in cases {
        let fs = OpendalFileSystem::init(&Url::parse(url).unwrap(), None, None, None).unwrap();
        assert_eq!(fs.op.info().root(), "/data/");
    }
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
