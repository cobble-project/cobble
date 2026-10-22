use super::*;
use crate::file::FileSystemRegistry;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use url::Url;

pub(crate) struct TestFileSystem {
    pub(crate) inner: Arc<dyn FileSystem>,
    pub(crate) atomic: AtomicBool,
    fail_marker: AtomicBool,
    fail_delete: AtomicBool,
    pub(crate) fail_delete_at: AtomicUsize,
    pub(crate) fail_delete_after: AtomicBool,
    pub(crate) delete_trace: Mutex<Vec<String>>,
    rename_calls: AtomicUsize,
    before_read: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    pub(crate) fail_rename_to: Mutex<Option<String>>,
}

impl TestFileSystem {
    pub(crate) fn new(inner: Arc<dyn FileSystem>) -> Self {
        Self {
            inner,
            atomic: AtomicBool::new(false),
            fail_marker: AtomicBool::new(false),
            fail_delete: AtomicBool::new(false),
            fail_delete_at: AtomicUsize::new(usize::MAX),
            fail_delete_after: AtomicBool::new(false),
            delete_trace: Mutex::new(Vec::new()),
            rename_calls: AtomicUsize::new(0),
            before_read: Mutex::new(None),
            fail_rename_to: Mutex::new(None),
        }
    }
}

impl FileSystem for TestFileSystem {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn supports_atomic_replace(&self) -> bool {
        self.atomic.load(Ordering::SeqCst)
    }
    fn init(
        _: &Url,
        _: Option<String>,
        _: Option<String>,
        _: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        unreachable!()
    }
    fn create_dir(&self, path: &str) -> Result<()> {
        self.inner.create_dir(path)
    }
    fn exists(&self, path: &str) -> Result<bool> {
        self.inner.exists(path)
    }
    fn delete(&self, path: &str) -> Result<()> {
        let mut trace = self.delete_trace.lock().unwrap();
        let index = trace.len();
        trace.push(path.to_string());
        drop(trace);
        let indexed_failure = self.fail_delete_at.load(Ordering::SeqCst) == index;
        if indexed_failure && !self.fail_delete_after.load(Ordering::SeqCst) {
            return Err(Error::IoError("injected indexed cleanup failure".into()));
        }
        if self.fail_delete.load(Ordering::SeqCst) {
            return Err(Error::IoError("injected cleanup failure".to_string()));
        }
        self.inner.delete(path)?;
        if indexed_failure {
            return Err(Error::IoError(
                "injected error after successful deletion".into(),
            ));
        }
        Ok(())
    }
    fn delete_async(&self, path: &str) -> Result<()> {
        self.delete(path)
    }
    fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.rename_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_rename_to.lock().unwrap().as_deref() == Some(to) {
            return Err(Error::IoError("injected publication failure".to_string()));
        }
        if !self.supports_atomic_replace() && self.inner.exists(to)? {
            return Err(Error::IoError("rename unavailable".to_string()));
        }
        self.inner.rename(from, to)
    }
    fn list(&self, path: &str) -> Result<Vec<String>> {
        self.inner.list(path)
    }
    fn open_read(&self, path: &str) -> Result<Box<dyn RandomAccessFile>> {
        let before_read = self.before_read.lock().unwrap().take();
        if let Some(before_read) = before_read {
            before_read();
        }
        self.inner.open_read(path)
    }
    fn open_write(&self, path: &str) -> Result<Box<dyn SequentialWriteFile>> {
        if path.contains(".complete-") && self.fail_marker.load(Ordering::SeqCst) {
            return Err(Error::IoError("injected marker failure".to_string()));
        }
        self.inner.open_write(path)
    }
    fn last_modified(&self, path: &str) -> Result<Option<u64>> {
        self.inner.last_modified(path)
    }
}

fn setup() -> (tempfile::TempDir, Arc<TestFileSystem>) {
    let dir = tempfile::tempdir().unwrap();
    let fs = FileSystemRegistry::new()
        .get_or_register(format!("file://{}", dir.path().display()))
        .unwrap();
    (dir, Arc::new(TestFileSystem::new(fs)))
}

fn read(fs: &dyn FileSystem, path: &str) -> Result<Bytes> {
    let reader = open_read(fs, path)?;
    reader.read_at(0, reader.size())
}

fn raw_write(fs: &dyn FileSystem, path: &str, bytes: &[u8]) {
    let mut writer = fs.open_write(path).unwrap();
    writer.write(bytes).unwrap();
    writer.close().unwrap();
}

#[test]
fn no_rename_new_overwrite_cleanup_and_plain_payload() {
    let (_dir, fs) = setup();
    write(fs.clone(), "CURRENT", b"first").unwrap();
    assert_eq!(read(fs.as_ref(), "CURRENT").unwrap(), b"first"[..]);
    write(fs.clone(), "CURRENT", b"second").unwrap();
    assert_eq!(read(fs.as_ref(), "CURRENT").unwrap(), b"second"[..]);
    assert!(!fs.exists("CURRENT").unwrap());
    assert!(exists(fs.as_ref(), "CURRENT").unwrap());
    assert_eq!(completed(fs.as_ref(), "CURRENT").unwrap().unwrap().0, 2);
    assert_eq!(fs.list("CURRENT-ATOMIC").unwrap().len(), 2);
    delete_versions(fs.as_ref(), "CURRENT").unwrap();
    assert!(!exists(fs.as_ref(), "CURRENT").unwrap());
    assert!(!fs.exists("CURRENT-ATOMIC/").unwrap());
    assert_eq!(fs.rename_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn reader_re_resolves_when_cleanup_removes_selected_payload() {
    let (_dir, fs) = setup();
    write(fs.clone(), "CURRENT", b"first").unwrap();
    let next = fs.clone();
    *fs.before_read.lock().unwrap() = Some(Box::new(move || {
        write(next, "CURRENT", b"second").unwrap();
    }));
    assert_eq!(read(fs.as_ref(), "CURRENT").unwrap(), b"second"[..]);

    delete_versions(fs.as_ref(), "CURRENT").unwrap();
    raw_write(fs.inner.as_ref(), "CURRENT", b"legacy");
    let next = fs.clone();
    *fs.before_read.lock().unwrap() = Some(Box::new(move || {
        write(next, "CURRENT", b"migrated").unwrap();
    }));
    assert_eq!(read(fs.as_ref(), "CURRENT").unwrap(), b"migrated"[..]);
}

#[test]
fn unfinished_payloads_and_marker_failures_do_not_publish_or_reuse_sequences() {
    let (_dir, fs) = setup();
    let mut writer = ReplaceWriter::new(fs.clone(), "CURRENT").unwrap();
    writer.write(b"incomplete").unwrap();
    drop(writer);
    assert!(!exists(fs.as_ref(), "CURRENT").unwrap());
    fs.fail_marker.store(true, Ordering::SeqCst);
    assert!(write(fs.clone(), "CURRENT", b"unpublished").is_err());
    assert!(!exists(fs.as_ref(), "CURRENT").unwrap());
    fs.fail_marker.store(false, Ordering::SeqCst);
    write(fs.clone(), "CURRENT", b"committed").unwrap();
    assert_eq!(completed(fs.as_ref(), "CURRENT").unwrap().unwrap().0, 3);
    let mut incomplete = ReplaceWriter::new(fs.clone(), "CURRENT").unwrap();
    incomplete.write(b"partial-new").unwrap();
    drop(incomplete);
    assert_eq!(read(fs.as_ref(), "CURRENT").unwrap(), b"committed"[..]);
}

#[test]
fn committed_corruption_never_falls_back() {
    let (_dir, fs) = setup();
    write(fs.clone(), "CURRENT", b"committed").unwrap();
    raw_write(fs.inner.as_ref(), "CURRENT", b"old-format");
    raw_write(
        fs.inner.as_ref(),
        "CURRENT-ATOMIC/00000000000000000001.data",
        b"truncated",
    );
    assert!(matches!(
        read(fs.as_ref(), "CURRENT"),
        Err(Error::ChecksumMismatch(_))
    ));
}

#[test]
#[cfg(unix)]
fn migration_priorities_and_cleanup_are_explicit() {
    let (_dir, fs) = setup();
    raw_write(fs.inner.as_ref(), "CURRENT", b"legacy");
    assert_eq!(read(fs.as_ref(), "CURRENT").unwrap(), b"legacy"[..]);
    fs.fail_delete.store(true, Ordering::SeqCst);
    write(fs.clone(), "CURRENT", b"versioned").unwrap();
    assert_eq!(read(fs.as_ref(), "CURRENT").unwrap(), b"versioned"[..]);
    fs.atomic.store(true, Ordering::SeqCst);
    assert_eq!(read(fs.as_ref(), "CURRENT").unwrap(), b"legacy"[..]);
    write(fs.clone(), "CURRENT", b"atomic-new").unwrap();
    assert_eq!(read(fs.as_ref(), "CURRENT").unwrap(), b"atomic-new"[..]);
    fs.fail_delete.store(false, Ordering::SeqCst);
    write(fs.clone(), "CURRENT", b"final").unwrap();
    assert!(!fs.exists("CURRENT-ATOMIC/").unwrap());
    fs.inner.delete("CURRENT").unwrap();
    fs.atomic.store(false, Ordering::SeqCst);
    write(fs.clone(), "CURRENT", b"versioned-again").unwrap();
    fs.atomic.store(true, Ordering::SeqCst);
    assert_eq!(
        read(fs.as_ref(), "CURRENT").unwrap(),
        b"versioned-again"[..]
    );
    write(fs.clone(), "CURRENT", b"flat-again").unwrap();
    assert!(!fs.exists("CURRENT-ATOMIC/").unwrap());
}

#[test]
fn offline_delete_removes_both_formats_and_abandoned_payloads() {
    let (_dir, fs) = setup();
    fs.fail_delete.store(true, Ordering::SeqCst);
    write(fs.clone(), "CURRENT", b"first").unwrap();
    write(fs.clone(), "CURRENT", b"second").unwrap();
    raw_write(fs.inner.as_ref(), "CURRENT", b"legacy");
    assert!(delete(fs.as_ref(), "CURRENT").is_err());
    assert_eq!(read(fs.as_ref(), "CURRENT").unwrap(), b"second"[..]);
    fs.fail_delete.store(false, Ordering::SeqCst);
    delete(fs.as_ref(), "CURRENT").unwrap();
    assert!(!exists(fs.as_ref(), "CURRENT").unwrap());
    assert!(!fs.exists("CURRENT-ATOMIC/").unwrap());
    fs.create_dir("CURRENT-ATOMIC").unwrap();
    raw_write(
        fs.as_ref(),
        "CURRENT-ATOMIC/00000000000000000001.data",
        b"orphan",
    );
    assert!(!exists(fs.as_ref(), "CURRENT").unwrap());
    delete(fs.as_ref(), "CURRENT").unwrap();
    delete(fs.as_ref(), "CURRENT").unwrap();
    assert!(!fs.exists("CURRENT-ATOMIC/").unwrap());
}
