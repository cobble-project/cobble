use super::*;
use crate::file::{RandomAccessFile, SequentialWriteFile};
use crate::snapshot::manifest::{ManifestSnapshot, decode_manifest};
use crate::{Config, DbBuilder, ExpandStorageMode};
use size::Size;
use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

fn volume(path: &Path) -> VolumeDescriptor {
    VolumeDescriptor::single_volume(path.to_str().unwrap()).remove(0)
}

fn config(path: &Path) -> Config {
    Config {
        volumes: vec![volume(path)],
        total_buckets: 1,
        num_columns: 1,
        memtable_capacity: Size::from_mib(16),
        memtable_buffer_count: 2,
        block_cache_size: Size::from_const(0),
        l0_file_limit: 100,
        snapshot_on_flush: false,
        snapshot_retention: None,
        ..Config::default()
    }
}

fn database(path: &Path) -> Arc<Db> {
    Arc::new(
        DbBuilder::new(config(path))
            .db_id("source")
            .bucket_ranges(vec![0..=0])
            .open()
            .unwrap(),
    )
}

fn assert_value(db: &Db, key: &[u8], value: &[u8]) {
    assert_eq!(db.get(0, key).unwrap().unwrap()[0].as_deref(), Some(value));
}

fn exported_manifest(metadata: &ShardSnapshotMetadata) -> ManifestSnapshot {
    let path = Url::parse(&metadata.manifest_path)
        .unwrap()
        .to_file_path()
        .unwrap();
    let bytes = crate::file::metadata_io::read_metadata_payload_from_path_for_test(path).unwrap();
    match decode_manifest(&bytes).unwrap() {
        ManifestPayload::Snapshot(manifest) => manifest,
        _ => panic!("export must contain a full standalone manifest"),
    }
}

#[derive(Default)]
struct Gate {
    state: Mutex<(usize, bool)>,
    changed: Condvar,
}

impl Gate {
    fn enter(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 += 1;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn wait(&self) {
        self.wait_for(1);
    }

    fn wait_for(&self, count: usize) {
        let state = self.state.lock().unwrap();
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(20), |state| state.0 < count)
            .unwrap();
        assert!(
            state.0 >= count && !timeout.timed_out(),
            "export never reached copy barrier"
        );
    }

    fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}

struct TestFs {
    inner: Arc<dyn FileSystem>,
    gate: Option<Arc<Gate>>,
    peer_gate: Option<Arc<Gate>>,
    first_write: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
    fail: bool,
    short_write: bool,
    commit_handle: Mutex<Option<ExportSnapshotHandle>>,
    cancel_on_commit: Arc<AtomicBool>,
    cancel_before_commit: Arc<Mutex<Option<ExportSnapshotHandle>>>,
}

impl TestFs {
    fn new(destination: &VolumeDescriptor, gate: Option<Arc<Gate>>, fail: bool) -> Self {
        Self {
            inner: FileSystemRegistry::new()
                .get_or_register_volume(destination)
                .unwrap(),
            gate,
            peer_gate: None,
            first_write: Arc::new(AtomicBool::new(true)),
            active: Arc::new(AtomicUsize::new(0)),
            fail,
            short_write: true,
            commit_handle: Mutex::new(None),
            cancel_on_commit: Arc::new(AtomicBool::new(false)),
            cancel_before_commit: Arc::new(Mutex::new(None)),
        }
    }
}

struct TestWriter {
    inner: Box<dyn SequentialWriteFile>,
    gate: Option<Arc<Gate>>,
    peer_gate: Option<Arc<Gate>>,
    local_first_write: bool,
    first_write: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
    fail: bool,
    short_write: bool,
    closed: bool,
    cancel_on_close: Option<Arc<Mutex<Option<ExportSnapshotHandle>>>>,
}

impl File for TestWriter {
    fn close(&mut self) -> Result<()> {
        let result = self.inner.close();
        if !self.closed {
            self.active.fetch_sub(1, Ordering::SeqCst);
            self.closed = true;
        }
        if let Some(handle) = self
            .cancel_on_close
            .as_ref()
            .and_then(|handle| handle.lock().unwrap().clone())
        {
            assert!(
                handle.cancel(),
                "cancellation must win before publication starts"
            );
        }
        result
    }
    fn size(&self) -> usize {
        self.inner.size()
    }
}

impl SequentialWriteFile for TestWriter {
    fn write(&mut self, data: &[u8]) -> Result<usize> {
        if self.local_first_write {
            self.local_first_write = false;
            let first = self.first_write.swap(false, Ordering::SeqCst);
            let gate = if first { &self.gate } else { &self.peer_gate };
            if let Some(gate) = gate {
                gate.enter();
            }
            if first && self.fail {
                self.inner.write(&data[..data.len().min(3)])?;
                return Err(Error::IoError("injected export copy failure".into()));
            }
        }
        self.inner.write(if self.short_write {
            &data[..data.len().min(3)]
        } else {
            data
        })
    }
}

impl FileSystem for TestFs {
    fn as_any(&self) -> &dyn Any {
        self
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
        assert_eq!(
            self.active.load(Ordering::SeqCst),
            0,
            "cleanup raced an open writer"
        );
        self.inner.delete(path)
    }
    fn delete_async(&self, path: &str) -> Result<()> {
        self.delete(path)
    }
    fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.inner.rename(from, to)?;
        if let Some(handle) = self.commit_handle.lock().unwrap().as_ref() {
            self.cancel_on_commit.store(true, Ordering::Release);
            assert!(!handle.cancel(), "publication already started");
        }
        Ok(())
    }
    fn list(&self, path: &str) -> Result<Vec<String>> {
        self.inner.list(path)
    }
    fn open_read(&self, path: &str) -> Result<Box<dyn RandomAccessFile>> {
        self.inner.open_read(path)
    }
    fn open_write(&self, path: &str) -> Result<Box<dyn SequentialWriteFile>> {
        let inner = self.inner.open_write(path)?;
        self.active.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(TestWriter {
            inner,
            gate: self.gate.clone(),
            peer_gate: self.peer_gate.clone(),
            local_first_write: true,
            first_write: self.first_write.clone(),
            active: self.active.clone(),
            fail: self.fail,
            short_write: self.short_write,
            closed: false,
            cancel_on_close: path
                .contains("/snapshot/_SNAPSHOT-")
                .then(|| Arc::clone(&self.cancel_before_commit)),
        }))
    }
    fn last_modified(&self, path: &str) -> Result<Option<u64>> {
        self.inner.last_modified(path)
    }
}

#[test]
fn snapshot_export_restores_vlog_and_schema_after_source_deletion() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source-volume");
    let destination = root.path().join("export-volume");
    let mut options = config(&source);
    options.value_separation_threshold = Some(Size::from_const(16));
    let db = DbBuilder::new(options)
        .db_id("source")
        .bucket_ranges(vec![0..=0])
        .open()
        .unwrap();
    let payload = vec![42; 256];
    db.put(0, b"first", 0, &payload).unwrap();
    let first = db.create_snapshot_and_wait("export first").unwrap();
    let mut builder = db.update_schema();
    builder.add_column(1, None, None, None).unwrap();
    builder.commit();
    db.put(0, b"second", 0, b"second-value").unwrap();
    let snapshot = db.create_snapshot_and_wait("export second").unwrap();
    assert!(db.retain_snapshot(snapshot));
    let current = source.join(db.id()).join("snapshot/CURRENT");
    let before = std::fs::read(&current).ok();
    db.put(0, b"later", 0, b"not-in-snapshot").unwrap();
    let exported = db
        .export_snapshot(snapshot, volume(&destination))
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(std::fs::read(&current).ok(), before);
    assert!(db.snapshot_manager.pin_completed_snapshot(first).is_ok());
    let manifest = exported_manifest(&exported);
    assert!(!manifest.vlog_files.is_empty());
    assert_eq!(manifest.latest_schema_id, 1);
    assert_eq!(manifest.wal_checkpoint_id, 0);
    assert!(manifest.wal_volume.is_none());
    assert!(
        manifest
            .vlog_files
            .iter()
            .all(|file| file.origin == ReplicaOrigin::Owned)
    );
    assert_eq!(manifest.timestamp_seconds, exported.timestamp_seconds);
    assert_eq!(manifest.bucket_ranges, exported.ranges);
    assert!(db.expire_snapshot(snapshot).unwrap());
    assert!(
        !source
            .join(db.id())
            .join(snapshot_manifest_name(snapshot))
            .exists(),
        "export leaked its temporary pin"
    );
    db.close().unwrap();
    std::fs::remove_dir_all(&source).unwrap();
    for _ in 0..2 {
        let restored =
            Db::open_new_with_manifest_path(config(&destination), &exported.manifest_path).unwrap();
        assert_value(&restored, b"first", &payload);
        assert_value(&restored, b"second", b"second-value");
        assert!(restored.get(0, b"later").unwrap().is_none());
        let id = restored
            .create_snapshot_and_wait("restored ownership")
            .unwrap();
        restored.expire_snapshot(id).unwrap();
        restored.close().unwrap();
        assert!(
            Url::parse(&exported.manifest_path)
                .unwrap()
                .to_file_path()
                .unwrap()
                .exists()
        );
    }
}

#[test]
fn snapshot_export_pins_expired_snapshot_and_parallel_exports_independently() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let db = database(&source);
    db.put(0, b"key", 0, b"value").unwrap();
    let snapshot = db.create_snapshot_and_wait("blocked export").unwrap();
    let source_manifest = source.join(db.id()).join(snapshot_manifest_name(snapshot));
    let mut handles = Vec::new();
    let mut gates = Vec::new();
    for index in 0..2 {
        let destination = volume(&root.path().join(format!("export-{index}")));
        let gate = Arc::new(Gate::default());
        let fs = Arc::new(TestFs::new(&destination, Some(gate.clone()), false));
        let db = db.clone();
        handles.push(std::thread::spawn(move || {
            db.export_snapshot_to_filesystem(snapshot, destination, Some(fs))
                .unwrap()
                .wait()
                .unwrap()
        }));
        gate.wait();
        gates.push(gate);
    }
    assert!(db.expire_snapshot(snapshot).unwrap());
    assert!(source_manifest.exists());
    gates[0].release();
    let first = handles.remove(0).join().unwrap();
    assert!(
        source_manifest.exists(),
        "first export released another export's pin"
    );
    gates[1].release();
    let second = handles.remove(0).join().unwrap();
    assert!(!source_manifest.exists());
    db.close().unwrap();
    std::fs::remove_dir_all(source).unwrap();
    for exported in [first, second] {
        let destination = Url::parse(&exported.manifest_path)
            .unwrap()
            .to_file_path()
            .unwrap();
        let base = destination
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap();
        let restored =
            Db::open_new_with_manifest_path(config(base), exported.manifest_path).unwrap();
        assert_value(&restored, b"key", b"value");
        restored.close().unwrap();
    }
}

#[test]
fn snapshot_export_failure_and_cancellation_clean_only_private_files() {
    for fail in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let db = database(&root.path().join("source"));
        db.put(0, b"key", 0, b"value").unwrap();
        let snapshot = db.create_snapshot_and_wait("failed export").unwrap();
        let destination_path = root.path().join("destination");
        std::fs::create_dir_all(&destination_path).unwrap();
        std::fs::write(destination_path.join("sentinel"), b"unrelated").unwrap();
        let destination = volume(&destination_path);
        let gate = Arc::new(Gate::default());
        let fs = Arc::new(TestFs::new(&destination, Some(gate.clone()), fail));
        let handle = db
            .export_snapshot_to_filesystem(snapshot, destination, Some(fs.clone()))
            .unwrap();
        gate.wait();
        if !fail {
            assert!(handle.cancel());
        }
        gate.release();
        assert!(handle.wait().is_err());
        assert_eq!(fs.active.load(Ordering::SeqCst), 0);
        assert_eq!(std::fs::read_dir(destination_path).unwrap().count(), 1);
        assert_value(&db, b"key", b"value");
        assert!(db.snapshot_manager.pin_completed_snapshot(snapshot).is_ok());
        assert!(db.expire_snapshot(snapshot).unwrap());
        assert!(
            !root
                .path()
                .join("source")
                .join(db.id())
                .join(snapshot_manifest_name(snapshot))
                .exists(),
            "failed export leaked a pin"
        );
        assert_value(&db, b"key", b"value");
        db.close().unwrap();
    }
}

#[test]
fn snapshot_export_commit_wins_cancellation_and_short_writes_restore() {
    let root = tempfile::tempdir().unwrap();
    let db = database(&root.path().join("source"));
    db.put(0, b"key", 0, b"value").unwrap();
    let snapshot = db.create_snapshot_and_wait("commit export").unwrap();
    let destination_path = root.path().join("destination");
    let destination = volume(&destination_path);
    let gate = Arc::new(Gate::default());
    let fs = Arc::new(TestFs::new(&destination, Some(gate.clone()), false));
    let handle = db
        .export_snapshot_to_filesystem(snapshot, destination, Some(fs.clone()))
        .unwrap();
    gate.wait();
    *fs.commit_handle.lock().unwrap() = Some(handle.clone());
    gate.release();
    let exported = handle.wait().unwrap();
    assert!(fs.cancel_on_commit.load(Ordering::Acquire));
    let restored =
        Db::open_new_with_manifest_path(config(&destination_path), exported.manifest_path).unwrap();
    assert_value(&restored, b"key", b"value");
    restored.close().unwrap();
    db.close().unwrap();
}

#[test]
fn snapshot_export_rejects_separate_metadata_volume_overlap_for_empty_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let metadata = root.path().join("metadata");
    let mut options = config(&root.path().join("data"));
    options.volumes[0].kinds = 0;
    options.volumes[0].set_usage(crate::VolumeUsageKind::PrimaryDataPriorityHigh);
    options.volumes.push(VolumeDescriptor::new(
        metadata.to_str().unwrap(),
        vec![crate::VolumeUsageKind::Meta],
    ));
    let db = DbBuilder::new(options)
        .db_id("source")
        .bucket_ranges(vec![0..=0])
        .open()
        .unwrap();
    let snapshot = db
        .create_snapshot_and_wait("empty metadata snapshot")
        .unwrap();
    assert!(
        db.export_snapshot(snapshot, volume(&metadata.join(db.id()).join("exports")))
            .unwrap()
            .wait()
            .is_err()
    );
    db.close().unwrap();
}

#[test]
fn snapshot_export_active_incremental_chain_survives_source_deletion() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let destination = root.path().join("destination");
    let mut options = config(&source);
    options.active_memtable_incremental_snapshot_ratio = 1.0;
    let db = Arc::new(
        DbBuilder::new(options)
            .db_id("source")
            .bucket_ranges(vec![0..=0])
            .open()
            .unwrap(),
    );
    db.put(0, b"first", 0, b"one").unwrap();
    let base = db.create_snapshot_and_wait("active base").unwrap();
    db.put(0, b"second", 0, b"two").unwrap();
    let snapshot = db.create_snapshot_and_wait("active incremental").unwrap();
    let original = load_manifest_for_snapshot(&db.file_manager, snapshot).unwrap();
    let gate = Arc::new(Gate::default());
    let target_volume = volume(&destination);
    let fs = Arc::new(TestFs::new(&target_volume, Some(gate.clone()), false));
    let handle = {
        let db = db.clone();
        std::thread::spawn(move || {
            db.export_snapshot_to_filesystem(snapshot, target_volume, Some(fs))
                .unwrap()
                .wait()
                .unwrap()
        })
    };
    gate.wait();
    assert!(db.expire_snapshot(base).unwrap());
    assert!(db.expire_snapshot(snapshot).unwrap());
    for segment in &original.active_memtable_data {
        assert!(source.join(db.id()).join(&segment.path).exists());
    }
    gate.release();
    let exported = handle.join().unwrap();
    let manifest = exported_manifest(&exported);
    assert!(!manifest.active_memtable_data.is_empty());
    db.close().unwrap();
    std::fs::remove_dir_all(source).unwrap();
    let restored =
        Db::open_new_with_manifest_path(config(&destination), exported.manifest_path).unwrap();
    assert_value(&restored, b"first", b"one");
    assert_value(&restored, b"second", b"two");
    restored.close().unwrap();
}

#[test]
fn snapshot_export_rejects_unfinished_and_overlapping_destinations() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let db = database(&source);
    let pending = db.snapshot_manager.create_snapshot(None);
    assert!(
        db.export_snapshot(pending.id, volume(&root.path().join("unused")))
            .is_err()
    );
    db.cancel_snapshot(pending.id).unwrap();
    let snapshot = db.create_snapshot_and_wait("empty snapshot").unwrap();
    assert!(
        db.export_snapshot(snapshot, volume(&source.join(db.id()).join("schema")))
            .unwrap()
            .wait()
            .is_err()
    );
    #[cfg(unix)]
    {
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(source.join(db.id()), &alias).unwrap();
        assert!(
            db.export_snapshot(snapshot, volume(&alias))
                .unwrap()
                .wait()
                .is_err()
        );
    }
    db.close().unwrap();
}

#[test]
fn snapshot_export_materializes_imported_files_without_donor_dependencies() {
    let root = tempfile::tempdir().unwrap();
    let source_volume = root.path().join("source-volume");
    let destination = root.path().join("destination");
    let mut options = config(&source_volume);
    options.total_buckets = 2;
    options.value_separation_threshold = Some(Size::from_const(16));
    let donor = DbBuilder::new(options.clone())
        .db_id("donor")
        .bucket_ranges(vec![1..=1])
        .open()
        .unwrap();
    let payload = vec![7; 128];
    donor.put(1, b"donor-key", 0, &payload).unwrap();
    let donor_snapshot = donor.create_snapshot_and_wait("donor snapshot").unwrap();
    let source = DbBuilder::new(options)
        .db_id("source")
        .bucket_ranges(vec![0..=0])
        .open()
        .unwrap();
    source.put(0, b"local-key", 0, b"local-value").unwrap();
    source
        .expand_bucket_with_storage_mode(
            "donor",
            Some(donor_snapshot),
            None,
            ExpandStorageMode::ReferencePersistent,
        )
        .unwrap();
    let snapshot = source.create_snapshot_and_wait("imported export").unwrap();
    let original = load_manifest_for_snapshot(&source.file_manager, snapshot).unwrap();
    assert!(
        original
            .tree_levels
            .iter()
            .flatten()
            .flat_map(|level| &level.files)
            .any(|file| matches!(file.origin, ReplicaOrigin::ExternalPersistent { .. }))
    );
    let exported = source
        .export_snapshot(snapshot, volume(&destination))
        .unwrap()
        .wait()
        .unwrap();
    let manifest = exported_manifest(&exported);
    assert_eq!(manifest.timestamp_seconds, original.timestamp_seconds);
    assert_eq!(manifest.seq_id, original.seq_id);
    assert_eq!(manifest.topology_epoch, original.topology_epoch);
    assert_eq!(manifest.bucket_ranges, original.bucket_ranges);
    for (path, origin) in manifest
        .tree_levels
        .iter()
        .flatten()
        .flat_map(|level| &level.files)
        .map(|file| (&file.path, &file.origin))
        .chain(
            manifest
                .vlog_files
                .iter()
                .map(|file| (&file.path, &file.origin)),
        )
    {
        assert_eq!(*origin, ReplicaOrigin::Owned);
        assert!(!path.contains("source-volume"));
        assert!(path.contains(&exported.db_id));
    }
    source.close().unwrap();
    donor.close().unwrap();
    std::fs::remove_dir_all(source_volume).unwrap();
    let mut restored_options = config(&destination);
    restored_options.total_buckets = 2;
    let restored =
        Db::open_new_with_manifest_path(restored_options, exported.manifest_path).unwrap();
    assert_value(&restored, b"local-key", b"local-value");
    assert_eq!(
        restored.get(1, b"donor-key").unwrap().unwrap()[0].as_deref(),
        Some(payload.as_slice())
    );
    restored.close().unwrap();
}

#[test]
fn snapshot_export_pins_incremental_manifest_base_during_expiration() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let destination_path = root.path().join("destination");
    let db = database(&source);
    db.put(0, b"first", 0, b"one").unwrap();
    let base = db.create_snapshot_and_wait("incremental base").unwrap();
    db.put(0, b"second", 0, b"two").unwrap();
    let snapshot = db.create_snapshot_and_wait("incremental child").unwrap();
    assert_eq!(
        crate::snapshot::manifest::load_manifest_chain(&db.file_manager, snapshot)
            .unwrap()
            .len(),
        2
    );
    let destination = volume(&destination_path);
    let gate = Arc::new(Gate::default());
    let fs = Arc::new(TestFs::new(&destination, Some(gate.clone()), false));
    let handle = {
        let db = db.clone();
        std::thread::spawn(move || {
            db.export_snapshot_to_filesystem(snapshot, destination, Some(fs))
                .unwrap()
                .wait()
                .unwrap()
        })
    };
    gate.wait();
    assert!(db.expire_snapshot(base).unwrap());
    assert!(db.expire_snapshot(snapshot).unwrap());
    for id in [base, snapshot] {
        assert!(
            source
                .join(db.id())
                .join(snapshot_manifest_name(id))
                .exists()
        );
    }
    gate.release();
    let exported = handle.join().unwrap();
    exported_manifest(&exported);
    for id in [base, snapshot] {
        assert!(
            !source
                .join(db.id())
                .join(snapshot_manifest_name(id))
                .exists()
        );
    }
    db.close().unwrap();
    std::fs::remove_dir_all(source).unwrap();
    let restored =
        Db::open_new_with_manifest_path(config(&destination_path), exported.manifest_path).unwrap();
    assert_value(&restored, b"first", b"one");
    assert_value(&restored, b"second", b"two");
    restored.close().unwrap();
}

fn bounded_result(handle: ExportSnapshotHandle) -> Result<ShardSnapshotMetadata> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || tx.send(handle.wait()).unwrap());
    rx.recv_timeout(Duration::from_secs(20))
        .expect("export result did not complete")
}

#[test]
fn export_cancellation_does_not_wait_for_unrelated_transfer_permits() {
    let root = tempfile::tempdir().unwrap();
    let db = database(&root.path().join("source"));
    db.put(0, b"key", 0, b"value").unwrap();
    let snapshot = db.create_snapshot_and_wait("queued cancellation").unwrap();
    let semaphore = db.file_manager.transfer_semaphore();
    let permits = semaphore
        .clone()
        .try_acquire_many_owned(semaphore.available_permits() as u32)
        .unwrap();
    let handle = db
        .export_snapshot(snapshot, volume(&root.path().join("destination")))
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let acquiring = acquire_export_transfer_permit(&db.file_manager, &handle.state);
        tokio::pin!(acquiring);
        // Poll the production acquisition path into an actual semaphore wait before cancelling.
        let first_poll = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(acquiring.as_mut(), cx))
        })
        .await;
        assert!(first_poll.is_pending());
        assert!(handle.cancel());
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(20), acquiring)
                .await
                .unwrap(),
            Err(Error::CancelledError(_))
        ));
    });
    assert!(matches!(
        bounded_result(handle.clone()),
        Err(Error::CancelledError(_))
    ));
    assert!(handle.is_finished());
    assert!(!handle.cancel());
    drop(permits);
    db.close().unwrap();
}

#[test]
fn exports_copy_in_parallel_with_the_shared_snapshot_transfer_limit() {
    let root = tempfile::tempdir().unwrap();
    let mut options = config(&root.path().join("source"));
    options.file_transfer_concurrency = 3;
    let db = DbBuilder::new(options)
        .db_id("source")
        .bucket_ranges(vec![0..=0])
        .open()
        .unwrap();
    db.put(0, b"first", 0, b"one").unwrap();
    db.create_snapshot_and_wait("parallel base").unwrap();
    db.put(0, b"second", 0, b"two").unwrap();
    let snapshot = db.create_snapshot_and_wait("parallel copies").unwrap();
    let semaphore = db.file_manager.transfer_semaphore();
    let normal_transfer_permit = semaphore.clone().try_acquire_owned().unwrap();
    let gate = Arc::new(Gate::default());
    let mut handles = Vec::new();
    let mut filesystems = Vec::new();
    for index in 0..2 {
        let destination = volume(&root.path().join(format!("destination-{index}")));
        let mut fs = TestFs::new(&destination, Some(gate.clone()), false);
        fs.peer_gate = Some(gate.clone());
        let fs = Arc::new(fs);
        handles.push(
            db.export_snapshot_to_filesystem(snapshot, destination, Some(fs.clone()))
                .unwrap(),
        );
        filesystems.push(fs);
    }
    gate.wait_for(2);
    assert_eq!(
        filesystems
            .iter()
            .map(|fs| fs.active.load(Ordering::SeqCst))
            .sum::<usize>(),
        2
    );
    assert_eq!(semaphore.available_permits(), 0);
    gate.release();
    for handle in handles {
        bounded_result(handle).unwrap();
    }
    assert_eq!(semaphore.available_permits(), 2);
    drop(normal_transfer_permit);
    db.close().unwrap();
}

#[test]
fn export_failure_drains_blocked_peer_before_cleanup_and_releasing_source_pin() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let db = database(&source);
    db.put(0, b"key", 0, b"value").unwrap();
    let snapshot = db.create_snapshot_and_wait("failure drain").unwrap();
    let destination_path = root.path().join("destination");
    let destination = volume(&destination_path);
    let first_gate = Arc::new(Gate::default());
    let peers = Arc::new(Gate::default());
    let mut fs = TestFs::new(&destination, Some(first_gate.clone()), true);
    fs.peer_gate = Some(peers.clone());
    let fs = Arc::new(fs);
    let handle = db
        .export_snapshot_to_filesystem(snapshot, destination, Some(fs.clone()))
        .unwrap();
    first_gate.wait();
    peers.wait();
    db.expire_snapshot(snapshot).unwrap();
    first_gate.release();
    assert!(!handle.is_finished());
    assert!(
        source
            .join(db.id())
            .join(snapshot_manifest_name(snapshot))
            .exists()
    );
    peers.release();
    let error = bounded_result(handle).unwrap_err();
    assert!(error.to_string().contains("injected export copy failure"));
    assert_eq!(fs.active.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read_dir(destination_path).unwrap().count(), 0);
    assert!(
        !source
            .join(db.id())
            .join(snapshot_manifest_name(snapshot))
            .exists()
    );
    db.close().unwrap();
}

#[test]
fn export_can_cancel_after_manifest_write_before_publication() {
    let root = tempfile::tempdir().unwrap();
    let db = database(&root.path().join("source"));
    db.put(0, b"key", 0, b"value").unwrap();
    let snapshot = db
        .create_snapshot_and_wait("precommit cancellation")
        .unwrap();
    let destination = volume(&root.path().join("destination"));
    let gate = Arc::new(Gate::default());
    let fs = Arc::new(TestFs::new(&destination, Some(gate.clone()), false));
    let handle = db
        .export_snapshot_to_filesystem(snapshot, destination, Some(fs.clone()))
        .unwrap();
    gate.wait();
    *fs.cancel_before_commit.lock().unwrap() = Some(handle.clone());
    gate.release();
    assert!(matches!(
        bounded_result(handle),
        Err(Error::CancelledError(_))
    ));
    assert_eq!(fs.active.load(Ordering::SeqCst), 0);
    db.close().unwrap();
}

#[test]
fn cloned_export_handles_support_concurrent_async_and_cached_results() {
    let root = tempfile::tempdir().unwrap();
    let db = database(&root.path().join("source"));
    db.put(0, b"key", 0, b"value").unwrap();
    let snapshot = db.create_snapshot_and_wait("async result").unwrap();
    let destination = volume(&root.path().join("destination"));
    let gate = Arc::new(Gate::default());
    let fs = Arc::new(TestFs::new(&destination, Some(gate.clone()), false));
    let handle = db
        .export_snapshot_to_filesystem(snapshot, destination, Some(fs))
        .unwrap();
    gate.wait();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = runtime.block_on(async {
        let first = handle.clone();
        let second = handle.clone();
        let waiters = [
            tokio::spawn(async move { first.result().await }),
            tokio::spawn(async move { second.result().await }),
        ];
        tokio::task::yield_now().await;
        gate.release();
        let first = waiters
            .into_iter()
            .map(|waiter| async move { waiter.await.unwrap().unwrap() });
        let mut paths = Vec::new();
        for waiter in first {
            paths.push(
                tokio::time::timeout(Duration::from_secs(20), waiter)
                    .await
                    .unwrap()
                    .manifest_path,
            );
        }
        paths
    });
    assert_eq!(result[0], result[1]);
    assert_eq!(handle.wait().unwrap().manifest_path, result[0]);
    assert!(handle.is_finished());
    db.close().unwrap();
}

#[test]
fn export_keeps_database_access_until_io_finishes_and_handles_forced_close() {
    for forced in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let db = database(&root.path().join("source"));
        db.put(0, b"key", 0, b"value").unwrap();
        let snapshot = db.create_snapshot_and_wait("close protection").unwrap();
        let destination_path = root.path().join("destination");
        let destination = volume(&destination_path);
        let gate = Arc::new(Gate::default());
        let fs = Arc::new(TestFs::new(&destination, Some(gate.clone()), false));
        let handle = db
            .export_snapshot_to_filesystem(snapshot, destination, Some(fs))
            .unwrap();
        gate.wait();
        if forced {
            db.db_lifecycle
                .mark_error(Error::IoError("injected DB failure".into()));
            drop(db);
            gate.release();
        } else {
            let (tx, rx) = std::sync::mpsc::channel();
            let closing = db.clone();
            let thread = std::thread::spawn(move || tx.send(closing.close()).unwrap());
            assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
            gate.release();
            rx.recv_timeout(Duration::from_secs(20)).unwrap().unwrap();
            thread.join().unwrap();
        }
        let exported = bounded_result(handle).unwrap();
        let restored =
            Db::open_new_with_manifest_path(config(&destination_path), exported.manifest_path)
                .unwrap();
        assert_value(&restored, b"key", b"value");
        restored.close().unwrap();
    }
}

#[test]
fn dropping_export_handles_does_not_release_protection_or_abort_copies() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let db = database(&source);
    db.put(0, b"key", 0, b"value").unwrap();
    let snapshot = db.create_snapshot_and_wait("dropped handle").unwrap();
    let destination = volume(&root.path().join("destination"));
    let gate = Arc::new(Gate::default());
    let fs = Arc::new(TestFs::new(&destination, Some(gate.clone()), false));
    let handle = db
        .export_snapshot_to_filesystem(snapshot, destination, Some(fs.clone()))
        .unwrap();
    gate.wait();
    db.expire_snapshot(snapshot).unwrap();
    drop(handle);
    assert!(
        source
            .join(db.id())
            .join(snapshot_manifest_name(snapshot))
            .exists()
    );
    let (tx, rx) = std::sync::mpsc::channel();
    let closing = db.clone();
    let thread = std::thread::spawn(move || tx.send(closing.close()).unwrap());
    assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
    gate.release();
    rx.recv_timeout(Duration::from_secs(20)).unwrap().unwrap();
    thread.join().unwrap();
    assert_eq!(fs.active.load(Ordering::SeqCst), 0);
    assert!(
        !source
            .join(db.id())
            .join(snapshot_manifest_name(snapshot))
            .exists()
    );
}
