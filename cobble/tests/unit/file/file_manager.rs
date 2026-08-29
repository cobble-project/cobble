use super::*;
use crate::data_file::{DataFile, DataFileType};
use crate::file::FileSystemRegistry;
use crate::file::files::File;
use crate::metrics_manager::MetricsManager;
use std::sync::atomic::AtomicBool;

pub(crate) mod test_utils {
    use crate::file::file_system::FileSystem;
    use std::sync::Arc;

    pub(crate) fn wait_for_file_deletion(fs: &Arc<dyn FileSystem>, path: &str) {
        for _ in 0..50 {
            if !fs.exists(path).unwrap() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
}

impl FileManager {
    pub(crate) fn preferred_replica_key(&self, file_id: FileId) -> Result<ReplicaKey> {
        let replica = self.preferred_readable_replica(file_id)?;
        Ok(ReplicaKey {
            file_id,
            replica_id: replica.replica_id,
        })
    }
}

static TEST_ROOT: &str = "file:///tmp/file_manager_test";

fn cleanup_test_root() {
    let _ = std::fs::remove_dir_all("/tmp/file_manager_test");
}

fn create_test_file_manager() -> (Arc<dyn FileSystem>, FileManager) {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register(TEST_ROOT).unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-test"));
    let fm = FileManager::with_defaults(Arc::clone(&fs), metrics_manager).unwrap();
    (fs, fm)
}

#[test]
#[serial_test::serial(file)]
fn resume_residual_scan_registers_all_matching_primary_tiers() {
    let root = "/tmp/file_manager_resume_residual_scan";
    let _ = std::fs::remove_dir_all(root);
    let high_root = format!("{root}/high");
    let low_root = format!("{root}/low");
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{high_root}"),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{low_root}"),
                vec![VolumeUsageKind::PrimaryDataPriorityLow],
            ),
        ],
        ..Config::default()
    };
    for volume_root in [&high_root, &low_root] {
        std::fs::create_dir_all(format!("{volume_root}/db/data/nested")).unwrap();
        std::fs::write(format!("{volume_root}/db/data/shared.sst"), b"shared").unwrap();
        std::fs::write(
            format!("{volume_root}/db/data/nested/ignored.sst"),
            b"ignored",
        )
        .unwrap();
    }
    let fm = FileManager::from_config(
        &config,
        "db",
        Arc::new(MetricsManager::new("resume-residual-scan")),
    )
    .unwrap();

    let candidates = fm.scan_primary_residual_files();
    assert_eq!(candidates.len(), 2);
    assert!(
        candidates
            .iter()
            .all(|candidate| candidate.file_name == "shared.sst")
    );
    for candidate in candidates {
        fm.register_primary_residual_replica(7, &candidate.absolute_path, candidate.size_bytes)
            .unwrap();
    }
    let logical = fm.get_logical_file(7).unwrap();
    assert_eq!(logical.replica_ids().len(), 2);
    assert!(fm.select_primary_residual_replica(7, PrimaryDataPlacement::Standard));
    assert_eq!(
        logical
            .preferred_replica_any()
            .unwrap()
            .tracked
            .volume
            .as_ref()
            .unwrap()
            .priority
            .rank(),
        3
    );
    fm.commit_logical_files([7]);
    fm.adopt_primary_residual_replicas(7);
    assert!(
        logical
            .replica_state_snapshot()
            .replicas
            .iter()
            .all(|replica| {
                replica.lifecycle() == ReplicaLifecycle::OwnedReady
                    && replica.tracked.physical_delete_policy()
                        == PhysicalDeletePolicy::ManagedDelete
            })
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_logical_file_attachment_metadata_and_replica_replacement() {
    let (fs, fm) = create_test_file_manager();
    let fm = Arc::new(fm);
    let (file_id, mut writer) = fm.create_data_file().unwrap();
    writer.write(b"data").unwrap();
    writer.close().unwrap();

    let file = DataFile::new(
        DataFileType::SSTable,
        b"a".to_vec(),
        b"z".to_vec(),
        file_id,
        TrackedFileId::new(&fm, file_id),
        7,
        4,
        0..=0,
        0..=0,
    )
    .with_separated_values(true);
    file.set_max_expired_at(42);
    let logical = file.logical_file().unwrap();
    assert_eq!(logical.metadata(), None);
    fm.finalize_data_file(&file).unwrap();

    assert_eq!(logical.commit_state(), FileCommitState::Uncommitted);
    assert_eq!(logical.metadata(), Some(file.build_immutable_metadata()));
    assert!(matches!(
        logical.preferred_replica_lifecycle(),
        Some(ReplicaLifecycle::OwnedReady)
    ));
    assert!(logical.preferred_replica().is_some());
    let old_reader = fm.open_data_file_reader(file_id).unwrap();
    assert_eq!(old_reader.read_at(0, 4).unwrap().as_ref(), b"data");
    let old_cache_key = fm.preferred_replica_key(file_id).unwrap();
    assert!(fm.reader_cache.lock().unwrap().contains_key(&old_cache_key));

    let conflicting = DataFile::new(
        DataFileType::SSTable,
        b"b".to_vec(),
        b"z".to_vec(),
        file_id,
        TrackedFileId::new(&fm, file_id),
        7,
        4,
        0..=0,
        0..=0,
    );
    conflicting.set_max_expired_at(42);
    assert!(fm.finalize_data_file(&conflicting).is_err());

    let path = fm.get_data_file_full_path(file_id).unwrap();
    let mut replacement_writer = fs.open_write("replacement.sst").unwrap();
    replacement_writer.write(b"next").unwrap();
    replacement_writer.close().unwrap();
    let replacement = Arc::new(TrackedFile::managed(
        "replacement.sst".to_string(),
        Arc::clone(&fs),
        None,
    ));
    let source = fm.data_file_ref(file_id).unwrap();
    let source_replica_id = fm
        .select_new_replica_retaining_source_if(file_id, &source, Arc::clone(&replacement))
        .unwrap();
    fm.retire_replica(file_id, source_replica_id);
    assert!(
        fm.select_new_replica_retaining_source_if(file_id, &source, Arc::clone(&source))
            .is_none()
    );
    assert!(Arc::ptr_eq(
        &logical.preferred_replica().unwrap().tracked,
        &replacement
    ));
    let new_reader = fm.open_data_file_reader(file_id).unwrap();
    assert_eq!(new_reader.read_at(0, 4).unwrap().as_ref(), b"next");
    assert_eq!(old_reader.read_at(0, 4).unwrap().as_ref(), b"data");
    assert!(!fm.reader_cache.lock().unwrap().contains_key(&old_cache_key));
    let new_cache_key = fm.preferred_replica_key(file_id).unwrap();
    assert_ne!(new_cache_key, old_cache_key);
    assert!(fm.reader_cache.lock().unwrap().contains_key(&new_cache_key));

    let persistent_file_id = file_id + 10;
    let leased_file_id = file_id + 11;
    for (path, bytes) in [
        ("external-persistent.sst", b"persistent".as_slice()),
        ("external-leased.sst", b"leased".as_slice()),
    ] {
        let mut writer = fs.open_write(path).unwrap();
        writer.write(bytes).unwrap();
        writer.close().unwrap();
    }
    fm.register_external_persistent_replica(
        persistent_file_id,
        "external-persistent.sst",
        "snapshot-source".to_string(),
    )
    .unwrap();
    fm.register_external_leased_replica(
        leased_file_id,
        "external-leased.sst",
        "runtime-export".to_string(),
    )
    .unwrap();
    {
        let consumer = FileManager::with_defaults(
            Arc::clone(&fs),
            Arc::new(MetricsManager::new("file-manager-external-consumer")),
        )
        .unwrap();
        consumer
            .register_external_persistent_replica(
                persistent_file_id,
                "external-persistent.sst",
                "snapshot-source".to_string(),
            )
            .unwrap();
        consumer
            .register_external_leased_replica(
                leased_file_id,
                "external-leased.sst",
                "runtime-export".to_string(),
            )
            .unwrap();
        assert!(matches!(
            consumer.preferred_replica_origin(persistent_file_id),
            Some(ReplicaOrigin::ExternalPersistent { ref source_id }) if source_id == "snapshot-source"
        ));
        assert!(matches!(
            consumer.preferred_replica_origin(leased_file_id),
            Some(ReplicaOrigin::ExternalLeased { ref export_id }) if export_id == "runtime-export"
        ));
    }
    assert!(fs.exists("external-persistent.sst").unwrap());
    assert!(fs.exists("external-leased.sst").unwrap());

    fm.register_data_file_readonly(file_id + 1, &path).unwrap();
    assert_eq!(
        fm.get_logical_file(file_id + 1)
            .unwrap()
            .preferred_replica_lifecycle(),
        Some(ReplicaLifecycle::ReadonlyView)
    );
    assert_eq!(
        fm.data_file_ref(file_id + 1)
            .unwrap()
            .physical_delete_policy(),
        PhysicalDeletePolicy::Retained
    );
    fm.register_data_file_pending_adoption(file_id + 2, &path)
        .unwrap();
    let pending = fm.get_logical_file(file_id + 2).unwrap();
    assert_eq!(pending.commit_state(), FileCommitState::Uncommitted);
    assert_eq!(
        pending.preferred_replica_lifecycle(),
        Some(ReplicaLifecycle::PendingAdoption)
    );
    assert!(pending.preferred_replica().is_some());
    assert_eq!(
        fm.data_file_ref(file_id + 2)
            .unwrap()
            .physical_delete_policy(),
        PhysicalDeletePolicy::Retained
    );
    fm.commit_logical_files([file_id + 2]);
    fm.adopt_data_file(file_id + 2).unwrap();
    assert_eq!(pending.commit_state(), FileCommitState::Committed);
    assert_eq!(
        pending.preferred_replica_lifecycle(),
        Some(ReplicaLifecycle::OwnedReady)
    );
    assert_eq!(
        fm.data_file_ref(file_id + 2)
            .unwrap()
            .physical_delete_policy(),
        PhysicalDeletePolicy::ManagedDelete
    );
    fm.register_uncommitted_data_file(file_id + 3, &path)
        .unwrap();
    assert_eq!(
        fm.data_file_ref(file_id + 3)
            .unwrap()
            .physical_delete_policy(),
        PhysicalDeletePolicy::ManagedDelete
    );

    cleanup_test_root();
}

struct TestCopyReader {
    data: Bytes,
}

impl File for TestCopyReader {
    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn size(&self) -> usize {
        self.data.len()
    }
}

impl RandomAccessFile for TestCopyReader {
    fn prefers_read_ahead(&self) -> bool {
        true
    }

    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error> {
        Ok(self.data.slice(offset..offset + size))
    }
}

struct TestCopyWriter {
    data: Arc<Mutex<Vec<u8>>>,
    close_called: Arc<AtomicBool>,
    cancel_after_write: Option<Arc<AtomicU8>>,
}

impl File for TestCopyWriter {
    fn close(&mut self) -> Result<(), Error> {
        self.close_called.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn size(&self) -> usize {
        self.data.lock().unwrap().len()
    }
}

impl SequentialWriteFile for TestCopyWriter {
    fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        self.data.lock().unwrap().extend_from_slice(data);
        if let Some(state) = &self.cancel_after_write {
            state.store(SnapshotLifecycleState::Cancelled as u8, Ordering::SeqCst);
        }
        Ok(data.len())
    }
}

fn test_copy_writer(
    fs: &Arc<dyn FileSystem>,
    cancel_after_write: Option<Arc<AtomicU8>>,
) -> (TrackedWriter, Arc<Mutex<Vec<u8>>>, Arc<AtomicBool>) {
    let data = Arc::new(Mutex::new(Vec::new()));
    let close_called = Arc::new(AtomicBool::new(false));
    let inner = TestCopyWriter {
        data: Arc::clone(&data),
        close_called: Arc::clone(&close_called),
        cancel_after_write,
    };
    let tracked = Arc::new(TrackedFile::managed(
        "test-copy".to_string(),
        Arc::clone(fs),
        None,
    ));
    (
        TrackedWriter::new(Box::new(inner), tracked),
        data,
        close_called,
    )
}

#[test]
#[serial_test::serial(file)]
fn test_copy_reader_cancels_after_last_write_before_close() {
    let (fs, fm) = create_test_file_manager();
    let lifecycle_state = Arc::new(AtomicU8::new(0));
    let (mut writer, data, close_called) =
        test_copy_writer(&fs, Some(Arc::clone(&lifecycle_state)));
    let source = TestCopyReader {
        data: Bytes::from_static(b"copy-me"),
    };

    let result = fm.copy_reader_to_tracked_writer_with_cancel(
        &source,
        &mut writer,
        Some(lifecycle_state.as_ref()),
    );

    assert!(matches!(result, Err(Error::CancelledError(_))));
    assert_eq!(data.lock().unwrap().as_slice(), b"copy-me");
    assert!(!close_called.load(Ordering::SeqCst));
    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_random_access_reader_wrappers_preserve_read_ahead_capability() {
    let (fs, _fm) = create_test_file_manager();
    let inner: Arc<dyn RandomAccessFile> = Arc::new(TestCopyReader {
        data: Bytes::from_static(b"remote"),
    });
    let cached: Arc<dyn RandomAccessFile> =
        Arc::new(CachedRandomAccessFile::new(Arc::clone(&inner)));
    assert!(cached.prefers_read_ahead());

    let tracked = Arc::new(TrackedFile::managed("test-reader".to_string(), fs, None));
    let tracked_reader = TrackedReader::new(cached, tracked);
    assert!(tracked_reader.prefers_read_ahead());
    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_copy_reader_closes_after_last_write_without_cancellation() {
    let (fs, fm) = create_test_file_manager();
    let (mut writer, data, close_called) = test_copy_writer(&fs, None);
    let source = TestCopyReader {
        data: Bytes::from_static(b"copy-me"),
    };

    fm.copy_reader_to_tracked_writer_with_cancel(&source, &mut writer, None)
        .unwrap();

    assert_eq!(data.lock().unwrap().as_slice(), b"copy-me");
    assert!(close_called.load(Ordering::SeqCst));
    cleanup_test_root();
}

#[test]
fn data_volume_clones_share_usage_and_saturate_removal() {
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/file_manager_usage_test")
        .unwrap();
    let volume = DataVolume {
        fs,
        base_dir: None,
        size_limit: None,
        used_bytes: Arc::new(AtomicU64::new(0)),
        storage_file_bytes: None,
        projected_offload_bytes: AtomicU64::new(0),
        priority: VolumePriority::High,
        supports_primary_data: true,
        supports_meta: true,
        snapshot_persistable: true,
        readonly_source: false,
    };
    let clone = volume.clone();
    volume.add_usage(17);
    assert_eq!(clone.used_bytes.load(Ordering::SeqCst), 17);
    clone.subtract_usage(99);
    assert_eq!(volume.used_bytes.load(Ordering::SeqCst), 0);
    volume
        .used_bytes
        .store(u64::MAX.saturating_sub(2), Ordering::SeqCst);
    clone.add_usage(9);
    assert_eq!(volume.used_bytes.load(Ordering::SeqCst), u64::MAX);
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_create_data_file() {
    let (_fs, fm) = create_test_file_manager();

    // Create a data file
    let (file_id, mut writer) = fm.create_data_file().unwrap();
    assert_eq!(file_id, 1);
    assert!(fm.has_data_file(file_id));

    // Write some data
    writer.write(b"test data").unwrap();
    writer.close().unwrap();

    // Verify we can read it back
    let reader = fm.open_data_file_reader(file_id).unwrap();
    let data = reader.read_at(0, 9).unwrap();
    assert_eq!(&data[..], b"test data");

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_multiple_data_files() {
    let (_fs, fm) = create_test_file_manager();

    // Create multiple files
    let (id1, mut w1) = fm.create_data_file().unwrap();
    let (id2, mut w2) = fm.create_data_file().unwrap();
    let (id3, mut w3) = fm.create_data_file().unwrap();

    assert_eq!(id1, 1);
    assert_eq!(id2, 2);
    assert_eq!(id3, 3);
    assert_eq!(fm.data_file_count(), 3);

    // Write different data to each
    w1.write(b"file1").unwrap();
    w2.write(b"file2").unwrap();
    w3.write(b"file3").unwrap();
    w1.close().unwrap();
    w2.close().unwrap();
    w3.close().unwrap();

    // Read back and verify
    let r1 = fm.open_data_file_reader(id1).unwrap();
    let r2 = fm.open_data_file_reader(id2).unwrap();
    let r3 = fm.open_data_file_reader(id3).unwrap();

    assert_eq!(&r1.read_at(0, 5).unwrap()[..], b"file1");
    assert_eq!(&r2.read_at(0, 5).unwrap()[..], b"file2");
    assert_eq!(&r3.read_at(0, 5).unwrap()[..], b"file3");

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_remove_data_file() {
    let (fs, fm) = create_test_file_manager();

    // Create and write a file
    let (file_id, mut writer) = fm.create_data_file().unwrap();
    writer.write(b"data").unwrap();
    writer.close().unwrap();
    drop(writer); // Drop the writer to release the Arc reference

    let path = fm.get_data_file_path(file_id).unwrap();
    assert!(fs.exists(&path).unwrap());

    // Remove with delete
    fm.remove_data_file(file_id).unwrap();
    assert!(!fm.has_data_file(file_id));
    test_utils::wait_for_file_deletion(&fs, &path);
    assert!(!fs.exists(&path).unwrap());

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_remove_data_file_with_snapshot_ref() {
    let (fs, fm) = create_test_file_manager();

    let (file_id, mut writer) = fm.create_data_file().unwrap();
    writer.write(b"data").unwrap();
    writer.close().unwrap();
    drop(writer);

    let tracked = fm.data_file_ref(file_id).unwrap();
    let path = fm.get_data_file_path(file_id).unwrap();

    fm.remove_data_file(file_id).unwrap();
    assert!(!fm.has_data_file(file_id));
    assert!(fs.exists(&path).unwrap());

    tracked.dereference();
    assert!(fs.exists(&path).unwrap());
    drop(tracked);
    test_utils::wait_for_file_deletion(&fs, &path);
    assert!(!fs.exists(&path).unwrap());

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_metadata_files() {
    let (_fs, fm) = create_test_file_manager();
    let initial_usage = fm.meta_volume.used_bytes.load(Ordering::SeqCst);

    // Create a metadata file
    let mut writer = fm.create_metadata_file("manifest").unwrap();
    writer.write(b"manifest data").unwrap();
    writer.close().unwrap();

    assert!(fm.meta_volume.used_bytes.load(Ordering::SeqCst) > initial_usage);

    assert!(fm.has_metadata_file("manifest"));
    assert_eq!(fm.metadata_file_count(), 1);

    // Read it back
    let reader = fm.open_metadata_file_reader("manifest").unwrap();
    let data = reader.read_at(0, 13).unwrap();
    assert_eq!(&data[..], b"manifest data");
    drop(reader);

    // Remove it
    fm.remove_metadata_file("manifest").unwrap();
    assert!(!fm.has_metadata_file("manifest"));
    assert_eq!(
        fm.meta_volume.used_bytes.load(Ordering::SeqCst),
        initial_usage
    );

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_set_next_file_id() {
    let (_fs, fm) = create_test_file_manager();

    // Set a custom starting ID
    fm.set_next_file_id(100);
    assert_eq!(fm.peek_next_file_id(), 100);

    let (file_id, mut writer) = fm.create_data_file().unwrap();
    assert_eq!(file_id, 100);
    writer.close().unwrap();

    let (file_id2, mut writer2) = fm.create_data_file().unwrap();
    assert_eq!(file_id2, 101);
    writer2.close().unwrap();

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_create_with_specific_id() {
    let (_fs, fm) = create_test_file_manager();

    // Create file with specific ID
    let mut writer = fm.create_data_file_with_id(50, None).unwrap();
    writer.write(b"data50").unwrap();
    writer.close().unwrap();

    assert!(fm.has_data_file(50));
    assert_eq!(fm.peek_next_file_id(), 51);

    // Should fail if ID already exists
    let result = fm.create_data_file_with_id(50, None);
    assert!(result.is_err());

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_register_existing_file() {
    let (fs, fm) = create_test_file_manager();

    // Create a file directly on the filesystem
    let path = "data/existing_file.sst";
    let mut writer = fs.open_write(path).unwrap();
    writer.write(b"existing").unwrap();
    writer.close().unwrap();

    // Register it with path
    fm.register_data_file(999, path).unwrap();
    assert!(fm.has_data_file(999));
    assert_eq!(fm.peek_next_file_id(), 1000);

    // Can read it
    let reader = fm.open_data_file_reader(999).unwrap();
    let data = reader.read_at(0, 8).unwrap();
    assert_eq!(&data[..], b"existing");

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_data_file_ids() {
    let (_fs, fm) = create_test_file_manager();

    let (id1, mut w1) = fm.create_data_file().unwrap();
    let (id2, mut w2) = fm.create_data_file().unwrap();
    w1.close().unwrap();
    w2.close().unwrap();

    let mut ids = fm.data_file_ids();
    ids.sort();
    assert_eq!(ids, vec![id1, id2]);

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_metadata_file_names() {
    let (_fs, fm) = create_test_file_manager();

    let mut w1 = fm.create_metadata_file("manifest").unwrap();
    let mut w2 = fm.create_metadata_file("wal").unwrap();
    w1.close().unwrap();
    w2.close().unwrap();

    let mut names = fm.metadata_file_names();
    names.sort();
    assert_eq!(names, vec!["manifest", "wal"]);

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_volume_priority_selection() {
    let root = "/tmp/file_manager_volume_priority";
    let _ = std::fs::remove_dir_all(root);
    let registry = FileSystemRegistry::new();
    let high_fs = registry
        .get_or_register(format!("file://{}/high", root))
        .unwrap();
    let low_fs = registry
        .get_or_register(format!("file://{}/low", root))
        .unwrap();
    let high_volume = DataVolume {
        fs: Arc::clone(&high_fs),
        base_dir: Some(format!("{}/high", root)),
        size_limit: Some(128),
        used_bytes: Arc::new(AtomicU64::new(0)),
        storage_file_bytes: None,
        projected_offload_bytes: AtomicU64::new(0),
        priority: VolumePriority::High,
        supports_primary_data: true,
        supports_meta: false,
        snapshot_persistable: true,
        readonly_source: false,
    };
    let low_volume = DataVolume {
        fs: Arc::clone(&low_fs),
        base_dir: Some(format!("{}/low", root)),
        size_limit: None,
        used_bytes: Arc::new(AtomicU64::new(0)),
        storage_file_bytes: None,
        projected_offload_bytes: AtomicU64::new(0),
        priority: VolumePriority::Low,
        supports_primary_data: true,
        supports_meta: false,
        snapshot_persistable: false,
        readonly_source: false,
    };
    let options = FileManagerOptions {
        base_dir: "db".to_string(),
        base_file_size: 64,
        ..FileManagerOptions::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-test"));
    let fm = FileManager::new(vec![high_volume, low_volume], options, metrics_manager).unwrap();

    let (file_id1, mut writer1) = fm.create_data_file().unwrap();
    writer1.write(&[b'a'; 80]).unwrap();
    writer1.close().unwrap();
    let path1 = fm.get_data_file_path(file_id1).unwrap();
    assert!(high_fs.exists(&path1).unwrap());

    let (file_id2, mut writer2) = fm.create_data_file().unwrap();
    writer2.write(&[b'b'; 8]).unwrap();
    writer2.close().unwrap();
    let path2 = fm.get_data_file_path(file_id2).unwrap();
    assert!(low_fs.exists(&path2).unwrap());

    let manifest = "manifest";
    let mut meta_writer = fm.create_metadata_file(manifest).unwrap();
    meta_writer.write(b"meta").unwrap();
    meta_writer.close().unwrap();
    let meta_path = fm.get_metadata_file_path(manifest).unwrap();
    assert!(high_fs.exists(&meta_path).unwrap());

    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_snapshot_volume_for_metadata_and_copy() {
    crate::metrics_registry::init_metrics();
    let root = "/tmp/file_manager_snapshot_volume";
    let _ = std::fs::remove_dir_all(root);
    let primary_url = format!("file://{}/primary", root);
    let snapshot_url = format!("file://{}/snapshot", root);
    let registry = FileSystemRegistry::new();
    let primary_fs = registry.get_or_register(primary_url.clone()).unwrap();
    let snapshot_fs = registry.get_or_register(snapshot_url.clone()).unwrap();
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                primary_url,
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                snapshot_url,
                vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
            ),
        ],
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-snapshot"));
    let db_id = "file-manager-storage-metrics";
    let fm = FileManager::from_config(&config, db_id, metrics_manager).unwrap();

    let (source_file_id, mut source_writer) = fm.create_data_file().unwrap();
    source_writer.write(b"source-bytes").unwrap();
    source_writer.close().unwrap();
    drop(source_writer);
    let source_path = fm.get_data_file_path(source_file_id).unwrap();
    assert!(primary_fs.exists(&source_path).unwrap());

    let mut metadata_writer = fm.create_metadata_file("snapshot/MANIFEST").unwrap();
    metadata_writer.write(b"manifest").unwrap();
    metadata_writer.close().unwrap();
    drop(metadata_writer);
    let metadata_path = fm.get_metadata_file_path("snapshot/MANIFEST").unwrap();
    assert!(snapshot_fs.exists(&metadata_path).unwrap());
    assert!(!primary_fs.exists(&metadata_path).unwrap());

    assert_storage_file_bytes(db_id, "0", 12.0);
    assert_storage_file_bytes(db_id, "1", 16.0);

    let source = fm.data_file_ref(source_file_id).unwrap();
    let logical = fm.get_logical_file(source_file_id).unwrap();
    fm.remove_data_file(source_file_id).unwrap();
    assert!(!fm.has_data_file(source_file_id));
    let copied = fm
        .snapshot_replica_for_tracked_file(source_file_id, &source, Some(&logical), None, None)
        .unwrap();
    let copied_path = copied.path().to_string();
    let pinned_path = copied.absolute_path();
    assert!(snapshot_fs.exists(&copied_path).unwrap());
    let copied_reader = copied.fs().open_read(&copied_path).unwrap();
    assert_eq!(&copied_reader.read_at(0, 12).unwrap()[..], b"source-bytes");
    drop(copied_reader);
    assert_storage_file_bytes(db_id, "0", 12.0);
    assert_storage_file_bytes(db_id, "1", 28.0);

    let replica_id = logical
        .replica_ids()
        .into_iter()
        .find(|replica_id| *replica_id != 0)
        .unwrap();
    logical.set_replica_lifecycle(replica_id, ReplicaLifecycle::OwnedReady);
    let copied_again = fm
        .snapshot_replica_for_tracked_file(source_file_id, &source, Some(&logical), None, None)
        .unwrap();
    assert_eq!(copied.absolute_path(), copied_again.absolute_path());
    assert!(logical.retain_and_select_replica_if(&source, replica_id));
    assert!(logical.select_durable_and_preferred(replica_id));
    assert_eq!(copied.absolute_path(), pinned_path);
    assert!(logical.remove_replica(0).is_some());
    source.dereference();
    drop(source);
    fm.remove_metadata_file("snapshot/MANIFEST").unwrap();
    test_utils::wait_for_file_deletion(&primary_fs, &source_path);
    assert_storage_file_bytes(db_id, "0", 0.0);
    assert_storage_file_bytes(db_id, "1", 12.0);

    drop(copied_again);
    drop(copied);
    drop(logical);
    test_utils::wait_for_file_deletion(&snapshot_fs, &copied_path);
    assert_storage_file_bytes(db_id, "1", 0.0);
    let _ = std::fs::remove_dir_all(root);
}

fn assert_storage_file_bytes(db_id: &str, volume: &str, expected: f64) {
    let sample = crate::metrics_registry::snapshot_metrics(Some(db_id))
        .into_iter()
        .find(|sample| {
            sample.name == "storage_file_bytes"
                && sample
                    .labels
                    .iter()
                    .any(|(key, value)| key == "volume" && value == volume)
        })
        .expect("configured volume must expose a storage-byte gauge");
    assert!(matches!(sample.value, crate::MetricValue::Gauge(value) if value == expected));
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_meta_volume_uses_first_snapshot_only_volume() {
    let root = "/tmp/file_manager_snapshot_meta_first";
    let _ = std::fs::remove_dir_all(root);
    let primary_url = format!("file://{}/primary", root);
    let snapshot_a_url = format!("file://{}/snapshot-a", root);
    let snapshot_b_url = format!("file://{}/snapshot-b", root);
    let registry = FileSystemRegistry::new();
    let snapshot_a_fs = registry.get_or_register(snapshot_a_url.clone()).unwrap();
    let snapshot_b_fs = registry.get_or_register(snapshot_b_url.clone()).unwrap();
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                primary_url,
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(snapshot_a_url, vec![VolumeUsageKind::Snapshot]),
            crate::VolumeDescriptor::new(snapshot_b_url, vec![VolumeUsageKind::Snapshot]),
        ],
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-snapshot-meta-first"));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
    let mut metadata_writer = fm.create_metadata_file("snapshot/MANIFEST").unwrap();
    metadata_writer.write(b"manifest").unwrap();
    metadata_writer.close().unwrap();
    let metadata_path = fm.get_metadata_file_path("snapshot/MANIFEST").unwrap();
    assert!(snapshot_a_fs.exists(&metadata_path).unwrap());
    assert!(!snapshot_b_fs.exists(&metadata_path).unwrap());
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_meta_volume_prefers_meta_kind_over_snapshot_kind() {
    let root = "/tmp/file_manager_meta_kind_preferred";
    let _ = std::fs::remove_dir_all(root);
    let primary_url = format!("file://{}/primary", root);
    let snapshot_url = format!("file://{}/snapshot", root);
    let meta_url = format!("file://{}/meta", root);
    let registry = FileSystemRegistry::new();
    let snapshot_fs = registry.get_or_register(snapshot_url.clone()).unwrap();
    let meta_fs = registry.get_or_register(meta_url.clone()).unwrap();
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                primary_url,
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(snapshot_url, vec![VolumeUsageKind::Snapshot]),
            crate::VolumeDescriptor::new(meta_url, vec![VolumeUsageKind::Meta]),
        ],
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-meta-kind"));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();

    let mut metadata_writer = fm.create_metadata_file("snapshot/MANIFEST").unwrap();
    metadata_writer.write(b"manifest").unwrap();
    metadata_writer.close().unwrap();
    let metadata_path = fm.get_metadata_file_path("snapshot/MANIFEST").unwrap();
    assert!(meta_fs.exists(&metadata_path).unwrap());
    assert!(!snapshot_fs.exists(&metadata_path).unwrap());

    let (source_file_id, mut source_writer) = fm.create_data_file().unwrap();
    source_writer.write(b"source-bytes").unwrap();
    source_writer.close().unwrap();
    let source = fm.data_file_ref(source_file_id).unwrap();
    let logical = fm.get_logical_file(source_file_id).unwrap();
    let copied = fm
        .snapshot_replica_for_tracked_file(source_file_id, &source, Some(&logical), None, None)
        .unwrap();
    source.dereference();
    let copied_path = copied.path().to_string();
    assert!(snapshot_fs.exists(&copied_path).unwrap());
    assert!(!meta_fs.exists(&copied_path).unwrap());
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_meta_volume_uses_first_meta_volume() {
    let root = "/tmp/file_manager_meta_first_meta_volume";
    let _ = std::fs::remove_dir_all(root);
    let primary_url = format!("file://{}/primary", root);
    let meta_a_url = format!("file://{}/meta-a", root);
    let meta_b_url = format!("file://{}/meta-b", root);
    let registry = FileSystemRegistry::new();
    let meta_a_fs = registry.get_or_register(meta_a_url.clone()).unwrap();
    let meta_b_fs = registry.get_or_register(meta_b_url.clone()).unwrap();
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                primary_url,
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(meta_a_url, vec![VolumeUsageKind::Meta]),
            crate::VolumeDescriptor::new(meta_b_url, vec![VolumeUsageKind::Meta]),
        ],
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-meta-first-meta"));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();

    let mut metadata_writer = fm.create_metadata_file("snapshot/MANIFEST").unwrap();
    metadata_writer.write(b"manifest").unwrap();
    metadata_writer.close().unwrap();
    let metadata_path = fm.get_metadata_file_path("snapshot/MANIFEST").unwrap();
    assert!(meta_a_fs.exists(&metadata_path).unwrap());
    assert!(!meta_b_fs.exists(&metadata_path).unwrap());
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_meta_volume_uses_first_snapshot_persistable_when_no_meta() {
    let root = "/tmp/file_manager_snapshot_meta_shared_priority";
    let _ = std::fs::remove_dir_all(root);
    let high_url = format!("file://{}/high", root);
    let low_url = format!("file://{}/low", root);
    let registry = FileSystemRegistry::new();
    let high_fs = registry.get_or_register(high_url.clone()).unwrap();
    let low_fs = registry.get_or_register(low_url.clone()).unwrap();
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                low_url,
                vec![
                    VolumeUsageKind::Snapshot,
                    VolumeUsageKind::PrimaryDataPriorityLow,
                ],
            ),
            crate::VolumeDescriptor::new(
                high_url,
                vec![
                    VolumeUsageKind::Snapshot,
                    VolumeUsageKind::PrimaryDataPriorityHigh,
                ],
            ),
        ],
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-snapshot-meta-first"));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
    let mut metadata_writer = fm.create_metadata_file("snapshot/MANIFEST").unwrap();
    metadata_writer.write(b"manifest").unwrap();
    metadata_writer.close().unwrap();
    let metadata_path = fm.get_metadata_file_path("snapshot/MANIFEST").unwrap();
    assert!(low_fs.exists(&metadata_path).unwrap());
    assert!(!high_fs.exists(&metadata_path).unwrap());
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_rejects_readonly_volume_with_other_kinds() {
    let root = "/tmp/file_manager_readonly_kinds";
    let _ = std::fs::remove_dir_all(root);
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}/primary", root),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}/bad", root),
                vec![VolumeUsageKind::Readonly, VolumeUsageKind::Snapshot],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}/snapshot", root),
                vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
            ),
        ],
        ..Config::default()
    };
    let err = match FileManager::data_volumes_from_config(&config) {
        Ok(_) => panic!("expected readonly exclusivity error"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("readonly must be exclusive"),
        "unexpected error: {}",
        err
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_register_data_file_for_restore_copies_from_readonly_source() {
    let root = "/tmp/file_manager_restore_readonly_snapshot_dir";
    let primary_root = format!("{}/primary", root);
    let snapshot_root = format!("{}/snapshot", root);
    let readonly_root = format!("{}/readonly", root);
    let _ = std::fs::remove_dir_all(root);
    let registry = FileSystemRegistry::new();
    let readonly_fs = registry
        .get_or_register(format!("file://{}", readonly_root))
        .unwrap();
    readonly_fs.create_dir("db").unwrap();
    readonly_fs.create_dir("db/snapshot").unwrap();
    let source_path = "db/snapshot/source.sst";
    let mut source_writer = readonly_fs.open_write(source_path).unwrap();
    source_writer.write(b"restore-source").unwrap();
    source_writer.close().unwrap();

    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}", primary_root),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}", snapshot_root),
                vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}", readonly_root),
                vec![VolumeUsageKind::Readonly],
            ),
        ],
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-restore-readonly"));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
    let source_full_path = format!("file://{}/{}", readonly_root, source_path);
    fm.register_data_file_for_restore(
        42,
        &source_full_path,
        ReplicaOrigin::ExternalPersistent {
            source_id: "readonly-snapshot".to_string(),
        },
        PrimaryDataPlacement::Standard,
        None,
        None,
    )
    .unwrap();
    assert!(fm.is_data_file_on_primary_volume(42));
    assert_eq!(fm.preferred_replica_origin(42), Some(ReplicaOrigin::Owned));
    assert_eq!(
        fm.get_data_file_path(42).as_deref(),
        Some("db/data/source.sst")
    );
    let restored_reader = fm.open_data_file_reader(42).unwrap();
    assert_eq!(
        &restored_reader.read_at(0, "restore-source".len()).unwrap()[..],
        b"restore-source"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_register_data_file_for_restore_places_snapshot_files_by_type() {
    let root = "/tmp/file_manager_restore_snapshot_volume_any_path";
    let primary_root = format!("{}/primary", root);
    let low_primary_root = format!("{}/low-primary", root);
    let snapshot_root = format!("{}/snapshot", root);
    let _ = std::fs::remove_dir_all(root);
    let registry = FileSystemRegistry::new();
    let snapshot_fs = registry
        .get_or_register(format!("file://{}", snapshot_root))
        .unwrap();
    snapshot_fs.create_dir("db").unwrap();
    snapshot_fs.create_dir("db/data").unwrap();
    let source_path = "db/data/source.sst";
    let mut source_writer = snapshot_fs.open_write(source_path).unwrap();
    source_writer.write(b"restore-source").unwrap();
    source_writer.close().unwrap();
    let vlog_source_path = "db/data/vlog.sst";
    let mut vlog_writer = snapshot_fs.open_write(vlog_source_path).unwrap();
    vlog_writer.write(b"vlog-restore-source").unwrap();
    vlog_writer.close().unwrap();

    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}", primary_root),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}", low_primary_root),
                vec![VolumeUsageKind::PrimaryDataPriorityLow],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}", snapshot_root),
                vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
            ),
        ],
        vlog_low_priority_primary_enabled: true,
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-restore-snapshot-any"));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
    let source_full_path = format!("file://{}/{}", snapshot_root, source_path);
    fm.register_data_file_for_restore(
        66,
        &source_full_path,
        ReplicaOrigin::ExternalPersistent {
            source_id: "snapshot-source".to_string(),
        },
        PrimaryDataPlacement::Standard,
        None,
        None,
    )
    .unwrap();
    assert!(fm.is_data_file_on_primary_volume(66));
    assert_eq!(
        fm.preferred_tracked_file(66)
            .and_then(|tracked| tracked.volume.as_ref().map(|volume| volume.priority.rank())),
        Some(3),
        "SST restore should retain the normal highest-priority placement"
    );
    assert!(fm.is_data_file_on_snapshot_volume(66));
    let logical = fm.get_logical_file(66).unwrap();
    assert_eq!(logical.replica_ids().len(), 2);
    assert!(matches!(
        logical
            .replica_at_absolute_path(&source_full_path)
            .map(|replica| replica.origin()),
        Some(ReplicaOrigin::ExternalPersistent { ref source_id }) if source_id == "snapshot-source"
    ));
    let vlog_source_full_path = format!("file://{snapshot_root}/{vlog_source_path}");
    fm.register_data_file_for_restore(
        67,
        &vlog_source_full_path,
        ReplicaOrigin::ExternalPersistent {
            source_id: "snapshot-source".to_string(),
        },
        PrimaryDataPlacement::Vlog,
        None,
        None,
    )
    .unwrap();

    assert_eq!(
        fm.preferred_tracked_file(67)
            .and_then(|tracked| tracked.volume.as_ref().map(|volume| volume.priority.rank())),
        Some(1)
    );
    assert!(fm.is_data_file_on_snapshot_volume(67));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_register_data_file_for_restore_shared_snapshot_volume_is_readonly() {
    let root = "/tmp/file_manager_restore_shared_snapshot_volume";
    let shared_root = format!("{}/shared", root);
    let _ = std::fs::remove_dir_all(root);
    std::fs::create_dir_all(format!("{}/db/data", shared_root)).unwrap();
    let source_local_path = format!("{}/db/data/source.sst", shared_root);
    std::fs::write(&source_local_path, b"restore-source").unwrap();

    let config = Config {
        volumes: vec![crate::VolumeDescriptor::new(
            format!("file://{}", shared_root),
            vec![
                VolumeUsageKind::PrimaryDataPriorityHigh,
                VolumeUsageKind::Snapshot,
                VolumeUsageKind::Meta,
            ],
        )],
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-restore-shared-snapshot"));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
    let source_full_path = format!("file://{}/db/data/source.sst", shared_root);
    fm.register_data_file_for_restore(
        88,
        &source_full_path,
        ReplicaOrigin::ExternalLeased {
            export_id: "shared-snapshot".to_string(),
        },
        PrimaryDataPlacement::Standard,
        None,
        None,
    )
    .unwrap();
    assert!(fm.is_data_file_on_snapshot_volume(88));
    fm.remove_data_file(88).unwrap();
    assert!(std::path::Path::new(&source_local_path).exists());
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_register_data_file_for_restore_copies_from_readonly_data_dir() {
    let root = "/tmp/file_manager_restore_readonly_data_dir";
    let primary_root = format!("{}/primary", root);
    let snapshot_root = format!("{}/snapshot", root);
    let readonly_root = format!("{}/readonly", root);
    let _ = std::fs::remove_dir_all(root);
    let registry = FileSystemRegistry::new();
    let readonly_fs = registry
        .get_or_register(format!("file://{}", readonly_root))
        .unwrap();
    readonly_fs.create_dir("db").unwrap();
    readonly_fs.create_dir("db/data").unwrap();
    let source_path = "db/data/source.sst";
    let mut source_writer = readonly_fs.open_write(source_path).unwrap();
    source_writer.write(b"restore-source").unwrap();
    source_writer.close().unwrap();

    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}", primary_root),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}", snapshot_root),
                vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}", readonly_root),
                vec![VolumeUsageKind::Readonly],
            ),
        ],
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-restore-readonly-data"));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
    let source_full_path = format!("file://{}/{}", readonly_root, source_path);
    fm.register_data_file_for_restore(
        77,
        &source_full_path,
        ReplicaOrigin::ExternalPersistent {
            source_id: "readonly-data".to_string(),
        },
        PrimaryDataPlacement::Standard,
        None,
        None,
    )
    .unwrap();
    assert!(fm.is_data_file_on_primary_volume(77));
    assert_eq!(
        fm.get_data_file_path(77).as_deref(),
        Some("db/data/source.sst")
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_register_data_file_for_restore_moves_primary_vlog_to_requested_tier() {
    let root = "/tmp/file_manager_restore_primary_source_owned";
    let primary_root = format!("{}/primary", root);
    let low_primary_root = format!("{}/low-primary", root);
    let _ = std::fs::remove_dir_all(root);
    std::fs::create_dir_all(format!("{}/db/data", primary_root)).unwrap();
    let source_local_path = format!("{}/db/data/source.sst", primary_root);
    std::fs::write(&source_local_path, b"restore-source").unwrap();

    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}", primary_root),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}", low_primary_root),
                vec![VolumeUsageKind::PrimaryDataPriorityLow],
            ),
        ],
        vlog_low_priority_primary_enabled: true,
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-restore-primary-owned"));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
    let source_full_path = format!("file://{}/db/data/source.sst", primary_root);
    fm.register_data_file_for_restore(
        120,
        &source_full_path,
        ReplicaOrigin::Owned,
        PrimaryDataPlacement::Vlog,
        None,
        None,
    )
    .unwrap();
    let tracked = fm.preferred_tracked_file(120).unwrap();
    assert_eq!(
        tracked.volume.as_ref().map(|volume| volume.priority.rank()),
        Some(1),
        "restore placement moves a VLOG source into the configured low tier"
    );
    assert_eq!(
        tracked.physical_delete_policy(),
        PhysicalDeletePolicy::ManagedDelete
    );
    let tracked_fs = Arc::clone(tracked.fs());
    let tracked_path = tracked.path().to_string();
    drop(tracked);
    fm.remove_data_file(120).unwrap();
    test_utils::wait_for_file_deletion(&tracked_fs, &tracked_path);
    assert!(
        std::path::Path::new(&source_local_path).exists(),
        "the copied source remains outside the restored logical-file lifecycle"
    );
    let _ = std::fs::remove_dir_all(root);
}
