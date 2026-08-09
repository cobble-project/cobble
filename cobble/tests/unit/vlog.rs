use super::*;
use crate::file::{FileManager, FileSystem, FileSystemRegistry, TrackedFileId};
use crate::metrics_manager::MetricsManager;
use crate::{Config, VolumeDescriptor, VolumeUsageKind};
use size::Size;
use std::sync::Arc;

static TEST_ROOT: &str = "file:///tmp/vlog_test";

type TieredFileManager = (
    Arc<FileManager>,
    Arc<dyn FileSystem>,
    Arc<dyn FileSystem>,
    Arc<dyn FileSystem>,
);

fn cleanup_test_root() {
    let _ = std::fs::remove_dir_all("/tmp/vlog_test");
}

fn tiered_file_manager(root: &str, vlog_low_priority_primary_enabled: bool) -> TieredFileManager {
    let registry = FileSystemRegistry::new();
    let high = registry
        .get_or_register(format!("file://{root}/high"))
        .unwrap();
    let low_a = registry
        .get_or_register(format!("file://{root}/low-a"))
        .unwrap();
    let low_b = registry
        .get_or_register(format!("file://{root}/low-b"))
        .unwrap();
    let config = Config {
        volumes: vec![
            VolumeDescriptor::new(
                format!("file://{root}/high"),
                vec![
                    VolumeUsageKind::PrimaryDataPriorityHigh,
                    VolumeUsageKind::Meta,
                ],
            ),
            VolumeDescriptor::new(
                format!("file://{root}/low-a"),
                vec![VolumeUsageKind::PrimaryDataPriorityLow],
            ),
            VolumeDescriptor::new(
                format!("file://{root}/low-b"),
                vec![VolumeUsageKind::PrimaryDataPriorityLow],
            ),
        ],
        vlog_low_priority_primary_enabled,
        ..Config::default()
    };
    let file_manager = Arc::new(
        FileManager::from_config(
            &config,
            "db",
            Arc::new(MetricsManager::new("vlog-tier-test")),
        )
        .unwrap(),
    );
    (file_manager, high, low_a, low_b)
}

#[test]
#[serial_test::serial(file)]
fn vlog_uses_lowest_primary_tier_when_enabled() {
    let root = "/tmp/vlog_lowest_primary";
    let _ = std::fs::remove_dir_all(root);
    let (file_manager, high, low_a, low_b) = tiered_file_manager(root, true);

    let (data_file_id, mut data_writer) = file_manager.create_data_file().unwrap();
    data_writer.close().unwrap();
    let data_path = file_manager.get_data_file_path(data_file_id).unwrap();
    assert!(high.exists(&data_path).unwrap());

    let store = VlogStore::new(Arc::clone(&file_manager), 64, 1);
    let (mut vlog, edit) = store.create_writer().unwrap();
    vlog.close().unwrap();
    let vlog_file_id = edit.new_files[0].1.file_id();
    let vlog_path = file_manager.get_data_file_path(vlog_file_id).unwrap();
    assert!(!high.exists(&vlog_path).unwrap());
    assert_ne!(
        low_a.exists(&vlog_path).unwrap(),
        low_b.exists(&vlog_path).unwrap()
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn vlog_uses_highest_primary_tier_by_default() {
    let root = "/tmp/vlog_default_primary";
    let _ = std::fs::remove_dir_all(root);
    let (file_manager, high, low_a, low_b) = tiered_file_manager(root, false);
    let store = VlogStore::new(Arc::clone(&file_manager), 64, 1);

    let (mut vlog, edit) = store.create_writer().unwrap();
    vlog.close().unwrap();
    let vlog_file_id = edit.new_files[0].1.file_id();
    let vlog_path = file_manager.get_data_file_path(vlog_file_id).unwrap();
    assert!(high.exists(&vlog_path).unwrap());
    assert!(!low_a.exists(&vlog_path).unwrap());
    assert!(!low_b.exists(&vlog_path).unwrap());

    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn vlog_lowest_primary_tier_does_not_fallback_when_unavailable() {
    let root = "/tmp/vlog_lowest_primary_unavailable";
    let _ = std::fs::remove_dir_all(root);
    let mut low_volume = VolumeDescriptor::new(
        format!("file://{root}/low"),
        vec![VolumeUsageKind::PrimaryDataPriorityLow],
    );
    low_volume.size_limit = Some(Size::from_const(64));
    let config = Config {
        volumes: vec![
            VolumeDescriptor::new(
                format!("file://{root}/high"),
                vec![
                    VolumeUsageKind::PrimaryDataPriorityHigh,
                    VolumeUsageKind::Meta,
                ],
            ),
            low_volume,
        ],
        base_file_size: Size::from_const(64),
        vlog_low_priority_primary_enabled: true,
        ..Config::default()
    };
    let file_manager = Arc::new(
        FileManager::from_config(
            &config,
            "db",
            Arc::new(MetricsManager::new("vlog-tier-unavailable-test")),
        )
        .unwrap(),
    );

    let error = match file_manager.create_vlog_data_file() {
        Ok(_) => panic!("VLOG creation should not fall back to the high-priority tier"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("All lowest-priority primary data volumes are full or write-stopped")
    );
    assert_eq!(file_manager.peek_next_file_id(), 2);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_vlog_writer_reader() {
    cleanup_test_root();
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register(TEST_ROOT).unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("vlog-test"));
    let file_manager = Arc::new(FileManager::with_defaults(fs, metrics_manager).unwrap());
    let store = VlogStore::new(Arc::clone(&file_manager), 64, usize::MAX);
    let version = VlogVersion::new();
    let (mut vlog, edit) = store.create_writer().unwrap();
    let version = version.apply_edit(edit);
    let first = vlog.add_value(b"hello").unwrap();
    let second = vlog.add_value(b"world!").unwrap();
    let large = vec![b'a'; 2000];
    let third = vlog.add_value(&large).unwrap();
    assert_eq!(first.offset, 0);
    assert_eq!(second.offset, (VLOG_RECORD_HEADER_SIZE + 5) as u32);
    vlog.close().unwrap();

    let first_value = store.read_pointer(&version, first).unwrap();
    let second_value = store.read_pointer(&version, second).unwrap();
    let third_value = store.read_pointer(&version, third).unwrap();
    assert_eq!(&first_value[..], b"hello");
    assert_eq!(&second_value[..], b"world!");
    assert_eq!(&third_value[..], &large[..]);
    cleanup_test_root();
}

#[test]
fn test_vlog_file_seq_wraps() {
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register(TEST_ROOT).unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("vlog-test"));
    let file_manager = Arc::new(FileManager::with_defaults(fs, metrics_manager).unwrap());
    let store = VlogStore::with_start_seq(Arc::clone(&file_manager), 64, usize::MAX, u32::MAX);
    let version = VlogVersion::new();
    let (mut vlog, edit) = store.create_writer().unwrap();
    assert_eq!(vlog.file_seq(), u32::MAX);
    vlog.close().unwrap();
    let version = version.apply_edit(edit);
    let (mut vlog, edit) = store.create_writer().unwrap();
    assert_eq!(vlog.file_seq(), 0);
    vlog.close().unwrap();
    let _version = version.apply_edit(edit);
}

#[test]
fn test_should_separate() {
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register(TEST_ROOT).unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("vlog-test"));
    let file_manager = Arc::new(FileManager::with_defaults(fs, metrics_manager).unwrap());
    let store = VlogStore::new(Arc::clone(&file_manager), 64, 8);
    assert!(!store.should_separate(8));
    assert!(store.should_separate(9));
}

#[test]
fn test_should_not_separate_when_threshold_disabled() {
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register(TEST_ROOT).unwrap();
    let metrics_manager = Arc::new(MetricsManager::new("vlog-test"));
    let file_manager = Arc::new(FileManager::with_defaults(fs, metrics_manager).unwrap());
    let store = VlogStore::new(Arc::clone(&file_manager), 64, 0);
    assert!(!store.should_separate(1));
    assert!(!store.should_separate(1024));
}

#[test]
fn test_vlog_version_removes_zero_valid_entry_file() {
    let version = VlogVersion::from_files_with_entries(vec![(7, TrackedFileId::untracked(42), 1)]);
    let mut edit = VlogEdit::default();
    edit.add_entry_delta(7, -1);
    let next = version.apply_edit(edit);
    assert!(next.file_id(7).is_none());
}
