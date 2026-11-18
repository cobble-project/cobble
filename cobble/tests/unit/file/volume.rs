//! Volume accounting, placement, and configuration coverage.

use super::{DataVolume, FileManager, VolumePriority};
use crate::Config;
use crate::config::VolumeUsageKind;
use crate::file::file_manager::FileManagerOptions;
use crate::file::{File, FileSystemRegistry, SequentialWriteFile};
use crate::metrics_manager::MetricsManager;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[test]
fn data_volume_clones_share_usage_and_saturate_removal() {
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/file_manager_usage_test")
        .unwrap();
    let volume = DataVolume {
        fs,
        deletions: Default::default(),
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
        deletions: Default::default(),
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
        deletions: Default::default(),
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
