//! Selection, watermarks, and READONLY promotion coverage.

use super::*;
use crate::data_file::DataFile;
use crate::file::{File, FileSystemRegistry, RandomAccessFile, SequentialWriteFile};
use crate::sst::{
    PinnedSstReadMetadata, SSTIteratorOptions, SSTPointReader, SSTWriter, SSTWriterOptions,
};
use crate::{Config, Error, MetricsManager, VolumeUsageKind};
use bytes::Bytes;
use size::Size;
use std::time::Duration;

fn pressure(rank: u8) -> VolumePressure {
    VolumePressure {
        priority_rank: rank,
        used_bytes: 1,
        size_limit: Some(2),
    }
}

fn register_readonly_sst(
    file_manager: &Arc<FileManager>,
    readonly_root: &std::path::Path,
    file_id: FileId,
    partitioned_index: bool,
) -> Arc<DataFile> {
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", readonly_root.display()))
        .unwrap();
    fs.create_dir("db").unwrap();
    fs.create_dir("db/data").unwrap();
    let relative_path = format!("db/data/{file_id}.sst");
    let mut writer = SSTWriter::new(
        fs.open_write(&relative_path).unwrap(),
        SSTWriterOptions {
            block_size: 32,
            bloom_filter_enabled: true,
            partitioned_index,
            block_checksum_enabled: false,
            ..SSTWriterOptions::default()
        },
    );
    for key in [b"key000".as_slice(), b"key001", b"key002", b"key003"] {
        writer.add(key, b"value").unwrap();
    }
    let result = writer.finish_with_range().unwrap();
    let path = readonly_root.join(&relative_path);
    file_manager
        .register_data_file_readonly(file_id, &format!("file://{}", path.display()))
        .unwrap();
    let data_file = DataFile::new(
        DataFileType::SSTable,
        result.first_key,
        result.last_key,
        file_id,
        crate::file::TrackedFileId::new(file_manager, file_id),
        0,
        result.file_size,
        0u16..=0u16,
        0u16..=0u16,
    );
    data_file.set_meta_bytes(result.meta_bytes);
    if let Some(metadata) = result.sst_read_metadata {
        data_file.set_sst_read_metadata(metadata);
    }
    Arc::new(data_file)
}

fn store_readonly_sst_state(
    db_state: &Arc<DbStateHandle>,
    level_ordinal: u8,
    data_file: Arc<DataFile>,
) {
    let current = db_state.load();
    db_state.store(crate::db_state::DbState {
        seq_id: current.seq_id,
        topology_epoch: current.topology_epoch,
        bucket_ranges: vec![0u16..=0u16],
        multi_lsm_version: crate::db_state::MultiLSMTreeVersion::new(crate::lsm::LSMTreeVersion {
            levels: vec![crate::lsm::Level {
                ordinal: level_ordinal,
                tiered: level_ordinal == 0,
                files: vec![data_file],
            }],
        }),
        vlog_version: crate::vlog::VlogVersion::new(),
        active: current.active.clone(),
        active_schema: current.active_schema.clone(),
        min_source_schema_by_cf: Vec::new(),
        immutables: current.immutables.clone(),
        truncation_cursors: current.truncation_cursors.clone(),
        suggested_base_snapshot_id: None,
    });
}

struct FailingReader {
    size: usize,
}

impl File for FailingReader {
    fn close(&mut self) -> crate::Result<()> {
        Ok(())
    }

    fn size(&self) -> usize {
        self.size
    }
}

impl RandomAccessFile for FailingReader {
    fn read_at(&self, _offset: usize, _size: usize) -> crate::Result<Bytes> {
        Err(Error::IoError(
            "injected pinned metadata read failure".to_string(),
        ))
    }
}

#[test]
fn backfill_watermark_can_exceed_half_offload_but_never_eighty_percent() {
    assert_eq!(effective_backfill_trigger_watermark(0.70, 0.85), 0.70);
    assert_eq!(effective_backfill_trigger_watermark(0.90, 0.95), 0.80);
    assert!((effective_backfill_trigger_watermark(0.70, 0.60) - 0.59).abs() < f64::EPSILON);
}

#[test]
fn largest_file_policy_picks_largest() {
    let policy = LargestFileOffloadPolicy;
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/offload-policy-largest")
        .unwrap();
    let candidates = vec![
        (
            7,
            Arc::new(TrackedFile::managed("a".to_string(), Arc::clone(&fs), None)),
        ),
        (
            3,
            Arc::new(TrackedFile::managed("b".to_string(), Arc::clone(&fs), None)),
        ),
    ];
    candidates[0].1.update_size_bytes(128);
    candidates[1].1.update_size_bytes(256);
    assert_eq!(
        policy.select_candidate(&candidates, &pressure(3), &pressure(2)),
        Some(3)
    );
}

#[test]
fn largest_file_policy_handles_empty_candidates() {
    let policy = LargestFileOffloadPolicy;
    assert!(
        policy
            .select_candidate(&[], &pressure(3), &pressure(2))
            .is_none()
    );
}

#[test]
fn largest_file_policy_tie_breaks_by_file_id() {
    let policy = LargestFileOffloadPolicy;
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/offload-policy-tie")
        .unwrap();
    let candidates = vec![
        (
            12,
            Arc::new(TrackedFile::managed("b".to_string(), Arc::clone(&fs), None)),
        ),
        (
            6,
            Arc::new(TrackedFile::managed("a".to_string(), Arc::clone(&fs), None)),
        ),
    ];
    candidates[0].1.update_size_bytes(64);
    candidates[1].1.update_size_bytes(64);
    assert_eq!(
        policy.select_candidate(&candidates, &pressure(3), &pressure(2)),
        Some(6)
    );
}

#[test]
fn priority_policy_prefers_lower_priority_over_larger_size() {
    let policy = PriorityOffloadPolicy;
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/offload-policy-priority")
        .unwrap();
    let candidates = vec![
        (
            11,
            Arc::new(TrackedFile::managed("a".to_string(), Arc::clone(&fs), None)),
        ),
        (
            22,
            Arc::new(TrackedFile::managed("b".to_string(), Arc::clone(&fs), None)),
        ),
    ];
    candidates[0].1.update_size_bytes(1024);
    candidates[1].1.update_size_bytes(32);
    candidates[0].1.set_priority(200);
    candidates[1].1.set_priority(3);
    assert_eq!(
        policy.select_candidate(&candidates, &pressure(3), &pressure(2)),
        Some(22)
    );
}

#[test]
#[serial_test::serial(file)]
fn test_select_offload_candidate_uses_policy() {
    let root = "/tmp/file_manager_offload_policy";
    let _ = std::fs::remove_dir_all(root);
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}/high", root),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}/low", root),
                vec![VolumeUsageKind::PrimaryDataPriorityLow],
            ),
        ],
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-policy"));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
    let (small_id, mut small_writer) = fm.create_data_file().unwrap();
    small_writer.write(&[b'a'; 32]).unwrap();
    small_writer.close().unwrap();
    let (large_id, mut large_writer) = fm.create_data_file().unwrap();
    large_writer.write(&[b'b'; 128]).unwrap();
    large_writer.close().unwrap();
    let source_volume = fm.primary_volume_by_rank(3).unwrap();
    let target_volume = fm.primary_volume_by_rank(1).unwrap();
    let selected = fm.select_offload_candidate(&source_volume, &target_volume);
    assert_eq!(selected, Some(large_id));
    assert_ne!(small_id, large_id);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_select_offload_candidate_prefers_lower_priority_file() {
    let root = "/tmp/file_manager_offload_priority_policy";
    let _ = std::fs::remove_dir_all(root);
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}/high", root),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}/low", root),
                vec![VolumeUsageKind::PrimaryDataPriorityLow],
            ),
        ],
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-priority"));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
    let (large_high_pri_id, mut large_writer) = fm.create_data_file().unwrap();
    large_writer.write(&vec![b'a'; 512]).unwrap();
    large_writer.close().unwrap();
    let (small_low_pri_id, mut small_writer) = fm.create_data_file().unwrap();
    small_writer.write(&[b'b'; 32]).unwrap();
    small_writer.close().unwrap();
    fm.set_data_file_priority(large_high_pri_id, 200).unwrap();
    fm.set_data_file_priority(small_low_pri_id, 3).unwrap();
    let source_volume = fm.primary_volume_by_rank(3).unwrap();
    let target_volume = fm.primary_volume_by_rank(1).unwrap();
    let selected = fm.select_offload_candidate(&source_volume, &target_volume);
    assert_eq!(selected, Some(small_low_pri_id));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_select_offload_candidate_uses_configured_largest_file_policy() {
    let root = "/tmp/file_manager_offload_policy_option_largest";
    let _ = std::fs::remove_dir_all(root);
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}/high", root),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}/low", root),
                vec![VolumeUsageKind::PrimaryDataPriorityLow],
            ),
        ],
        primary_volume_offload_policy: crate::PrimaryVolumeOffloadPolicyKind::LargestFile,
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-policy-option"));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
    let (large_high_pri_id, mut large_writer) = fm.create_data_file().unwrap();
    large_writer.write(&vec![b'a'; 512]).unwrap();
    large_writer.close().unwrap();
    let (small_low_pri_id, mut small_writer) = fm.create_data_file().unwrap();
    small_writer.write(&[b'b'; 32]).unwrap();
    small_writer.close().unwrap();
    fm.set_data_file_priority(large_high_pri_id, 200).unwrap();
    fm.set_data_file_priority(small_low_pri_id, 3).unwrap();
    let source_volume = fm.primary_volume_by_rank(3).unwrap();
    let target_volume = fm.primary_volume_by_rank(1).unwrap();
    let selected = fm.select_offload_candidate(&source_volume, &target_volume);
    assert_eq!(selected, Some(large_high_pri_id));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_backfill_prefers_low_lsm_levels_and_skips_unreferenced_files() {
    let root = "/tmp/file_manager_backfill_priority";
    let _ = std::fs::remove_dir_all(root);
    let mut high = crate::VolumeDescriptor::new(
        format!("file://{root}/high"),
        vec![VolumeUsageKind::PrimaryDataPriorityHigh],
    );
    high.size_limit = Some(Size::from_kib(8));
    let low = crate::VolumeDescriptor::new(
        format!("file://{root}/low"),
        vec![VolumeUsageKind::PrimaryDataPriorityLow],
    );
    let config = Config {
        volumes: vec![high, low],
        base_file_size: Size::from_const(64),
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-backfill-priority"));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
    let low_volume = fm.primary_volume_by_rank(1).unwrap();
    let high_volume = fm.primary_volume_by_rank(3).unwrap();

    let create_on_low = |size: usize| {
        let (file_id, mut writer) = fm.create_data_file().unwrap();
        writer.write(&vec![b'x'; size]).unwrap();
        writer.close().unwrap();
        assert!(
            fm.move_file_to_primary_volume(file_id, &low_volume)
                .unwrap()
        );
        file_id
    };
    let l0_file = create_on_low(128);
    let l3_file = create_on_low(256);
    let vlog_file = create_on_low(384);
    let unreferenced_file = create_on_low(512);

    let referenced_priorities = HashMap::from([
        (l0_file, crate::file::lsm_file_priority_for_level(0)),
        (l3_file, crate::file::lsm_file_priority_for_level(3)),
        (vlog_file, crate::file::VLOG_FILE_PRIORITY),
    ]);
    let mut excluded = HashSet::new();
    assert_eq!(
        fm.select_backfill_candidate_with_exclusions(
            &high_volume,
            &referenced_priorities,
            &excluded,
            u64::MAX,
        ),
        Some(l0_file)
    );
    excluded.insert(l0_file);
    assert_eq!(
        fm.select_backfill_candidate_with_exclusions(
            &high_volume,
            &referenced_priorities,
            &excluded,
            u64::MAX,
        ),
        Some(l3_file)
    );
    excluded.insert(l3_file);
    assert_eq!(
        fm.select_backfill_candidate_with_exclusions(
            &high_volume,
            &referenced_priorities,
            &excluded,
            u64::MAX,
        ),
        Some(vlog_file)
    );
    excluded.insert(vlog_file);
    assert_eq!(
        fm.select_backfill_candidate_with_exclusions(
            &high_volume,
            &referenced_priorities,
            &excluded,
            u64::MAX,
        ),
        None
    );
    assert!(!referenced_priorities.contains_key(&unreferenced_file));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_readonly_load_keeps_vlog_on_lowest_primary_tier() {
    let dir = tempfile::tempdir().unwrap();
    let high_root = dir.path().join("high");
    let low_root = dir.path().join("low");
    let readonly_root = dir.path().join("readonly");
    std::fs::create_dir_all(readonly_root.join("db/data")).unwrap();

    let mut high = crate::VolumeDescriptor::new(
        format!("file://{}", high_root.display()),
        vec![VolumeUsageKind::PrimaryDataPriorityHigh],
    );
    high.size_limit = Some(Size::from_const(200));
    let low = crate::VolumeDescriptor::new(
        format!("file://{}", low_root.display()),
        vec![VolumeUsageKind::PrimaryDataPriorityLow],
    );
    let readonly = crate::VolumeDescriptor::new(
        format!("file://{}", readonly_root.display()),
        vec![VolumeUsageKind::Readonly],
    );
    let config = Config {
        // Flink puts the checkpoint-backed low tier before the local high tier. Target
        // selection must follow priority, not this descriptor order.
        volumes: vec![low, high, readonly],
        file_transfer_concurrency: 1,
        base_file_size: Size::from_const(64),
        vlog_low_priority_primary_enabled: true,
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-readonly-load"));
    let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());

    let register_readonly = |file_id: FileId, size: usize| {
        let path = readonly_root.join(format!("db/data/{file_id}.sst"));
        std::fs::write(&path, vec![b'x'; size]).unwrap();
        fm.register_data_file_readonly(file_id, &format!("file://{}", path.display()))
            .unwrap();
        Arc::new(crate::data_file::DataFile::new(
            crate::data_file::DataFileType::SSTable,
            vec![file_id as u8],
            vec![file_id as u8 + 1],
            file_id,
            crate::file::TrackedFileId::new(&fm, file_id),
            0,
            size,
            0u16..=0u16,
            0u16..=0u16,
        ))
    };
    let l0_file = register_readonly(101, 256);
    let l3_file = register_readonly(102, 64);
    let added_after_mark = register_readonly(103, 32);
    let vlog_file = register_readonly(104, 32);
    let old_l0_readonly_tracking = fm.preferred_tracked_file(101).unwrap();
    let source_paths = [101, 102, 103, 104].map(|file_id| {
        (
            file_id,
            readonly_root.join(format!("db/data/{file_id}.sst")),
        )
    });

    let db_state = Arc::new(crate::db_state::DbStateHandle::new());
    let initial = db_state.load();
    db_state.store(crate::db_state::DbState {
        seq_id: initial.seq_id,
        topology_epoch: initial.topology_epoch,
        bucket_ranges: vec![0u16..=0u16],
        multi_lsm_version: crate::db_state::MultiLSMTreeVersion::new(crate::lsm::LSMTreeVersion {
            levels: vec![
                crate::lsm::Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![Arc::clone(&l0_file)],
                },
                crate::lsm::Level {
                    ordinal: 3,
                    tiered: false,
                    files: vec![Arc::clone(&l3_file)],
                },
            ],
        }),
        vlog_version: crate::vlog::VlogVersion::from_files_with_entries(vec![(
            0,
            crate::file::TrackedFileId::new(&fm, vlog_file.file_id),
            1,
        )]),
        active: initial.active.clone(),
        active_schema: initial.active_schema.clone(),
        min_source_schema_by_cf: Vec::new(),
        immutables: initial.immutables.clone(),
        truncation_cursors: initial.truncation_cursors.clone(),
        suggested_base_snapshot_id: None,
    });
    assert_eq!(
        fm.mark_readonly_files_for_primary_load(&db_state, None, false),
        3
    );

    let marked = db_state.load();
    db_state.store(crate::db_state::DbState {
        seq_id: marked.seq_id,
        topology_epoch: marked.topology_epoch,
        bucket_ranges: marked.bucket_ranges.clone(),
        multi_lsm_version: crate::db_state::MultiLSMTreeVersion::new(crate::lsm::LSMTreeVersion {
            levels: vec![
                crate::lsm::Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![l0_file, added_after_mark],
                },
                crate::lsm::Level {
                    ordinal: 3,
                    tiered: false,
                    files: vec![l3_file],
                },
            ],
        }),
        vlog_version: marked.vlog_version.clone(),
        active: marked.active.clone(),
        active_schema: marked.active_schema.clone(),
        min_source_schema_by_cf: Vec::new(),
        immutables: marked.immutables.clone(),
        truncation_cursors: marked.truncation_cursors.clone(),
        suggested_base_snapshot_id: None,
    });

    assert_eq!(fm.trigger_primary_tiering_if_needed(&db_state).unwrap(), 1);
    assert!(fm.wait_for_offload_idle(Duration::from_secs(5)));
    assert_eq!(
        fm.preferred_tracked_file(101)
            .and_then(|tracked| { tracked.volume.as_ref().map(|volume| volume.priority.rank()) }),
        Some(1),
        "the highest-priority L0 file should fall back to low when high cannot fit it"
    );
    assert!(
        !Arc::ptr_eq(
            &fm.preferred_tracked_file(101).unwrap(),
            &old_l0_readonly_tracking
        ),
        "the old READONLY TrackedFile must no longer be preferred"
    );
    assert_eq!(
        fm.get_logical_file(101).unwrap().replica_ids().len(),
        1,
        "readonly promotion must retire its source replica from the logical file"
    );

    assert_eq!(fm.trigger_primary_tiering_if_needed(&db_state).unwrap(), 1);
    assert!(fm.wait_for_offload_idle(Duration::from_secs(5)));
    assert_eq!(
        fm.preferred_tracked_file(102)
            .and_then(|tracked| { tracked.volume.as_ref().map(|volume| volume.priority.rank()) }),
        Some(3),
        "the next marked file should use high when it fits"
    );
    assert_eq!(fm.trigger_primary_tiering_if_needed(&db_state).unwrap(), 1);
    assert!(fm.wait_for_offload_idle(Duration::from_secs(5)));
    assert_eq!(
        fm.preferred_tracked_file(104)
            .and_then(|tracked| { tracked.volume.as_ref().map(|volume| volume.priority.rank()) }),
        Some(1),
        "the VLog file should load to the low tier after all marked LSM files"
    );
    assert!(
        fm.preferred_tracked_file(103).is_some_and(|tracked| {
            tracked
                .volume
                .as_ref()
                .is_some_and(|volume| volume.readonly_source)
        }),
        "a file that became current after marking must remain unmarked"
    );
    for (_, path) in source_paths {
        assert!(
            path.exists(),
            "loading must never delete the original READONLY file"
        );
    }
}

#[test]
fn readonly_load_pins_eligible_sst_after_promotion_and_reuses_it() {
    let dir = tempfile::tempdir().unwrap();
    let primary_root = dir.path().join("primary");
    let readonly_root = dir.path().join("readonly");
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}", primary_root.display()),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}", readonly_root.display()),
                vec![VolumeUsageKind::Readonly],
            ),
        ],
        sst_pinned_metadata_max_level: Some(0),
        sst_pinned_metadata_partitions_enabled: true,
        ..Config::default()
    };
    let fm = Arc::new(
        FileManager::from_config(
            &config,
            "readonly-load-pin",
            Arc::new(MetricsManager::new("readonly-load-pin")),
        )
        .unwrap(),
    );
    let data_file = register_readonly_sst(&fm, &readonly_root, 301, true);
    let readonly_source = readonly_root.join("db/data/301.sst");
    let residual_target = primary_root.join("readonly-load-pin/data/301.sst");
    std::fs::copy(&readonly_source, &residual_target).unwrap();
    let db_state = Arc::new(DbStateHandle::new());
    store_readonly_sst_state(&db_state, 0, Arc::clone(&data_file));

    assert_eq!(
        fm.mark_readonly_files_for_primary_load(
            &db_state,
            config.sst_pinned_metadata_max_level,
            config.sst_pinned_metadata_partitions_enabled,
        ),
        1
    );
    assert_eq!(fm.trigger_primary_tiering_if_needed(&db_state).unwrap(), 1);
    assert!(fm.wait_for_offload_idle(Duration::from_secs(5)));
    assert!(fm.is_data_file_on_primary_volume(data_file.file_id));
    assert_ne!(
        fm.get_data_file_path(data_file.file_id).as_deref(),
        Some("readonly-load-pin/data/301.sst"),
        "runtime readonly loading must not adopt an unregistered residual"
    );
    assert_eq!(
        std::fs::read_dir(residual_target.parent().unwrap())
            .unwrap()
            .count(),
        2
    );

    let pin = data_file
        .pinned_sst_read_metadata()
        .expect("eligible SST should be pinned after promotion");
    assert!(pin.index_partition(0).unwrap().is_some());
    assert!(pin.filter_partition(0).unwrap().is_some());

    let reader = fm.open_data_file_reader(data_file.file_id).unwrap();
    let reused = PinnedSstReadMetadata::get_or_load(&reader, data_file.as_ref(), false, false)
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&pin, &reused));
    assert_eq!(
        SSTPointReader::get_exact(
            Box::new(fm.open_data_file_reader(data_file.file_id).unwrap()),
            data_file.as_ref(),
            SSTIteratorOptions {
                bloom_filter_enabled: true,
                ..SSTIteratorOptions::default()
            },
            None,
            b"key002",
        )
        .unwrap()
        .as_ref()
        .map(|value| value.bytes().as_ref()),
        Some(b"value".as_slice())
    );
}

#[test]
fn readonly_load_keeps_ineligible_and_existing_pins_as_noops() {
    let dir = tempfile::tempdir().unwrap();
    let primary_root = dir.path().join("primary");
    let readonly_root = dir.path().join("readonly");
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}", primary_root.display()),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}", readonly_root.display()),
                vec![VolumeUsageKind::Readonly],
            ),
        ],
        ..Config::default()
    };
    let fm = Arc::new(
        FileManager::from_config(
            &config,
            "readonly-load-noop",
            Arc::new(MetricsManager::new("readonly-load-noop")),
        )
        .unwrap(),
    );
    let data_file = register_readonly_sst(&fm, &readonly_root, 302, false);
    let db_state = Arc::new(DbStateHandle::new());
    store_readonly_sst_state(&db_state, 1, Arc::clone(&data_file));

    assert_eq!(
        fm.mark_readonly_files_for_primary_load(&db_state, Some(0), true),
        1
    );
    assert_eq!(fm.trigger_primary_tiering_if_needed(&db_state).unwrap(), 1);
    assert!(fm.wait_for_offload_idle(Duration::from_secs(5)));
    assert!(fm.is_data_file_on_primary_volume(data_file.file_id));
    assert!(
        data_file.pinned_sst_read_metadata().is_none(),
        "an ineligible level must not be pinned by promotion"
    );

    let reader = fm.open_data_file_reader(data_file.file_id).unwrap();
    let pin = PinnedSstReadMetadata::get_or_load(&reader, data_file.as_ref(), true, false)
        .unwrap()
        .unwrap();
    fm.pin_promoted_readonly_sst_metadata(data_file.file_id, &db_state, true);
    let reused = data_file.pinned_sst_read_metadata().unwrap();
    assert!(Arc::ptr_eq(&pin, &reused));

    let ineligible = referenced_readonly_load_requests(&db_state, Some(0), true);
    assert!(!ineligible.get(&data_file.file_id).unwrap().pin_metadata);
    assert!(!ineligible.get(&data_file.file_id).unwrap().pin_partitions);

    let disabled = referenced_readonly_load_requests(&db_state, None, true);
    assert!(!disabled.get(&data_file.file_id).unwrap().pin_metadata);
    assert!(!disabled.get(&data_file.file_id).unwrap().pin_partitions);
}

#[test]
fn readonly_load_request_uses_lsm_level_and_sst_type_for_pinning() {
    let sst = Arc::new(DataFile::new_untracked(
        DataFileType::SSTable,
        vec![0],
        vec![1],
        304,
        0,
        1,
        0u16..=0u16,
        0u16..=0u16,
    ));
    let parquet = Arc::new(DataFile::new_untracked(
        DataFileType::Parquet,
        vec![1],
        vec![2],
        305,
        0,
        1,
        0u16..=0u16,
        0u16..=0u16,
    ));
    let db_state = Arc::new(DbStateHandle::new());
    let current = db_state.load();
    db_state.store(crate::db_state::DbState {
        seq_id: current.seq_id,
        topology_epoch: current.topology_epoch,
        bucket_ranges: vec![0u16..=0u16],
        multi_lsm_version: crate::db_state::MultiLSMTreeVersion::new(crate::lsm::LSMTreeVersion {
            levels: vec![
                crate::lsm::Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![Arc::clone(&sst), parquet],
                },
                crate::lsm::Level {
                    ordinal: 3,
                    tiered: false,
                    files: vec![sst],
                },
            ],
        }),
        vlog_version: crate::vlog::VlogVersion::new(),
        active: current.active.clone(),
        active_schema: current.active_schema.clone(),
        min_source_schema_by_cf: Vec::new(),
        immutables: current.immutables.clone(),
        truncation_cursors: current.truncation_cursors.clone(),
        suggested_base_snapshot_id: None,
    });

    let requests = referenced_readonly_load_requests(&db_state, Some(0), true);
    assert!(requests.get(&304).unwrap().pin_metadata);
    assert!(requests.get(&304).unwrap().pin_partitions);
    assert!(!requests.get(&305).unwrap().pin_metadata);
    assert!(!requests.get(&305).unwrap().pin_partitions);

    let disabled = referenced_readonly_load_requests(&db_state, None, true);
    assert!(!disabled.get(&304).unwrap().pin_metadata);
    assert!(!disabled.get(&304).unwrap().pin_partitions);
    assert!(!disabled.get(&305).unwrap().pin_metadata);
    assert!(!disabled.get(&305).unwrap().pin_partitions);

    let mut invalid = ReadonlyLoadRequest {
        priority: 0,
        pin_metadata: false,
        pin_partitions: false,
    };
    invalid.merge(ReadonlyLoadRequest {
        priority: 1,
        pin_metadata: false,
        pin_partitions: true,
    });
    assert!(!invalid.pin_metadata);
    assert!(!invalid.pin_partitions);
}

#[test]
fn readonly_load_pin_failure_keeps_promotion_and_allows_foreground_retry() {
    let dir = tempfile::tempdir().unwrap();
    let primary_root = dir.path().join("primary");
    let readonly_root = dir.path().join("readonly");
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}", primary_root.display()),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}", readonly_root.display()),
                vec![VolumeUsageKind::Readonly],
            ),
        ],
        ..Config::default()
    };
    let fm = Arc::new(
        FileManager::from_config(
            &config,
            "readonly-load-pin-failure",
            Arc::new(MetricsManager::new("readonly-load-pin-failure")),
        )
        .unwrap(),
    );
    let data_file = register_readonly_sst(&fm, &readonly_root, 303, false);
    let db_state = Arc::new(DbStateHandle::new());
    store_readonly_sst_state(&db_state, 0, Arc::clone(&data_file));

    let target = fm.primary_volume_by_rank(3).unwrap();
    let mut progress = |_| {};
    let mut rollback = || {};
    assert!(
        fm.move_file_to_primary_volume_with_progress(
            data_file.file_id,
            &target,
            &mut progress,
            &mut rollback,
        )
        .unwrap()
    );
    assert!(fm.is_data_file_on_primary_volume(data_file.file_id));

    let cache_key = fm.preferred_replica_key(data_file.file_id).unwrap();
    fm.reader_cache.lock().unwrap().insert(
        cache_key.clone(),
        Arc::new(FailingReader {
            size: data_file.size,
        }),
    );
    fm.pin_promoted_readonly_sst_metadata(data_file.file_id, &db_state, false);
    assert!(data_file.pinned_sst_read_metadata().is_none());
    assert!(fm.is_data_file_on_primary_volume(data_file.file_id));

    fm.reader_cache.lock().unwrap().remove(&cache_key);
    assert_eq!(
        SSTPointReader::get_exact(
            Box::new(fm.open_data_file_reader(data_file.file_id).unwrap()),
            data_file.as_ref(),
            SSTIteratorOptions {
                bloom_filter_enabled: true,
                pin_metadata: true,
                ..SSTIteratorOptions::default()
            },
            None,
            b"key001",
        )
        .unwrap()
        .as_ref()
        .map(|value| value.bytes().as_ref()),
        Some(b"value".as_slice())
    );
    assert!(data_file.pinned_sst_read_metadata().is_some());
}

#[test]
fn test_readonly_load_keeps_mark_when_no_primary_volume_can_fit() {
    let dir = tempfile::tempdir().unwrap();
    let primary_root = dir.path().join("primary");
    let readonly_root = dir.path().join("readonly");
    std::fs::create_dir_all(readonly_root.join("db/data")).unwrap();
    let source_path = readonly_root.join("db/data/201.sst");
    std::fs::write(&source_path, vec![b'x'; 128]).unwrap();

    let mut primary = crate::VolumeDescriptor::new(
        format!("file://{}", primary_root.display()),
        vec![VolumeUsageKind::PrimaryDataPriorityHigh],
    );
    primary.size_limit = Some(Size::from_const(100));
    let readonly = crate::VolumeDescriptor::new(
        format!("file://{}", readonly_root.display()),
        vec![VolumeUsageKind::Readonly],
    );
    let config = Config {
        volumes: vec![primary, readonly],
        base_file_size: Size::from_const(32),
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("readonly-load-no-space"));
    let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
    fm.register_data_file_readonly(201, &format!("file://{}", source_path.display()))
        .unwrap();
    let data_file = Arc::new(crate::data_file::DataFile::new(
        crate::data_file::DataFileType::SSTable,
        vec![0],
        vec![1],
        201,
        crate::file::TrackedFileId::new(&fm, 201),
        0,
        128,
        0u16..=0u16,
        0u16..=0u16,
    ));
    let db_state = Arc::new(crate::db_state::DbStateHandle::new());
    let current = db_state.load();
    db_state.store(crate::db_state::DbState {
        seq_id: current.seq_id,
        topology_epoch: current.topology_epoch,
        bucket_ranges: vec![0u16..=0u16],
        multi_lsm_version: crate::db_state::MultiLSMTreeVersion::new(crate::lsm::LSMTreeVersion {
            levels: vec![crate::lsm::Level {
                ordinal: 0,
                tiered: true,
                files: vec![data_file],
            }],
        }),
        vlog_version: crate::vlog::VlogVersion::new(),
        active: current.active.clone(),
        active_schema: current.active_schema.clone(),
        min_source_schema_by_cf: Vec::new(),
        immutables: current.immutables.clone(),
        truncation_cursors: current.truncation_cursors.clone(),
        suggested_base_snapshot_id: None,
    });

    assert_eq!(
        fm.mark_readonly_files_for_primary_load(&db_state, None, false),
        1
    );
    assert_eq!(fm.trigger_primary_tiering_if_needed(&db_state).unwrap(), 0);
    assert!(
        fm.offload_runtime.pending_readonly_loads.contains_key(&201),
        "a capacity-blocked load must remain marked for a later retry"
    );
    assert!(fm.preferred_tracked_file(201).is_some_and(|tracked| {
        tracked
            .volume
            .as_ref()
            .is_some_and(|volume| volume.readonly_source)
    }));
    assert!(source_path.exists());
    let worker = fm.start_primary_tiering_worker(&db_state, None).unwrap();
    assert!(
        worker.is_some(),
        "READONLY loading needs a scanner even with one primary tier"
    );
    let worker = worker.unwrap();
    worker.stop();
    worker.join();
}

#[test]
#[serial_test::serial(file)]
fn test_trigger_offload_loops_until_projected_watermark_recovers() {
    let root = "/tmp/file_manager_offload_loop_trigger";
    let _ = std::fs::remove_dir_all(root);
    let high_url = format!("file://{}/high", root);
    let low_url = format!("file://{}/low", root);
    let registry = FileSystemRegistry::new();
    let low_fs = registry.get_or_register(low_url.clone()).unwrap();
    let mut high =
        crate::VolumeDescriptor::new(high_url, vec![VolumeUsageKind::PrimaryDataPriorityHigh]);
    high.size_limit = Some(Size::from_const(1200));
    let low = crate::VolumeDescriptor::new(low_url, vec![VolumeUsageKind::PrimaryDataPriorityLow]);
    let config = Config {
        volumes: vec![high, low],
        base_file_size: Size::from_const(64),
        primary_volume_write_stop_watermark: 0.95,
        primary_volume_offload_trigger_watermark: 0.4,
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-loop-trigger"));
    let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
    let mut file_ids = Vec::new();
    for _ in 0..3 {
        let (file_id, mut writer) = fm.create_data_file().unwrap();
        writer.write(&vec![b'x'; 300]).unwrap();
        writer.close().unwrap();
        file_ids.push(file_id);
    }

    let scheduled = fm.trigger_offload_if_needed().unwrap();
    assert_eq!(scheduled, 2);
    assert!(fm.wait_for_offload_idle(Duration::from_secs(20)));

    let moved_to_low = file_ids
        .iter()
        .filter(|file_id| {
            let path = fm.get_data_file_path(**file_id).unwrap();
            low_fs.exists(&path).unwrap_or(false)
        })
        .count();
    assert!(moved_to_low >= 2);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_trigger_offload_uses_planned_bytes_to_avoid_overscheduling() {
    let root = "/tmp/file_manager_offload_planned_backpressure";
    let _ = std::fs::remove_dir_all(root);
    let mut high = crate::VolumeDescriptor::new(
        format!("file://{}/high", root),
        vec![VolumeUsageKind::PrimaryDataPriorityHigh],
    );
    high.size_limit = Some(Size::from_kib(1));
    let low = crate::VolumeDescriptor::new(
        format!("file://{}/low", root),
        vec![VolumeUsageKind::PrimaryDataPriorityLow],
    );
    let config = Config {
        volumes: vec![high, low],
        base_file_size: Size::from_const(64),
        primary_volume_write_stop_watermark: 0.95,
        primary_volume_offload_trigger_watermark: 0.8,
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-backpressure"));
    let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
    for _ in 0..3 {
        let (_id, mut writer) = fm.create_data_file().unwrap();
        writer.write(&vec![b'x'; 300]).unwrap();
        writer.close().unwrap();
    }
    let first = fm.trigger_offload_if_needed().unwrap();
    let second = fm.trigger_offload_if_needed().unwrap();
    assert_eq!(first, 1);
    assert_eq!(second, 0);
    assert!(fm.wait_for_offload_idle(Duration::from_secs(20)));
    let _ = std::fs::remove_dir_all(root);
}
