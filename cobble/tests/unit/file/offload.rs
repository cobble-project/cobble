use super::*;
use crate::file::logical_file::ReplicaLifecycle;
use crate::file::{File, FileSystemRegistry, RandomAccessFile, SequentialWriteFile, test_utils};
use crate::sst::{
    PinnedSstReadMetadata, SSTIteratorOptions, SSTPointReader, SSTWriter, SSTWriterOptions,
};
use crate::{Config, MetricsManager, VolumeUsageKind};
use bytes::Bytes;
use size::Size;

impl FileManager {
    fn wait_for_offload_idle(&self, timeout: Duration) -> bool {
        self.offload_runtime.wait_idle(timeout)
    }

    fn primary_volume_by_rank(&self, rank: u8) -> Option<Arc<DataVolume>> {
        self.offload_runtime.primary_volume_by_rank(rank)
    }
}

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
fn background_primary_targets_keep_vlogs_on_lowest_tier() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}/high", dir.path().display()),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}/low", dir.path().display()),
                vec![VolumeUsageKind::PrimaryDataPriorityLow],
            ),
        ],
        vlog_low_priority_primary_enabled: true,
        ..Config::default()
    };
    let fm = FileManager::from_config(
        &config,
        "db",
        Arc::new(MetricsManager::new("background-vlog-placement")),
    )
    .unwrap();

    assert_eq!(
        fm.select_adoption_target(1, PrimaryDataPlacement::Vlog)
            .map(|volume| volume.priority.rank()),
        Some(1),
        "leased adoption and persistent-cache copies share this target selector"
    );
    assert_eq!(
        fm.select_adoption_target(1, PrimaryDataPlacement::Standard)
            .map(|volume| volume.priority.rank()),
        Some(3),
        "ordinary SST copies retain the highest-priority target"
    );
}

#[test]
fn tiering_runtime_executes_multiple_jobs_concurrently() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}/high", dir.path().display()),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}/low", dir.path().display()),
                vec![VolumeUsageKind::PrimaryDataPriorityLow],
            ),
        ],
        file_transfer_concurrency: 2,
        ..Config::default()
    };
    let metrics = Arc::new(MetricsManager::new("tiering-runtime-concurrency"));
    let fm = FileManager::from_config(&config, "db", metrics).unwrap();
    let source = fm.primary_volume_by_rank(3).unwrap();
    let target = fm.primary_volume_by_rank(1).unwrap();
    let gate = Arc::new((Mutex::new((0usize, false)), Condvar::new()));
    let handler_gate = Arc::clone(&gate);
    let handler = Arc::new(move |_| {
        let (lock, condvar) = handler_gate.as_ref();
        let mut state = lock.lock().unwrap();
        state.0 += 1;
        condvar.notify_all();
        while !state.1 {
            state = condvar.wait(state).unwrap();
        }
    });

    for file_id in [101, 102] {
        fm.offload_runtime
            .schedule(
                file_id,
                OffloadJobPlan {
                    source_volume: Arc::clone(&source),
                    target_volume: Arc::clone(&target),
                    reserved_incoming_bytes: 10,
                    projected_source_release_bytes: 10,
                    copied_bytes: Arc::new(AtomicU64::new(0)),
                    direction: PrimaryTieringDirection::Offload,
                },
                handler.clone(),
                None,
            )
            .unwrap();
    }

    let (lock, condvar) = gate.as_ref();
    let state = lock.lock().unwrap();
    let (mut state, _) = condvar
        .wait_timeout_while(state, Duration::from_secs(2), |state| state.0 < 2)
        .unwrap();
    let ran_concurrently = state.0 == 2;
    state.1 = true;
    condvar.notify_all();
    drop(state);

    assert!(fm.wait_for_offload_idle(Duration::from_secs(5)));
    assert!(
        ran_concurrently,
        "both workers should start before either job is released"
    );
}

#[test]
fn target_accounting_replaces_written_reservation_with_actual_usage() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}/high", dir.path().display()),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}/low", dir.path().display()),
                vec![VolumeUsageKind::PrimaryDataPriorityLow],
            ),
        ],
        ..Config::default()
    };
    let metrics = Arc::new(MetricsManager::new("tiering-runtime-accounting"));
    let fm = FileManager::from_config(&config, "db", metrics).unwrap();
    let source = fm.primary_volume_by_rank(3).unwrap();
    let target = fm.primary_volume_by_rank(1).unwrap();
    target.add_usage(20);
    fm.offload_runtime.planned_jobs.insert(
        103,
        OffloadJobPlan {
            source_volume: source,
            target_volume: Arc::clone(&target),
            reserved_incoming_bytes: 100,
            projected_source_release_bytes: 0,
            copied_bytes: Arc::new(AtomicU64::new(0)),
            direction: PrimaryTieringDirection::Offload,
        },
    );

    assert_eq!(
        fm.offload_runtime.projected_target_physical_bytes(&target),
        120
    );
    target.add_usage(40);
    fm.offload_runtime.record_copy_progress(103, 40);
    assert_eq!(
        fm.offload_runtime.projected_target_physical_bytes(&target),
        120,
        "written bytes must replace, not duplicate, the incoming reservation"
    );

    target.subtract_usage(40);
    fm.offload_runtime.reset_copy_progress(103);
    assert_eq!(
        fm.offload_runtime.projected_target_physical_bytes(&target),
        120,
        "a failed temporary copy restores the full incoming reservation"
    );
    fm.offload_runtime.complete_job(103);
}

#[test]
fn source_accounting_does_not_claim_snapshot_retained_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        volumes: crate::VolumeDescriptor::single_volume(format!("file://{}", dir.path().display())),
        ..Config::default()
    };
    let metrics = Arc::new(MetricsManager::new("tiering-source-accounting"));
    let fm = FileManager::from_config(&config, "db", metrics).unwrap();
    let (file_id, mut writer) = fm.create_data_file().unwrap();
    writer.write(&[b'x'; 128]).unwrap();
    writer.close().unwrap();

    let tracked = fm.preferred_tracked_file(file_id).unwrap();
    assert_eq!(projected_source_release_bytes(&tracked), 128);

    let snapshot_ref = fm.data_file_ref(file_id).unwrap();
    assert_eq!(
        projected_source_release_bytes(snapshot_ref.as_ref()),
        0,
        "snapshot-retained bytes remain part of physical source usage"
    );
    snapshot_ref.dereference();
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
fn test_offload_move_is_async_and_keeps_reads_available() {
    let root = "/tmp/file_manager_offload_async";
    let _ = std::fs::remove_dir_all(root);
    let high_url = format!("file://{}/high", root);
    let low_url = format!("file://{}/low", root);
    let registry = FileSystemRegistry::new();
    let high_fs = registry.get_or_register(high_url.clone()).unwrap();
    let low_fs = registry.get_or_register(low_url.clone()).unwrap();
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(high_url, vec![VolumeUsageKind::PrimaryDataPriorityHigh]),
            crate::VolumeDescriptor::new(low_url, vec![VolumeUsageKind::PrimaryDataPriorityLow]),
        ],
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-async"));
    let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
    let payload = vec![b'x'; 8 * 1024 * 1024];
    let (file_id, mut writer) = fm.create_data_file().unwrap();
    writer.write(&payload).unwrap();
    writer.close().unwrap();

    let old_path = fm.get_data_file_path(file_id).unwrap();
    assert!(high_fs.exists(&old_path).unwrap());
    let old_reader = fm.open_data_file_reader(file_id).unwrap();
    let target_volume = fm.primary_volume_by_rank(1).unwrap();

    assert!(fm.schedule_offload_move(file_id, &target_volume).unwrap());
    assert!(!fm.schedule_offload_move(file_id, &target_volume).unwrap());
    assert!(fm.wait_for_offload_idle(Duration::from_secs(20)));

    assert_eq!(
        old_reader.read_at(payload.len() - 16, 16).unwrap().as_ref(),
        &payload[payload.len() - 16..]
    );
    let new_path = fm.get_data_file_path(file_id).unwrap();
    assert_ne!(old_path, new_path);
    assert!(low_fs.exists(&new_path).unwrap());
    let new_reader = fm.open_data_file_reader(file_id).unwrap();
    assert_eq!(
        new_reader.read_at(payload.len() - 16, 16).unwrap().as_ref(),
        &payload[payload.len() - 16..]
    );
    assert_eq!(
        fm.get_logical_file(file_id).unwrap().replica_ids().len(),
        1,
        "the retired high-tier source must not remain in the logical replica set"
    );
    drop(old_reader);
    test_utils::wait_for_file_deletion(&high_fs, &old_path);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn persistent_cache_moves_without_changing_durable_route_and_evicts_when_low_is_full() {
    let root = "/tmp/file_manager_persistent_cache_tiering";
    let _ = std::fs::remove_dir_all(root);
    let mut high = crate::VolumeDescriptor::new(
        format!("file://{root}/high"),
        vec![VolumeUsageKind::PrimaryDataPriorityHigh],
    );
    high.size_limit = Some(Size::from_bytes(512));
    let mut low = crate::VolumeDescriptor::new(
        format!("file://{root}/low"),
        vec![VolumeUsageKind::PrimaryDataPriorityLow],
    );
    low.size_limit = Some(Size::from_bytes(512));
    let external_url = format!("file://{root}/external");
    let config = Config {
        volumes: vec![
            high,
            low,
            crate::VolumeDescriptor::new(external_url.clone(), vec![VolumeUsageKind::Snapshot]),
        ],
        ..Config::default()
    };
    let registry = FileSystemRegistry::new();
    let external_fs = registry.get_or_register(external_url).unwrap();
    external_fs.create_dir("db").unwrap();
    let mut writer = external_fs.open_write("db/external.sst").unwrap();
    writer.write(&[b'x'; 64]).unwrap();
    writer.close().unwrap();

    let fm = Arc::new(
        FileManager::from_config(
            &config,
            "db",
            Arc::new(MetricsManager::new("persistent-cache-tiering")),
        )
        .unwrap(),
    );
    let file_id = 91;
    fm.register_external_persistent_replica(
        file_id,
        &format!("{root}/external/db/external.sst"),
        "source".to_string(),
    )
    .unwrap();
    let logical = fm.get_logical_file(file_id).unwrap();
    logical.set_commit_state(crate::file::logical_file::FileCommitState::Committed);
    logical.set_persistent_cache_requested(true);
    let high_volume = fm.primary_volume_by_rank(3).unwrap();
    let low_volume = fm.primary_volume_by_rank(1).unwrap();
    assert!(
        fm.cache_external_persistent_file(file_id, &high_volume, &mut |_| {})
            .unwrap()
    );
    assert!(
        fm.move_persistent_cache_to_volume_with_progress(
            file_id,
            &low_volume,
            &mut |_| {},
            &mut || {},
        )
        .unwrap()
    );
    assert_eq!(
        fm.preferred_tracked_file(file_id)
            .unwrap()
            .volume
            .as_ref()
            .unwrap()
            .priority
            .rank(),
        1
    );
    assert!(matches!(
        fm.durable_data_file_path_with_origin(file_id)
            .map(|(_, origin)| origin),
        Some(ReplicaOrigin::ExternalPersistent { .. })
    ));

    assert!(fm.evict_preferred_persistent_cache(file_id).unwrap());
    assert!(
        fm.cache_external_persistent_file(file_id, &high_volume, &mut |_| {})
            .unwrap()
    );
    let second_file_id = 92;
    fm.register_external_persistent_replica(
        second_file_id,
        &format!("{root}/external/db/external.sst"),
        "source".to_string(),
    )
    .unwrap();
    let second_logical = fm.get_logical_file(second_file_id).unwrap();
    second_logical.set_commit_state(crate::file::logical_file::FileCommitState::Committed);
    second_logical.set_persistent_cache_requested(true);
    assert!(
        fm.cache_external_persistent_file(second_file_id, &high_volume, &mut |_| {})
            .unwrap()
    );
    high_volume.add_usage(400);
    let data_file = Arc::new(DataFile::new_untracked(
        DataFileType::SSTable,
        vec![0],
        vec![1],
        file_id,
        0,
        64,
        0u16..=0u16,
        0u16..=0u16,
    ));
    let second_data_file = Arc::new(DataFile::new_untracked(
        DataFileType::SSTable,
        vec![1],
        vec![2],
        second_file_id,
        0,
        64,
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
            levels: vec![crate::lsm::Level {
                ordinal: 0,
                tiered: true,
                files: vec![data_file, second_data_file],
            }],
        }),
        vlog_version: crate::vlog::VlogVersion::new(),
        active: current.active.clone(),
        immutables: current.immutables.clone(),
        truncation_cursors: current.truncation_cursors.clone(),
        suggested_base_snapshot_id: None,
    });
    assert_eq!(fm.trigger_primary_tiering_if_needed(&db_state).unwrap(), 2);
    assert!(fm.wait_for_offload_idle(Duration::from_secs(5)));
    for current_file_id in [file_id, second_file_id] {
        assert_eq!(
            fm.preferred_tracked_file(current_file_id)
                .unwrap()
                .volume
                .as_ref()
                .unwrap()
                .priority
                .rank(),
            1
        );
        assert!(matches!(
            fm.durable_data_file_path_with_origin(current_file_id)
                .map(|(_, origin)| origin),
            Some(ReplicaOrigin::ExternalPersistent { .. })
        ));
    }

    assert!(fm.evict_preferred_persistent_cache(file_id).unwrap());
    assert!(
        fm.cache_external_persistent_file(file_id, &high_volume, &mut |_| {})
            .unwrap()
    );
    low_volume.add_usage(500);
    assert!(fm.trigger_primary_tiering_if_needed(&db_state).unwrap() >= 1);
    assert!(matches!(
        fm.preferred_replica_origin(file_id),
        Some(ReplicaOrigin::ExternalPersistent { .. })
    ));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_offload_promotes_existing_snapshot_replica_on_target_volume() {
    let root = "/tmp/file_manager_offload_promote_snapshot_replica";
    let _ = std::fs::remove_dir_all(root);
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}/high", root),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}/low", root),
                vec![
                    VolumeUsageKind::PrimaryDataPriorityLow,
                    VolumeUsageKind::Snapshot,
                ],
            ),
        ],
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new(
        "file-manager-offload-promote-snapshot-replica",
    ));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();

    let (source_file_id, mut source_writer) = fm.create_data_file().unwrap();
    source_writer.write(&vec![b'x'; 512]).unwrap();
    source_writer.close().unwrap();
    let source_path = fm.get_data_file_path(source_file_id).unwrap();

    let source = fm.data_file_ref(source_file_id).unwrap();
    let logical = fm.get_logical_file(source_file_id).unwrap();
    let snapshot_replica = fm
        .snapshot_replica_for_tracked_file(source_file_id, &source, Some(&logical), None, None)
        .unwrap();
    source.dereference();
    let replica_id = logical
        .replica_ids()
        .into_iter()
        .find(|replica_id| *replica_id != 0)
        .unwrap();
    logical.set_replica_lifecycle(replica_id, ReplicaLifecycle::OwnedReady);
    let snapshot_replica_path = snapshot_replica.path().to_string();

    let target_volume = fm.primary_volume_by_rank(1).unwrap();
    let promoted = fm
        .move_file_to_primary_volume(source_file_id, &target_volume)
        .unwrap();
    assert!(promoted);
    assert_eq!(
        fm.get_data_file_path(source_file_id).unwrap(),
        snapshot_replica_path
    );
    assert_ne!(source_path, snapshot_replica_path);
    assert_eq!(logical.replica_ids().len(), 1);
    assert!(
        !Arc::ptr_eq(&logical.preferred_replica_any().unwrap().tracked, &source),
        "promotion must remove the old preferred source replica"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_backfill_preserves_source_until_snapshot_reference_is_released() {
    let root = "/tmp/file_manager_backfill_snapshot_lifecycle";
    let _ = std::fs::remove_dir_all(root);
    let high_url = format!("file://{root}/high");
    let low_url = format!("file://{root}/low");
    let registry = FileSystemRegistry::new();
    let low_fs = registry.get_or_register(low_url.clone()).unwrap();
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(high_url, vec![VolumeUsageKind::PrimaryDataPriorityHigh]),
            crate::VolumeDescriptor::new(
                low_url,
                vec![
                    VolumeUsageKind::PrimaryDataPriorityLow,
                    VolumeUsageKind::Snapshot,
                ],
            ),
        ],
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new(
        "file-manager-backfill-snapshot-lifecycle",
    ));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
    let low_volume = fm.primary_volume_by_rank(1).unwrap();
    let high_volume = fm.primary_volume_by_rank(3).unwrap();

    let (file_id, mut writer) = fm.create_data_file().unwrap();
    writer.write(b"snapshot-retained").unwrap();
    writer.close().unwrap();
    assert!(
        fm.move_file_to_primary_volume(file_id, &low_volume)
            .unwrap()
    );
    let low_path = fm.get_data_file_path(file_id).unwrap();
    let snapshot_ref = fm.data_file_ref(file_id).unwrap();

    assert!(
        fm.move_file_to_primary_volume(file_id, &high_volume)
            .unwrap()
    );
    assert!(
        low_fs.exists(&low_path).unwrap(),
        "backfill must retain the low-tier source while a snapshot references it"
    );
    assert_eq!(
        fm.get_logical_file(file_id).unwrap().replica_ids().len(),
        1,
        "the backfilled source is retained only by the snapshot reference"
    );

    snapshot_ref.dereference();
    assert!(low_fs.exists(&low_path).unwrap());
    drop(snapshot_ref);
    test_utils::wait_for_file_deletion(&low_fs, &low_path);
    assert!(
        !low_fs.exists(&low_path).unwrap(),
        "the source may be deleted only after the snapshot reference is released"
    );
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
        .as_deref(),
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
        .as_deref(),
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
fn test_move_deletes_uncommitted_target_when_file_is_removed_during_copy() {
    let dir = tempfile::tempdir().unwrap();
    let source_root = dir.path().join("source");
    let target_root = dir.path().join("target");
    let config = Config {
        volumes: vec![
            crate::VolumeDescriptor::new(
                format!("file://{}", source_root.display()),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            crate::VolumeDescriptor::new(
                format!("file://{}", target_root.display()),
                vec![VolumeUsageKind::PrimaryDataPriorityLow],
            ),
        ],
        base_file_size: Size::from_const(64),
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("move-delete-during-copy"));
    let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
    let (file_id, mut writer) = fm.create_data_file().unwrap();
    writer.write(&vec![b'x'; 1024]).unwrap();
    writer.close().unwrap();
    drop(writer);
    let source_path = fm.get_data_file_path(file_id).unwrap();
    let source_fs = fm.primary_volume_by_rank(3).unwrap().fs().clone();
    let target_volume = fm.primary_volume_by_rank(1).unwrap();
    let target_fs = target_volume.fs().clone();
    let mut copied_target_path = None;
    let mut removed = false;
    let moved = {
        let mut progress = |_: u64| {
            if removed {
                return;
            }
            let target_name = target_fs
                .list("db/data")
                .unwrap()
                .into_iter()
                .next()
                .expect("copy target should exist before commit");
            copied_target_path = Some(format!("db/data/{target_name}"));
            fm.remove_data_file(file_id).unwrap();
            removed = true;
        };
        fm.move_file_to_primary_volume_with_progress(
            file_id,
            &target_volume,
            &mut progress,
            &mut || {},
        )
        .unwrap()
    };

    assert!(!moved, "a logically removed file must not commit its copy");
    assert!(!fm.has_data_file(file_id));
    let copied_target_path = copied_target_path.expect("copy target path");
    test_utils::wait_for_file_deletion(&target_fs, &copied_target_path);
    assert!(
        !target_fs.exists(&copied_target_path).unwrap(),
        "the uncommitted target copy must be deleted"
    );
    test_utils::wait_for_file_deletion(&source_fs, &source_path);
    assert!(
        !source_fs.exists(&source_path).unwrap(),
        "the owned source must also follow normal lifecycle deletion"
    );
}

#[test]
#[serial_test::serial(file)]
fn test_primary_tiering_worker_backfills_only_current_lsm_files() {
    let root = "/tmp/file_manager_backfill_worker";
    let _ = std::fs::remove_dir_all(root);
    let high = crate::VolumeDescriptor::new(
        format!("file://{root}/high"),
        vec![VolumeUsageKind::PrimaryDataPriorityHigh],
    );
    let low = crate::VolumeDescriptor::new(
        format!("file://{root}/low"),
        vec![VolumeUsageKind::PrimaryDataPriorityLow],
    );
    let config = Config {
        volumes: vec![high, low],
        base_file_size: Size::from_const(64),
        vlog_low_priority_primary_enabled: true,
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-backfill-worker"));
    let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
    let low_volume = fm.primary_volume_by_rank(1).unwrap();

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
    let l0_file_id = create_on_low(256);
    let vlog_file_id = create_on_low(256);
    let unreferenced_file_id = create_on_low(256);

    let data_file = Arc::new(crate::data_file::DataFile::new(
        crate::data_file::DataFileType::SSTable,
        vec![0],
        vec![1],
        l0_file_id,
        crate::file::TrackedFileId::new(&fm, l0_file_id),
        0,
        256,
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
        vlog_version: crate::vlog::VlogVersion::from_files_with_entries(vec![(
            0,
            crate::file::TrackedFileId::new(&fm, vlog_file_id),
            1,
        )]),
        active: current.active.clone(),
        immutables: current.immutables.clone(),
        truncation_cursors: current.truncation_cursors.clone(),
        suggested_base_snapshot_id: None,
    });
    let referenced_priorities = referenced_primary_file_priorities(&db_state);
    assert_eq!(
        referenced_priorities.get(&l0_file_id),
        Some(&crate::file::lsm_file_priority_for_level(0))
    );
    assert_eq!(
        referenced_priorities.get(&vlog_file_id),
        Some(&crate::file::VLOG_FILE_PRIORITY)
    );
    assert!(!referenced_priorities.contains_key(&unreferenced_file_id));

    let worker = fm
        .start_primary_tiering_worker(&db_state, None)
        .unwrap()
        .unwrap();
    let moved = (0..100).any(|_| {
        let on_high = fm
            .preferred_tracked_file(l0_file_id)
            .and_then(|tracked| {
                tracked
                    .volume
                    .as_ref()
                    .map(|volume| volume.priority.rank() == 3)
            })
            .unwrap_or(false);
        if !on_high {
            std::thread::sleep(Duration::from_millis(20));
        }
        on_high
    });
    worker.stop();
    worker.join();
    assert!(fm.wait_for_offload_idle(Duration::from_secs(20)));

    assert!(moved, "the current L0 file should be backfilled");
    assert_eq!(
        fm.preferred_tracked_file(vlog_file_id)
            .and_then(|tracked| { tracked.volume.as_ref().map(|volume| volume.priority.rank()) }),
        Some(1),
        "direct-low VLOG files must not be backfilled to the high-priority tier"
    );
    assert_eq!(
        fm.preferred_tracked_file(unreferenced_file_id)
            .and_then(|tracked| { tracked.volume.as_ref().map(|volume| volume.priority.rank()) }),
        Some(1),
        "an unreferenced tracked file must remain on the low-priority volume"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_create_data_file_with_offload_triggers_background_offload() {
    let root = "/tmp/file_manager_offload_trigger_watermark";
    let _ = std::fs::remove_dir_all(root);
    let high_url = format!("file://{}/high", root);
    let low_url = format!("file://{}/low", root);
    let registry = FileSystemRegistry::new();
    let high_fs = registry.get_or_register(high_url.clone()).unwrap();
    let low_fs = registry.get_or_register(low_url.clone()).unwrap();
    let mut high =
        crate::VolumeDescriptor::new(high_url, vec![VolumeUsageKind::PrimaryDataPriorityHigh]);
    high.size_limit = Some(Size::from_kib(1));
    let low = crate::VolumeDescriptor::new(low_url, vec![VolumeUsageKind::PrimaryDataPriorityLow]);
    let config = Config {
        volumes: vec![high, low],
        base_file_size: Size::from_const(64),
        primary_volume_write_stop_watermark: 0.95,
        primary_volume_offload_trigger_watermark: 0.5,
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-watermark"));
    let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());

    let (file_id, mut writer) = fm.create_data_file().unwrap();
    writer.write(&vec![b'x'; 600]).unwrap();
    writer.close().unwrap();
    let old_path = fm.get_data_file_path(file_id).unwrap();
    assert!(high_fs.exists(&old_path).unwrap());

    let (_new_id, mut new_writer) = fm.create_data_file_with_offload().unwrap();
    new_writer.write(b"small").unwrap();
    new_writer.close().unwrap();

    assert!(fm.wait_for_offload_idle(Duration::from_secs(20)));
    let new_path = fm.get_data_file_path(file_id).unwrap();
    assert_ne!(new_path, old_path);
    assert!(low_fs.exists(&new_path).unwrap());
    let _ = std::fs::remove_dir_all(root);
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

#[test]
#[serial_test::serial(file)]
fn test_write_stop_watermark_blocks_new_writes() {
    let root = "/tmp/file_manager_write_stop_watermark";
    let _ = std::fs::remove_dir_all(root);
    let high_url = format!("file://{}/high", root);
    let mut high =
        crate::VolumeDescriptor::new(high_url, vec![VolumeUsageKind::PrimaryDataPriorityHigh]);
    high.size_limit = Some(Size::from_kib(1));
    let config = Config {
        volumes: vec![high],
        base_file_size: Size::from_const(64),
        primary_volume_write_stop_watermark: 0.5,
        primary_volume_offload_trigger_watermark: 0.4,
        ..Config::default()
    };
    let metrics_manager = Arc::new(MetricsManager::new("file-manager-write-stop-watermark"));
    let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();

    let (_id, mut writer) = fm.create_data_file().unwrap();
    writer.write(&vec![b'x'; 600]).unwrap();
    writer.close().unwrap();

    let err = match fm.create_data_file() {
        Ok(_) => panic!("writes should stop after crossing write-stop watermark"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("All primary data volumes are full")
    );
    let _ = std::fs::remove_dir_all(root);
}
