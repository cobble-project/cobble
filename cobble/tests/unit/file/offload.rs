use super::*;
use crate::file::logical_file::ReplicaLifecycle;
use crate::file::{File, FileSystemRegistry, RandomAccessFile, SequentialWriteFile, test_utils};
use crate::{Config, MetricsManager, VolumeUsageKind};
use size::Size;

impl FileManager {
    pub(super) fn wait_for_offload_idle(&self, timeout: Duration) -> bool {
        self.offload_runtime.wait_idle(timeout)
    }

    pub(super) fn primary_volume_by_rank(&self, rank: u8) -> Option<Arc<DataVolume>> {
        self.offload_runtime.primary_volume_by_rank(rank)
    }
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
    assert_eq!(fm.offload_runtime.semaphore.available_permits(), 2);
    assert_eq!(source.projected_offload_bytes(), 0);
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
    let mut residual_writer = low_fs.open_write(&old_path).unwrap();
    residual_writer.write(&payload).unwrap();
    residual_writer.close().unwrap();
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
    assert!(
        low_fs.exists(&old_path).unwrap(),
        "normal offload must not adopt an unregistered same-name residual"
    );
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
    let source_absolute_path = source.absolute_path();
    let source_volume = Arc::clone(source.volume.as_ref().unwrap());
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
    let snapshot_replica_absolute_path = snapshot_replica.absolute_path();
    let snapshot_volume = Arc::clone(snapshot_replica.volume.as_ref().unwrap());
    assert_eq!(source_path, snapshot_replica_path);
    assert_ne!(source_absolute_path, snapshot_replica_absolute_path);
    assert!(!Arc::ptr_eq(&source_volume, &snapshot_volume));

    let target_volume = fm.primary_volume_by_rank(1).unwrap();
    let promoted = fm
        .move_file_to_primary_volume(source_file_id, &target_volume)
        .unwrap();
    assert!(promoted);
    assert_eq!(
        fm.get_data_file_path(source_file_id).unwrap(),
        snapshot_replica_path
    );
    assert_eq!(logical.replica_ids().len(), 1);
    assert!(Arc::ptr_eq(
        logical
            .preferred_replica_any()
            .unwrap()
            .tracked
            .volume
            .as_ref()
            .unwrap(),
        &target_volume
    ));
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
    let l0_low_path = fm.get_data_file_path(l0_file_id).unwrap();
    std::fs::copy(
        format!("{root}/low/{l0_low_path}"),
        format!("{root}/high/{l0_low_path}"),
    )
    .unwrap();

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
        active_schema: current.active_schema.clone(),
        min_source_schema_by_cf: Vec::new(),
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
    assert_ne!(
        fm.get_data_file_path(l0_file_id).as_deref(),
        Some(l0_low_path.as_str()),
        "runtime backfill must not adopt an unregistered residual"
    );
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
    let low = crate::VolumeDescriptor::new(
        low_url,
        vec![
            VolumeUsageKind::PrimaryDataPriorityLow,
            VolumeUsageKind::Snapshot,
        ],
    );
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
    assert_eq!(new_path, old_path);
    assert!(low_fs.exists(&new_path).unwrap());

    let current = fm.data_file_ref(file_id).unwrap();
    assert!(current.absolute_path().contains("/low/"));
    let logical = fm.get_logical_file(file_id).unwrap();
    let snapshot_replica = fm
        .snapshot_replica_for_tracked_file(file_id, &current, Some(&logical), None, None)
        .unwrap();
    assert!(Arc::ptr_eq(&current, &snapshot_replica));
    assert_eq!(snapshot_replica.path(), old_path);
    current.dereference();
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
