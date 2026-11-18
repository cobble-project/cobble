//! Adoption placement and persistent-cache lifecycle coverage.

use super::*;
use crate::data_file::{DataFile, DataFileType};
use crate::file::FileSystemRegistry;
use crate::{Config, MetricsManager, VolumeUsageKind};
use size::Size;
use std::time::Duration;

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
    let high_cache_path = fm.get_data_file_path(file_id).unwrap();
    assert_eq!(high_cache_path, "db/data/external.sst");
    assert!(
        fm.move_persistent_cache_to_volume_with_progress(
            file_id,
            &low_volume,
            &mut |_| {},
            &mut || {},
        )
        .unwrap()
    );
    assert_eq!(fm.get_data_file_path(file_id).unwrap(), high_cache_path);
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
        active_schema: current.active_schema.clone(),
        min_source_schema_by_cf: Vec::new(),
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
