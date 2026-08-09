use super::*;
use crate::db_state::full_bucket_range;
use crate::file::FileManager;
use crate::metrics_manager::MetricsManager;
use crate::{Config, DbBuilder, RuntimeManifestMode, VolumeDescriptor};
use serial_test::serial;
use size::Size;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

fn cleanup_test_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn adoption_barrier_requires_every_live_import() {
    let record = |barrier| ImportRecord {
        version: 1,
        export_id: "export".to_string(),
        source_db_id: "source".to_string(),
        snapshot_id: 1,
        target_db_id: "target".to_string(),
        ranges: vec![0..=0],
        import_snapshot_id: Some(2),
        adoption_barrier_snapshot_id: barrier,
    };
    assert_eq!(
        shared_adoption_barrier_id(&[record(Some(3)), record(Some(3))]),
        Some(3)
    );
    assert_eq!(
        shared_adoption_barrier_id(&[record(Some(3)), record(None)]),
        None
    );
}

#[test]
#[serial(file)]
fn test_expand_bucket_from_latest_snapshot() {
    let root = "/tmp/db_expand_bucket";
    cleanup_test_root(root);
    let mut config = Config {
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        sst_bloom_filter_enabled: true,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        ..Config::default()
    };
    config.total_buckets = 8;
    config.file_transfer_concurrency = 1;
    let source = Db::open(config.clone(), vec![2u16..=3u16]).unwrap();
    source.put(2, b"k1", 0, b"v1").unwrap();
    let (tx, rx) = mpsc::channel();
    let source_snapshot = source
        .snapshot_with_callback(move |result| {
            let _ = tx.send(result);
        })
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .snapshot_id,
        source_snapshot
    );
    source.put(2, b"k1-second", 0, b"v1-second").unwrap();
    let (tx, rx) = mpsc::channel();
    let source_snapshot = source
        .snapshot_with_callback(move |result| {
            let _ = tx.send(result);
        })
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .snapshot_id,
        source_snapshot
    );

    let mut target_config = config.clone();
    target_config.runtime_manifest_mode = RuntimeManifestMode::Enabled;
    let target = Db::open(target_config, vec![0u16..=1u16]).unwrap();
    let imported_snapshot = target
        .expand_bucket(source.id().to_string(), Some(source_snapshot), None)
        .unwrap();
    assert_eq!(imported_snapshot, source_snapshot);
    assert!(
        !source.expire_snapshot(source_snapshot).unwrap(),
        "the export lease must keep the source snapshot alive until adoption finishes"
    );

    let value = target.get(2, b"k1").unwrap().unwrap();
    assert_eq!(value[0].as_deref(), Some(&b"v1"[..]));

    target.put(3, b"k2", 0, b"v2").unwrap();
    let value = target.get(3, b"k2").unwrap().unwrap();
    assert_eq!(value[0].as_deref(), Some(&b"v2"[..]));

    let target_id = target.id().to_string();
    let target_metrics = Arc::new(MetricsManager::new("expand-target-manifest"));
    let target_file_manager =
        Arc::new(FileManager::from_config(&config, &target_id, target_metrics).unwrap());
    let import_snapshot = *list_snapshot_manifest_ids(&target_file_manager)
        .unwrap()
        .last()
        .unwrap();
    let import_manifest =
        crate::snapshot::load_manifest_for_snapshot(&target_file_manager, import_snapshot).unwrap();
    assert_eq!(import_manifest.bucket_ranges, vec![0u16..=3u16]);
    let imported_origins = import_manifest
        .tree_levels
        .iter()
        .flatten()
        .flat_map(|level| level.files.iter())
        .map(|file| &file.origin)
        .chain(import_manifest.vlog_files.iter().map(|file| &file.origin))
        .collect::<Vec<_>>();
    assert!(!imported_origins.is_empty());
    assert!(
        imported_origins
            .iter()
            .all(|origin| { matches!(origin, ReplicaOrigin::ExternalLeased { .. }) })
    );
    target
        .wait_for_expand_adoption(Duration::from_secs(10))
        .unwrap();
    let target_snapshot = *list_snapshot_manifest_ids(&target_file_manager)
        .unwrap()
        .last()
        .unwrap();
    let target_manifest =
        crate::snapshot::load_manifest_for_snapshot(&target_file_manager, target_snapshot).unwrap();
    assert!(
        target_manifest
            .tree_levels
            .iter()
            .flatten()
            .flat_map(|level| level.files.iter())
            .all(|file| matches!(file.origin, ReplicaOrigin::Owned))
    );
    assert!(
        target_file_manager
            .list_metadata_names("imports")
            .unwrap()
            .is_empty()
    );
    let runtime_store =
        crate::runtime_manifest::RuntimeManifestStore::new(Arc::clone(&target.file_manager));
    let runtime_manifest = (0..100)
        .find_map(|_| {
            let current = runtime_store.load_current().unwrap();
            if current.as_ref().is_some_and(|manifest| {
                manifest
                    .manifest
                    .tree_levels
                    .iter()
                    .flatten()
                    .flat_map(|level| level.files.iter())
                    .all(|file| matches!(file.origin, ReplicaOrigin::Owned))
            }) {
                current
            } else {
                std::thread::sleep(Duration::from_millis(10));
                None
            }
        })
        .expect("runtime manifest must publish adopted owned paths");
    assert!(
        runtime_manifest
            .manifest
            .tree_levels
            .iter()
            .flatten()
            .flat_map(|level| level.files.iter())
            .all(|file| matches!(file.origin, ReplicaOrigin::Owned))
    );
    let persistent = Db::open(config.clone(), vec![4u16..=5u16]).unwrap();
    persistent
        .expand_bucket_with_storage_mode(
            source.id().to_string(),
            Some(source_snapshot),
            None,
            ExpandStorageMode::ReferencePersistent,
        )
        .unwrap();
    let persistent_file_manager = Arc::new(
        FileManager::from_config(
            &config,
            persistent.id(),
            Arc::new(MetricsManager::new("expand-persistent-manifest")),
        )
        .unwrap(),
    );
    let persistent_snapshot = *list_snapshot_manifest_ids(&persistent_file_manager)
        .unwrap()
        .last()
        .unwrap();
    let persistent_manifest =
        crate::snapshot::load_manifest_for_snapshot(&persistent_file_manager, persistent_snapshot)
            .unwrap();
    assert!(
        persistent_manifest
            .tree_levels
            .iter()
            .flatten()
            .flat_map(|level| level.files.iter())
            .all(|file| matches!(file.origin, ReplicaOrigin::ExternalPersistent { .. }))
    );

    let persistent_file_ids = persistent
        .db_state
        .load()
        .multi_lsm_version
        .tree_versions_cloned()
        .into_iter()
        .flat_map(|tree| tree.levels.clone().into_iter())
        .flat_map(|level| level.files.into_iter())
        .map(|file| file.file_id)
        .collect::<Vec<_>>();
    std::thread::sleep(Duration::from_millis(1100));
    assert!(persistent_file_ids.iter().all(|file_id| {
        matches!(
            persistent.file_manager.preferred_replica_origin(*file_id),
            Some(ReplicaOrigin::ExternalPersistent { .. })
        )
    }));

    let mut cached_config = config.clone();
    cached_config.runtime_manifest_mode = RuntimeManifestMode::Enabled;
    let cached = Db::open(cached_config, vec![6u16..=7u16]).unwrap();
    cached
        .expand_bucket_with_storage_mode(
            source.id().to_string(),
            Some(source_snapshot),
            None,
            ExpandStorageMode::ReferencePersistentWithCache,
        )
        .unwrap();
    let cached_file_ids = cached
        .db_state
        .load()
        .multi_lsm_version
        .tree_versions_cloned()
        .into_iter()
        .flat_map(|tree| tree.levels.clone().into_iter())
        .flat_map(|level| level.files.into_iter())
        .map(|file| file.file_id)
        .collect::<Vec<_>>();
    let deadline = Instant::now() + Duration::from_secs(10);
    while cached_file_ids.iter().any(|file_id| {
        !matches!(
            cached.file_manager.preferred_replica_origin(*file_id),
            Some(ReplicaOrigin::Owned)
        )
    }) && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(cached_file_ids.iter().all(|file_id| {
        matches!(
            cached.file_manager.preferred_replica_origin(*file_id),
            Some(ReplicaOrigin::Owned)
        )
    }));
    assert_eq!(
        cached.get(2, b"k1").unwrap().unwrap()[0].as_deref(),
        Some(&b"v1"[..])
    );
    assert!(cached_file_ids.iter().all(|file_id| {
        matches!(
            cached
                .file_manager
                .durable_data_file_path_with_origin(*file_id)
                .map(|(_, origin)| origin),
            Some(ReplicaOrigin::ExternalPersistent { .. })
        )
    }));
    let cached_runtime_store =
        crate::runtime_manifest::RuntimeManifestStore::new(Arc::clone(&cached.file_manager));
    let cached_runtime_manifest = cached_runtime_store.load_current().unwrap().unwrap();
    assert!(
        cached_runtime_manifest
            .manifest
            .tree_levels
            .iter()
            .flatten()
            .flat_map(|level| level.files.iter())
            .all(|file| matches!(file.origin, ReplicaOrigin::ExternalPersistent { .. }))
    );

    let cached_id = cached.id().to_string();
    let (cache_tx, cache_rx) = mpsc::channel();
    let cached_snapshot = cached
        .snapshot_with_callback(move |result| {
            let _ = cache_tx.send(result);
        })
        .unwrap();
    assert_eq!(
        cache_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .snapshot_id,
        cached_snapshot
    );
    let cached_manifest_manager = Arc::new(
        FileManager::from_config(
            &config,
            &cached_id,
            Arc::new(MetricsManager::new("expand-persistent-cache-manifest")),
        )
        .unwrap(),
    );
    let cached_manifest =
        crate::snapshot::load_manifest_for_snapshot(&cached_manifest_manager, cached_snapshot)
            .unwrap();
    assert!(
        cached_manifest
            .tree_levels
            .iter()
            .flatten()
            .flat_map(|level| level.files.iter())
            .all(|file| matches!(file.origin, ReplicaOrigin::ExternalPersistent { .. }))
    );
    for file_id in &cached_file_ids {
        assert!(
            cached
                .file_manager
                .evict_preferred_persistent_cache(*file_id)
                .unwrap()
        );
    }
    assert!(cached_file_ids.iter().all(|file_id| {
        matches!(
            cached.file_manager.preferred_replica_origin(*file_id),
            Some(ReplicaOrigin::ExternalPersistent { .. })
        )
    }));
    drop(cached);
    let reopened_cached =
        Db::open_from_snapshot(config.clone(), cached_snapshot, cached_id).unwrap();
    // Cache requests are runtime policy. Reopening restores the durable external route;
    // the embedding runtime may request local loading again after startup.
    assert!(cached_file_ids.iter().all(|file_id| {
        matches!(
            reopened_cached
                .file_manager
                .preferred_replica_origin(*file_id),
            Some(ReplicaOrigin::ExternalPersistent { .. })
        )
    }));
    assert_eq!(
        reopened_cached.get(2, b"k1").unwrap().unwrap()[0].as_deref(),
        Some(&b"v1"[..])
    );
    drop(reopened_cached);
    drop(persistent);
    assert!(source.get(2, b"k1").unwrap().is_some());

    assert!(source.expire_snapshot(source_snapshot).unwrap());
    drop(source);
    drop(target);
    let reopened = Db::open_from_snapshot(config.clone(), target_snapshot, target_id).unwrap();
    assert_eq!(
        reopened.get(2, b"k1").unwrap().unwrap()[0].as_deref(),
        Some(&b"v1"[..])
    );
    drop(reopened);
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_expand_bucket_outside_source_rejected() {
    let root = "/tmp/db_expand_bucket_outside_source";
    cleanup_test_root(root);
    let mut config = Config {
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        sst_bloom_filter_enabled: true,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        ..Config::default()
    };
    config.total_buckets = 8;
    let source = Db::open(config.clone(), vec![1u16..=2u16]).unwrap();
    source.put(1, b"k1", 0, b"v1").unwrap();
    let target = Db::open(config, vec![3u16..=4u16]).unwrap();
    let (tx, rx) = mpsc::channel();
    let snapshot_id = source
        .snapshot_with_callback(move |result| {
            let _ = tx.send(result);
        })
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .snapshot_id,
        snapshot_id
    );
    let err = target
        .expand_bucket(
            source.id().to_string(),
            Some(snapshot_id),
            Some(vec![0u16..=1u16]),
        )
        .unwrap_err();
    assert!(matches!(err, Error::ConfigError(_)));

    drop(target);
    drop(source);
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_expand_bucket_accepts_full_range_with_empty_source() {
    let root = "/tmp/db_expand_bucket_empty_source";
    cleanup_test_root(root);
    let config = Config {
        total_buckets: 4,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        ..Config::default()
    };
    let source = Db::open(config.clone(), vec![2u16..=3u16]).unwrap();
    let (tx, rx) = mpsc::channel();
    let snapshot_id = source
        .snapshot_with_callback(move |result| {
            let _ = tx.send(result);
        })
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .snapshot_id,
        snapshot_id
    );
    let target = Db::open(config, std::iter::once(full_bucket_range(2)).collect()).unwrap();
    target
        .expand_bucket(
            source.id().to_string(),
            Some(snapshot_id),
            Some(vec![2u16..=3u16]),
        )
        .unwrap();
    target.put(2, b"k", 0, b"v").unwrap();
    let got = target.get(2, b"k").unwrap().unwrap();
    assert_eq!(got[0].as_deref(), Some(&b"v"[..]));
    drop(target);
    drop(source);
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_expand_bucket_restores_active_memtable_segments() {
    let root = "/tmp/db_expand_bucket_active_segments";
    cleanup_test_root(root);
    let config = Config {
        total_buckets: 8,
        memtable_capacity: Size::from_kib(8),
        memtable_buffer_count: 2,
        num_columns: 1,
        value_separation_threshold: Some(Size::from_const(1)),
        active_memtable_incremental_snapshot_ratio: 1.0,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        ..Config::default()
    };
    let source = Db::open(config.clone(), vec![4u16..=5u16]).unwrap();
    source.put(4, b"k-sep", 0, b"payload-separated").unwrap();
    let (tx, rx) = mpsc::channel();
    let snapshot_id = source
        .snapshot_with_callback(move |result| {
            let _ = tx.send(result);
        })
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .snapshot_id,
        snapshot_id
    );
    let source_metrics = Arc::new(MetricsManager::new("rescale-source-manifest"));
    let source_file_manager =
        Arc::new(FileManager::from_config(&config, source.id(), source_metrics).unwrap());
    let source_manifest =
        crate::snapshot::load_manifest_for_snapshot(&source_file_manager, snapshot_id).unwrap();
    assert!(!source_manifest.active_memtable_data.is_empty());

    let target = Db::open(config, vec![0u16..=1u16]).unwrap();
    target
        .expand_bucket(source.id().to_string(), Some(snapshot_id), None)
        .unwrap();
    let got = target.get(4, b"k-sep").unwrap().unwrap();
    assert_eq!(got[0].as_deref(), Some(&b"payload-separated"[..]));

    drop(target);
    drop(source);
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_shrink_bucket_removes_data_from_kicked_range() {
    let root = "/tmp/db_shrink_bucket";
    cleanup_test_root(root);
    let config = Config {
        total_buckets: 8,
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        sst_bloom_filter_enabled: true,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        ..Config::default()
    };
    let db = Db::open(config.clone(), vec![0u16..=3u16]).unwrap();
    db.put(1, b"k1", 0, b"v1").unwrap();
    db.put(2, b"k2", 0, b"v2").unwrap();

    let shrink_snapshot = db.shrink_bucket(vec![2u16..=3u16]).unwrap();
    let bucket_input = db.shard_snapshot_input(shrink_snapshot).unwrap();
    assert_eq!(bucket_input.ranges, vec![0u16..=1u16]);

    let kept = db.get(1, b"k1").unwrap().unwrap();
    assert_eq!(kept[0].as_deref(), Some(&b"v1"[..]));
    let removed = db.get(2, b"k2").unwrap();
    assert!(removed.is_none());

    let metrics = Arc::new(MetricsManager::new("shrink-manifest"));
    let file_manager = Arc::new(FileManager::from_config(&config, db.id(), metrics).unwrap());
    let manifest =
        crate::snapshot::load_manifest_for_snapshot(&file_manager, shrink_snapshot).unwrap();
    assert_eq!(manifest.bucket_ranges, vec![0u16..=3u16]);

    let post_shrink_snapshot = *list_snapshot_manifest_ids(&file_manager)
        .unwrap()
        .last()
        .unwrap();
    let post_shrink_manifest =
        crate::snapshot::load_manifest_for_snapshot(&file_manager, post_shrink_snapshot).unwrap();
    assert_eq!(post_shrink_manifest.bucket_ranges, vec![0u16..=1u16]);
    let db_id = db.id().to_string();
    drop(db);
    let reopened = Db::open_from_snapshot(config.clone(), post_shrink_snapshot, db_id).unwrap();
    assert_eq!(
        reopened.get(1, b"k1").unwrap().unwrap()[0].as_deref(),
        Some(&b"v1"[..])
    );
    assert!(reopened.get(2, b"k2").unwrap().is_none());
    drop(reopened);
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_shrink_bucket_rejects_outside_range() {
    let root = "/tmp/db_shrink_bucket_outside";
    cleanup_test_root(root);
    let config = Config {
        total_buckets: 8,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        ..Config::default()
    };
    let db = Db::open(config, vec![0u16..=1u16]).unwrap();
    let err = db.shrink_bucket(vec![2u16..=2u16]).unwrap_err();
    assert!(matches!(err, Error::ConfigError(_)));
    drop(db);
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn test_shrink_bucket_rejects_removing_all_ranges() {
    let root = "/tmp/db_shrink_bucket_all";
    cleanup_test_root(root);
    let config = Config {
        total_buckets: 8,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        ..Config::default()
    };
    let db = Db::open(config, vec![0u16..=1u16]).unwrap();
    let err = db.shrink_bucket(vec![0u16..=1u16]).unwrap_err();
    assert!(matches!(err, Error::ConfigError(_)));
    drop(db);
    cleanup_test_root(root);
}

#[test]
#[serial(file)]
fn dedicated_rescale_rejects_pending_compaction_result() {
    let root = "/tmp/db_dedicated_rescale_pending_result";
    cleanup_test_root(root);
    let mut config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        compaction_mode: crate::config::CompactionMode::Dedicated,
        compaction_dedicated_poll_interval_ms: 1_000,
        ..Config::default()
    };
    config.total_buckets = 4;
    let db = DbBuilder::new(config)
        .bucket_ranges(vec![0u16..=3u16])
        .db_id("dedicated-rescale-pending")
        .open()
        .unwrap();
    if let Some(poller) = &db.dedicated_poller {
        poller.stop();
        poller.join();
    }
    let result = crate::compaction::dedicated::DedicatedCompactionResult {
        version: crate::compaction::dedicated::DEDICATED_COMPACTION_RESULT_VERSION,
        job_id: "pending-rescale".to_string(),
        source: crate::compaction::dedicated::DedicatedCompactionSource::Runtime {
            generation: 1,
            seq_id: db.db_state.load().seq_id,
        },
        topology_epoch: db.db_state.load().topology_epoch,
        lsm_tree_idx: 0,
        tree_scope: LSMTreeScope::new(0u16..=3u16, 0),
        operation: crate::compaction::dedicated::DedicatedCompactionOperation::Drop {
            inputs: Vec::new(),
        },
        vlog_entry_deltas: Vec::new(),
        created_at_ms: 0,
    };
    crate::compaction::dedicated::publish_dedicated_compaction_result(&db.file_manager, &result)
        .unwrap();

    let err = db.shrink_bucket(vec![0u16..=0u16]).unwrap_err();
    assert!(err.to_string().contains("dedicated compaction is active"));

    crate::compaction::dedicated::delete_dedicated_compaction_result(
        &db.file_manager,
        &result.job_id,
    )
    .unwrap();
    db.close().unwrap();
    cleanup_test_root(root);
}
