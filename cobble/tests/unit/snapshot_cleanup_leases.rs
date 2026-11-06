use super::*;
use crate::rescale_protocol::{ExportLease, write_export_lease};
use crate::tool::snapshot_cleanup::tests::{cleanup_config, create_materialized_snapshot};
use crate::{CoordinatorConfig, Db, DbBuilder, DbCoordinator, ShardSnapshotMetadata};

fn lease(
    root: &std::path::Path,
    config: &Config,
    shard: &ShardSnapshotMetadata,
) -> std::path::PathBuf {
    // Publish through the same metadata writer used by real bucket adoption.
    let manager = FileManager::from_config(
        config,
        &shard.db_id,
        Arc::new(MetricsManager::new(&shard.db_id)),
    )
    .unwrap();
    write_export_lease(
        &manager,
        &ExportLease {
            version: 1,
            export_id: "adopting".into(),
            source_db_id: shard.db_id.clone(),
            target_db_id: "consumer".into(),
            snapshot_id: shard.snapshot_id,
            ranges: vec![0..=0],
        },
    )
    .unwrap();
    root.join(&shard.db_id).join("exports/LEASE-adopting.json")
}

#[test]
fn shard_range_cleanup_is_partial_and_preserves_leased_incremental_chain() {
    for incremental in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut config = cleanup_config(root.path());
        config.snapshot_disable_incremental_base_link = !incremental;
        let db = Db::open(config.clone(), vec![0..=0]).unwrap();
        let a = create_materialized_snapshot(&db, b"a", &[1; 128]);
        let b = create_materialized_snapshot(&db, b"b", &[2; 128]);
        let c = create_materialized_snapshot(&db, b"c", &[3; 128]);
        let metadata = db.shard_snapshot_metadata(b).unwrap();
        db.close().unwrap();
        drop(db);
        let lease_path = lease(root.path(), &config, &metadata);
        let options = SnapshotCleanupOptions {
            include_older_snapshots: true,
        };
        let result =
            cleanup_shard_snapshot(&config.volumes[0], &[], &metadata.db_id, c, options).unwrap();
        assert_eq!(
            result.deleted_shard_snapshots,
            if incremental { 1 } else { 2 }
        );
        let ids = result
            .preserved_snapshots
            .iter()
            .map(|s| s.snapshot_id)
            .collect::<Vec<_>>();
        assert_eq!(ids, if incremental { vec![a, b] } else { vec![b] });
        assert!(
            result
                .preserved_snapshots
                .iter()
                .all(|s| s.reasons.contains(&SnapshotPreservationReason::ExportLease))
        );
        if incremental {
            assert!(
                result.preserved_snapshots[0]
                    .reasons
                    .contains(&SnapshotPreservationReason::IncrementalDependency)
            );
        }
        let restored = Db::resume_from_snapshot(config.clone(), b, &metadata.db_id).unwrap();
        assert!(restored.get(0, b"b").unwrap().is_some());
        restored.close().unwrap();
        drop(restored);
        assert_eq!(
            cleanup_shard_snapshot(&config.volumes[0], &[], &metadata.db_id, c, options)
                .unwrap()
                .deleted_files,
            0
        );
        std::fs::remove_file(lease_path).unwrap();
        let result =
            cleanup_shard_snapshot(&config.volumes[0], &[], &metadata.db_id, c, options).unwrap();
        assert!(result.preserved_snapshots.is_empty());
        assert_eq!(
            result.deleted_shard_snapshots,
            if incremental { 2 } else { 1 }
        );
        assert_eq!(
            cleanup_shard_snapshot(&config.volumes[0], &[], &metadata.db_id, c, options).unwrap(),
            SnapshotCleanupResult::default()
        );
    }
}

fn shard(config: &Config, id: &str, bucket: u16) -> ShardSnapshotMetadata {
    let db = DbBuilder::new(config.clone())
        .db_id(id)
        .bucket_ranges(vec![bucket..=bucket])
        .open()
        .unwrap();
    db.put_with_options(bucket, b"k", 0, [42; 128], &crate::WriteOptions::default())
        .unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    db.snapshot_with_callback(move |result| {
        send.send(result).unwrap();
    })
    .unwrap();
    let metadata = recv
        .recv_timeout(std::time::Duration::from_secs(10))
        .unwrap()
        .unwrap();
    db.close().unwrap();
    metadata
}

#[test]
fn global_range_cleanup_keeps_blocked_global_intact_and_retries_after_lease_release() {
    let root = tempfile::tempdir().unwrap();
    let mut config = cleanup_config(root.path());
    config.total_buckets = 2;
    let a = shard(&config, "a", 0);
    let b = shard(&config, "b", 1);
    let c = shard(&config, "c", 0);
    let d = shard(&config, "d", 1);
    let coordinator = DbCoordinator::open(CoordinatorConfig::from_config(&config)).unwrap();
    for (id, shards) in [(10, vec![a.clone(), b.clone()]), (20, vec![c, d])] {
        let global = coordinator
            .take_global_snapshot_with_id(2, shards, id)
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();
    }
    drop(coordinator);
    let lease_path = lease(root.path(), &config, &a);
    let options = SnapshotCleanupOptions {
        include_older_snapshots: true,
    };
    let result = cleanup_global_snapshot(&config.volumes[0], &[], 20, options).unwrap();
    assert_eq!(result.deleted_global_snapshots, 1);
    assert_eq!(result.deleted_shard_snapshots, 2);
    assert_eq!(result.preserved_snapshots.len(), 3);
    assert!(
        result
            .preserved_snapshots
            .iter()
            .any(|s| s.db_id.is_none() && s.snapshot_id == 10)
    );
    assert!(root.path().join("snapshot/SNAPSHOT-10").exists());
    // Blocking one shard must not leave a retained global with a half-deleted sibling.
    for s in [&a, &b] {
        let restored = Db::resume_from_snapshot(config.clone(), s.snapshot_id, &s.db_id).unwrap();
        restored.close().unwrap();
    }
    std::fs::remove_file(lease_path).unwrap();
    let result = cleanup_global_snapshot(&config.volumes[0], &[], 20, options).unwrap();
    assert_eq!(result.deleted_global_snapshots, 1);
    assert_eq!(result.deleted_shard_snapshots, 2);
    assert!(result.preserved_snapshots.is_empty());
    assert_eq!(
        cleanup_global_snapshot(&config.volumes[0], &[], 20, options).unwrap(),
        SnapshotCleanupResult::default()
    );
}

#[test]
fn shared_shard_lease_does_not_block_removing_one_global_reference() {
    let root = tempfile::tempdir().unwrap();
    let config = cleanup_config(root.path());
    let a = shard(&config, "a", 0);
    let coordinator = DbCoordinator::open(CoordinatorConfig::from_config(&config)).unwrap();
    for id in [10, 20] {
        let global = coordinator
            .take_global_snapshot_with_id(1, vec![a.clone()], id)
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();
    }
    drop(coordinator);
    lease(root.path(), &config, &a);
    let result = cleanup_global_snapshot(
        &config.volumes[0],
        &[],
        10,
        SnapshotCleanupOptions::default(),
    )
    .unwrap();
    assert_eq!(result.deleted_global_snapshots, 1);
    assert_eq!(result.deleted_shard_snapshots, 0);
    assert_eq!(result.preserved_snapshots.len(), 1);
    assert_eq!(result.preserved_snapshots[0].db_id.as_deref(), Some("a"));
    assert!(root.path().join("snapshot/SNAPSHOT-20").exists());
}

#[test]
fn invalid_or_unresolved_leases_fail_before_any_deletion() {
    let root = tempfile::tempdir().unwrap();
    let config = cleanup_config(root.path());
    let a = shard(&config, "a", 0);
    let lease_path = lease(root.path(), &config, &a);
    let original = std::fs::read(&lease_path).unwrap();
    for field in [
        "corrupt",
        "version",
        "source_db_id",
        "snapshot_id",
        "export_id",
    ] {
        let mut json: serde_json::Value = serde_json::from_slice(&original).unwrap();
        match field {
            "version" | "snapshot_id" => json[field] = serde_json::json!(99999),
            "source_db_id" | "export_id" => json[field] = serde_json::json!("wrong"),
            _ => {}
        }
        std::fs::write(
            &lease_path,
            if field == "corrupt" {
                b"bad".to_vec()
            } else {
                serde_json::to_vec(&json).unwrap()
            },
        )
        .unwrap();
        assert!(
            cleanup_shard_snapshot(
                &config.volumes[0],
                &[],
                &a.db_id,
                a.snapshot_id,
                SnapshotCleanupOptions::default()
            )
            .is_err()
        );
        assert!(
            root.path()
                .join("a/snapshot")
                .join(format!("SNAPSHOT-{}", a.snapshot_id))
                .exists()
        );
    }
}
