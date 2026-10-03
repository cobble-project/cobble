use super::*;
use crate::metrics_manager::MetricsManager;
use crate::{Config, VolumeDescriptor};
use tokio::time::{Duration, timeout};

fn snapshot_with_base(id: u64, base_snapshot_id: Option<u64>) -> Arc<DbSnapshot> {
    let mut snapshot = DbSnapshot::new(id, &format!("SNAPSHOT-{id}"), None);
    snapshot.base_snapshot_id = base_snapshot_id;
    Arc::new(snapshot)
}

#[test]
fn suggested_base_fallback_skips_cancelled_ancestors() {
    let grandparent = snapshot_with_base(1, None);
    let parent = snapshot_with_base(2, Some(1));
    assert!(parent.try_cancel());
    let child = snapshot_with_base(3, Some(2));

    let snapshots = BTreeMap::from([
        (1, Arc::clone(&grandparent)),
        (2, Arc::clone(&parent)),
        (3, Arc::clone(&child)),
    ]);

    assert_eq!(suggested_base_fallback_id(&snapshots, 3), Some(1));
}

#[test]
fn suggested_base_fallback_clears_on_broken_chain() {
    let parent = snapshot_with_base(2, Some(99));
    assert!(parent.try_cancel());
    let child = snapshot_with_base(3, Some(2));

    let snapshots = BTreeMap::from([(2, Arc::clone(&parent)), (3, Arc::clone(&child))]);

    assert_eq!(suggested_base_fallback_id(&snapshots, 3), None);
}

#[test]
fn schema_cleanup_rechecks_references_acquired_after_snapshot_removal() {
    schema_cleanup_after_new_capture(false);
}

#[test]
fn schema_cleanup_does_not_mark_captured_schema_as_persisted() {
    schema_cleanup_after_new_capture(true);
}

fn schema_cleanup_after_new_capture(new_schema: bool) {
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root.path().display())),
        total_buckets: 4,
        num_columns: 1,
        ..Config::default()
    };
    let file_manager = Arc::new(
        FileManager::from_config(
            &config,
            "schema-cleanup-race",
            Arc::new(MetricsManager::new("schema-cleanup-race")),
        )
        .unwrap(),
    );
    let schema_manager = Arc::new(SchemaManager::new(1));
    let manager = SnapshotManager::new(
        Arc::clone(&file_manager),
        Arc::clone(&schema_manager),
        Arc::new(DbLifecycle::new_open()),
        None,
        false,
        false,
        vec![0..=3],
        Arc::new(crate::time::SystemTimeProvider),
    );
    let handle = DbStateHandle::new();
    handle.configure_multi_lsm(4, &[0..=3]).unwrap();
    let first = manager.create_snapshot(None);
    assert!(manager.finish_snapshot(first.id, &handle.load(), Vec::new(), &handle, None, 0, None));
    manager.materialize(first.id).unwrap();

    // Pause expiration at its actual boundary: the old record/refcounts are removed, but
    // physical schema cleanup has not run. Capture a new owner before resuming that cleanup.
    {
        let mut state = manager.state.lock().unwrap();
        let removed = state.snapshots.remove(&first.id).unwrap();
        state.completed.remove(&first.id);
        state.incremental_references.remove(&first.id);
        state.incremental_ref_counts.remove(&first.id);
        decrement_schema_ref_counts(&mut state.schema_ref_counts, &removed.referenced_schema_ids);
        assert!(state.schema_ref_counts.is_empty());
    }
    if new_schema {
        let mut builder = schema_manager.builder();
        builder.add_column(1, None, None, None).unwrap();
        builder.commit();
    }
    let next = manager.create_snapshot(None);
    assert!(manager.finish_snapshot(next.id, &handle.load(), Vec::new(), &handle, None, 0, None));
    manager.cleanup_expired_schema_files().unwrap();
    let schema_dir = root.path().join("schema-cleanup-race/schema");
    assert_eq!(schema_dir.join("schema-0").exists(), !new_schema);
    assert!(!schema_dir.join("schema-1").exists());
    manager.materialize(next.id).unwrap();
    if new_schema {
        assert!(schema_dir.join("schema-1").exists());
        // Deleting an earlier schema must not make a later persisted schema appear absent.
        manager.cleanup_expired_schema_files().unwrap();
        assert!(!schema_dir.join("schema-0").exists());
        assert!(schema_dir.join("schema-1").exists());
    }
    let manifest_path = file_manager
        .get_metadata_file_full_path(&snapshot_manifest_name(next.id))
        .unwrap();
    manager.close().unwrap();
    let restored = crate::Db::open_new_with_manifest_path(config, manifest_path).unwrap();
    assert_eq!(restored.current_schema().version(), u64::from(new_schema));
    restored.close().unwrap();
}

#[test]
fn snapshot_copy_permits_bound_concurrent_transfers() {
    let root = "/tmp/snapshot_copy_transfer_budget";
    let _ = std::fs::remove_dir_all(root);
    let file_manager = FileManager::from_config(
        &Config {
            file_transfer_concurrency: 2,
            volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
            ..Config::default()
        },
        "snapshot-copy-transfer-budget",
        Arc::new(MetricsManager::new("snapshot-copy-transfer-budget")),
    )
    .unwrap();
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        let first = acquire_snapshot_transfer_permit(&file_manager)
            .await
            .unwrap();
        let second = acquire_snapshot_transfer_permit(&file_manager)
            .await
            .unwrap();

        assert!(
            timeout(
                Duration::from_millis(20),
                acquire_snapshot_transfer_permit(&file_manager)
            )
            .await
            .is_err()
        );

        drop(first);
        assert!(
            timeout(
                Duration::from_secs(1),
                acquire_snapshot_transfer_permit(&file_manager)
            )
            .await
            .unwrap()
            .is_ok()
        );
        drop(second);
    });
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn schema_cleanup_and_materialization_preserve_sparse_restored_versions() {
    for versions in [vec![2], vec![0, 5]] {
        let root = tempfile::tempdir().unwrap();
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root.path().display())),
            total_buckets: 4,
            num_columns: 1,
            ..Config::default()
        };
        let file_manager = Arc::new(
            FileManager::from_config(
                &config,
                "sparse-schemas",
                Arc::new(MetricsManager::new("sparse-schemas")),
            )
            .unwrap(),
        );
        let schemas = versions
            .iter()
            .map(|id| {
                crate::schema::Schema::new(
                    *id,
                    1,
                    vec![crate::merge_operator::default_merge_operator()],
                )
            })
            .collect::<Vec<_>>();
        for schema in &schemas {
            crate::schema::persist_schema(&file_manager, schema).unwrap();
        }
        let schema_manager = Arc::new(SchemaManager::from_schemas(schemas, 1, None));
        let manager = SnapshotManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&schema_manager),
            Arc::new(DbLifecycle::new_open()),
            None,
            false,
            false,
            vec![0..=3],
            Arc::new(crate::time::SystemTimeProvider),
        );
        manager.cleanup_expired_schema_files().unwrap();
        let schema_dir = root.path().join("sparse-schemas/schema");
        for id in &versions {
            assert!(!schema_dir.join(format!("schema-{id}")).exists());
        }
        let handle = DbStateHandle::new();
        handle.configure_multi_lsm(4, &[0..=3]).unwrap();
        if versions.len() > 1 {
            let missing = manager.create_snapshot(None);
            assert!(manager.finish_snapshot(
                missing.id,
                &handle.load(),
                Vec::new(),
                &handle,
                None,
                0,
                None
            ));
            // Sparse loaded versions are valid, but a manifest actually requiring an absent
            // intermediate version must still fail rather than silently skipping that ref.
            {
                let mut state = manager.state.lock().unwrap();
                let mut record = (**state.snapshots.get(&missing.id).unwrap()).clone();
                record.referenced_schema_ids.insert(1);
                state.schema_ref_counts.insert(1, 1);
                state.snapshots.insert(missing.id, Arc::new(record));
            }
            assert!(matches!(
                manager.materialize(missing.id),
                Err(Error::InvalidState(message)) if message == "Missing schema version 1"
            ));
        }
        let next = manager.create_snapshot(None);
        assert!(manager.finish_snapshot(
            next.id,
            &handle.load(),
            Vec::new(),
            &handle,
            None,
            0,
            None
        ));
        manager.materialize(next.id).unwrap();
        for id in &versions {
            assert!(schema_dir.join(format!("schema-{id}")).exists());
        }
        assert!(!schema_dir.join("schema-1").exists());
        let manifest_path = file_manager
            .get_metadata_file_full_path(&snapshot_manifest_name(next.id))
            .unwrap();
        manager.close().unwrap();
        let restored = crate::Db::open_new_with_manifest_path(config, manifest_path).unwrap();
        assert_eq!(
            restored.current_schema().version(),
            *versions.last().unwrap()
        );
        let (tx, rx) = std::sync::mpsc::channel();
        restored
            .snapshot_with_callback(move |result| {
                tx.send(result).unwrap();
            })
            .unwrap();
        rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
        restored.close().unwrap();
    }
}
