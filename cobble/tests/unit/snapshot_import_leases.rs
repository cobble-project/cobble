use super::*;
use crate::{
    Config, Db, ImportStorageMode, SnapshotCleanupOptions, VolumeDescriptor, VolumeUsageKind,
};
use size::Size;
use std::path::Path;
use std::time::Duration;

fn reclaim_import_leases(
    metadata: &impl LeaseMetadata,
    db_id: &str,
    referenced: &BTreeSet<String>,
    mode: LeaseCleanupMode,
) -> Result<()> {
    ImportLeases::load(metadata, db_id)?.reclaim(metadata, referenced, mode)
}

fn config(root: &Path, sources: &[&Path], blocked: bool) -> Config {
    let mut volumes = VolumeDescriptor::single_volume(root.to_str().unwrap());
    if blocked {
        // Keep adoption pending deterministically, without relying on copy timing.
        volumes[0].size_limit = Some(Size::from_const(1));
    }
    volumes.extend(sources.iter().map(|path| {
        VolumeDescriptor::new(path.to_str().unwrap(), vec![VolumeUsageKind::Readonly])
    }));
    Config {
        volumes,
        total_buckets: 8,
        num_columns: 1,
        snapshot_retention: None,
        snapshot_disable_incremental_base_link: true,
        snapshot_on_flush: false,
        active_memtable_incremental_snapshot_ratio: 0.0,
        l0_file_limit: 100,
        value_separation_threshold: Some(Size::from_const(32)),
        ..Config::default()
    }
}

fn snapshot(db: &Db) -> crate::ShardSnapshotMetadata {
    let (send, receive) = std::sync::mpsc::channel();
    db.snapshot_with_callback(move |result| {
        let _ = send.send(result);
    })
    .unwrap();
    receive
        .recv_timeout(Duration::from_secs(20))
        .unwrap()
        .unwrap()
}

fn cleanup(config: &Config, id: &str) -> crate::SnapshotCleanupResult {
    crate::cleanup_shard_snapshot(
        &config.volumes[0],
        &config.volumes[1..],
        id,
        u64::MAX,
        SnapshotCleanupOptions {
            include_older_snapshots: true,
        },
    )
    .unwrap()
}

#[test]
fn offline_retired_middle_import_releases_upstream_after_successor_detaches() {
    let dir = tempfile::tempdir().unwrap();
    let a_root = dir.path().join("a");
    let b_root = dir.path().join("b");
    let c_root = dir.path().join("c");
    let a_config = config(&a_root, &[], false);
    let a = Db::open(a_config.clone(), vec![0..=7]).unwrap();
    a.put(4, b"key", 0, vec![b'x'; 4096]).unwrap();
    let a_cp = snapshot(&a);
    a.close().unwrap();
    drop(a);
    let b_config = config(&b_root, &[&a_root], true);
    let b = Db::open_new_with_manifest_path(
        b_config.clone(),
        &a_cp.manifest_path,
        Some(vec![4..=5]),
        ImportStorageMode::AdoptAsync,
    )
    .unwrap();
    let b_cp = snapshot(&b);
    assert!(
        b.wait_for_import_adoption(Duration::from_millis(20))
            .is_err()
    );
    b.close().unwrap();
    drop(b);

    let c_config = config(&c_root, &[&a_root, &b_root], true);
    let c = Db::open_new_with_manifest_path(
        c_config,
        &b_cp.manifest_path,
        Some(vec![4..=4]),
        ImportStorageMode::AdoptAsync,
    )
    .unwrap();
    let c_cp = snapshot(&c);
    assert!(
        c.wait_for_import_adoption(Duration::from_millis(20))
            .is_err()
    );
    c.close().unwrap();
    drop(c);
    // Downstream C still needs B's snapshot, including its upstream A dependency.
    assert!(
        !cleanup(&b_config, &b_cp.db_id)
            .preserved_snapshots
            .is_empty()
    );
    assert!(
        !cleanup(&a_config, &a_cp.db_id)
            .preserved_snapshots
            .is_empty()
    );

    let c = Db::resume_from_snapshot(
        config(&c_root, &[&a_root, &b_root], false),
        c_cp.snapshot_id,
        &c_cp.db_id,
    )
    .unwrap();
    c.expire_snapshot(c_cp.snapshot_id).unwrap();
    c.wait_for_import_adoption(Duration::from_secs(20)).unwrap();
    assert_eq!(
        c.get(4, b"key").unwrap().unwrap()[0]
            .as_ref()
            .unwrap()
            .as_ref(),
        &[b'x'; 4096]
    );
    c.close().unwrap();
    drop(c);

    // An I/O routing failure after deleting B's snapshots must preserve its START
    // and A's lease, so the same request can be retried with the correct routes.
    assert!(
        crate::cleanup_shard_snapshot(
            &b_config.volumes[0],
            &[],
            &b_cp.db_id,
            u64::MAX,
            SnapshotCleanupOptions {
                include_older_snapshots: true
            },
        )
        .is_err()
    );
    assert!(
        !cleanup(&a_config, &a_cp.db_id)
            .preserved_snapshots
            .is_empty()
    );
    assert!(
        cleanup(&b_config, &b_cp.db_id)
            .preserved_snapshots
            .is_empty()
    );
    let a_deleted = cleanup(&a_config, &a_cp.db_id);
    assert!(a_deleted.preserved_snapshots.is_empty());
    assert!(a_deleted.deleted_shard_snapshots > 0);
    assert_eq!(
        cleanup(&b_config, &b_cp.db_id),
        crate::SnapshotCleanupResult::default()
    );
}

#[test]
fn runtime_retains_upstream_until_last_historical_reference_expires() {
    let dir = tempfile::tempdir().unwrap();
    let a_root = dir.path().join("a");
    let b_root = dir.path().join("b");
    let a_config = config(&a_root, &[], false);
    let a = Db::open(a_config.clone(), vec![0..=7]).unwrap();
    a.put(4, b"key", 0, vec![b'x'; 4096]).unwrap();
    let source = snapshot(&a);
    a.close().unwrap();
    drop(a);
    let b = Db::open_new_with_manifest_path(
        config(&b_root, &[&a_root], true),
        &source.manifest_path,
        Some(vec![4..=4]),
        ImportStorageMode::AdoptAsync,
    )
    .unwrap();
    let historical = snapshot(&b);
    b.close().unwrap();
    drop(b);
    let b = Db::resume_from_snapshot(
        config(&b_root, &[&a_root], false),
        historical.snapshot_id,
        &historical.db_id,
    )
    .unwrap();
    // Finish copying but keep the historical recovery point referencing A.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let imports = std::fs::read_dir(b_root.join(&historical.db_id).join("imports"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        if imports.iter().any(|name| name.ends_with(".END")) {
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        b.wait_for_import_adoption(Duration::from_millis(20))
            .is_err()
    );
    assert!(
        !cleanup(&a_config, &source.db_id)
            .preserved_snapshots
            .is_empty()
    );
    b.expire_snapshot(historical.snapshot_id).unwrap();
    b.wait_for_import_adoption(Duration::from_secs(20)).unwrap();
    assert!(
        cleanup(&a_config, &source.db_id)
            .preserved_snapshots
            .is_empty()
    );
    assert!(b.get(4, b"key").unwrap().is_some());
    b.close().unwrap();
}

#[test]
fn global_cleanup_keeps_shard_inventory_until_upstream_release_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let a_root = dir.path().join("a");
    let b_root = dir.path().join("b");
    let a_config = config(&a_root, &[], false);
    let a = Db::open(a_config.clone(), vec![0..=7]).unwrap();
    a.put(4, b"key", 0, vec![b'x'; 4096]).unwrap();
    let source = snapshot(&a);
    a.close().unwrap();
    drop(a);
    let b_config = config(&b_root, &[&a_root], true);
    let b = Db::open_new_with_manifest_path(
        b_config.clone(),
        &source.manifest_path,
        None,
        ImportStorageMode::AdoptAsync,
    )
    .unwrap();
    snapshot(&b);
    let coordinator =
        crate::DbCoordinator::open(crate::CoordinatorConfig::from_config(&b_config)).unwrap();
    for entry in std::fs::read_dir(b_root.join(b.id()).join("snapshot")).unwrap() {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        let Some(id) = name
            .strip_prefix("SNAPSHOT-")
            .and_then(|id| id.parse::<u64>().ok())
        else {
            continue;
        };
        let global = coordinator
            .take_global_snapshot_with_id(8, vec![b.shard_snapshot_metadata(id).unwrap()], id)
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();
    }
    drop(coordinator);
    b.close().unwrap();
    drop(b);
    let options = SnapshotCleanupOptions {
        include_older_snapshots: true,
    };
    assert!(crate::cleanup_global_snapshot(&b_config.volumes[0], &[], u64::MAX, options).is_err());
    assert!(
        std::fs::read_dir(b_root.join("snapshot"))
            .unwrap()
            .any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("SNAPSHOT-")
            })
    );
    assert!(
        !cleanup(&a_config, &source.db_id)
            .preserved_snapshots
            .is_empty()
    );
    let result = crate::cleanup_global_snapshot(
        &b_config.volumes[0],
        &b_config.volumes[1..],
        u64::MAX,
        options,
    )
    .unwrap();
    assert!(result.preserved_snapshots.is_empty());
    assert!(result.deleted_global_snapshots > 0);
    assert!(
        cleanup(&a_config, &source.db_id)
            .preserved_snapshots
            .is_empty()
    );
}

#[test]
fn collector_retries_source_release_and_start_removal_without_losing_metadata() {
    use crate::rescale_protocol::ImportEnd;
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    struct Metadata {
        files: RefCell<BTreeMap<String, Vec<u8>>>,
        source_present: RefCell<bool>,
        fail: RefCell<Option<String>>,
    }
    impl LeaseMetadata for Metadata {
        fn import_names(&self) -> Result<Vec<String>> {
            Ok(self
                .files
                .borrow()
                .keys()
                .map(|key| key.strip_prefix("imports/").unwrap().to_string())
                .collect())
        }
        fn read(&self, name: &str) -> Result<Option<Vec<u8>>> {
            Ok(self.files.borrow().get(name).cloned())
        }
        fn remove(&self, name: &str) -> Result<()> {
            if self.fail.borrow().as_deref() == Some(name) {
                return Err(Error::IoError("injected delete failure".into()));
            }
            self.files.borrow_mut().remove(name);
            Ok(())
        }
        fn release_source(&self, _: &ImportStart) -> Result<()> {
            if self.fail.borrow().as_deref() == Some("source") {
                return Err(Error::IoError("injected release failure".into()));
            }
            *self.source_present.borrow_mut() = false;
            Ok(())
        }
    }
    let start = import_start_name("ab");
    let end = import_end_name("ab");
    let metadata = Metadata {
        files: RefCell::new(BTreeMap::from([(
            start.clone(),
            serde_json::to_vec(&ImportStart {
                version: 1,
                export_id: "ab".into(),
                source_db_id: "a".into(),
                source_manifest_path: "file:///a/snapshot/SNAPSHOT-0".into(),
                snapshot_id: 0,
                target_db_id: "b".into(),
                ranges: vec![0..=0],
                import_snapshot_id: None,
            })
            .unwrap(),
        )])),
        source_present: RefCell::new(true),
        fail: RefCell::new(None),
    };
    reclaim_import_leases(&metadata, "b", &BTreeSet::new(), LeaseCleanupMode::Online).unwrap();
    assert!(
        *metadata.source_present.borrow(),
        "current runtime needs its completion boundary"
    );
    metadata.files.borrow_mut().insert(
        end.clone(),
        serde_json::to_vec(&ImportEnd {
            version: 1,
            export_id: "ab".into(),
            snapshot_id: 1,
        })
        .unwrap(),
    );
    reclaim_import_leases(
        &metadata,
        "b",
        &BTreeSet::from(["ab".into()]),
        LeaseCleanupMode::Online,
    )
    .unwrap();
    assert!(
        *metadata.source_present.borrow(),
        "historical snapshots still reference A"
    );
    *metadata.fail.borrow_mut() = Some("source".into());
    assert!(
        reclaim_import_leases(&metadata, "b", &BTreeSet::new(), LeaseCleanupMode::Online).is_err()
    );
    assert!(metadata.read(&start).unwrap().is_some());
    *metadata.fail.borrow_mut() = Some(start.clone());
    assert!(
        reclaim_import_leases(&metadata, "b", &BTreeSet::new(), LeaseCleanupMode::Online).is_err()
    );
    assert!(!*metadata.source_present.borrow());
    assert!(metadata.read(&start).unwrap().is_some());
    *metadata.fail.borrow_mut() = Some(end.clone());
    assert!(
        reclaim_import_leases(&metadata, "b", &BTreeSet::new(), LeaseCleanupMode::Online).is_err()
    );
    assert!(metadata.read(&start).unwrap().is_none());
    assert!(metadata.read(&end).unwrap().is_some());
    *metadata.fail.borrow_mut() = None;
    reclaim_import_leases(&metadata, "b", &BTreeSet::new(), LeaseCleanupMode::Online).unwrap();
    reclaim_import_leases(&metadata, "b", &BTreeSet::new(), LeaseCleanupMode::Online).unwrap();
    assert!(metadata.files.borrow().is_empty());
}
