use super::*;
use crate::{Config, DbBuilder, ImportStorageMode, VolumeDescriptor, VolumeUsageKind};
use size::Size;
use std::path::Path;
use std::time::Duration;

struct ImportFaultRegistry {
    root: std::path::PathBuf,
    fail_prefix: String,
}

impl crate::file::ProcessFileSystemRegistry for ImportFaultRegistry {
    fn try_init(
        &self,
        request: &crate::file::ProcessFileSystemRequest,
    ) -> Result<Option<Arc<dyn crate::file::FileSystem>>> {
        let location = url::Url::parse(&request.base_dir)?;
        if location.scheme() != "import-fault" {
            return Ok(None);
        }
        let local = self.root.join(location.path().trim_start_matches('/'));
        let inner =
            crate::file::FileSystemRegistry::new().get_or_register(local.to_str().unwrap())?;
        let fs = Arc::new(crate::file::atomic_replace::tests::TestFileSystem::new(
            inner,
        ));
        fs.atomic.store(true, Ordering::SeqCst);
        *fs.fail_rename_containing.lock().unwrap() = Some(self.fail_prefix.clone());
        Ok(Some(fs))
    }
}

struct ImportFaultRegistryGuard;
impl Drop for ImportFaultRegistryGuard {
    fn drop(&mut self) {
        crate::file::clear_process_custom_file_system_registry();
    }
}

#[derive(Default)]
struct ImportGovernance(std::sync::Mutex<Vec<(String, Vec<RangeInclusive<u16>>)>>);
impl crate::DbGovernance for ImportGovernance {
    fn register_db(&self, id: &str, ranges: &[RangeInclusive<u16>], _: u32) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .push((id.to_string(), ranges.to_vec()));
        Ok(())
    }
    fn unregister_db(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

fn config(root: &Path) -> Config {
    Config {
        total_buckets: 8,
        num_columns: 1,
        volumes: VolumeDescriptor::single_volume(root.to_str().unwrap()),
        snapshot_retention: None,
        snapshot_disable_incremental_base_link: true,
        snapshot_on_flush: false,
        active_memtable_incremental_snapshot_ratio: 0.0,
        l0_file_limit: 100,
        value_separation_threshold: Some(Size::from_const(32)),
        ..Config::default()
    }
}

fn target_config(root: &Path, sources: &[&Path], blocked: bool) -> Config {
    let mut result = config(root);
    if blocked {
        // No imported SST/VLOG can fit. The public API still installs external references,
        // but asynchronous adoption cannot make progress until a subsequent reopen adds space.
        result.volumes[0].size_limit = Some(Size::from_const(1));
    }
    result.volumes.extend(sources.iter().map(|source| {
        VolumeDescriptor::new(source.to_str().unwrap(), vec![VolumeUsageKind::Readonly])
    }));
    result
}

fn stop_tiering(db: &Db) {
    if let Some(worker) = &db.primary_tiering_worker {
        worker.stop();
        worker.join();
    }
}

#[test]
fn new_import_ranges_modes_identity_schema_active_and_vlog() {
    for active_ratio in [0.0, 1.0] {
        let root = tempfile::tempdir().unwrap();
        let source_root = root.path().join("source");
        let mut source_config = config(&source_root);
        source_config.active_memtable_incremental_snapshot_ratio = active_ratio;
        source_config.wal_enabled = true;
        source_config.volumes[0].set_usage(VolumeUsageKind::Wal);
        let source = Db::open(source_config, vec![0..=2, 4..=7]).unwrap();
        let mut schema = source.update_schema();
        schema.add_column(1, None, None, None).unwrap();
        schema.commit();
        let value = vec![b'v'; 4096];
        for bucket in [0, 1, 2, 4, 5, 6, 7] {
            source.put(bucket, b"key", 0, &value).unwrap();
            source.put(bucket, b"key", 1, b"second").unwrap();
        }
        let snapshot = source.create_snapshot_and_wait("source boundary").unwrap();
        let source_id = source.id().to_string();
        let manifest = source
            .shard_snapshot_metadata(snapshot)
            .unwrap()
            .manifest_path;
        source.put(1, b"wal-tail", 0, b"must not import").unwrap();
        source.force_close();

        for mode in [
            ImportStorageMode::AdoptAsync,
            ImportStorageMode::ReferencePersistent,
            ImportStorageMode::ReferencePersistentWithCache,
        ] {
            for use_manifest in [false, true] {
                for ranges in [None, Some(vec![1..=2]), Some(vec![1..=2, 1..=1, 4..=5])] {
                    let target_root = if use_manifest {
                        root.path().join(uuid::Uuid::new_v4().to_string())
                    } else {
                        source_root.clone()
                    };
                    let sources = if use_manifest {
                        vec![source_root.as_path()]
                    } else {
                        vec![]
                    };
                    let mut target_config = target_config(&target_root, &sources, false);
                    target_config.wal_enabled = true;
                    target_config.volumes[0].set_usage(VolumeUsageKind::Wal);
                    let builder = DbBuilder::new(target_config.clone())
                        .db_id(&source_id)
                        .bucket_ranges(vec![7..=7]);
                    let target = if use_manifest {
                        builder.open_new_with_manifest_path(&manifest, ranges.clone(), mode)
                    } else {
                        builder.open_new_with_snapshot(&source_id, snapshot, ranges.clone(), mode)
                    }
                    .unwrap();
                    assert_ne!(target.id(), source_id);
                    let expected =
                        normalize_bucket_ranges(ranges.unwrap_or_else(|| vec![0..=2, 4..=7]));
                    assert_eq!(target.db_state.load().bucket_ranges, expected);
                    assert_eq!(target.current_schema().version(), 1);
                    if mode != ImportStorageMode::AdoptAsync {
                        assert!(load_import_starts(&target.file_manager).unwrap().is_empty());
                        if active_ratio == 0.0 {
                            let state = target.db_state.load();
                            let files = state.multi_lsm_version.tree_versions_cloned();
                            assert!(
                                files
                                    .iter()
                                    .flat_map(|tree| &tree.levels)
                                    .flat_map(|level| &level.files)
                                    .all(|file| {
                                        matches!(
                                            target
                                                .file_manager
                                                .durable_data_file_path_with_origin(file.file_id),
                                            Some((_, ReplicaOrigin::ExternalPersistent { .. }))
                                        )
                                    })
                            );
                        }
                    }
                    for bucket in [0, 1, 2, 4, 5, 6, 7] {
                        if expected.iter().any(|range| range.contains(&bucket)) {
                            let row = target.get(bucket, b"key").unwrap().unwrap();
                            assert_eq!(row[0].as_deref(), Some(value.as_slice()));
                            assert_eq!(row[1].as_deref(), Some(b"second".as_slice()));
                            assert!(target.get(bucket, b"wal-tail").unwrap().is_none());
                        } else {
                            assert!(matches!(target.get(bucket, b"key"), Ok(None) | Err(_)));
                        }
                    }
                    let target_id = target.id().to_string();
                    target.close().unwrap();
                    drop(target);
                    let reopened = Db::resume(target_config, &target_id).unwrap();
                    assert_eq!(reopened.current_schema().version(), 1);
                    assert!(reopened.get(1, b"key").unwrap().is_some());
                    reopened.close().unwrap();
                }
            }
        }
    }
}

#[test]
fn new_import_rejects_empty_invalid_and_uncovered_ranges() {
    let root = tempfile::tempdir().unwrap();
    let options = config(root.path());
    let source = Db::open(options.clone(), vec![0..=2, 4..=7]).unwrap();
    let snapshot = source.create_snapshot_and_wait("ranges").unwrap();
    for ranges in [
        vec![],
        vec![3..=3],
        vec![2..=4],
        vec![8..=8],
        vec![RangeInclusive::new(7, 4)],
    ] {
        assert!(
            Db::open_new_with_snapshot(
                options.clone(),
                source.id(),
                snapshot,
                Some(ranges),
                ImportStorageMode::AdoptAsync
            )
            .is_err()
        );
    }
    assert!(
        source
            .file_manager
            .list_metadata_names("exports")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn new_import_active_segments_may_have_no_rows_in_requested_ranges() {
    let root = tempfile::tempdir().unwrap();
    let mut options = config(root.path());
    options.active_memtable_incremental_snapshot_ratio = 1.0;
    let source = Db::open(options.clone(), vec![0..=7]).unwrap();
    source.put(4, b"outside", 0, b"not imported").unwrap();
    let snapshot = source
        .create_snapshot_and_wait("active outside range")
        .unwrap();
    let imported = Db::open_new_with_snapshot(
        options,
        source.id(),
        snapshot,
        Some(vec![0..=1]),
        ImportStorageMode::AdoptAsync,
    )
    .unwrap();
    assert!(imported.get(0, b"outside").unwrap().is_none());
    assert!(matches!(imported.get(4, b"outside"), Ok(None) | Err(_)));
    imported.close().unwrap();
    source.close().unwrap();
}

#[test]
fn new_import_registers_injected_governance_with_new_identity_and_requested_ranges() {
    let root = tempfile::tempdir().unwrap();
    let options = config(root.path());
    let source = Db::open(options.clone(), vec![0..=7]).unwrap();
    let snapshot = source.create_snapshot_and_wait("governance").unwrap();
    let governance = Arc::new(ImportGovernance::default());
    let imported = DbBuilder::new(options)
        .db_id(source.id())
        .bucket_ranges(vec![0..=7])
        .governance(governance.clone())
        .open_new_with_snapshot(
            source.id(),
            snapshot,
            Some(vec![4..=5]),
            ImportStorageMode::ReferencePersistent,
        )
        .unwrap();
    assert_eq!(
        *governance.0.lock().unwrap(),
        vec![(imported.id().to_string(), vec![4..=5])]
    );
    imported.close().unwrap();
    source.close().unwrap();
}

#[test]
#[serial_test::serial(process_file_system_registry)]
fn new_import_faults_before_and_after_start_preserve_source_and_recovery_protection() {
    for after_start in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let source_root = root.path().join("source");
        let source = Db::open(config(&source_root), vec![0..=7]).unwrap();
        source.put(4, b"key", 0, vec![b'x'; 4096]).unwrap();
        let snapshot = source.create_snapshot_and_wait("fault source").unwrap();
        let source_manifest = source
            .shard_snapshot_metadata(snapshot)
            .unwrap()
            .manifest_path;
        let source_id = source.id().to_string();
        source.close().unwrap();
        crate::file::register_process_custom_file_system_registry(Arc::new(ImportFaultRegistry {
            root: root.path().to_path_buf(),
            fail_prefix: if after_start {
                "snapshot/SNAPSHOT-"
            } else {
                "imports/IMPORT-"
            }
            .into(),
        }));
        let _registry = ImportFaultRegistryGuard;
        let mut options = target_config(&root.path().join("target"), &[&source_root], true);
        options.volumes[0].base_dir = "import-fault:///target".into();
        options.runtime_manifest_mode = crate::RuntimeManifestMode::Enabled;
        assert!(
            Db::open_new_with_manifest_path(
                options,
                source_manifest,
                None,
                ImportStorageMode::AdoptAsync
            )
            .is_err()
        );
        let leases = source.file_manager.list_metadata_names("exports").unwrap();
        assert_eq!(leases.len(), usize::from(after_start));
        assert!(
            source_root
                .join(&source_id)
                .join("snapshot")
                .join(format!("SNAPSHOT-{snapshot}"))
                .exists()
        );
        if after_start {
            let lease_path = source_root
                .join(&source_id)
                .join("exports")
                .join(&leases[0]);
            let lease = ExportLease::decode(&std::fs::read(lease_path).unwrap()).unwrap();
            let target_dir = root.path().join("target").join(lease.target_db_id);
            assert!(
                target_dir
                    .join(import_start_name(&lease.export_id))
                    .exists()
            );
            assert!(!target_dir.join("snapshot/SNAPSHOT-0").exists());
            assert!(!target_dir.join("runtime/CURRENT").exists());
            // Even an uncertain/failed publication keeps its real persistent retention root.
            let cleanup = crate::cleanup_shard_snapshot(
                &config(&source_root).volumes[0],
                &[],
                &source_id,
                snapshot,
                crate::SnapshotCleanupOptions::default(),
            )
            .unwrap();
            assert!(!cleanup.preserved_snapshots.is_empty());
        }
    }
}

#[test]
fn new_import_failed_active_replay_does_not_publish_or_remove_source() {
    let root = tempfile::tempdir().unwrap();
    let source_root = root.path().join("source");
    let target_root = root.path().join("target");
    let mut options = config(&source_root);
    options.active_memtable_incremental_snapshot_ratio = 1.0;
    let source = Db::open(options, vec![0..=7]).unwrap();
    source.put(4, b"key", 0, b"value").unwrap();
    let snapshot = source.create_snapshot_and_wait("active source").unwrap();
    let manifest =
        crate::snapshot::load_manifest_for_snapshot(&source.file_manager, snapshot).unwrap();
    let active = manifest.active_memtable_data.first().unwrap();
    // Inject a valid metadata envelope with an invalid active-segment payload. Failure happens
    // after lease acquisition/topology install, exercising rollback rather than preflight only.
    let mut writer = source
        .file_manager
        .create_metadata_file(&active.path)
        .unwrap();
    writer.write(b"broken active segment").unwrap();
    writer.close().unwrap();
    let source_manifest = source
        .shard_snapshot_metadata(snapshot)
        .unwrap()
        .manifest_path;
    let source_id = source.id().to_string();
    source.close().unwrap();
    let mut target_options = target_config(&target_root, &[&source_root], false);
    target_options.runtime_manifest_mode = crate::RuntimeManifestMode::Enabled;
    assert!(
        Db::open_new_with_manifest_path(
            target_options,
            source_manifest,
            None,
            ImportStorageMode::AdoptAsync
        )
        .is_err()
    );
    assert!(
        source
            .file_manager
            .list_metadata_names("exports")
            .unwrap()
            .is_empty()
    );
    assert!(
        source_root
            .join(source_id)
            .join("snapshot")
            .join(format!("SNAPSHOT-{snapshot}"))
            .exists()
    );
    for entry in std::fs::read_dir(&target_root).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            assert!(!entry.path().join("snapshot/SNAPSHOT-0").exists());
            assert!(!entry.path().join("runtime/CURRENT").exists());
        }
    }
}

#[test]
fn new_import_disk_restart_cleanup_and_transitive_snapshot_protection() {
    for snapshot_after in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let a_root = root.path().join("a");
        let b_root = root.path().join("b");
        let c_root = root.path().join("c");
        let a_config = config(&a_root);
        let a = Db::open(a_config.clone(), vec![0..=7]).unwrap();
        a.put(4, b"key", 0, vec![b'x'; 4096]).unwrap();
        let a_snapshot = a.create_snapshot_and_wait("durable A").unwrap();
        let a_id = a.id().to_string();
        let a_manifest = a.shard_snapshot_metadata(a_snapshot).unwrap().manifest_path;
        a.close().unwrap();
        drop(a);

        let blocked_b = target_config(&b_root, &[&a_root], true);
        let b = if snapshot_after {
            Db::open_new_with_manifest_path(
                blocked_b.clone(),
                a_manifest,
                Some(vec![4..=5]),
                ImportStorageMode::AdoptAsync,
            )
            .unwrap()
        } else {
            let b = Db::open(blocked_b.clone(), vec![0..=3]).unwrap();
            b.expand_bucket_from_manifest_with_options(
                &a_id,
                a_manifest,
                Some(vec![4..=5]),
                ImportStorageMode::AdoptAsync,
                RescaleOptions {
                    snapshot_before: false,
                    snapshot_after,
                },
            )
            .unwrap();
            b
        };
        stop_tiering(&b);
        let starts = load_import_starts(&b.file_manager).unwrap();
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].import_snapshot_id.is_some(), snapshot_after);
        assert_eq!(
            list_snapshot_manifest_ids(&b.file_manager).unwrap().len(),
            usize::from(snapshot_after)
        );
        let b_snapshot = b.create_snapshot_and_wait("retained dependent B").unwrap();
        // This snapshot has no transitive lease or explicit retention flag. Adoption must not
        // mistake a later user snapshot for the import's absent immediate snapshot.
        let b_manual_snapshot = b.create_snapshot_and_wait("manual dependent B").unwrap();
        let b_id = b.id().to_string();
        let b_manifest = b.shard_snapshot_metadata(b_snapshot).unwrap().manifest_path;
        b.close().unwrap();
        drop(b);

        let blocked_c = target_config(&c_root, &[&a_root, &b_root], true);
        let c = Db::open_new_with_manifest_path(
            blocked_c.clone(),
            b_manifest,
            None,
            ImportStorageMode::AdoptAsync,
        )
        .unwrap();
        stop_tiering(&c);
        let c_id = c.id().to_string();
        c.close().unwrap();
        drop(c);

        let cleanup = || {
            crate::cleanup_shard_snapshot(
                &a_config.volumes[0],
                &[],
                &a_id,
                a_snapshot,
                crate::SnapshotCleanupOptions::default(),
            )
            .unwrap()
        };
        assert!(!cleanup().preserved_snapshots.is_empty());
        let b_options = target_config(&b_root, &[&a_root], false);
        let b = Db::resume(b_options.clone(), &b_id).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            b.adoption_coordinator.tick().unwrap();
            let starts = load_import_starts(&b.file_manager).unwrap();
            if starts.iter().all(|record| {
                load_import_end(&b.file_manager, &record.export_id)
                    .unwrap()
                    .is_some()
            }) {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !b.expire_snapshot(b_snapshot).unwrap(),
            "C's real lease retains B's dependent snapshot"
        );
        assert!(b.shard_snapshot_metadata(b_manual_snapshot).is_ok());
        assert!(
            b.wait_for_import_adoption(Duration::from_millis(30))
                .is_err()
        );
        b.close().unwrap();
        drop(b);
        assert!(!cleanup().preserved_snapshots.is_empty());

        let c_options = target_config(&c_root, &[&a_root, &b_root], false);
        let c = Db::resume(c_options.clone(), &c_id).unwrap();
        c.wait_for_import_adoption(Duration::from_secs(20)).unwrap();
        assert!(c.get(4, b"key").unwrap().is_some());
        c.close().unwrap();
        drop(c);
        let b = Db::resume(b_options.clone(), &b_id).unwrap();
        assert!(b.expire_snapshot(b_snapshot).unwrap());
        assert!(b.expire_snapshot(b_manual_snapshot).unwrap());
        b.wait_for_import_adoption(Duration::from_secs(20)).unwrap();
        b.close().unwrap();
        drop(b);
        assert!(cleanup().preserved_snapshots.is_empty());
        for (options, id) in [(b_options, b_id), (c_options, c_id)] {
            let db = Db::resume(options, id).unwrap();
            assert!(db.get(4, b"key").unwrap().is_some());
            db.put(4, b"after-cleanup", 0, b"owned").unwrap();
            db.create_snapshot_and_wait("after source cleanup").unwrap();
            db.close().unwrap();
        }
    }
}
