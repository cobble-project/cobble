use super::*;
use crate::file::atomic_replace::tests::TestFileSystem;
use crate::file::{File, SequentialWriteFile};
use crate::tool::snapshot_cleanup::tests::{
    cleanup_config, cleanup_resolved, create_materialized_snapshot, write_cleanup_manifest,
};
use crate::{Config, CoordinatorConfig, Db, DbBuilder, DbCoordinator};
use std::sync::atomic::Ordering;

struct Fixture {
    _root: tempfile::TempDir,
    config: Config,
    storage: Storage,
    fs: Arc<TestFileSystem>,
    ids: [u64; 4],
}

impl Fixture {
    fn new(cutoff: u64) -> Self {
        let root = tempfile::tempdir().unwrap();
        let config = cleanup_config(root.path());
        let db = DbBuilder::new(config.clone())
            .db_id("primary")
            .bucket_ranges(std::iter::once(0..=0).collect())
            .open()
            .unwrap();
        let mut refs = Vec::new();
        for key in [b"a", b"b", b"c"] {
            let id = create_materialized_snapshot(&db, key, &[42; 128]);
            refs.push(db.shard_snapshot_metadata(id).unwrap());
        }
        db.close().unwrap();
        drop(db);
        let other = DbBuilder::new(config.clone())
            .db_id("other")
            .bucket_ranges(std::iter::once(0..=0).collect())
            .open()
            .unwrap();
        let other_id = create_materialized_snapshot(&other, b"survivor", b"independent");
        let mut independent = cleanup_resolved(&config, "other", other_id);
        independent.id = refs[2].snapshot_id + 1;
        let mut survivor = refs[0].clone();
        survivor.snapshot_id = independent.id;
        survivor.manifest_path = append_storage_path(
            &normalize_storage_path_to_url(root.path().to_str().unwrap()).unwrap(),
            &format!("primary/snapshot/SNAPSHOT-{}", independent.id),
        );
        write_cleanup_manifest(
            root.path(),
            "primary",
            independent.id,
            &ManifestPayload::Snapshot(independent),
        );
        let ids = [
            refs[0].snapshot_id,
            refs[1].snapshot_id,
            refs[2].snapshot_id,
            survivor.snapshot_id,
        ];
        refs.push(survivor);
        other.close().unwrap();
        drop(other);
        let coordinator = DbCoordinator::open(CoordinatorConfig::from_config(&config)).unwrap();
        for (id, shard) in [10, 20, 30, 40].into_iter().zip(refs) {
            let global = coordinator
                .take_global_snapshot_with_id(1, vec![shard], id)
                .unwrap();
            coordinator.materialize_global_snapshot(&global).unwrap();
        }
        drop(coordinator);
        let real = FileSystemRegistry::new()
            .get_or_register_volume(&config.volumes[0])
            .unwrap();
        let fs = Arc::new(TestFileSystem::new(real));
        // Keep immutable CURRENT commits for marker/payload failure coverage.
        atomic_replace::write(
            fs.clone(),
            "snapshot/CURRENT",
            &crate::file::metadata_io::encode_metadata_payload_for_test(
                format!("SNAPSHOT-{cutoff}").as_bytes(),
            ),
        )
        .unwrap();
        atomic_replace::write(
            fs.clone(),
            "primary/snapshot/CURRENT",
            &crate::file::metadata_io::encode_metadata_payload_for_test(
                format!("SNAPSHOT-{}", ids[2]).as_bytes(),
            ),
        )
        .unwrap();
        let mut orphan = fs
            .open_write("snapshot/CURRENT-ATOMIC/00000000000000000000.data")
            .unwrap();
        orphan.write(b"abandoned").unwrap();
        orphan.close().unwrap();
        fs.delete_trace.lock().unwrap().clear();
        let base = normalize_storage_path_to_url(root.path().to_str().unwrap()).unwrap();
        let storage = Storage {
            routes: vec![Route {
                base,
                fs: fs.clone(),
            }],
        };
        Self {
            _root: root,
            config,
            storage,
            fs,
            ids,
        }
    }

    fn run(&self, cutoff: u64) -> Result<SnapshotCleanupResult> {
        let shard = Shard::load(
            &self.storage,
            "primary".into(),
            FileKey::new(0, "primary/snapshot"),
        )?;
        let mut globals = BTreeMap::new();
        let mut selected_globals = BTreeSet::new();
        let mut selected_shards = BTreeSet::new();
        for id in self.storage.list_ids(&FileKey::new(0, "snapshot"))? {
            let bytes = self
                .storage
                .read(&FileKey::new(0, global_snapshot_manifest_path(id)))?;
            let global: GlobalSnapshotManifest = serde_json::from_slice(&bytes).unwrap();
            let references = global
                .shard_snapshots
                .iter()
                .map(|s| (0, s.snapshot_id))
                .collect::<Vec<_>>();
            if id <= cutoff {
                selected_globals.insert(id);
                selected_shards.extend(&references);
            }
            globals.insert(id, references);
        }
        CleanupScope {
            shards: vec![shard],
            selected: selected_shards,
            globals: Some(globals),
            selected_globals,
        }
        .plan(
            &self.storage,
            SnapshotCleanupOptions {
                preserve_dependencies: true,
                include_older_snapshots: true,
            },
        )?
        .execute(&self.storage)
    }

    fn assert_survivors(&self, cutoff: u64) {
        let restored = Db::open_from_snapshot(self.config.clone(), self.ids[3], "primary").unwrap();
        assert_eq!(
            restored.get(0, b"survivor").unwrap().unwrap()[0].as_deref(),
            Some(b"independent".as_slice())
        );
        restored.close().unwrap();
        if cutoff == 10 {
            let restored =
                Db::open_from_snapshot(self.config.clone(), self.ids[2], "primary").unwrap();
            for key in [b"a", b"b", b"c"] {
                assert_eq!(
                    restored.get(0, key).unwrap().unwrap()[0].as_deref(),
                    Some([42; 128].as_slice())
                );
            }
            restored.close().unwrap();
        }
    }
}

#[test]
fn cleanup_retries_every_delete_boundary_without_breaking_protected_survivors() {
    for cutoff in [10, 30] {
        let baseline = Fixture::new(cutoff);
        baseline.run(cutoff).unwrap();
        baseline.assert_survivors(cutoff);
        let trace = baseline.fs.delete_trace.lock().unwrap().clone();
        eprintln!(
            "[snapshot-cleanup-faults] cutoff={cutoff}: {} delete boundaries, before/after failure + retry + survivor restore",
            trace.len()
        );
        assert!(
            trace
                .iter()
                .any(|path| path.starts_with("snapshot/SNAPSHOT-"))
        );
        assert!(trace.iter().any(|path| path.contains(".complete-")));
        assert!(trace.iter().any(|path| path.ends_with(".data")));
        if cutoff == 30 {
            assert!(trace.iter().any(|path| path.contains("/data/")));
            assert!(
                trace
                    .iter()
                    .any(|path| path.starts_with("primary/snapshot/SNAPSHOT-"))
            );
        }
        for fail_delete_after in [false, true] {
            for (fail_at, failed_path) in trace.iter().enumerate() {
                let fixture = Fixture::new(cutoff);
                fixture.fs.fail_delete_at.store(fail_at, Ordering::SeqCst);
                fixture
                    .fs
                    .fail_delete_after
                    .store(fail_delete_after, Ordering::SeqCst);
                assert!(
                    fixture.run(cutoff).is_err(),
                    "delete {fail_at} did not fail"
                );
                fixture.assert_survivors(cutoff);
                fixture.run(cutoff).unwrap_or_else(|error| {
                    panic!(
                        "retry failed at delete {fail_at} ({}): {error}",
                        failed_path
                    )
                });
                fixture.assert_survivors(cutoff);
                assert_eq!(fixture.run(cutoff).unwrap().deleted_files, 0);
                assert!(!atomic_replace::exists(fixture.fs.as_ref(), "snapshot/CURRENT").unwrap());
                assert!(!fixture.fs.exists("snapshot/CURRENT-ATOMIC/").unwrap());
            }
        }
    }
}

#[test]
fn cleanup_preflight_rejects_broken_cycles_and_checksum_without_mutation() {
    for kind in ["missing-base", "cycle", "checksum"] {
        let fixture = Fixture::new(30);
        let key = FileKey::new(0, format!("primary/snapshot/SNAPSHOT-{}", fixture.ids[1]));
        let mut payload = decode_manifest(&fixture.storage.read(&key).unwrap()).unwrap();
        if kind == "checksum" {
            std::fs::write(fixture._root.path().join(&key.path), b"corrupt").unwrap();
        } else {
            let ManifestPayload::IncrementalSnapshot(ref mut m) = payload else {
                panic!("expected incremental")
            };
            m.base_snapshot_id = if kind == "cycle" {
                fixture.ids[2]
            } else {
                9999
            };
            write_cleanup_manifest(fixture._root.path(), "primary", fixture.ids[1], &payload);
        }
        assert!(fixture.run(30).is_err());
        assert!(fixture.fs.delete_trace.lock().unwrap().is_empty());
        fixture.assert_survivors(30);
    }
}

#[test]
fn cleanup_empty_missing_target_and_cutoffs_do_not_select_unrelated_snapshots() {
    let root = tempfile::tempdir().unwrap();
    let config = cleanup_config(root.path());
    for include_older_snapshots in [false, true] {
        let options = SnapshotCleanupOptions {
            include_older_snapshots,
            ..Default::default()
        };
        assert_eq!(
            cleanup_shard_snapshot(&config.volumes[0], &[], "absent", 123, options).unwrap(),
            SnapshotCleanupResult::default()
        );
        assert_eq!(
            cleanup_global_snapshot(&config.volumes[0], &[], 123, options).unwrap(),
            SnapshotCleanupResult::default()
        );
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
    let fixture = Fixture::new(40);
    assert_eq!(fixture.run(5).unwrap(), SnapshotCleanupResult::default());
    assert!(fixture.fs.delete_trace.lock().unwrap().is_empty());
    let result = fixture.run(20).unwrap();
    assert_eq!(result.deleted_global_snapshots, 2);
    assert_eq!(result.deleted_shard_snapshots, 0);
    assert_eq!(result.preserved_snapshots.len(), 2);
    assert!(fixture.fs.exists("snapshot/SNAPSHOT-30").unwrap());
    assert!(fixture.fs.exists("snapshot/SNAPSHOT-40").unwrap());
    fixture.assert_survivors(10);
    assert_eq!(
        cleanup_shard_snapshot(
            &fixture.config.volumes[0],
            &[],
            "primary",
            9999,
            SnapshotCleanupOptions::default()
        )
        .unwrap(),
        SnapshotCleanupResult::default()
    );

    // An interrupted final cleanup can leave CURRENT without any global manifests.
    let real = FileSystemRegistry::new()
        .get_or_register_volume(&config.volumes[0])
        .unwrap();
    let fs = Arc::new(TestFileSystem::new(real));
    fs.create_dir("snapshot").unwrap();
    atomic_replace::write(
        fs.clone(),
        "snapshot/CURRENT",
        &crate::file::metadata_io::encode_metadata_payload_for_test(b"SNAPSHOT-123"),
    )
    .unwrap();
    let storage = Storage {
        routes: vec![Route {
            base: normalize_storage_path_to_url(root.path().to_str().unwrap()).unwrap(),
            fs: fs.clone(),
        }],
    };
    let scope = CleanupScope {
        shards: Vec::new(),
        selected: BTreeSet::new(),
        globals: Some(BTreeMap::new()),
        selected_globals: BTreeSet::new(),
    };
    scope
        .plan(&storage, SnapshotCleanupOptions::default())
        .unwrap()
        .execute(&storage)
        .unwrap();
    assert!(!fs.exists("snapshot/CURRENT-ATOMIC/").unwrap());
    fs.create_dir("snapshot/CURRENT-ATOMIC").unwrap();
    let mut orphan = fs
        .open_write("snapshot/CURRENT-ATOMIC/00000000000000000001.data")
        .unwrap();
    orphan.write(b"interrupted deletion").unwrap();
    orphan.close().unwrap();
    scope
        .plan(&storage, SnapshotCleanupOptions::default())
        .unwrap()
        .execute(&storage)
        .unwrap();
    assert!(!fs.exists("snapshot/CURRENT-ATOMIC/").unwrap());
}

#[test]
fn cleanup_preserves_manifest_locations_referenced_through_another_route() {
    let mut fixture = Fixture::new(40);
    // Keep route and shard records distinct: they are separate references to one
    // physical snapshot, not a reason to merge filesystem configuration or state.
    fixture.storage.routes.push(Route {
        base: fixture.storage.routes[0].base.clone(),
        fs: fixture.fs.clone(),
    });
    let id = fixture.ids[2];
    let shards = [0, 1]
        .into_iter()
        .map(|volume| {
            Shard::load(
                &fixture.storage,
                "primary".into(),
                FileKey::new(volume, "primary/snapshot"),
            )
            .unwrap()
        })
        .collect();
    let result = CleanupScope {
        shards,
        selected: BTreeSet::from([(0, id)]),
        globals: Some(BTreeMap::from([(30, vec![(0, id)]), (40, vec![(1, id)])])),
        selected_globals: BTreeSet::from([30]),
    }
    .plan(&fixture.storage, SnapshotCleanupOptions::default())
    .unwrap()
    .execute(&fixture.storage)
    .unwrap();
    assert_eq!(result.deleted_files + result.deleted_shard_snapshots, 0);
    assert_eq!(result.deleted_global_snapshots, 1);
    assert_eq!(result.preserved_snapshots.len(), 1);
    assert!(atomic_replace::exists(fixture.fs.as_ref(), "snapshot/CURRENT").unwrap());
    let restored = Db::open_from_snapshot(fixture.config.clone(), id, "primary").unwrap();
    for key in [b"a", b"b", b"c"] {
        assert_eq!(
            restored.get(0, key).unwrap().unwrap()[0].as_deref(),
            Some([42; 128].as_slice())
        );
    }
    restored.close().unwrap();
}
