use super::{
    SnapshotCleanupOptions, cleanup_global_snapshot, cleanup_shard_snapshot, prune_shard_snapshot,
};
use crate::snapshot::{list_snapshot_manifest_ids, load_manifest_for_snapshot};
use crate::{Config, Db, GovernanceMode, MetricsManager, Result, VolumeDescriptor, WriteOptions};
use std::collections::HashSet;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;
use uuid::Uuid;

fn test_config(root: &str) -> Config {
    Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        total_buckets: 1,
        memtable_capacity: size::Size::from_kib(16),
        l0_file_limit: 4,
        snapshot_retention: None,
        snapshot_only_track: true,
        snapshot_disable_incremental_base_link: true,
        governance_mode: GovernanceMode::Filesystem,
        ..Config::default()
    }
}

fn open_single_bucket_db(config: Config) -> Db {
    Db::open(
        config,
        std::iter::once(0u16..=0u16).collect::<Vec<RangeInclusive<u16>>>(),
    )
    .expect("open db")
}

pub(super) fn create_materialized_snapshot(db: &Db, key: &[u8], value: &[u8]) -> u64 {
    db.put_with_options(0, key, 0, value, &WriteOptions::default())
        .expect("put value");
    let (tx, rx) = mpsc::channel();
    let snapshot_id = db
        .snapshot_with_callback(move |result| {
            tx.send(result).expect("send snapshot callback result");
        })
        .expect("start snapshot");
    let callback_result = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("receive snapshot callback result");
    callback_result.expect("snapshot callback should succeed");
    snapshot_id
}

fn referenced_data_paths(manifest: &crate::snapshot::ManifestSnapshot) -> HashSet<String> {
    let mut paths = HashSet::new();
    for levels in &manifest.tree_levels {
        for level in levels {
            for file in &level.files {
                paths.insert(file.path.clone());
            }
        }
    }
    for vlog in &manifest.vlog_files {
        paths.insert(vlog.path.clone());
    }
    paths
}

fn path_exists(file_manager: &Arc<crate::file::FileManager>, path: &str) -> bool {
    if let Some(local_path) = path.strip_prefix("file://") {
        return std::path::Path::new(local_path).exists();
    }
    if file_manager
        .meta_volume
        .fs()
        .exists(path)
        .expect("check path existence on meta volume")
    {
        return true;
    }
    file_manager.data_volumes.iter().any(|volume| {
        volume
            .fs()
            .exists(path)
            .expect("check path existence on data volume")
    })
}

fn wait_until_local_file_gone(path: &str, timeout: Duration) -> bool {
    let Some(local_path) = path.strip_prefix("file://") else {
        return !path_exists_local(path);
    };
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if !std::path::Path::new(local_path).exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    !std::path::Path::new(local_path).exists()
}

fn path_exists_local(path: &str) -> bool {
    std::path::Path::new(path).exists()
}

fn list_snapshot_ids(config: &Config, db_id: &str) -> Result<Vec<u64>> {
    let normalized = config.normalize_volume_paths()?;
    let metrics_manager = Arc::new(MetricsManager::new(db_id));
    let file_manager = Arc::new(crate::file::FileManager::from_config(
        &normalized,
        db_id,
        metrics_manager,
    )?);
    list_snapshot_manifest_ids(&file_manager)
}

#[test]
fn test_prune_shard_snapshot_removes_snapshot_manifest() {
    let root = format!("/tmp/prune_shard_snapshot_{}", Uuid::new_v4());
    let config = test_config(&root);
    let db = open_single_bucket_db(config.clone());
    let db_id = db.id().to_string();

    let s0 = create_materialized_snapshot(&db, b"k0", &vec![b'a'; 64 * 1024]);
    let s1 = create_materialized_snapshot(&db, b"k1", &vec![b'b'; 64 * 1024]);
    let s2 = create_materialized_snapshot(&db, b"k2", &vec![b'c'; 64 * 1024]);
    db.close().expect("close db");

    let normalized = config.normalize_volume_paths().expect("normalize config");
    let metrics_manager = Arc::new(MetricsManager::new(&db_id));
    let file_manager = Arc::new(
        crate::file::FileManager::from_config(&normalized, &db_id, metrics_manager)
            .expect("create file manager"),
    );

    let s0_manifest = load_manifest_for_snapshot(&file_manager, s0).expect("load snapshot s0");
    let s1_manifest = load_manifest_for_snapshot(&file_manager, s1).expect("load snapshot s1");
    let s2_manifest = load_manifest_for_snapshot(&file_manager, s2).expect("load snapshot s2");
    let target_paths = referenced_data_paths(&s2_manifest);
    let other_paths: HashSet<String> = referenced_data_paths(&s0_manifest)
        .into_iter()
        .chain(referenced_data_paths(&s1_manifest))
        .collect();
    let unique_target_paths: Vec<String> = target_paths.difference(&other_paths).cloned().collect();
    let shared_target_paths: Vec<String> =
        target_paths.intersection(&other_paths).cloned().collect();
    assert!(
        !unique_target_paths.is_empty(),
        "expected pruned snapshot to contain uniquely referenced data files"
    );
    assert!(
        !shared_target_paths.is_empty(),
        "expected pruned snapshot to share some data files with surviving snapshots"
    );

    assert!(prune_shard_snapshot(config.clone(), db_id.clone(), s2).expect("prune snapshot"));

    let snapshot_ids = list_snapshot_ids(&config, &db_id).expect("list snapshot ids");
    assert_eq!(snapshot_ids, vec![s0, s1]);
    assert!(
        !path_exists(
            &file_manager,
            &file_manager.metadata_path(&crate::paths::snapshot_manifest_relative_path(s2))
        ),
        "pruned snapshot manifest should be deleted"
    );
    for path in unique_target_paths {
        assert!(
            wait_until_local_file_gone(&path, Duration::from_secs(2)),
            "unique data file should be deleted when pruning snapshot: {}",
            path
        );
    }
    for path in shared_target_paths {
        assert!(
            path_exists(&file_manager, &path),
            "shared data file should remain after pruning snapshot: {}",
            path
        );
    }
    let _ = load_manifest_for_snapshot(&file_manager, s0).expect("remaining snapshot s0 exists");
    let _ = load_manifest_for_snapshot(&file_manager, s1).expect("remaining snapshot s1 exists");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_prune_shard_snapshot_returns_false_for_missing_snapshot() {
    let root = format!("/tmp/prune_shard_snapshot_missing_{}", Uuid::new_v4());
    let config = test_config(&root);
    let db = open_single_bucket_db(config.clone());
    let db_id = db.id().to_string();
    let _ = create_materialized_snapshot(&db, b"k0", b"v0");
    db.close().expect("close db");

    assert!(!prune_shard_snapshot(config, db_id, 9_999).expect("prune missing snapshot"));

    let _ = std::fs::remove_dir_all(root);
}

pub(super) fn cleanup_config(root: &std::path::Path) -> Config {
    Config {
        volumes: VolumeDescriptor::single_volume(root.to_str().unwrap()),
        total_buckets: 1,
        num_columns: 1,
        memtable_capacity: size::Size::from_mib(4),
        l0_file_limit: 100,
        snapshot_retention: None,
        snapshot_on_flush: false,
        snapshot_only_track: true,
        value_separation_threshold: Some(size::Size::from_const(16)),
        ..Config::default()
    }
}

pub(super) fn cleanup_resolved(
    config: &Config,
    db_id: &str,
    id: u64,
) -> crate::snapshot::ManifestSnapshot {
    let normalized = config.normalize_volume_paths().unwrap();
    let manager = Arc::new(
        crate::file::FileManager::from_config(
            &normalized,
            db_id,
            Arc::new(MetricsManager::new(db_id)),
        )
        .unwrap(),
    );
    load_manifest_for_snapshot(&manager, id).unwrap()
}

fn cleanup_manifest(
    root: &std::path::Path,
    db_id: &str,
    id: u64,
) -> crate::snapshot::manifest::ManifestPayload {
    let path = root.join(db_id).join(format!("snapshot/SNAPSHOT-{id}"));
    let bytes = crate::file::metadata_io::read_metadata_payload_from_path_for_test(path).unwrap();
    crate::snapshot::manifest::decode_manifest(&bytes).unwrap()
}

pub(super) fn write_cleanup_manifest(
    root: &std::path::Path,
    db_id: &str,
    id: u64,
    manifest: &crate::snapshot::manifest::ManifestPayload,
) {
    let path = root.join(db_id).join(format!("snapshot/SNAPSHOT-{id}"));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        path,
        crate::file::metadata_io::encode_metadata_payload_for_test(
            &serde_json::to_vec(manifest).unwrap(),
        ),
    )
    .unwrap();
}

fn cleanup_pointer(root: &std::path::Path, db_id: &str, id: u64) -> std::path::PathBuf {
    let path = root.join(db_id).join("snapshot/CURRENT");
    std::fs::write(
        &path,
        crate::file::metadata_io::encode_metadata_payload_for_test(
            format!("SNAPSHOT-{id}").as_bytes(),
        ),
    )
    .unwrap();
    path
}

#[test]
fn cleanup_shard_option_matrix_preserves_incremental_chains_and_shared_files() {
    use crate::snapshot::manifest::{
        ManifestPayload, load_manifest_chain, load_manifest_chain_from_path, load_manifest_entry,
    };
    for preserve_dependencies in [true, false] {
        for include_older_snapshots in [true, false] {
            // The full-survivor case shares SST/VLOG/schema files without depending on
            // selected manifests. The incremental case also needs their complete chain.
            for incremental_survivor in [true, false] {
                let root = tempfile::tempdir().unwrap();
                let config = cleanup_config(root.path());
                let db = open_single_bucket_db(config.clone());
                let db_id = db.id().to_string();
                let value = vec![42; 128];
                let a = create_materialized_snapshot(&db, b"a", &value);
                let mut schema = db.update_schema();
                schema.add_column(1, None, None, None).unwrap();
                schema.commit();
                let b = create_materialized_snapshot(&db, b"b", &value);
                let c = create_materialized_snapshot(&db, b"c", &value);
                assert!(
                    matches!(cleanup_manifest(root.path(), &db_id, b), ManifestPayload::IncrementalSnapshot(m) if m.base_snapshot_id == a)
                );
                assert!(
                    matches!(cleanup_manifest(root.path(), &db_id, c), ManifestPayload::IncrementalSnapshot(m) if m.base_snapshot_id == b)
                );
                let resolved = cleanup_resolved(&config, &db_id, c);
                // All loading paths must reconstruct the same complete metadata, not just files.
                let manager = Arc::new(
                    crate::file::FileManager::from_config(
                        &config.normalize_volume_paths().unwrap(),
                        &db_id,
                        Arc::new(MetricsManager::new(&db_id)),
                    )
                    .unwrap(),
                );
                let by_id = load_manifest_chain(&manager, c).unwrap();
                let by_path = load_manifest_chain_from_path(
                    &manager,
                    &db.shard_snapshot_metadata(c).unwrap().manifest_path,
                )
                .unwrap();
                let storage = super::Storage::new(&config.volumes[0], &[]).unwrap();
                let shard = super::Shard::load(
                    &storage,
                    db_id.clone(),
                    super::FileKey::new(0, format!("{db_id}/snapshot")),
                )
                .unwrap();
                let mut loaded = std::collections::HashMap::new();
                assert_eq!(by_id.len(), by_path.len());
                for (entry, path_entry) in by_id.iter().zip(&by_path) {
                    let cached = load_manifest_entry(&manager, entry.snapshot_id, &loaded).unwrap();
                    let cleaned = shard.resolved(entry.snapshot_id).unwrap().unwrap();
                    let expected = serde_json::to_value(&entry.manifest).unwrap();
                    for actual in [&path_entry.manifest, &cached.manifest, &cleaned] {
                        assert_eq!(serde_json::to_value(actual).unwrap(), expected);
                    }
                    if let ManifestPayload::IncrementalSnapshot(delta) =
                        cleanup_manifest(root.path(), &db_id, entry.snapshot_id)
                    {
                        assert_eq!(entry.manifest.data_size_bytes, delta.data_size_bytes);
                        assert_eq!(
                            entry.manifest.incremental_data_size_bytes,
                            delta.incremental_data_size_bytes
                        );
                    }
                    loaded.insert(entry.snapshot_id, cached);
                }
                let paths = referenced_data_paths(&resolved);
                assert!(!resolved.vlog_files.is_empty());
                db.close().unwrap();
                drop(db);
                if !incremental_survivor {
                    write_cleanup_manifest(
                        root.path(),
                        &db_id,
                        c,
                        &ManifestPayload::Snapshot(resolved),
                    );
                }
                let pointer = cleanup_pointer(root.path(), &db_id, c);
                let options = SnapshotCleanupOptions {
                    preserve_dependencies,
                    include_older_snapshots,
                };
                let result =
                    cleanup_shard_snapshot(&config.volumes[0], &[], &db_id, b, options).unwrap();
                let retained_chain = preserve_dependencies && incremental_survivor;
                assert_eq!(
                    result.preserved_snapshots.len(),
                    if retained_chain {
                        if include_older_snapshots { 2 } else { 1 }
                    } else {
                        0
                    }
                );
                assert_eq!(
                    result.deleted_shard_snapshots,
                    if retained_chain {
                        0
                    } else if include_older_snapshots {
                        2
                    } else {
                        1
                    }
                );
                assert_eq!(pointer.exists(), preserve_dependencies);
                for path in &paths {
                    if preserve_dependencies {
                        assert!(
                            url::Url::parse(path)
                                .unwrap()
                                .to_file_path()
                                .unwrap()
                                .exists()
                        );
                    }
                }
                if preserve_dependencies {
                    let restored = Db::open_from_snapshot(config.clone(), c, &db_id).unwrap();
                    for key in [b"a", b"b", b"c"] {
                        assert_eq!(
                            restored.get(0, key).unwrap().unwrap()[0].as_deref(),
                            Some(value.as_slice())
                        );
                    }
                    restored.close().unwrap();
                } else {
                    assert!(Db::open_from_snapshot(config.clone(), c, &db_id).is_err());
                }
                let retry =
                    cleanup_shard_snapshot(&config.volumes[0], &[], &db_id, b, options).unwrap();
                assert_eq!(retry.deleted_files + retry.deleted_shard_snapshots, 0);
            }
        }
    }
}

#[test]
fn cleanup_global_protects_other_shards_and_never_deletes_external_origins() {
    use crate::file::logical_file::ReplicaOrigin;
    use crate::snapshot::manifest::{ManifestPayload, ManifestVlogFile};
    use crate::{CoordinatorConfig, DbCoordinator};
    for preserve_dependencies in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let config = cleanup_config(root.path());
        let db = open_single_bucket_db(config.clone());
        let id = create_materialized_snapshot(&db, b"key", &[42; 128]);
        let db_id = db.id().to_string();
        let first = db.shard_snapshot_metadata(id).unwrap();
        let mut manifest = cleanup_resolved(&config, &db_id, id);
        db.close().unwrap();
        drop(db);
        let owned = &mut manifest.tree_levels[0][0].files[0];
        let original = url::Url::parse(&owned.path)
            .unwrap()
            .to_file_path()
            .unwrap();
        let job_path = root
            .path()
            .join(&db_id)
            .join("compaction/jobs/job/data/owned.sst");
        std::fs::create_dir_all(job_path.parent().unwrap()).unwrap();
        std::fs::rename(original, &job_path).unwrap();
        owned.path = url::Url::from_file_path(&job_path).unwrap().to_string();
        let persistent = root
            .path()
            .join(&db_id)
            .join("data/external-persistent.vlog");
        let leased = root.path().join(&db_id).join("data/external-leased.vlog");
        std::fs::write(&persistent, b"external").unwrap();
        std::fs::write(&leased, b"external").unwrap();
        for (index, (path, origin)) in [
            (
                &persistent,
                ReplicaOrigin::ExternalPersistent {
                    source_id: "source".into(),
                },
            ),
            (
                &leased,
                ReplicaOrigin::ExternalLeased {
                    export_id: "export".into(),
                },
            ),
        ]
        .into_iter()
        .enumerate()
        {
            manifest.vlog_files.push(ManifestVlogFile {
                file_seq: 99 + index as u32,
                file_id: 999 + index as u64,
                path: url::Url::from_file_path(path).unwrap().to_string(),
                valid_entries: 1,
                origin,
            });
        }
        // A second shard references the first shard's physical files, but not its manifest.
        let mut second_manifest = manifest.clone();
        second_manifest.id = 42;
        for levels in &mut second_manifest.tree_levels {
            for level in levels {
                for file in &mut level.files {
                    file.origin = ReplicaOrigin::ExternalPersistent {
                        source_id: db_id.clone(),
                    };
                }
            }
        }
        write_cleanup_manifest(
            root.path(),
            &db_id,
            id,
            &ManifestPayload::Snapshot(manifest.clone()),
        );
        write_cleanup_manifest(
            root.path(),
            "other",
            42,
            &ManifestPayload::Snapshot(second_manifest),
        );
        let mut second = first.clone();
        second.db_id = "other".into();
        second.snapshot_id = 42;
        second.manifest_path =
            url::Url::from_file_path(root.path().join("other/snapshot/SNAPSHOT-42"))
                .unwrap()
                .to_string();
        let coordinator = DbCoordinator::open(CoordinatorConfig::from_config(&config)).unwrap();
        coordinator
            .materialize_global_snapshot(
                &coordinator
                    .take_global_snapshot_with_id(1, vec![first], 10)
                    .unwrap(),
            )
            .unwrap();
        coordinator
            .materialize_global_snapshot(
                &coordinator
                    .take_global_snapshot_with_id(1, vec![second], 20)
                    .unwrap(),
            )
            .unwrap();
        let pointer = root.path().join("snapshot/CURRENT");
        let options = SnapshotCleanupOptions {
            preserve_dependencies,
            include_older_snapshots: false,
        };
        let selected_manifest = root
            .path()
            .join(&db_id)
            .join(format!("snapshot/SNAPSHOT-{id}"));
        std::fs::write(&selected_manifest, b"bad checksum").unwrap();
        assert!(cleanup_global_snapshot(&config.volumes[0], &[], 10, options).is_err());
        assert!(job_path.exists() && root.path().join("snapshot/SNAPSHOT-10").exists());
        let other_volume = tempfile::tempdir().unwrap();
        let missing_file = other_volume.path().join("data.sst");
        std::fs::copy(&job_path, &missing_file).unwrap();
        let mut missing_route = manifest.clone();
        missing_route.tree_levels[0][0].files[0].path =
            url::Url::from_file_path(&missing_file).unwrap().to_string();
        write_cleanup_manifest(
            root.path(),
            &db_id,
            id,
            &ManifestPayload::Snapshot(missing_route),
        );
        assert!(cleanup_global_snapshot(&config.volumes[0], &[], 10, options).is_err());
        assert!(
            missing_file.exists()
                && job_path.exists()
                && root.path().join("snapshot/SNAPSHOT-10").exists()
        );
        write_cleanup_manifest(
            root.path(),
            &db_id,
            id,
            &ManifestPayload::Snapshot(manifest),
        );
        let result = cleanup_global_snapshot(&config.volumes[0], &[], 10, options).unwrap();
        assert_eq!(result.deleted_global_snapshots, 1);
        assert_eq!(result.deleted_shard_snapshots, 1);
        assert_eq!(job_path.exists(), preserve_dependencies);
        assert!(persistent.exists() && leased.exists());
        assert_eq!(pointer.exists(), preserve_dependencies);
        // Force-cleanup retry tolerates its removed selected shard and never rereads data.
        assert_eq!(
            cleanup_global_snapshot(&config.volumes[0], &[], 10, options)
                .unwrap()
                .deleted_files,
            0
        );
    }
}

#[test]
fn cleanup_preserves_shared_relative_active_segments() {
    for preserve_dependencies in [true, false] {
        for include_older_snapshots in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let mut config = cleanup_config(root.path());
            config.active_memtable_incremental_snapshot_ratio = 1.0;
            let db = open_single_bucket_db(config.clone());
            let db_id = db.id().to_string();
            let _a = create_materialized_snapshot(&db, b"a", b"first");
            let b = create_materialized_snapshot(&db, b"b", b"second");
            let c = create_materialized_snapshot(&db, b"c", b"third");
            let before = cleanup_resolved(&config, &db_id, b);
            let surviving = cleanup_resolved(&config, &db_id, c);
            assert!(!before.active_memtable_data.is_empty());
            let shared = before
                .active_memtable_data
                .iter()
                .map(|segment| {
                    assert!(segment.path.starts_with("snapshot/SNAPSHOT-ACTIVE-DATA-"));
                    assert!(
                        surviving
                            .active_memtable_data
                            .iter()
                            .any(|other| other.path == segment.path)
                    );
                    root.path().join(&db_id).join(&segment.path)
                })
                .collect::<Vec<_>>();
            db.close().unwrap();
            drop(db);
            let options = SnapshotCleanupOptions {
                preserve_dependencies,
                include_older_snapshots,
            };
            let result =
                cleanup_shard_snapshot(&config.volumes[0], &[], &db_id, b, options).unwrap();
            assert!(result.deleted_shard_snapshots > 0);
            for path in shared {
                assert_eq!(path.exists(), preserve_dependencies);
            }
            if preserve_dependencies {
                let restored = Db::open_from_snapshot(config.clone(), c, &db_id).unwrap();
                for (key, value) in [
                    (b"a", b"first".as_slice()),
                    (b"b", b"second".as_slice()),
                    (b"c", b"third".as_slice()),
                ] {
                    assert_eq!(
                        restored.get(0, key).unwrap().unwrap()[0].as_deref(),
                        Some(value)
                    );
                }
                restored.close().unwrap();
            } else {
                assert!(Db::open_from_snapshot(config.clone(), c, &db_id).is_err());
            }
        }
    }
}
