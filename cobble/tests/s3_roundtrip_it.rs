#![cfg(feature = "storage-s3")]

use cobble::{Config, SingleDb, VolumeDescriptor, VolumeUsageKind};
use opendal::Operator;
use opendal::services;
use size::Size;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use url::Url;

const PROGRESS_LOG_INTERVAL: usize = 16 * 1024;

fn must_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing required env var: {}", name))
}

fn has_required_env() -> bool {
    [
        "COBBLE_S3_ENDPOINT",
        "COBBLE_S3_BUCKET",
        "COBBLE_S3_ACCESS_ID",
        "COBBLE_S3_SECRET_KEY",
    ]
    .iter()
    .all(|name| std::env::var(name).is_ok())
}

fn make_value(idx: usize, value_len: usize) -> Vec<u8> {
    let mut value = vec![0u8; value_len];
    for (pos, byte) in value.iter_mut().enumerate() {
        *byte = ((idx.wrapping_mul(31) + pos.wrapping_mul(17)) % 251) as u8;
    }
    value
}

fn log_stage(stage: &str, stage_start: Instant, total_start: Instant) {
    eprintln!(
        "[s3-roundtrip] stage={stage} stage_elapsed={:.3}s total_elapsed={:.3}s",
        stage_start.elapsed().as_secs_f64(),
        total_start.elapsed().as_secs_f64()
    );
}

fn log_progress(
    stage: &str,
    completed: usize,
    total: usize,
    stage_start: Instant,
    total_start: Instant,
) {
    if !completed.is_multiple_of(PROGRESS_LOG_INTERVAL) && completed != total {
        return;
    }
    eprintln!(
        "[s3-roundtrip] stage={stage} progress={completed}/{total} ({:.1}%) stage_elapsed={:.3}s total_elapsed={:.3}s",
        (completed as f64 / total as f64) * 100.0,
        stage_start.elapsed().as_secs_f64(),
        total_start.elapsed().as_secs_f64()
    );
}

fn s3_test_runtime() -> &'static tokio::runtime::Runtime {
    // OpenDAL shares its HTTP pool, so its connections must outlive individual tests.
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

fn list_wal_objects(
    endpoint: &str,
    bucket: &str,
    access_id: &str,
    secret_key: &str,
    root_prefix: &str,
    db_id: &str,
) -> Vec<String> {
    let builder = services::S3::default()
        .bucket(bucket)
        .root(root_prefix)
        .endpoint(endpoint)
        .region("us-east-1")
        .access_key_id(access_id)
        .secret_access_key(secret_key);
    // OpenDAL ≥ 0.58: `Operator::new` returns a finished `Operator` directly
    // (the previous `.finish()` builder terminator was removed, see opendal #7743).
    let operator = Operator::new(builder).unwrap();
    s3_test_runtime()
        .block_on(operator.list(&format!("{db_id}/wal/")))
        .unwrap()
        .into_iter()
        .filter(|entry| entry.name().starts_with("WAL-"))
        .map(|entry| entry.path().to_string())
        .collect()
}

struct SnapshotS3Prefix {
    runtime: &'static tokio::runtime::Runtime,
    operator: Operator,
    base_dir: Url,
    access_id: String,
    secret_key: String,
    endpoint: String,
    prefix: String,
}

impl SnapshotS3Prefix {
    fn new() -> Self {
        let endpoint = must_env("COBBLE_S3_ENDPOINT");
        let bucket = must_env("COBBLE_S3_BUCKET");
        let access_id = must_env("COBBLE_S3_ACCESS_ID");
        let secret_key = must_env("COBBLE_S3_SECRET_KEY");
        let endpoint_url = Url::parse(&endpoint).unwrap();
        let prefix = format!("cobble-snapshot-to-{}", uuid::Uuid::new_v4());
        let operator = Operator::new(
            services::S3::default()
                .bucket(&bucket)
                .root(&prefix)
                .endpoint(&endpoint)
                .region("us-east-1")
                .access_key_id(&access_id)
                .secret_access_key(&secret_key),
        )
        .unwrap();
        let base_dir = Url::parse(&format!(
            "s3://{}:{}/{}/{}/",
            endpoint_url.host_str().unwrap(),
            endpoint_url.port_or_known_default().unwrap(),
            bucket,
            prefix,
        ))
        .unwrap();
        eprintln!("[snapshot-to-s3] endpoint={endpoint} bucket={bucket} prefix={prefix}");
        Self {
            runtime: s3_test_runtime(),
            operator,
            base_dir,
            access_id,
            secret_key,
            endpoint,
            prefix,
        }
    }

    fn volume(&self, directory: &str) -> VolumeDescriptor {
        let mut url = self.base_dir.clone();
        url.set_path(&format!("{}{directory}", self.base_dir.path()));
        // Deliberately retain slashes in endpoint/root options: appending files must mutate only
        // the URL path, and sibling/schema lookup must not split the query's slash components.
        url.set_query(Some(&format!(
            "endpoint={}&root=/{}/objects/{directory}&region=us-east-1&disable_config_load=true&disable_ec2_metadata=true&enable_virtual_host_style=false",
            self.endpoint, self.prefix,
        )));
        let mut volume = VolumeDescriptor::new(
            url.to_string(),
            vec![
                VolumeUsageKind::Meta,
                VolumeUsageKind::PrimaryDataPriorityHigh,
                VolumeUsageKind::Snapshot,
            ],
        );
        volume.access_id = Some(self.access_id.clone());
        volume.secret_key = Some(self.secret_key.clone());
        volume
    }

    fn plain_volume(&self, directory: &str) -> VolumeDescriptor {
        let mut volume = self.volume(directory);
        let mut url = Url::parse(&volume.base_dir).unwrap();
        volume.custom_options = Some(url.query_pairs().into_owned().collect());
        url.set_query(None);
        volume.base_dir = url.to_string();
        volume
    }

    fn manifest(&self, metadata: &cobble::ShardSnapshotMetadata) -> serde_json::Value {
        let url = Url::parse(&metadata.manifest_path).unwrap();
        let relative = url.path().strip_prefix(self.base_dir.path()).unwrap();
        let bytes = self
            .runtime
            .block_on(self.operator.read(&format!("objects/{relative}")))
            .unwrap()
            .to_bytes();
        assert!(bytes.ends_with(b"mcs1"));
        serde_json::from_slice(&bytes[..bytes.len() - 8]).unwrap()
    }

    fn delete(&self, directory: &str) {
        let physical = if directory.is_empty() {
            String::new()
        } else {
            format!("objects/{directory}")
        };
        self.runtime
            .block_on(async { self.operator.delete_with(&physical).recursive(true).await })
            .unwrap();
    }
}

impl Drop for SnapshotS3Prefix {
    fn drop(&mut self) {
        self.delete("");
        eprintln!("[snapshot-to-s3] cleaned isolated test prefix");
    }
}

fn snapshot_config(volume: VolumeDescriptor, active_ratio: f64) -> Config {
    Config {
        volumes: vec![volume],
        total_buckets: 1,
        num_columns: 1,
        memtable_capacity: Size::from_mib(16),
        memtable_buffer_count: 2,
        l0_file_limit: 100,
        snapshot_on_flush: false,
        snapshot_retention: None,
        block_cache_size: Size::from_const(0),
        value_separation_threshold: Some(Size::from_const(16)),
        active_memtable_incremental_snapshot_ratio: active_ratio,
        ..Config::default()
    }
}

fn wait_snapshot(db: &cobble::Db) -> cobble::ShardSnapshotMetadata {
    let (sender, receiver) = mpsc::channel();
    db.snapshot_with_callback(move |result| sender.send(result).unwrap())
        .unwrap();
    receiver
        .recv_timeout(Duration::from_secs(60))
        .unwrap()
        .unwrap()
}

fn write_snapshot_stage(db: &cobble::Db, stage: usize) {
    for i in 0..128 {
        let index = stage * 128 + i;
        db.put(
            0,
            format!("key-{index:04}").as_bytes(),
            0,
            make_value(index, 256),
        )
        .unwrap();
    }
}

fn verify_snapshot_cut(config: Config, metadata: &cobble::ShardSnapshotMetadata, stages: usize) {
    let restored =
        cobble::Db::open_new_with_manifest_path(config, &metadata.manifest_path).unwrap();
    for index in 0..stages * 128 {
        assert_eq!(
            restored
                .get(0, format!("key-{index:04}").as_bytes())
                .unwrap()
                .unwrap()[0]
                .as_deref(),
            Some(make_value(index, 256).as_slice())
        );
    }
    assert!(
        restored
            .get(0, format!("key-{:04}", stages * 128).as_bytes())
            .unwrap()
            .is_none()
    );
    let rows = restored
        .scan(0, b"".as_slice()..b"zz".as_slice())
        .unwrap()
        .collect::<cobble::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows.len(), stages * 128);
    for (index, (key, columns)) in rows.iter().enumerate() {
        assert_eq!(key.as_ref(), format!("key-{index:04}").as_bytes());
        assert_eq!(
            columns[0].as_deref(),
            Some(make_value(index, 256).as_slice())
        );
    }
    restored.close().unwrap();
}

#[test]
fn s3_snapshot_cleanup_shard_global_and_exported_cases() {
    if !has_required_env() {
        eprintln!("skipping snapshot cleanup S3 test: COBBLE_S3_* env vars are not fully set");
        return;
    }
    use cobble::{
        CoordinatorConfig, Db, DbCoordinator, SnapshotCleanupOptions, cleanup_global_snapshot,
        cleanup_shard_snapshot,
    };

    let storage = SnapshotS3Prefix::new();
    let exists = |directory: &str, relative: &str| {
        storage
            .runtime
            .block_on(
                storage
                    .operator
                    .exists(&format!("objects/{directory}/{relative}")),
            )
            .unwrap()
    };
    for preserve_dependencies in [true, false] {
        for include_older_snapshots in [false, true] {
            let options = SnapshotCleanupOptions {
                preserve_dependencies,
                include_older_snapshots,
            };
            for global in [false, true] {
                let directory =
                    format!("cleanup-{global}-{preserve_dependencies}-{include_older_snapshots}");
                let shard_directory = format!("{directory}/shards");
                let volume = storage.volume(&shard_directory);
                let config = snapshot_config(volume.clone(), 0.0);
                let db = Db::open(config.clone(), std::iter::once(0..=0).collect()).unwrap();
                let mut snapshots = Vec::new();
                for stage in 0..3 {
                    write_snapshot_stage(&db, stage);
                    snapshots.push(wait_snapshot(&db));
                }
                let [a, b, c] = snapshots.as_slice() else {
                    unreachable!()
                };
                assert_eq!(storage.manifest(b)["base_snapshot_id"], a.snapshot_id);
                assert_eq!(storage.manifest(c)["base_snapshot_id"], b.snapshot_id);
                db.close().unwrap();

                if global {
                    let meta = storage.volume(&directory);
                    let coordinator = DbCoordinator::open(CoordinatorConfig {
                        volumes: vec![meta.clone()],
                        snapshot_retention: None,
                    })
                    .unwrap();
                    // Global ids deliberately differ from shard ids. G40 reuses B directly.
                    for (id, shard) in [(10, a), (20, b), (30, c), (40, b)] {
                        let snapshot = coordinator
                            .take_global_snapshot_with_id(1, vec![shard.clone()], id)
                            .unwrap();
                        coordinator.materialize_global_snapshot(&snapshot).unwrap();
                    }
                    drop(coordinator);
                    let result =
                        cleanup_global_snapshot(&meta, std::slice::from_ref(&volume), 20, options)
                            .unwrap();
                    assert_eq!(result.deleted_files == 0, preserve_dependencies);
                    assert_eq!(
                        result.deleted_global_snapshots,
                        if include_older_snapshots { 2 } else { 1 }
                    );
                    assert!(!exists(&directory, "snapshot/SNAPSHOT-20"));
                    assert_eq!(
                        exists(&directory, "snapshot/SNAPSHOT-10"),
                        !include_older_snapshots
                    );
                    let coordinator = DbCoordinator::open(CoordinatorConfig {
                        volumes: vec![meta.clone()],
                        snapshot_retention: None,
                    })
                    .unwrap();
                    assert_eq!(
                        coordinator
                            .load_current_global_snapshot()
                            .unwrap()
                            .map(|s| s.id),
                        preserve_dependencies.then_some(40)
                    );
                    drop(coordinator);
                    assert_eq!(
                        cleanup_global_snapshot(&meta, std::slice::from_ref(&volume), 20, options)
                            .unwrap()
                            .deleted_global_snapshots,
                        0
                    );
                } else {
                    let result =
                        cleanup_shard_snapshot(&volume, &[], &b.db_id, b.snapshot_id, options)
                            .unwrap();
                    assert_eq!(result.deleted_files == 0, preserve_dependencies);
                    assert_eq!(
                        result.deleted_shard_snapshots,
                        if preserve_dependencies {
                            0
                        } else if include_older_snapshots {
                            2
                        } else {
                            1
                        }
                    );
                    assert_eq!(
                        cleanup_shard_snapshot(&volume, &[], &b.db_id, b.snapshot_id, options)
                            .unwrap()
                            .deleted_shard_snapshots,
                        0
                    );
                }
                assert_eq!(
                    exists(
                        &shard_directory,
                        &format!("{}/snapshot/SNAPSHOT-{}", b.db_id, b.snapshot_id)
                    ),
                    preserve_dependencies
                );
                if preserve_dependencies {
                    verify_snapshot_cut(config, c, 3);
                } else {
                    assert!(Db::open_new_with_manifest_path(config, &c.manifest_path).is_err());
                }
            }

            // Independent shard exports have no base chain. Preserve a sibling export.
            let directory = format!("export-{preserve_dependencies}-{include_older_snapshots}");
            let source = storage.volume(&format!("{directory}/source"));
            let destination = storage.volume(&format!("{directory}/shards"));
            let db = Db::open(
                snapshot_config(source, 1.0),
                std::iter::once(0..=0).collect(),
            )
            .unwrap();
            write_snapshot_stage(&db, 0);
            let active = wait_snapshot(&db);
            let exported = db
                .export_snapshot(active.snapshot_id, destination.clone())
                .unwrap()
                .wait()
                .unwrap();
            assert!(
                !storage.manifest(&exported)["active_memtable_data"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            let sibling = db.snapshot_to(destination.clone()).unwrap().wait().unwrap();
            db.close().unwrap();
            let result = cleanup_shard_snapshot(
                &destination,
                &[],
                &exported.db_id,
                exported.snapshot_id,
                options,
            )
            .unwrap();
            assert_eq!(result.deleted_shard_snapshots, 1);
            assert!(result.deleted_files > 0);
            verify_snapshot_cut(snapshot_config(destination.clone(), 0.0), &sibling, 1);

            // Publish the remaining independent shard as a global export and delete its CURRENT.
            let coordinator = DbCoordinator::open(CoordinatorConfig {
                volumes: vec![storage.volume("export-coordinator")],
                snapshot_retention: None,
            })
            .unwrap();
            let snapshot = coordinator
                .take_exported_global_snapshot(1, vec![sibling.clone()])
                .unwrap();
            let meta = storage.volume(&directory);
            coordinator
                .materialize_exported_global_snapshot(&snapshot, meta.clone())
                .unwrap();
            drop(coordinator);
            let result =
                cleanup_global_snapshot(&meta, std::slice::from_ref(&destination), 0, options)
                    .unwrap();
            assert_eq!(result.deleted_global_snapshots, 1);
            assert_eq!(result.deleted_shard_snapshots, 1);
            assert!(result.deleted_files > 0);
            assert!(!exists(&directory, "snapshot/SNAPSHOT-0"));
            assert!(!exists(
                &directory,
                &format!(
                    "shards/{}/snapshot/SNAPSHOT-{}",
                    sibling.db_id, sibling.snapshot_id
                )
            ));
            assert_eq!(
                cleanup_global_snapshot(&meta, &[destination], 0, options)
                    .unwrap()
                    .deleted_files,
                0
            );
            eprintln!(
                "[snapshot-cleanup-s3] preserve={preserve_dependencies} older={include_older_snapshots}: shard/global/export passed"
            );
        }
    }
}

#[test]
fn s3_snapshot_cleanup_nested_routes_and_missing_objects() {
    if !has_required_env() {
        eprintln!("skipping snapshot cleanup S3 test: COBBLE_S3_* env vars are not fully set");
        return;
    }
    use cobble::{
        CoordinatorConfig, Db, DbCoordinator, SnapshotCleanupOptions, cleanup_global_snapshot,
        cleanup_shard_snapshot,
    };

    let storage = SnapshotS3Prefix::new();
    let source = snapshot_config(storage.volume("corner-source"), 0.0);
    let destination = storage.volume("corner-exports");
    let db = Db::open(source, std::iter::once(0..=0).collect()).unwrap();
    write_snapshot_stage(&db, 0);
    let snapshot = wait_snapshot(&db);
    let mut exports = Vec::new();
    for _ in 0..3 {
        exports.push(
            db.export_snapshot(snapshot.snapshot_id, destination.clone())
                .unwrap()
                .wait()
                .unwrap(),
        );
    }
    db.close().unwrap();
    for (index, exported) in exports.iter().enumerate() {
        let manifest = storage.manifest(exported);
        let files = manifest["tree_levels"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|levels| levels.as_array().unwrap())
            .flat_map(|level| level["files"].as_array().unwrap())
            .chain(manifest["vlog_files"].as_array().unwrap())
            .map(|file| {
                let url = Url::parse(file["path"].as_str().unwrap()).unwrap();
                format!(
                    "objects/{}",
                    url.path().strip_prefix(storage.base_dir.path()).unwrap()
                )
            })
            .collect::<Vec<_>>();
        assert!(files.len() >= 2, "cover both SST and VLOG");
        let sentinel = format!("objects/corner-exports/{}/keep-me", exported.db_id);
        storage
            .runtime
            .block_on(storage.operator.write(&sentinel, "unrelated"))
            .unwrap();
        let missing = cleanup_shard_snapshot(
            &destination,
            &[],
            &exported.db_id,
            exported.snapshot_id + 100,
            SnapshotCleanupOptions::default(),
        )
        .unwrap();
        assert_eq!(missing.deleted_files + missing.deleted_shard_snapshots, 0);
        let nested = storage.volume(&format!("corner-exports/{}/data", exported.db_id));
        if index == 2 {
            // Export paths omit URL options. A surviving full snapshot can refer to the same
            // objects through the parent volume's configured URL instead of the nested route.
            let mut survivor = manifest.clone();
            survivor["id"] = (exported.snapshot_id + 1).into();
            let query = Url::parse(&destination.base_dir)
                .unwrap()
                .query()
                .unwrap()
                .to_owned();
            for levels in survivor["tree_levels"].as_array_mut().unwrap() {
                for level in levels.as_array_mut().unwrap() {
                    for file in level["files"].as_array_mut().unwrap() {
                        let mut path = Url::parse(file["path"].as_str().unwrap()).unwrap();
                        path.set_query(Some(&query));
                        file["path"] = path.to_string().into();
                    }
                }
            }
            for file in survivor["vlog_files"].as_array_mut().unwrap() {
                let mut path = Url::parse(file["path"].as_str().unwrap()).unwrap();
                path.set_query(Some(&query));
                file["path"] = path.to_string().into();
            }
            let mut bytes = serde_json::to_vec(&survivor).unwrap();
            bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
            bytes.extend_from_slice(b"mcs1");
            let mut metadata = exported.clone();
            metadata.snapshot_id += 1;
            let mut path = Url::parse(&metadata.manifest_path).unwrap();
            path.set_path(&format!(
                "{}/SNAPSHOT-{}",
                path.path().rsplit_once('/').unwrap().0,
                metadata.snapshot_id
            ));
            metadata.manifest_path = path.to_string();
            let object = format!(
                "objects/{}",
                path.path().strip_prefix(storage.base_dir.path()).unwrap()
            );
            storage
                .runtime
                .block_on(storage.operator.write(&object, bytes))
                .unwrap();
            verify_snapshot_cut(snapshot_config(destination.clone(), 0.0), &metadata, 1);
            let result = cleanup_shard_snapshot(
                &destination,
                &[nested],
                &exported.db_id,
                exported.snapshot_id,
                SnapshotCleanupOptions::default(),
            )
            .unwrap();
            assert_eq!(result.deleted_shard_snapshots, 1);
            assert_eq!(
                result.deleted_files, 0,
                "shared objects reached through different configured roots must be protected"
            );
            verify_snapshot_cut(snapshot_config(destination.clone(), 0.0), &metadata, 1);
            // The same shard manifest can also be shared by globals using two routes.
            let meta = storage.volume("");
            let coordinator = DbCoordinator::open(CoordinatorConfig {
                volumes: vec![meta.clone()],
                snapshot_retention: None,
            })
            .unwrap();
            for id in [10, 20] {
                let mut reference = metadata.clone();
                if id == 20 {
                    let mut path = Url::parse(&reference.manifest_path).unwrap();
                    path.set_query(Url::parse(&meta.base_dir).unwrap().query());
                    reference.manifest_path = path.to_string();
                }
                let global = coordinator
                    .take_global_snapshot_with_id(1, vec![reference], id)
                    .unwrap();
                coordinator.materialize_global_snapshot(&global).unwrap();
            }
            drop(coordinator);
            let result = cleanup_global_snapshot(
                &meta,
                std::slice::from_ref(&destination),
                10,
                SnapshotCleanupOptions::default(),
            )
            .unwrap();
            assert_eq!(result.deleted_global_snapshots, 1);
            assert_eq!(
                result.deleted_shard_snapshots, 0,
                "a manifest shared through two routes must survive"
            );
            let coordinator = DbCoordinator::open(CoordinatorConfig {
                volumes: vec![meta],
                snapshot_retention: None,
            })
            .unwrap();
            assert_eq!(
                coordinator
                    .load_current_global_snapshot()
                    .unwrap()
                    .unwrap()
                    .id,
                20
            );
            verify_snapshot_cut(snapshot_config(destination.clone(), 0.0), &metadata, 1);
            continue;
        }
        // A missing data object must not stop cleanup of the remaining snapshot assets.
        storage
            .runtime
            .block_on(storage.operator.delete(&files[0]))
            .unwrap();
        // Both descriptors address the same object namespace, with different configured roots.
        let routes = if index == 0 { Vec::new() } else { vec![nested] };
        let result = cleanup_shard_snapshot(
            &destination,
            &routes,
            &exported.db_id,
            exported.snapshot_id,
            SnapshotCleanupOptions::default(),
        )
        .unwrap();
        assert_eq!(result.deleted_shard_snapshots, 1);
        for file in files {
            assert!(
                !storage
                    .runtime
                    .block_on(storage.operator.exists(&file))
                    .unwrap(),
                "orphan snapshot object: {file}"
            );
        }
        assert!(
            storage
                .runtime
                .block_on(storage.operator.exists(&sentinel))
                .unwrap()
        );
        assert_eq!(
            cleanup_shard_snapshot(
                &destination,
                &routes,
                &exported.db_id,
                exported.snapshot_id,
                SnapshotCleanupOptions::default(),
            )
            .unwrap()
            .deleted_files,
            0
        );
    }
}

#[test]
fn s3_snapshot_to_preserves_incremental_chains_and_active_merge_cuts() {
    if !has_required_env() {
        eprintln!("skipping snapshot_to S3 test: COBBLE_S3_* env vars are not fully set");
        return;
    }
    let storage = SnapshotS3Prefix::new();
    let source_config = snapshot_config(storage.volume("source"), 0.0);
    let export_volume = storage.volume("exports");
    let db = cobble::DbBuilder::new(source_config.clone())
        .db_id("sst-chain")
        .bucket_ranges(vec![0..=0])
        .open()
        .unwrap();
    write_snapshot_stage(&db, 0);
    let a = wait_snapshot(&db);
    write_snapshot_stage(&db, 1);
    let b = wait_snapshot(&db);
    write_snapshot_stage(&db, 2);
    let e = db
        .snapshot_to(export_volume.clone())
        .unwrap()
        .wait()
        .unwrap();
    let c = wait_snapshot(&db); // Only E's one new L0 file differs from B.
    write_snapshot_stage(&db, 3);
    let d = wait_snapshot(&db);
    for (name, snapshot, base) in [
        ("A", &a, None),
        ("B", &b, Some(a.snapshot_id)),
        ("E", &e, None),
        ("C", &c, Some(b.snapshot_id)),
        ("D", &d, Some(c.snapshot_id)),
    ] {
        let manifest = storage.manifest(snapshot);
        assert_eq!(
            manifest
                .get("base_snapshot_id")
                .and_then(|value| value.as_u64()),
            base,
            "{name} has wrong raw manifest base"
        );
        eprintln!(
            "[snapshot-to-s3] SST {name} id={} base={base:?}",
            snapshot.snapshot_id
        );
    }
    let exported = storage.manifest(&e);
    assert!(
        exported["active_memtable_data"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(!exported["vlog_files"].as_array().unwrap().is_empty());
    assert!(exported.get("tree_levels").is_some());
    for (snapshot, stages) in [(&a, 1), (&b, 2), (&c, 3), (&d, 4)] {
        verify_snapshot_cut(source_config.clone(), snapshot, stages);
    }
    verify_snapshot_cut(snapshot_config(storage.plain_volume("exports"), 0.0), &e, 3);
    assert!(db.expire_snapshot(a.snapshot_id).unwrap());
    assert!(db.expire_snapshot(b.snapshot_id).unwrap());
    verify_snapshot_cut(source_config.clone(), &c, 3);
    // The existing delta format permits one new L0 file per tree. Two are a valid full fallback.
    write_snapshot_stage(&db, 4);
    let e2 = db
        .snapshot_to(export_volume.clone())
        .unwrap()
        .wait()
        .unwrap();
    write_snapshot_stage(&db, 5);
    let fallback = wait_snapshot(&db);
    assert!(
        storage
            .manifest(&fallback)
            .get("base_snapshot_id")
            .is_none()
    );
    eprintln!(
        "[snapshot-to-s3] SST fallback id={} full (two new L0 files since D={})",
        fallback.snapshot_id, d.snapshot_id
    );
    verify_snapshot_cut(source_config, &fallback, 6);
    db.close().unwrap();
    storage.delete("source/");
    verify_snapshot_cut(snapshot_config(export_volume.clone(), 0.0), &e, 3);
    verify_snapshot_cut(snapshot_config(export_volume.clone(), 0.0), &e2, 5);

    let active_config = snapshot_config(storage.volume("active-source"), 1.0);
    let db = cobble::DbBuilder::new(active_config.clone())
        .db_id("active-chain")
        .bucket_ranges(vec![0..=0])
        .open()
        .unwrap();
    let mut schema = db.update_schema();
    schema
        .set_column_operator(None, 0, Arc::new(cobble::U64CounterMergeOperator))
        .unwrap();
    schema.commit();
    db.put(0, b"counter", 0, 100u64.to_le_bytes()).unwrap();
    // Seed an existing L0 so E later adds to B's level instead of introducing a new level,
    // which the ordinary delta format legitimately encodes as a full fallback.
    db.snapshot_to(export_volume.clone())
        .unwrap()
        .wait()
        .unwrap();
    db.merge(0, b"counter", 0, 1u64.to_le_bytes()).unwrap();
    let a = wait_snapshot(&db);
    db.merge(0, b"counter", 0, 2u64.to_le_bytes()).unwrap();
    let b = wait_snapshot(&db);
    assert!(
        !storage.manifest(&b)["active_memtable_data"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    db.merge(0, b"counter", 0, 4u64.to_le_bytes()).unwrap();
    let e = db
        .snapshot_to(export_volume.clone())
        .unwrap()
        .wait()
        .unwrap();
    let c = wait_snapshot(&db);
    assert_eq!(
        storage.manifest(&c)["base_snapshot_id"].as_u64(),
        Some(b.snapshot_id)
    );
    assert!(
        storage.manifest(&c)["active_memtable_data"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    db.merge(0, b"counter", 0, 8u64.to_le_bytes()).unwrap();
    let d = wait_snapshot(&db);
    assert!(
        !storage.manifest(&d)["active_memtable_data"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    for (name, snapshot, expected, config) in [
        ("A", &a, 101u64, active_config.clone()),
        ("B", &b, 103, active_config.clone()),
        ("E", &e, 107, snapshot_config(export_volume.clone(), 1.0)),
        ("C", &c, 107, active_config.clone()),
        ("D", &d, 115, active_config.clone()),
    ] {
        let restored =
            cobble::Db::open_new_with_manifest_path(config, &snapshot.manifest_path).unwrap();
        let value = restored.get(0, b"counter").unwrap().unwrap();
        assert_eq!(
            u64::from_le_bytes(value[0].as_deref().unwrap().try_into().unwrap()),
            expected,
            "active {name} replayed an overlapping segment"
        );
        let rows = restored
            .scan(0, b"".as_slice()..b"zz".as_slice())
            .unwrap()
            .collect::<cobble::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.as_ref(), b"counter");
        assert_eq!(
            rows[0].1[0].as_deref(),
            Some(expected.to_le_bytes().as_slice())
        );
        restored.close().unwrap();
        eprintln!(
            "[snapshot-to-s3] active {name} id={} counter={expected}",
            snapshot.snapshot_id
        );
    }
    assert!(
        storage.manifest(&e)["active_memtable_data"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    db.close().unwrap();
    storage.delete("active-source/");
    let restored = cobble::Db::open_new_with_manifest_path(
        snapshot_config(export_volume, 1.0),
        &e.manifest_path,
    )
    .unwrap();
    assert_eq!(
        restored.get(0, b"counter").unwrap().unwrap()[0].as_deref(),
        Some(107u64.to_le_bytes().as_slice())
    );
    restored.close().unwrap();
    eprintln!(
        "[snapshot-to-s3] all raw bases, point reads, scans, expiration and source deletion checks passed"
    );
}

#[test]
fn s3_wal_snapshot_checkpoint_and_truncate() {
    if !has_required_env() {
        eprintln!("skipping S3 WAL test: COBBLE_S3_* env vars are not fully set");
        return;
    }
    let endpoint = must_env("COBBLE_S3_ENDPOINT");
    let bucket = must_env("COBBLE_S3_BUCKET");
    let access_id = must_env("COBBLE_S3_ACCESS_ID");
    let secret_key = must_env("COBBLE_S3_SECRET_KEY");
    let endpoint_url = Url::parse(&endpoint).unwrap();
    let endpoint_host = endpoint_url.host_str().unwrap();
    let endpoint_port = endpoint_url.port_or_known_default().unwrap();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let root_prefix = format!("cobble-s3-wal-{unique}");
    let base_dir = format!(
        "s3://{}:{}/{}/{}?endpoint_scheme={}&region=us-east-1&disable_config_load=true&disable_ec2_metadata=true&enable_virtual_host_style=false",
        endpoint_host,
        endpoint_port,
        bucket,
        root_prefix,
        endpoint_url.scheme()
    );
    let mut data_volume = VolumeDescriptor::new(
        base_dir.clone(),
        vec![
            VolumeUsageKind::Meta,
            VolumeUsageKind::PrimaryDataPriorityHigh,
            VolumeUsageKind::Snapshot,
        ],
    );
    data_volume.access_id = Some(access_id.clone());
    data_volume.secret_key = Some(secret_key.clone());
    let mut wal_volume = VolumeDescriptor::new(base_dir, vec![VolumeUsageKind::Wal]);
    wal_volume.access_id = Some(access_id.clone());
    wal_volume.secret_key = Some(secret_key.clone());
    let config = Config {
        volumes: vec![data_volume, wal_volume],
        wal_enabled: true,
        wal_flush_interval_ms: 5,
        num_columns: 1,
        total_buckets: 1,
        ..Config::default()
    };
    let db_id = format!("wal-shard-{unique}");
    let db = cobble::DbBuilder::new(config.clone())
        .bucket_ranges(vec![0..=0])
        .db_id(&db_id)
        .open()
        .unwrap();
    db.put(0, b"before-snapshot", 0, b"value").unwrap();
    let before_snapshot = list_wal_objects(
        &endpoint,
        &bucket,
        &access_id,
        &secret_key,
        &root_prefix,
        &db_id,
    );
    assert_eq!(before_snapshot.len(), 1);
    let (tx, rx) = mpsc::channel();
    db.snapshot_with_callback(move |result| tx.send(result).unwrap())
        .unwrap();
    rx.recv().unwrap().unwrap();
    assert!(
        list_wal_objects(
            &endpoint,
            &bucket,
            &access_id,
            &secret_key,
            &root_prefix,
            &db_id,
        )
        .is_empty()
    );

    db.put(0, b"after-snapshot", 0, b"value").unwrap();
    db.close().unwrap();
    let after_snapshot = list_wal_objects(
        &endpoint,
        &bucket,
        &access_id,
        &secret_key,
        &root_prefix,
        &db_id,
    );
    assert_eq!(after_snapshot.len(), 1);

    let mut recovery_config = config;
    recovery_config.wal_enabled = false;
    recovery_config
        .volumes
        .retain(|volume| !volume.supports(VolumeUsageKind::Wal));
    let resumed = cobble::Db::resume_with_recovery_mode(
        recovery_config,
        &db_id,
        cobble::RecoveryMode::LatestWithWal,
    )
    .unwrap();
    assert!(resumed.get(0, b"before-snapshot").unwrap().is_some());
    assert!(resumed.get(0, b"after-snapshot").unwrap().is_some());
    resumed.put(0, b"after-resume", 0, b"value").unwrap();
    let (tx, rx) = mpsc::channel();
    resumed
        .snapshot_with_callback(move |result| tx.send(result).unwrap())
        .unwrap();
    rx.recv().unwrap().unwrap();
    resumed.close().unwrap();
    let after_resume = list_wal_objects(
        &endpoint,
        &bucket,
        &access_id,
        &secret_key,
        &root_prefix,
        &db_id,
    );
    assert!(after_resume.is_empty());
}

#[test]
fn s3_roundtrip_write_snapshot_resume() {
    if !has_required_env() {
        eprintln!("skipping s3 roundtrip test: COBBLE_S3_* env vars are not fully set");
        return;
    }
    let total_start = Instant::now();
    let endpoint = must_env("COBBLE_S3_ENDPOINT");
    let bucket = must_env("COBBLE_S3_BUCKET");
    let access_id = must_env("COBBLE_S3_ACCESS_ID");
    let secret_key = must_env("COBBLE_S3_SECRET_KEY");
    let endpoint_url = Url::parse(&endpoint).expect("COBBLE_S3_ENDPOINT must be a valid URL");
    let endpoint_host = endpoint_url
        .host_str()
        .expect("COBBLE_S3_ENDPOINT must include host");
    let endpoint_port = endpoint_url
        .port_or_known_default()
        .expect("COBBLE_S3_ENDPOINT must include explicit or default port");
    let endpoint_scheme = endpoint_url.scheme();

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic")
        .as_millis();
    let root_prefix = format!("cobble-s3-roundtrip-{}", unique);
    let base_dir = format!(
        "s3://{}:{}/{}/{}?endpoint_scheme={}&region=us-east-1&disable_config_load=true&disable_ec2_metadata=true&enable_virtual_host_style=false",
        endpoint_host, endpoint_port, bucket, root_prefix, endpoint_scheme
    );

    let mut volume = VolumeDescriptor::new(
        base_dir,
        vec![
            VolumeUsageKind::Meta,
            VolumeUsageKind::PrimaryDataPriorityHigh,
            VolumeUsageKind::Snapshot,
        ],
    );
    volume.access_id = Some(access_id);
    volume.secret_key = Some(secret_key);

    let config = Config {
        volumes: vec![volume],
        memtable_capacity: Size::from_mib(4),
        memtable_buffer_count: 2,
        num_columns: 1,
        total_buckets: 1,
        l0_file_limit: 4,
        l1_base_bytes: Size::from_mib(16),
        level_size_multiplier: 4,
        max_level: 6,
        base_file_size: Size::from_mib(4),
        block_cache_size: Size::from_mib(256),
        log_console: true,
        ..Config::default()
    };

    // 1 GiB total payload: 131_072 keys x 4 KiB values.
    let value_len = 4096usize;
    let target_bytes = 512 * 1024 * 1024usize;
    let records = target_bytes / value_len;
    eprintln!(
        "[s3-roundtrip] starting endpoint={} bucket={} root_prefix={} records={} value_len={} target_bytes={} block_cache_size={}",
        endpoint,
        bucket,
        root_prefix,
        records,
        value_len,
        target_bytes,
        config.block_cache_size.bytes()
    );

    let open_start = Instant::now();
    let db = SingleDb::open(config.clone()).expect("open single db on s3 should succeed");
    log_stage("open", open_start, total_start);

    let write_start = Instant::now();
    for idx in 0..records {
        let key = format!("bulk-key-{idx:08}");
        let value = make_value(idx, value_len);
        db.put(0, key.as_bytes(), 0, value)
            .expect("write should succeed");
        log_progress("write", idx + 1, records, write_start, total_start);
    }
    log_stage("write", write_start, total_start);

    let verify_before_snapshot_start = Instant::now();
    for idx in 0..records {
        let key = format!("bulk-key-{idx:08}");
        let expected = make_value(idx, value_len);
        let got = db
            .get(0, key.as_bytes())
            .expect("read should succeed")
            .expect("value should exist");
        let col = got[0].as_ref().expect("column 0 should exist");
        assert_eq!(col.as_ref(), expected.as_slice(), "mismatch at idx={idx}");
        log_progress(
            "verify-before-snapshot",
            idx + 1,
            records,
            verify_before_snapshot_start,
            total_start,
        );
    }
    log_stage(
        "verify-before-snapshot",
        verify_before_snapshot_start,
        total_start,
    );

    let (tx, rx) = mpsc::channel();
    let snapshot_start = Instant::now();
    let snapshot_id = db
        .snapshot_with_callback(move |result| {
            let _ = tx.send(result.map(|_| ()));
        })
        .expect("snapshot should be scheduled");
    let callback_result = rx
        .recv_timeout(Duration::from_secs(180))
        .expect("snapshot callback should complete");
    callback_result.expect("snapshot materialization should succeed");
    log_stage("snapshot", snapshot_start, total_start);

    let close_before_resume_start = Instant::now();
    db.close().expect("close db should succeed");
    log_stage(
        "close-before-resume",
        close_before_resume_start,
        total_start,
    );

    let resume_start = Instant::now();
    let resumed = SingleDb::resume(config, snapshot_id).expect("resume should succeed");
    log_stage("resume", resume_start, total_start);

    let verify_after_resume_start = Instant::now();
    for idx in 0..records {
        let key = format!("bulk-key-{idx:08}");
        let expected = make_value(idx, value_len);
        let got = resumed
            .get(0, key.as_bytes())
            .expect("read after resume should succeed")
            .expect("value should exist after resume");
        let col = got[0].as_ref().expect("column 0 should exist after resume");
        assert_eq!(
            col.as_ref(),
            expected.as_slice(),
            "mismatch after resume at idx={idx}"
        );
        log_progress(
            "verify-after-resume",
            idx + 1,
            records,
            verify_after_resume_start,
            total_start,
        );
    }
    log_stage(
        "verify-after-resume",
        verify_after_resume_start,
        total_start,
    );

    let close_after_resume_start = Instant::now();
    resumed.close().expect("close resumed db should succeed");
    log_stage("close-after-resume", close_after_resume_start, total_start);
    log_stage("complete", total_start, total_start);
}
