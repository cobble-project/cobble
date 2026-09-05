//! Real process boundary coverage for dedicated schema transforms.
use bytes::Bytes;
use cobble::{
    BytesMergeOperator, ColumnEvolution, CompactionMode, Config, Db, DbBuilder,
    DedicatedCompactionExecution, DedicatedCompactionExecutor, DedicatedCompactionMonitor,
    DedicatedCompactionPlan, ReadOnlyDbBuilder, RuntimeManifestMode, U32CounterMergeOperator,
    VolumeDescriptor,
};
use size::Size;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn parse_number(value: Option<Bytes>) -> cobble::Result<Option<Bytes>> {
    value
        .map(|value| {
            let number = std::str::from_utf8(&value)
                .map_err(|err| cobble::Error::InvalidState(err.to_string()))?
                .parse::<u32>()
                .map_err(|err| cobble::Error::InvalidState(err.to_string()))?;
            Ok(Bytes::copy_from_slice(&number.to_le_bytes()))
        })
        .transpose()
}

fn render_number(value: Option<Bytes>) -> cobble::Result<Option<Bytes>> {
    value
        .map(|value| {
            let bytes = value.as_ref().try_into().map_err(|_| {
                cobble::Error::InvalidState("transform expected a u32 column".into())
            })?;
            Ok(Bytes::from(format!("n={}", u32::from_le_bytes(bytes))))
        })
        .transpose()
}

fn config(root: &str, snapshot_source: bool) -> Config {
    Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
        num_columns: 2,
        total_buckets: 1,
        memtable_capacity: Size::from_mib(1),
        block_cache_size: Size::from_mib(1),
        base_file_size: Size::from_mib(1),
        value_separation_threshold: Some(Size::from_const(1)),
        compaction_mode: CompactionMode::Dedicated,
        compaction_dedicated_poll_interval_ms: 100,
        snapshot_disable_incremental_base_link: true,
        l0_file_limit: 2,
        runtime_manifest_mode: if snapshot_source {
            RuntimeManifestMode::Disabled
        } else {
            RuntimeManifestMode::Auto
        },
        log_console: false,
        ..Config::default()
    }
}

fn snapshot(db: &Db) -> cobble::ShardSnapshotInput {
    let (tx, rx) = std::sync::mpsc::channel();
    db.snapshot_with_callback(move |result| tx.send(result).unwrap())
        .unwrap();
    rx.recv_timeout(Duration::from_secs(30)).unwrap().unwrap()
}

fn metadata(path: &str) -> serde_json::Value {
    serde_json::from_slice(
        &cobble::test_utils::read_metadata_payload_from_path_for_test(
            path.strip_prefix("file://").unwrap_or(path),
        )
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn dedicated_transforms_cross_process_and_preserve_snapshot_values() {
    const CHILD_ROOT: &str = "COBBLE_DEDICATED_TRANSFORM_TEST_ROOT";
    if let Ok(root) = std::env::var(CHILD_ROOT) {
        let snapshot_source = std::env::var("COBBLE_TRANSFORM_SOURCE").unwrap() == "snapshot";
        let plan =
            DedicatedCompactionPlan::decode(&std::fs::read(format!("{root}/plan.json")).unwrap())
                .unwrap();
        let executor = DedicatedCompactionExecutor::open(config(&root, snapshot_source)).unwrap();
        if std::env::var("COBBLE_TRANSFORM_MISSING").unwrap() == "true" {
            let err = executor.execute(&plan).unwrap_err();
            assert!(err.to_string().contains("not registered"), "{err}");
        } else {
            executor
                .register_schema_transform("parse", parse_number)
                .unwrap();
            executor
                .register_schema_transform("render", render_number)
                .unwrap();
            assert!(matches!(
                executor.execute(&plan).unwrap(),
                DedicatedCompactionExecution::ResultPublished { .. }
            ));
        }
        return;
    }

    for snapshot_source in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_str().unwrap();
        let config = config(root, snapshot_source);
        let db = DbBuilder::new(config.clone())
            .db_id("transform-shard")
            .bucket_ranges(vec![0..=0])
            .register_schema_transform("parse", parse_number)
            .unwrap()
            .register_schema_transform("render", render_number)
            .unwrap()
            .open()
            .unwrap();
        let source_id = db.current_schema().version();
        let keys: Vec<_> = (0..64).map(|i| format!("key-{i:03}")).collect();
        for key in &keys {
            db.put(0, key, 0, b"04").unwrap();
            db.put(0, key, 1, b"payload").unwrap();
        }
        let old_snapshot = snapshot(&db);
        for key in &keys {
            db.merge(0, key, 0, b"2").unwrap();
        }
        snapshot(&db);

        let source = |index, transform: Option<&str>| ColumnEvolution::Source {
            source_index: index,
            transform_id: transform.map(str::to_owned),
        };
        let mut schema = db.update_schema();
        schema
            .remap_columns(
                None,
                vec![
                    source(1, None),
                    source(0, Some("parse")),
                    ColumnEvolution::Default {
                        value: Bytes::from_static(b"D"),
                    },
                ],
            )
            .unwrap();
        schema
            .replace_column(None, 1, Arc::new(U32CounterMergeOperator))
            .unwrap();
        schema.commit();
        // No file references this intermediate schema: it must still be loaded.
        let mut schema = db.update_schema();
        schema
            .remap_columns(
                None,
                vec![
                    source(1, Some("render")),
                    source(0, None),
                    source(2, None),
                    ColumnEvolution::Null,
                ],
            )
            .unwrap();
        schema
            .replace_column(None, 0, Arc::new(BytesMergeOperator))
            .unwrap();
        schema.commit();
        let target_id = db.current_schema().version();
        assert_eq!(target_id, source_id + 2);
        for key in &keys {
            db.merge(0, key, 0, b"-tail").unwrap();
        }
        let before = snapshot(&db);
        let before_manifest = metadata(&before.manifest_path);
        let files = |manifest: &serde_json::Value| -> Vec<serde_json::Value> {
            manifest["tree_levels"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|tree| tree.as_array().unwrap())
                .flat_map(|level| level["files"].as_array().unwrap())
                .cloned()
                .collect()
        };
        let input_files = files(&before_manifest);
        assert!(
            input_files
                .iter()
                .any(|file| file["schema_id"] == source_id)
        );
        assert!(
            input_files
                .iter()
                .any(|file| file["schema_id"] == target_id)
        );
        assert!(
            input_files
                .iter()
                .all(|file| file["schema_id"] != source_id + 1)
        );
        assert!(!before_manifest["vlog_files"].as_array().unwrap().is_empty());
        db.close().unwrap();
        drop(db);

        let mut monitor = DedicatedCompactionMonitor::watch_databases(
            config.clone(),
            vec![format!("{root}/transform-shard")],
        )
        .unwrap();
        monitor
            .register_schema_transform("parse", parse_number)
            .unwrap();
        monitor
            .register_schema_transform("render", render_number)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let plan = loop {
            if let Some(plan) = monitor.poll().unwrap().pop() {
                break plan;
            }
            assert!(Instant::now() < deadline, "no transform compaction plan");
            std::thread::sleep(Duration::from_millis(20));
        };
        std::fs::write(temp.path().join("plan.json"), plan.encode().unwrap()).unwrap();
        let result_path = temp.path().join(format!(
            "transform-shard/compaction/results/COMPACTION-{}",
            plan.job_id(),
        ));
        for missing in [true, false] {
            let mut child = ChildGuard(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "dedicated_transforms_cross_process_and_preserve_snapshot_values",
                        "--nocapture",
                    ])
                    .env(CHILD_ROOT, root)
                    .env(
                        "COBBLE_TRANSFORM_SOURCE",
                        if snapshot_source {
                            "snapshot"
                        } else {
                            "runtime"
                        },
                    )
                    .env("COBBLE_TRANSFORM_MISSING", missing.to_string())
                    .spawn()
                    .unwrap(),
            );
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                if let Some(status) = child.0.try_wait().unwrap() {
                    assert!(status.success(), "compactor child failed: {status}");
                    break;
                }
                assert!(Instant::now() < deadline, "compactor child timed out");
                std::thread::sleep(Duration::from_millis(20));
            }
            if missing {
                assert!(!result_path.exists());
                for file in &input_files {
                    assert!(
                        std::path::Path::new(
                            file["path"]
                                .as_str()
                                .unwrap()
                                .strip_prefix("file://")
                                .unwrap()
                        )
                        .exists()
                    );
                }
            }
        }
        assert!(
            result_path.exists(),
            "published result must survive executor exit"
        );
        let result = metadata(result_path.to_str().unwrap());
        assert_eq!(result["vlog_entry_deltas"], serde_json::json!([[0, -128]]));
        assert!(
            result["operation"]["Rewrite"]["outputs"]
                .as_array()
                .unwrap()
                .iter()
                .all(|file| file["path"]
                    .as_str()
                    .unwrap()
                    .contains(&format!("compaction/jobs/{}/data/", plan.job_id())))
        );
        let db = DbBuilder::new(config.clone())
            .db_id("transform-shard")
            .register_schema_transform("parse", parse_number)
            .unwrap()
            .register_schema_transform("render", render_number)
            .unwrap()
            .resume()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while result_path.exists() {
            assert!(Instant::now() < deadline, "writer did not consume result");
            std::thread::sleep(Duration::from_millis(20));
        }
        let after = snapshot(&db);
        let after_manifest = metadata(&after.manifest_path);
        assert!(
            files(&after_manifest)
                .iter()
                .all(|file| file["schema_id"] == target_id),
            "compaction was not applied at target schema {target_id}"
        );
        // Old-schema pointers were materialized; target-schema merge pointers remain live.
        let vlogs = after_manifest["vlog_files"].as_array().unwrap();
        assert_eq!(vlogs.len(), 1);
        assert_eq!(vlogs[0]["file_seq"], 1);
        assert_eq!(vlogs[0]["valid_entries"], 64);
        db.close().unwrap();
        let current = ReadOnlyDbBuilder::new(config.clone())
            .db_id("transform-shard")
            .register_schema_transform("parse", parse_number)
            .unwrap()
            .register_schema_transform("render", render_number)
            .unwrap()
            .open(after.snapshot_id)
            .unwrap();
        let old = ReadOnlyDbBuilder::new(config)
            .db_id("transform-shard")
            .open(old_snapshot.snapshot_id)
            .unwrap();
        for key in keys {
            assert_eq!(
                current.get(0, key.as_bytes()).unwrap(),
                Some(vec![
                    Some(Bytes::from_static(b"n=42-tail")),
                    Some(Bytes::from_static(b"payload")),
                    Some(Bytes::from_static(b"D")),
                    None,
                ])
            );
            assert_eq!(
                old.get(0, key.as_bytes()).unwrap(),
                Some(vec![
                    Some(Bytes::from_static(b"04")),
                    Some(Bytes::from_static(b"payload")),
                ])
            );
        }
    }
}
