use bytes::Bytes;
use cobble::{
    Config, DbBuilder, ShardSnapshotMetadata, TransformSpec, VolumeDescriptor, VolumeUsageKind,
};
use cobble_table::catalog::{Catalog, FileCatalog, FileCatalogConfig, TableIdentifier};
use cobble_table::{
    LogicalType, SchemaChange, TableKey, TableKeyBuilder, TableScanPlan, TableScanSplit,
    TableSchema, Value, ValueCodec, register_schema_transforms,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;

type TableTransform = Box<dyn Fn(Option<Bytes>) -> cobble::Result<Option<Bytes>> + Send + Sync>;

fn string_to_binary_factory(spec: &[u8]) -> cobble::Result<TableTransform> {
    if spec != b"string-to-binary" {
        return Err(cobble::Error::InputError(
            "unexpected table transform spec".to_string(),
        ));
    }
    Ok(Box::new(|value| {
        value
            .map(|raw| {
                let value = ValueCodec::decode(&LogicalType::string().nullable(), &raw)
                    .map_err(|error| cobble::Error::InputError(error.to_string()))?;
                let Value::String(value) = value else {
                    return Err(cobble::Error::InputError(
                        "expected a string table value".to_string(),
                    ));
                };
                ValueCodec::encode(
                    &LogicalType::binary().nullable(),
                    &Value::Binary(value.into()),
                )
                .map(Bytes::from)
                .map_err(|error| cobble::Error::InputError(error.to_string()))
            })
            .transpose()
    }))
}

#[test]
fn catalog_tables_share_storage_routes_and_isolate_snapshots_across_restarts() {
    let root = tempfile::tempdir().unwrap();
    let warehouse = root.path().join("warehouse");
    let local = root.path().join("local");
    let external = root.path().join("external");
    std::fs::create_dir_all(&external).unwrap();
    let mut shared = VolumeDescriptor::new(
        format!("file://{}", warehouse.display()),
        vec![
            VolumeUsageKind::PrimaryDataPriorityHigh,
            VolumeUsageKind::PrimaryDataPriorityMedium,
            VolumeUsageKind::PrimaryDataPriorityLow,
            VolumeUsageKind::Meta,
            VolumeUsageKind::Snapshot,
        ],
    );
    shared.access_id = Some("catalog-runtime-access".into());
    shared.secret_key = Some("catalog-runtime-secret".into());
    let catalog_config = Config {
        volumes: vec![shared],
        total_buckets: 2,
        ..Config::default()
    };
    let mut runtime_volumes =
        VolumeDescriptor::single_volume(format!("file://{}", local.display()));
    for (directory, kind) in [
        ("medium", VolumeUsageKind::PrimaryDataPriorityMedium),
        ("low", VolumeUsageKind::PrimaryDataPriorityLow),
    ] {
        runtime_volumes.push(VolumeDescriptor::new(
            format!("file://{}", root.path().join(directory).display()),
            vec![kind],
        ));
    }
    runtime_volumes.push(VolumeDescriptor::new(
        format!("file://{}", external.display()),
        vec![VolumeUsageKind::Readonly, VolumeUsageKind::Cache],
    ));
    let runtime = Config {
        volumes: runtime_volumes,
        total_buckets: 2,
        ..Config::default()
    };
    let catalog = FileCatalog::open(&catalog_config, FileCatalogConfig::new("warehouse")).unwrap();
    catalog.create_namespace(vec!["app".into()]).unwrap();
    let users_id = TableIdentifier::new(["app"], "users");
    let events_id = TableIdentifier::new(["app"], "events");
    let schema = TableSchema::builder()
        .field("id", LogicalType::int64())
        .field("name", LogicalType::string().nullable())
        .field("score", LogicalType::int8().nullable())
        .primary_key(["id"])
        .bucket_key(["id"])
        .build()
        .unwrap();
    let users = catalog
        .create_table(users_id.clone(), schema.clone())
        .unwrap();
    let write_plan = users.new_write_builder().total_buckets(2).build().unwrap();
    assert!(
        users
            .writer_builder(runtime.clone())
            .unwrap()
            .register_schema_transform("cobble.table/v1", string_to_binary_factory)
            .is_err()
    );
    let write_plan_json = serde_json::to_string(&write_plan).unwrap();
    assert!(!write_plan_json.contains("catalog-runtime-access"));
    assert!(!write_plan_json.contains("catalog-runtime-secret"));
    let unsupported_plan_json = write_plan_json.replacen("\"version\":1", "\"version\":2", 1);
    let unsupported_plan =
        serde_json::from_str::<cobble_table::TableWritePlan>(&unsupported_plan_json).unwrap();
    assert!(unsupported_plan.writer_builder(runtime.clone()).is_err());
    let events = catalog
        .create_table(events_id.clone(), schema.clone())
        .unwrap();
    assert!(!format!("{users:?}").contains("catalog-runtime-secret"));
    let left = users
        .writer_builder(runtime.clone())
        .unwrap()
        .bucket(0)
        .open()
        .unwrap();
    let right = users
        .writer_builder(runtime.clone())
        .unwrap()
        .bucket(1)
        .open()
        .unwrap();
    // Repeating a shard ID in another table must not share its metadata or CURRENT.
    let other_left = events
        .writer_builder(runtime.clone())
        .unwrap()
        .bucket(0)
        .open()
        .unwrap();
    let other_right = events
        .writer_builder(runtime.clone())
        .unwrap()
        .bucket(1)
        .open()
        .unwrap();
    let rows = (0..16)
        .map(|id| {
            vec![
                Value::Int64(id),
                Value::String(format!("user-{id}")),
                Value::Int8(id as i8),
            ]
        })
        .collect::<Vec<_>>();
    let keys = rows
        .iter()
        .map(|row| key(left.key_builder(), &row[0]))
        .collect::<Vec<_>>();
    for (row, key) in rows.iter().zip(&keys) {
        if key.bucket() == 0 {
            left.put(row).unwrap();
        } else {
            right.put(row).unwrap();
        }
        let event_row = [
            row[0].clone(),
            Value::String("event".into()),
            row[2].clone(),
        ];
        if key.bucket() == 0 {
            other_left.put(&event_row).unwrap();
        } else {
            other_right.put(&event_row).unwrap();
        }
    }
    let left_snapshot = left.snapshot_and_wait().unwrap();
    let right_snapshot = right.snapshot_and_wait().unwrap();
    let coordinator = Arc::new(users.coordinator(runtime.clone()).unwrap());
    let committer = users.snapshot_committer(runtime.clone(), 2).unwrap();
    let first = committer
        .commit_batch(1, vec![left_snapshot.clone(), right_snapshot.clone()])
        .unwrap()
        .unwrap();
    let other_committer = events.snapshot_committer(runtime.clone(), 2).unwrap();
    let other_snapshots = [
        other_left.snapshot_and_wait().unwrap(),
        other_right.snapshot_and_wait().unwrap(),
    ];
    other_committer
        .commit_batch(1, other_snapshots.to_vec())
        .unwrap()
        .unwrap();
    let user_root = warehouse.join(format!(
        "warehouse/tables/TABLE-{}",
        users.table_id().as_u32()
    ));
    let event_root = warehouse.join(format!(
        "warehouse/tables/TABLE-{}",
        events.table_id().as_u32()
    ));
    assert!(user_root.join("snapshot/CURRENT").is_file());
    assert!(event_root.join("snapshot/CURRENT").is_file());
    assert!(
        !local
            .join(format!(
                "warehouse/tables/TABLE-{}/snapshot/CURRENT",
                users.table_id()
            ))
            .exists()
    );
    let properties = std::fs::read_to_string(user_root.join("bucket-0/PROPERTIES")).unwrap();
    assert!(properties.contains(&format!("file://{}", external.display())));
    // A combined source/cache volume retains the source root but scopes its writable cache.
    assert!(properties.contains(&format!(
        "file://{}/warehouse/tables/TABLE-{}",
        external.display(),
        users.table_id()
    )));
    assert!(!properties.contains("catalog-runtime-secret"));

    let mut manual_reader_runtime = runtime.clone();
    manual_reader_runtime.reader.reload_tolerance_seconds = 3600;
    let reader = users
        .reader_builder(manual_reader_runtime)
        .unwrap()
        .current_global_snapshot()
        .open()
        .unwrap();
    let mut auto_runtime = runtime.clone();
    auto_runtime.reader.reload_tolerance_seconds = 0;
    let auto_reader = users
        .reader_builder(auto_runtime)
        .unwrap()
        .current_global_snapshot()
        .open()
        .unwrap();
    assert_eq!(auto_reader.schema().as_ref(), &schema);
    assert_eq!(
        reader.multi_get(&keys).unwrap(),
        rows.iter().cloned().map(Some).collect::<Vec<_>>()
    );
    let old_projection = reader.project_by_names(&["score"]).unwrap();
    let old_scan = reader.scan(keys[0].bucket()).unwrap();
    let scan_plan = reader.scan_plan().unwrap();
    let scan_plan_json = serde_json::to_string(&scan_plan).unwrap();
    assert!(!scan_plan_json.contains("catalog-runtime-access"));
    assert!(!scan_plan_json.contains("catalog-runtime-secret"));
    assert!(scan_plan_json.contains(&format!("warehouse/tables/TABLE-{}", users.table_id())));
    let event_reader = events
        .reader_builder(runtime.clone())
        .unwrap()
        .current_global_snapshot()
        .open()
        .unwrap();
    assert_eq!(
        event_reader.get(&keys[0]).unwrap(),
        Some(vec![
            rows[0][0].clone(),
            Value::String("event".into()),
            rows[0][2].clone(),
        ])
    );
    drop(other_left);
    drop(other_right);
    let evolved_events = catalog
        .evolve_schema(
            &events_id,
            vec![SchemaChange::TransformField {
                field_name: "name".into(),
                logical_type: LogicalType::binary().nullable(),
                transform: TransformSpec {
                    transform_type: "test.table.transform".into(),
                    spec: Bytes::from_static(b"string-to-binary"),
                },
            }],
        )
        .unwrap();
    let evolved_event_writer = evolved_events
        .writer_builder(runtime.clone())
        .unwrap()
        .register_schema_transform("test.table.transform", string_to_binary_factory)
        .unwrap()
        .bucket(keys[0].bucket())
        .resume_from_snapshot(other_snapshots[keys[0].bucket() as usize].snapshot_id)
        .unwrap();
    assert_eq!(
        evolved_event_writer.get(&keys[0]).unwrap(),
        Some(vec![
            rows[0][0].clone(),
            Value::Binary(Bytes::from_static(b"event")),
            rows[0][2].clone(),
        ])
    );
    let evolved_event_snapshot = evolved_event_writer.snapshot_and_wait().unwrap();
    drop(evolved_event_writer);
    let evolved_event_reader = evolved_events
        .readonly_table_builder(runtime.clone())
        .unwrap()
        .register_schema_transform("test.table.transform", string_to_binary_factory)
        .unwrap()
        .shard_snapshot(
            &evolved_event_snapshot.db_id,
            evolved_event_snapshot.snapshot_id,
        )
        .open()
        .unwrap();
    assert_eq!(
        evolved_event_reader.get(&keys[0]).unwrap(),
        Some(vec![
            rows[0][0].clone(),
            Value::Binary(Bytes::from_static(b"event")),
            rows[0][2].clone(),
        ])
    );
    let left_index = keys.iter().position(|key| key.bucket() == 0).unwrap();
    let right_index = keys.iter().position(|key| key.bucket() == 1).unwrap();
    let shard_reader = users
        .readonly_table_builder(runtime.clone())
        .unwrap()
        .shard_snapshot(&left_snapshot.db_id, left_snapshot.snapshot_id)
        .open()
        .unwrap();
    assert_eq!(
        shard_reader.get(&keys[left_index]).unwrap(),
        Some(rows[left_index].clone())
    );
    assert!(
        users
            .reader_builder(runtime.clone())
            .unwrap()
            .table_name("not-this-table")
            .current_global_snapshot()
            .open()
            .is_err()
    );

    drop(left);
    drop(right);
    let renamed = catalog.rename_table(&users_id, "accounts".into()).unwrap();
    let accounts_id = renamed.identifier().clone();
    assert_eq!(renamed.table_id(), users.table_id());
    drop(catalog);
    let catalog = FileCatalog::open(&catalog_config, FileCatalogConfig::new("warehouse")).unwrap();
    let loaded = catalog.load_table(&accounts_id).unwrap();
    let renamed_reader = loaded
        .reader_builder(runtime.clone())
        .unwrap()
        .current_global_snapshot()
        .open()
        .unwrap();
    assert_eq!(renamed_reader.get(&keys[0]).unwrap(), Some(rows[0].clone()));
    let materialization_runtime = Config {
        volumes: VolumeDescriptor::single_volume(format!(
            "file://{}",
            root.path().join("materialization-runtime").display()
        )),
        total_buckets: 2,
        ..Config::default()
    };
    let replay_builder = DbBuilder::new(materialization_runtime.clone())
        .db_id("replay-shard")
        .bucket_ranges(vec![0..=0]);
    register_schema_transforms(&replay_builder).unwrap();
    let replay_db = Arc::new(replay_builder.open().unwrap());
    let mapping_builder = DbBuilder::new(materialization_runtime)
        .db_id("mapping-retry")
        .bucket_ranges(vec![1..=1]);
    register_schema_transforms(&mapping_builder).unwrap();
    let mapping_db = Arc::new(mapping_builder.open().unwrap());
    let replay_initial = catalog
        .materialize_table(Arc::clone(&replay_db), &accounts_id)
        .unwrap();
    drop(replay_initial);
    let mut mapping_initial = catalog
        .materialize_table(Arc::clone(&mapping_db), &accounts_id)
        .unwrap();
    mapping_initial.put(&rows[right_index]).unwrap();
    mapping_initial.snapshot_and_wait().unwrap();
    let stale_projection = mapping_initial.project_by_names(&["score"]).unwrap();
    let mut stale_scan = mapping_initial.scan(keys[right_index].bucket()).unwrap();
    catalog
        .evolve_schema(
            &accounts_id,
            vec![
                SchemaChange::RenameField {
                    field_name: "name".into(),
                    new_name: "label".into(),
                },
                SchemaChange::AlterFieldType {
                    field_name: "score".into(),
                    logical_type: LogicalType::int16().nullable(),
                },
            ],
        )
        .unwrap();
    let evolved = catalog
        .evolve_schema(
            &accounts_id,
            vec![
                SchemaChange::AlterFieldType {
                    field_name: "score".into(),
                    logical_type: LogicalType::int64().nullable(),
                },
                SchemaChange::AddField {
                    name: "region".into(),
                    logical_type: LogicalType::string().nullable(),
                },
            ],
        )
        .unwrap();
    assert!(!reader.refresh().unwrap());
    assert_eq!(reader.schema().as_ref(), &schema);
    assert_eq!(auto_reader.get(&keys[0]).unwrap(), Some(rows[0].clone()));
    let old_reader = evolved
        .reader_builder(runtime.clone())
        .unwrap()
        .global_snapshot(first.id)
        .open()
        .unwrap();
    assert_eq!(table_field_count(&left_snapshot), 3);
    let intermediate = warehouse.join(format!(
        "warehouse/catalog/tables/TABLE-{}/schemas/SCHEMA-1",
        users.table_id()
    ));
    let unavailable_intermediate = intermediate.with_extension("unavailable");
    std::fs::rename(&intermediate, &unavailable_intermediate).unwrap();
    assert!(
        catalog
            .materialize_table(Arc::clone(&replay_db), &accounts_id)
            .is_err()
    );
    std::fs::rename(&unavailable_intermediate, &intermediate).unwrap();
    let replayed = catalog
        .materialize_table(Arc::clone(&replay_db), &accounts_id)
        .unwrap();
    assert_eq!(replayed.schema(), evolved.schema());
    drop(replayed);

    let failed_mapping =
        catalog_schema_mapping_path(&warehouse, users.table_id().as_u32(), "mapping-retry", 1);
    std::fs::create_dir_all(&failed_mapping).unwrap();
    assert!(
        catalog
            .materialize_table(Arc::clone(&mapping_db), &accounts_id)
            .is_err()
    );
    std::fs::remove_dir(&failed_mapping).unwrap();
    assert!(mapping_initial.get(&keys[right_index]).is_err());
    assert!(
        mapping_initial
            .multi_get(&[keys[right_index].clone()])
            .is_err()
    );
    assert!(mapping_initial.put(&rows[right_index]).is_err());
    assert!(
        mapping_initial
            .put_with_options(&rows[right_index], &cobble::WriteOptions::with_ttl(1))
            .is_err()
    );
    assert!(mapping_initial.delete(&keys[right_index]).is_err());
    assert!(mapping_initial.scan(keys[right_index].bucket()).is_err());
    assert!(stale_projection.get(&keys[right_index]).is_err());
    assert_eq!(stale_scan.next().unwrap().unwrap(), rows[right_index]);
    assert!(stale_scan.next().is_none());
    drop(stale_scan);

    assert!(evolved.refresh_writer(&mut mapping_initial).unwrap());
    assert!(!evolved.refresh_writer(&mut mapping_initial).unwrap());
    assert!(events.refresh_writer(&mut mapping_initial).is_err());
    assert_eq!(mapping_initial.schema(), evolved.schema());
    let Value::Int8(score) = rows[right_index][2] else {
        panic!("catalog schema score starts as int8");
    };
    let refreshed_row = vec![
        rows[right_index][0].clone(),
        rows[right_index][1].clone(),
        Value::Int64(i64::from(score)),
        Value::Null,
    ];
    assert_eq!(
        mapping_initial.get(&keys[right_index]).unwrap(),
        Some(refreshed_row.clone())
    );
    mapping_initial
        .put_with_options(&refreshed_row, &cobble::WriteOptions::with_ttl(1))
        .unwrap();
    assert_eq!(
        mapping_initial.get(&keys[right_index]).unwrap(),
        Some(refreshed_row)
    );
    assert_eq!(
        mapping_initial
            .project_by_names(&["score"])
            .unwrap()
            .get(&keys[right_index])
            .unwrap(),
        Some(vec![Value::Int64(i64::from(score))])
    );
    let mut unrelated = mapping_db.update_schema();
    unrelated.ensure_column_family_exists("unrelated").unwrap();
    unrelated.commit();
    assert!(mapping_initial.get(&keys[right_index]).unwrap().is_some());
    drop(mapping_initial);
    drop(catalog);

    // The worker supplies fresh local primary/cache roots; the plan never appends catalog paths.
    let worker_scan_plan = serde_json::from_str::<TableScanPlan>(&scan_plan_json).unwrap();
    let worker_scan_runtime = Config {
        volumes: vec![
            VolumeDescriptor::new(
                format!("file://{}", root.path().join("worker-primary").display()),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            VolumeDescriptor::new(
                format!("file://{}", root.path().join("worker-cache").display()),
                vec![VolumeUsageKind::Cache],
            ),
        ],
        total_buckets: 1,
        ..Config::default()
    };
    // A worker needs only the serialized, fixed plan, not a live catalog or its latest schema.
    let worker_plan =
        serde_json::from_str::<cobble_table::TableWritePlan>(&write_plan_json).unwrap();
    let mut worker_runtime = runtime.clone();
    worker_runtime.total_buckets = 1;
    let worker_bucket = 1;
    let worker = worker_plan
        .writer_builder(worker_runtime.clone())
        .unwrap()
        .bucket(worker_bucket)
        .open()
        .unwrap();
    assert_eq!(worker.schema(), &schema);
    let worker_index = keys
        .iter()
        .position(|key| key.bucket() == worker_bucket)
        .unwrap();
    let worker_key = key(worker.key_builder(), &rows[worker_index][0]);
    assert_eq!(worker_key.bucket(), keys[worker_index].bucket());
    worker.put(&rows[worker_index]).unwrap();
    let worker_snapshot = worker.snapshot_and_wait().unwrap();
    drop(worker);
    let worker_historical = worker_plan
        .writer_builder(worker_runtime)
        .unwrap()
        .bucket(worker_bucket)
        .resume_from_snapshot(worker_snapshot.snapshot_id)
        .unwrap();
    assert_eq!(worker_historical.schema(), &schema);
    assert_eq!(
        worker_historical.get(&worker_key).unwrap(),
        Some(rows[worker_index].clone())
    );
    drop(worker_historical);

    let resumed = evolved
        .writer_builder(runtime.clone())
        .unwrap()
        .bucket(0)
        .resume_from_snapshot(left_snapshot.snapshot_id)
        .unwrap();
    let mut expected = rows[left_index].clone();
    let Value::Int8(score) = rows[left_index][2] else {
        panic!("catalog schema score starts as int8");
    };
    expected[2] = Value::Int64(i64::from(score));
    expected.push(Value::Null);
    assert_eq!(resumed.schema(), evolved.schema());
    assert_eq!(resumed.get(&keys[left_index]).unwrap(), Some(expected));
    let historical_snapshot = resumed
        .shard_snapshot_metadata(left_snapshot.snapshot_id)
        .unwrap();
    assert_eq!(table_field_count(&historical_snapshot), 3);
    let resumed_snapshot = resumed.snapshot_and_wait().unwrap();
    assert_eq!(table_field_count(&resumed_snapshot), 4);
    drop(resumed);

    let resumed_right = evolved
        .writer_builder(runtime.clone())
        .unwrap()
        .bucket(1)
        .resume_from_snapshot(right_snapshot.snapshot_id)
        .unwrap();
    let resumed_right_snapshot = resumed_right.snapshot_and_wait().unwrap();
    drop(resumed_right);
    let evolved_global = committer
        .commit_batch(2, vec![resumed_snapshot.clone(), resumed_right_snapshot])
        .unwrap()
        .unwrap();
    assert_eq!(auto_reader.schema().as_ref(), &schema);
    assert_eq!(old_reader.schema().as_ref(), &schema);
    assert!(!old_reader.refresh().unwrap());
    assert_eq!(
        old_reader.get(&keys[left_index]).unwrap(),
        Some(rows[left_index].clone())
    );
    let current_path = user_root.join("snapshot/CURRENT");
    let valid_current = std::fs::read(&current_path).unwrap();
    std::fs::write(&current_path, b"corrupt global current\n").unwrap();
    assert!(reader.refresh().is_err());
    assert!(auto_reader.get(&keys[0]).is_err());
    assert_eq!(auto_reader.schema().as_ref(), &schema);
    assert_eq!(reader.get(&keys[0]).unwrap(), Some(rows[0].clone()));
    std::fs::write(&current_path, &valid_current).unwrap();
    let candidate_manifest = std::path::PathBuf::from(
        resumed_snapshot
            .manifest_path
            .strip_prefix("file://")
            .unwrap_or(&resumed_snapshot.manifest_path),
    );
    let unavailable_manifest = candidate_manifest.with_extension("unavailable");
    std::fs::rename(&candidate_manifest, &unavailable_manifest).unwrap();
    assert!(reader.refresh().is_err());
    assert!(auto_reader.get(&keys[0]).is_err());
    assert_eq!(auto_reader.schema().as_ref(), &schema);
    assert_eq!(reader.schema().as_ref(), &schema);
    assert_eq!(reader.get(&keys[0]).unwrap(), Some(rows[0].clone()));
    assert_eq!(reader.scan_plan().unwrap().snapshot_id(), first.id);
    std::fs::rename(&unavailable_manifest, &candidate_manifest).unwrap();
    assert!(reader.refresh().unwrap());
    assert!(!reader.refresh().unwrap());
    assert_eq!(reader.schema().as_ref(), evolved.schema());
    let evolved_reader = evolved
        .reader_builder(runtime.clone())
        .unwrap()
        .global_snapshot(evolved_global.id)
        .open()
        .unwrap();
    let transformed_rows = rows
        .iter()
        .map(|row| {
            let Value::Int8(score) = row[2] else {
                panic!("catalog schema score starts as int8");
            };
            vec![
                row[0].clone(),
                row[1].clone(),
                Value::Int64(i64::from(score)),
                Value::Null,
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(
        evolved_reader.multi_get(&keys).unwrap(),
        transformed_rows
            .iter()
            .cloned()
            .map(Some)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        reader.multi_get(&keys).unwrap(),
        transformed_rows
            .iter()
            .cloned()
            .map(Some)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        auto_reader.multi_get(&keys).unwrap(),
        transformed_rows
            .iter()
            .cloned()
            .map(Some)
            .collect::<Vec<_>>()
    );
    assert_eq!(auto_reader.schema().as_ref(), evolved.schema());
    assert_eq!(
        old_projection.get(&keys[0]).unwrap(),
        Some(vec![rows[0][2].clone()])
    );
    assert_eq!(
        old_scan.collect::<Result<Vec<_>, _>>().unwrap(),
        rows.iter()
            .zip(&keys)
            .filter(|(_, key)| key.bucket() == keys[0].bucket())
            .map(|(row, _)| row.clone())
            .collect::<Vec<_>>()
    );
    let mut worker_splits = worker_scan_plan.splits().unwrap();
    let first_split_json = serde_json::to_string(&worker_splits[0]).unwrap();
    worker_splits[0] = serde_json::from_str::<TableScanSplit>(&first_split_json).unwrap();
    let mut worker_rows = worker_splits
        .into_iter()
        .flat_map(|split| split.create_scanner(worker_scan_runtime.clone()).unwrap())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    worker_rows.sort_by_key(|row| match row.first() {
        Some(Value::Int64(id)) => *id,
        _ => panic!("catalog schema always starts with an int64 id"),
    });
    assert_eq!(worker_rows, rows);
    let evolved_shard_reader = evolved
        .readonly_table_builder(runtime.clone())
        .unwrap()
        .shard_snapshot(&resumed_snapshot.db_id, resumed_snapshot.snapshot_id)
        .open()
        .unwrap();
    assert_eq!(
        evolved_shard_reader.get(&keys[left_index]).unwrap(),
        Some(transformed_rows[left_index].clone())
    );
    let evolved_plan = evolved_reader.scan_plan().unwrap();
    let mut evolved_splits = evolved_plan.splits().unwrap();
    let split_json = serde_json::to_string(&evolved_splits[0]).unwrap();
    evolved_splits[0] = serde_json::from_str(&split_json).unwrap();
    let mut scanned_rows = evolved_splits
        .into_iter()
        .flat_map(|split| {
            split
                .scanner_builder(worker_scan_runtime.clone())
                .open()
                .unwrap()
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    scanned_rows.sort_by_key(|row| match row.first() {
        Some(Value::Int64(id)) => *id,
        _ => panic!("catalog schema always starts with an int64 id"),
    });
    assert_eq!(scanned_rows, transformed_rows);

    // An explicit historical restore must not apply the catalog's latest schema.
    let restored = users
        .readonly_table_builder(runtime.clone())
        .unwrap()
        .shard_snapshot(&left_snapshot.db_id, left_snapshot.snapshot_id)
        .open()
        .unwrap();
    assert_eq!(restored.schema(), &schema);
    assert_eq!(
        restored.get(&keys[left_index]).unwrap(),
        Some(rows[left_index].clone())
    );
    drop(restored);
    // The refreshed reader now follows the committed global snapshot.
    assert_eq!(
        coordinator
            .load_current_global_snapshot()
            .unwrap()
            .unwrap()
            .id,
        evolved_global.id
    );
    assert_eq!(
        reader.get(&keys[0]).unwrap(),
        Some(transformed_rows[0].clone())
    );
}

fn key(mut builder: TableKeyBuilder, value: &Value) -> TableKey {
    builder.push(value.clone());
    builder.build().unwrap()
}

fn table_field_count(snapshot: &ShardSnapshotMetadata) -> usize {
    snapshot
        .column_families
        .values()
        .find(|family| {
            family
                .options
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("format"))
                .and_then(serde_json::Value::as_str)
                == Some("cobble-table")
        })
        .unwrap()
        .options
        .metadata
        .as_ref()
        .unwrap()["schema"]["fields"]
        .as_array()
        .unwrap()
        .len()
}

fn catalog_schema_mapping_path(
    warehouse: &Path,
    table_id: u32,
    db_id: &str,
    catalog_schema_id: u32,
) -> PathBuf {
    let digest = Sha256::digest(db_id.as_bytes());
    let shard = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    warehouse.join(format!(
        "warehouse/catalog/tables/TABLE-{table_id}/shards/{shard}/SCHEMA-{catalog_schema_id}"
    ))
}
