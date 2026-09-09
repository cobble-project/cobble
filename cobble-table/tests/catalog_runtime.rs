use cobble::{Config, ShardSnapshotMetadata, VolumeDescriptor, VolumeUsageKind};
use cobble_table::catalog::{Catalog, FileCatalog, FileCatalogConfig, TableIdentifier};
use cobble_table::{LogicalType, SchemaChange, TableKey, TableKeyBuilder, TableSchema, Value};
use std::sync::Arc;

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
        total_buckets: 4,
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
        total_buckets: 4,
        ..Config::default()
    };
    let catalog = FileCatalog::open(&catalog_config, FileCatalogConfig::new("warehouse")).unwrap();
    catalog.create_namespace(vec!["app".into()]).unwrap();
    let users_id = TableIdentifier::new(["app"], "users");
    let events_id = TableIdentifier::new(["app"], "events");
    let schema = TableSchema::builder()
        .field("id", LogicalType::int64())
        .field("name", LogicalType::string().nullable())
        .primary_key(["id"])
        .bucket_key(["id"])
        .build()
        .unwrap();
    let users = catalog
        .create_table(users_id.clone(), schema.clone())
        .unwrap();
    let write_plan = users.new_write_builder().total_buckets(4).build().unwrap();
    let write_plan_json = serde_json::to_string(&write_plan).unwrap();
    assert!(!write_plan_json.contains("catalog-runtime-access"));
    assert!(!write_plan_json.contains("catalog-runtime-secret"));
    let unsupported_plan_json = write_plan_json.replacen("\"version\":1", "\"version\":2", 1);
    let unsupported_plan =
        serde_json::from_str::<cobble_table::TableWritePlan>(&unsupported_plan_json).unwrap();
    assert!(unsupported_plan.writer_builder(runtime.clone()).is_err());
    let events = catalog.create_table(events_id, schema.clone()).unwrap();
    assert!(!format!("{users:?}").contains("catalog-runtime-secret"));
    let left = users
        .writer_builder(runtime.clone())
        .unwrap()
        .db_id("shard-0")
        .bucket_ranges(vec![0..=1])
        .open()
        .unwrap();
    let right = users
        .writer_builder(runtime.clone())
        .unwrap()
        .db_id("shard-1")
        .bucket_ranges(vec![2..=3])
        .open()
        .unwrap();
    // Repeating a shard ID in another table must not share its metadata or CURRENT.
    let other = events
        .writer_builder(runtime.clone())
        .unwrap()
        .db_id("shard-0")
        .bucket_ranges(vec![0..=3])
        .open()
        .unwrap();
    let rows = (0..16)
        .map(|id| vec![Value::Int64(id), Value::String(format!("user-{id}"))])
        .collect::<Vec<_>>();
    let keys = rows
        .iter()
        .map(|row| key(left.key_builder(), &row[0]))
        .collect::<Vec<_>>();
    for (row, key) in rows.iter().zip(&keys) {
        if key.bucket() < 2 {
            left.put(row).unwrap();
        } else {
            right.put(row).unwrap();
        }
        other
            .put(&[row[0].clone(), Value::String("event".into())])
            .unwrap();
    }
    let left_snapshot = left.snapshot_and_wait().unwrap();
    let right_snapshot = right.snapshot_and_wait().unwrap();
    let coordinator = Arc::new(users.coordinator(runtime.clone()).unwrap());
    let committer = users.snapshot_committer(runtime.clone(), 2).unwrap();
    let first = committer
        .commit_batch(1, vec![left_snapshot.clone(), right_snapshot])
        .unwrap()
        .unwrap();
    let other_committer = events.snapshot_committer(runtime.clone(), 2).unwrap();
    other_committer
        .commit_batch(1, vec![other.snapshot_and_wait().unwrap()])
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
    let properties = std::fs::read_to_string(user_root.join("shard-0/PROPERTIES")).unwrap();
    assert!(properties.contains(&format!("file://{}", external.display())));
    // A combined source/cache volume retains the source root but scopes its writable cache.
    assert!(properties.contains(&format!(
        "file://{}/warehouse/tables/TABLE-{}",
        external.display(),
        users.table_id()
    )));
    assert!(!properties.contains("catalog-runtime-secret"));

    let reader = users
        .reader_builder(runtime.clone())
        .unwrap()
        .current_global_snapshot()
        .open()
        .unwrap();
    assert_eq!(
        reader.multi_get(&keys).unwrap(),
        rows.iter().cloned().map(Some).collect::<Vec<_>>()
    );
    let event_reader = events
        .reader_builder(runtime.clone())
        .unwrap()
        .current_global_snapshot()
        .open()
        .unwrap();
    assert_eq!(
        event_reader.get(&keys[0]).unwrap(),
        Some(vec![rows[0][0].clone(), Value::String("event".into())])
    );
    let left_index = keys.iter().position(|key| key.bucket() < 2).unwrap();
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
    drop(other);
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
    let evolved = catalog
        .evolve_schema(
            &accounts_id,
            vec![SchemaChange::AddField {
                name: "region".into(),
                logical_type: LogicalType::string().nullable(),
            }],
        )
        .unwrap();
    let old_reader = evolved
        .reader_builder(runtime.clone())
        .unwrap()
        .global_snapshot(first.id)
        .open()
        .unwrap();
    assert_eq!(old_reader.schema(), &schema);
    assert_eq!(
        old_reader.get(&keys[left_index]).unwrap(),
        Some(rows[left_index].clone())
    );
    assert_eq!(table_field_count(&left_snapshot), 2);
    drop(catalog);

    // A worker needs only the serialized, fixed plan, not a live catalog or its latest schema.
    let worker_plan =
        serde_json::from_str::<cobble_table::TableWritePlan>(&write_plan_json).unwrap();
    let mut worker_runtime = runtime.clone();
    worker_runtime.total_buckets = 1;
    let worker = worker_plan
        .writer_builder(worker_runtime.clone())
        .unwrap()
        .db_id("worker-shard")
        .bucket_ranges(vec![0..=3])
        .open()
        .unwrap();
    assert_eq!(worker.schema(), &schema);
    let worker_index = keys.iter().position(|key| key.bucket() != 0).unwrap();
    let worker_key = key(worker.key_builder(), &rows[worker_index][0]);
    assert_eq!(worker_key.bucket(), keys[worker_index].bucket());
    worker.put(&rows[worker_index]).unwrap();
    let worker_snapshot = worker.snapshot_and_wait().unwrap();
    drop(worker);
    let worker_historical = worker_plan
        .writer_builder(worker_runtime)
        .unwrap()
        .db_id("worker-shard")
        .bucket_ranges(vec![0..=3])
        .open_from_snapshot(worker_snapshot.snapshot_id)
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
        .db_id("shard-0")
        .bucket_ranges(vec![0..=1])
        .resume()
        .unwrap();
    let mut expected = rows[left_index].clone();
    expected.push(Value::Null);
    assert_eq!(resumed.schema(), evolved.schema());
    assert_eq!(resumed.get(&keys[left_index]).unwrap(), Some(expected));
    let historical_snapshot = resumed
        .shard_snapshot_metadata(left_snapshot.snapshot_id)
        .unwrap();
    assert_eq!(table_field_count(&historical_snapshot), 2);
    let resumed_snapshot = resumed.snapshot_and_wait().unwrap();
    assert_eq!(table_field_count(&resumed_snapshot), 3);
    drop(resumed);

    // An explicit historical restore must not apply the catalog's latest schema.
    let restored = evolved
        .writer_builder(runtime.clone())
        .unwrap()
        .db_id(&left_snapshot.db_id)
        .bucket_ranges(vec![0..=1])
        .open_from_snapshot(left_snapshot.snapshot_id)
        .unwrap();
    assert_eq!(restored.schema(), &schema);
    assert_eq!(
        restored.get(&keys[left_index]).unwrap(),
        Some(rows[left_index].clone())
    );
    drop(restored);
    // The table-local pointer is still the original published global snapshot.
    assert_eq!(
        coordinator
            .load_current_global_snapshot()
            .unwrap()
            .unwrap()
            .id,
        first.id
    );
    assert_eq!(reader.get(&keys[0]).unwrap(), Some(rows[0].clone()));
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
