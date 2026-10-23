use bytes::Bytes;
use cobble::{Config, DbBuilder, ReadOptions, TimeProviderKind, VolumeDescriptor, WriteOptions};
use cobble_table::catalog::{
    Catalog, FileCatalog, FileCatalogConfig, SchemaChange, TableIdentifier,
};
use cobble_table::snapshot::TableSnapshotCommitter;
use cobble_table::{
    DataField, LogicalType, ReadOnlyTableBuilder, Table, TableKey, TableReaderBuilder, TableSchema,
    TableStore, Value, ValueCodec, list_element_transform, register_schema_transforms,
    table_merge_operator_resolver,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn config(root: &std::path::Path) -> Config {
    Config {
        volumes: VolumeDescriptor::single_volume(
            url::Url::from_directory_path(root).unwrap().to_string(),
        ),
        total_buckets: 1,
        ..Config::default()
    }
}

fn schema() -> TableSchema {
    TableSchema::builder()
        .field("id", LogicalType::int64())
        .field(
            "items",
            LogicalType::list(LogicalType::int32().nullable()).nullable(),
        )
        .field("note", LogicalType::string())
        .primary_key(["id"])
        .bucket_key(["id"])
        .build()
        .unwrap()
}

fn key(table: &Table, id: i64) -> TableKey {
    let mut builder = table.key_builder();
    builder.push(Value::Int64(id));
    builder.build().unwrap()
}
fn row(items: Value) -> Vec<Value> {
    vec![Value::Int64(1), items, Value::String("unchanged".into())]
}
fn list(values: &[i32]) -> Value {
    Value::List(values.iter().copied().map(Value::Int32).collect())
}

fn stored_column(
    config: &Config,
    snapshot: &cobble::ShardSnapshotMetadata,
    name: &str,
    column: usize,
    key: &TableKey,
) -> Bytes {
    let builder = cobble::ReadOnlyDbBuilder::new(config.clone())
        .db_id(&snapshot.db_id)
        .merge_operator_resolver(table_merge_operator_resolver(None));
    register_schema_transforms(&builder).unwrap();
    builder
        .open(snapshot.snapshot_id)
        .unwrap()
        .get_with_options(
            0,
            key.encoded(),
            &ReadOptions::default().with_column_family(name),
        )
        .unwrap()
        .unwrap()[column]
        .clone()
        .unwrap()
}

fn assert_snapshot_readers(
    config: &Config,
    name: &str,
    db_id: &str,
    snapshot: u64,
    global: u64,
    expected: Vec<Value>,
) {
    let table = ReadOnlyTableBuilder::new(config.clone())
        .table_name(name)
        .shard_snapshot(db_id, snapshot)
        .open()
        .unwrap();
    let mut builder = table.key_builder();
    builder.push(Value::Int64(1));
    let key = builder.build().unwrap();
    assert_eq!(table.get(&key).unwrap(), Some(expected.clone()));
    assert_eq!(
        table.multi_get(std::slice::from_ref(&key)).unwrap(),
        vec![Some(expected.clone())]
    );
    assert_eq!(
        table
            .scan(0)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![expected.clone()]
    );
    assert_eq!(
        table
            .project_by_names(&["items"])
            .unwrap()
            .get(&key)
            .unwrap(),
        Some(vec![expected[1].clone()])
    );
    let reader = TableReaderBuilder::new(config.clone())
        .table_name(name)
        .global_snapshot(global)
        .open()
        .unwrap();
    assert_eq!(reader.get(&key).unwrap(), Some(expected.clone()));
    assert_eq!(
        reader.multi_get(std::slice::from_ref(&key)).unwrap(),
        vec![Some(expected.clone())]
    );
    assert_eq!(
        reader
            .project_by_names(&["items"])
            .unwrap()
            .get(&key)
            .unwrap(),
        Some(vec![expected[1].clone()])
    );
    assert_eq!(
        reader
            .scan(0)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![expected.clone()]
    );
    let rows = reader
        .scan_plan()
        .unwrap()
        .splits()
        .unwrap()
        .into_iter()
        .flat_map(|split| split.create_scanner(config.clone()).unwrap())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, vec![expected]);
}

#[test]
fn list_append_preserves_value_codec_bytes_resets_and_shared_store_snapshots() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let mut store = TableStore::builder(config.clone())
        .db_id("lists")
        .bucket_ranges(vec![0..=0])
        .open()
        .unwrap();
    let table = store.create_table("events", schema()).unwrap();
    let key = key(&table, 1);
    table.put(&row(Value::Null)).unwrap();
    store.snapshot_and_wait().unwrap();
    table
        .append(&key, "items", &[Value::Int32(1), Value::Null])
        .unwrap();
    let values = Value::List(vec![Value::Int32(1), Value::Null]);
    let expected_bytes = vec![1, 2, 0, 0, 0, 5, 0, 0, 0, 1, 128, 0, 0, 1, 1, 0, 0, 0, 0];
    assert_eq!(
        ValueCodec::encode(&schema().fields[1].logical_type, &values).unwrap(),
        expected_bytes
    );
    let encoded = store.snapshot_and_wait().unwrap();
    assert_eq!(
        stored_column(&config, &encoded, "events", 0, &key).as_ref(),
        expected_bytes
    );
    assert_eq!(table.get(&key).unwrap(), Some(row(values)));
    table.clear_list(&key, "items").unwrap();
    store.snapshot_and_wait().unwrap();
    table.append(&key, "items", &[Value::Int32(2)]).unwrap();
    assert_eq!(
        table.get_values(&key).unwrap(),
        Some(vec![list(&[2]), Value::String("unchanged".into())])
    );
    assert_eq!(
        table.multi_get(&[key.clone(), key.clone()]).unwrap(),
        vec![Some(row(list(&[2]))); 2]
    );
    assert_eq!(
        table.multi_get_values(std::slice::from_ref(&key)).unwrap(),
        vec![Some(vec![list(&[2]), Value::String("unchanged".into())])]
    );
    let projection = table.project_by_names(&["items", "id"]).unwrap();
    assert_eq!(
        projection.get(&key).unwrap(),
        Some(vec![list(&[2]), Value::Int64(1)])
    );
    assert_eq!(
        projection.multi_get(std::slice::from_ref(&key)).unwrap(),
        vec![Some(vec![list(&[2]), Value::Int64(1)])]
    );
    assert_eq!(
        projection
            .scan(0)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![vec![list(&[2]), Value::Int64(1)]]
    );
    assert_eq!(
        table
            .scan(0)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![row(list(&[2]))]
    );
    assert!(table.append(&key, "note", &[Value::Int32(3)]).is_err());

    let only_schema = TableSchema::builder()
        .field("id", LogicalType::int64())
        .field("items", LogicalType::list(LogicalType::int32().nullable()))
        .primary_key(["id"])
        .bucket_key(["id"])
        .build()
        .unwrap();
    let only = store.create_table("only", only_schema.clone()).unwrap();
    let only_key = {
        let mut builder = only.key_builder();
        builder.push(Value::Int64(1));
        builder.build().unwrap()
    };
    only.append(&only_key, "items", &[Value::Int32(7)]).unwrap();
    store.snapshot_and_wait().unwrap();
    only.delete(&only_key).unwrap();
    store.snapshot_and_wait().unwrap();
    only.append(&only_key, "items", &[Value::Int32(8), Value::Null])
        .unwrap();
    let checkpoint = store.snapshot_and_wait().unwrap();
    assert_eq!(
        only.get(&only_key).unwrap(),
        Some(vec![
            Value::Int64(1),
            Value::List(vec![Value::Int32(8), Value::Null])
        ])
    );
    assert_eq!(
        stored_column(&config, &checkpoint, "only", 0, &only_key).as_ref(),
        ValueCodec::encode(
            &only_schema.fields[1].logical_type,
            &Value::List(vec![Value::Int32(8), Value::Null])
        )
        .unwrap()
    );
    drop(only);
    let committer = TableSnapshotCommitter::open(&config, 2).unwrap();
    let global = committer
        .commit_batch(1, vec![checkpoint.clone()])
        .unwrap()
        .unwrap();
    assert_snapshot_readers(
        &config,
        "events",
        "lists",
        checkpoint.snapshot_id,
        global.id,
        row(list(&[2])),
    );
    table.put(&row(list(&[9]))).unwrap();
    table.append(&key, "items", &[Value::Int32(10)]).unwrap();
    store.snapshot_and_wait().unwrap();
    assert_eq!(table.get(&key).unwrap(), Some(row(list(&[9, 10]))));
    table.delete(&key).unwrap();
    store.snapshot_and_wait().unwrap();
    table.put(&row(list(&[]))).unwrap();
    table.append(&key, "items", &[Value::Int32(11)]).unwrap();
    assert_eq!(table.get(&key).unwrap(), Some(row(list(&[11]))));
    drop(projection);
    drop(table);
    store.switch_to_snapshot(checkpoint.snapshot_id).unwrap();
    assert_eq!(
        store.open_table("events").unwrap().get(&key).unwrap(),
        Some(row(list(&[2])))
    );
    store.close().unwrap();
    drop(store);
    let store = TableStore::builder(config.clone())
        .db_id("lists")
        .resume_from_snapshot(checkpoint.snapshot_id)
        .unwrap();
    assert_eq!(
        store.open_table("events").unwrap().get(&key).unwrap(),
        Some(row(list(&[2])))
    );
    store.close().unwrap();

    // Simulate a persisted legacy Table: identical metadata/ValueCodec bytes, Bytes operator.
    let db = Arc::new(
        DbBuilder::new(config)
            .db_id("legacy")
            .bucket_ranges(vec![0..=0])
            .merge_operator_resolver(table_merge_operator_resolver(None))
            .open()
            .unwrap(),
    );
    let table = Table::create(Arc::clone(&db), "events", schema()).unwrap();
    table.put(&row(list(&[1]))).unwrap();
    drop(table);
    let mut update = db.update_schema();
    update
        .set_column_operator(
            Some("events".into()),
            0,
            Arc::new(cobble::BytesMergeOperator),
        )
        .unwrap();
    update
        .clear_column_metadata(Some("events".into()), 0)
        .unwrap();
    let before = update.commit().version();
    let table = Table::open(Arc::clone(&db), "events").unwrap();
    assert_eq!(db.current_schema().version(), before + 1);
    table.append(&key, "items", &[Value::Int32(2)]).unwrap();
    assert_eq!(table.get(&key).unwrap(), Some(row(list(&[1, 2]))));
    drop(table);
    let mut update = db.update_schema();
    update
        .set_column_operator(
            Some("events".into()),
            0,
            Arc::new(cobble::U64CounterMergeOperator),
        )
        .unwrap();
    let version = update.commit().version();
    assert!(Table::open(Arc::clone(&db), "events").is_err());
    assert_eq!(db.current_schema().version(), version);
    db.close().unwrap();
}

#[test]
fn list_append_forwards_ordinary_row_ttl_with_existing_best_effort_merge_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    config.time_provider = TimeProviderKind::Manual;
    config.ttl_enabled = true;
    config.default_ttl_seconds = Some(20);
    // Keep SST records separate: no embedded worker should strengthen renewal by rewriting them.
    config.compaction_mode = cobble::CompactionMode::Dedicated;
    let db = Arc::new(
        DbBuilder::new(config)
            .db_id("ttl")
            .bucket_ranges(vec![0..=0])
            .merge_operator_resolver(table_merge_operator_resolver(None))
            .open()
            .unwrap(),
    );
    let schema = TableSchema::builder()
        .field("id", LogicalType::int64())
        .field("items", LogicalType::list(LogicalType::int32()))
        .primary_key(["id"])
        .bucket_key(["id"])
        .build()
        .unwrap();
    let table = Table::create(Arc::clone(&db), "events", schema).unwrap();
    let key = key(&table, 1);
    let mut short = WriteOptions::default();
    short.ttl_seconds = Some(5);
    let mut longer = WriteOptions::default();
    longer.ttl_seconds = Some(20);
    let row = |values: &[i32]| vec![Value::Int64(1), list(values)];
    db.set_time(100);
    table.put_with_options(&row(&[1]), &short).unwrap();
    table
        .append_with_options(&key, "items", &[Value::Int32(2)], &longer)
        .unwrap();
    assert_eq!(table.get(&key).unwrap(), Some(row(&[1, 2])));
    db.set_time(121);
    assert_eq!(table.get(&key).unwrap(), None);

    db.set_time(200);
    table.put_with_options(&row(&[10]), &short).unwrap();
    table.snapshot_and_wait().unwrap();
    db.set_time(202);
    table
        .append_with_options(&key, "items", &[Value::Int32(20)], &longer)
        .unwrap();
    table.snapshot_and_wait().unwrap();
    assert_eq!(table.get(&key).unwrap(), Some(row(&[10, 20])));
    db.set_time(206);
    // Best effort means the newer append does not retain the older expired SST base.
    assert_eq!(table.get(&key).unwrap(), Some(row(&[20])));
    db.set_time(223);
    assert_eq!(table.get(&key).unwrap(), None);

    db.set_time(230);
    table.put(&row(&[30])).unwrap(); // ordinary configured default row TTL
    db.set_time(240);
    table
        .append_with_options(&key, "items", &[], &longer)
        .unwrap();
    db.set_time(251);
    assert_eq!(
        table.get(&key).unwrap(),
        None,
        "empty append must not renew TTL"
    );
    db.close().unwrap();
}

fn nested_type() -> LogicalType {
    LogicalType::struct_type(vec![
        DataField::new(0, "number", LogicalType::int64().nullable()).unwrap(),
    ])
}
fn wrap_int_factory(
    _: &[u8],
) -> cobble::Result<
    impl Fn(Option<Bytes>) -> cobble::Result<Option<Bytes>> + Send + Sync + 'static + use<>,
> {
    Ok(list_element_transform(true, true, |bytes| {
        let value = ValueCodec::decode(&LogicalType::int64().nullable(), &bytes)
            .map_err(|error| cobble::Error::InputError(error.to_string()))?;
        ValueCodec::encode(&nested_type(), &Value::Struct(vec![value]))
            .map(Bytes::from)
            .map_err(|error| cobble::Error::InputError(error.to_string()))
    }))
}

#[test]
fn list_catalog_widening_custom_transform_and_dedicated_compaction_keep_common_encoding() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    config.compaction_mode = cobble::CompactionMode::Dedicated;
    config.compaction_dedicated_poll_interval_ms = 50;
    config.snapshot_disable_incremental_base_link = true;
    config.l0_file_limit = 2;
    let catalog = FileCatalog::open(&config, FileCatalogConfig::new("catalog")).unwrap();
    catalog.create_namespace(vec!["app".into()]).unwrap();
    let id = TableIdentifier::new(["app"], "events");
    let mut initial_schema = schema();
    initial_schema.fields[1].logical_type.nullable = false;
    let initial = catalog.create_table(id.clone(), initial_schema).unwrap();
    let make_store = || {
        TableStore::builder(config.clone())
            .db_id("catalog-shard")
            .bucket_ranges(vec![0..=0])
            .register_schema_transform("wrap-int", wrap_int_factory)
            .unwrap()
    };
    let mut store = make_store().open().unwrap();
    let table = store.materialize_table(&initial).unwrap();
    let key = key(&table, 1);
    table.put(&row(list(&[1, 2]))).unwrap();
    let old = store.snapshot_and_wait().unwrap();
    let evolved = catalog
        .evolve_schema(
            &id,
            vec![
                SchemaChange::RenameField {
                    field_name: "items".into(),
                    new_name: "history".into(),
                },
                SchemaChange::AddField {
                    name: "extra".into(),
                    logical_type: LogicalType::list(LogicalType::string()).nullable(),
                },
                SchemaChange::DropField {
                    field_name: "note".into(),
                },
                SchemaChange::AlterFieldType {
                    field_name: "history".into(),
                    logical_type: LogicalType::list(LogicalType::int64().nullable()).nullable(),
                },
            ],
        )
        .unwrap();
    drop(table);
    let table = store.materialize_table(&evolved).unwrap();
    assert_eq!(
        table.get(&key).unwrap(),
        Some(vec![
            Value::Int64(1),
            Value::List(vec![Value::Int64(1), Value::Int64(2)]),
            Value::Null
        ])
    );
    table
        .append(&key, "extra", &[Value::String("x".into())])
        .unwrap();
    store.snapshot_and_wait().unwrap();
    let transformed = catalog
        .evolve_schema(
            &id,
            vec![SchemaChange::TransformField {
                field_name: "history".into(),
                logical_type: LogicalType::list(nested_type()).nullable(),
                transform: cobble::TransformSpec {
                    transform_type: "wrap-int".into(),
                    spec: Bytes::new(),
                },
            }],
        )
        .unwrap();
    let cobble_table::LogicalTypeKind::List { element_type } =
        &transformed.schema().fields[1].logical_type.kind
    else {
        unreachable!()
    };
    let cobble_table::LogicalTypeKind::Struct { fields } = &element_type.kind else {
        unreachable!()
    };
    assert!(fields[0].id.0 > 2);
    drop(table);
    let table = store.materialize_table(&transformed).unwrap();
    table
        .append(&key, "history", &[Value::Struct(vec![Value::Int64(3)])])
        .unwrap();
    table
        .append(&key, "extra", &[Value::String("y".into())])
        .unwrap();
    let expected = vec![
        Value::Int64(1),
        Value::List(vec![
            Value::Struct(vec![Value::Int64(1)]),
            Value::Struct(vec![Value::Int64(2)]),
            Value::Struct(vec![Value::Int64(3)]),
        ]),
        Value::List(vec![Value::String("x".into()), Value::String("y".into())]),
    ];
    assert_eq!(table.get(&key).unwrap(), Some(expected.clone()));
    let latest = store.snapshot_and_wait().unwrap();
    drop(table);
    store.switch_to_snapshot(old.snapshot_id).unwrap();
    assert_eq!(
        store
            .open_materialized_table(&transformed)
            .unwrap()
            .get(&key)
            .unwrap(),
        Some(row(list(&[1, 2])))
    );
    store.switch_to_snapshot(latest.snapshot_id).unwrap();
    assert_eq!(
        store
            .open_materialized_table(&transformed)
            .unwrap()
            .get(&key)
            .unwrap(),
        Some(expected.clone())
    );
    store.close().unwrap();
    drop(store);

    let planner = cobble::DedicatedCompactionPlanner::open_with_resolver(
        config.clone(),
        "catalog-shard",
        Some(table_merge_operator_resolver(None)),
    )
    .unwrap();
    register_schema_transforms(&planner).unwrap();
    planner
        .register_schema_transform("wrap-int", wrap_int_factory)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let plan = loop {
        if let cobble::DedicatedCompactionPlanning::Plan(plan) = planner.plan().unwrap() {
            break plan;
        }
        assert!(Instant::now() < deadline, "no List compaction plan");
        std::thread::sleep(Duration::from_millis(20));
    };
    let executor = cobble::DedicatedCompactionExecutor::open_with_resolver(
        config.clone(),
        Some(table_merge_operator_resolver(None)),
    )
    .unwrap();
    register_schema_transforms(&executor).unwrap();
    executor
        .register_schema_transform("wrap-int", wrap_int_factory)
        .unwrap();
    assert!(matches!(
        executor.execute(&plan).unwrap(),
        cobble::DedicatedCompactionExecution::ResultPublished { .. }
    ));
    let result = root.path().join(format!(
        "catalog-shard/compaction/results/COMPACTION-{}",
        plan.job_id()
    ));
    assert!(result.exists());
    let published: serde_json::Value = serde_json::from_slice(
        &cobble::test_utils::read_metadata_payload_from_path_for_test(&result).unwrap(),
    )
    .unwrap();
    let output_paths = published["operation"]["Rewrite"]["outputs"]
        .as_array()
        .expect("List compaction must rewrite inputs")
        .iter()
        .map(|file| file["path"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert!(!output_paths.is_empty());
    let store = make_store()
        .resume_from_snapshot(latest.snapshot_id)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while result.exists() {
        assert!(Instant::now() < deadline, "writer did not adopt compaction");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        store
            .open_materialized_table(&transformed)
            .unwrap()
            .get(&key)
            .unwrap(),
        Some(expected)
    );
    let compacted = store.snapshot_and_wait().unwrap();
    let manifest_path = url::Url::parse(&compacted.manifest_path)
        .unwrap()
        .to_file_path()
        .unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(
        &cobble::test_utils::read_metadata_payload_from_path_for_test(&manifest_path).unwrap(),
    )
    .unwrap();
    let durable_paths = manifest["tree_levels"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|tree| tree.as_array().unwrap())
        .flat_map(|level| level["files"].as_array().unwrap())
        .map(|file| file["path"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(
        output_paths
            .iter()
            .all(|path| durable_paths.contains(&path.as_str())),
        "dedicated outputs were not adopted"
    );
    store.close().unwrap();
}
