use cobble::{Config, VolumeDescriptor, VolumeUsageKind};
use cobble_table::catalog::{
    CatalogResult, CatalogRuntimeContext, CatalogSchemaId, CatalogSchemaStore,
    CatalogSchemaVersion, CatalogTable, SchemaChange, ShardSchemaMapping, TableId, TableIdentifier,
};
use cobble_table::{LogicalType, TableSchema, TableWritePlan, Value};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct MemorySchemaStore {
    versions: Mutex<Vec<CatalogSchemaVersion>>,
    mappings: Mutex<Vec<ShardSchemaMapping>>,
}

impl CatalogSchemaStore for MemorySchemaStore {
    fn load_schema_version(
        &self,
        table_id: TableId,
        schema_id: CatalogSchemaId,
    ) -> CatalogResult<CatalogSchemaVersion> {
        let version = self.versions.lock().unwrap()[schema_id.as_u32() as usize].clone();
        assert_eq!(version.table_id(), table_id);
        Ok(version)
    }

    fn record_shard_schema_mapping(&self, mapping: ShardSchemaMapping) -> CatalogResult<()> {
        let mut mappings = self.mappings.lock().unwrap();
        mappings.retain(|previous| {
            previous.table_id() != mapping.table_id()
                || previous.db_id() != mapping.db_id()
                || previous.catalog_schema_id() != mapping.catalog_schema_id()
        });
        mappings.push(mapping);
        Ok(())
    }
}

#[test]
fn external_catalog_plan_resumes_old_shard_without_a_live_backend() {
    let root = tempfile::tempdir().unwrap();
    let warehouse = root.path().join("warehouse");
    let shared = Config {
        volumes: vec![VolumeDescriptor::new(
            format!("file://{}", warehouse.display()),
            vec![VolumeUsageKind::Meta, VolumeUsageKind::Snapshot],
        )],
        total_buckets: 1,
        ..Config::default()
    };
    let runtime = Config {
        volumes: VolumeDescriptor::single_volume(format!(
            "file://{}",
            root.path().join("local").display()
        )),
        total_buckets: 1,
        ..Config::default()
    };
    let store = Arc::new(MemorySchemaStore::default());
    let context = Arc::new(CatalogRuntimeContext::new(shared, "warehouse", store.clone()).unwrap());
    let identifier = TableIdentifier::new(["app"], "users");
    let initial = CatalogSchemaVersion::initial(
        TableId::new(7),
        TableSchema::builder()
            .field("id", LogicalType::int64())
            .field("score", LogicalType::int32().nullable())
            .primary_key(["id"])
            .bucket_key(["id"])
            .build()
            .unwrap(),
    )
    .unwrap();
    store.versions.lock().unwrap().push(initial.clone());
    let table = CatalogTable::new(identifier.clone(), initial.clone(), context.clone()).unwrap();
    let mut writer = table
        .writer_builder(runtime.clone())
        .unwrap()
        .bucket(0)
        .open()
        .unwrap();
    let mut keys = writer.key_builder();
    keys.push(Value::Int64(1));
    let key = keys.build().unwrap();
    writer.put(&[Value::Int64(1), Value::Int32(42)]).unwrap();
    let old_snapshot = writer.snapshot_and_wait().unwrap();

    // A connected table uses the external store for evolution and mapping reports.
    let widened = initial
        .evolve(vec![SchemaChange::AlterFieldType {
            field_name: "score".into(),
            logical_type: LogicalType::int64().nullable(),
        }])
        .unwrap();
    store.versions.lock().unwrap().push(widened.clone());
    let evolved = CatalogTable::new(identifier.clone(), widened.clone(), context.clone()).unwrap();
    assert!(evolved.refresh_writer(&mut writer).unwrap());
    assert_eq!(
        writer.get(&key).unwrap(),
        Some(vec![Value::Int64(1), Value::Int64(42)])
    );
    assert!(store.mappings.lock().unwrap().iter().any(|mapping| {
        mapping.table_id() == initial.table_id()
            && mapping.catalog_schema_id() == widened.catalog_schema_id()
            && mapping.db_id() == "bucket-0"
    }));
    drop(writer);

    let latest = widened
        .evolve(vec![SchemaChange::AddField {
            name: "label".into(),
            logical_type: LogicalType::string().nullable(),
        }])
        .unwrap();
    store.versions.lock().unwrap().push(latest.clone());
    let current = CatalogTable::new(identifier, latest.clone(), context.clone()).unwrap();
    let plan = current.new_write_builder().build().unwrap();
    let payload = serde_json::to_vec(&plan).unwrap();
    // Reject incomplete or mixed-table histories before opening a writer.
    let encoded: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    for malformed in [
        serde_json::json!([]),
        serde_json::json!([encoded["schema_history"][0], encoded["schema_history"][2]]),
        {
            let mut history = encoded["schema_history"].clone();
            history[1]["table_id"] = serde_json::json!(99);
            history
        },
    ] {
        let mut invalid = encoded.clone();
        invalid["schema_history"] = malformed;
        let invalid: TableWritePlan = serde_json::from_value(invalid).unwrap();
        assert!(invalid.writer_builder(runtime.clone()).is_err());
    }
    let weak_store = Arc::downgrade(&store);
    drop((plan, current, evolved, table, context, store));
    assert!(weak_store.upgrade().is_none());

    // This shard predates both changes. The detached worker must replay the full chain,
    // with no file catalog directory or live store available.
    assert!(!warehouse.join("warehouse/catalog").exists());
    let detached: TableWritePlan = serde_json::from_slice(&payload).unwrap();
    let resumed = detached
        .writer_builder(runtime.clone())
        .unwrap()
        .bucket(0)
        .resume_from_snapshot(old_snapshot.snapshot_id)
        .unwrap();
    assert_eq!(resumed.schema(), latest.schema());
    assert_eq!(
        resumed.get(&key).unwrap(),
        Some(vec![Value::Int64(1), Value::Int64(42), Value::Null])
    );
    let snapshot = resumed.snapshot_and_wait().unwrap();
    drop(resumed);
    let reopened = detached
        .writer_builder(runtime)
        .unwrap()
        .bucket(0)
        .resume_from_snapshot(snapshot.snapshot_id)
        .unwrap();
    assert_eq!(
        reopened.get(&key).unwrap(),
        Some(vec![Value::Int64(1), Value::Int64(42), Value::Null])
    );
    assert!(!warehouse.join("warehouse/catalog").exists());
}
