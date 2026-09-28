use cobble::{Config, VolumeDescriptor, VolumeUsageKind};
use cobble_table::catalog::{
    Catalog, CatalogError, CatalogResult, CatalogRuntimeContext, CatalogSchemaId,
    CatalogSchemaStore, CatalogSchemaVersion, CatalogTable, SchemaChange, ShardSchemaMapping,
    TableId, TableIdentifier,
};
use cobble_table::{LogicalType, TableScanPlan, TableSchema, TableWritePlan, Value};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct MemoryState {
    next_table_id: u32,
    namespaces: BTreeSet<Vec<String>>,
    tables: HashMap<TableIdentifier, TableId>,
    versions: HashMap<TableId, Vec<CatalogSchemaVersion>>,
    mappings: Vec<ShardSchemaMapping>,
}

struct MemorySchemaStore(Arc<Mutex<MemoryState>>);

impl CatalogSchemaStore for MemorySchemaStore {
    fn load_schema_version(
        &self,
        table_id: TableId,
        schema_id: CatalogSchemaId,
    ) -> CatalogResult<CatalogSchemaVersion> {
        self.0
            .lock()
            .unwrap()
            .versions
            .get(&table_id)
            .and_then(|versions| versions.get(schema_id.as_u32() as usize))
            .cloned()
            .ok_or_else(|| CatalogError::InvalidMetadata("missing schema version".into()))
    }

    fn record_shard_schema_mapping(&self, mapping: ShardSchemaMapping) -> CatalogResult<()> {
        let mut state = self.0.lock().unwrap();
        state.mappings.retain(|previous| {
            previous.table_id() != mapping.table_id()
                || previous.db_id() != mapping.db_id()
                || previous.catalog_schema_id() != mapping.catalog_schema_id()
        });
        state.mappings.push(mapping);
        Ok(())
    }
}

struct MemoryCatalog {
    state: Arc<Mutex<MemoryState>>,
    context: Arc<CatalogRuntimeContext>,
}

impl MemoryCatalog {
    fn new(config: Config, storage_id: &str) -> (Arc<Self>, Arc<MemorySchemaStore>) {
        let state = Arc::new(Mutex::new(MemoryState {
            next_table_id: 1,
            ..MemoryState::default()
        }));
        let store = Arc::new(MemorySchemaStore(Arc::clone(&state)));
        let context =
            Arc::new(CatalogRuntimeContext::new(config, storage_id, store.clone()).unwrap());
        (Arc::new(Self { state, context }), store)
    }

    fn table(
        &self,
        state: &MemoryState,
        identifier: &TableIdentifier,
    ) -> CatalogResult<CatalogTable> {
        let table_id = *state
            .tables
            .get(identifier)
            .ok_or_else(|| CatalogError::TableNotFound(identifier.clone()))?;
        let version = state.versions[&table_id].last().unwrap().clone();
        CatalogTable::new(identifier.clone(), version, Arc::clone(&self.context))
    }
}

impl Catalog for MemoryCatalog {
    fn create_namespace(&self, namespace: Vec<String>) -> CatalogResult<()> {
        if namespace.is_empty() || namespace.iter().any(String::is_empty) {
            return Err(CatalogError::InvalidIdentifier("empty namespace".into()));
        }
        if !self
            .state
            .lock()
            .unwrap()
            .namespaces
            .insert(namespace.clone())
        {
            return Err(CatalogError::NamespaceAlreadyExists(namespace));
        }
        Ok(())
    }

    fn list_namespaces(&self) -> CatalogResult<Vec<Vec<String>>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .namespaces
            .iter()
            .cloned()
            .collect())
    }

    fn drop_namespace(&self, namespace: &[String]) -> CatalogResult<()> {
        let mut state = self.state.lock().unwrap();
        if !state.namespaces.contains(namespace) {
            return Err(CatalogError::NamespaceNotFound(namespace.to_vec()));
        }
        if state
            .tables
            .keys()
            .any(|identifier| identifier.namespace() == namespace)
        {
            return Err(CatalogError::NamespaceNotEmpty(namespace.to_vec()));
        }
        state.namespaces.remove(namespace);
        Ok(())
    }

    fn create_table(
        &self,
        identifier: TableIdentifier,
        schema: TableSchema,
    ) -> CatalogResult<CatalogTable> {
        let mut state = self.state.lock().unwrap();
        if !state.namespaces.contains(identifier.namespace()) {
            return Err(CatalogError::NamespaceNotFound(
                identifier.namespace().to_vec(),
            ));
        }
        if state.tables.contains_key(&identifier) {
            return Err(CatalogError::TableAlreadyExists(identifier));
        }
        let table_id = TableId::new(state.next_table_id);
        let version = CatalogSchemaVersion::initial(table_id, schema)?;
        let table = CatalogTable::new(
            identifier.clone(),
            version.clone(),
            Arc::clone(&self.context),
        )?;
        state.next_table_id += 1;
        state.tables.insert(identifier, table_id);
        state.versions.insert(table_id, vec![version]);
        Ok(table)
    }

    fn load_table(&self, identifier: &TableIdentifier) -> CatalogResult<CatalogTable> {
        self.table(&self.state.lock().unwrap(), identifier)
    }

    fn load_table_schema(
        &self,
        identifier: &TableIdentifier,
        catalog_schema_id: CatalogSchemaId,
    ) -> CatalogResult<TableSchema> {
        let state = self.state.lock().unwrap();
        let table_id = *state
            .tables
            .get(identifier)
            .ok_or_else(|| CatalogError::TableNotFound(identifier.clone()))?;
        state.versions[&table_id]
            .get(catalog_schema_id.as_u32() as usize)
            .map(|version| version.schema().clone())
            .ok_or_else(|| CatalogError::SchemaNotFound {
                table: identifier.clone(),
                catalog_schema_id,
            })
    }

    fn evolve_schema(
        &self,
        identifier: &TableIdentifier,
        changes: Vec<SchemaChange>,
    ) -> CatalogResult<CatalogTable> {
        let mut state = self.state.lock().unwrap();
        let table_id = *state
            .tables
            .get(identifier)
            .ok_or_else(|| CatalogError::TableNotFound(identifier.clone()))?;
        let versions = state.versions.get_mut(&table_id).unwrap();
        let next = versions.last().unwrap().evolve(changes)?;
        let table = CatalogTable::new(identifier.clone(), next.clone(), Arc::clone(&self.context))?;
        versions.push(next);
        Ok(table)
    }

    fn list_tables(&self, namespace: &[String]) -> CatalogResult<Vec<TableIdentifier>> {
        let state = self.state.lock().unwrap();
        if !state.namespaces.contains(namespace) {
            return Err(CatalogError::NamespaceNotFound(namespace.to_vec()));
        }
        let mut tables = state
            .tables
            .keys()
            .filter(|identifier| identifier.namespace() == namespace)
            .cloned()
            .collect::<Vec<_>>();
        tables.sort_by(|left, right| left.name().cmp(right.name()));
        Ok(tables)
    }

    fn table_exists(&self, identifier: &TableIdentifier) -> CatalogResult<bool> {
        Ok(self.state.lock().unwrap().tables.contains_key(identifier))
    }

    fn rename_table(
        &self,
        identifier: &TableIdentifier,
        new_name: String,
    ) -> CatalogResult<CatalogTable> {
        let mut state = self.state.lock().unwrap();
        let table_id = *state
            .tables
            .get(identifier)
            .ok_or_else(|| CatalogError::TableNotFound(identifier.clone()))?;
        let renamed = TableIdentifier::new(identifier.namespace().to_vec(), new_name);
        if state.tables.contains_key(&renamed) {
            return Err(CatalogError::TableAlreadyExists(renamed));
        }
        let version = state.versions[&table_id].last().unwrap().clone();
        let table = CatalogTable::new(renamed.clone(), version, Arc::clone(&self.context))?;
        state.tables.remove(identifier);
        state.tables.insert(renamed, table_id);
        Ok(table)
    }

    fn drop_table(&self, identifier: &TableIdentifier) -> CatalogResult<()> {
        self.state
            .lock()
            .unwrap()
            .tables
            .remove(identifier)
            .map(|_| ())
            .ok_or_else(|| CatalogError::TableNotFound(identifier.clone()))
    }
}

fn round_trip<T: Serialize + DeserializeOwned>(value: &T) -> T {
    serde_json::from_slice(&serde_json::to_vec(value).unwrap()).unwrap()
}

fn row_id(row: &[Value]) -> i64 {
    let Value::Int64(id) = row[0] else {
        panic!("row id must be int64");
    };
    id
}

fn scan_rows(plan: &TableScanPlan, runtime: &Config) -> Vec<Vec<Value>> {
    let mut rows = plan
        .splits()
        .unwrap()
        .into_iter()
        .map(|split| round_trip(&split))
        .flat_map(|split| split.create_scanner(runtime.clone()).unwrap())
        .collect::<cobble_table::Result<Vec<_>>>()
        .unwrap();
    rows.sort_by_key(|row| row_id(row));
    rows
}

#[test]
fn external_catalog_lifecycle_and_two_shard_transport_without_file_catalog() {
    let root = tempfile::tempdir().unwrap();
    let warehouse = root.path().join("warehouse");
    let shared = Config {
        volumes: vec![VolumeDescriptor::new(
            format!("file://{}", warehouse.display()),
            vec![
                VolumeUsageKind::Meta,
                VolumeUsageKind::Snapshot,
                VolumeUsageKind::Wal,
            ],
        )],
        total_buckets: 2,
        ..Config::default()
    };
    let runtime = Config {
        volumes: VolumeDescriptor::single_volume(format!(
            "file://{}",
            root.path().join("local").display()
        )),
        total_buckets: 2,
        ..Config::default()
    };
    let (backend, store) = MemoryCatalog::new(shared, "warehouse");
    let catalog: Arc<dyn Catalog> = backend.clone();
    let namespace = vec!["app".to_string()];
    let scratch = vec!["scratch".to_string()];
    assert!(catalog.list_namespaces().unwrap().is_empty());
    catalog.create_namespace(namespace.clone()).unwrap();
    catalog.create_namespace(scratch.clone()).unwrap();
    assert!(matches!(
        catalog.create_namespace(namespace.clone()),
        Err(CatalogError::NamespaceAlreadyExists(_))
    ));
    let temporary = TableIdentifier::new(scratch.clone(), "temporary");
    let schema = TableSchema::builder()
        .field("id", LogicalType::int64())
        .field("score", LogicalType::int32().nullable())
        .primary_key(["id"])
        .bucket_key(["id"])
        .build()
        .unwrap();
    let first_temporary = catalog
        .create_table(temporary.clone(), schema.clone())
        .unwrap();
    assert!(catalog.table_exists(&temporary).unwrap());
    assert!(matches!(
        catalog.drop_namespace(&scratch),
        Err(CatalogError::NamespaceNotEmpty(_))
    ));
    let renamed_id = TableIdentifier::new(scratch.clone(), "renamed");
    let renamed = catalog.rename_table(&temporary, "renamed".into()).unwrap();
    assert_eq!(renamed.table_id(), first_temporary.table_id());
    assert!(!catalog.table_exists(&temporary).unwrap());
    assert_eq!(
        catalog.list_tables(&scratch).unwrap(),
        vec![renamed_id.clone()]
    );
    catalog.drop_table(&renamed_id).unwrap();
    let recreated = catalog
        .create_table(temporary.clone(), schema.clone())
        .unwrap();
    assert_ne!(recreated.table_id(), first_temporary.table_id());
    catalog.drop_table(&temporary).unwrap();
    catalog.drop_namespace(&scratch).unwrap();
    assert_eq!(catalog.list_namespaces().unwrap(), vec![namespace.clone()]);

    let identifier = TableIdentifier::new(namespace.clone(), "users");
    assert!(matches!(
        catalog.load_table(&identifier),
        Err(CatalogError::TableNotFound(_))
    ));
    let initial = catalog
        .create_table(identifier.clone(), schema.clone())
        .unwrap();
    assert!(matches!(
        catalog.create_table(identifier.clone(), schema.clone()),
        Err(CatalogError::TableAlreadyExists(_))
    ));
    assert_eq!(
        catalog.load_table(&identifier).unwrap().table_id(),
        initial.table_id()
    );
    assert_eq!(
        catalog.list_tables(&namespace).unwrap(),
        vec![identifier.clone()]
    );
    assert_eq!(
        catalog
            .load_table_schema(&identifier, CatalogSchemaId::from(0))
            .unwrap(),
        schema
    );

    // These JSON round trips simulate transport boundaries in one process, not a multiprocess run.
    let plan_v0: TableWritePlan = round_trip(&initial.new_write_builder().build().unwrap());
    let mut writers = [
        plan_v0
            .writer_builder(runtime.clone())
            .unwrap()
            .bucket(0)
            .open()
            .unwrap(),
        plan_v0
            .writer_builder(runtime.clone())
            .unwrap()
            .bucket(1)
            .open()
            .unwrap(),
    ];
    let mut keys = Vec::new();
    let mut rows = Vec::new();
    let mut bucket_counts = [0usize; 2];
    for id in 0..64i64 {
        let mut builder = writers[0].key_builder();
        builder.push(Value::Int64(id));
        let key = builder.build().unwrap();
        let bucket = usize::from(key.bucket());
        let row = vec![Value::Int64(id), Value::Int32(id as i32 + 10)];
        writers[bucket].put(&row).unwrap();
        bucket_counts[bucket] += 1;
        keys.push(key);
        rows.push(row);
    }
    assert!(bucket_counts.iter().all(|count| *count > 0));
    let old_snapshots = writers
        .iter()
        .map(|writer| round_trip(&writer.snapshot_and_wait().unwrap()))
        .collect::<Vec<_>>();
    let committer = initial.snapshot_committer(runtime.clone(), 4).unwrap();
    let first_global = committer
        .commit_batch(1, old_snapshots.clone())
        .unwrap()
        .unwrap();
    let mut reader_runtime = runtime.clone();
    reader_runtime.reader.reload_tolerance_seconds = 3600;
    let latest_reader = initial
        .reader_builder(reader_runtime.clone())
        .unwrap()
        .current_global_snapshot()
        .open()
        .unwrap();
    let fixed_reader = initial
        .reader_builder(reader_runtime)
        .unwrap()
        .global_snapshot(first_global.id)
        .open()
        .unwrap();
    assert_eq!(
        latest_reader.multi_get(&keys).unwrap(),
        rows.iter().cloned().map(Some).collect::<Vec<_>>()
    );
    let original_scan: TableScanPlan = round_trip(&latest_reader.scan_plan().unwrap());
    assert_eq!(scan_rows(&original_scan, &runtime), rows);

    let widened = catalog
        .evolve_schema(
            &identifier,
            vec![SchemaChange::AlterFieldType {
                field_name: "score".into(),
                logical_type: LogicalType::int64().nullable(),
            }],
        )
        .unwrap();
    assert_eq!(
        catalog
            .load_table_schema(&identifier, CatalogSchemaId::from(0))
            .unwrap(),
        schema
    );
    assert!(widened.refresh_writer(&mut writers[0]).unwrap());
    assert!(store.0.lock().unwrap().mappings.iter().any(|mapping| {
        mapping.table_id() == initial.table_id()
            && mapping.catalog_schema_id() == CatalogSchemaId::from(1)
            && mapping.db_id() == "bucket-0"
    }));
    let latest = catalog
        .evolve_schema(
            &identifier,
            vec![SchemaChange::AddField {
                name: "label".into(),
                logical_type: LogicalType::string().nullable(),
            }],
        )
        .unwrap();
    assert_eq!(
        catalog
            .load_table_schema(&identifier, CatalogSchemaId::from(1))
            .unwrap(),
        widened.schema().clone()
    );
    assert!(matches!(
        catalog.load_table_schema(&identifier, CatalogSchemaId::from(3)),
        Err(CatalogError::SchemaNotFound { .. })
    ));
    assert!(!latest_reader.refresh().unwrap());
    assert_eq!(latest_reader.schema().as_ref(), &schema);
    drop(writers);

    let plan_v2 = latest.new_write_builder().build().unwrap();
    let payload = serde_json::to_vec(&plan_v2).unwrap();
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
    let latest_schema = latest.schema().clone();
    let weak_store = Arc::downgrade(&store);
    drop((
        plan_v0,
        plan_v2,
        initial,
        widened,
        latest,
        recreated,
        renamed,
        first_temporary,
        catalog,
        backend,
        store,
    ));
    assert!(weak_store.upgrade().is_none());
    assert!(!warehouse.join("warehouse/catalog").exists());

    // Both detached workers replay two schema transitions from their original shard snapshots.
    let detached: TableWritePlan = serde_json::from_slice(&payload).unwrap();
    let mut expected = rows
        .iter()
        .map(|row| vec![row[0].clone(), Value::Int64(row_id(row) + 10), Value::Null])
        .collect::<Vec<_>>();
    let mut new_snapshots = Vec::new();
    for bucket in 0..2u16 {
        let writer = detached
            .writer_builder(runtime.clone())
            .unwrap()
            .bucket(bucket)
            .resume_from_snapshot(old_snapshots[usize::from(bucket)].snapshot_id)
            .unwrap();
        assert_eq!(writer.schema(), &latest_schema);
        let index = keys.iter().position(|key| key.bucket() == bucket).unwrap();
        expected[index] = vec![
            rows[index][0].clone(),
            Value::Int64(1000 + row_id(&rows[index])),
            Value::String("updated".into()),
        ];
        writer.put(&expected[index]).unwrap();
        new_snapshots.push(round_trip(&writer.snapshot_and_wait().unwrap()));
    }
    committer.commit_batch(2, new_snapshots).unwrap().unwrap();
    assert_eq!(latest_reader.schema().as_ref(), &schema);
    assert!(latest_reader.refresh().unwrap());
    assert_eq!(latest_reader.schema().as_ref(), &latest_schema);
    assert_eq!(
        latest_reader.multi_get(&keys).unwrap(),
        expected.iter().cloned().map(Some).collect::<Vec<_>>()
    );
    assert_eq!(
        scan_rows(&round_trip(&latest_reader.scan_plan().unwrap()), &runtime),
        expected
    );
    assert!(!fixed_reader.refresh().unwrap());
    assert_eq!(fixed_reader.schema().as_ref(), &schema);
    assert_eq!(
        fixed_reader.multi_get(&keys).unwrap(),
        rows.iter().cloned().map(Some).collect::<Vec<_>>()
    );
    assert_eq!(scan_rows(&original_scan, &runtime), rows);
    assert!(!warehouse.join("warehouse/catalog").exists());
}
