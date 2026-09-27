use super::runtime::CatalogRuntimeContext;
use super::store::CatalogStore;
use crate::catalog::{
    Catalog, CatalogError, CatalogResult, CatalogSchemaId, CatalogSchemaVersion, CatalogTable,
    SchemaChange, ShardSchemaMapping, TableId, TableIdentifier, physical_table_name,
};
use crate::evolution::schema_field_ids;
use crate::metadata::TableMetadata;
use crate::{Table, TableError, TableSchema, TableWritePlan};
use cobble::{Config, Db};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use uuid::Uuid;

const CATALOG_FORMAT: &str = "cobble-table-catalog";
const CATALOG_VERSION: u32 = 1;

/// Runtime-only configuration for a file catalog.
///
/// Volume descriptors and credentials remain in [`cobble::Config`] and are never written into
/// catalog metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileCatalogConfig {
    storage_id: String,
}

impl FileCatalogConfig {
    pub fn new(storage_id: impl Into<String>) -> Self {
        Self {
            storage_id: storage_id.into(),
        }
    }

    pub fn storage_id(&self) -> &str {
        &self.storage_id
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CurrentPointer {
    format: String,
    version: u32,
    generation: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CatalogManifest {
    format: String,
    version: u32,
    generation: u64,
    next_table_id: u32,
    namespaces: Vec<NamespaceEntry>,
}

impl CatalogManifest {
    fn empty() -> Self {
        Self {
            format: CATALOG_FORMAT.to_string(),
            version: CATALOG_VERSION,
            generation: 0,
            next_table_id: 1,
            namespaces: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NamespaceEntry {
    namespace: Vec<String>,
    namespace_id: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NamespaceManifest {
    format: String,
    version: u32,
    generation: u64,
    namespace: Vec<String>,
    tables: Vec<TableEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableEntry {
    name: String,
    table_id: TableId,
    catalog_schema_id: CatalogSchemaId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableIdentity {
    format: String,
    version: u32,
    table_id: TableId,
    physical_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableSchemaRecord {
    format: String,
    version: u32,
    #[serde(flatten)]
    schema_version: CatalogSchemaVersion,
}

impl TableSchemaRecord {
    fn new(schema_version: CatalogSchemaVersion) -> Self {
        Self {
            format: CATALOG_FORMAT.to_string(),
            version: CATALOG_VERSION,
            schema_version,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ShardSchemaMappingFile {
    format: String,
    version: u32,
    table_id: TableId,
    db_id: String,
    catalog_schema_id: CatalogSchemaId,
    core_schema_id: u64,
}

/// File-backed table catalog.
///
/// Operations are serialized across handles in this process and refresh CURRENT before every
/// read or mutation. One active catalog mutator is still required across processes; distributed
/// locking is intentionally outside this implementation.
pub struct FileCatalog {
    store: CatalogStore,
    runtime_context: Arc<CatalogRuntimeContext>,
    state: Mutex<CatalogManifest>,
    operation_lock: Arc<Mutex<()>>,
}

impl FileCatalog {
    pub fn open(config: &Config, catalog_config: FileCatalogConfig) -> CatalogResult<Self> {
        let store = CatalogStore::open(config, catalog_config.storage_id())?;
        let manifest = load_catalog_manifest(&store)?;
        let operation_lock = process_operation_lock(catalog_config.storage_id())?;
        let runtime_context = Arc::new(CatalogRuntimeContext {
            config: config.clone(),
            storage_id: catalog_config.storage_id().to_string(),
        });
        Ok(Self {
            store,
            runtime_context,
            state: Mutex::new(manifest),
            operation_lock,
        })
    }

    /// Materialize the current catalog schema into one writable shard.
    ///
    /// Calls for a shard must not run concurrently with other core schema updates.
    pub fn materialize_table(
        &self,
        db: Arc<Db>,
        identifier: &TableIdentifier,
    ) -> CatalogResult<Table> {
        let table = self.load_table(identifier)?;
        let (physical_name, target) = materialize_loaded_table(&self.store, db.as_ref(), &table)?;
        Table::from_metadata(db, physical_name, target).map_err(Into::into)
    }

    fn with_current<T>(
        &self,
        operation: impl FnOnce(&mut CatalogManifest) -> CatalogResult<T>,
    ) -> CatalogResult<T> {
        let _operation = lock(&self.operation_lock)?;
        let mut manifest = lock(&self.state)?;
        *manifest = load_catalog_manifest(&self.store)?;
        operation(&mut manifest)
    }

    fn namespace_entry<'a>(
        manifest: &'a CatalogManifest,
        namespace: &[String],
    ) -> Option<&'a NamespaceEntry> {
        manifest
            .namespaces
            .iter()
            .find(|entry| entry.namespace == namespace)
    }

    fn required_namespace<'a>(
        manifest: &'a CatalogManifest,
        namespace: &[String],
    ) -> CatalogResult<&'a NamespaceEntry> {
        Self::namespace_entry(manifest, namespace)
            .ok_or_else(|| CatalogError::NamespaceNotFound(namespace.to_vec()))
    }

    fn load_namespace(&self, entry: &NamespaceEntry) -> CatalogResult<NamespaceManifest> {
        let prefix = namespace_prefix(entry.namespace_id);
        let current: CurrentPointer = read_json(&self.store, &format!("{prefix}/CURRENT"))?;
        validate_header(&current.format, current.version)?;
        let manifest: NamespaceManifest = read_json(
            &self.store,
            &format!("{prefix}/NAMESPACE-{}", current.generation),
        )?;
        validate_header(&manifest.format, manifest.version)?;
        if manifest.generation != current.generation || manifest.namespace != entry.namespace {
            return Err(CatalogError::InvalidMetadata(
                "namespace manifest does not match CURRENT or catalog entry".to_string(),
            ));
        }
        Ok(manifest)
    }

    fn load_identity(&self, table_id: TableId) -> CatalogResult<TableIdentity> {
        let identity: TableIdentity = read_json(&self.store, &table_identity_path(table_id))?;
        validate_header(&identity.format, identity.version)?;
        debug_assert_eq!(identity.table_id, table_id);
        debug_assert_eq!(identity.physical_name, physical_table_name(table_id));
        Ok(identity)
    }

    fn load_schema(
        &self,
        table_id: TableId,
        catalog_schema_id: CatalogSchemaId,
    ) -> CatalogResult<TableSchemaRecord> {
        load_table_schema_record(&self.store, table_id, catalog_schema_id)
    }

    fn catalog_table(
        &self,
        identifier: TableIdentifier,
        identity: TableIdentity,
        schema: TableSchemaRecord,
    ) -> CatalogTable {
        CatalogTable {
            identifier,
            table_id: identity.table_id,
            catalog_schema_id: schema.schema_version.catalog_schema_id(),
            schema: schema.schema_version.schema().clone(),
            runtime_context: Arc::clone(&self.runtime_context),
        }
    }

    fn commit_catalog(
        &self,
        current: &mut CatalogManifest,
        next: CatalogManifest,
    ) -> CatalogResult<()> {
        write_json(&self.store, &format!("CATALOG-{}", next.generation), &next)?;
        write_json(
            &self.store,
            "CURRENT",
            &CurrentPointer {
                format: CATALOG_FORMAT.to_string(),
                version: CATALOG_VERSION,
                generation: next.generation,
            },
        )?;
        *current = next;
        Ok(())
    }

    fn commit_namespace(
        &self,
        entry: &NamespaceEntry,
        manifest: &NamespaceManifest,
    ) -> CatalogResult<()> {
        let prefix = namespace_prefix(entry.namespace_id);
        write_json(
            &self.store,
            &format!("{prefix}/NAMESPACE-{}", manifest.generation),
            manifest,
        )?;
        write_json(
            &self.store,
            &format!("{prefix}/CURRENT"),
            &CurrentPointer {
                format: CATALOG_FORMAT.to_string(),
                version: CATALOG_VERSION,
                generation: manifest.generation,
            },
        )
    }
}

fn materialize_loaded_table(
    store: &CatalogStore,
    db: &Db,
    table: &CatalogTable,
) -> CatalogResult<(String, TableMetadata)> {
    materialize_with_store(
        store,
        db,
        table.table_id,
        table.catalog_schema_id,
        &table.schema,
    )
}

fn materialize_with_store(
    store: &CatalogStore,
    db: &Db,
    table_id: TableId,
    catalog_schema_id: CatalogSchemaId,
    schema: &TableSchema,
) -> CatalogResult<(String, TableMetadata)> {
    super::materialize::materialize_table_definition(
        db,
        table_id,
        catalog_schema_id,
        schema,
        |schema_id| Ok(load_table_schema_record(store, table_id, schema_id)?.schema_version),
        |schema_id, core_schema_id| {
            write_schema_mapping(
                store,
                shard_schema_mapping(table_id, db, schema_id, core_schema_id),
            )
        },
    )
}

pub(crate) fn materialize_catalog_table(table: &CatalogTable, db: Arc<Db>) -> CatalogResult<Table> {
    let store = CatalogStore::open(
        &table.runtime_context.config,
        &table.runtime_context.storage_id,
    )?;
    let (physical_name, target) = materialize_loaded_table(&store, db.as_ref(), table)?;
    Table::from_metadata(db, physical_name, target).map_err(Into::into)
}

pub(crate) fn refresh_catalog_table_writer(
    table: &CatalogTable,
    writer: &mut Table,
) -> CatalogResult<bool> {
    let physical_name = physical_table_name(table.table_id);
    if writer.name() != physical_name {
        return Err(TableError::InvalidSchema(
            "Table does not belong to this catalog table".to_string(),
        )
        .into());
    }
    let store = CatalogStore::open(
        &table.runtime_context.config,
        &table.runtime_context.storage_id,
    )?;
    materialize_loaded_table(&store, writer.db(), table)?;
    writer.refresh_schema().map_err(Into::into)
}

pub(crate) fn materialize_write_plan(
    store_config: &Config,
    db: &Db,
    plan: &TableWritePlan,
) -> crate::Result<(String, TableMetadata)> {
    plan.validate()?;
    let store = CatalogStore::open(store_config, &plan.storage_id)
        .map_err(|error| TableError::internal(error.to_string()))?;
    materialize_with_store(
        &store,
        db,
        plan.table_id,
        plan.catalog_schema_id,
        &plan.schema,
    )
    .map_err(|error| TableError::internal(error.to_string()))
}

fn shard_schema_mapping(
    table_id: TableId,
    db: &Db,
    catalog_schema_id: CatalogSchemaId,
    core_schema_id: u64,
) -> ShardSchemaMappingFile {
    ShardSchemaMappingFile {
        format: CATALOG_FORMAT.to_string(),
        version: CATALOG_VERSION,
        table_id,
        db_id: db.id().to_string(),
        catalog_schema_id,
        core_schema_id,
    }
}

fn write_schema_mapping(
    store: &CatalogStore,
    mapping: ShardSchemaMappingFile,
) -> CatalogResult<()> {
    let path = schema_mapping_path(mapping.table_id, &mapping.db_id, mapping.catalog_schema_id);
    if store.exists(&path)? {
        let existing: ShardSchemaMappingFile = read_json(store, &path)?;
        validate_header(&existing.format, existing.version)?;
        validate_schema_mapping_key(
            &existing,
            mapping.table_id,
            &mapping.db_id,
            mapping.catalog_schema_id,
        )?;
        if existing.core_schema_id <= mapping.core_schema_id {
            return Ok(());
        }
    }
    write_json(store, &path, &mapping)
}

fn load_table_schema_record(
    store: &CatalogStore,
    table_id: TableId,
    catalog_schema_id: CatalogSchemaId,
) -> CatalogResult<TableSchemaRecord> {
    let record: TableSchemaRecord =
        read_json(store, &table_schema_path(table_id, catalog_schema_id))?;
    validate_header(&record.format, record.version)?;
    if record.schema_version.table_id() != table_id
        || record.schema_version.catalog_schema_id() != catalog_schema_id
    {
        return Err(CatalogError::InvalidMetadata(
            "table schema record does not match its lookup key".to_string(),
        ));
    }
    #[cfg(debug_assertions)]
    {
        debug_assert!(record.schema_version.schema().validate().is_ok());
        debug_assert!(
            record
                .schema_version
                .used_field_ids()
                .windows(2)
                .all(|window| window[0] < window[1])
        );
        debug_assert!(
            schema_field_ids(record.schema_version.schema())
                .iter()
                .all(|field_id| record
                    .schema_version
                    .used_field_ids()
                    .binary_search(field_id)
                    .is_ok())
        );
    }
    Ok(record)
}

impl FileCatalog {
    /// Load the core schema id used for one table schema on a shard.
    pub fn load_shard_schema_mapping(
        &self,
        identifier: &TableIdentifier,
        db_id: &str,
        catalog_schema_id: CatalogSchemaId,
    ) -> CatalogResult<ShardSchemaMapping> {
        let table = self.load_table(identifier)?;
        if catalog_schema_id > table.catalog_schema_id {
            return Err(CatalogError::SchemaNotFound {
                table: identifier.clone(),
                catalog_schema_id,
            });
        }
        let mapping: ShardSchemaMappingFile = read_json(
            &self.store,
            &schema_mapping_path(table.table_id, db_id, catalog_schema_id),
        )?;
        validate_header(&mapping.format, mapping.version)?;
        validate_schema_mapping_key(&mapping, table.table_id, db_id, catalog_schema_id)?;
        Ok(ShardSchemaMapping {
            table_id: mapping.table_id,
            db_id: mapping.db_id,
            catalog_schema_id: mapping.catalog_schema_id,
            core_schema_id: mapping.core_schema_id,
        })
    }
}

impl Catalog for FileCatalog {
    fn create_namespace(&self, namespace: Vec<String>) -> CatalogResult<()> {
        validate_namespace(&namespace)?;
        self.with_current(|current| {
            if Self::namespace_entry(current, &namespace).is_some() {
                return Err(CatalogError::NamespaceAlreadyExists(namespace));
            }
            let entry = NamespaceEntry {
                namespace: namespace.clone(),
                namespace_id: Uuid::new_v4(),
            };
            self.commit_namespace(
                &entry,
                &NamespaceManifest {
                    format: CATALOG_FORMAT.to_string(),
                    version: CATALOG_VERSION,
                    generation: 1,
                    namespace,
                    tables: Vec::new(),
                },
            )?;
            let mut next = current.clone();
            next.generation += 1;
            next.namespaces.push(entry);
            next.namespaces
                .sort_by(|left, right| left.namespace.cmp(&right.namespace));
            self.commit_catalog(current, next)
        })
    }

    fn list_namespaces(&self) -> CatalogResult<Vec<Vec<String>>> {
        self.with_current(|current| {
            Ok(current
                .namespaces
                .iter()
                .map(|entry| entry.namespace.clone())
                .collect())
        })
    }

    fn drop_namespace(&self, namespace: &[String]) -> CatalogResult<()> {
        validate_namespace(namespace)?;
        self.with_current(|current| {
            let entry = Self::required_namespace(current, namespace)?.clone();
            if !self.load_namespace(&entry)?.tables.is_empty() {
                return Err(CatalogError::NamespaceNotEmpty(namespace.to_vec()));
            }
            let mut next = current.clone();
            next.generation += 1;
            next.namespaces
                .retain(|candidate| candidate.namespace != namespace);
            self.commit_catalog(current, next)
        })
    }

    fn create_table(
        &self,
        identifier: TableIdentifier,
        schema: TableSchema,
    ) -> CatalogResult<CatalogTable> {
        validate_identifier(&identifier)?;
        schema.validate()?;
        self.with_current(|current| {
            let namespace_entry =
                Self::required_namespace(current, identifier.namespace())?.clone();
            let mut namespace = self.load_namespace(&namespace_entry)?;
            if namespace
                .tables
                .iter()
                .any(|entry| entry.name == identifier.name())
            {
                return Err(CatalogError::TableAlreadyExists(identifier));
            }
            let table_id = TableId::new(current.next_table_id);
            let mut next = current.clone();
            next.generation += 1;
            next.next_table_id = next.next_table_id.checked_add(1).ok_or_else(|| {
                CatalogError::InvalidMetadata("table id space exhausted".to_string())
            })?;
            self.commit_catalog(current, next)?;
            let identity = TableIdentity {
                format: CATALOG_FORMAT.to_string(),
                version: CATALOG_VERSION,
                table_id,
                physical_name: physical_table_name(table_id),
            };
            write_json(&self.store, &table_identity_path(table_id), &identity)?;
            let schema = TableSchemaRecord::new(CatalogSchemaVersion::initial(table_id, schema)?);
            write_json(
                &self.store,
                &table_schema_path(table_id, schema.schema_version.catalog_schema_id()),
                &schema,
            )?;
            namespace.generation += 1;
            namespace.tables.push(TableEntry {
                name: identifier.name().to_string(),
                table_id,
                catalog_schema_id: schema.schema_version.catalog_schema_id(),
            });
            namespace
                .tables
                .sort_by(|left, right| left.name.cmp(&right.name));
            self.commit_namespace(&namespace_entry, &namespace)?;
            Ok(self.catalog_table(identifier, identity, schema))
        })
    }

    fn load_table(&self, identifier: &TableIdentifier) -> CatalogResult<CatalogTable> {
        validate_identifier(identifier)?;
        self.with_current(|current| {
            let namespace_entry = Self::required_namespace(current, identifier.namespace())?;
            let namespace = self.load_namespace(namespace_entry)?;
            let entry = namespace
                .tables
                .iter()
                .find(|entry| entry.name == identifier.name())
                .ok_or_else(|| CatalogError::TableNotFound(identifier.clone()))?;
            let identity = self.load_identity(entry.table_id)?;
            let schema = self.load_schema(entry.table_id, entry.catalog_schema_id)?;
            Ok(self.catalog_table(identifier.clone(), identity, schema))
        })
    }

    fn load_table_schema(
        &self,
        identifier: &TableIdentifier,
        catalog_schema_id: CatalogSchemaId,
    ) -> CatalogResult<TableSchema> {
        validate_identifier(identifier)?;
        self.with_current(|current| {
            let namespace_entry = Self::required_namespace(current, identifier.namespace())?;
            let namespace = self.load_namespace(namespace_entry)?;
            let entry = namespace
                .tables
                .iter()
                .find(|entry| entry.name == identifier.name())
                .ok_or_else(|| CatalogError::TableNotFound(identifier.clone()))?;
            if catalog_schema_id > entry.catalog_schema_id {
                return Err(CatalogError::SchemaNotFound {
                    table: identifier.clone(),
                    catalog_schema_id,
                });
            }
            Ok(self
                .load_schema(entry.table_id, catalog_schema_id)?
                .schema_version
                .schema()
                .clone())
        })
    }

    fn evolve_schema(
        &self,
        identifier: &TableIdentifier,
        changes: Vec<SchemaChange>,
    ) -> CatalogResult<CatalogTable> {
        validate_identifier(identifier)?;
        self.with_current(|current| {
            let namespace_entry =
                Self::required_namespace(current, identifier.namespace())?.clone();
            let mut namespace = self.load_namespace(&namespace_entry)?;
            let entry_index = namespace
                .tables
                .iter()
                .position(|entry| entry.name == identifier.name())
                .ok_or_else(|| CatalogError::TableNotFound(identifier.clone()))?;
            let table_id = namespace.tables[entry_index].table_id;
            let current_catalog_schema_id = namespace.tables[entry_index].catalog_schema_id;
            let current_schema = self.load_schema(table_id, current_catalog_schema_id)?;
            let record = TableSchemaRecord::new(current_schema.schema_version.evolve(changes)?);
            let next_catalog_schema_id = record.schema_version.catalog_schema_id();
            write_json(
                &self.store,
                &table_schema_path(table_id, next_catalog_schema_id),
                &record,
            )?;
            namespace.tables[entry_index].catalog_schema_id = next_catalog_schema_id;
            namespace.generation += 1;
            self.commit_namespace(&namespace_entry, &namespace)?;
            Ok(self.catalog_table(identifier.clone(), self.load_identity(table_id)?, record))
        })
    }

    fn list_tables(&self, namespace: &[String]) -> CatalogResult<Vec<TableIdentifier>> {
        validate_namespace(namespace)?;
        self.with_current(|current| {
            let namespace_entry = Self::required_namespace(current, namespace)?;
            Ok(self
                .load_namespace(namespace_entry)?
                .tables
                .into_iter()
                .map(|entry| TableIdentifier::new(namespace.to_vec(), entry.name))
                .collect())
        })
    }

    fn table_exists(&self, identifier: &TableIdentifier) -> CatalogResult<bool> {
        validate_identifier(identifier)?;
        self.with_current(|current| {
            let Some(namespace_entry) = Self::namespace_entry(current, identifier.namespace())
            else {
                return Ok(false);
            };
            Ok(self
                .load_namespace(namespace_entry)?
                .tables
                .iter()
                .any(|entry| entry.name == identifier.name()))
        })
    }

    fn rename_table(
        &self,
        identifier: &TableIdentifier,
        new_name: String,
    ) -> CatalogResult<CatalogTable> {
        validate_identifier(identifier)?;
        let new_identifier = identifier.renamed(new_name);
        validate_identifier(&new_identifier)?;
        self.with_current(|current| {
            let namespace_entry =
                Self::required_namespace(current, identifier.namespace())?.clone();
            let mut namespace = self.load_namespace(&namespace_entry)?;
            let source_index = namespace
                .tables
                .iter()
                .position(|entry| entry.name == identifier.name())
                .ok_or_else(|| CatalogError::TableNotFound(identifier.clone()))?;
            if namespace
                .tables
                .iter()
                .any(|entry| entry.name == new_identifier.name())
            {
                return Err(CatalogError::TableAlreadyExists(new_identifier));
            }
            let entry = &mut namespace.tables[source_index];
            let table_id = entry.table_id;
            let catalog_schema_id = entry.catalog_schema_id;
            entry.name = new_identifier.name().to_string();
            namespace.generation += 1;
            namespace
                .tables
                .sort_by(|left, right| left.name.cmp(&right.name));
            self.commit_namespace(&namespace_entry, &namespace)?;
            let identity = self.load_identity(table_id)?;
            let schema = self.load_schema(table_id, catalog_schema_id)?;
            Ok(self.catalog_table(new_identifier, identity, schema))
        })
    }

    fn drop_table(&self, identifier: &TableIdentifier) -> CatalogResult<()> {
        validate_identifier(identifier)?;
        self.with_current(|current| {
            let namespace_entry =
                Self::required_namespace(current, identifier.namespace())?.clone();
            let mut namespace = self.load_namespace(&namespace_entry)?;
            let original_len = namespace.tables.len();
            namespace
                .tables
                .retain(|entry| entry.name != identifier.name());
            if namespace.tables.len() == original_len {
                return Err(CatalogError::TableNotFound(identifier.clone()));
            }
            namespace.generation += 1;
            self.commit_namespace(&namespace_entry, &namespace)
        })
    }
}

fn load_catalog_manifest(store: &CatalogStore) -> CatalogResult<CatalogManifest> {
    if !store.exists("CURRENT")? {
        return Ok(CatalogManifest::empty());
    }
    let current: CurrentPointer = read_json(store, "CURRENT")?;
    validate_header(&current.format, current.version)?;
    let manifest: CatalogManifest = read_json(store, &format!("CATALOG-{}", current.generation))?;
    validate_header(&manifest.format, manifest.version)?;
    if manifest.generation != current.generation {
        return Err(CatalogError::InvalidMetadata(
            "catalog generation does not match CURRENT".to_string(),
        ));
    }
    Ok(manifest)
}

fn process_operation_lock(storage_id: &str) -> CatalogResult<Arc<Mutex<()>>> {
    static LOCKS: OnceLock<Mutex<HashMap<String, Weak<Mutex<()>>>>> = OnceLock::new();
    let mut locks = lock(LOCKS.get_or_init(|| Mutex::new(HashMap::new())))?;
    if let Some(existing) = locks.get(storage_id).and_then(Weak::upgrade) {
        return Ok(existing);
    }
    let operation_lock = Arc::new(Mutex::new(()));
    locks.insert(storage_id.to_string(), Arc::downgrade(&operation_lock));
    Ok(operation_lock)
}

fn lock<T>(mutex: &Mutex<T>) -> CatalogResult<std::sync::MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| CatalogError::InvalidMetadata("catalog lock poisoned".to_string()))
}

fn validate_header(format: &str, version: u32) -> CatalogResult<()> {
    if format != CATALOG_FORMAT || version != CATALOG_VERSION {
        return Err(CatalogError::InvalidMetadata(format!(
            "unsupported format/version: {format}/{version}"
        )));
    }
    Ok(())
}

fn validate_namespace(namespace: &[String]) -> CatalogResult<()> {
    if namespace.is_empty() {
        return Err(CatalogError::InvalidIdentifier(
            "namespace must contain at least one component".to_string(),
        ));
    }
    for component in namespace {
        validate_name("namespace component", component)?;
    }
    Ok(())
}

fn validate_identifier(identifier: &TableIdentifier) -> CatalogResult<()> {
    validate_namespace(identifier.namespace())?;
    validate_name("table name", identifier.name())
}

fn validate_name(label: &str, value: &str) -> CatalogResult<()> {
    if value.is_empty() || value != value.trim() || value.chars().any(char::is_control) {
        return Err(CatalogError::InvalidIdentifier(format!(
            "invalid {label}: {value:?}"
        )));
    }
    Ok(())
}

fn namespace_prefix(namespace_id: Uuid) -> String {
    format!("namespaces/{namespace_id}")
}

fn table_identity_path(table_id: TableId) -> String {
    format!("tables/TABLE-{table_id}/IDENTITY")
}

fn table_schema_path(table_id: TableId, schema_id: CatalogSchemaId) -> String {
    format!(
        "tables/TABLE-{table_id}/schemas/SCHEMA-{}",
        schema_id.as_u32()
    )
}

fn schema_mapping_path(
    table_id: TableId,
    db_id: &str,
    catalog_schema_id: CatalogSchemaId,
) -> String {
    let digest = Sha256::digest(db_id.as_bytes());
    let mut shard = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut shard, "{byte:02x}").expect("writing to a string cannot fail");
    }
    format!(
        "tables/TABLE-{table_id}/shards/{shard}/SCHEMA-{}",
        catalog_schema_id.as_u32()
    )
}

fn validate_schema_mapping_key(
    mapping: &ShardSchemaMappingFile,
    table_id: TableId,
    db_id: &str,
    catalog_schema_id: CatalogSchemaId,
) -> CatalogResult<()> {
    if mapping.table_id != table_id
        || mapping.db_id != db_id
        || mapping.catalog_schema_id != catalog_schema_id
    {
        return Err(CatalogError::InvalidMetadata(
            "shard schema mapping does not match its lookup key".to_string(),
        ));
    }
    Ok(())
}

fn read_json<T: DeserializeOwned>(store: &CatalogStore, path: &str) -> CatalogResult<T> {
    let bytes = store.read(path)?;
    serde_json::from_slice(&bytes).map_err(|error| CatalogError::InvalidMetadata(error.to_string()))
}

fn write_json<T: Serialize>(store: &CatalogStore, path: &str, value: &T) -> CatalogResult<()> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| CatalogError::InvalidMetadata(error.to_string()))?;
    store.write(path, &bytes)?;
    Ok(())
}
