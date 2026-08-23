use super::catalog_store::CatalogStore;
use super::model::{
    CatalogSchemaId, CatalogTable, FileCatalogConfig, SchemaChange, TableId, TableIdentifier,
};
use crate::{DataField, FieldId, LogicalType, LogicalTypeKind, TableError, TableSchema};
use cobble::Config;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use thiserror::Error;
use uuid::Uuid;

const CATALOG_FORMAT: &str = "cobble-table-catalog";
const CATALOG_VERSION: u32 = 1;
pub type CatalogResult<T> = std::result::Result<T, CatalogError>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CatalogError {
    #[error("invalid catalog identifier: {0}")]
    InvalidIdentifier(String),
    #[error("namespace already exists: {0:?}")]
    NamespaceAlreadyExists(Vec<String>),
    #[error("namespace not found: {0:?}")]
    NamespaceNotFound(Vec<String>),
    #[error("namespace is not empty: {0:?}")]
    NamespaceNotEmpty(Vec<String>),
    #[error("table already exists: {0:?}")]
    TableAlreadyExists(TableIdentifier),
    #[error("table not found: {0:?}")]
    TableNotFound(TableIdentifier),
    #[error("catalog schema {catalog_schema_id} not found for table {table:?}")]
    SchemaNotFound {
        table: TableIdentifier,
        catalog_schema_id: CatalogSchemaId,
    },
    #[error("invalid catalog schema evolution: {0}")]
    InvalidSchemaEvolution(String),
    #[error("invalid catalog metadata: {0}")]
    InvalidMetadata(String),
    #[error(transparent)]
    Storage(#[from] cobble::Error),
    #[error(transparent)]
    Table(#[from] TableError),
}

/// Semantic catalog operations.
pub trait Catalog: Send + Sync {
    fn create_namespace(&self, namespace: Vec<String>) -> CatalogResult<()>;
    fn list_namespaces(&self) -> CatalogResult<Vec<Vec<String>>>;
    fn drop_namespace(&self, namespace: &[String]) -> CatalogResult<()>;
    fn create_table(
        &self,
        identifier: TableIdentifier,
        schema: TableSchema,
    ) -> CatalogResult<CatalogTable>;
    fn load_table(&self, identifier: &TableIdentifier) -> CatalogResult<CatalogTable>;
    fn load_table_schema(
        &self,
        identifier: &TableIdentifier,
        catalog_schema_id: CatalogSchemaId,
    ) -> CatalogResult<TableSchema>;
    fn evolve_schema(
        &self,
        identifier: &TableIdentifier,
        changes: Vec<SchemaChange>,
    ) -> CatalogResult<CatalogTable>;
    fn list_tables(&self, namespace: &[String]) -> CatalogResult<Vec<TableIdentifier>>;
    fn table_exists(&self, identifier: &TableIdentifier) -> CatalogResult<bool>;
    fn rename_table(
        &self,
        identifier: &TableIdentifier,
        new_name: String,
    ) -> CatalogResult<CatalogTable>;
    fn drop_table(&self, identifier: &TableIdentifier) -> CatalogResult<()>;
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
    table_id: TableId,
    catalog_schema_id: CatalogSchemaId,
    schema: TableSchema,
    used_field_ids: Vec<FieldId>,
}

/// File-backed table catalog.
///
/// Operations are serialized across handles in this process and refresh CURRENT before every
/// read or mutation. One active catalog mutator is still required across processes; distributed
/// locking is intentionally outside this implementation.
pub struct FileCatalog {
    store: CatalogStore,
    state: Mutex<CatalogManifest>,
    operation_lock: Arc<Mutex<()>>,
}

impl FileCatalog {
    pub fn open(config: &Config, catalog_config: FileCatalogConfig) -> CatalogResult<Self> {
        let store = CatalogStore::open(config, catalog_config.storage_id())?;
        let manifest = load_catalog_manifest(&store)?;
        let operation_lock = process_operation_lock(catalog_config.storage_id())?;
        Ok(Self {
            store,
            state: Mutex::new(manifest),
            operation_lock,
        })
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
        let path = table_schema_path(table_id, catalog_schema_id);
        let record: TableSchemaRecord = read_json(&self.store, &path)?;
        validate_header(&record.format, record.version)?;
        debug_assert_eq!(record.table_id, table_id);
        debug_assert_eq!(record.catalog_schema_id, catalog_schema_id);
        #[cfg(debug_assertions)]
        {
            debug_assert!(record.schema.validate().is_ok());
            debug_assert!(
                record
                    .used_field_ids
                    .windows(2)
                    .all(|window| window[0] < window[1])
            );
            debug_assert!(
                schema_field_ids(&record.schema)
                    .iter()
                    .all(|field_id| record.used_field_ids.binary_search(field_id).is_ok())
            );
        }
        Ok(record)
    }

    fn catalog_table(
        identifier: TableIdentifier,
        identity: TableIdentity,
        schema: TableSchemaRecord,
    ) -> CatalogTable {
        CatalogTable {
            identifier,
            table_id: identity.table_id,
            catalog_schema_id: schema.catalog_schema_id,
            schema: schema.schema,
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
            let schema = TableSchemaRecord {
                format: CATALOG_FORMAT.to_string(),
                version: CATALOG_VERSION,
                table_id,
                catalog_schema_id: CatalogSchemaId::INITIAL,
                used_field_ids: schema_field_ids(&schema),
                schema,
            };
            write_json(
                &self.store,
                &table_schema_path(table_id, schema.catalog_schema_id),
                &schema,
            )?;
            namespace.generation += 1;
            namespace.tables.push(TableEntry {
                name: identifier.name().to_string(),
                table_id,
                catalog_schema_id: schema.catalog_schema_id,
            });
            namespace
                .tables
                .sort_by(|left, right| left.name.cmp(&right.name));
            self.commit_namespace(&namespace_entry, &namespace)?;
            Ok(Self::catalog_table(identifier, identity, schema))
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
            Ok(Self::catalog_table(identifier.clone(), identity, schema))
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
            Ok(self.load_schema(entry.table_id, catalog_schema_id)?.schema)
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
            let mut used_field_ids = current_schema
                .used_field_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            let next_schema =
                apply_schema_changes(current_schema.schema, changes, &mut used_field_ids)?;
            let next_catalog_schema_id = current_catalog_schema_id.next().ok_or_else(|| {
                CatalogError::InvalidSchemaEvolution("schema id space exhausted".to_string())
            })?;
            let record = TableSchemaRecord {
                format: CATALOG_FORMAT.to_string(),
                version: CATALOG_VERSION,
                table_id,
                catalog_schema_id: next_catalog_schema_id,
                schema: next_schema,
                used_field_ids: sorted_field_ids(used_field_ids),
            };
            write_json(
                &self.store,
                &table_schema_path(table_id, next_catalog_schema_id),
                &record,
            )?;
            namespace.tables[entry_index].catalog_schema_id = next_catalog_schema_id;
            namespace.generation += 1;
            self.commit_namespace(&namespace_entry, &namespace)?;
            Ok(Self::catalog_table(
                identifier.clone(),
                self.load_identity(table_id)?,
                record,
            ))
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
            Ok(Self::catalog_table(new_identifier, identity, schema))
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

fn physical_table_name(table_id: TableId) -> String {
    format!("t{table_id}")
}

fn table_schema_path(table_id: TableId, schema_id: CatalogSchemaId) -> String {
    format!(
        "tables/TABLE-{table_id}/schemas/SCHEMA-{}",
        schema_id.as_u32()
    )
}

fn apply_schema_changes(
    mut schema: TableSchema,
    changes: Vec<SchemaChange>,
    used_field_ids: &mut HashSet<FieldId>,
) -> CatalogResult<TableSchema> {
    if changes.is_empty() {
        return Err(CatalogError::InvalidSchemaEvolution(
            "schema changes must not be empty".to_string(),
        ));
    }
    let key_ids = schema
        .primary_key
        .iter()
        .chain(&schema.bucket_key)
        .copied()
        .collect::<HashSet<_>>();

    for change in changes {
        match change {
            SchemaChange::AddField(field) => {
                let mut added_ids = HashSet::new();
                collect_field_ids(&field, &mut added_ids);
                if let Some(reused) = added_ids.intersection(used_field_ids).next() {
                    return Err(CatalogError::InvalidSchemaEvolution(format!(
                        "field {} already exists",
                        reused.0
                    )));
                }
                if !field.logical_type.nullable {
                    return Err(CatalogError::InvalidSchemaEvolution(format!(
                        "added field '{}' must be nullable",
                        field.name
                    )));
                }
                used_field_ids.extend(added_ids);
                schema.fields.push(field);
            }
            SchemaChange::RenameField { field_id, new_name } => {
                let field = schema
                    .fields
                    .iter_mut()
                    .find(|field| field.id == field_id)
                    .ok_or_else(|| {
                        CatalogError::InvalidSchemaEvolution(format!(
                            "field {} does not exist",
                            field_id.0
                        ))
                    })?;
                field.name = new_name;
            }
            SchemaChange::DropField(field_id) => {
                if key_ids.contains(&field_id) {
                    return Err(CatalogError::InvalidSchemaEvolution(format!(
                        "key field {} cannot be dropped",
                        field_id.0
                    )));
                }
                let index = schema
                    .fields
                    .iter()
                    .position(|field| field.id == field_id)
                    .ok_or_else(|| {
                        CatalogError::InvalidSchemaEvolution(format!(
                            "field {} does not exist",
                            field_id.0
                        ))
                    })?;
                schema.fields.remove(index);
            }
        }
    }
    TableSchema::new(schema.fields, schema.primary_key, schema.bucket_key)
        .map_err(|error| CatalogError::InvalidSchemaEvolution(error.to_string()))
}

fn schema_field_ids(schema: &TableSchema) -> Vec<FieldId> {
    let mut field_ids = HashSet::new();
    for field in &schema.fields {
        collect_field_ids(field, &mut field_ids);
    }
    sorted_field_ids(field_ids)
}

fn sorted_field_ids(field_ids: HashSet<FieldId>) -> Vec<FieldId> {
    let mut field_ids = field_ids.into_iter().collect::<Vec<_>>();
    field_ids.sort_unstable();
    field_ids
}

fn collect_field_ids(field: &DataField, field_ids: &mut HashSet<FieldId>) {
    field_ids.insert(field.id);
    collect_type_field_ids(&field.logical_type, field_ids);
}

fn collect_type_field_ids(logical_type: &LogicalType, field_ids: &mut HashSet<FieldId>) {
    match &logical_type.kind {
        LogicalTypeKind::List { element_type } => {
            collect_type_field_ids(element_type, field_ids);
        }
        LogicalTypeKind::Map {
            key_type,
            value_type,
        } => {
            collect_type_field_ids(key_type, field_ids);
            collect_type_field_ids(value_type, field_ids);
        }
        LogicalTypeKind::Struct { fields } => {
            for field in fields {
                collect_field_ids(field, field_ids);
            }
        }
        LogicalTypeKind::Extension { extension } => {
            collect_type_field_ids(&extension.physical_type, field_ids);
        }
        _ => {}
    }
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
