use super::catalog_store::CatalogStore;
use super::model::{CatalogTable, FileCatalogConfig, TableId, TableIdentifier};
use crate::metadata::TableMetadata;
use crate::{TableError, TableSchema};
use cobble::Config;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::HashMap;
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableIdentity {
    format: String,
    version: u32,
    table_id: TableId,
    physical_name: String,
    metadata: TableMetadata,
}

/// File-backed table catalog.
///
/// Operations are serialized across handles in this process and refresh CURRENT before every
/// read or mutation. Phase 1 still requires one active catalog mutator across processes;
/// distributed locking is intentionally outside this phase.
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
        validate_namespace(namespace)?;
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
        validate_namespace_manifest(&manifest)?;
        Ok(manifest)
    }

    fn load_identity(&self, table_id: TableId) -> CatalogResult<TableIdentity> {
        let identity: TableIdentity = read_json(&self.store, &table_identity_path(table_id))?;
        validate_header(&identity.format, identity.version)?;
        if identity.table_id != table_id || identity.physical_name != physical_table_name(table_id)
        {
            return Err(CatalogError::InvalidMetadata(format!(
                "table identity does not match {table_id}"
            )));
        }
        identity.metadata.validate()?;
        Ok(identity)
    }

    fn catalog_table(
        identifier: TableIdentifier,
        identity: TableIdentity,
    ) -> CatalogResult<CatalogTable> {
        identity.metadata.validate()?;
        Ok(CatalogTable {
            identifier,
            table_id: identity.table_id,
            schema: identity.metadata.schema,
        })
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
            let metadata = TableMetadata::compile(schema)?;
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
                metadata,
            };
            write_json(&self.store, &table_identity_path(table_id), &identity)?;
            namespace.generation += 1;
            namespace.tables.push(TableEntry {
                name: identifier.name().to_string(),
                table_id,
            });
            namespace
                .tables
                .sort_by(|left, right| left.name.cmp(&right.name));
            self.commit_namespace(&namespace_entry, &namespace)?;
            Self::catalog_table(identifier, identity)
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
            Self::catalog_table(identifier.clone(), self.load_identity(entry.table_id)?)
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
            entry.name = new_identifier.name().to_string();
            namespace.generation += 1;
            namespace
                .tables
                .sort_by(|left, right| left.name.cmp(&right.name));
            self.commit_namespace(&namespace_entry, &namespace)?;
            Self::catalog_table(new_identifier, self.load_identity(table_id)?)
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
    validate_catalog_manifest(&manifest)?;
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

fn validate_catalog_manifest(manifest: &CatalogManifest) -> CatalogResult<()> {
    validate_header(&manifest.format, manifest.version)?;
    for entry in &manifest.namespaces {
        validate_namespace(&entry.namespace)?;
    }
    let mut namespaces = manifest
        .namespaces
        .iter()
        .map(|entry| &entry.namespace)
        .collect::<Vec<_>>();
    namespaces.sort();
    if namespaces.windows(2).any(|window| window[0] == window[1]) {
        return Err(CatalogError::InvalidMetadata(
            "duplicate namespace in catalog manifest".to_string(),
        ));
    }
    Ok(())
}

fn validate_namespace_manifest(manifest: &NamespaceManifest) -> CatalogResult<()> {
    let mut names = manifest
        .tables
        .iter()
        .map(|entry| entry.name.as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    if names.windows(2).any(|window| window[0] == window[1]) {
        return Err(CatalogError::InvalidMetadata(
            "duplicate table name in namespace manifest".to_string(),
        ));
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
    format!("tables/TABLE-{table_id}")
}

fn physical_table_name(table_id: TableId) -> String {
    format!("t{table_id}")
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
