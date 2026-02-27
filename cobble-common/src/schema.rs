use crate::error::{Error, Result};
use crate::file::{BufferedWriter, File, FileManager};
use crate::merge_operator::{
    MergeOperator, default_merge_operator, default_merge_operator_ref, merge_operator_by_id,
};
use crate::paths::schema_file_relative_path;
use crate::snapshot::ManifestSnapshot;
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Runtime schema for merge semantics.
#[derive(Clone)]
pub struct Schema {
    version: u64,
    num_columns: usize,
    operators: Arc<Vec<Arc<dyn MergeOperator>>>,
}

impl Schema {
    pub(crate) fn new(
        version: u64,
        num_columns: usize,
        mut operators: Vec<Arc<dyn MergeOperator>>,
    ) -> Self {
        if operators.len() < num_columns {
            operators.resize_with(num_columns, default_merge_operator);
        } else if operators.len() > num_columns {
            operators.truncate(num_columns);
        }
        Self {
            version,
            num_columns,
            operators: Arc::new(operators),
        }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn num_columns(&self) -> usize {
        self.num_columns
    }

    pub(crate) fn operator(&self, column_idx: usize) -> &dyn MergeOperator {
        self.operators
            .get(column_idx)
            .unwrap_or_else(|| default_merge_operator_ref())
            .as_ref()
    }

    pub(crate) fn operator_ids(&self, num_columns: usize) -> Vec<String> {
        (0..num_columns)
            .map(|column_idx| self.operator(column_idx).id())
            .collect()
    }

    pub(crate) fn empty() -> Arc<Self> {
        Arc::new(Self::new(0, 0, Vec::new()))
    }
}

/// Manages versioned runtime schema snapshots.
#[derive(Clone)]
pub(crate) struct SchemaManager {
    latest_schema: Arc<ArcSwap<Schema>>,
    schemas: Arc<RwLock<BTreeMap<u64, Arc<Schema>>>>,
    max_persisted_schema_id: Arc<RwLock<Option<u64>>>,
    next_version: Arc<AtomicU64>,
}

impl SchemaManager {
    pub(crate) fn new(num_columns: usize) -> Self {
        Self::from_initial_schema(Arc::new(Schema::new(
            0,
            num_columns,
            vec![default_merge_operator(); num_columns],
        )))
    }

    pub(crate) fn from_schemas(schemas: Vec<Schema>, default_num_columns: usize) -> Self {
        let mut versions: BTreeMap<u64, Arc<Schema>> = BTreeMap::new();
        for schema in schemas {
            let version = schema.version();
            versions.insert(version, Arc::new(schema));
        }
        if versions.is_empty() {
            let initial = Arc::new(Schema::new(
                0,
                default_num_columns,
                vec![default_merge_operator(); default_num_columns],
            ));
            versions.insert(0, Arc::clone(&initial));
            return Self {
                latest_schema: Arc::new(ArcSwap::from(initial)),
                schemas: Arc::new(RwLock::new(versions)),
                max_persisted_schema_id: Arc::new(RwLock::new(None)),
                next_version: Arc::new(AtomicU64::new(1)),
            };
        }
        let (max_version, latest) = versions
            .iter()
            .next_back()
            .map(|(version, schema)| (*version, Arc::clone(schema)))
            .expect("versions not empty");
        Self {
            latest_schema: Arc::new(ArcSwap::from(latest)),
            schemas: Arc::new(RwLock::new(versions)),
            max_persisted_schema_id: Arc::new(RwLock::new(Some(max_version))),
            next_version: Arc::new(AtomicU64::new(max_version.saturating_add(1))),
        }
    }

    pub(crate) fn from_manifest(
        file_manager: &Arc<FileManager>,
        manifest: &ManifestSnapshot,
        default_num_columns: usize,
    ) -> Result<Self> {
        Self::from_manifests(file_manager, std::iter::once(manifest), default_num_columns)
    }

    pub(crate) fn from_manifests<'a, I>(
        file_manager: &Arc<FileManager>,
        manifests: I,
        default_num_columns: usize,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = &'a ManifestSnapshot>,
    {
        let mut schema_ids = BTreeSet::new();
        for manifest in manifests {
            collect_schema_ids_from_manifest(manifest, &mut schema_ids);
        }
        let schemas = schema_ids
            .into_iter()
            .map(|schema_id| load_schema(file_manager, schema_id, default_num_columns))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::from_schemas(schemas, default_num_columns))
    }

    fn from_initial_schema(initial: Arc<Schema>) -> Self {
        let next_version = initial.version().saturating_add(1);
        let mut versions = BTreeMap::new();
        versions.insert(initial.version(), Arc::clone(&initial));
        Self {
            latest_schema: Arc::new(ArcSwap::from(initial)),
            schemas: Arc::new(RwLock::new(versions)),
            max_persisted_schema_id: Arc::new(RwLock::new(None)),
            next_version: Arc::new(AtomicU64::new(next_version)),
        }
    }

    pub(crate) fn latest_schema(&self) -> Arc<Schema> {
        self.latest_schema.load_full()
    }

    pub(crate) fn builder(self: &Arc<Self>) -> SchemaBuilder {
        self.builder_from(self.latest_schema())
    }

    pub(crate) fn builder_from(self: &Arc<Self>, schema: Arc<Schema>) -> SchemaBuilder {
        SchemaBuilder::from_schema(Arc::clone(self), schema)
    }

    fn commit_build(
        &self,
        num_columns: usize,
        operators: Vec<Arc<dyn MergeOperator>>,
    ) -> Arc<Schema> {
        let version = self.next_version.fetch_add(1, Ordering::SeqCst);
        let schema = Arc::new(Schema::new(version, num_columns, operators));
        self.latest_schema.store(Arc::clone(&schema));
        self.schemas
            .write()
            .unwrap()
            .insert(schema.version(), Arc::clone(&schema));
        schema
    }

    pub(crate) fn persist_schemas_up_to(
        &self,
        file_manager: &FileManager,
        max_schema_id: u64,
    ) -> Result<()> {
        let start = self
            .max_persisted_schema_id
            .read()
            .unwrap()
            .map_or(0, |id| id.saturating_add(1));
        if start > max_schema_id {
            return Ok(());
        }
        let schemas_to_persist: Vec<Arc<Schema>> = {
            let schemas = self.schemas.read().unwrap();
            let mut out = Vec::with_capacity((max_schema_id - start + 1) as usize);
            for schema_id in start..=max_schema_id {
                let schema = schemas.get(&schema_id).ok_or_else(|| {
                    Error::InvalidState(format!("Missing schema version {}", schema_id))
                })?;
                out.push(Arc::clone(schema));
            }
            out
        };
        for schema in schemas_to_persist {
            persist_schema(file_manager, schema.as_ref())?;
        }
        *self.max_persisted_schema_id.write().unwrap() = Some(max_schema_id);
        Ok(())
    }
}

fn collect_schema_ids_from_manifest(manifest: &ManifestSnapshot, schema_ids: &mut BTreeSet<u64>) {
    schema_ids.insert(manifest.latest_schema_id);
    for level in &manifest.levels {
        for file in &level.files {
            schema_ids.insert(file.schema_id);
        }
    }
}

#[derive(Deserialize, Serialize)]
struct SchemaFile {
    id: u64,
    num_columns: usize,
    #[serde(default)]
    merge_operator_ids: Vec<String>,
}

pub(crate) fn persist_schema(file_manager: &FileManager, schema: &Schema) -> Result<()> {
    let schema_file = SchemaFile {
        id: schema.version(),
        num_columns: schema.num_columns(),
        merge_operator_ids: schema.operator_ids(schema.num_columns()),
    };
    let json = serde_json::to_vec(&schema_file)
        .map_err(|err| Error::IoError(format!("Failed to encode schema file: {}", err)))?;
    let mut writer = BufferedWriter::new(
        file_manager.create_metadata_file(&schema_file_relative_path(schema.version()))?,
        4096,
    );
    writer.write(json.as_ref())?;
    writer.close()?;
    Ok(())
}

pub(crate) fn load_schema(
    file_manager: &Arc<FileManager>,
    schema_id: u64,
    default_num_columns: usize,
) -> Result<Schema> {
    let reader =
        file_manager.open_metadata_file_reader_untracked(&schema_file_relative_path(schema_id))?;
    let bytes = reader.read_at(0, reader.size())?;
    let schema_file: SchemaFile = serde_json::from_slice(bytes.as_ref())
        .map_err(|err| Error::IoError(format!("Failed to decode schema file: {}", err)))?;
    if schema_file.id != schema_id {
        return Err(Error::IoError(format!(
            "Schema file id mismatch: expected {}, got {}",
            schema_id, schema_file.id
        )));
    }
    let mut operators: Vec<Arc<dyn MergeOperator>> = schema_file
        .merge_operator_ids
        .iter()
        .map(|id| merge_operator_by_id(id))
        .collect();
    let num_columns = schema_file.num_columns.max(default_num_columns);
    if operators.len() < num_columns {
        operators.resize_with(num_columns, default_merge_operator);
    }
    Ok(Schema::new(schema_file.id, num_columns, operators))
}

pub struct SchemaBuilder {
    manager: Arc<SchemaManager>,
    num_columns: usize,
    operators: Vec<Arc<dyn MergeOperator>>,
}

impl SchemaBuilder {
    fn from_schema(manager: Arc<SchemaManager>, schema: Arc<Schema>) -> Self {
        Self {
            manager,
            num_columns: schema.num_columns(),
            operators: schema.operators.as_ref().clone(),
        }
    }

    pub fn set_column_operator(
        &mut self,
        column_idx: usize,
        operator: Arc<dyn MergeOperator>,
    ) -> Result<()> {
        if self.operators.is_empty() && column_idx > 0 {
            self.operators = vec![default_merge_operator(); column_idx + 1];
        } else if column_idx >= self.operators.len() {
            self.operators
                .resize_with(column_idx + 1, default_merge_operator);
        }
        if column_idx >= self.num_columns {
            self.num_columns = column_idx + 1;
        }
        self.operators[column_idx] = operator;
        Ok(())
    }

    pub fn set_all(&mut self, operators: Vec<Arc<dyn MergeOperator>>) {
        self.operators = operators;
    }

    pub fn commit(self) -> Arc<Schema> {
        self.manager.commit_build(self.num_columns, self.operators)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_operator::MergeOperator;
    use bytes::Bytes;

    struct BracketMergeOperator;

    impl MergeOperator for BracketMergeOperator {
        fn merge(&self, existing_value: Bytes, value: Bytes) -> Result<Bytes> {
            let existing = existing_value.as_ref();
            Ok(format!(
                "[{}+{}]",
                String::from_utf8_lossy(existing),
                String::from_utf8_lossy(value.as_ref())
            )
            .into_bytes()
            .into())
        }
    }

    #[test]
    fn test_schema_builder_set_column_operator() {
        let manager = Arc::new(SchemaManager::new(2));
        let mut builder = manager.builder();
        builder
            .set_column_operator(1, Arc::new(BracketMergeOperator))
            .unwrap();
        let schema = builder.commit();
        assert_eq!(schema.version(), 1);
        let op = schema.operator(1);
        let merged = op
            .merge(Bytes::from_static(b"x"), Bytes::from_static(b"y"))
            .unwrap();
        assert_eq!(merged.as_ref(), b"[x+y]");
    }

    #[test]
    fn test_schema_falls_back_to_default_for_missing_column() {
        let manager = SchemaManager::new(1);
        let schema = manager.latest_schema();
        let op = schema.operator(10);
        let merged = op
            .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"))
            .unwrap();
        assert_eq!(merged.as_ref(), b"ab");
    }

    #[test]
    fn test_schema_builder_extends_columns() {
        let manager = Arc::new(SchemaManager::new(1));
        let base = manager.latest_schema();
        let mut builder = manager.builder_from(base);
        builder
            .set_column_operator(3, Arc::new(BracketMergeOperator))
            .unwrap();
        let schema = builder.commit();
        assert_eq!(schema.num_columns(), 4);
        let merged0 = schema
            .operator(0)
            .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"))
            .unwrap();
        let merged3 = schema
            .operator(3)
            .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"))
            .unwrap();
        assert_eq!(merged0.as_ref(), b"ab");
        assert_eq!(merged3.as_ref(), b"[a+b]");
    }
}
