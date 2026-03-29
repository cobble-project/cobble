use crate::error::{Error, Result};
use crate::file::{BufferedWriter, File, FileManager};
use crate::merge_operator::{
    MergeOperator, MergeOperatorResolver, default_merge_operator, default_merge_operator_ref,
    merge_operator_by_id,
};
use crate::paths::schema_file_relative_path;
use crate::snapshot::ManifestSnapshot;
use crate::r#type::{Column, Value, ValueType};
use arc_swap::ArcSwap;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) trait SchemaEvolution: Send + Sync {
    fn id(&self) -> &'static str;
    fn evolve(&self, value: Value) -> Result<Value>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BuiltinSchemaEvolution {
    Noop,
    ColumnAdd {
        indexes: Arc<Vec<usize>>,
        default_values: Arc<Vec<Option<Bytes>>>,
    },
    ColumnDelete {
        indexes: Arc<Vec<usize>>,
    },
}

impl SchemaEvolution for BuiltinSchemaEvolution {
    fn id(&self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::ColumnAdd { .. } => "column_add",
            Self::ColumnDelete { .. } => "column_delete",
        }
    }

    fn evolve(&self, value: Value) -> Result<Value> {
        let expired_at = value.expired_at();
        let mut columns = value.columns;
        match self {
            Self::Noop => {}
            Self::ColumnAdd {
                indexes,
                default_values,
            } => {
                if indexes.len() != default_values.len() {
                    return Err(Error::InvalidState(format!(
                        "Column add evolution indexes/defaults length mismatch: {} != {}",
                        indexes.len(),
                        default_values.len()
                    )));
                }
                for (&index, default_value) in indexes.iter().zip(default_values.iter()) {
                    if index > columns.len() {
                        return Err(Error::InvalidState(format!(
                            "Cannot add column at index {} when value has {} columns",
                            index,
                            columns.len()
                        )));
                    }
                    columns.insert(
                        index,
                        default_value
                            .as_ref()
                            .map(|bytes| Column::new(ValueType::Put, bytes.clone())),
                    );
                }
            }
            Self::ColumnDelete { indexes } => {
                for &index in indexes.iter() {
                    if index >= columns.len() {
                        return Err(Error::InvalidState(format!(
                            "Cannot delete column at index {} when value has {} columns",
                            index,
                            columns.len()
                        )));
                    }
                    columns.remove(index);
                }
            }
        }
        Ok(Value::new_with_expired_at(columns, expired_at))
    }
}

impl BuiltinSchemaEvolution {
    fn indexes(&self) -> Option<&[usize]> {
        match self {
            Self::Noop => None,
            Self::ColumnAdd { indexes, .. } | Self::ColumnDelete { indexes } => {
                Some(indexes.as_ref())
            }
        }
    }

    fn default_values(&self) -> Option<&[Option<Bytes>]> {
        match self {
            Self::ColumnAdd { default_values, .. } => Some(default_values.as_ref()),
            Self::Noop | Self::ColumnDelete { .. } => None,
        }
    }
}

/// Runtime schema for merge semantics.
#[derive(Clone)]
pub struct Schema {
    version: u64,
    num_columns: usize,
    operators: Arc<Vec<Arc<dyn MergeOperator>>>,
    evolution: BuiltinSchemaEvolution,
    pub(crate) column_metadata: Arc<Vec<Option<JsonValue>>>,
}

impl Schema {
    pub(crate) fn new(
        version: u64,
        num_columns: usize,
        operators: Vec<Arc<dyn MergeOperator>>,
    ) -> Self {
        Self::new_with_evolution(
            version,
            num_columns,
            operators,
            BuiltinSchemaEvolution::Noop,
            Vec::new(),
        )
    }

    fn new_with_evolution(
        version: u64,
        num_columns: usize,
        mut operators: Vec<Arc<dyn MergeOperator>>,
        evolution: BuiltinSchemaEvolution,
        mut column_metadata: Vec<Option<JsonValue>>,
    ) -> Self {
        if operators.len() < num_columns {
            operators.resize_with(num_columns, default_merge_operator);
        } else if operators.len() > num_columns {
            operators.truncate(num_columns);
        }
        if column_metadata.len() < num_columns {
            column_metadata.resize_with(num_columns, || None);
        } else if column_metadata.len() > num_columns {
            column_metadata.truncate(num_columns);
        }
        Self {
            version,
            num_columns,
            operators: Arc::new(operators),
            evolution,
            column_metadata: Arc::new(column_metadata),
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

    /// Return the merge operator id strings for all columns.
    pub fn all_operator_ids(&self) -> Vec<String> {
        self.operator_ids(self.num_columns)
    }

    pub(crate) fn empty() -> Arc<Self> {
        Arc::new(Self::new(0, 0, Vec::new()))
    }

    fn evolution(&self) -> BuiltinSchemaEvolution {
        self.evolution.clone()
    }

    pub(crate) fn column_metadata(&self) -> &[Option<JsonValue>] {
        self.column_metadata.as_ref()
    }

    pub fn column_metadata_at(&self, column_idx: usize) -> Option<&JsonValue> {
        self.column_metadata
            .get(column_idx)
            .and_then(|metadata| metadata.as_ref())
    }
}

/// Manages versioned runtime schema snapshots.
#[derive(Clone)]
pub(crate) struct SchemaManager {
    latest_schema: Arc<ArcSwap<Schema>>,
    schemas: Arc<RwLock<BTreeMap<u64, Arc<Schema>>>>,
    evolutions: Arc<RwLock<BTreeMap<u64, BuiltinSchemaEvolution>>>,
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
                evolutions: Arc::new(RwLock::new(BTreeMap::new())),
                max_persisted_schema_id: Arc::new(RwLock::new(None)),
                next_version: Arc::new(AtomicU64::new(1)),
            };
        }
        let mut evolutions = BTreeMap::new();
        for schema in versions.values() {
            evolutions.insert(schema.version(), schema.evolution());
        }
        let (max_version, latest) = versions
            .iter()
            .next_back()
            .map(|(version, schema)| (*version, Arc::clone(schema)))
            .expect("versions not empty");
        Self {
            latest_schema: Arc::new(ArcSwap::from(latest)),
            schemas: Arc::new(RwLock::new(versions)),
            evolutions: Arc::new(RwLock::new(evolutions)),
            max_persisted_schema_id: Arc::new(RwLock::new(Some(max_version))),
            next_version: Arc::new(AtomicU64::new(max_version.saturating_add(1))),
        }
    }

    pub(crate) fn from_manifest(
        file_manager: &Arc<FileManager>,
        manifest: &ManifestSnapshot,
        default_num_columns: usize,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        Self::from_manifests(
            file_manager,
            std::iter::once(manifest),
            default_num_columns,
            resolver,
        )
    }

    pub(crate) fn from_manifests<'a, I>(
        file_manager: &Arc<FileManager>,
        manifests: I,
        default_num_columns: usize,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
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
            .map(|schema_id| {
                load_schema(
                    file_manager,
                    schema_id,
                    default_num_columns,
                    resolver.as_ref(),
                )
            })
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
            evolutions: Arc::new(RwLock::new(BTreeMap::new())),
            max_persisted_schema_id: Arc::new(RwLock::new(None)),
            next_version: Arc::new(AtomicU64::new(next_version)),
        }
    }

    pub(crate) fn latest_schema(&self) -> Arc<Schema> {
        self.latest_schema.load_full()
    }

    pub(crate) fn current_num_columns(&self) -> usize {
        self.latest_schema.load().num_columns()
    }

    pub(crate) fn schema(&self, schema_id: u64) -> Result<Arc<Schema>> {
        self.schemas
            .read()
            .unwrap()
            .get(&schema_id)
            .cloned()
            .ok_or_else(|| Error::InvalidState(format!("Missing schema version {}", schema_id)))
    }

    pub(crate) fn builder(self: &Arc<Self>) -> SchemaBuilder {
        self.builder_from(self.latest_schema())
    }

    pub(crate) fn builder_from(self: &Arc<Self>, schema: Arc<Schema>) -> SchemaBuilder {
        SchemaBuilder::from_schema(Arc::clone(self), schema)
    }

    fn commit_build(
        &self,
        base_schema: &Schema,
        num_columns: usize,
        operators: Vec<Arc<dyn MergeOperator>>,
        evolution: BuiltinSchemaEvolution,
        column_metadata: Vec<Option<JsonValue>>,
    ) -> Arc<Schema> {
        let latest = self.latest_schema();
        assert_eq!(
            latest.version(),
            base_schema.version(),
            "schema builder commit version conflict"
        );
        let version = self.next_version.fetch_add(1, Ordering::SeqCst);
        let schema = Arc::new(Schema::new_with_evolution(
            version,
            num_columns,
            operators,
            evolution.clone(),
            column_metadata,
        ));
        self.latest_schema.store(Arc::clone(&schema));
        self.schemas
            .write()
            .unwrap()
            .insert(schema.version(), Arc::clone(&schema));
        self.evolutions
            .write()
            .unwrap()
            .insert(schema.version(), evolution);
        schema
    }

    pub(crate) fn evolve_value(
        &self,
        mut value: Value,
        from_schema_id: u64,
        to_schema_id: u64,
    ) -> Result<Value> {
        if from_schema_id == to_schema_id {
            return Ok(value);
        }
        if from_schema_id > to_schema_id {
            return Err(Error::InvalidState(format!(
                "cannot evolve schema from {} down to {}",
                from_schema_id, to_schema_id
            )));
        }
        let evolutions = self.evolutions.read().unwrap();
        for schema_id in (from_schema_id + 1)..=to_schema_id {
            let evolution = evolutions.get(&schema_id).ok_or_else(|| {
                Error::InvalidState(format!(
                    "Missing schema evolution for version {}",
                    schema_id
                ))
            })?;
            value = evolution.evolve(value)?;
        }
        Ok(value)
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

    pub(crate) fn max_persisted_schema_id(&self) -> Option<u64> {
        *self.max_persisted_schema_id.read().unwrap()
    }

    pub(crate) fn update_max_persisted_schema_id_from_live(&self, live_schema_ids: &BTreeSet<u64>) {
        *self.max_persisted_schema_id.write().unwrap() =
            live_schema_ids.iter().next_back().copied();
    }

    pub(crate) fn register_schema_from_file(
        &self,
        file_manager: &Arc<FileManager>,
        schema_id: u64,
        default_num_columns: usize,
    ) -> Result<()> {
        if self.schemas.read().unwrap().contains_key(&schema_id) {
            return Ok(());
        }
        let schema = Arc::new(load_schema(
            file_manager,
            schema_id,
            default_num_columns,
            None,
        )?);
        self.schemas
            .write()
            .unwrap()
            .insert(schema.version(), Arc::clone(&schema));
        self.evolutions
            .write()
            .unwrap()
            .insert(schema.version(), schema.evolution());
        let target_next = schema.version().saturating_add(1);
        let mut current_next = self.next_version.load(Ordering::SeqCst);
        while current_next < target_next {
            match self.next_version.compare_exchange(
                current_next,
                target_next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(value) => current_next = value,
            }
        }
        Ok(())
    }
}

fn collect_schema_ids_from_manifest(manifest: &ManifestSnapshot, schema_ids: &mut BTreeSet<u64>) {
    let latest_schema_id = manifest.latest_schema_id;
    schema_ids.insert(latest_schema_id);
    for tree_levels in &manifest.tree_levels {
        for level in tree_levels {
            for file in &level.files {
                if file.schema_id <= latest_schema_id {
                    for schema_id in file.schema_id..=latest_schema_id {
                        schema_ids.insert(schema_id);
                    }
                } else {
                    schema_ids.insert(file.schema_id);
                }
            }
        }
    }
}

#[derive(Deserialize, Serialize)]
struct SchemaFile {
    id: u64,
    num_columns: usize,
    #[serde(default)]
    merge_operator_ids: Vec<String>,
    #[serde(default)]
    evolution_id: Option<String>,
    #[serde(default)]
    evolution_indexes: Option<Vec<usize>>,
    #[serde(default)]
    evolution_default_values: Option<Vec<Option<Vec<u8>>>>,
    #[serde(default)]
    column_metadata: Vec<Option<JsonValue>>,
}

pub(crate) fn persist_schema(file_manager: &FileManager, schema: &Schema) -> Result<()> {
    let schema_file = SchemaFile {
        id: schema.version(),
        num_columns: schema.num_columns(),
        merge_operator_ids: schema.operator_ids(schema.num_columns()),
        evolution_id: Some(schema.evolution().id().to_string()),
        evolution_indexes: schema.evolution().indexes().map(|indexes| indexes.to_vec()),
        evolution_default_values: schema.evolution().default_values().map(|defaults| {
            defaults
                .iter()
                .map(|default_value| default_value.as_ref().map(|bytes| bytes.to_vec()))
                .collect()
        }),
        column_metadata: schema.column_metadata().to_vec(),
    };
    let json = serde_json::to_vec(&schema_file)
        .map_err(|err| Error::FileFormatError(format!("Failed to encode schema file: {}", err)))?;
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
    _default_num_columns: usize,
    resolver: Option<&Arc<dyn MergeOperatorResolver>>,
) -> Result<Schema> {
    let reader =
        file_manager.open_metadata_file_reader_untracked(&schema_file_relative_path(schema_id))?;
    let bytes = reader.read_at(0, reader.size())?;
    let schema_file: SchemaFile = serde_json::from_slice(bytes.as_ref())
        .map_err(|err| Error::FileFormatError(format!("Failed to decode schema file: {}", err)))?;
    if schema_file.id != schema_id {
        return Err(Error::InvalidState(format!(
            "Schema file id mismatch: expected {}, got {}",
            schema_id, schema_file.id
        )));
    }
    let mut operators: Vec<Arc<dyn MergeOperator>> = schema_file
        .merge_operator_ids
        .iter()
        .enumerate()
        .map(|(idx, id)| {
            let metadata = schema_file
                .column_metadata
                .get(idx)
                .and_then(|metadata| metadata.as_ref());
            merge_operator_by_id(id, metadata, resolver)
        })
        .collect::<Result<Vec<_>>>()?;
    let evolution = match schema_file.evolution_id.as_deref() {
        Some("noop") | None => BuiltinSchemaEvolution::Noop,
        Some("column_add") => {
            let indexes = schema_file.evolution_indexes.ok_or_else(|| {
                Error::FileFormatError(
                    "Missing evolution_indexes for column_add schema evolution".to_string(),
                )
            })?;
            let default_values = schema_file
                .evolution_default_values
                .unwrap_or_else(|| vec![None; indexes.len()])
                .into_iter()
                .map(|default_value| default_value.map(Bytes::from))
                .collect::<Vec<_>>();
            if default_values.len() != indexes.len() {
                return Err(Error::FileFormatError(format!(
                    "Invalid evolution_default_values length for column_add schema evolution: {} != {}",
                    default_values.len(),
                    indexes.len()
                )));
            }
            BuiltinSchemaEvolution::ColumnAdd {
                indexes: Arc::new(indexes),
                default_values: Arc::new(default_values),
            }
        }
        Some("column_delete") => BuiltinSchemaEvolution::ColumnDelete {
            indexes: Arc::new(schema_file.evolution_indexes.ok_or_else(|| {
                Error::FileFormatError(
                    "Missing evolution_indexes for column_delete schema evolution".to_string(),
                )
            })?),
        },
        Some(id) => {
            return Err(Error::InvalidState(format!(
                "Unknown schema evolution id '{}'",
                id
            )));
        }
    };
    let num_columns = schema_file.num_columns;
    if operators.len() < num_columns {
        operators.resize_with(num_columns, default_merge_operator);
    }
    Ok(Schema::new_with_evolution(
        schema_file.id,
        num_columns,
        operators,
        evolution,
        schema_file.column_metadata,
    ))
}

pub struct SchemaBuilder {
    manager: Arc<SchemaManager>,
    base_schema: Arc<Schema>,
    num_columns: usize,
    operators: Vec<Arc<dyn MergeOperator>>,
    evolution: BuiltinSchemaEvolution,
    column_metadata: Vec<Option<JsonValue>>,
}

impl SchemaBuilder {
    fn from_schema(manager: Arc<SchemaManager>, schema: Arc<Schema>) -> Self {
        Self {
            manager,
            base_schema: Arc::clone(&schema),
            num_columns: schema.num_columns(),
            operators: schema.operators.as_ref().clone(),
            evolution: BuiltinSchemaEvolution::Noop,
            column_metadata: schema.column_metadata().to_vec(),
        }
    }

    /// Set the merge operator for a column index.
    pub fn set_column_operator(
        &mut self,
        column_idx: usize,
        operator: Arc<dyn MergeOperator>,
    ) -> Result<()> {
        let old_num_columns = self.num_columns;
        if self.operators.is_empty() && column_idx > 0 {
            self.operators = vec![default_merge_operator(); column_idx + 1];
        } else if column_idx >= self.operators.len() {
            self.operators
                .resize_with(column_idx + 1, default_merge_operator);
        }
        if column_idx >= self.num_columns {
            self.num_columns = column_idx + 1;
        }
        if self.num_columns > old_num_columns {
            if matches!(self.evolution, BuiltinSchemaEvolution::ColumnDelete { .. }) {
                return Err(Error::InvalidState(
                    "Cannot mix add and delete schema evolution in one commit".to_string(),
                ));
            }
            let mut added_indexes = Vec::with_capacity(self.num_columns - old_num_columns);
            for idx in old_num_columns..self.num_columns {
                added_indexes.push(idx);
            }
            match &mut self.evolution {
                BuiltinSchemaEvolution::Noop => {
                    self.evolution = BuiltinSchemaEvolution::ColumnAdd {
                        indexes: Arc::new(added_indexes),
                        default_values: Arc::new(vec![None; self.num_columns - old_num_columns]),
                    };
                }
                BuiltinSchemaEvolution::ColumnAdd {
                    indexes,
                    default_values,
                } => {
                    Arc::make_mut(indexes).extend(added_indexes);
                    Arc::make_mut(default_values)
                        .extend((0..(self.num_columns - old_num_columns)).map(|_| None));
                }
                BuiltinSchemaEvolution::ColumnDelete { .. } => unreachable!(),
            }
            self.column_metadata.resize_with(self.num_columns, || None);
        } else if self.column_metadata.len() < self.num_columns {
            self.column_metadata.resize_with(self.num_columns, || None);
        }
        let operator_metadata = operator.metadata();
        self.operators[column_idx] = operator;
        if operator_metadata.is_some() {
            self.column_metadata[column_idx] = operator_metadata;
        }
        Ok(())
    }

    /// Add a new column at the specified index, shifting existing columns at and after that index to the right.
    /// Cannot be used in the same commit as `delete_column`.
    pub fn add_column(
        &mut self,
        column_idx: usize,
        operator: Option<Arc<dyn MergeOperator>>,
        default_value: Option<Bytes>,
    ) -> Result<()> {
        if column_idx > self.num_columns {
            return Err(Error::InvalidState(format!(
                "Cannot add column at index {} when schema has {} columns",
                column_idx, self.num_columns
            )));
        }
        if matches!(self.evolution, BuiltinSchemaEvolution::ColumnDelete { .. }) {
            return Err(Error::InvalidState(
                "Cannot mix add and delete schema evolution in one commit".to_string(),
            ));
        }
        self.num_columns += 1;
        self.operators
            .insert(column_idx, operator.unwrap_or_else(default_merge_operator));
        let metadata = self.operators[column_idx].metadata();
        self.column_metadata.insert(column_idx, metadata);
        match &mut self.evolution {
            BuiltinSchemaEvolution::Noop => {
                self.evolution = BuiltinSchemaEvolution::ColumnAdd {
                    indexes: Arc::new(vec![column_idx]),
                    default_values: Arc::new(vec![default_value]),
                };
            }
            BuiltinSchemaEvolution::ColumnAdd {
                indexes,
                default_values,
            } => {
                Arc::make_mut(indexes).push(column_idx);
                Arc::make_mut(default_values).push(default_value);
            }
            BuiltinSchemaEvolution::ColumnDelete { .. } => unreachable!(),
        }
        Ok(())
    }

    /// Delete the column at the specified index, shifting existing columns after that index to the left.
    /// Cannot be used in the same commit as `add_column`.
    pub fn delete_column(&mut self, column_idx: usize) -> Result<()> {
        if self.num_columns == 0 || column_idx >= self.num_columns {
            return Err(Error::InvalidState(format!(
                "Cannot delete column at index {} when schema has {} columns",
                column_idx, self.num_columns
            )));
        }
        if matches!(self.evolution, BuiltinSchemaEvolution::ColumnAdd { .. }) {
            return Err(Error::InvalidState(
                "Cannot mix add and delete schema evolution in one commit".to_string(),
            ));
        }
        self.num_columns -= 1;
        self.operators.remove(column_idx);
        self.column_metadata.remove(column_idx);
        match &mut self.evolution {
            BuiltinSchemaEvolution::Noop => {
                self.evolution = BuiltinSchemaEvolution::ColumnDelete {
                    indexes: Arc::new(vec![column_idx]),
                };
            }
            BuiltinSchemaEvolution::ColumnDelete { indexes } => {
                Arc::make_mut(indexes).push(column_idx);
            }
            BuiltinSchemaEvolution::ColumnAdd { .. } => unreachable!(),
        }
        Ok(())
    }

    /// Commit the schema changes and return the new schema.
    pub fn commit(self) -> Arc<Schema> {
        self.manager.commit_build(
            self.base_schema.as_ref(),
            self.num_columns,
            self.operators,
            self.evolution,
            self.column_metadata,
        )
    }

    pub fn set_column_metadata(&mut self, column_idx: usize, value: JsonValue) -> Result<()> {
        if column_idx >= self.num_columns {
            return Err(Error::InvalidState(format!(
                "Cannot set metadata for column {} when schema has {} columns",
                column_idx, self.num_columns
            )));
        }
        if self.column_metadata.len() < self.num_columns {
            self.column_metadata.resize_with(self.num_columns, || None);
        }
        self.column_metadata[column_idx] = Some(value);
        Ok(())
    }

    pub fn clear_column_metadata(&mut self, column_idx: usize) -> Result<()> {
        if column_idx >= self.num_columns {
            return Err(Error::InvalidState(format!(
                "Cannot clear metadata for column {} when schema has {} columns",
                column_idx, self.num_columns
            )));
        }
        if self.column_metadata.len() < self.num_columns {
            self.column_metadata.resize_with(self.num_columns, || None);
        }
        self.column_metadata[column_idx] = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_operator::MergeOperator;
    use crate::r#type::{Column, ValueType};
    use bytes::Bytes;

    struct BracketMergeOperator;

    impl MergeOperator for BracketMergeOperator {
        fn merge(
            &self,
            existing_value: Bytes,
            value: Bytes,
            _time_provider: Option<&dyn crate::TimeProvider>,
        ) -> Result<(Bytes, Option<ValueType>)> {
            let existing = existing_value.as_ref();
            Ok((
                format!(
                    "[{}+{}]",
                    String::from_utf8_lossy(existing),
                    String::from_utf8_lossy(value.as_ref())
                )
                .into_bytes()
                .into(),
                None,
            ))
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
        let (merged, _) = op
            .merge(Bytes::from_static(b"x"), Bytes::from_static(b"y"), None)
            .unwrap();
        assert_eq!(merged.as_ref(), b"[x+y]");
    }

    #[test]
    fn test_schema_falls_back_to_default_for_missing_column() {
        let manager = SchemaManager::new(1);
        let schema = manager.latest_schema();
        let op = schema.operator(10);
        let (merged, _) = op
            .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"), None)
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
        let (merged0, _) = schema
            .operator(0)
            .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"), None)
            .unwrap();
        let (merged3, _) = schema
            .operator(3)
            .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"), None)
            .unwrap();
        assert_eq!(merged0.as_ref(), b"ab");
        assert_eq!(merged3.as_ref(), b"[a+b]");
    }

    #[test]
    fn test_schema_evolution_add_column() {
        let manager = Arc::new(SchemaManager::new(1));
        let mut builder = manager.builder();
        builder.add_column(1, None, None).unwrap();
        let schema = builder.commit();
        assert_eq!(schema.version(), 1);
        assert_eq!(schema.num_columns(), 2);

        let value = Value::new(vec![Some(Column::new(
            ValueType::Put,
            Bytes::from_static(b"v"),
        ))]);
        let evolved = manager.evolve_value(value, 0, 1).unwrap();
        assert_eq!(evolved.columns().len(), 2);
        assert!(evolved.columns()[1].is_none());
    }

    #[test]
    fn test_schema_evolution_add_column_with_default() {
        let manager = Arc::new(SchemaManager::new(1));
        let mut builder = manager.builder();
        builder
            .add_column(1, None, Some(Bytes::from_static(b"default")))
            .unwrap();
        let schema = builder.commit();
        assert_eq!(schema.version(), 1);
        assert_eq!(schema.num_columns(), 2);

        let value = Value::new(vec![Some(Column::new(
            ValueType::Put,
            Bytes::from_static(b"v"),
        ))]);
        let evolved = manager.evolve_value(value, 0, 1).unwrap();
        assert_eq!(evolved.columns().len(), 2);
        let default_column = evolved.columns()[1].as_ref().unwrap();
        assert_eq!(*default_column.value_type(), ValueType::Put);
        assert_eq!(default_column.data().as_ref(), b"default");
    }

    #[test]
    fn test_schema_evolution_delete_column() {
        let manager = Arc::new(SchemaManager::new(2));
        let mut builder = manager.builder();
        builder.delete_column(1).unwrap();
        let schema = builder.commit();
        assert_eq!(schema.version(), 1);
        assert_eq!(schema.num_columns(), 1);

        let value = Value::new(vec![
            Some(Column::new(ValueType::Put, Bytes::from_static(b"v0"))),
            Some(Column::new(ValueType::Put, Bytes::from_static(b"v1"))),
        ]);
        let evolved = manager.evolve_value(value, 0, 1).unwrap();
        assert_eq!(evolved.columns().len(), 1);
        assert_eq!(
            evolved.columns()[0].as_ref().unwrap().data().as_ref(),
            b"v0"
        );
    }
}
