use crate::error::{Error, Result};
use crate::file::{BufferedWriter, File, FileManager, MetadataReader};
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
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) const DEFAULT_COLUMN_FAMILY_ID: u8 = 0;
pub(crate) const DEFAULT_COLUMN_FAMILY_NAME: &str = "default";
pub(crate) const MAX_COLUMN_FAMILY_COUNT: usize = 230;
const SCHEMA_FILE_FORMAT_VERSION: u32 = 1;

#[derive(Clone)]
pub(crate) struct ColumnFamily {
    id: u8,
    name: String,
    operators: Vec<Arc<dyn MergeOperator>>,
    column_metadata: Vec<Option<JsonValue>>,
    evolution: BuiltinSchemaEvolution,
    projection: Option<Vec<usize>>,
}

impl ColumnFamily {
    fn new(
        id: u8,
        name: String,
        mut operators: Vec<Arc<dyn MergeOperator>>,
        mut column_metadata: Vec<Option<JsonValue>>,
    ) -> Self {
        if column_metadata.len() < operators.len() {
            column_metadata.resize_with(operators.len(), || None);
        } else if column_metadata.len() > operators.len() {
            column_metadata.truncate(operators.len());
        }
        if operators.is_empty() && !column_metadata.is_empty() {
            operators.resize_with(column_metadata.len(), default_merge_operator);
        }
        Self {
            id,
            name,
            operators,
            column_metadata,
            evolution: BuiltinSchemaEvolution::Noop,
            projection: None,
        }
    }

    fn num_columns(&self) -> usize {
        self.operators.len()
    }

    fn visible_num_columns(&self) -> usize {
        self.projection
            .as_ref()
            .map_or_else(|| self.num_columns(), Vec::len)
    }

    fn resolve_visible_column_index(&self, column_idx: usize) -> usize {
        self.projection
            .as_ref()
            .map(|projection| projection[column_idx])
            .unwrap_or(column_idx)
    }

    fn operator_ids(&self) -> Vec<String> {
        self.operators
            .iter()
            .map(|operator| operator.id())
            .collect()
    }
}

fn default_column_families(num_columns: usize) -> Vec<ColumnFamily> {
    vec![ColumnFamily::new(
        DEFAULT_COLUMN_FAMILY_ID,
        DEFAULT_COLUMN_FAMILY_NAME.to_string(),
        vec![default_merge_operator(); num_columns],
        vec![None; num_columns],
    )]
}

fn build_column_family_name_index(column_families: &[ColumnFamily]) -> HashMap<String, u8> {
    column_families
        .iter()
        .map(|family| (family.name.clone(), family.id))
        .collect()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ColumnFamilyFile {
    id: u8,
    name: String,
    merge_operator_ids: Vec<String>,
    column_metadata: Vec<Option<JsonValue>>,
    evolution_id: Option<String>,
    evolution_indexes: Option<Vec<usize>>,
    evolution_default_values: Option<Vec<Option<Vec<u8>>>>,
}

fn normalize_column_family_name(name: impl Into<String>) -> Result<String> {
    let normalized = name.into().trim().to_string();
    if normalized.is_empty() {
        return Err(Error::InvalidState(
            "column family name cannot be empty".to_string(),
        ));
    }
    Ok(normalized)
}

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

#[allow(clippy::type_complexity)]
fn encode_evolution(
    evolution: &BuiltinSchemaEvolution,
) -> (
    Option<String>,
    Option<Vec<usize>>,
    Option<Vec<Option<Vec<u8>>>>,
) {
    (
        Some(evolution.id().to_string()),
        evolution.indexes().map(|indexes| indexes.to_vec()),
        evolution.default_values().map(|defaults| {
            defaults
                .iter()
                .map(|default_value| default_value.as_ref().map(|bytes| bytes.to_vec()))
                .collect()
        }),
    )
}

fn decode_evolution(
    evolution_id: Option<&str>,
    evolution_indexes: Option<Vec<usize>>,
    evolution_default_values: Option<Vec<Option<Vec<u8>>>>,
) -> Result<BuiltinSchemaEvolution> {
    match evolution_id {
        Some("noop") | None => Ok(BuiltinSchemaEvolution::Noop),
        Some("column_add") => {
            let indexes = evolution_indexes.ok_or_else(|| {
                Error::FileFormatError(
                    "Missing evolution_indexes for column_add schema evolution".to_string(),
                )
            })?;
            let default_values = evolution_default_values
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
            Ok(BuiltinSchemaEvolution::ColumnAdd {
                indexes: Arc::new(indexes),
                default_values: Arc::new(default_values),
            })
        }
        Some("column_delete") => Ok(BuiltinSchemaEvolution::ColumnDelete {
            indexes: Arc::new(evolution_indexes.ok_or_else(|| {
                Error::FileFormatError(
                    "Missing evolution_indexes for column_delete schema evolution".to_string(),
                )
            })?),
        }),
        Some(id) => Err(Error::InvalidState(format!(
            "Unknown schema evolution id '{}'",
            id
        ))),
    }
}

/// Runtime schema for merge semantics.
///
/// When `projection` is set, this schema is a lightweight view over the
/// original: `operator(i)` and `column_metadata_at(i)` remap through
/// `projection[i]` without cloning operators or metadata.
#[derive(Clone)]
pub struct Schema {
    version: u64,
    column_families: Arc<Vec<ColumnFamily>>,
    column_family_name_index: Arc<HashMap<String, u8>>,
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
        Self::new_with_column_families(
            version,
            vec![ColumnFamily::new(
                DEFAULT_COLUMN_FAMILY_ID,
                DEFAULT_COLUMN_FAMILY_NAME.to_string(),
                operators,
                vec![None; num_columns],
            )],
        )
    }

    pub(crate) fn new_for_column_family(
        version: u64,
        column_family_id: u8,
        operators: Vec<Arc<dyn MergeOperator>>,
        column_metadata: Vec<Option<JsonValue>>,
    ) -> Self {
        let family_name = if column_family_id == DEFAULT_COLUMN_FAMILY_ID {
            DEFAULT_COLUMN_FAMILY_NAME.to_string()
        } else {
            format!("remote-cf-{}", column_family_id)
        };
        Self::new_with_column_families(
            version,
            vec![ColumnFamily::new(
                column_family_id,
                family_name,
                operators,
                column_metadata,
            )],
        )
    }

    fn new_with_column_families(version: u64, mut column_families: Vec<ColumnFamily>) -> Self {
        if column_families.is_empty() {
            column_families = default_column_families(0);
        }
        if !column_families
            .iter()
            .any(|entry| entry.id == DEFAULT_COLUMN_FAMILY_ID)
        {
            column_families.insert(
                0,
                ColumnFamily::new(
                    DEFAULT_COLUMN_FAMILY_ID,
                    DEFAULT_COLUMN_FAMILY_NAME.to_string(),
                    Vec::new(),
                    Vec::new(),
                ),
            );
        }
        for family in &mut column_families {
            family.projection = None;
            if family.column_metadata.len() < family.operators.len() {
                family
                    .column_metadata
                    .resize_with(family.operators.len(), || None);
            } else if family.column_metadata.len() > family.operators.len() {
                family.column_metadata.truncate(family.operators.len());
            }
            if family.operators.is_empty() && !family.column_metadata.is_empty() {
                family
                    .operators
                    .resize_with(family.column_metadata.len(), default_merge_operator);
            }
        }
        let column_family_name_index = build_column_family_name_index(&column_families);
        Self {
            version,
            column_families: Arc::new(column_families),
            column_family_name_index: Arc::new(column_family_name_index),
        }
    }

    #[inline]
    fn column_family_id_by_name(&self, name: &str) -> Option<u8> {
        self.column_family_name_index.get(name).copied()
    }

    pub(crate) fn resolve_column_family_id(&self, column_family: Option<&str>) -> Result<u8> {
        match column_family {
            Some(name) => {
                let normalized = normalize_column_family_name(name.to_string())?;
                self.column_family_id_by_name(normalized.as_str())
                    .ok_or_else(|| {
                        Error::IoError(format!("Unknown column family '{}'", normalized))
                    })
            }
            None => Ok(DEFAULT_COLUMN_FAMILY_ID),
        }
    }

    pub(crate) fn num_columns_in_family(&self, column_family_id: u8) -> Option<usize> {
        self.column_family_by_id(column_family_id)
            .map(ColumnFamily::visible_num_columns)
    }

    pub(crate) fn column_family_id_list(&self) -> Vec<u8> {
        self.column_families
            .iter()
            .map(|family| family.id)
            .collect()
    }

    pub fn column_family_ids(&self) -> BTreeMap<String, u8> {
        self.column_families
            .iter()
            .map(|family| (family.name.clone(), family.id))
            .collect()
    }

    fn column_family_by_id(&self, column_family_id: u8) -> Option<&ColumnFamily> {
        match self.column_families.get(column_family_id as usize)? {
            family if family.id == column_family_id => Some(family),
            _ => None,
        }
    }

    /// Creates a projected schema for the given column indices.
    ///
    /// The returned schema shares the original operators and metadata via `Arc`
    /// (no content clone). `operator(i)` remaps through `selected_columns[i]`
    /// to the original schema, so merge semantics remain correct after
    /// `ColumnMaskingIterator` re-indexes columns.
    pub(crate) fn project(&self, selected_columns: &[usize]) -> Arc<Schema> {
        self.project_in_family(DEFAULT_COLUMN_FAMILY_ID, selected_columns)
    }

    pub(crate) fn project_in_family(
        &self,
        column_family_id: u8,
        selected_columns: &[usize],
    ) -> Arc<Schema> {
        let mut column_families = self.column_families.as_ref().clone();
        for family in &mut column_families {
            family.evolution = BuiltinSchemaEvolution::Noop;
            family.projection = None;
        }
        if let Some(projected_family) = column_families
            .iter_mut()
            .find(|family| family.id == column_family_id)
        {
            projected_family.projection = Some(selected_columns.to_vec());
        }
        Arc::new(Schema {
            version: self.version,
            column_families: Arc::new(column_families),
            column_family_name_index: Arc::clone(&self.column_family_name_index),
        })
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn num_columns(&self) -> usize {
        self.default_family()
            .map_or(0, ColumnFamily::visible_num_columns)
    }

    pub(crate) fn operator(&self, column_idx: usize) -> &dyn MergeOperator {
        self.operator_in_family(DEFAULT_COLUMN_FAMILY_ID, column_idx)
    }

    pub(crate) fn operator_in_family(
        &self,
        column_family_id: u8,
        column_idx: usize,
    ) -> &dyn MergeOperator {
        let family = match self.column_family_by_id(column_family_id) {
            Some(family) => family,
            None => return default_merge_operator_ref().as_ref(),
        };
        let actual_idx = family.resolve_visible_column_index(column_idx);
        family
            .operators
            .get(actual_idx)
            .unwrap_or_else(|| default_merge_operator_ref())
            .as_ref()
    }

    pub(crate) fn operator_ids(&self, num_columns: usize) -> Vec<String> {
        (0..num_columns)
            .map(|column_idx| self.operator(column_idx).id())
            .collect()
    }

    pub(crate) fn operator_ids_for_column_family_id(&self, column_family_id: u8) -> Vec<String> {
        let num_columns = self.num_columns_in_family(column_family_id).unwrap_or(0);
        (0..num_columns)
            .map(|column_idx| self.operator_in_family(column_family_id, column_idx).id())
            .collect()
    }

    /// Return the merge operator id strings for all columns.
    pub fn all_operator_ids(&self) -> Vec<String> {
        self.operator_ids(self.num_columns())
    }

    /// Return the merge operator id strings for all columns in one column family.
    pub fn operator_ids_in_family(&self, column_family: &str) -> Result<Vec<String>> {
        let column_family_id = self.resolve_column_family_id(Some(column_family))?;
        Ok(self.operator_ids_for_column_family_id(column_family_id))
    }

    pub(crate) fn empty() -> Arc<Self> {
        Arc::new(Self::new(0, 0, Vec::new()))
    }

    fn evolution(&self) -> BuiltinSchemaEvolution {
        self.default_family()
            .map(|family| family.evolution.clone())
            .unwrap_or(BuiltinSchemaEvolution::Noop)
    }

    pub(crate) fn column_metadata(&self) -> Cow<'_, [Option<JsonValue>]> {
        let family = match self.default_family() {
            Some(family) => family,
            None => return Cow::Owned(Vec::new()),
        };
        self.column_metadata_for_family(family)
    }

    pub(crate) fn column_metadata_for_column_family_id(
        &self,
        column_family_id: u8,
    ) -> Cow<'_, [Option<JsonValue>]> {
        let family = match self.column_family_by_id(column_family_id) {
            Some(family) => family,
            None => return Cow::Owned(Vec::new()),
        };
        self.column_metadata_for_family(family)
    }

    fn column_metadata_for_family<'a>(
        &self,
        family: &'a ColumnFamily,
    ) -> Cow<'a, [Option<JsonValue>]> {
        match &family.projection {
            None => Cow::Borrowed(family.column_metadata.as_slice()),
            Some(projection) => Cow::Owned(
                projection
                    .iter()
                    .map(|&idx| family.column_metadata.get(idx).cloned().flatten())
                    .collect(),
            ),
        }
    }

    pub fn column_metadata_at(
        &self,
        column_family: Option<&str>,
        column_idx: usize,
    ) -> Result<Option<&JsonValue>> {
        let column_family_id = self.resolve_column_family_id(column_family)?;
        let Some(family) = self.column_family_by_id(column_family_id) else {
            return Ok(None);
        };
        let actual_idx = family.resolve_visible_column_index(column_idx);
        Ok(family
            .column_metadata
            .get(actual_idx)
            .and_then(|metadata| metadata.as_ref()))
    }

    pub fn column_families(&self) -> Vec<(String, usize)> {
        self.column_families
            .iter()
            .map(|entry| (entry.name.clone(), entry.num_columns()))
            .collect()
    }

    fn default_family(&self) -> Option<&ColumnFamily> {
        self.column_family_by_id(DEFAULT_COLUMN_FAMILY_ID)
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
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        Self::from_manifests(file_manager, std::iter::once(manifest), resolver)
    }

    pub(crate) fn from_manifests<'a, I>(
        file_manager: &Arc<FileManager>,
        manifests: I,
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
            .map(|schema_id| load_schema(file_manager, schema_id, resolver.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::from_schemas(schemas, 1))
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
        column_families: Vec<ColumnFamily>,
    ) -> Arc<Schema> {
        let latest = self.latest_schema();
        assert_eq!(
            latest.version(),
            base_schema.version(),
            "schema builder commit version conflict"
        );
        let version = self.next_version.fetch_add(1, Ordering::SeqCst);
        let schema = Arc::new(Schema::new_with_column_families(version, column_families));
        let evolution = schema.evolution();
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
    ) -> Result<()> {
        if self.schemas.read().unwrap().contains_key(&schema_id) {
            return Ok(());
        }
        let schema = Arc::new(load_schema(file_manager, schema_id, None)?);
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
    format_version: u32,
    id: u64,
    column_families: Vec<ColumnFamilyFile>,
}

pub(crate) fn persist_schema(file_manager: &FileManager, schema: &Schema) -> Result<()> {
    let column_families = schema
        .column_families
        .iter()
        .map(|family| {
            let (evolution_id, evolution_indexes, evolution_default_values) =
                encode_evolution(&family.evolution);
            ColumnFamilyFile {
                id: family.id,
                name: family.name.clone(),
                merge_operator_ids: family.operator_ids(),
                column_metadata: family.column_metadata.clone(),
                evolution_id,
                evolution_indexes,
                evolution_default_values,
            }
        })
        .collect();
    let schema_file = SchemaFile {
        format_version: SCHEMA_FILE_FORMAT_VERSION,
        id: schema.version(),
        column_families,
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
    resolver: Option<&Arc<dyn MergeOperatorResolver>>,
) -> Result<Schema> {
    let reader =
        file_manager.open_metadata_file_reader_untracked(&schema_file_relative_path(schema_id))?;
    let payload = MetadataReader::new(reader).read_all()?;
    let schema_file: SchemaFile = serde_json::from_slice(payload.as_ref())
        .map_err(|err| Error::FileFormatError(format!("Failed to decode schema file: {}", err)))?;
    if schema_file.id != schema_id {
        return Err(Error::InvalidState(format!(
            "Schema file id mismatch: expected {}, got {}",
            schema_id, schema_file.id
        )));
    }
    if schema_file.format_version != SCHEMA_FILE_FORMAT_VERSION {
        return Err(Error::FileFormatError(format!(
            "Unsupported schema file format version {} (expected {})",
            schema_file.format_version, SCHEMA_FILE_FORMAT_VERSION
        )));
    }
    let column_families = load_column_families(&schema_file, resolver)?;
    if column_families.len() > MAX_COLUMN_FAMILY_COUNT {
        return Err(Error::FileFormatError(format!(
            "column family count {} exceeds max {}",
            column_families.len(),
            MAX_COLUMN_FAMILY_COUNT
        )));
    }
    Ok(Schema::new_with_column_families(
        schema_file.id,
        column_families,
    ))
}

fn load_column_families(
    schema_file: &SchemaFile,
    resolver: Option<&Arc<dyn MergeOperatorResolver>>,
) -> Result<Vec<ColumnFamily>> {
    let mut column_families = Vec::with_capacity(schema_file.column_families.len());
    for family in &schema_file.column_families {
        let mut operators: Vec<Arc<dyn MergeOperator>> = family
            .merge_operator_ids
            .iter()
            .enumerate()
            .map(|(idx, id)| {
                let metadata = family
                    .column_metadata
                    .get(idx)
                    .and_then(|metadata| metadata.as_ref());
                merge_operator_by_id(id, metadata, resolver)
            })
            .collect::<Result<Vec<_>>>()?;
        if operators.len() < family.column_metadata.len() {
            operators.resize_with(family.column_metadata.len(), default_merge_operator);
        }
        let mut column_family = ColumnFamily::new(
            family.id,
            family.name.clone(),
            operators,
            family.column_metadata.clone(),
        );
        column_family.evolution = decode_evolution(
            family.evolution_id.as_deref(),
            family.evolution_indexes.clone(),
            family.evolution_default_values.clone(),
        )?;
        column_families.push(column_family);
    }
    for (position, family) in column_families.iter().enumerate() {
        let expected_id = u8::try_from(position).map_err(|_| {
            Error::FileFormatError(format!(
                "column family position {} exceeds u8 range",
                position
            ))
        })?;
        if family.id != expected_id {
            return Err(Error::FileFormatError(format!(
                "column family id mismatch at position {}: expected {}, got {}",
                position, expected_id, family.id
            )));
        }
    }
    if column_families.is_empty() {
        return Err(Error::FileFormatError(
            "Schema file contains no column families".to_string(),
        ));
    }
    Ok(column_families)
}

pub struct SchemaBuilder {
    manager: Arc<SchemaManager>,
    base_schema: Arc<Schema>,
    column_families: Vec<ColumnFamily>,
    column_family_name_index: HashMap<String, u8>,
}

impl SchemaBuilder {
    fn from_schema(manager: Arc<SchemaManager>, schema: Arc<Schema>) -> Self {
        let mut column_families = schema.column_families.as_ref().clone();
        for family in &mut column_families {
            family.evolution = BuiltinSchemaEvolution::Noop;
            family.projection = None;
        }
        Self {
            manager,
            base_schema: Arc::clone(&schema),
            column_families,
            column_family_name_index: schema.column_family_name_index.as_ref().clone(),
        }
    }

    /// Set the merge operator for a column index.
    pub fn set_column_operator(
        &mut self,
        column_family: Option<String>,
        column_idx: usize,
        operator: Arc<dyn MergeOperator>,
    ) -> Result<()> {
        let family_position = self.resolve_existing_family_position(column_family)?;
        let old_num_columns = self.column_families[family_position].num_columns();
        if column_idx >= old_num_columns {
            let add_count = column_idx + 1 - old_num_columns;
            let added_indexes = (0..add_count)
                .map(|offset| old_num_columns + offset)
                .collect();
            self.record_added_columns_for_family(
                family_position,
                added_indexes,
                vec![None; add_count],
            )?;
            let default_family = &mut self.column_families[family_position];
            for _ in 0..add_count {
                default_family.operators.push(default_merge_operator());
                default_family.column_metadata.push(None);
            }
        }
        let operator_metadata = operator.metadata();
        let family = &mut self.column_families[family_position];
        family.operators[column_idx] = operator;
        if operator_metadata.is_some() {
            family.column_metadata[column_idx] = operator_metadata;
        }
        Ok(())
    }

    /// Add a new column at the specified index, shifting existing columns at and after that index to the right.
    /// `column_family` sets the target family for the new column; when `None`, it uses `default`.
    /// Cannot be used in the same commit as `delete_column`.
    pub fn add_column(
        &mut self,
        column_idx: usize,
        operator: Option<Arc<dyn MergeOperator>>,
        default_value: Option<Bytes>,
        column_family: Option<String>,
    ) -> Result<()> {
        let column_family_id = match column_family {
            Some(name) => self.ensure_column_family(name.as_str())?,
            None => DEFAULT_COLUMN_FAMILY_ID,
        };
        let family_position = usize::from(column_family_id);
        if !matches!(
            self.column_families.get(family_position),
            Some(family) if family.id == column_family_id
        ) {
            return Err(Error::InvalidState("column family not found".to_string()));
        }
        let family_column_count = self.column_families[family_position].num_columns();
        if column_idx > family_column_count {
            return Err(Error::InvalidState(format!(
                "Cannot add column {} in column family {} with {} columns",
                column_idx, column_family_id, family_column_count
            )));
        }
        self.record_added_columns_for_family(
            family_position,
            vec![column_idx],
            vec![default_value],
        )?;
        let family = &mut self.column_families[family_position];
        let merge_operator = operator.unwrap_or_else(default_merge_operator);
        let metadata = merge_operator.metadata();
        family.operators.insert(column_idx, merge_operator);
        family.column_metadata.insert(column_idx, metadata);
        Ok(())
    }

    /// Delete the column at the specified index, shifting existing columns after that index to the left.
    /// Cannot be used in the same commit as `add_column`.
    pub fn delete_column(
        &mut self,
        column_family: Option<String>,
        column_idx: usize,
    ) -> Result<()> {
        let family_position = self.resolve_existing_family_position(column_family)?;
        let family_id = self.column_families[family_position].id;
        let family_columns = self.column_families[family_position].num_columns();
        if column_idx >= family_columns {
            return Err(Error::InvalidState(format!(
                "Cannot resolve column {} in column family {} with {} columns",
                column_idx, family_id, family_columns
            )));
        }
        self.record_deleted_column_for_family(family_position, column_idx)?;
        let family = &mut self.column_families[family_position];
        family.operators.remove(column_idx);
        family.column_metadata.remove(column_idx);
        Ok(())
    }

    /// Commit the schema changes and return the new schema.
    pub fn commit(self) -> Arc<Schema> {
        self.manager
            .commit_build(self.base_schema.as_ref(), self.column_families)
    }

    fn ensure_column_family(&mut self, name: &str) -> Result<u8> {
        let normalized = normalize_column_family_name(name.to_string())?;
        if let Some(id) = self.column_family_id_by_name(normalized.as_str()) {
            return Ok(id);
        }
        if self.column_families.len() >= MAX_COLUMN_FAMILY_COUNT {
            return Err(Error::InvalidState(format!(
                "column family count exceeds max {}",
                MAX_COLUMN_FAMILY_COUNT
            )));
        }
        let next_id = u8::try_from(self.column_families.len()).map_err(|_| {
            Error::InvalidState(format!(
                "column family count exceeds max {}",
                MAX_COLUMN_FAMILY_COUNT
            ))
        })?;
        self.column_families.push(ColumnFamily::new(
            next_id,
            normalized.clone(),
            Vec::new(),
            Vec::new(),
        ));
        self.column_family_name_index.insert(normalized, next_id);
        Ok(next_id)
    }

    pub fn ensure_column_family_exists(&mut self, column_family: impl Into<String>) -> Result<u8> {
        let column_family = column_family.into();
        self.ensure_column_family(&column_family)
    }

    #[inline]
    fn column_family_id_by_name(&self, name: &str) -> Option<u8> {
        self.column_family_name_index.get(name).copied()
    }

    fn record_added_columns_for_family(
        &mut self,
        family_position: usize,
        indexes: Vec<usize>,
        default_values: Vec<Option<Bytes>>,
    ) -> Result<()> {
        if indexes.len() != default_values.len() {
            return Err(Error::InvalidState(format!(
                "Column add evolution indexes/defaults length mismatch: {} != {}",
                indexes.len(),
                default_values.len()
            )));
        }
        let evolution = &mut self.column_families[family_position].evolution;
        if matches!(evolution, BuiltinSchemaEvolution::ColumnDelete { .. }) {
            return Err(Error::InvalidState(
                "Cannot mix add and delete schema evolution in one commit".to_string(),
            ));
        }
        match evolution {
            BuiltinSchemaEvolution::Noop => {
                *evolution = BuiltinSchemaEvolution::ColumnAdd {
                    indexes: Arc::new(indexes),
                    default_values: Arc::new(default_values),
                };
            }
            BuiltinSchemaEvolution::ColumnAdd {
                indexes: existing_indexes,
                default_values: existing_default_values,
            } => {
                Arc::make_mut(existing_indexes).extend(indexes);
                Arc::make_mut(existing_default_values).extend(default_values);
            }
            BuiltinSchemaEvolution::ColumnDelete { .. } => unreachable!(),
        }
        Ok(())
    }

    fn record_deleted_column_for_family(
        &mut self,
        family_position: usize,
        index: usize,
    ) -> Result<()> {
        let evolution = &mut self.column_families[family_position].evolution;
        if matches!(evolution, BuiltinSchemaEvolution::ColumnAdd { .. }) {
            return Err(Error::InvalidState(
                "Cannot mix add and delete schema evolution in one commit".to_string(),
            ));
        }
        match evolution {
            BuiltinSchemaEvolution::Noop => {
                *evolution = BuiltinSchemaEvolution::ColumnDelete {
                    indexes: Arc::new(vec![index]),
                };
            }
            BuiltinSchemaEvolution::ColumnDelete { indexes } => {
                Arc::make_mut(indexes).push(index);
            }
            BuiltinSchemaEvolution::ColumnAdd { .. } => unreachable!(),
        }
        Ok(())
    }

    fn resolve_existing_family_position(&self, column_family: Option<String>) -> Result<usize> {
        let column_family_id = match column_family {
            Some(name) => {
                let normalized = normalize_column_family_name(name)?;
                self.column_family_id_by_name(normalized.as_str())
                    .ok_or_else(|| {
                        Error::InvalidState(format!("Unknown column family '{}'", normalized))
                    })
            }
            None => Ok(DEFAULT_COLUMN_FAMILY_ID),
        }?;
        let position = usize::from(column_family_id);
        if self
            .column_families
            .get(position)
            .is_some_and(|family| family.id == column_family_id)
        {
            Ok(position)
        } else {
            Err(Error::InvalidState("column family not found".to_string()))
        }
    }

    pub fn set_column_metadata(
        &mut self,
        column_family: Option<String>,
        column_idx: usize,
        value: JsonValue,
    ) -> Result<()> {
        let family_position = self.resolve_existing_family_position(column_family)?;
        let family = self
            .column_families
            .get_mut(family_position)
            .ok_or_else(|| Error::InvalidState("column family not found".to_string()))?;
        if column_idx >= family.num_columns() {
            return Err(Error::InvalidState(format!(
                "Cannot resolve column {} in column family {} with {} columns",
                column_idx,
                family.id,
                family.num_columns()
            )));
        }
        family.column_metadata[column_idx] = Some(value);
        Ok(())
    }

    pub fn clear_column_metadata(
        &mut self,
        column_family: Option<String>,
        column_idx: usize,
    ) -> Result<()> {
        let family_position = self.resolve_existing_family_position(column_family)?;
        let family = self
            .column_families
            .get_mut(family_position)
            .ok_or_else(|| Error::InvalidState("column family not found".to_string()))?;
        if column_idx >= family.num_columns() {
            return Err(Error::InvalidState(format!(
                "Cannot resolve column {} in column family {} with {} columns",
                column_idx,
                family.id,
                family.num_columns()
            )));
        }
        family.column_metadata[column_idx] = None;
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
            .set_column_operator(None, 1, Arc::new(BracketMergeOperator))
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
            .set_column_operator(None, 3, Arc::new(BracketMergeOperator))
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
        builder.add_column(1, None, None, None).unwrap();
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
            .add_column(1, None, Some(Bytes::from_static(b"default")), None)
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
        builder.delete_column(None, 1).unwrap();
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

    #[test]
    fn test_schema_default_column_family() {
        let manager = Arc::new(SchemaManager::new(2));
        let schema = manager.latest_schema();
        let families = schema.column_families();
        assert_eq!(families.len(), 1);
        assert_eq!(
            families[0],
            (DEFAULT_COLUMN_FAMILY_NAME.to_string(), 2usize)
        );
    }

    #[test]
    fn test_schema_builder_assign_new_column_to_new_column_family() {
        let manager = Arc::new(SchemaManager::new(1));
        let mut builder = manager.builder();
        builder
            .add_column(0, None, None, Some("metrics".to_string()))
            .expect("add column with column family");
        let schema = builder.commit();
        let families = schema.column_families();
        assert_eq!(families.len(), 2);
        assert_eq!(
            families[0],
            (DEFAULT_COLUMN_FAMILY_NAME.to_string(), 1usize)
        );
        assert_eq!(families[1], ("metrics".to_string(), 1usize));
    }

    #[test]
    fn test_schema_builder_family_local_column_indexing() {
        let manager = Arc::new(SchemaManager::new(1));
        let mut builder = manager.builder();
        builder
            .add_column(0, None, None, Some("metrics".to_string()))
            .unwrap();
        builder
            .add_column(0, None, None, Some("tags".to_string()))
            .unwrap();
        builder
            .add_column(1, None, None, Some("metrics".to_string()))
            .unwrap();
        let schema = builder.commit();
        assert_eq!(schema.num_columns(), 1);
        let families = schema.column_families();
        assert_eq!(
            families,
            vec![
                (DEFAULT_COLUMN_FAMILY_NAME.to_string(), 1usize),
                ("metrics".to_string(), 2usize),
                ("tags".to_string(), 1usize)
            ]
        );
    }

    #[test]
    fn test_schema_builder_column_family_count_limit() {
        let manager = Arc::new(SchemaManager::new(1));
        let mut builder = manager.builder();
        for i in 1..MAX_COLUMN_FAMILY_COUNT {
            builder
                .add_column(0, None, None, Some(format!("cf{}", i)))
                .expect("define column family within limit");
        }
        let err = builder
            .add_column(0, None, None, Some("overflow".to_string()))
            .expect_err("overflow should fail");
        assert!(err.to_string().contains("exceeds max"));
    }

    #[test]
    fn test_schema_public_column_family_metadata() {
        let manager = Arc::new(SchemaManager::new(1));
        let mut builder = manager.builder();
        builder
            .add_column(0, None, None, Some("metrics".to_string()))
            .unwrap();
        builder
            .set_column_operator(
                Some("metrics".to_string()),
                0,
                Arc::new(BracketMergeOperator),
            )
            .unwrap();
        let schema = builder.commit();

        assert_eq!(
            schema.column_family_ids(),
            BTreeMap::from([("default".to_string(), 0), ("metrics".to_string(), 1),])
        );
        assert_eq!(
            schema.operator_ids_in_family("metrics").unwrap(),
            vec![BracketMergeOperator.id().to_string()]
        );
    }
}
