//! Owns the runtime schema registry and the persisted schema format.

use super::evolution::{ColumnEvolutionFile, SchemaEvolution, evolve_value_with_transition};
use super::{
    ColumnEvolution, ColumnFamily, ColumnFamilyOptions, MAX_COLUMN_FAMILY_COUNT, Schema,
    SchemaBuilder, SchemaTransformRegistry, SnapshotColumnFamily, TransitionCompatibility,
};
use crate::db_status::OwnedDbAccessGuard;
use crate::error::{Error, Result};
use crate::file::{BufferedWriter, File, FileManager, MetadataReader};
use crate::merge_operator::{
    MergeOperator, MergeOperatorResolver, default_merge_operator, merge_operator_by_id,
};
use crate::paths::{schema_file_path_from_snapshot_manifest_path, schema_file_relative_path};
use crate::snapshot::ManifestSnapshot;
use crate::r#type::Value;
use arc_swap::ArcSwap;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

const SCHEMA_FILE_FORMAT_VERSION: u32 = 2;

/// Manages versioned runtime schema snapshots.
#[derive(Clone)]
pub(crate) struct SchemaManager {
    latest_schema: Arc<ArcSwap<Schema>>,
    pub(super) schemas: Arc<RwLock<BTreeMap<u64, Arc<Schema>>>>,
    // Restored schema histories can be sparse; do not infer persistence from a maximum version.
    persisted_schema_ids: Arc<RwLock<BTreeSet<u64>>>,
    next_version: Arc<AtomicU64>,
    resolver: Option<Arc<dyn MergeOperatorResolver>>,
    pub(super) transforms: Arc<SchemaTransformRegistry>,
}

impl SchemaManager {
    pub(crate) fn register_transform<F, T>(
        &self,
        transform_type: impl Into<String>,
        factory: F,
    ) -> Result<()>
    where
        F: Fn(&[u8]) -> Result<T> + Send + Sync + 'static,
        T: Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync + 'static,
    {
        self.transforms.register(transform_type, factory)
    }

    pub(crate) fn new(num_columns: usize) -> Self {
        Self::from_initial_schema(Arc::new(Schema::new(
            0,
            num_columns,
            vec![default_merge_operator(); num_columns],
        )))
    }

    pub(crate) fn new_with_transform_registry(
        num_columns: usize,
        transforms: Arc<SchemaTransformRegistry>,
    ) -> Self {
        Self::from_initial_schema_with_transforms(
            Arc::new(Schema::new(
                0,
                num_columns,
                vec![default_merge_operator(); num_columns],
            )),
            transforms,
        )
    }

    pub(crate) fn new_with_runtime_wiring(
        num_columns: usize,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
        transforms: Arc<SchemaTransformRegistry>,
    ) -> Self {
        let mut manager = Self::new_with_transform_registry(num_columns, transforms);
        manager.resolver = resolver;
        manager
    }

    pub(crate) fn from_schemas(
        schemas: Vec<Schema>,
        default_num_columns: usize,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Self {
        let mut pending = schemas
            .into_iter()
            .map(|schema| (schema.version(), schema))
            .collect::<BTreeMap<_, _>>();
        let mut versions: BTreeMap<u64, Arc<Schema>> = BTreeMap::new();
        while let Some((version, mut schema)) = pending.pop_first() {
            let previous = version
                .checked_sub(1)
                .and_then(|previous_version| versions.get(&previous_version))
                .cloned();
            schema.compile_transition_compatibility(previous.as_deref());
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
                persisted_schema_ids: Arc::new(RwLock::new(BTreeSet::new())),
                next_version: Arc::new(AtomicU64::new(1)),
                resolver,
                transforms: Arc::new(SchemaTransformRegistry::default()),
            };
        }
        let (max_version, latest) = versions
            .iter()
            .next_back()
            .map(|(version, schema)| (*version, Arc::clone(schema)))
            .expect("versions not empty");
        Self {
            latest_schema: Arc::new(ArcSwap::from(latest)),
            persisted_schema_ids: Arc::new(RwLock::new(versions.keys().copied().collect())),
            schemas: Arc::new(RwLock::new(versions)),
            next_version: Arc::new(AtomicU64::new(max_version.saturating_add(1))),
            resolver,
            transforms: Arc::new(SchemaTransformRegistry::default()),
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
        Self::from_persisted_schema_ids(file_manager, schema_ids, resolver)
    }

    /// Rebuilds schemas referenced by a persisted runtime or snapshot layout.
    pub(crate) fn from_persisted_schema_ids<I>(
        file_manager: &Arc<FileManager>,
        schema_ids: I,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = u64>,
    {
        let schemas = schema_ids
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|schema_id| load_schema(file_manager, schema_id, resolver.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::from_schemas(schemas, 1, resolver))
    }

    pub(crate) fn from_snapshot_source_manifests<'a, I>(
        file_manager: &Arc<FileManager>,
        source_manifest_path: &str,
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
            .map(|schema_id| {
                let schema_path =
                    schema_file_path_from_snapshot_manifest_path(source_manifest_path, schema_id)?;
                load_schema_from_path(file_manager, schema_id, &schema_path, resolver.as_ref())
            })
            .collect::<Result<Vec<_>>>()?;
        let manager = Self::from_schemas(schemas, 1, resolver);
        manager.persisted_schema_ids.write().unwrap().clear();
        Ok(manager)
    }

    fn from_initial_schema(initial: Arc<Schema>) -> Self {
        Self::from_initial_schema_with_transforms(
            initial,
            Arc::new(SchemaTransformRegistry::default()),
        )
    }

    fn from_initial_schema_with_transforms(
        initial: Arc<Schema>,
        transforms: Arc<SchemaTransformRegistry>,
    ) -> Self {
        let next_version = initial.version().saturating_add(1);
        let mut versions = BTreeMap::new();
        versions.insert(initial.version(), Arc::clone(&initial));
        Self {
            latest_schema: Arc::new(ArcSwap::from(initial)),
            schemas: Arc::new(RwLock::new(versions)),
            persisted_schema_ids: Arc::new(RwLock::new(BTreeSet::new())),
            next_version: Arc::new(AtomicU64::new(next_version)),
            resolver: None,
            transforms,
        }
    }

    pub(crate) fn latest_schema(&self) -> Arc<Schema> {
        self.latest_schema.load_full()
    }

    /// Selects the writable schema for a restored historical state while retaining every loaded
    /// schema version in the registry. `next_version` deliberately remains above the largest
    /// loaded version so future schema ids cannot collide with snapshots that are still managed.
    pub(crate) fn select_latest_schema_for_restore(&self, schema_id: u64) -> Result<()> {
        self.latest_schema.store(self.schema(schema_id)?);
        Ok(())
    }

    pub(crate) fn merge_operator_resolver(&self) -> Option<Arc<dyn MergeOperatorResolver>> {
        self.resolver.as_ref().map(Arc::clone)
    }

    pub(crate) fn transform_registry(&self) -> Arc<SchemaTransformRegistry> {
        Arc::clone(&self.transforms)
    }

    /// Attach runtime transform implementations and resolve all loaded schemas.
    pub(crate) fn with_transform_registry(
        mut self,
        transforms: Arc<SchemaTransformRegistry>,
    ) -> Result<Self> {
        self.transforms = transforms;
        self.resolve_loaded_schema_transforms()?;
        Ok(self)
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
        self.builder_from_with_access(self.latest_schema(), None)
    }

    pub(crate) fn builder_from(self: &Arc<Self>, schema: Arc<Schema>) -> SchemaBuilder {
        self.builder_from_with_access(schema, None)
    }

    pub(crate) fn builder_with_access(
        self: &Arc<Self>,
        access_guard: Option<OwnedDbAccessGuard>,
    ) -> SchemaBuilder {
        self.builder_from_with_access(self.latest_schema(), access_guard)
    }

    pub(crate) fn builder_from_with_access(
        self: &Arc<Self>,
        schema: Arc<Schema>,
        access_guard: Option<OwnedDbAccessGuard>,
    ) -> SchemaBuilder {
        SchemaBuilder::from_schema(Arc::clone(self), schema, access_guard)
    }

    pub(super) fn commit_build(
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
        let mut schema = Schema::new_with_column_families(version, column_families);
        schema.compile_transition_compatibility(Some(base_schema));
        let schema = Arc::new(schema);
        self.latest_schema.store(Arc::clone(&schema));
        self.schemas
            .write()
            .unwrap()
            .insert(schema.version(), Arc::clone(&schema));
        schema
    }

    pub(crate) fn evolve_value_in_family(
        &self,
        mut value: Value,
        from_schema_id: u64,
        to_schema_id: u64,
        column_family_id: u8,
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
        let schemas = self.schemas.read().unwrap();
        for schema_id in (from_schema_id + 1)..=to_schema_id {
            let schema = schemas.get(&schema_id).ok_or_else(|| {
                Error::InvalidState(format!("Missing schema for version {}", schema_id))
            })?;
            if let Some(columns) = &schema.evolution_in_family(column_family_id)?.columns {
                value = evolve_value_with_transition(columns, value)?;
            }
        }
        Ok(value)
    }

    pub(crate) fn is_builtin_compatible_transition(
        &self,
        from_schema_id: u64,
        to_schema_id: u64,
        column_family_id: u8,
    ) -> Result<bool> {
        if from_schema_id > to_schema_id {
            return Err(Error::InvalidState(format!(
                "cannot evolve schema from {} down to {}",
                from_schema_id, to_schema_id
            )));
        }
        let schemas = self.schemas.read().unwrap();
        for schema_id in (from_schema_id + 1)..=to_schema_id {
            let schema = schemas.get(&schema_id).ok_or_else(|| {
                Error::InvalidState(format!("Missing schema for version {}", schema_id))
            })?;
            if schema.evolution_in_family(column_family_id)?.compatibility
                != TransitionCompatibility::Compatible
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn persist_schemas_up_to(
        &self,
        file_manager: &FileManager,
        max_schema_id: u64,
    ) -> Result<()> {
        let schemas_to_persist: Vec<Arc<Schema>> = {
            let schemas = self.schemas.read().unwrap();
            if !schemas.contains_key(&max_schema_id) {
                return Err(Error::InvalidState(format!(
                    "Missing schema version {}",
                    max_schema_id
                )));
            }
            schemas
                .range(..=max_schema_id)
                .map(|(_, schema)| Arc::clone(schema))
                .collect()
        };
        self.persist_schema_versions(file_manager, &schemas_to_persist)
    }

    pub(crate) fn persist_loaded_schemas(&self, file_manager: &FileManager) -> Result<()> {
        let schemas_to_persist: Vec<Arc<Schema>> = {
            let schemas = self.schemas.read().unwrap();
            schemas.values().cloned().collect()
        };
        self.persist_schema_versions(file_manager, &schemas_to_persist)
    }

    fn persist_schema_versions(
        &self,
        file_manager: &FileManager,
        schemas: &[Arc<Schema>],
    ) -> Result<()> {
        let mut persisted = self.persisted_schema_ids.write().unwrap();
        for schema in schemas {
            if !persisted.contains(&schema.version()) {
                persist_schema(file_manager, schema.as_ref())?;
                persisted.insert(schema.version());
            }
        }
        Ok(())
    }

    pub(crate) fn remove_unreferenced_schema_files(
        &self,
        file_manager: &FileManager,
        live_schema_ids: &BTreeSet<u64>,
    ) -> Result<()> {
        // Serialize deletion with persistence. Invalidate before any I/O so a partial failure
        // cannot cause a later snapshot to skip rewriting a missing schema.
        let mut persisted = self.persisted_schema_ids.write().unwrap();
        let obsolete = persisted
            .difference(live_schema_ids)
            .copied()
            .collect::<Vec<_>>();
        for id in obsolete {
            persisted.remove(&id);
            file_manager.remove_metadata_file(&schema_file_relative_path(id))?;
        }
        Ok(())
    }

    pub(crate) fn register_schema_from_file(
        &self,
        file_manager: &Arc<FileManager>,
        schema_id: u64,
    ) -> Result<()> {
        self.register_schema_from_file_with_resolver(
            file_manager,
            schema_id,
            self.resolver.as_ref(),
        )
    }

    /// Loads a persisted schema file from the shared metadata volume and registers it, preserving
    /// its original schema version. The merge-operator resolver lets column families that reference
    /// custom operators resolve to the correct implementation; pass `None` to fall back to the
    /// built-in operators. Used by snapshot/restore to rebuild the writer's schema registry.
    pub(crate) fn register_schema_from_file_with_resolver(
        &self,
        file_manager: &Arc<FileManager>,
        schema_id: u64,
        resolver: Option<&Arc<dyn MergeOperatorResolver>>,
    ) -> Result<()> {
        if self.schemas.read().unwrap().contains_key(&schema_id) {
            return Ok(());
        }
        let mut schema = load_schema(file_manager, schema_id, resolver)?;
        self.resolve_schema_transforms(&mut schema)?;
        self.register_loaded_schema(schema);
        self.persisted_schema_ids.write().unwrap().insert(schema_id);
        Ok(())
    }

    /// Registers an in-memory `SchemaFile` definition (e.g. received over the remote-compaction
    /// protocol) into the registry, preserving its original schema version. This is how the remote
    /// compactor reconstructs the writer's schema registry: the writer carries the schema
    /// definitions it needs in the compaction request, so the compactor can decode input SST files
    /// stamped with non-zero schema ids and stamp output with the writer's latest schema version —
    /// without reading the shared volume.
    pub(crate) fn register_schema_from_def(
        &self,
        schema_file: &SchemaFile,
        resolver: Option<&Arc<dyn MergeOperatorResolver>>,
    ) -> Result<()> {
        if self.schemas.read().unwrap().contains_key(&schema_file.id) {
            return Ok(());
        }
        let mut schema = schema_from_file(schema_file, resolver)?;
        self.resolve_schema_transforms(&mut schema)?;
        self.register_loaded_schema(schema);
        Ok(())
    }

    /// Inserts an already-loaded schema into the registry, promotes it to latest when it is newer
    /// than the current latest, and advances `next_version` past it. ArcSwap guarantees the
    /// promotion is atomic; a concurrent loader that registers an even-newer version simply wins,
    /// which is correct because both are writer-persisted schemas.
    fn register_loaded_schema(&self, schema: Schema) {
        let mut schemas = self.schemas.write().unwrap();
        let version = schema.version();
        schemas.insert(version, Arc::new(schema));
        // Remote schema files can arrive out of order. Recompile this schema
        // and its loaded successors so each one inherits the corrected barrier.
        let versions = schemas
            .range(version..)
            .map(|(version, _)| *version)
            .collect::<Vec<_>>();
        for version in versions {
            let previous = version
                .checked_sub(1)
                .and_then(|previous_version| schemas.get(&previous_version).cloned());
            let mut current = schemas
                .get(&version)
                .expect("loaded schema is present")
                .as_ref()
                .clone();
            current.compile_transition_compatibility(previous.as_deref());
            schemas.insert(version, Arc::new(current));
        }
        if let Some((_, latest)) = schemas.last_key_value() {
            self.latest_schema.store(Arc::clone(latest));
        }
        drop(schemas);
        let target_next = version.saturating_add(1);
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
    }

    fn resolve_loaded_schema_transforms(&self) -> Result<()> {
        let loaded = self
            .schemas
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut resolved = Vec::with_capacity(loaded.len());
        for schema in loaded {
            let mut schema = schema.as_ref().clone();
            self.resolve_schema_transforms(&mut schema)?;
            resolved.push(Arc::new(schema));
        }
        let latest_version = self.latest_schema().version();
        let mut schemas = self.schemas.write().unwrap();
        for schema in resolved {
            schemas.insert(schema.version(), Arc::clone(&schema));
            if schema.version() == latest_version {
                self.latest_schema.store(schema);
            }
        }
        Ok(())
    }

    fn resolve_schema_transforms(&self, schema: &mut Schema) -> Result<()> {
        let families = Arc::make_mut(&mut schema.column_families);
        for family in families {
            let Some(columns) = family.evolution.columns.as_ref() else {
                continue;
            };
            let mut transforms = HashMap::new();
            for (column_index, column) in columns.iter().enumerate() {
                if let ColumnEvolution::Source {
                    transform: Some(transform),
                    ..
                } = column
                {
                    transforms.insert(column_index, self.transforms.resolve(transform)?);
                }
            }
            family.evolution.transforms = Arc::new(transforms);
        }
        Ok(())
    }
}

pub(crate) fn collect_schema_ids_from_manifest(
    manifest: &ManifestSnapshot,
    schema_ids: &mut BTreeSet<u64>,
) {
    let latest_schema_id = manifest.latest_schema_id;
    let mut min_schema_id = latest_schema_id;
    for tree_levels in &manifest.tree_levels {
        for level in tree_levels {
            for file in &level.files {
                if file.schema_id <= latest_schema_id {
                    min_schema_id = min_schema_id.min(file.schema_id);
                } else {
                    schema_ids.insert(file.schema_id);
                }
            }
        }
    }
    // Intermediate evolution steps are required even when no SST references them.
    schema_ids.extend(min_schema_id..=latest_schema_id);
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ColumnFamilyFile {
    id: u8,
    name: String,
    merge_operator_ids: Vec<String>,
    column_metadata: Vec<Option<JsonValue>>,
    options: ColumnFamilyOptions,
    pub(super) transition: Option<Vec<ColumnEvolutionFile>>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SchemaFile {
    pub(crate) format_version: u32,
    pub(crate) id: u64,
    pub(crate) column_families: Vec<ColumnFamilyFile>,
}

// v0.4.0 persisted these fields instead of v2's `transition`. This
// compatibility path accepts only identity evolutions.
#[derive(Deserialize)]
struct LegacyV1ColumnFamilyFile {
    id: u8,
    name: String,
    merge_operator_ids: Vec<String>,
    column_metadata: Vec<Option<JsonValue>>,
    options: ColumnFamilyOptions,
    evolution_id: Option<String>,
    evolution_indexes: Option<Vec<usize>>,
    evolution_default_values: Option<Vec<Option<Vec<u8>>>>,
}

impl<'de> Deserialize<'de> for SchemaFile {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireSchemaFile {
            format_version: u32,
            id: u64,
            column_families: Vec<JsonValue>,
        }

        let wire = WireSchemaFile::deserialize(deserializer)?;
        if !matches!(wire.format_version, 1 | SCHEMA_FILE_FORMAT_VERSION) {
            return Err(serde::de::Error::custom(format!(
                "Unsupported schema file format version {} (expected 1 or {})",
                wire.format_version, SCHEMA_FILE_FORMAT_VERSION
            )));
        }
        let mut column_families = Vec::with_capacity(wire.column_families.len());
        for (position, family) in wire.column_families.into_iter().enumerate() {
            let decoded = match wire.format_version {
                SCHEMA_FILE_FORMAT_VERSION => serde_json::from_value(family),
                1 => {
                    let legacy: LegacyV1ColumnFamilyFile =
                        serde_json::from_value(family).map_err(|err| {
                            serde::de::Error::custom(format!(
                                "invalid v1 column family at position {}: {}",
                                position, err
                            ))
                        })?;
                    if !matches!(legacy.evolution_id.as_deref(), None | Some("noop"))
                        || legacy
                            .evolution_indexes
                            .as_ref()
                            .is_some_and(|indexes| !indexes.is_empty())
                        || legacy
                            .evolution_default_values
                            .as_ref()
                            .is_some_and(|defaults| !defaults.is_empty())
                    {
                        return Err(serde::de::Error::custom(format!(
                            "unsupported v1 schema evolution in column family '{}'",
                            legacy.name
                        )));
                    }
                    Ok(ColumnFamilyFile {
                        id: legacy.id,
                        name: legacy.name,
                        merge_operator_ids: legacy.merge_operator_ids,
                        column_metadata: legacy.column_metadata,
                        options: legacy.options,
                        transition: None,
                    })
                }
                _ => unreachable!("schema file version checked above"),
            }
            .map_err(|err| {
                serde::de::Error::custom(format!(
                    "invalid column family at position {}: {}",
                    position, err
                ))
            })?;
            column_families.push(decoded);
        }
        Ok(Self {
            format_version: SCHEMA_FILE_FORMAT_VERSION,
            id: wire.id,
            column_families,
        })
    }
}

pub(crate) fn persist_schema(file_manager: &FileManager, schema: &Schema) -> Result<()> {
    let schema_file = schema_to_file(schema);
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

/// Serializes a `Schema` into the portable `SchemaFile` representation used both for on-disk
/// persistence and for the remote-compaction protocol. Carrying `SchemaFile` in the request lets
/// the compactor reconstruct the writer's schema registry without reading the shared volume, so
/// remote compaction works even when it is triggered by a flush before the first checkpoint
/// persists schemas.
pub(crate) fn schema_to_file(schema: &Schema) -> SchemaFile {
    let column_families = schema
        .column_families
        .iter()
        .map(|family| ColumnFamilyFile {
            id: family.id,
            name: family.name.clone(),
            merge_operator_ids: family.operator_ids(),
            column_metadata: family.column_metadata.clone(),
            options: family.options.clone(),
            transition: family
                .evolution
                .columns
                .as_ref()
                .map(|columns| columns.iter().map(ColumnEvolutionFile::from).collect()),
        })
        .collect();
    SchemaFile {
        format_version: SCHEMA_FILE_FORMAT_VERSION,
        id: schema.version(),
        column_families,
    }
}

/// Deserializes a `SchemaFile` (received over the protocol or read from disk) into a `Schema`,
/// resolving merge operators via the resolver. Validates the format version and column-family
/// invariants.
pub(crate) fn schema_from_file(
    schema_file: &SchemaFile,
    resolver: Option<&Arc<dyn MergeOperatorResolver>>,
) -> Result<Schema> {
    if schema_file.format_version != SCHEMA_FILE_FORMAT_VERSION {
        return Err(Error::FileFormatError(format!(
            "Unsupported schema file format version {} (expected {})",
            schema_file.format_version, SCHEMA_FILE_FORMAT_VERSION
        )));
    }
    let column_families = load_column_families(schema_file, resolver)?;
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

pub(super) fn schema_file_from_payload(payload: &[u8]) -> Result<SchemaFile> {
    serde_json::from_slice(payload)
        .map_err(|err| Error::FileFormatError(format!("Failed to decode schema file: {}", err)))
}

pub(crate) fn snapshot_column_families_from_payload(
    payload: &[u8],
    schema_id: u64,
) -> Result<BTreeMap<String, SnapshotColumnFamily>> {
    let schema_file = schema_file_from_payload(payload)?;
    if schema_file.format_version != SCHEMA_FILE_FORMAT_VERSION {
        return Err(Error::FileFormatError(format!(
            "Unsupported schema file format version {} (expected {})",
            schema_file.format_version, SCHEMA_FILE_FORMAT_VERSION
        )));
    }
    if schema_file.id != schema_id {
        return Err(Error::InvalidState(format!(
            "Schema file id mismatch: expected {}, got {}",
            schema_id, schema_file.id
        )));
    }
    if schema_file.column_families.len() > MAX_COLUMN_FAMILY_COUNT {
        return Err(Error::FileFormatError(format!(
            "column family count {} exceeds max {}",
            schema_file.column_families.len(),
            MAX_COLUMN_FAMILY_COUNT
        )));
    }
    let mut column_families = BTreeMap::new();
    for family in schema_file.column_families {
        if column_families
            .insert(
                family.name,
                SnapshotColumnFamily {
                    id: family.id,
                    num_columns: family
                        .merge_operator_ids
                        .len()
                        .max(family.column_metadata.len()),
                    options: family.options,
                },
            )
            .is_some()
        {
            return Err(Error::FileFormatError(
                "schema file contains duplicate column family names".to_string(),
            ));
        }
    }
    if column_families.is_empty() {
        return Err(Error::FileFormatError(
            "Schema file contains no column families".to_string(),
        ));
    }
    Ok(column_families)
}

pub(crate) fn snapshot_column_families_from_schema(
    schema: &Schema,
) -> BTreeMap<String, SnapshotColumnFamily> {
    schema
        .column_families
        .iter()
        .map(|family| {
            (
                family.name.clone(),
                SnapshotColumnFamily {
                    id: family.id,
                    num_columns: family.num_columns(),
                    options: family.options.clone(),
                },
            )
        })
        .collect()
}

pub(crate) fn load_schema(
    file_manager: &Arc<FileManager>,
    schema_id: u64,
    resolver: Option<&Arc<dyn MergeOperatorResolver>>,
) -> Result<Schema> {
    let reader =
        file_manager.open_metadata_file_reader_untracked(&schema_file_relative_path(schema_id))?;
    let payload = MetadataReader::new(reader).read_all()?;
    let schema_file = schema_file_from_payload(payload.as_ref())?;
    if schema_file.id != schema_id {
        return Err(Error::InvalidState(format!(
            "Schema file id mismatch: expected {}, got {}",
            schema_id, schema_file.id
        )));
    }
    schema_from_file(&schema_file, resolver)
}

pub(crate) fn load_schema_from_path(
    file_manager: &Arc<FileManager>,
    schema_id: u64,
    schema_path: &str,
    resolver: Option<&Arc<dyn MergeOperatorResolver>>,
) -> Result<Schema> {
    let reader = file_manager.open_metadata_file_reader_at_path(schema_path)?;
    let payload = MetadataReader::new(reader).read_all()?;
    let schema_file = schema_file_from_payload(payload.as_ref())?;
    if schema_file.id != schema_id {
        return Err(Error::InvalidState(format!(
            "Schema file id mismatch: expected {}, got {}",
            schema_id, schema_file.id
        )));
    }
    schema_from_file(&schema_file, resolver)
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
            family.options.clone(),
        );
        column_family.evolution =
            family
                .transition
                .as_ref()
                .map_or_else(SchemaEvolution::identity, |transition| {
                    SchemaEvolution::with_columns(
                        transition
                            .iter()
                            .cloned()
                            .map(ColumnEvolution::from)
                            .collect(),
                        TransitionCompatibility::Unknown,
                        HashMap::new(),
                    )
                });
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
