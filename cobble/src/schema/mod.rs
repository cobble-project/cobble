//! Runtime schemas and column-family definitions.

use crate::error::{Error, Result};
use crate::merge_operator::{MergeOperator, default_merge_operator, default_merge_operator_ref};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

mod builder;
mod evolution;
mod manager;
mod projection;

pub use builder::SchemaBuilder;
pub use evolution::{ColumnEvolution, TransformSpec};
use evolution::{SchemaEvolution, classify_columns};
pub(crate) use evolution::{SchemaTransformRegistry, TransitionCompatibility};
#[cfg(test)]
pub(crate) use manager::persist_schema;
pub(crate) use manager::{
    SchemaFile, SchemaManager, collect_schema_ids_from_manifest, schema_from_file, schema_to_file,
    snapshot_column_families_from_payload, snapshot_column_families_from_schema,
};
pub(crate) use projection::SchemaProjectionRoute;

pub(crate) const DEFAULT_COLUMN_FAMILY_ID: u8 = 0;
pub(crate) const DEFAULT_COLUMN_FAMILY_NAME: &str = "default";
pub(crate) const MAX_COLUMN_FAMILY_COUNT: usize = 230;

/// Registers executable factories for persisted schema transform specifications.
pub trait SchemaTransformRegistrar {
    /// Register one transform factory for this runtime endpoint.
    fn register_schema_transform<F, T>(
        &self,
        transform_type: impl Into<String>,
        factory: F,
    ) -> Result<()>
    where
        F: Fn(&[u8]) -> Result<T> + Send + Sync + 'static,
        T: Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync + 'static;
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
/// Column-family scoped schema options.
///
/// These options are persisted in schema metadata and applied to write/read behavior.
/// For example, when `value_has_ttl` is `false`, write-side TTL input is ignored for
/// that column family.
pub struct ColumnFamilyOptions {
    /// Whether values in this column family carry a value-level TTL timestamp.
    ///
    /// - `true`: write options TTL may set `expired_at`.
    /// - `false`: write options TTL is ignored and values are written without expiration.
    pub value_has_ttl: bool,
    /// Optional column-family metadata for higher-level structured wrappers.
    pub metadata: Option<JsonValue>,
}

/// Persisted metadata for one column family in a shard snapshot schema.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotColumnFamily {
    pub id: u8,
    pub num_columns: usize,
    pub options: ColumnFamilyOptions,
}

impl Default for ColumnFamilyOptions {
    fn default() -> Self {
        Self {
            value_has_ttl: true,
            metadata: None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ColumnFamily {
    id: u8,
    name: String,
    operators: Vec<Arc<dyn MergeOperator>>,
    column_metadata: Vec<Option<JsonValue>>,
    options: ColumnFamilyOptions,
    evolution: SchemaEvolution,
    /// Last schema version at or before this schema whose transition for this
    /// family needs materialized execution. This is derived runtime metadata
    /// and deliberately never enters the persisted schema representation.
    last_incompatible_schema_id: u64,
    projection: Option<Vec<usize>>,
}

impl ColumnFamily {
    fn new(
        id: u8,
        name: String,
        mut operators: Vec<Arc<dyn MergeOperator>>,
        mut column_metadata: Vec<Option<JsonValue>>,
        options: ColumnFamilyOptions,
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
            options,
            evolution: SchemaEvolution::identity(),
            last_incompatible_schema_id: 0,
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
        ColumnFamilyOptions::default(),
    )]
}

fn build_column_family_name_index(column_families: &[ColumnFamily]) -> HashMap<String, u8> {
    column_families
        .iter()
        .map(|family| (family.name.clone(), family.id))
        .collect()
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
    fn compile_transition_compatibility(&mut self, previous: Option<&Schema>) {
        let families = Arc::make_mut(&mut self.column_families);
        for family in families {
            let previous_family = previous.and_then(|schema| schema.column_family_by_id(family.id));
            let inherited_barrier = previous_family
                .map(|previous| previous.last_incompatible_schema_id)
                .unwrap_or(self.version);
            let compatibility = match family.evolution.columns.as_ref() {
                Some(columns) if previous.is_some() || self.version == 0 => previous_family
                    .map_or(TransitionCompatibility::Compatible, |previous| {
                        classify_columns(previous, family, columns)
                    }),
                Some(_) => TransitionCompatibility::Unknown,
                None => TransitionCompatibility::Compatible,
            };
            let columns = family.evolution.columns.as_ref().map(Arc::clone);
            let transforms = Arc::clone(&family.evolution.transforms);
            family.evolution = SchemaEvolution {
                columns,
                transforms,
                compatibility,
            };
            family.last_incompatible_schema_id =
                if compatibility == TransitionCompatibility::Compatible {
                    inherited_barrier
                } else {
                    self.version
                };
        }
    }

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
                ColumnFamilyOptions::default(),
            )],
        )
    }

    pub(crate) fn new_for_column_family(
        version: u64,
        column_family_id: u8,
        operators: Vec<Arc<dyn MergeOperator>>,
        column_metadata: Vec<Option<JsonValue>>,
        options: ColumnFamilyOptions,
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
                options,
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
                    ColumnFamilyOptions::default(),
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

    pub fn num_columns_in_family(&self, column_family_id: u8) -> Option<usize> {
        self.column_family_by_id(column_family_id)
            .map(ColumnFamily::visible_num_columns)
    }

    pub fn value_has_ttl_in_family(&self, column_family_id: u8) -> bool {
        self.column_family_by_id(column_family_id)
            .map(|family| family.options.value_has_ttl)
            .unwrap_or(true)
    }

    pub fn column_family_options_in_family(&self, column_family_id: u8) -> ColumnFamilyOptions {
        self.column_family_options_ref(column_family_id)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn column_family_options_ref(
        &self,
        column_family_id: u8,
    ) -> Option<&ColumnFamilyOptions> {
        self.column_family_by_id(column_family_id)
            .map(|family| &family.options)
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

    fn evolution_in_family(&self, column_family_id: u8) -> Result<&SchemaEvolution> {
        self.column_family_by_id(column_family_id)
            .map(|family| &family.evolution)
            .ok_or_else(|| {
                Error::InvalidState(format!(
                    "schema {} is missing column family {}",
                    self.version, column_family_id
                ))
            })
    }

    pub(crate) fn transition_compatibility_in_family(
        &self,
        column_family_id: u8,
    ) -> Result<TransitionCompatibility> {
        self.column_family_by_id(column_family_id)
            .map(|family| family.evolution.compatibility)
            .ok_or_else(|| {
                Error::InvalidState(format!(
                    "schema {} is missing column family {}",
                    self.version, column_family_id
                ))
            })
    }

    pub(crate) fn last_incompatible_schema_id_in_family(
        &self,
        column_family_id: u8,
    ) -> Option<u64> {
        self.column_family_by_id(column_family_id)
            .map(|family| family.last_incompatible_schema_id)
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

#[cfg(test)]
#[path = "../../tests/unit/schema.rs"]
mod tests;
