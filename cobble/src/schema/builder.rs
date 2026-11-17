//! Stages column-family edits and compiles their transitions before commit.

use super::evolution::{ExecutableTransform, SchemaEvolution, compile_evolution};
use super::{
    ColumnEvolution, ColumnFamily, ColumnFamilyOptions, DEFAULT_COLUMN_FAMILY_ID,
    MAX_COLUMN_FAMILY_COUNT, Schema, SchemaManager, normalize_column_family_name,
};
use crate::db_status::OwnedDbAccessGuard;
use crate::error::{Error, Result};
use crate::merge_operator::{MergeOperator, default_merge_operator};
use bytes::Bytes;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

pub struct SchemaBuilder {
    manager: Arc<SchemaManager>,
    base_schema: Arc<Schema>,
    column_families: Vec<ColumnFamily>,
    column_family_name_index: HashMap<String, u8>,
    /// The complete target-to-source mapping for each column family.
    transitions: Vec<Vec<ColumnEvolution>>,
    /// Executable callbacks for transformed target columns, prepared before publication.
    resolved_transforms: Vec<HashMap<usize, Arc<dyn ExecutableTransform>>>,
    _access_guard: Option<OwnedDbAccessGuard>,
}

impl SchemaBuilder {
    pub(super) fn from_schema(
        manager: Arc<SchemaManager>,
        schema: Arc<Schema>,
        access_guard: Option<OwnedDbAccessGuard>,
    ) -> Self {
        let mut column_families = schema.column_families.as_ref().clone();
        for family in &mut column_families {
            family.evolution = SchemaEvolution::identity();
            family.projection = None;
        }
        let transitions = column_families
            .iter()
            .map(|family| {
                (0..family.num_columns())
                    .map(|source_index| ColumnEvolution::Source {
                        source_index,
                        transform: None,
                    })
                    .collect()
            })
            .collect::<Vec<_>>();
        let resolved_transforms = (0..column_families.len()).map(|_| HashMap::new()).collect();
        Self {
            manager,
            base_schema: Arc::clone(&schema),
            column_families,
            column_family_name_index: schema.column_family_name_index.as_ref().clone(),
            transitions,
            resolved_transforms,
            _access_guard: access_guard,
        }
    }

    /// Returns the current staged column count for a column family.
    ///
    /// This includes preceding add, delete, and remap operations in this
    /// builder, rather than only the schema that started the update.
    pub fn num_columns_in_family(&self, column_family: Option<String>) -> Result<usize> {
        let family_position = self.resolve_existing_family_position(column_family)?;
        Ok(self.column_families[family_position].num_columns())
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

    /// Replace one existing column's merge semantics without changing its position.
    ///
    /// This only changes the target column definition. Values retain their physical
    /// representation, so callers must use a compatible operator or a later custom
    /// transform evolution when the representation changes.
    pub fn replace_column(
        &mut self,
        column_family: Option<String>,
        column_idx: usize,
        operator: Arc<dyn MergeOperator>,
    ) -> Result<()> {
        let family_position = self.resolve_existing_family_position(column_family)?;
        let family = &mut self.column_families[family_position];
        if column_idx >= family.num_columns() {
            return Err(Error::InvalidState(format!(
                "Cannot resolve column {} in column family {} with {} columns",
                column_idx,
                family.id,
                family.num_columns()
            )));
        }
        let metadata = operator.metadata();
        family.operators[column_idx] = operator;
        family.column_metadata[column_idx] = metadata;
        Ok(())
    }

    /// Atomically replace all columns in one family from current sources, defaults, or nulls.
    ///
    /// Each entry describes one target column. A source transform must be
    /// registered through [`Db::register_schema_transform`](crate::Db::register_schema_transform) before calling
    /// this method. A single schema transition stores at most one transform per
    /// column, so remapping an already transformed source with another transform
    /// returns an error; commit an intermediate schema instead.
    pub fn remap_columns(
        &mut self,
        column_family: Option<String>,
        columns: Vec<ColumnEvolution>,
    ) -> Result<()> {
        let family_position = self.resolve_existing_family_position(column_family)?;
        let old_columns = self.transitions[family_position].len();
        let mut used_sources = vec![false; old_columns];
        for column in &columns {
            if let ColumnEvolution::Source { source_index, .. } = column {
                if *source_index >= old_columns {
                    return Err(Error::InvalidState(format!(
                        "Cannot remap source column {source_index} from {old_columns} columns"
                    )));
                }
                if std::mem::replace(&mut used_sources[*source_index], true) {
                    return Err(Error::InvalidState(format!(
                        "Cannot remap source column {source_index} more than once"
                    )));
                }
            }
        }

        let old_transition = &self.transitions[family_position];
        let old_resolved_transforms = &self.resolved_transforms[family_position];
        let composed_transition = columns
            .iter()
            .map(|column| match column {
                ColumnEvolution::Source {
                    source_index,
                    transform,
                } => match &old_transition[*source_index] {
                    ColumnEvolution::Source {
                        source_index: original_source_index,
                        transform: existing_transform,
                    } => match (existing_transform, transform) {
                        (Some(existing_transform), Some(transform)) => {
                            Err(Error::InvalidState(format!(
                                "Cannot compose schema transforms {:?} and {:?} in one schema commit; commit an intermediate schema first",
                                existing_transform, transform
                            )))
                        }
                        (Some(_), None) => Ok(old_transition[*source_index].clone()),
                        (None, transform) => Ok(ColumnEvolution::Source {
                            source_index: *original_source_index,
                            transform: transform.clone(),
                        }),
                    },
                    ColumnEvolution::Default { .. } | ColumnEvolution::Null
                        if transform.is_some() =>
                    {
                        Err(Error::InvalidState(
                            "Cannot apply a schema transform to a default or null source in one schema commit"
                                .to_string(),
                        ))
                    }
                    evolution => Ok(evolution.clone()),
                },
                evolution => Ok(evolution.clone()),
            })
            .collect::<Result<Vec<_>>>()?;

        let mut resolved_transforms = HashMap::new();
        for (target_column, column) in columns.iter().enumerate() {
            let resolved = match column {
                ColumnEvolution::Source {
                    transform: Some(transform),
                    ..
                } => Some(self.manager.transforms.resolve(transform)?),
                ColumnEvolution::Source {
                    source_index,
                    transform: None,
                } => old_resolved_transforms.get(source_index).cloned(),
                ColumnEvolution::Default { .. } | ColumnEvolution::Null => None,
            };
            if let Some(resolved) = resolved {
                resolved_transforms.insert(target_column, resolved);
            }
        }

        let family = &mut self.column_families[family_position];
        let old_operators = std::mem::take(&mut family.operators);
        let old_metadata = std::mem::take(&mut family.column_metadata);
        family.operators = columns
            .iter()
            .map(|column| match column {
                ColumnEvolution::Source { source_index, .. } => {
                    Arc::clone(&old_operators[*source_index])
                }
                ColumnEvolution::Default { .. } | ColumnEvolution::Null => default_merge_operator(),
            })
            .collect();
        family.column_metadata = columns
            .iter()
            .map(|column| match column {
                ColumnEvolution::Source { source_index, .. } => old_metadata[*source_index].clone(),
                ColumnEvolution::Default { .. } | ColumnEvolution::Null => None,
            })
            .collect();
        self.transitions[family_position] = composed_transition;
        self.resolved_transforms[family_position] = resolved_transforms;
        Ok(())
    }

    /// Commit the schema changes and return the new schema.
    pub fn commit(mut self) -> Arc<Schema> {
        for (position, family) in self.column_families.iter_mut().enumerate() {
            let previous = self.base_schema.column_families.get(position);
            family.evolution = compile_evolution(
                previous,
                family,
                &self.transitions[position],
                std::mem::take(&mut self.resolved_transforms[position]),
            );
        }
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
            ColumnFamilyOptions::default(),
        ));
        self.column_family_name_index.insert(normalized, next_id);
        self.transitions.push(Vec::new());
        self.resolved_transforms.push(HashMap::new());
        Ok(next_id)
    }

    pub fn ensure_column_family_exists(&mut self, column_family: impl Into<String>) -> Result<u8> {
        let column_family = column_family.into();
        self.ensure_column_family(&column_family)
    }

    pub fn set_column_family_value_has_ttl(
        &mut self,
        column_family: Option<String>,
        value_has_ttl: bool,
    ) -> Result<()> {
        self.set_column_family_options(
            column_family,
            ColumnFamilyOptions {
                value_has_ttl,
                ..ColumnFamilyOptions::default()
            },
        )
    }

    /// Set all persisted options for one column family.
    ///
    /// When `options.value_has_ttl` is `false`, TTL supplied through write options is
    /// ignored for that family.
    pub fn set_column_family_options(
        &mut self,
        column_family: Option<String>,
        options: ColumnFamilyOptions,
    ) -> Result<()> {
        let family_position = match column_family {
            Some(name) => usize::from(self.ensure_column_family(name.as_str())?),
            None => usize::from(DEFAULT_COLUMN_FAMILY_ID),
        };
        self.column_families[family_position].options = options;
        Ok(())
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
        for (&index, default_value) in indexes.iter().zip(default_values.iter()) {
            let target = default_value
                .as_ref()
                .cloned()
                .map_or(ColumnEvolution::Null, |value| ColumnEvolution::Default {
                    value,
                });
            self.transitions[family_position].insert(index, target);
            self.shift_resolved_transforms_for_insert(family_position, index);
        }
        Ok(())
    }

    fn record_deleted_column_for_family(
        &mut self,
        family_position: usize,
        index: usize,
    ) -> Result<()> {
        self.transitions[family_position].remove(index);
        let resolved = &mut self.resolved_transforms[family_position];
        let shifted = std::mem::take(resolved)
            .into_iter()
            .filter_map(|(column, transform)| match column.cmp(&index) {
                std::cmp::Ordering::Less => Some((column, transform)),
                std::cmp::Ordering::Equal => None,
                std::cmp::Ordering::Greater => Some((column - 1, transform)),
            })
            .collect();
        *resolved = shifted;
        Ok(())
    }

    fn shift_resolved_transforms_for_insert(&mut self, family_position: usize, index: usize) {
        let resolved = &mut self.resolved_transforms[family_position];
        *resolved = std::mem::take(resolved)
            .into_iter()
            .map(|(column, transform)| {
                (if column >= index { column + 1 } else { column }, transform)
            })
            .collect();
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
