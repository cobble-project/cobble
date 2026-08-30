use super::evolution::SchemaTransform;
use super::{ColumnEvolution, DEFAULT_COLUMN_FAMILY_ID, Schema, SchemaEvolution, SchemaManager};
use crate::error::{Error, Result};
use bytes::Bytes;
use std::sync::Arc;

#[derive(Clone)]
enum RouteSource {
    Source(usize),
    Default(Bytes),
    Null,
}

#[derive(Clone)]
struct SchemaColumnRoute {
    source: RouteSource,
    transforms: Vec<Arc<dyn SchemaTransform>>,
}

/// A compiled source-schema to target-schema column route.
#[derive(Clone)]
pub(crate) struct SchemaProjectionRoute {
    columns: Vec<SchemaColumnRoute>,
}

impl SchemaProjectionRoute {
    pub(crate) fn apply(&self, source_values: &[Option<Bytes>]) -> Result<Vec<Option<Bytes>>> {
        self.columns
            .iter()
            .map(|route| {
                let mut value = match &route.source {
                    RouteSource::Source(index) => {
                        source_values.get(*index).cloned().ok_or_else(|| {
                            Error::InvalidState(format!(
                                "Schema route references source column {} from {} columns",
                                index,
                                source_values.len()
                            ))
                        })?
                    }
                    RouteSource::Default(value) => Some(value.clone()),
                    RouteSource::Null => None,
                };
                for transform in &route.transforms {
                    value = transform.apply(value)?;
                }
                Ok(value)
            })
            .collect()
    }
}

impl Schema {
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
            family.evolution = SchemaEvolution::identity();
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
}

impl SchemaManager {
    pub(crate) fn compile_projection_route(
        &self,
        source_schema_id: u64,
        target_schema_id: u64,
        column_family_id: u8,
    ) -> Result<SchemaProjectionRoute> {
        if source_schema_id > target_schema_id {
            return Err(Error::InvalidState(format!(
                "cannot compile schema route from {} down to {}",
                source_schema_id, target_schema_id
            )));
        }
        let schemas = self.schemas.read().unwrap();
        let source_schema = schemas.get(&source_schema_id).ok_or_else(|| {
            Error::InvalidState(format!("Missing schema version {}", source_schema_id))
        })?;
        let target_schema = schemas.get(&target_schema_id).ok_or_else(|| {
            Error::InvalidState(format!("Missing schema version {}", target_schema_id))
        })?;
        let source_family = source_schema
            .column_family_by_id(column_family_id)
            .ok_or_else(|| {
                Error::InvalidState(format!(
                    "schema {} is missing column family {}",
                    source_schema_id, column_family_id
                ))
            })?;
        let target_family = target_schema
            .column_family_by_id(column_family_id)
            .ok_or_else(|| {
                Error::InvalidState(format!(
                    "schema {} is missing column family {}",
                    target_schema_id, column_family_id
                ))
            })?;
        let mut columns = (0..target_family.num_columns())
            .map(|index| SchemaColumnRoute {
                source: RouteSource::Source(index),
                transforms: Vec::new(),
            })
            .collect::<Vec<_>>();

        for schema_id in ((source_schema_id + 1)..=target_schema_id).rev() {
            let schema = schemas.get(&schema_id).ok_or_else(|| {
                Error::InvalidState(format!("Missing schema version {}", schema_id))
            })?;
            let family = schema
                .column_family_by_id(column_family_id)
                .ok_or_else(|| {
                    Error::InvalidState(format!(
                        "schema {} is missing column family {}",
                        schema_id, column_family_id
                    ))
                })?;
            let Some(evolution) = family.evolution.columns.as_ref() else {
                continue;
            };
            for route in &mut columns {
                let RouteSource::Source(target_column) = &route.source else {
                    continue;
                };
                let target_column = *target_column;
                let evolution = evolution.get(target_column).ok_or_else(|| {
                    Error::InvalidState(format!(
                        "schema {} evolution is missing target column {}",
                        schema_id, target_column
                    ))
                })?;
                match evolution {
                    ColumnEvolution::Source {
                        source_index,
                        transform_id,
                    } => {
                        if let Some(transform_id) = transform_id {
                            route
                                .transforms
                                .push(self.transforms.resolve(transform_id)?);
                        }
                        route.source = RouteSource::Source(*source_index);
                    }
                    ColumnEvolution::Default { value } => {
                        route.source = RouteSource::Default(value.clone());
                    }
                    ColumnEvolution::Null => route.source = RouteSource::Null,
                }
            }
        }
        for route in &columns {
            if let RouteSource::Source(index) = route.source
                && index >= source_family.num_columns()
            {
                return Err(Error::InvalidState(format!(
                    "schema route references source column {} from {} columns",
                    index,
                    source_family.num_columns()
                )));
            }
        }
        for route in &mut columns {
            route.transforms.reverse();
        }
        Ok(SchemaProjectionRoute { columns })
    }
}
