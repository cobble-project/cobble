use super::ColumnFamily;
use crate::error::{Error, Result};
use crate::r#type::{Column, Value, ValueType};
use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};

/// One target column in a schema transition.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnEvolution {
    Source {
        source_index: usize,
        transform_id: Option<String>,
    },
    Default {
        value: Bytes,
    },
    Null,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum ColumnEvolutionFile {
    Source {
        source_index: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        transform_id: Option<String>,
    },
    Default {
        value: Vec<u8>,
    },
    Null,
}

impl From<&ColumnEvolution> for ColumnEvolutionFile {
    fn from(evolution: &ColumnEvolution) -> Self {
        match evolution {
            ColumnEvolution::Source {
                source_index,
                transform_id,
            } => Self::Source {
                source_index: *source_index,
                transform_id: transform_id.clone(),
            },
            ColumnEvolution::Default { value } => Self::Default {
                value: value.to_vec(),
            },
            ColumnEvolution::Null => Self::Null,
        }
    }
}

impl From<ColumnEvolutionFile> for ColumnEvolution {
    fn from(evolution: ColumnEvolutionFile) -> Self {
        match evolution {
            ColumnEvolutionFile::Source {
                source_index,
                transform_id,
            } => Self::Source {
                source_index,
                transform_id,
            },
            ColumnEvolutionFile::Default { value } => Self::Default {
                value: Bytes::from(value),
            },
            ColumnEvolutionFile::Null => Self::Null,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransitionCompatibility {
    Compatible,
    Incompatible,
    Unknown,
}

/// Runtime state for one schema transition.
///
/// `columns == None` means the transition is an identity for this column family.
/// Derived execution metadata belongs here rather than in the persisted schema model.
#[derive(Clone, Debug)]
pub(super) struct SchemaEvolution {
    pub(super) columns: Option<Arc<Vec<ColumnEvolution>>>,
    pub(super) compatibility: TransitionCompatibility,
}

impl SchemaEvolution {
    pub(super) fn identity() -> Self {
        Self {
            columns: None,
            compatibility: TransitionCompatibility::Compatible,
        }
    }

    pub(super) fn with_columns(
        columns: Vec<ColumnEvolution>,
        compatibility: TransitionCompatibility,
    ) -> Self {
        Self {
            columns: Some(Arc::new(columns)),
            compatibility,
        }
    }
}

pub(crate) trait SchemaTransform: Send + Sync {
    fn apply(&self, value: Option<Bytes>) -> Result<Option<Bytes>>;
}

impl<F> SchemaTransform for F
where
    F: Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync,
{
    fn apply(&self, value: Option<Bytes>) -> Result<Option<Bytes>> {
        self(value)
    }
}

#[derive(Default)]
pub(crate) struct SchemaTransformRegistry {
    transforms: RwLock<HashMap<String, Arc<dyn SchemaTransform>>>,
}

impl SchemaTransformRegistry {
    pub(crate) fn register<F>(&self, transform_id: impl Into<String>, transform: F) -> Result<()>
    where
        F: SchemaTransform + 'static,
    {
        let transform_id = transform_id.into();
        let mut transforms = self.transforms.write().unwrap();
        if transforms.contains_key(&transform_id) {
            return Err(Error::InvalidState(format!(
                "Schema transform '{}' is already registered",
                transform_id
            )));
        }
        transforms.insert(transform_id, Arc::new(transform));
        Ok(())
    }

    pub(super) fn resolve(&self, transform_id: &str) -> Result<Arc<dyn SchemaTransform>> {
        self.transforms
            .read()
            .unwrap()
            .get(transform_id)
            .cloned()
            .ok_or_else(|| {
                Error::InvalidState(format!(
                    "Schema transform '{}' is not registered",
                    transform_id
                ))
            })
    }
}

pub(super) fn evolve_value_with_transition(
    transition: &[ColumnEvolution],
    value: Value,
) -> Result<Value> {
    let expired_at = value.expired_at();
    let mut source = value.columns;
    let source_len = source.len();
    let whole_row_delete = source_len != 0
        && source
            .iter()
            .all(|column| is_delete_column(column.as_ref()));
    let mut columns = Vec::with_capacity(transition.len());
    for target in transition {
        columns.push(match target {
            ColumnEvolution::Source {
                source_index,
                transform_id: None,
            } => source
                .get_mut(*source_index)
                .ok_or_else(|| {
                    Error::InvalidState(format!(
                        "Cannot evolve source column {source_index} from {} columns",
                        source_len
                    ))
                })?
                .take(),
            ColumnEvolution::Source {
                transform_id: Some(transform_id),
                ..
            } => {
                return Err(Error::InvalidState(format!(
                    "Schema transform '{}' requires materialized row execution",
                    transform_id
                )));
            }
            ColumnEvolution::Default { value } => {
                if whole_row_delete {
                    Some(Column::new(ValueType::Delete, Bytes::new()))
                } else {
                    Some(Column::new(ValueType::Put, value.clone()))
                }
            }
            ColumnEvolution::Null => {
                if whole_row_delete {
                    Some(Column::new(ValueType::Delete, Bytes::new()))
                } else {
                    None
                }
            }
        });
    }
    Ok(Value::new_with_expired_at(columns, expired_at))
}

fn is_delete_column(column: Option<&Column>) -> bool {
    column.is_some_and(|column| *column.value_type() == ValueType::Delete)
}

pub(super) fn compile_evolution(
    previous: Option<&ColumnFamily>,
    target: &ColumnFamily,
    columns: &[ColumnEvolution],
) -> SchemaEvolution {
    let Some(previous) = previous else {
        return SchemaEvolution::identity();
    };
    let identity = columns.iter().enumerate().all(|(index, evolution)| {
        matches!(
            evolution,
            ColumnEvolution::Source {
                source_index,
                transform_id: None,
            } if *source_index == index
        )
    });
    if identity
        && columns.len() == previous.num_columns()
        && target
            .operators
            .iter()
            .map(|operator| operator.id())
            .eq(previous.operators.iter().map(|operator| operator.id()))
        && target.column_metadata == previous.column_metadata
        && target.options == previous.options
    {
        SchemaEvolution::identity()
    } else {
        SchemaEvolution::with_columns(
            columns.to_vec(),
            classify_columns(previous, target, columns),
        )
    }
}

pub(super) fn classify_columns(
    previous: &ColumnFamily,
    target: &ColumnFamily,
    columns: &[ColumnEvolution],
) -> TransitionCompatibility {
    if previous.options != target.options {
        return TransitionCompatibility::Incompatible;
    }
    let mut source_indexes = BTreeSet::new();
    for (target_index, evolution) in columns.iter().enumerate() {
        let ColumnEvolution::Source {
            source_index,
            transform_id: None,
        } = evolution
        else {
            return TransitionCompatibility::Incompatible;
        };
        let Some(source_operator) = previous.operators.get(*source_index) else {
            return TransitionCompatibility::Incompatible;
        };
        let Some(target_operator) = target.operators.get(target_index) else {
            return TransitionCompatibility::Incompatible;
        };
        if !source_indexes.insert(*source_index)
            || source_operator.id() != target_operator.id()
            || previous.column_metadata.get(*source_index)
                != target.column_metadata.get(target_index)
        {
            return TransitionCompatibility::Incompatible;
        }
    }
    TransitionCompatibility::Compatible
}
