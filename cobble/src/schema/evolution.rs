use super::ColumnFamily;
use crate::error::{Error, Result};
use crate::r#type::{Column, Value, ValueType};
use base64::Engine as _;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};

/// Opaque configuration for a schema transform.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransformSpec {
    pub transform_type: String,
    #[serde(with = "base64_bytes")]
    pub spec: Bytes,
}

mod base64_bytes {
    use super::*;

    pub(super) fn serialize<S>(value: &Bytes, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(value))
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Bytes, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(value)
            .map(Bytes::from)
            .map_err(serde::de::Error::custom)
    }
}

/// One target column in a schema transition.
///
/// [`SchemaBuilder::remap_columns`](super::SchemaBuilder::remap_columns) accepts one
/// entry for every target column. Source indexes refer to the builder's current
/// columns, so a remap can be combined with earlier add, delete, or remap calls
/// before the schema is committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnEvolution {
    /// Retain a current source column, optionally applying a persisted transform.
    Source {
        /// Index of the source column in the builder's current column layout.
        source_index: usize,
        /// Transform descriptor resolved during schema setup.
        transform: Option<TransformSpec>,
    },
    /// Populate the target column with a fixed value when the source row is live.
    Default {
        /// Bytes to write for the target column.
        value: Bytes,
    },
    /// Populate the target column with no value when the source row is live.
    Null,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum ColumnEvolutionFile {
    Source {
        source_index: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        transform: Option<TransformSpec>,
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
                transform,
            } => Self::Source {
                source_index: *source_index,
                transform: transform.clone(),
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
                transform,
            } => Self::Source {
                source_index,
                transform,
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
#[derive(Clone)]
pub(super) struct SchemaEvolution {
    pub(super) columns: Option<Arc<Vec<ColumnEvolution>>>,
    /// Resolved once during schema setup, keyed by target column; shared by read plans.
    pub(super) transforms: Arc<HashMap<usize, Arc<dyn ExecutableTransform>>>,
    pub(super) compatibility: TransitionCompatibility,
}

impl std::fmt::Debug for SchemaEvolution {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchemaEvolution")
            .field("columns", &self.columns)
            .field("transforms", &self.transforms.len())
            .field("compatibility", &self.compatibility)
            .finish()
    }
}

impl SchemaEvolution {
    pub(super) fn identity() -> Self {
        Self {
            columns: None,
            transforms: Arc::new(HashMap::new()),
            compatibility: TransitionCompatibility::Compatible,
        }
    }

    pub(super) fn with_columns(
        columns: Vec<ColumnEvolution>,
        compatibility: TransitionCompatibility,
        transforms: HashMap<usize, Arc<dyn ExecutableTransform>>,
    ) -> Self {
        Self {
            columns: Some(Arc::new(columns)),
            transforms: Arc::new(transforms),
            compatibility,
        }
    }
}

/// Converts the logical bytes for one source column during schema evolution.
pub(crate) trait ExecutableTransform: Send + Sync {
    /// Convert a source value into the target value.
    fn apply(&self, value: Option<Bytes>) -> Result<Option<Bytes>>;
}

impl<F> ExecutableTransform for F
where
    F: Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync,
{
    fn apply(&self, value: Option<Bytes>) -> Result<Option<Bytes>> {
        self(value)
    }
}

/// Runtime registry of schema transform factories.
///
/// Schema files retain specifications; executable callbacks belong to a database instance.
#[derive(Default)]
pub(crate) struct SchemaTransformRegistry {
    factories: RwLock<HashMap<String, Arc<dyn TransformFactory>>>,
}

trait TransformFactory: Send + Sync {
    fn build(&self, spec: &[u8]) -> Result<Arc<dyn ExecutableTransform>>;
}

impl<F, T> TransformFactory for F
where
    F: Fn(&[u8]) -> Result<T> + Send + Sync,
    T: Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync + 'static,
{
    fn build(&self, spec: &[u8]) -> Result<Arc<dyn ExecutableTransform>> {
        Ok(Arc::new(self(spec)?))
    }
}

impl SchemaTransformRegistry {
    pub(crate) fn register<F, T>(&self, transform_type: impl Into<String>, factory: F) -> Result<()>
    where
        F: Fn(&[u8]) -> Result<T> + Send + Sync + 'static,
        T: Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync + 'static,
    {
        let transform_type = transform_type.into();
        if transform_type.trim().is_empty() {
            return Err(Error::InvalidState(
                "Schema transform type must not be empty".to_string(),
            ));
        }
        let mut factories = self.factories.write().unwrap();
        if factories.contains_key(&transform_type) {
            return Err(Error::InvalidState(format!(
                "Schema transform '{}' is already registered",
                transform_type
            )));
        }
        factories.insert(transform_type, Arc::new(factory));
        Ok(())
    }

    pub(super) fn resolve(
        &self,
        transform: &TransformSpec,
    ) -> Result<Arc<dyn ExecutableTransform>> {
        let factory = self
            .factories
            .read()
            .unwrap()
            .get(&transform.transform_type)
            .cloned()
            .ok_or_else(|| {
                Error::InvalidState(format!(
                    "Schema transform '{}' is not registered",
                    transform.transform_type
                ))
            })?;
        factory.build(&transform.spec)
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
                transform: None,
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
                transform: Some(transform),
                ..
            } => {
                return Err(Error::InvalidState(format!(
                    "Schema transform '{}' requires materialized row execution",
                    transform.transform_type
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
    transforms: HashMap<usize, Arc<dyn ExecutableTransform>>,
) -> SchemaEvolution {
    let Some(previous) = previous else {
        return SchemaEvolution::identity();
    };
    let identity = columns.iter().enumerate().all(|(index, evolution)| {
        matches!(
            evolution,
            ColumnEvolution::Source {
                source_index,
                transform: None,
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
            transforms,
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
            transform: None,
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
