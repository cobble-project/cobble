use crate::{LogicalType, LogicalTypeKind, Result, TableError, Value, ValueCodec};
use bytes::Bytes;
use cobble::{SchemaTransformRegistrar, TransformSpec};
use serde::{Deserialize, Serialize};

/// Transform type for lossless built-in table value representation changes.
pub(crate) const TABLE_TRANSFORM_TYPE: &str = "cobble.table/v1";

/// Install Table's built-in transforms before opening a core builder or starting a compactor.
///
/// Table builders do this automatically. Register once per target; duplicate
/// registrations are rejected by the target's normal registration rules.
pub fn register_schema_transforms(target: &impl SchemaTransformRegistrar) -> cobble::Result<()> {
    target.register_schema_transform(TABLE_TRANSFORM_TYPE, table_transform_factory)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableTransformSpec {
    source: LogicalType,
    target: LogicalType,
    operation: TableWidening,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TableWidening {
    Integer,
    Float,
    Decimal,
    TimePrecision,
    TimestampPrecision,
    Nullability,
}

pub(crate) fn compile_table_transform(
    source: &LogicalType,
    target: &LogicalType,
) -> Result<Option<TransformSpec>> {
    if source == target {
        return Ok(None);
    }
    let operation = table_transform_operation(source, target)?;
    let spec = TableTransformSpec {
        source: source.clone(),
        target: target.clone(),
        operation,
    };
    Ok(Some(TransformSpec {
        transform_type: TABLE_TRANSFORM_TYPE.to_string(),
        spec: serde_json::to_vec(&spec)
            .map(Bytes::from)
            .map_err(|error| TableError::InvalidSchema(error.to_string()))?,
    }))
}

/// Resolve a persisted built-in table transform specification.
///
/// The returned callback preserves absent columns and converts only validated
/// table value bytes; malformed specifications and values fail the operation.
pub(crate) fn table_transform_factory(
    spec: &[u8],
) -> cobble::Result<
    impl Fn(Option<Bytes>) -> cobble::Result<Option<Bytes>> + Send + Sync + 'static + use<>,
> {
    let spec: TableTransformSpec = serde_json::from_slice(spec)
        .map_err(|error| cobble::Error::InputError(error.to_string()))?;
    let operation =
        table_transform_operation(&spec.source, &spec.target).map_err(table_transform_error)?;
    if operation != spec.operation {
        return Err(cobble::Error::InputError(
            "table transform operation does not match its source and target types".to_string(),
        ));
    }
    let source = spec.source;
    let target = spec.target;
    Ok(move |value| {
        let Some(value) = value else {
            return Ok(None);
        };
        let value =
            ValueCodec::decode_bytes_validated(&source, value).map_err(table_transform_error)?;
        let value =
            convert_table_value(value, &target, operation).map_err(table_transform_error)?;
        ValueCodec::encode_validated(&target, &value)
            .map(Bytes::from)
            .map(Some)
            .map_err(table_transform_error)
    })
}

fn table_transform_operation(source: &LogicalType, target: &LogicalType) -> Result<TableWidening> {
    source.validate()?;
    target.validate()?;
    if source.nullable && !target.nullable {
        return Err(TableError::InvalidSchema(
            "table type changes cannot tighten nullability".to_string(),
        ));
    }
    let operation = match (&source.kind, &target.kind) {
        (
            LogicalTypeKind::Int8,
            LogicalTypeKind::Int16 | LogicalTypeKind::Int32 | LogicalTypeKind::Int64,
        )
        | (LogicalTypeKind::Int16, LogicalTypeKind::Int32 | LogicalTypeKind::Int64)
        | (LogicalTypeKind::Int32, LogicalTypeKind::Int64) => TableWidening::Integer,
        (LogicalTypeKind::Float32, LogicalTypeKind::Float64) => TableWidening::Float,
        (
            LogicalTypeKind::Decimal {
                precision: source_precision,
                scale: source_scale,
            },
            LogicalTypeKind::Decimal {
                precision: target_precision,
                scale: target_scale,
            },
        ) if target_precision >= source_precision && target_scale == source_scale => {
            TableWidening::Decimal
        }
        (
            LogicalTypeKind::Time {
                precision: source_precision,
            },
            LogicalTypeKind::Time {
                precision: target_precision,
            },
        ) if target_precision >= source_precision => TableWidening::TimePrecision,
        (
            LogicalTypeKind::Timestamp {
                precision: source_precision,
                timestamp_kind: source_kind,
            },
            LogicalTypeKind::Timestamp {
                precision: target_precision,
                timestamp_kind: target_kind,
            },
        ) if target_precision >= source_precision && target_kind == source_kind => {
            TableWidening::TimestampPrecision
        }
        _ if source.kind == target.kind && !source.nullable && target.nullable => {
            TableWidening::Nullability
        }
        _ => {
            return Err(TableError::InvalidSchema(
                "unsupported lossless table type change".to_string(),
            ));
        }
    };
    Ok(operation)
}

fn convert_table_value(
    value: Value,
    target: &LogicalType,
    operation: TableWidening,
) -> Result<Value> {
    if matches!(value, Value::Null) {
        return Ok(value);
    }
    match (operation, value) {
        (TableWidening::Integer, Value::Int8(value)) => match &target.kind {
            LogicalTypeKind::Int16 => Ok(Value::Int16(i16::from(value))),
            LogicalTypeKind::Int32 => Ok(Value::Int32(i32::from(value))),
            LogicalTypeKind::Int64 => Ok(Value::Int64(i64::from(value))),
            _ => Err(TableError::codec("invalid integer widening target")),
        },
        (TableWidening::Integer, Value::Int16(value)) => match &target.kind {
            LogicalTypeKind::Int32 => Ok(Value::Int32(i32::from(value))),
            LogicalTypeKind::Int64 => Ok(Value::Int64(i64::from(value))),
            _ => Err(TableError::codec("invalid integer widening target")),
        },
        (TableWidening::Integer, Value::Int32(value)) => match &target.kind {
            LogicalTypeKind::Int64 => Ok(Value::Int64(i64::from(value))),
            _ => Err(TableError::codec("invalid integer widening target")),
        },
        (TableWidening::Float, Value::Float32(value)) => Ok(Value::Float64(f64::from(value))),
        (
            TableWidening::Decimal,
            Value::Decimal {
                unscaled,
                precision: _,
                scale: _,
            },
        ) => match &target.kind {
            LogicalTypeKind::Decimal { precision, scale } => Ok(Value::Decimal {
                precision: *precision,
                scale: *scale,
                unscaled,
            }),
            _ => Err(TableError::codec("invalid decimal widening target")),
        },
        (TableWidening::TimePrecision, Value::Time(value)) => Ok(Value::Time(value)),
        (TableWidening::TimestampPrecision, Value::Timestamp { seconds, nanos, .. }) => {
            match &target.kind {
                LogicalTypeKind::Timestamp {
                    precision,
                    timestamp_kind,
                } => Ok(Value::Timestamp {
                    precision: *precision,
                    timestamp_kind: *timestamp_kind,
                    seconds,
                    nanos,
                }),
                _ => Err(TableError::codec("invalid timestamp widening target")),
            }
        }
        (TableWidening::Nullability, value) => Ok(value),
        _ => Err(TableError::codec("invalid table transform value")),
    }
}

fn table_transform_error(error: TableError) -> cobble::Error {
    cobble::Error::InputError(error.to_string())
}
