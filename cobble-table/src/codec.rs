use crate::{LogicalType, LogicalTypeKind, Result, TableError, TimestampKind};
use bytes::Bytes;
use std::ops::Range;

const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// Schema-directed dynamic value used by the `cobble-table-v1` codec.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Decimal {
        precision: u8,
        scale: u8,
        unscaled: i128,
    },
    Date(i32),
    Time(i64),
    Timestamp {
        precision: u8,
        timestamp_kind: TimestampKind,
        seconds: i64,
        nanos: u32,
    },
    String(String),
    Binary(Bytes),
    List(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Struct(Vec<Value>),
    Extension {
        type_id: String,
        value: Box<Value>,
    },
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::Binary(Bytes::from(value))
    }
}

impl From<Bytes> for Value {
    fn from(value: Bytes) -> Self {
        Self::Binary(value)
    }
}

/// Ordered, schema-directed composite-key codec.
pub struct KeyCodec;

impl KeyCodec {
    pub fn encode_row(types: &[LogicalType], values: &[Value]) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        Self::encode_row_into(types, values, &mut out)?;
        Ok(out)
    }

    pub fn encode_row_into(
        types: &[LogicalType],
        values: &[Value],
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let start = out.len();
        let result = Self::append_row(types, values, out);
        if result.is_err() {
            out.truncate(start);
        }
        result
    }

    /// Encodes a row whose logical types were already validated when its table was opened.
    pub(crate) fn encode_row_with_prefix_validated(
        types: &[LogicalType],
        values: &[Value],
        prefix_fields: usize,
        out: &mut Vec<u8>,
    ) -> Result<usize> {
        let start = out.len();
        let result = (|| {
            if types.len() != values.len() {
                return Err(TableError::codec(
                    "key field count does not match value count",
                ));
            }
            debug_assert!(prefix_fields <= types.len());
            let mut prefix_end = start;
            for (index, (logical_type, value)) in types.iter().zip(values).enumerate() {
                encode_key(logical_type, value, out)?;
                if index + 1 == prefix_fields {
                    prefix_end = out.len();
                }
            }
            Ok(prefix_end - start)
        })();
        if result.is_err() {
            out.truncate(start);
        }
        result
    }

    /// Encodes selected fields from a row whose logical types were validated at table creation.
    pub(crate) fn encode_row_from_positions_validated(
        types: &[LogicalType],
        row: &[Value],
        positions: &[usize],
        prefix_fields: usize,
    ) -> Result<(Vec<u8>, usize)> {
        debug_assert_eq!(types.len(), positions.len());
        debug_assert!(prefix_fields <= types.len());
        let mut encoded = Vec::new();
        let mut prefix_end = 0;
        for (index, (logical_type, position)) in types.iter().zip(positions).enumerate() {
            encode_key(logical_type, &row[*position], &mut encoded)?;
            if index + 1 == prefix_fields {
                prefix_end = encoded.len();
            }
        }
        Ok((encoded, prefix_end))
    }

    fn append_row(types: &[LogicalType], values: &[Value], out: &mut Vec<u8>) -> Result<()> {
        if types.len() != values.len() {
            return Err(TableError::codec(
                "key field count does not match value count",
            ));
        }
        for (logical_type, value) in types.iter().zip(values) {
            logical_type.validate()?;
            encode_key(logical_type, value, out)?;
        }
        Ok(())
    }

    pub fn encode_scalar(logical_type: &LogicalType, value: &Value) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        logical_type.validate()?;
        encode_key(logical_type, value, &mut out)?;
        Ok(out)
    }

    pub fn decode_row(types: &[LogicalType], encoded: &[u8]) -> Result<Vec<Value>> {
        for logical_type in types {
            logical_type.validate()?;
        }
        Self::decode_row_validated(types, encoded)
    }

    /// Decodes a key with types validated at table creation.
    pub(crate) fn decode_row_validated(
        types: &[LogicalType],
        encoded: &[u8],
    ) -> Result<Vec<Value>> {
        let mut offset = 0;
        let mut values = Vec::with_capacity(types.len());
        for logical_type in types {
            let (value, consumed) = decode_key(logical_type, &encoded[offset..])?;
            offset += consumed;
            values.push(value);
        }
        ensure_consumed(encoded.len(), offset)?;
        Ok(values)
    }

    /// Decodes a key directly into its row positions for an already validated table layout.
    pub(crate) fn decode_row_into_positions_validated(
        types: &[LogicalType],
        encoded: &[u8],
        positions: &[usize],
        row: &mut [Value],
    ) -> Result<()> {
        debug_assert_eq!(types.len(), positions.len());
        debug_assert!(positions.iter().all(|position| *position < row.len()));
        let mut offset = 0;
        for (logical_type, position) in types.iter().zip(positions) {
            let (value, consumed) = decode_key(logical_type, &encoded[offset..])?;
            row[*position] = value;
            offset += consumed;
        }
        ensure_consumed(encoded.len(), offset)
    }

    pub fn decode_scalar(logical_type: &LogicalType, encoded: &[u8]) -> Result<Value> {
        logical_type.validate()?;
        let (value, consumed) = decode_key(logical_type, encoded)?;
        ensure_consumed(encoded.len(), consumed)?;
        Ok(value)
    }
}

/// Schema-directed value codec. `encode_into` appends to caller-owned memory.
pub struct ValueCodec;

impl ValueCodec {
    pub fn encode(logical_type: &LogicalType, value: &Value) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        Self::encode_into(logical_type, value, &mut out)?;
        Ok(out)
    }

    pub fn encode_into(logical_type: &LogicalType, value: &Value, out: &mut Vec<u8>) -> Result<()> {
        let start = out.len();
        let result = logical_type
            .validate()
            .and_then(|_| encode_value(logical_type, value, out));
        if result.is_err() {
            out.truncate(start);
        }
        result
    }

    /// Encodes a value whose logical type was validated at table creation.
    pub(crate) fn encode_validated(logical_type: &LogicalType, value: &Value) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        encode_value(logical_type, value, &mut out)?;
        Ok(out)
    }

    pub fn encoded_size(logical_type: &LogicalType, value: &Value) -> Result<usize> {
        logical_type.validate()?;
        value_size(logical_type, value)
    }

    pub fn decode(logical_type: &LogicalType, encoded: &[u8]) -> Result<Value> {
        logical_type.validate()?;
        decode_value(logical_type, Input::Borrowed(encoded))
    }

    /// Decode an owned buffer. Every `Binary` value, including nested values, keeps a zero-copy
    /// slice of `encoded`; retain the returned value only while retaining that allocation.
    pub fn decode_bytes(logical_type: &LogicalType, encoded: Bytes) -> Result<Value> {
        logical_type.validate()?;
        decode_value(logical_type, Input::Owned(encoded))
    }

    /// Decodes a value with a logical type validated at table creation.
    pub(crate) fn decode_bytes_validated(
        logical_type: &LogicalType,
        encoded: Bytes,
    ) -> Result<Value> {
        decode_value(logical_type, Input::Owned(encoded))
    }
}

enum Input<'a> {
    Borrowed(&'a [u8]),
    Owned(Bytes),
}

impl Input<'_> {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(bytes) => bytes,
        }
    }

    fn slice(&self, range: Range<usize>) -> Self {
        match self {
            Self::Borrowed(bytes) => Self::Borrowed(&bytes[range]),
            Self::Owned(bytes) => Self::Owned(bytes.slice(range)),
        }
    }

    fn into_bytes(self) -> Bytes {
        match self {
            Self::Borrowed(bytes) => Bytes::copy_from_slice(bytes),
            Self::Owned(bytes) => bytes,
        }
    }
}

fn encode_key(logical_type: &LogicalType, value: &Value, out: &mut Vec<u8>) -> Result<()> {
    if !logical_type.is_key_compatible() {
        return Err(TableError::codec("type cannot be used as a key"));
    }
    match (&logical_type.kind, value) {
        (LogicalTypeKind::Boolean, Value::Boolean(value)) => out.push(u8::from(*value)),
        (LogicalTypeKind::Int8, Value::Int8(value)) => ordered_i128(*value as i128, 1, out),
        (LogicalTypeKind::Int16, Value::Int16(value)) => ordered_i128(*value as i128, 2, out),
        (LogicalTypeKind::Int32, Value::Int32(value)) => ordered_i128(*value as i128, 4, out),
        (LogicalTypeKind::Int64, Value::Int64(value)) => ordered_i128(*value as i128, 8, out),
        (
            LogicalTypeKind::Decimal { precision, scale },
            Value::Decimal {
                precision: actual_precision,
                scale: actual_scale,
                unscaled,
            },
        ) => {
            validate_decimal(
                *precision,
                *scale,
                *actual_precision,
                *actual_scale,
                *unscaled,
            )?;
            ordered_i128(*unscaled, decimal_width(*precision), out);
        }
        (LogicalTypeKind::Date, Value::Date(value)) => ordered_i128(*value as i128, 4, out),
        (LogicalTypeKind::Time { precision }, Value::Time(value)) => {
            validate_time(*value, *precision)?;
            ordered_i128(*value as i128, 8, out);
        }
        (
            LogicalTypeKind::Timestamp {
                precision,
                timestamp_kind,
            },
            Value::Timestamp {
                precision: actual_precision,
                timestamp_kind: actual_kind,
                seconds,
                nanos,
            },
        ) => {
            validate_timestamp(
                *precision,
                *timestamp_kind,
                *actual_precision,
                *actual_kind,
                *seconds,
                *nanos,
            )?;
            ordered_i128(*seconds as i128, 8, out);
            out.extend_from_slice(&nanos.to_be_bytes());
        }
        (LogicalTypeKind::String, Value::String(value)) => append_escaped(value.as_bytes(), out),
        (LogicalTypeKind::Binary, Value::Binary(value)) => append_escaped(value, out),
        _ => return Err(type_mismatch(logical_type, value)),
    }
    Ok(())
}

fn decode_key(logical_type: &LogicalType, encoded: &[u8]) -> Result<(Value, usize)> {
    if !logical_type.is_key_compatible() {
        return Err(TableError::codec("type cannot be used as a key"));
    }
    match &logical_type.kind {
        LogicalTypeKind::Boolean => Ok((Value::Boolean(read_bool(encoded)?), 1)),
        LogicalTypeKind::Int8 => Ok((Value::Int8(read_ordered(encoded, 1)? as i8), 1)),
        LogicalTypeKind::Int16 => Ok((Value::Int16(read_ordered(encoded, 2)? as i16), 2)),
        LogicalTypeKind::Int32 => Ok((Value::Int32(read_ordered(encoded, 4)? as i32), 4)),
        LogicalTypeKind::Int64 => Ok((Value::Int64(read_ordered(encoded, 8)? as i64), 8)),
        LogicalTypeKind::Decimal { precision, scale } => {
            let width = decimal_width(*precision);
            let unscaled = read_ordered(encoded, width)?;
            validate_decimal(*precision, *scale, *precision, *scale, unscaled)?;
            Ok((
                Value::Decimal {
                    precision: *precision,
                    scale: *scale,
                    unscaled,
                },
                width,
            ))
        }
        LogicalTypeKind::Date => Ok((Value::Date(read_ordered(encoded, 4)? as i32), 4)),
        LogicalTypeKind::Time { precision } => {
            let value = read_ordered(encoded, 8)? as i64;
            validate_time(value, *precision)?;
            Ok((Value::Time(value), 8))
        }
        LogicalTypeKind::Timestamp {
            precision,
            timestamp_kind,
        } => {
            require_len(encoded, 12)?;
            let seconds = read_ordered(encoded, 8)? as i64;
            let nanos = u32::from_be_bytes(encoded[8..12].try_into().unwrap());
            validate_timestamp(
                *precision,
                *timestamp_kind,
                *precision,
                *timestamp_kind,
                seconds,
                nanos,
            )?;
            Ok((
                Value::Timestamp {
                    precision: *precision,
                    timestamp_kind: *timestamp_kind,
                    seconds,
                    nanos,
                },
                12,
            ))
        }
        LogicalTypeKind::String => {
            let (bytes, consumed) = read_escaped(encoded)?;
            let value =
                String::from_utf8(bytes).map_err(|_| TableError::codec("invalid UTF-8 key"))?;
            Ok((Value::String(value), consumed))
        }
        LogicalTypeKind::Binary => {
            let (bytes, consumed) = read_escaped(encoded)?;
            Ok((Value::Binary(Bytes::from(bytes)), consumed))
        }
        _ => Err(TableError::codec("type cannot be used as a key")),
    }
}

fn encode_value(logical_type: &LogicalType, value: &Value, out: &mut Vec<u8>) -> Result<()> {
    if logical_type.nullable {
        match value {
            Value::Null => {
                out.push(0);
                return Ok(());
            }
            _ => out.push(1),
        }
    } else if matches!(value, Value::Null) {
        return Err(type_mismatch(logical_type, value));
    }
    encode_non_null(logical_type, value, out)
}

fn encode_non_null(logical_type: &LogicalType, value: &Value, out: &mut Vec<u8>) -> Result<()> {
    match (&logical_type.kind, value) {
        (LogicalTypeKind::Boolean, Value::Boolean(value)) => out.push(u8::from(*value)),
        (LogicalTypeKind::Int8, Value::Int8(value)) => ordered_i128(*value as i128, 1, out),
        (LogicalTypeKind::Int16, Value::Int16(value)) => ordered_i128(*value as i128, 2, out),
        (LogicalTypeKind::Int32, Value::Int32(value)) => ordered_i128(*value as i128, 4, out),
        (LogicalTypeKind::Int64, Value::Int64(value)) => ordered_i128(*value as i128, 8, out),
        (LogicalTypeKind::Float32, Value::Float32(value)) => {
            out.extend_from_slice(&value.to_le_bytes())
        }
        (LogicalTypeKind::Float64, Value::Float64(value)) => {
            out.extend_from_slice(&value.to_le_bytes())
        }
        (
            LogicalTypeKind::Decimal { precision, scale },
            Value::Decimal {
                precision: actual_precision,
                scale: actual_scale,
                unscaled,
            },
        ) => {
            validate_decimal(
                *precision,
                *scale,
                *actual_precision,
                *actual_scale,
                *unscaled,
            )?;
            ordered_i128(*unscaled, decimal_width(*precision), out);
        }
        (LogicalTypeKind::Date, Value::Date(value)) => ordered_i128(*value as i128, 4, out),
        (LogicalTypeKind::Time { precision }, Value::Time(value)) => {
            validate_time(*value, *precision)?;
            ordered_i128(*value as i128, 8, out);
        }
        (
            LogicalTypeKind::Timestamp {
                precision,
                timestamp_kind,
            },
            Value::Timestamp {
                precision: actual_precision,
                timestamp_kind: actual_kind,
                seconds,
                nanos,
            },
        ) => {
            validate_timestamp(
                *precision,
                *timestamp_kind,
                *actual_precision,
                *actual_kind,
                *seconds,
                *nanos,
            )?;
            ordered_i128(*seconds as i128, 8, out);
            out.extend_from_slice(&nanos.to_be_bytes());
        }
        (LogicalTypeKind::String, Value::String(value)) => out.extend_from_slice(value.as_bytes()),
        (LogicalTypeKind::Binary, Value::Binary(value)) => out.extend_from_slice(value),
        (LogicalTypeKind::List { element_type }, Value::List(values)) => {
            append_count(values.len(), out)?;
            for value in values {
                append_framed(element_type, value, out)?;
            }
        }
        (
            LogicalTypeKind::Map {
                key_type,
                value_type,
            },
            Value::Map(entries),
        ) => {
            append_count(entries.len(), out)?;
            for (key, value) in entries {
                append_framed(key_type, key, out)?;
                append_framed(value_type, value, out)?;
            }
        }
        (LogicalTypeKind::Struct { fields }, Value::Struct(values)) => {
            if fields.len() != values.len() {
                return Err(TableError::codec(
                    "struct field count does not match schema",
                ));
            }
            for (field, value) in fields.iter().zip(values) {
                append_framed(&field.logical_type, value, out)?;
            }
        }
        (LogicalTypeKind::Extension { extension }, Value::Extension { type_id, value })
            if type_id == &extension.type_id =>
        {
            encode_value(&extension.physical_type, value, out)?;
        }
        _ => return Err(type_mismatch(logical_type, value)),
    }
    Ok(())
}

fn decode_value(logical_type: &LogicalType, input: Input<'_>) -> Result<Value> {
    if !logical_type.nullable {
        return decode_non_null(logical_type, input);
    }
    let bytes = input.bytes();
    let Some((&marker, payload)) = bytes.split_first() else {
        return Err(TableError::codec("nullable value is missing marker"));
    };
    match marker {
        0 if payload.is_empty() => Ok(Value::Null),
        0 => Err(TableError::codec("null value has trailing bytes")),
        1 => decode_non_null(logical_type, input.slice(1..bytes.len())),
        _ => Err(TableError::codec("invalid nullable marker")),
    }
}

fn decode_non_null(logical_type: &LogicalType, input: Input<'_>) -> Result<Value> {
    let bytes = input.bytes();
    match &logical_type.kind {
        LogicalTypeKind::Boolean => Ok(Value::Boolean(read_bool_exact(bytes)?)),
        LogicalTypeKind::Int8 => Ok(Value::Int8(read_ordered_exact(bytes, 1)? as i8)),
        LogicalTypeKind::Int16 => Ok(Value::Int16(read_ordered_exact(bytes, 2)? as i16)),
        LogicalTypeKind::Int32 => Ok(Value::Int32(read_ordered_exact(bytes, 4)? as i32)),
        LogicalTypeKind::Int64 => Ok(Value::Int64(read_ordered_exact(bytes, 8)? as i64)),
        LogicalTypeKind::Float32 => Ok(Value::Float32(f32::from_le_bytes(read_exact(bytes)?))),
        LogicalTypeKind::Float64 => Ok(Value::Float64(f64::from_le_bytes(read_exact(bytes)?))),
        LogicalTypeKind::Decimal { precision, scale } => {
            let width = decimal_width(*precision);
            require_exact_len(bytes, width)?;
            let unscaled = read_ordered(bytes, width)?;
            validate_decimal(*precision, *scale, *precision, *scale, unscaled)?;
            Ok(Value::Decimal {
                precision: *precision,
                scale: *scale,
                unscaled,
            })
        }
        LogicalTypeKind::Date => Ok(Value::Date(read_ordered_exact(bytes, 4)? as i32)),
        LogicalTypeKind::Time { precision } => {
            let value = read_ordered_exact(bytes, 8)? as i64;
            validate_time(value, *precision)?;
            Ok(Value::Time(value))
        }
        LogicalTypeKind::Timestamp {
            precision,
            timestamp_kind,
        } => {
            require_exact_len(bytes, 12)?;
            let seconds = read_ordered(bytes, 8)? as i64;
            let nanos = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
            validate_timestamp(
                *precision,
                *timestamp_kind,
                *precision,
                *timestamp_kind,
                seconds,
                nanos,
            )?;
            Ok(Value::Timestamp {
                precision: *precision,
                timestamp_kind: *timestamp_kind,
                seconds,
                nanos,
            })
        }
        LogicalTypeKind::String => String::from_utf8(bytes.to_vec())
            .map(Value::String)
            .map_err(|_| TableError::codec("invalid UTF-8 value")),
        LogicalTypeKind::Binary => Ok(Value::Binary(input.into_bytes())),
        LogicalTypeKind::List { element_type } => {
            let mut cursor = 0;
            let count = read_count(bytes, &mut cursor)?;
            if count > (bytes.len() - cursor) / 4 {
                return Err(TableError::codec("list count exceeds encoded frames"));
            }
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_framed(element_type, &input, &mut cursor)?);
            }
            ensure_consumed(bytes.len(), cursor)?;
            Ok(Value::List(values))
        }
        LogicalTypeKind::Map {
            key_type,
            value_type,
        } => {
            let mut cursor = 0;
            let count = read_count(bytes, &mut cursor)?;
            if count > (bytes.len() - cursor) / 8 {
                return Err(TableError::codec("map count exceeds encoded frames"));
            }
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let key = decode_framed(key_type, &input, &mut cursor)?;
                let value = decode_framed(value_type, &input, &mut cursor)?;
                entries.push((key, value));
            }
            ensure_consumed(bytes.len(), cursor)?;
            Ok(Value::Map(entries))
        }
        LogicalTypeKind::Struct { fields } => {
            let mut cursor = 0;
            let mut values = Vec::with_capacity(fields.len());
            for field in fields {
                values.push(decode_framed(&field.logical_type, &input, &mut cursor)?);
            }
            ensure_consumed(bytes.len(), cursor)?;
            Ok(Value::Struct(values))
        }
        LogicalTypeKind::Extension { extension } => Ok(Value::Extension {
            type_id: extension.type_id.clone(),
            value: Box::new(decode_value(&extension.physical_type, input)?),
        }),
    }
}

fn value_size(logical_type: &LogicalType, value: &Value) -> Result<usize> {
    if logical_type.nullable && matches!(value, Value::Null) {
        return Ok(1);
    }
    if !logical_type.nullable && matches!(value, Value::Null) {
        return Err(type_mismatch(logical_type, value));
    }
    let marker = usize::from(logical_type.nullable);
    let payload = match (&logical_type.kind, value) {
        (LogicalTypeKind::Boolean, Value::Boolean(_)) | (LogicalTypeKind::Int8, Value::Int8(_)) => {
            1
        }
        (LogicalTypeKind::Int16, Value::Int16(_)) => 2,
        (LogicalTypeKind::Int32, Value::Int32(_))
        | (LogicalTypeKind::Float32, Value::Float32(_))
        | (LogicalTypeKind::Date, Value::Date(_)) => 4,
        (LogicalTypeKind::Int64, Value::Int64(_))
        | (LogicalTypeKind::Float64, Value::Float64(_)) => 8,
        (LogicalTypeKind::Time { precision }, Value::Time(value)) => {
            validate_time(*value, *precision)?;
            8
        }
        (
            LogicalTypeKind::Timestamp {
                precision,
                timestamp_kind,
            },
            Value::Timestamp {
                precision: actual_precision,
                timestamp_kind: actual_kind,
                seconds,
                nanos,
            },
        ) => {
            validate_timestamp(
                *precision,
                *timestamp_kind,
                *actual_precision,
                *actual_kind,
                *seconds,
                *nanos,
            )?;
            12
        }
        (
            LogicalTypeKind::Decimal { precision, scale },
            Value::Decimal {
                precision: actual_precision,
                scale: actual_scale,
                unscaled,
            },
        ) => {
            validate_decimal(
                *precision,
                *scale,
                *actual_precision,
                *actual_scale,
                *unscaled,
            )?;
            decimal_width(*precision)
        }
        (LogicalTypeKind::String, Value::String(value)) => value.len(),
        (LogicalTypeKind::Binary, Value::Binary(value)) => value.len(),
        (LogicalTypeKind::List { element_type }, Value::List(values)) => {
            ensure_count(values.len())?;
            values.iter().try_fold(4, |size, value| {
                checked_size_add(size, framed_size(element_type, value)?)
            })?
        }
        (
            LogicalTypeKind::Map {
                key_type,
                value_type,
            },
            Value::Map(entries),
        ) => {
            ensure_count(entries.len())?;
            entries.iter().try_fold(4, |size, (key, value)| {
                checked_size_add(
                    size,
                    checked_size_add(framed_size(key_type, key)?, framed_size(value_type, value)?)?,
                )
            })?
        }
        (LogicalTypeKind::Struct { fields }, Value::Struct(values))
            if fields.len() == values.len() =>
        {
            fields
                .iter()
                .zip(values)
                .try_fold(0, |size, (field, value)| {
                    checked_size_add(size, framed_size(&field.logical_type, value)?)
                })?
        }
        (LogicalTypeKind::Extension { extension }, Value::Extension { type_id, value })
            if type_id == &extension.type_id =>
        {
            value_size(&extension.physical_type, value)?
        }
        _ => return Err(type_mismatch(logical_type, value)),
    };
    checked_size_add(marker, payload)
}

fn append_framed(logical_type: &LogicalType, value: &Value, out: &mut Vec<u8>) -> Result<()> {
    let start = out.len();
    out.extend_from_slice(&[0; 4]);
    if let Err(error) = encode_value(logical_type, value, out) {
        out.truncate(start);
        return Err(error);
    }
    let length = match u32::try_from(out.len() - start - 4) {
        Ok(length) => length,
        Err(_) => {
            out.truncate(start);
            return Err(TableError::codec("nested value exceeds u32 length"));
        }
    };
    out[start..start + 4].copy_from_slice(&length.to_le_bytes());
    Ok(())
}

fn decode_framed(
    logical_type: &LogicalType,
    input: &Input<'_>,
    cursor: &mut usize,
) -> Result<Value> {
    let bytes = input.bytes();
    let length = read_u32(bytes, cursor)? as usize;
    let end = cursor
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| TableError::codec("nested value length exceeds container"))?;
    let value = decode_value(logical_type, input.slice(*cursor..end))?;
    *cursor = end;
    Ok(value)
}

fn append_count(count: usize, out: &mut Vec<u8>) -> Result<()> {
    out.extend_from_slice(
        &u32::try_from(count)
            .map_err(|_| TableError::codec("container has more than u32 values"))?
            .to_le_bytes(),
    );
    Ok(())
}

fn read_count(bytes: &[u8], cursor: &mut usize) -> Result<usize> {
    Ok(read_u32(bytes, cursor)? as usize)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let end = cursor
        .checked_add(4)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| TableError::codec("container is truncated"))?;
    let value = u32::from_le_bytes(bytes[*cursor..end].try_into().unwrap());
    *cursor = end;
    Ok(value)
}

fn ordered_i128(value: i128, width: usize, out: &mut Vec<u8>) {
    let start = out.len();
    out.extend_from_slice(&value.to_be_bytes()[16 - width..]);
    out[start] ^= 0x80;
}

fn read_ordered(bytes: &[u8], width: usize) -> Result<i128> {
    require_len(bytes, width)?;
    let mut fixed = [0u8; 16];
    fixed[16 - width..].copy_from_slice(&bytes[..width]);
    fixed[16 - width] ^= 0x80;
    if fixed[16 - width] & 0x80 != 0 {
        fixed[..16 - width].fill(0xff);
    }
    Ok(i128::from_be_bytes(fixed))
}

fn read_ordered_exact(bytes: &[u8], width: usize) -> Result<i128> {
    require_exact_len(bytes, width)?;
    read_ordered(bytes, width)
}

fn append_escaped(bytes: &[u8], out: &mut Vec<u8>) {
    for byte in bytes {
        if *byte == 0 {
            out.extend_from_slice(&[0, 0xff]);
        } else {
            out.push(*byte);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

fn read_escaped(bytes: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor] != 0 {
            out.push(bytes[cursor]);
            cursor += 1;
            continue;
        }
        let next = *bytes
            .get(cursor + 1)
            .ok_or_else(|| TableError::codec("unterminated escaped key"))?;
        match next {
            0 => return Ok((out, cursor + 2)),
            0xff => out.push(0),
            _ => return Err(TableError::codec("invalid key escape")),
        }
        cursor += 2;
    }
    Err(TableError::codec("unterminated escaped key"))
}

fn validate_decimal(
    precision: u8,
    scale: u8,
    actual_precision: u8,
    actual_scale: u8,
    unscaled: i128,
) -> Result<()> {
    if !(1..=38).contains(&precision) || scale > precision {
        return Err(TableError::codec("invalid decimal value or precision"));
    }
    let limit = 10_i128.pow(precision as u32);
    if precision != actual_precision
        || scale != actual_scale
        || unscaled <= -limit
        || unscaled >= limit
    {
        return Err(TableError::codec("invalid decimal value or precision"));
    }
    Ok(())
}

fn decimal_width(precision: u8) -> usize {
    if precision <= 9 {
        4
    } else if precision <= 18 {
        8
    } else {
        16
    }
}

fn validate_time(value: i64, precision: u8) -> Result<()> {
    if precision > 9
        || !(0..NANOS_PER_DAY).contains(&value)
        || !(value as u64).is_multiple_of(10_u64.pow((9 - precision) as u32))
    {
        return Err(TableError::codec("invalid time value"));
    }
    Ok(())
}

fn validate_timestamp(
    precision: u8,
    expected_kind: TimestampKind,
    actual_precision: u8,
    actual_kind: TimestampKind,
    seconds: i64,
    nanos: u32,
) -> Result<()> {
    if precision > 9
        || precision != actual_precision
        || expected_kind != actual_kind
        || nanos >= 1_000_000_000
        || !nanos.is_multiple_of(10_u32.pow((9 - precision) as u32))
    {
        return Err(TableError::codec(format!(
            "invalid timestamp seconds={seconds} nanos={nanos}"
        )));
    }
    Ok(())
}

fn read_bool(bytes: &[u8]) -> Result<bool> {
    match *bytes
        .first()
        .ok_or_else(|| TableError::codec("value is truncated"))?
    {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(TableError::codec("invalid boolean byte")),
    }
}
fn read_bool_exact(bytes: &[u8]) -> Result<bool> {
    require_exact_len(bytes, 1)?;
    read_bool(bytes)
}
fn require_len(bytes: &[u8], length: usize) -> Result<()> {
    if bytes.len() < length {
        Err(TableError::codec("value is truncated"))
    } else {
        Ok(())
    }
}
fn require_exact_len(bytes: &[u8], length: usize) -> Result<()> {
    if bytes.len() != length {
        Err(TableError::codec("invalid value length"))
    } else {
        Ok(())
    }
}
fn ensure_consumed(length: usize, consumed: usize) -> Result<()> {
    if length == consumed {
        Ok(())
    } else {
        Err(TableError::codec("trailing bytes"))
    }
}
fn read_exact<const N: usize>(bytes: &[u8]) -> Result<[u8; N]> {
    require_exact_len(bytes, N)?;
    Ok(bytes.try_into().unwrap())
}
fn type_mismatch(logical_type: &LogicalType, _: &Value) -> TableError {
    TableError::codec(format!("value does not match {:?}", logical_type.kind))
}

fn checked_size_add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| TableError::codec("encoded value exceeds addressable memory"))
}

fn ensure_count(count: usize) -> Result<()> {
    u32::try_from(count)
        .map(|_| ())
        .map_err(|_| TableError::codec("container has more than u32 values"))
}

fn framed_size(logical_type: &LogicalType, value: &Value) -> Result<usize> {
    let payload = value_size(logical_type, value)?;
    u32::try_from(payload).map_err(|_| TableError::codec("nested value exceeds u32 length"))?;
    checked_size_add(4, payload)
}
