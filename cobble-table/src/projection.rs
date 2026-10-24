use crate::codec::{
    Input, decode_value, ensure_consumed, non_null_input, read_count, read_frame, read_frame_range,
};
use crate::{LogicalType, LogicalTypeKind, Result, TableError, Value};
use bytes::Bytes;

/// A validated, reusable decoder selecting nested fields by stable field identity.
/// Scalar types, nullability, map keys and opaque extension types cannot change.
#[derive(Clone)]
pub struct ValueProjection {
    nullable: bool,
    kind: ProjectionKind,
}

#[derive(Clone)]
enum ProjectionKind {
    Full(LogicalType),
    Struct {
        slots: Vec<Option<(usize, ValueProjection)>>,
        output_len: usize,
    },
    List(Box<ValueProjection>),
    Map {
        key_type: LogicalType,
        value: Box<ValueProjection>,
    },
}

impl ValueProjection {
    /// Validate and compile the selection once for reuse across encoded values.
    /// Unselected struct payloads are skipped without decoding their contents;
    /// their containing frame lengths must still be valid.
    pub fn compile(source: &LogicalType, selection: &LogicalType) -> Result<Self> {
        source.validate()?;
        selection.validate()?;
        validate_identities(source)?;
        validate_identities(selection)?;
        Self::compile_validated(source, selection)
    }

    pub(crate) fn compile_validated(source: &LogicalType, selection: &LogicalType) -> Result<Self> {
        let invalid = || {
            TableError::InvalidSchema(
                "read projection changes a type, nullability, map key, or opaque extension"
                    .to_string(),
            )
        };
        if source.nullable != selection.nullable {
            return Err(invalid());
        }
        if source == selection {
            return Ok(Self {
                nullable: source.nullable,
                kind: ProjectionKind::Full(source.clone()),
            });
        }
        let kind = match (&source.kind, &selection.kind) {
            (LogicalTypeKind::Struct { fields }, LogicalTypeKind::Struct { fields: selected }) => {
                let mut slots = vec![None; fields.len()];
                for (output, field) in selected.iter().enumerate() {
                    let index = fields
                        .iter()
                        .position(|source| source.id == field.id)
                        .ok_or_else(|| {
                            TableError::InvalidSchema(format!(
                                "projection field {} does not exist in source struct",
                                field.id.0
                            ))
                        })?;
                    slots[index] = Some((
                        output,
                        Self::compile_validated(&fields[index].logical_type, &field.logical_type)?,
                    ));
                }
                ProjectionKind::Struct {
                    slots,
                    output_len: selected.len(),
                }
            }
            (
                LogicalTypeKind::List { element_type },
                LogicalTypeKind::List {
                    element_type: selected,
                },
            ) => ProjectionKind::List(Box::new(Self::compile_validated(element_type, selected)?)),
            (
                LogicalTypeKind::Map {
                    key_type,
                    value_type,
                },
                LogicalTypeKind::Map {
                    key_type: selected_key,
                    value_type: selected_value,
                },
            ) if key_type == selected_key => ProjectionKind::Map {
                key_type: (**key_type).clone(),
                value: Box::new(Self::compile_validated(value_type, selected_value)?),
            },
            _ => return Err(invalid()),
        };
        Ok(Self {
            nullable: source.nullable,
            kind,
        })
    }

    /// Decode a borrowed buffer; binary payloads are copied just as in ValueCodec.
    pub fn decode(&self, encoded: &[u8]) -> Result<Value> {
        self.decode_input(Input::Borrowed(encoded))
    }

    /// Decode an owned buffer, retaining zero-copy slices for selected binary payloads.
    pub fn decode_bytes(&self, encoded: Bytes) -> Result<Value> {
        self.decode_input(Input::Owned(encoded))
    }

    fn decode_input(&self, input: Input<'_>) -> Result<Value> {
        if let ProjectionKind::Full(logical_type) = &self.kind {
            return decode_value(logical_type, input);
        }
        let Some(input) = non_null_input(self.nullable, input)? else {
            return Ok(Value::Null);
        };
        let mut cursor = 0;
        let value = match &self.kind {
            ProjectionKind::Struct { slots, output_len } => {
                let mut values = vec![Value::Null; *output_len];
                for slot in slots {
                    let range = read_frame_range(input.bytes(), &mut cursor)?;
                    if let Some((output, decoder)) = slot {
                        values[*output] = decoder.decode_input(input.slice(range))?;
                    }
                }
                Value::Struct(values)
            }
            ProjectionKind::List(element) => {
                let count = read_count(input.bytes(), &mut cursor)?;
                if count > (input.bytes().len() - cursor) / 4 {
                    return Err(TableError::codec("list count exceeds encoded frames"));
                }
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(element.decode_input(read_frame(&input, &mut cursor)?)?);
                }
                Value::List(values)
            }
            ProjectionKind::Map { key_type, value } => {
                let count = read_count(input.bytes(), &mut cursor)?;
                if count > (input.bytes().len() - cursor) / 8 {
                    return Err(TableError::codec("map count exceeds encoded frames"));
                }
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    let key = decode_value(key_type, read_frame(&input, &mut cursor)?)?;
                    let value = value.decode_input(read_frame(&input, &mut cursor)?)?;
                    values.push((key, value));
                }
                Value::Map(values)
            }
            ProjectionKind::Full(_) => unreachable!(),
        };
        ensure_consumed(input.bytes().len(), cursor)?;
        Ok(value)
    }
}

fn validate_identities(logical_type: &LogicalType) -> Result<()> {
    match &logical_type.kind {
        LogicalTypeKind::Struct { fields } => {
            let mut ids = std::collections::HashSet::new();
            let mut names = std::collections::HashSet::new();
            for field in fields {
                if !ids.insert(field.id) || !names.insert(&field.name) {
                    return Err(TableError::InvalidSchema(
                        "duplicate struct field id or name in projection".to_string(),
                    ));
                }
                validate_identities(&field.logical_type)?;
            }
        }
        LogicalTypeKind::List { element_type } => validate_identities(element_type)?,
        LogicalTypeKind::Map {
            key_type,
            value_type,
        } => {
            validate_identities(key_type)?;
            validate_identities(value_type)?;
        }
        LogicalTypeKind::Extension { extension } => validate_identities(&extension.physical_type)?,
        _ => {}
    }
    Ok(())
}
