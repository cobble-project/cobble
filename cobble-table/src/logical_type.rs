use crate::{Result, TableError};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Stable identity of a field within a table schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FieldId(pub u32);

impl From<u32> for FieldId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

/// Timestamp semantic kind. Both kinds share the same physical encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampKind {
    WithoutTimeZone,
    WithLocalTimeZone,
}

/// Provider-defined semantics with a portable physical fallback.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtensionType {
    pub type_id: String,
    pub parameters: JsonValue,
    pub physical_type: Box<LogicalType>,
}

impl ExtensionType {
    pub fn new(
        type_id: impl Into<String>,
        parameters: JsonValue,
        physical_type: LogicalType,
    ) -> Result<Self> {
        let extension = Self {
            type_id: type_id.into(),
            parameters,
            physical_type: Box::new(physical_type),
        };
        extension.validate()?;
        Ok(extension)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.type_id.trim().is_empty() {
            return Err(TableError::InvalidSchema(
                "extension type id must not be empty".to_string(),
            ));
        }
        self.physical_type.validate()
    }
}

/// Cross-language logical type, including nullability at every nesting level.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalType {
    pub nullable: bool,
    #[serde(flatten)]
    pub kind: LogicalTypeKind,
}

/// Shape and parameters of a logical type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LogicalTypeKind {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal {
        precision: u8,
        scale: u8,
    },
    Date,
    Time {
        precision: u8,
    },
    Timestamp {
        precision: u8,
        timestamp_kind: TimestampKind,
    },
    String,
    Binary,
    List {
        element_type: Box<LogicalType>,
    },
    Map {
        key_type: Box<LogicalType>,
        value_type: Box<LogicalType>,
    },
    Struct {
        fields: Vec<DataField>,
    },
    Extension {
        extension: ExtensionType,
    },
}

impl LogicalType {
    fn new(kind: LogicalTypeKind) -> Self {
        Self {
            nullable: false,
            kind,
        }
    }

    pub fn boolean() -> Self {
        Self::new(LogicalTypeKind::Boolean)
    }

    pub fn int8() -> Self {
        Self::new(LogicalTypeKind::Int8)
    }

    pub fn int16() -> Self {
        Self::new(LogicalTypeKind::Int16)
    }

    pub fn int32() -> Self {
        Self::new(LogicalTypeKind::Int32)
    }

    pub fn int64() -> Self {
        Self::new(LogicalTypeKind::Int64)
    }

    pub fn float32() -> Self {
        Self::new(LogicalTypeKind::Float32)
    }

    pub fn float64() -> Self {
        Self::new(LogicalTypeKind::Float64)
    }

    pub fn decimal(precision: u8, scale: u8) -> Self {
        Self::new(LogicalTypeKind::Decimal { precision, scale })
    }

    pub fn date() -> Self {
        Self::new(LogicalTypeKind::Date)
    }

    pub fn time(precision: u8) -> Self {
        Self::new(LogicalTypeKind::Time { precision })
    }

    pub fn timestamp(precision: u8, timestamp_kind: TimestampKind) -> Self {
        Self::new(LogicalTypeKind::Timestamp {
            precision,
            timestamp_kind,
        })
    }

    pub fn string() -> Self {
        Self::new(LogicalTypeKind::String)
    }

    pub fn binary() -> Self {
        Self::new(LogicalTypeKind::Binary)
    }

    pub fn list(element_type: LogicalType) -> Self {
        Self::new(LogicalTypeKind::List {
            element_type: Box::new(element_type),
        })
    }

    pub fn map(key_type: LogicalType, value_type: LogicalType) -> Self {
        Self::new(LogicalTypeKind::Map {
            key_type: Box::new(key_type),
            value_type: Box::new(value_type),
        })
    }

    pub fn struct_type(fields: Vec<DataField>) -> Self {
        Self::new(LogicalTypeKind::Struct { fields })
    }

    pub fn extension(extension: ExtensionType) -> Self {
        Self::new(LogicalTypeKind::Extension { extension })
    }

    #[must_use]
    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    #[must_use]
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        match &self.kind {
            LogicalTypeKind::Decimal { precision, scale }
                if *precision == 0 || *precision > 38 || scale > precision =>
            {
                return Err(TableError::InvalidSchema(format!(
                    "invalid decimal precision/scale: {precision}/{scale}"
                )));
            }
            LogicalTypeKind::Time { precision } | LogicalTypeKind::Timestamp { precision, .. }
                if *precision > 9 =>
            {
                return Err(TableError::InvalidSchema(format!(
                    "time precision must be in [0, 9], got {precision}"
                )));
            }
            LogicalTypeKind::List { element_type } => element_type.validate()?,
            LogicalTypeKind::Map {
                key_type,
                value_type,
            } => {
                if key_type.nullable {
                    return Err(TableError::InvalidSchema(
                        "map key type must not be nullable".to_string(),
                    ));
                }
                key_type.validate()?;
                value_type.validate()?;
            }
            LogicalTypeKind::Struct { fields } => {
                for field in fields {
                    field.validate()?;
                }
            }
            LogicalTypeKind::Extension { extension } => extension.validate()?,
            _ => {}
        }
        Ok(())
    }

    pub(crate) fn is_key_compatible(&self) -> bool {
        !self.nullable
            && matches!(
                self.kind,
                LogicalTypeKind::Boolean
                    | LogicalTypeKind::Int8
                    | LogicalTypeKind::Int16
                    | LogicalTypeKind::Int32
                    | LogicalTypeKind::Int64
                    | LogicalTypeKind::Decimal { .. }
                    | LogicalTypeKind::Date
                    | LogicalTypeKind::Time { .. }
                    | LogicalTypeKind::Timestamp { .. }
                    | LogicalTypeKind::String
                    | LogicalTypeKind::Binary
            )
    }
}

/// Named field in a top-level table row or nested struct.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataField {
    pub id: FieldId,
    pub name: String,
    pub logical_type: LogicalType,
}

impl DataField {
    pub fn new(
        id: impl Into<FieldId>,
        name: impl Into<String>,
        logical_type: LogicalType,
    ) -> Result<Self> {
        let field = Self {
            id: id.into(),
            name: name.into(),
            logical_type,
        };
        field.validate()?;
        Ok(field)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(TableError::InvalidSchema(format!(
                "field {} name must not be empty",
                self.id.0
            )));
        }
        self.logical_type.validate()
    }
}
