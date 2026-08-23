use crate::layout::{LayoutCompiler, RecordLayoutDescriptor};
use crate::{Result, TableError, TableSchema};
use serde::{Deserialize, Serialize};

pub(crate) const TABLE_METADATA_FORMAT: &str = "cobble-table";
pub(crate) const TABLE_METADATA_VERSION_CURRENT: u32 = 1;

/// Internal versioned table metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableMetadata {
    pub(crate) format: String,
    pub(crate) version: u32,
    pub(crate) schema: TableSchema,
    pub(crate) layout: RecordLayoutDescriptor,
}

impl TableMetadata {
    pub(crate) fn compile(schema: TableSchema) -> Result<Self> {
        let layout = LayoutCompiler::compile(&schema)?;
        Ok(Self {
            format: TABLE_METADATA_FORMAT.to_string(),
            version: TABLE_METADATA_VERSION_CURRENT,
            schema,
            layout,
        })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.format != TABLE_METADATA_FORMAT {
            return Err(TableError::internal(format!(
                "unsupported metadata format: {}",
                self.format
            )));
        }
        if self.version != TABLE_METADATA_VERSION_CURRENT {
            return Err(TableError::internal(format!(
                "unsupported metadata version: {}",
                self.version
            )));
        }
        self.layout.validate_against(&self.schema)
    }

    pub(crate) fn to_value(&self) -> Result<serde_json::Value> {
        self.validate()?;
        serde_json::to_value(self).map_err(|error| TableError::internal(error.to_string()))
    }

    pub(crate) fn from_value(value: &serde_json::Value) -> Result<Self> {
        let metadata: Self = serde_json::from_value(value.clone())
            .map_err(|error| TableError::internal(error.to_string()))?;
        metadata.validate()?;
        Ok(metadata)
    }

    #[cfg(test)]
    pub(crate) fn to_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|error| TableError::internal(error.to_string()))
    }

    #[cfg(test)]
    pub(crate) fn from_json(bytes: &[u8]) -> Result<Self> {
        let metadata: Self = serde_json::from_slice(bytes)
            .map_err(|error| TableError::internal(error.to_string()))?;
        metadata.validate()?;
        Ok(metadata)
    }
}
