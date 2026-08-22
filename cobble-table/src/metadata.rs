use crate::{LayoutCompiler, RecordLayoutDescriptor, Result, TableError, TableSchema};
use serde::{Deserialize, Serialize};

pub const TABLE_METADATA_FORMAT: &str = "cobble-table";
pub const TABLE_METADATA_VERSION_CURRENT: u32 = 1;

/// Versioned metadata shared by Rust and language bindings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMetadata {
    pub format: String,
    pub version: u32,
    pub schema: TableSchema,
    pub layout: RecordLayoutDescriptor,
}

impl TableMetadata {
    pub fn compile(schema: TableSchema) -> Result<Self> {
        let layout = LayoutCompiler::compile(&schema)?;
        Ok(Self {
            format: TABLE_METADATA_FORMAT.to_string(),
            version: TABLE_METADATA_VERSION_CURRENT,
            schema,
            layout,
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.format != TABLE_METADATA_FORMAT {
            return Err(TableError::InvalidMetadata(format!(
                "unsupported metadata format: {}",
                self.format
            )));
        }
        if self.version != TABLE_METADATA_VERSION_CURRENT {
            return Err(TableError::InvalidMetadata(format!(
                "unsupported metadata version: {}",
                self.version
            )));
        }
        self.layout.validate_against(&self.schema)
    }

    pub fn to_json(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }

    pub fn to_pretty_json(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(serde_json::to_vec_pretty(self)?)
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        let metadata: Self = serde_json::from_slice(bytes)?;
        metadata.validate()?;
        Ok(metadata)
    }
}
