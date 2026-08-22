use thiserror::Error;

pub type Result<T> = std::result::Result<T, TableError>;

#[derive(Debug, Error)]
pub enum TableError {
    #[error("invalid table schema: {0}")]
    InvalidSchema(String),

    #[error("invalid record layout: {0}")]
    InvalidLayout(String),

    #[error("invalid table metadata: {0}")]
    InvalidMetadata(String),

    #[error("failed to encode or decode table metadata: {0}")]
    MetadataCodec(#[from] serde_json::Error),

    #[error("table codec error: {0}")]
    Codec(String),
}

impl TableError {
    pub(crate) fn codec(message: impl Into<String>) -> Self {
        Self::Codec(message.into())
    }
}
