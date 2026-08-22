use thiserror::Error;

pub type Result<T> = std::result::Result<T, TableError>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TableError {
    #[error("invalid table schema: {0}")]
    InvalidSchema(String),

    #[error("table codec error: {0}")]
    Codec(String),

    #[doc(hidden)]
    #[error("internal table error: {0}")]
    Internal(String),
}

impl TableError {
    pub(crate) fn codec(message: impl Into<String>) -> Self {
        Self::Codec(message.into())
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}
