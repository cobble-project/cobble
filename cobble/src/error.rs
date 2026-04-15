#[derive(Clone, Debug, thiserror::Error)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[error("Url parse error.")]
    UrlParseError(#[from] url::ParseError),
    #[error("File system error: {0}")]
    FileSystemError(String),
    #[error("IO error: {0}")]
    IoError(String),
    #[error("Memtable full: needed {needed} bytes but only {remaining} remaining.")]
    MemtableFull { needed: usize, remaining: usize },
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Input error: {0}")]
    InputError(String),
    #[error("Coordination error: {0}")]
    CoordinationError(String),
    #[error("Internal state error: {0}")]
    InvalidState(String),
    #[error("File format error: {0}")]
    FileFormatError(String),
    #[error("Metadata checksum error: {0}")]
    ChecksumMismatch(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
