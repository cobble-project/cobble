#[derive(Clone, Debug, thiserror::Error)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[error("Url parse error.")]
    UrlParseError(#[from] url::ParseError),
    #[error("File system error: {0}")]
    FileSystemError(String),
    #[error("IO error: {0}")]
    IoError(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;