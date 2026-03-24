use std::net::AddrParseError;

#[derive(Clone, Debug, thiserror::Error)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[error("Cobble error: {0}")]
    CobbleError(String),
    #[error("Input error: {0}")]
    InputError(String),
    #[error("HTTP server error: {0}")]
    HttpServerError(String),
    #[error("Address parse error: {0}")]
    AddrParseError(#[from] AddrParseError),
}

impl From<cobble::Error> for Error {
    fn from(value: cobble::Error) -> Self {
        Self::CobbleError(value.to_string())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
