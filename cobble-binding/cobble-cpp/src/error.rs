use cobble_binding::Error;

pub(crate) type BridgeResult<T> = std::result::Result<T, String>;

pub(crate) fn input_error(message: &str) -> String {
    format!("CB_INPUT: {message}")
}

pub(crate) fn format_cobble_error(error: Error) -> String {
    let prefix = match error {
        Error::UrlParseError(_) => "CB_URL",
        Error::FileSystemError(_) => "CB_FILE_SYSTEM",
        Error::IoError(_) => "CB_IO",
        Error::MemtableFull { .. } => "CB_MEMTABLE_FULL",
        Error::ConfigError(_) => "CB_CONFIGURATION",
        Error::InputError(_) => "CB_INPUT",
        Error::CoordinationError(_) => "CB_COORDINATION",
        Error::InvalidState(_) => "CB_INVALID_STATE",
        Error::FileFormatError(_) => "CB_FILE_FORMAT",
        Error::ChecksumMismatch(_) => "CB_CHECKSUM",
        Error::CancelledError(_) => "CB_CANCELLED",
    };
    format!("{prefix}: {error}")
}
