use cobble_binding::Error;
use pyo3::PyTypeInfo;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(_native, CobbleError, PyException);
create_exception!(_native, UrlError, CobbleError);
create_exception!(_native, FileSystemError, CobbleError);
create_exception!(_native, IoError, CobbleError);
create_exception!(_native, MemtableFullError, CobbleError);
create_exception!(_native, ConfigurationError, CobbleError);
create_exception!(_native, InputError, CobbleError);
create_exception!(_native, CoordinationError, CobbleError);
create_exception!(_native, InternalStateError, CobbleError);
create_exception!(_native, FileFormatError, CobbleError);
create_exception!(_native, ChecksumError, CobbleError);
create_exception!(_native, CancelledError, CobbleError);

pub(crate) fn map_error(error: Error) -> PyErr {
    let message = error.to_string();
    match error {
        Error::UrlParseError(_) => PyErr::new::<UrlError, _>(message),
        Error::FileSystemError(_) => PyErr::new::<FileSystemError, _>(message),
        Error::IoError(_) => PyErr::new::<IoError, _>(message),
        Error::MemtableFull { .. } => PyErr::new::<MemtableFullError, _>(message),
        Error::ConfigError(_) => PyErr::new::<ConfigurationError, _>(message),
        Error::InputError(_) => PyErr::new::<InputError, _>(message),
        Error::CoordinationError(_) => PyErr::new::<CoordinationError, _>(message),
        Error::InvalidState(_) => PyErr::new::<InternalStateError, _>(message),
        Error::FileFormatError(_) => PyErr::new::<FileFormatError, _>(message),
        Error::ChecksumMismatch(_) => PyErr::new::<ChecksumError, _>(message),
        Error::CancelledError(_) => PyErr::new::<CancelledError, _>(message),
    }
}

pub(crate) fn invalid_state(message: impl Into<String>) -> PyErr {
    PyErr::new::<InternalStateError, _>(message.into())
}

pub(crate) fn input_error(message: impl Into<String>) -> PyErr {
    PyErr::new::<InputError, _>(message.into())
}

fn add_exception<T>(module: &Bound<'_, PyModule>, name: &str, code: &str) -> PyResult<()>
where
    T: PyTypeInfo,
{
    let exception = module.py().get_type::<T>();
    exception.setattr("code", code)?;
    module.add(name, exception)
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    add_exception::<CobbleError>(module, "CobbleError", "CB_UNKNOWN")?;
    add_exception::<UrlError>(module, "UrlError", "CB_URL")?;
    add_exception::<FileSystemError>(module, "FileSystemError", "CB_FILE_SYSTEM")?;
    add_exception::<IoError>(module, "IoError", "CB_IO")?;
    add_exception::<MemtableFullError>(module, "MemtableFullError", "CB_MEMTABLE_FULL")?;
    add_exception::<ConfigurationError>(module, "ConfigurationError", "CB_CONFIGURATION")?;
    add_exception::<InputError>(module, "InputError", "CB_INPUT")?;
    add_exception::<CoordinationError>(module, "CoordinationError", "CB_COORDINATION")?;
    add_exception::<InternalStateError>(module, "InternalStateError", "CB_INVALID_STATE")?;
    add_exception::<FileFormatError>(module, "FileFormatError", "CB_FILE_FORMAT")?;
    add_exception::<ChecksumError>(module, "ChecksumError", "CB_CHECKSUM")?;
    add_exception::<CancelledError>(module, "CancelledError", "CB_CANCELLED")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_variants_map_to_stable_exception_types() {
        Python::initialize();
        Python::attach(|py| {
            let error = map_error(Error::InvalidState("closed".to_string()));
            assert!(error.is_instance_of::<InternalStateError>(py));
            assert!(error.to_string().ends_with("Internal state error: closed"));
        });
    }
}
