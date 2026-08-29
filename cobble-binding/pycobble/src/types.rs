use cobble_binding::RecoveryMode;
use pyo3::prelude::*;

#[pyclass(
    name = "RecoveryMode",
    module = "pycobble._native",
    eq,
    eq_int,
    from_py_object
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PyRecoveryMode {
    SnapshotOnly = 0,
    LatestWithWal = 1,
}

#[pyclass(
    name = "BufferStatus",
    module = "pycobble._native",
    eq,
    eq_int,
    skip_from_py_object
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PyBufferStatus {
    Ok = 0,
    NotFound = 1,
    End = 2,
    BufferTooSmall = 3,
    BlockBoundary = 4,
}

#[pyclass(name = "BufferResult", module = "pycobble._native", frozen)]
pub(crate) struct PyBufferResult {
    #[pyo3(get)]
    pub(crate) status: PyBufferStatus,
    #[pyo3(get)]
    pub(crate) bytes_written: usize,
    #[pyo3(get)]
    pub(crate) bytes_required: usize,
    #[pyo3(get)]
    pub(crate) row_count: usize,
}

impl PyBufferResult {
    pub(crate) fn new(
        status: PyBufferStatus,
        bytes_written: usize,
        bytes_required: usize,
        row_count: usize,
    ) -> Self {
        Self {
            status,
            bytes_written,
            bytes_required,
            row_count,
        }
    }
}

impl From<PyRecoveryMode> for RecoveryMode {
    fn from(value: PyRecoveryMode) -> Self {
        match value {
            PyRecoveryMode::SnapshotOnly => Self::SnapshotOnly,
            PyRecoveryMode::LatestWithWal => Self::LatestWithWal,
        }
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyRecoveryMode>()?;
    module.add_class::<PyBufferStatus>()?;
    module.add_class::<PyBufferResult>()
}
