use cobble_binding::{ExpandStorageMode, MemtableType, RecoveryMode};
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
    #[pyo3(name = "SNAPSHOT_ONLY")]
    SnapshotOnly = 0,
    #[pyo3(name = "LATEST_WITH_WAL")]
    LatestWithWal = 1,
}

#[pyclass(
    name = "MemtableType",
    module = "pycobble._native",
    eq,
    eq_int,
    from_py_object
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PyMemtableType {
    #[pyo3(name = "HASH")]
    Hash = 0,
    #[pyo3(name = "SKIPLIST")]
    Skiplist = 1,
    #[pyo3(name = "VEC")]
    Vec = 2,
    #[pyo3(name = "ADAPTIVE")]
    Adaptive = 3,
}

#[pyclass(
    name = "ExpandStorageMode",
    module = "pycobble._native",
    eq,
    eq_int,
    from_py_object
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PyExpandStorageMode {
    #[pyo3(name = "ADOPT_ASYNC")]
    AdoptAsync = 0,
    #[pyo3(name = "REFERENCE_PERSISTENT")]
    ReferencePersistent = 1,
    #[pyo3(name = "REFERENCE_PERSISTENT_WITH_CACHE")]
    ReferencePersistentWithCache = 2,
}

#[pyclass(
    name = "ReaderMode",
    module = "pycobble._native",
    eq,
    eq_int,
    skip_from_py_object
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PyReaderMode {
    #[pyo3(name = "CURRENT")]
    Current = 0,
    #[pyo3(name = "SNAPSHOT")]
    Snapshot = 1,
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
    #[pyo3(name = "OK")]
    Ok = 0,
    #[pyo3(name = "NOT_FOUND")]
    NotFound = 1,
    #[pyo3(name = "END")]
    End = 2,
    #[pyo3(name = "BUFFER_TOO_SMALL")]
    BufferTooSmall = 3,
    #[pyo3(name = "BLOCK_BOUNDARY")]
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

impl From<PyMemtableType> for MemtableType {
    fn from(value: PyMemtableType) -> Self {
        match value {
            PyMemtableType::Hash => Self::Hash,
            PyMemtableType::Skiplist => Self::Skiplist,
            PyMemtableType::Vec => Self::Vec,
            PyMemtableType::Adaptive => Self::Adaptive,
        }
    }
}

impl From<PyExpandStorageMode> for ExpandStorageMode {
    fn from(value: PyExpandStorageMode) -> Self {
        match value {
            PyExpandStorageMode::AdoptAsync => Self::AdoptAsync,
            PyExpandStorageMode::ReferencePersistent => Self::ReferencePersistent,
            PyExpandStorageMode::ReferencePersistentWithCache => Self::ReferencePersistentWithCache,
        }
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyRecoveryMode>()?;
    module.add_class::<PyMemtableType>()?;
    module.add_class::<PyExpandStorageMode>()?;
    module.add_class::<PyReaderMode>()?;
    module.add_class::<PyBufferStatus>()?;
    module.add_class::<PyBufferResult>()
}
