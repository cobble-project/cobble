use cobble_binding::{ExpandStorageMode, MemtableType, RecoveryMode};
use pyo3::PyTypeInfo;
use pyo3::prelude::*;

pub(crate) type PickleReduction<T> = (Py<PyAny>, T);

pub(crate) fn enum_reduce<T: PyTypeInfo>(
    py: Python<'_>,
    name: &'static str,
) -> PyResult<PickleReduction<(Py<PyAny>, &'static str)>> {
    Ok((
        py.import("builtins")?.getattr("getattr")?.unbind(),
        (py.get_type::<T>().into_any().unbind(), name),
    ))
}

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

#[pymethods]
impl PyRecoveryMode {
    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(Py<PyAny>, &'static str)>> {
        enum_reduce::<Self>(
            py,
            match self {
                Self::SnapshotOnly => "SNAPSHOT_ONLY",
                Self::LatestWithWal => "LATEST_WITH_WAL",
            },
        )
    }
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

#[pymethods]
impl PyMemtableType {
    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(Py<PyAny>, &'static str)>> {
        enum_reduce::<Self>(
            py,
            match self {
                Self::Hash => "HASH",
                Self::Skiplist => "SKIPLIST",
                Self::Vec => "VEC",
                Self::Adaptive => "ADAPTIVE",
            },
        )
    }
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

#[pymethods]
impl PyExpandStorageMode {
    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(Py<PyAny>, &'static str)>> {
        enum_reduce::<Self>(
            py,
            match self {
                Self::AdoptAsync => "ADOPT_ASYNC",
                Self::ReferencePersistent => "REFERENCE_PERSISTENT",
                Self::ReferencePersistentWithCache => "REFERENCE_PERSISTENT_WITH_CACHE",
            },
        )
    }
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

#[pymethods]
impl PyReaderMode {
    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(Py<PyAny>, &'static str)>> {
        enum_reduce::<Self>(
            py,
            match self {
                Self::Current => "CURRENT",
                Self::Snapshot => "SNAPSHOT",
            },
        )
    }
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

#[pymethods]
impl PyBufferStatus {
    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(Py<PyAny>, &'static str)>> {
        enum_reduce::<Self>(
            py,
            match self {
                Self::Ok => "OK",
                Self::NotFound => "NOT_FOUND",
                Self::End => "END",
                Self::BufferTooSmall => "BUFFER_TOO_SMALL",
                Self::BlockBoundary => "BLOCK_BOUNDARY",
            },
        )
    }
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

#[pymethods]
impl PyBufferResult {
    #[staticmethod]
    fn _restore(
        status: PyRef<'_, PyBufferStatus>,
        bytes_written: usize,
        bytes_required: usize,
        row_count: usize,
    ) -> Self {
        Self::new(*status, bytes_written, bytes_required, row_count)
    }

    fn __reduce__(
        &self,
        py: Python<'_>,
    ) -> PyResult<PickleReduction<(PyBufferStatus, usize, usize, usize)>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (
                self.status,
                self.bytes_written,
                self.bytes_required,
                self.row_count,
            ),
        ))
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
