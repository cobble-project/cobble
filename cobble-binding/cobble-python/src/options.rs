use crate::types::PickleReduction;
use cobble_binding::{ReadOptions, ScanOptions, WriteOptions};
use pyo3::prelude::*;
use size::Size;

type PickledReadOptions = (Option<String>, Option<Vec<usize>>);
type PickledWriteOptions = (Option<u32>, Option<String>, bool);

#[pyclass(
    name = "ReadOptions",
    module = "pycobble._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyReadOptions {
    pub(crate) inner: ReadOptions,
}

#[pymethods]
impl PyReadOptions {
    #[staticmethod]
    fn _restore(column_family: Option<String>, columns: Option<Vec<usize>>) -> Self {
        Self::new(column_family, columns)
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<PickledReadOptions>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (
                self.inner.column_family.clone(),
                self.inner.column_indices.clone(),
            ),
        ))
    }

    #[new]
    #[pyo3(signature = (*, column_family=None, columns=None))]
    fn new(column_family: Option<String>, columns: Option<Vec<usize>>) -> Self {
        let mut inner = columns.map_or_else(ReadOptions::default, ReadOptions::for_columns);
        if let Some(column_family) = column_family {
            inner = inner.with_column_family(column_family);
        }
        Self { inner }
    }

    #[getter]
    fn column_family(&self) -> Option<&str> {
        self.inner.column_family.as_deref()
    }

    #[getter]
    fn columns(&self) -> Option<Vec<usize>> {
        self.inner.column_indices.clone()
    }
}

#[pyclass(
    name = "WriteOptions",
    module = "pycobble._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyWriteOptions {
    pub(crate) inner: WriteOptions,
}

#[pymethods]
impl PyWriteOptions {
    #[staticmethod]
    fn _restore(
        ttl_seconds: Option<u32>,
        column_family: Option<String>,
        await_durable: bool,
    ) -> Self {
        Self::new(ttl_seconds, column_family, await_durable)
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<PickledWriteOptions>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (
                self.inner.ttl_seconds,
                self.inner.column_family.clone(),
                self.inner.await_durable,
            ),
        ))
    }

    #[new]
    #[pyo3(signature = (*, ttl_seconds=None, column_family=None, await_durable=true))]
    fn new(ttl_seconds: Option<u32>, column_family: Option<String>, await_durable: bool) -> Self {
        let mut inner = WriteOptions::default().with_await_durable(await_durable);
        inner.ttl_seconds = ttl_seconds;
        inner.column_family = column_family;
        Self { inner }
    }

    #[getter]
    fn ttl_seconds(&self) -> Option<u32> {
        self.inner.ttl_seconds
    }

    #[getter]
    fn column_family(&self) -> Option<&str> {
        self.inner.column_family.as_deref()
    }

    #[getter]
    fn await_durable(&self) -> bool {
        self.inner.await_durable
    }
}

#[pyclass(
    name = "ScanOptions",
    module = "pycobble._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyScanOptions {
    pub(crate) inner: ScanOptions,
}

#[pymethods]
impl PyScanOptions {
    #[staticmethod]
    fn _restore(
        column_family: Option<String>,
        columns: Option<Vec<usize>>,
        read_ahead_bytes: usize,
        max_rows: Option<usize>,
        preload_scan_cursor_block: bool,
        stop_at_block_boundary: bool,
    ) -> PyResult<Self> {
        Self::new(
            column_family,
            columns,
            read_ahead_bytes,
            max_rows,
            preload_scan_cursor_block,
            stop_at_block_boundary,
        )
    }

    #[allow(clippy::type_complexity)]
    fn __reduce__(
        &self,
        py: Python<'_>,
    ) -> PyResult<
        PickleReduction<(
            Option<String>,
            Option<Vec<usize>>,
            usize,
            Option<usize>,
            bool,
            bool,
        )>,
    > {
        let read_ahead_bytes =
            usize::try_from(self.inner.read_ahead_bytes.bytes()).map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(
                    "read_ahead_bytes cannot be pickled as a nonnegative size",
                )
            })?;
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (
                self.inner.column_family.clone(),
                self.inner.column_indices.clone(),
                read_ahead_bytes,
                self.inner.max_rows(),
                self.inner.preload_scan_cursor_block(),
                self.inner.should_stop_at_block_boundary(),
            ),
        ))
    }

    #[new]
    #[pyo3(signature = (*, column_family=None, columns=None, read_ahead_bytes=0, max_rows=None, preload_scan_cursor_block=false, stop_at_block_boundary=false))]
    fn new(
        column_family: Option<String>,
        columns: Option<Vec<usize>>,
        read_ahead_bytes: usize,
        max_rows: Option<usize>,
        preload_scan_cursor_block: bool,
        stop_at_block_boundary: bool,
    ) -> PyResult<Self> {
        if max_rows == Some(0) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "max_rows must be greater than zero",
            ));
        }
        let mut inner = columns.map_or_else(ScanOptions::default, ScanOptions::for_columns);
        if let Some(column_family) = column_family {
            inner = inner.with_column_family(column_family);
        }
        inner.read_ahead_bytes = Size::from_bytes(read_ahead_bytes);
        if let Some(max_rows) = max_rows {
            inner.set_max_rows(max_rows);
        }
        inner.set_preload_scan_cursor_block(preload_scan_cursor_block);
        inner = inner.with_stop_at_block_boundary(stop_at_block_boundary);
        Ok(Self { inner })
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyReadOptions>()?;
    module.add_class::<PyWriteOptions>()?;
    module.add_class::<PyScanOptions>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_preserve_python_configuration() {
        let read = PyReadOptions::new(Some("family".to_string()), Some(vec![2, 0]));
        assert_eq!(read.column_family(), Some("family"));
        assert_eq!(read.columns(), Some(vec![2, 0]));

        let write = PyWriteOptions::new(Some(9), Some("family".to_string()), false);
        assert_eq!(write.ttl_seconds(), Some(9));
        assert_eq!(write.column_family(), Some("family"));
        assert!(!write.await_durable());

        assert!(PyScanOptions::new(None, None, 0, Some(0), false, false).is_err());
    }
}
