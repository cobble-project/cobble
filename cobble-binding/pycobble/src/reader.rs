use crate::buffer::{InputBytes, WritableBuffer};
use crate::error::{input_error, invalid_state, map_error};
use crate::multi_get::{PyMultiGetResult, extract_keys};
use crate::options::{PyReadOptions, PyScanOptions};
use crate::row::PyOwnedRow;
use crate::scan::PyScanCursor;
use crate::snapshot::{PyGlobalSnapshot, snapshot};
use crate::types::{PyBufferResult, PyBufferStatus, PyReaderMode};
use cobble_binding::{Config, ReadOptions, Reader, ReaderConfig};
use pyo3::prelude::*;
use std::path::PathBuf;

#[pyclass(name = "Reader", module = "pycobble._native")]
pub(crate) struct PyReader {
    reader: Reader,
}

impl PyReader {
    fn open_config(config: Config, snapshot_id: Option<u64>) -> PyResult<Self> {
        opendal::install_default();
        let config = ReaderConfig::from_config(&config);
        let reader = match snapshot_id {
            Some(snapshot_id) => Reader::open(config, snapshot_id),
            None => Reader::open_current(config),
        }
        .map_err(map_error)?;
        Ok(Self { reader })
    }

    fn read_options(options: Option<PyRef<'_, PyReadOptions>>) -> ReadOptions {
        options.map_or_else(ReadOptions::default, |options| options.inner.clone())
    }
}

#[pymethods]
impl PyReader {
    #[staticmethod]
    fn open_current(py: Python<'_>, config_json: String) -> PyResult<Self> {
        py.detach(move || {
            Self::open_config(
                Config::from_json_str(&config_json).map_err(map_error)?,
                None,
            )
        })
    }

    #[staticmethod]
    fn open_current_file(py: Python<'_>, config_path: PathBuf) -> PyResult<Self> {
        py.detach(move || {
            Self::open_config(Config::from_path(config_path).map_err(map_error)?, None)
        })
    }

    #[staticmethod]
    fn open(py: Python<'_>, config_json: String, snapshot_id: u64) -> PyResult<Self> {
        py.detach(move || {
            Self::open_config(
                Config::from_json_str(&config_json).map_err(map_error)?,
                Some(snapshot_id),
            )
        })
    }

    #[staticmethod]
    fn open_file(py: Python<'_>, config_path: PathBuf, snapshot_id: u64) -> PyResult<Self> {
        py.detach(move || {
            Self::open_config(
                Config::from_path(config_path).map_err(map_error)?,
                Some(snapshot_id),
            )
        })
    }

    fn refresh(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.reader.configured_snapshot_id().is_some() {
            return Err(invalid_state(
                "pinned Reader cannot refresh; open current mode to follow the global snapshot pointer",
            ));
        }
        py.detach(|| self.reader.refresh().map_err(map_error))
    }

    #[pyo3(signature = (bucket, key, options=None))]
    fn get(
        &mut self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyReadOptions>>,
    ) -> PyResult<PyOwnedRow> {
        let key = InputBytes::extract(key)?;
        let options = Self::read_options(options);
        py.detach(|| {
            self.reader
                .get_with_options(bucket, key.as_ref(), &options)
                .map(PyOwnedRow::new)
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (keys, options=None))]
    fn multi_get(
        &mut self,
        py: Python<'_>,
        keys: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyReadOptions>>,
    ) -> PyResult<PyMultiGetResult> {
        let keys = extract_keys(keys)?;
        let options = Self::read_options(options);
        py.detach(|| {
            self.reader
                .multi_get_with_options(&keys, &options)
                .map(PyMultiGetResult::new)
                .map_err(map_error)
        })
    }

    fn get_column_into(
        &mut self,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        output: &Bound<'_, PyAny>,
        options: PyRef<'_, PyReadOptions>,
    ) -> PyResult<PyBufferResult> {
        let key = InputBytes::extract(key)?;
        let mut output = WritableBuffer::extract(output)?;
        if options.inner.column_indices.as_ref().map(Vec::len) != Some(1) {
            return Err(input_error(
                "get_column_into requires ReadOptions with exactly one column",
            ));
        }
        let Some(columns) = self
            .reader
            .get_with_options(bucket, key.as_ref(), &options.inner)
            .map_err(map_error)?
        else {
            return Ok(PyBufferResult::new(PyBufferStatus::NotFound, 0, 0, 0));
        };
        let Some(Some(column)) = columns.into_iter().next() else {
            return Ok(PyBufferResult::new(PyBufferStatus::NotFound, 0, 0, 0));
        };
        let required = column.len();
        if output.as_mut_slice().len() < required {
            return Ok(PyBufferResult::new(
                PyBufferStatus::BufferTooSmall,
                0,
                required,
                1,
            ));
        }
        output.as_mut_slice()[..required].copy_from_slice(&column);
        Ok(PyBufferResult::new(
            PyBufferStatus::Ok,
            required,
            required,
            1,
        ))
    }

    #[pyo3(signature = (bucket, start, end, options=None))]
    fn scan(
        &mut self,
        bucket: u16,
        start: &Bound<'_, PyAny>,
        end: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyScanOptions>>,
    ) -> PyResult<PyScanCursor> {
        let start = InputBytes::extract(start)?;
        let end = InputBytes::extract(end)?;
        if start.as_ref() > end.as_ref() {
            return Err(input_error("reader scan start must not exceed end"));
        }
        let options = options.map_or_else(Default::default, |options| options.inner.clone());
        let iterator = self
            .reader
            .scan_with_options(bucket, start.as_ref()..end.as_ref(), &options)
            .map_err(map_error)?;
        Ok(PyScanCursor::new_reader(bucket, iterator))
    }

    #[getter]
    fn mode(&self) -> PyReaderMode {
        if self.reader.configured_snapshot_id().is_some() {
            PyReaderMode::Snapshot
        } else {
            PyReaderMode::Current
        }
    }

    #[getter]
    fn configured_snapshot_id(&self) -> Option<u64> {
        self.reader.configured_snapshot_id()
    }

    #[getter]
    fn current_global_snapshot(&self) -> PyGlobalSnapshot {
        snapshot(self.reader.current_global_snapshot().clone())
    }

    fn list_global_snapshots(&self, py: Python<'_>) -> PyResult<Vec<PyGlobalSnapshot>> {
        py.detach(|| {
            self.reader
                .list_global_snapshot_manifests()
                .map(|values| values.into_iter().map(snapshot).collect())
                .map_err(map_error)
        })
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyReader>()
}
