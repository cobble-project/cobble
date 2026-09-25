use super::database::{PyStructuredScanCursor, encode_get_into, encode_multi_get_into};
use super::types::{
    PyStructuredMultiGetResult, PyStructuredReadOptions, PyStructuredRow, PyStructuredScanOptions,
    PyStructuredSchema, schema,
};
use crate::buffer::{InputBytes, WritableBuffer};
use crate::error::{input_error, invalid_state, map_error};
use crate::multi_get::extract_keys;
use crate::snapshot::{PyGlobalSnapshot, global_snapshot, snapshot};
use crate::types::{PyBufferResult, PyReaderMode};
use cobble_binding::structured::{StructuredReader, StructuredReaderBuilder};
use cobble_binding::{Config, ReaderConfig};
use pyo3::prelude::*;
use std::path::PathBuf;

#[pyclass(name = "StructuredReader", module = "pycobble._native")]
pub(crate) struct PyStructuredReader {
    reader: StructuredReader,
}

impl PyStructuredReader {
    fn open_config(config: Config, snapshot_id: Option<u64>) -> PyResult<Self> {
        opendal::install_default();
        let config = ReaderConfig::from_config(&config);
        let reader = match snapshot_id {
            Some(id) => StructuredReader::open(config, id),
            None => StructuredReader::open_current(config),
        }
        .map_err(map_error)?;
        Ok(Self { reader })
    }

    fn open_snapshot(config: Config, snapshot: PyGlobalSnapshot) -> PyResult<Self> {
        opendal::install_default();
        StructuredReaderBuilder::new(ReaderConfig::from_config(&config))
            .open_from_global_snapshot(global_snapshot(snapshot)?)
            .map(|reader| Self { reader })
            .map_err(map_error)
    }
}

#[pymethods]
impl PyStructuredReader {
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

    #[staticmethod]
    fn open_from_global_snapshot(
        py: Python<'_>,
        config_json: String,
        snapshot: PyGlobalSnapshot,
    ) -> PyResult<Self> {
        py.detach(move || {
            Self::open_snapshot(
                Config::from_json_str(&config_json).map_err(map_error)?,
                snapshot,
            )
        })
    }

    #[staticmethod]
    fn open_from_global_snapshot_file(
        py: Python<'_>,
        config_path: PathBuf,
        snapshot: PyGlobalSnapshot,
    ) -> PyResult<Self> {
        py.detach(move || {
            Self::open_snapshot(Config::from_path(config_path).map_err(map_error)?, snapshot)
        })
    }

    fn refresh(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.reader.configured_snapshot_id().is_some() {
            return Err(invalid_state(
                "pinned StructuredReader cannot refresh; open current mode to follow the global snapshot pointer",
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
        options: Option<PyRef<'_, PyStructuredReadOptions>>,
    ) -> PyResult<PyStructuredRow> {
        let key = InputBytes::extract(key)?;
        let options = options.map(|value| value.inner.clone());
        py.detach(|| {
            let result = match options {
                Some(ref options) => self.reader.get_with_options(bucket, key.as_ref(), options),
                None => self.reader.get(bucket, key.as_ref()),
            };
            result.map(PyStructuredRow::new).map_err(map_error)
        })
    }

    #[pyo3(signature = (bucket, key, output, options=None))]
    fn get_into(
        &mut self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        output: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyStructuredReadOptions>>,
    ) -> PyResult<PyBufferResult> {
        let key = InputBytes::extract(key)?;
        let mut output = WritableBuffer::extract(output)?;
        let options = options.map(|value| value.inner.clone());
        let columns = py.detach(|| {
            match options {
                Some(ref options) => self.reader.get_with_options(bucket, key.as_ref(), options),
                None => self.reader.get(bucket, key.as_ref()),
            }
            .map_err(map_error)
        })?;
        encode_get_into(
            bucket,
            key.as_ref(),
            columns.as_deref(),
            output.as_mut_slice(),
        )
    }

    #[pyo3(signature = (keys, options=None))]
    fn multi_get(
        &mut self,
        py: Python<'_>,
        keys: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyStructuredReadOptions>>,
    ) -> PyResult<PyStructuredMultiGetResult> {
        let keys = extract_keys(keys)?;
        let options = options.map(|value| value.inner.clone());
        py.detach(|| {
            let result = match options {
                Some(ref options) => self.reader.multi_get_with_options(&keys, options),
                None => self.reader.multi_get(&keys),
            };
            result
                .map(|rows| PyStructuredMultiGetResult { rows })
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (keys, output, options=None))]
    fn multi_get_into(
        &mut self,
        py: Python<'_>,
        keys: &Bound<'_, PyAny>,
        output: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyStructuredReadOptions>>,
    ) -> PyResult<PyBufferResult> {
        let keys = extract_keys(keys)?;
        let mut output = WritableBuffer::extract(output)?;
        let options = options.map(|value| value.inner.clone());
        let rows = py.detach(|| {
            match options {
                Some(ref options) => self.reader.multi_get_with_options(&keys, options),
                None => self.reader.multi_get(&keys),
            }
            .map_err(map_error)
        })?;
        encode_multi_get_into(&keys, &rows, output.as_mut_slice())
    }

    #[pyo3(signature = (bucket, start, end, options=None))]
    fn scan(
        &mut self,
        bucket: u16,
        start: &Bound<'_, PyAny>,
        end: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyStructuredScanOptions>>,
    ) -> PyResult<PyStructuredScanCursor> {
        let start = InputBytes::extract(start)?;
        let end = InputBytes::extract(end)?;
        if start.as_ref() > end.as_ref() {
            return Err(input_error("reader scan start must not exceed end"));
        }
        let options = options.map(|value| value.inner.clone());
        let iterator = match options {
            Some(ref options) => {
                self.reader
                    .scan_with_options(bucket, start.as_ref()..end.as_ref(), options)
            }
            None => self.reader.scan(bucket, start.as_ref()..end.as_ref()),
        }
        .map_err(map_error)?;
        Ok(PyStructuredScanCursor::new_reader(bucket, iterator))
    }

    fn current_schema(&self) -> PyStructuredSchema {
        schema(self.reader.current_schema())
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
    module.add_class::<PyStructuredReader>()
}
