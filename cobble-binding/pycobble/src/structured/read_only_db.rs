use super::database::{PyStructuredScanCursor, encode_get_into, encode_multi_get_into};
use super::types::{
    PyStructuredMultiGetResult, PyStructuredReadOptions, PyStructuredRow, PyStructuredScanOptions,
    PyStructuredSchema, schema,
};
use crate::buffer::{InputBytes, WritableBuffer};
use crate::error::{input_error, map_error};
use crate::multi_get::extract_keys;
use crate::types::PyBufferResult;
use cobble_binding::Config;
use cobble_binding::structured::StructuredReadOnlyDb;
use pyo3::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;

#[pyclass(name = "StructuredReadOnlyDb", module = "pycobble._native")]
pub(crate) struct PyStructuredReadOnlyDb {
    db: Arc<StructuredReadOnlyDb>,
}

impl PyStructuredReadOnlyDb {
    fn open_config(config: Config, snapshot_id: u64, db_id: String) -> PyResult<Self> {
        opendal::install_default();
        StructuredReadOnlyDb::open(config, snapshot_id, db_id)
            .map(|db| Self { db: Arc::new(db) })
            .map_err(map_error)
    }
}

#[pymethods]
impl PyStructuredReadOnlyDb {
    #[staticmethod]
    fn open(
        py: Python<'_>,
        config_json: String,
        snapshot_id: u64,
        db_id: String,
    ) -> PyResult<Self> {
        py.detach(move || {
            Self::open_config(
                Config::from_json_str(&config_json).map_err(map_error)?,
                snapshot_id,
                db_id,
            )
        })
    }

    #[staticmethod]
    fn open_file(
        py: Python<'_>,
        config_path: PathBuf,
        snapshot_id: u64,
        db_id: String,
    ) -> PyResult<Self> {
        py.detach(move || {
            Self::open_config(
                Config::from_path(config_path).map_err(map_error)?,
                snapshot_id,
                db_id,
            )
        })
    }

    #[getter]
    fn id(&self) -> String {
        self.db.id().to_owned()
    }

    #[pyo3(signature = (bucket, key, options=None))]
    fn get(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyStructuredReadOptions>>,
    ) -> PyResult<PyStructuredRow> {
        let key = InputBytes::extract(key)?;
        let db = Arc::clone(&self.db);
        let options = options.map(|value| value.inner.clone());
        py.detach(move || {
            let result = match options {
                Some(ref options) => db.get_with_options(bucket, key.as_ref(), options),
                None => db.get(bucket, key.as_ref()),
            };
            result.map(PyStructuredRow::new).map_err(map_error)
        })
    }

    #[pyo3(signature = (bucket, key, output, options=None))]
    fn get_into(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        output: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyStructuredReadOptions>>,
    ) -> PyResult<PyBufferResult> {
        let key = InputBytes::extract(key)?;
        let mut output = WritableBuffer::extract(output)?;
        let db = Arc::clone(&self.db);
        let options = options.map(|value| value.inner.clone());
        let columns = py.detach(|| {
            match options {
                Some(ref options) => db.get_with_options(bucket, key.as_ref(), options),
                None => db.get(bucket, key.as_ref()),
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
        &self,
        py: Python<'_>,
        keys: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyStructuredReadOptions>>,
    ) -> PyResult<PyStructuredMultiGetResult> {
        let keys = extract_keys(keys)?;
        let db = Arc::clone(&self.db);
        let options = options.map(|value| value.inner.clone());
        py.detach(move || {
            let result = match options {
                Some(ref options) => db.multi_get_with_options(&keys, options),
                None => db.multi_get(&keys),
            };
            result
                .map(|rows| PyStructuredMultiGetResult { rows })
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (keys, output, options=None))]
    fn multi_get_into(
        &self,
        py: Python<'_>,
        keys: &Bound<'_, PyAny>,
        output: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyStructuredReadOptions>>,
    ) -> PyResult<PyBufferResult> {
        let keys = extract_keys(keys)?;
        let mut output = WritableBuffer::extract(output)?;
        let db = Arc::clone(&self.db);
        let options = options.map(|value| value.inner.clone());
        let rows = py.detach(|| {
            match options {
                Some(ref options) => db.multi_get_with_options(&keys, options),
                None => db.multi_get(&keys),
            }
            .map_err(map_error)
        })?;
        encode_multi_get_into(&keys, &rows, output.as_mut_slice())
    }

    #[pyo3(signature = (bucket, start, end, options=None))]
    fn scan(
        &self,
        bucket: u16,
        start: &Bound<'_, PyAny>,
        end: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyStructuredScanOptions>>,
    ) -> PyResult<PyStructuredScanCursor> {
        let start = InputBytes::extract(start)?;
        let end = InputBytes::extract(end)?;
        if start.as_ref() > end.as_ref() {
            return Err(input_error("read-only scan start must not exceed end"));
        }
        let options = options.map(|value| value.inner.clone());
        let db = Arc::clone(&self.db);
        let iterator = match options {
            Some(ref options) => {
                db.scan_with_options(bucket, start.as_ref()..end.as_ref(), options)
            }
            None => db.scan(bucket, start.as_ref()..end.as_ref()),
        }
        .map_err(map_error)?;
        Ok(PyStructuredScanCursor::new_read_only(bucket, iterator, db))
    }

    fn current_schema(&self) -> PyStructuredSchema {
        schema(self.db.current_schema())
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyStructuredReadOnlyDb>()
}
