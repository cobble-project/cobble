use crate::buffer::{InputBytes, copy_single_column};
use crate::error::{input_error, map_error};
use crate::metrics::{PyMetricSample, metrics};
use crate::multi_get::{PyMultiGetResult, extract_keys};
use crate::options::{PyReadOptions, PyScanOptions};
use crate::row::PyOwnedRow;
use crate::scan::PyScanCursor;
use crate::schema::{PySchema, schema};
use crate::types::PyBufferResult;
use cobble_binding::{Config, ReadOnlyDb, ReadOptions};
use pyo3::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;

#[pyclass(name = "ReadOnlyDb", module = "pycobble._native")]
pub(crate) struct PyReadOnlyDb {
    db: Arc<ReadOnlyDb>,
}

impl PyReadOnlyDb {
    fn open_config(config: Config, snapshot_id: u64, db_id: String) -> PyResult<Self> {
        opendal::install_default();
        ReadOnlyDb::open_with_db_id(config, snapshot_id, db_id)
            .map(|db| Self { db: Arc::new(db) })
            .map_err(map_error)
    }

    fn read_options(options: Option<PyRef<'_, PyReadOptions>>) -> ReadOptions {
        options.map_or_else(ReadOptions::default, |options| options.inner.clone())
    }
}

#[pymethods]
impl PyReadOnlyDb {
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
        options: Option<PyRef<'_, PyReadOptions>>,
    ) -> PyResult<PyOwnedRow> {
        let key = InputBytes::extract(key)?;
        let db = Arc::clone(&self.db);
        let options = Self::read_options(options);
        py.detach(move || {
            db.get_with_options(bucket, key.as_ref(), &options)
                .map(PyOwnedRow::new)
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (keys, options=None))]
    fn multi_get(
        &self,
        py: Python<'_>,
        keys: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyReadOptions>>,
    ) -> PyResult<PyMultiGetResult> {
        let keys = extract_keys(keys)?;
        let db = Arc::clone(&self.db);
        let options = Self::read_options(options);
        py.detach(move || {
            db.multi_get_with_options(&keys, &options)
                .map(PyMultiGetResult::new)
                .map_err(map_error)
        })
    }

    fn get_column_into(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        output: &Bound<'_, PyAny>,
        options: PyRef<'_, PyReadOptions>,
    ) -> PyResult<PyBufferResult> {
        let key = InputBytes::extract(key)?;
        if options.inner.column_indices.as_ref().map(Vec::len) != Some(1) {
            return Err(input_error(
                "get_column_into requires ReadOptions with exactly one column",
            ));
        }
        let db = Arc::clone(&self.db);
        let options = options.inner.clone();
        let columns = py.detach(move || {
            db.get_with_options(bucket, key.as_ref(), &options)
                .map_err(map_error)
        })?;
        copy_single_column(columns, output)
    }

    #[pyo3(signature = (bucket, start=None, end=None, options=None))]
    fn scan(
        &self,
        bucket: u16,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        options: Option<PyRef<'_, PyScanOptions>>,
    ) -> PyResult<PyScanCursor> {
        let start = start.map(InputBytes::extract).transpose()?;
        let end = end.map(InputBytes::extract).transpose()?;
        let db = Arc::clone(&self.db);
        let options = options.map_or_else(Default::default, |options| options.inner.clone());
        let iterator = db
            .scan_with_options_bounds(
                bucket,
                start.as_ref().map(AsRef::as_ref),
                end.as_ref().map(AsRef::as_ref),
                &options,
            )
            .map_err(map_error)?;
        Ok(PyScanCursor::new_read_only(bucket, iterator, db))
    }

    fn current_schema(&self, py: Python<'_>) -> PyResult<PySchema> {
        let db = Arc::clone(&self.db);
        py.detach(move || schema(db.current_schema().as_ref()))
    }

    fn metrics(&self, py: Python<'_>) -> Vec<PyMetricSample> {
        let db = Arc::clone(&self.db);
        py.detach(move || metrics(db.metrics()))
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyReadOnlyDb>()
}
