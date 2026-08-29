use crate::buffer::{InputBytes, WritableBuffer};
use crate::error::{invalid_state, map_error};
use crate::multi_get::{PyMultiGetResult, extract_keys};
use crate::options::{PyReadOptions, PyWriteOptions};
use crate::row::PyOwnedRow;
use crate::scan::PyScanCursor;
use crate::types::PyRecoveryMode;
use crate::types::{PyBufferResult, PyBufferStatus};
use crate::write_batch::PyWriteBatch;
use cobble_binding::{Config, ReadOptions, SingleDb, WriteOptions};
use pyo3::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

const OPEN: u8 = 0;
const CLOSING: u8 = 1;
const CLOSED: u8 = 2;

#[pyclass(name = "SingleDb", module = "pycobble._native")]
pub(crate) struct PySingleDb {
    db: Arc<SingleDb>,
    lifecycle: AtomicU8,
}

impl PySingleDb {
    fn from_db(db: SingleDb) -> Self {
        Self {
            db: Arc::new(db),
            lifecycle: AtomicU8::new(OPEN),
        }
    }

    fn ensure_open(&self) -> PyResult<()> {
        match self.lifecycle.load(Ordering::Acquire) {
            OPEN => Ok(()),
            CLOSING => Err(invalid_state("SingleDb is closing")),
            CLOSED => Err(invalid_state("SingleDb is closed")),
            _ => Err(invalid_state("SingleDb has an invalid lifecycle state")),
        }
    }

    fn read_options(options: Option<PyRef<'_, PyReadOptions>>) -> ReadOptions {
        options.map_or_else(ReadOptions::default, |options| options.inner.clone())
    }

    fn write_options(options: Option<PyRef<'_, PyWriteOptions>>) -> WriteOptions {
        options.map_or_else(WriteOptions::default, |options| options.inner.clone())
    }
}

#[pymethods]
impl PySingleDb {
    #[staticmethod]
    fn open(py: Python<'_>, config_json: String) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            let config = Config::from_json_str(&config_json).map_err(map_error)?;
            SingleDb::open(config).map(Self::from_db).map_err(map_error)
        })
    }

    #[staticmethod]
    fn open_file(py: Python<'_>, config_path: PathBuf) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            let config = Config::from_path(config_path).map_err(map_error)?;
            SingleDb::open(config).map(Self::from_db).map_err(map_error)
        })
    }

    #[staticmethod]
    #[pyo3(signature = (config_json, snapshot_id, recovery_mode=PyRecoveryMode::SnapshotOnly))]
    fn resume(
        py: Python<'_>,
        config_json: String,
        snapshot_id: u64,
        recovery_mode: PyRecoveryMode,
    ) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            let config = Config::from_json_str(&config_json).map_err(map_error)?;
            SingleDb::resume_with_recovery_mode(config, snapshot_id, recovery_mode.into())
                .map(Self::from_db)
                .map_err(map_error)
        })
    }

    #[staticmethod]
    #[pyo3(signature = (config_path, snapshot_id, recovery_mode=PyRecoveryMode::SnapshotOnly))]
    fn resume_file(
        py: Python<'_>,
        config_path: PathBuf,
        snapshot_id: u64,
        recovery_mode: PyRecoveryMode,
    ) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            let config = Config::from_path(config_path).map_err(map_error)?;
            SingleDb::resume_with_recovery_mode(config, snapshot_id, recovery_mode.into())
                .map(Self::from_db)
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (bucket, key, column, value, options=None))]
    fn put(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        value: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyWriteOptions>>,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let key = InputBytes::extract(key)?;
        let value = InputBytes::extract(value)?;
        let db = Arc::clone(&self.db);
        let options = Self::write_options(options);
        py.detach(move || {
            db.put_with_options(bucket, key.as_ref(), column, value.as_ref(), &options)
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (bucket, key, column, value, options=None))]
    fn merge(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        value: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyWriteOptions>>,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let key = InputBytes::extract(key)?;
        let value = InputBytes::extract(value)?;
        let db = Arc::clone(&self.db);
        let options = Self::write_options(options);
        py.detach(move || {
            db.merge_with_options(bucket, key.as_ref(), column, value.as_ref(), &options)
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (bucket, key, column, options=None))]
    fn delete(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        options: Option<PyRef<'_, PyWriteOptions>>,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let key = InputBytes::extract(key)?;
        let db = Arc::clone(&self.db);
        let options = Self::write_options(options);
        py.detach(move || {
            db.delete_with_options(bucket, key.as_ref(), column, &options)
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (bucket, key, options=None))]
    fn get(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyReadOptions>>,
    ) -> PyResult<PyOwnedRow> {
        self.ensure_open()?;
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
        self.ensure_open()?;
        let keys = extract_keys(keys)?;
        let db = Arc::clone(&self.db);
        let options = Self::read_options(options);
        py.detach(move || {
            db.multi_get_with_options(&keys, &options)
                .map(PyMultiGetResult::new)
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (bucket, key, output, options))]
    fn get_column_into(
        &self,
        _py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        output: &Bound<'_, PyAny>,
        options: PyRef<'_, PyReadOptions>,
    ) -> PyResult<PyBufferResult> {
        self.ensure_open()?;
        let key = InputBytes::extract(key)?;
        let mut output = WritableBuffer::extract(output)?;
        if options.inner.column_indices.as_ref().map(Vec::len) != Some(1) {
            return Err(crate::error::input_error(
                "get_column_into requires ReadOptions with exactly one column",
            ));
        }
        let Some(columns) = self
            .db
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

    #[pyo3(signature = (batch, *, await_durable=true))]
    fn write(&self, py: Python<'_>, batch: &PyWriteBatch, await_durable: bool) -> PyResult<()> {
        self.ensure_open()?;
        let native_batch = batch.begin_write()?;
        let db = Arc::clone(&self.db);
        let options = WriteOptions::default().with_await_durable(await_durable);
        let result = py.detach(move || {
            db.write_batch_with_options(native_batch, &options)
                .map_err(map_error)
        });
        batch.finish_write(result.is_ok());
        result
    }

    #[pyo3(signature = (bucket, start=None, end=None, options=None))]
    fn scan(
        &self,
        _py: Python<'_>,
        bucket: u16,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        options: Option<PyRef<'_, crate::options::PyScanOptions>>,
    ) -> PyResult<PyScanCursor> {
        self.ensure_open()?;
        let start = start.map(InputBytes::extract).transpose()?;
        let end = end.map(InputBytes::extract).transpose()?;
        let db = Arc::clone(&self.db);
        let options = options.map_or_else(Default::default, |options| options.inner.clone());
        // DbIterator is thread-affine in the core today, so cursor creation and
        // advancement stay attached to the originating Python thread.
        let iterator = db
            .db()
            .scan_with_options_bounds(
                bucket,
                start.as_ref().map(AsRef::as_ref),
                end.as_ref().map(AsRef::as_ref),
                &options,
            )
            .map_err(map_error)?;
        Ok(PyScanCursor::new(bucket, iterator, db))
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<u64> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        py.detach(move || db.snapshot().map_err(map_error))
    }

    fn retain_snapshot(&self, py: Python<'_>, snapshot_id: u64) -> PyResult<bool> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        py.detach(move || db.retain_snapshot(snapshot_id).map_err(map_error))
    }

    fn expire_snapshot(&self, py: Python<'_>, snapshot_id: u64) -> PyResult<bool> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        py.detach(move || db.expire_snapshot(snapshot_id).map_err(map_error))
    }

    fn list_snapshots(&self, py: Python<'_>) -> PyResult<Vec<u64>> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        py.detach(move || {
            db.list_snapshots()
                .map(|snapshots| snapshots.into_iter().map(|snapshot| snapshot.id).collect())
                .map_err(map_error)
        })
    }

    fn set_time(&self, unix_seconds: u32) -> PyResult<()> {
        self.ensure_open()?;
        self.db.set_time(unix_seconds);
        Ok(())
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        match self
            .lifecycle
            .compare_exchange(OPEN, CLOSING, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {}
            Err(CLOSED) => return Ok(()),
            Err(CLOSING) => return Err(invalid_state("SingleDb is already closing")),
            Err(_) => return Err(invalid_state("SingleDb has an invalid lifecycle state")),
        }

        if Arc::strong_count(&self.db) != 1 {
            self.lifecycle.store(OPEN, Ordering::Release);
            return Err(invalid_state(
                "release all cursors, builders, and priority queues before closing SingleDb",
            ));
        }

        let db = Arc::clone(&self.db);
        match py.detach(move || db.close().map_err(map_error)) {
            Ok(()) => {
                self.lifecycle.store(CLOSED, Ordering::Release);
                Ok(())
            }
            Err(error) => {
                self.lifecycle.store(OPEN, Ordering::Release);
                Err(error)
            }
        }
    }

    fn __enter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _exception_type: &Bound<'_, PyAny>,
        _exception: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySingleDb>()?;
    module.add("Database", module.getattr("SingleDb")?)
}
