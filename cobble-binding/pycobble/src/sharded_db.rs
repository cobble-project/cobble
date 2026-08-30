use crate::buffer::{InputBytes, copy_single_column};
use crate::error::{input_error, invalid_state, map_error};
use crate::metrics::{PyMetricSample, metrics};
use crate::multi_get::{PyMultiGetResult, extract_keys};
use crate::options::{PyReadOptions, PyWriteOptions};
use crate::row::PyOwnedRow;
use crate::scan::PyScanCursor;
use crate::schema::{PySchema, PySchemaBuilder, schema};
use crate::snapshot::{PyBucketRange, PyPendingShardSnapshot, PyShardSnapshot, shard_input};
use crate::types::{PyBufferResult, PyExpandStorageMode, PyMemtableType, PyRecoveryMode};
use crate::write_batch::PyWriteBatch;
use cobble_binding::{Config, Db, ReadOptions, WriteOptions};
use pyo3::prelude::*;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

const OPEN: u8 = 0;
const CLOSING: u8 = 1;
const CLOSED: u8 = 2;

#[pyclass(name = "Db", module = "pycobble._native")]
pub(crate) struct PyDb {
    db: Arc<Db>,
    lifecycle: AtomicU8,
}

impl PyDb {
    fn from_db(db: Db) -> Self {
        Self {
            db: Arc::new(db),
            lifecycle: AtomicU8::new(OPEN),
        }
    }

    fn ensure_open(&self) -> PyResult<()> {
        match self.lifecycle.load(Ordering::Acquire) {
            OPEN => Ok(()),
            CLOSING => Err(invalid_state("Db is closing")),
            CLOSED => Err(invalid_state("Db is closed")),
            _ => Err(invalid_state("Db has an invalid lifecycle state")),
        }
    }

    fn read_options(options: Option<PyRef<'_, PyReadOptions>>) -> ReadOptions {
        options.map_or_else(ReadOptions::default, |options| options.inner.clone())
    }

    fn write_options(options: Option<PyRef<'_, PyWriteOptions>>) -> WriteOptions {
        options.map_or_else(WriteOptions::default, |options| options.inner.clone())
    }

    fn parse_json(config_json: &str) -> PyResult<Config> {
        Config::from_json_str(config_json).map_err(map_error)
    }

    fn parse_file(config_path: PathBuf) -> PyResult<Config> {
        Config::from_path(config_path).map_err(map_error)
    }

    fn full_range(config: &Config) -> PyResult<Vec<RangeInclusive<u16>>> {
        if config.total_buckets == 0 || config.total_buckets > u32::from(u16::MAX) + 1 {
            return Err(input_error("total_buckets must be in range 1..=65536"));
        }
        let end = u16::try_from(config.total_buckets - 1)
            .map_err(|_| input_error("total_buckets does not fit the bucket id range"))?;
        Ok(vec![0..=end])
    }

    fn ranges(
        config: &Config,
        ranges: Option<Vec<PyBucketRange>>,
        operation: &str,
    ) -> PyResult<Vec<RangeInclusive<u16>>> {
        let Some(ranges) = ranges else {
            return Self::full_range(config);
        };
        Self::full_range(config)?;
        if ranges.is_empty() {
            return Err(input_error(format!("{operation} ranges must not be empty")));
        }
        ranges
            .into_iter()
            .map(|range| {
                if range.start_inclusive > range.end_inclusive {
                    return Err(input_error(format!("{operation} range is reversed")));
                }
                if u32::from(range.end_inclusive) >= config.total_buckets {
                    return Err(input_error(format!(
                        "{operation} range {}..={} exceeds total_buckets {}",
                        range.start_inclusive, range.end_inclusive, config.total_buckets
                    )));
                }
                Ok(range.start_inclusive..=range.end_inclusive)
            })
            .collect()
    }

    fn unchecked_ranges(
        ranges: Vec<PyBucketRange>,
        operation: &str,
    ) -> PyResult<Vec<RangeInclusive<u16>>> {
        if ranges.is_empty() {
            return Err(input_error(format!("{operation} ranges must not be empty")));
        }
        Ok(ranges
            .into_iter()
            .map(|range| range.start_inclusive..=range.end_inclusive)
            .collect())
    }

    fn open_config(config: Config, ranges: Option<Vec<PyBucketRange>>) -> PyResult<Self> {
        opendal::install_default();
        let ranges = Self::ranges(&config, ranges, "open")?;
        Db::open(config, ranges)
            .map(Self::from_db)
            .map_err(map_error)
    }
}

#[pymethods]
impl PyDb {
    #[staticmethod]
    #[pyo3(signature = (config_json, ranges=None))]
    fn open(
        py: Python<'_>,
        config_json: String,
        ranges: Option<Vec<PyBucketRange>>,
    ) -> PyResult<Self> {
        py.detach(move || Self::open_config(Self::parse_json(&config_json)?, ranges))
    }

    #[staticmethod]
    #[pyo3(signature = (config_path, ranges=None))]
    fn open_file(
        py: Python<'_>,
        config_path: PathBuf,
        ranges: Option<Vec<PyBucketRange>>,
    ) -> PyResult<Self> {
        py.detach(move || Self::open_config(Self::parse_file(config_path)?, ranges))
    }

    #[staticmethod]
    #[pyo3(signature = (config_json, snapshot_id, db_id, recovery_mode=PyRecoveryMode::SnapshotOnly))]
    fn open_from_snapshot(
        py: Python<'_>,
        config_json: String,
        snapshot_id: u64,
        db_id: String,
        recovery_mode: PyRecoveryMode,
    ) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            Db::open_from_snapshot_with_recovery_mode(
                Self::parse_json(&config_json)?,
                snapshot_id,
                db_id,
                recovery_mode.into(),
            )
            .map(Self::from_db)
            .map_err(map_error)
        })
    }

    #[staticmethod]
    #[pyo3(signature = (config_path, snapshot_id, db_id, recovery_mode=PyRecoveryMode::SnapshotOnly))]
    fn open_from_snapshot_file(
        py: Python<'_>,
        config_path: PathBuf,
        snapshot_id: u64,
        db_id: String,
        recovery_mode: PyRecoveryMode,
    ) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            Db::open_from_snapshot_with_recovery_mode(
                Self::parse_file(config_path)?,
                snapshot_id,
                db_id,
                recovery_mode.into(),
            )
            .map(Self::from_db)
            .map_err(map_error)
        })
    }

    #[staticmethod]
    fn restore_new(
        py: Python<'_>,
        config_json: String,
        snapshot_id: u64,
        source_db_id: String,
    ) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            Db::open_new_with_snapshot(Self::parse_json(&config_json)?, snapshot_id, source_db_id)
                .map(Self::from_db)
                .map_err(map_error)
        })
    }

    #[staticmethod]
    fn restore_new_file(
        py: Python<'_>,
        config_path: PathBuf,
        snapshot_id: u64,
        source_db_id: String,
    ) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            Db::open_new_with_snapshot(Self::parse_file(config_path)?, snapshot_id, source_db_id)
                .map(Self::from_db)
                .map_err(map_error)
        })
    }

    #[staticmethod]
    fn restore_new_from_manifest(
        py: Python<'_>,
        config_json: String,
        manifest_path: String,
    ) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            Db::open_new_with_manifest_path(Self::parse_json(&config_json)?, manifest_path)
                .map(Self::from_db)
                .map_err(map_error)
        })
    }

    #[staticmethod]
    fn restore_new_from_manifest_file(
        py: Python<'_>,
        config_path: PathBuf,
        manifest_path: String,
    ) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            Db::open_new_with_manifest_path(Self::parse_file(config_path)?, manifest_path)
                .map(Self::from_db)
                .map_err(map_error)
        })
    }

    #[staticmethod]
    #[pyo3(signature = (config_json, db_id, recovery_mode=PyRecoveryMode::LatestWithWal))]
    fn resume(
        py: Python<'_>,
        config_json: String,
        db_id: String,
        recovery_mode: PyRecoveryMode,
    ) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            Db::resume_with_recovery_mode(
                Self::parse_json(&config_json)?,
                db_id,
                recovery_mode.into(),
            )
            .map(Self::from_db)
            .map_err(map_error)
        })
    }

    #[staticmethod]
    #[pyo3(signature = (config_path, db_id, recovery_mode=PyRecoveryMode::LatestWithWal))]
    fn resume_file(
        py: Python<'_>,
        config_path: PathBuf,
        db_id: String,
        recovery_mode: PyRecoveryMode,
    ) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            Db::resume_with_recovery_mode(
                Self::parse_file(config_path)?,
                db_id,
                recovery_mode.into(),
            )
            .map(Self::from_db)
            .map_err(map_error)
        })
    }

    #[staticmethod]
    #[pyo3(signature = (config_json, snapshot_id, db_id, recovery_mode=PyRecoveryMode::SnapshotOnly))]
    fn resume_from_snapshot(
        py: Python<'_>,
        config_json: String,
        snapshot_id: u64,
        db_id: String,
        recovery_mode: PyRecoveryMode,
    ) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            Db::resume_from_snapshot_with_recovery_mode(
                Self::parse_json(&config_json)?,
                snapshot_id,
                db_id,
                recovery_mode.into(),
            )
            .map(Self::from_db)
            .map_err(map_error)
        })
    }

    #[staticmethod]
    #[pyo3(signature = (config_path, snapshot_id, db_id, recovery_mode=PyRecoveryMode::SnapshotOnly))]
    fn resume_from_snapshot_file(
        py: Python<'_>,
        config_path: PathBuf,
        snapshot_id: u64,
        db_id: String,
        recovery_mode: PyRecoveryMode,
    ) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            Db::resume_from_snapshot_with_recovery_mode(
                Self::parse_file(config_path)?,
                snapshot_id,
                db_id,
                recovery_mode.into(),
            )
            .map(Self::from_db)
            .map_err(map_error)
        })
    }

    #[getter]
    fn id(&self) -> String {
        self.db.id().to_owned()
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
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        output: &Bound<'_, PyAny>,
        options: PyRef<'_, PyReadOptions>,
    ) -> PyResult<PyBufferResult> {
        self.ensure_open()?;
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
        let iterator = db
            .scan_with_options_bounds(
                bucket,
                start.as_ref().map(AsRef::as_ref),
                end.as_ref().map(AsRef::as_ref),
                &options,
            )
            .map_err(map_error)?;
        Ok(PyScanCursor::new_sharded(bucket, iterator, db))
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<u64> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        py.detach(move || db.snapshot().map_err(map_error))
    }

    fn start_snapshot(&self) -> PyResult<PyPendingShardSnapshot> {
        self.ensure_open()?;
        PyPendingShardSnapshot::start(&self.db)
    }

    fn take_snapshot(&self, py: Python<'_>) -> PyResult<PyShardSnapshot> {
        self.ensure_open()?;
        PyPendingShardSnapshot::start(&self.db)?.wait_result(py)
    }

    fn cancel_snapshot(&self, py: Python<'_>, snapshot_id: u64) -> PyResult<bool> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        py.detach(move || db.cancel_snapshot(snapshot_id).map_err(map_error))
    }

    fn get_shard_snapshot(&self, py: Python<'_>, snapshot_id: u64) -> PyResult<PyShardSnapshot> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        py.detach(move || {
            db.shard_snapshot_input(snapshot_id)
                .map(shard_input)
                .map_err(map_error)
        })
    }

    fn retain_snapshot(&self, snapshot_id: u64) -> PyResult<bool> {
        self.ensure_open()?;
        Ok(self.db.retain_snapshot(snapshot_id))
    }

    fn expire_snapshot(&self, py: Python<'_>, snapshot_id: u64) -> PyResult<bool> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        py.detach(move || db.expire_snapshot(snapshot_id).map_err(map_error))
    }

    fn switch_to_snapshot(&mut self, py: Python<'_>, snapshot_id: u64) -> PyResult<()> {
        self.ensure_open()?;
        let db = Arc::get_mut(&mut self.db).ok_or_else(|| {
            invalid_state(
                "switch_to_snapshot requires exclusive ownership; release all scan cursors and schema builders first",
            )
        })?;
        py.detach(move || db.switch_to_snapshot(snapshot_id).map_err(map_error))
    }

    #[pyo3(signature = (source_db_id, *, source_snapshot=None, ranges=None, storage_mode=PyExpandStorageMode::AdoptAsync))]
    fn expand_bucket(
        &self,
        py: Python<'_>,
        source_db_id: String,
        source_snapshot: Option<u64>,
        ranges: Option<Vec<PyBucketRange>>,
        storage_mode: PyExpandStorageMode,
    ) -> PyResult<u64> {
        self.ensure_open()?;
        let ranges = ranges
            .map(|ranges| Self::unchecked_ranges(ranges, "expand"))
            .transpose()?;
        let db = Arc::clone(&self.db);
        py.detach(move || {
            db.expand_bucket_with_storage_mode(
                source_db_id,
                source_snapshot,
                ranges,
                storage_mode.into(),
            )
            .map_err(map_error)
        })
    }

    fn wait_for_expand_adoption(&self, py: Python<'_>, timeout_seconds: f64) -> PyResult<()> {
        self.ensure_open()?;
        if !timeout_seconds.is_finite() || timeout_seconds < 0.0 {
            return Err(input_error(
                "expand adoption timeout must be finite and non-negative",
            ));
        }
        let timeout = Duration::try_from_secs_f64(timeout_seconds)
            .map_err(|_| input_error("expand adoption timeout is too large"))?;
        let db = Arc::clone(&self.db);
        py.detach(move || db.wait_for_expand_adoption(timeout).map_err(map_error))
    }

    fn shrink_bucket(&self, py: Python<'_>, ranges: Vec<PyBucketRange>) -> PyResult<u64> {
        self.ensure_open()?;
        let ranges = Self::unchecked_ranges(ranges, "shrink")?;
        let db = Arc::clone(&self.db);
        py.detach(move || db.shrink_bucket(ranges).map_err(map_error))
    }

    fn current_schema(&self, py: Python<'_>) -> PyResult<PySchema> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        py.detach(move || schema(db.current_schema().as_ref()))
    }

    fn update_schema(&self) -> PyResult<PySchemaBuilder> {
        self.ensure_open()?;
        Ok(PySchemaBuilder::new_sharded(Arc::clone(&self.db)))
    }

    fn metrics(&self, py: Python<'_>) -> PyResult<Vec<PyMetricSample>> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        Ok(py.detach(move || metrics(db.metrics())))
    }

    fn set_time(&self, unix_seconds: u32) -> PyResult<()> {
        self.ensure_open()?;
        self.db.set_time(unix_seconds);
        Ok(())
    }

    fn now_seconds(&self) -> PyResult<u32> {
        self.ensure_open()?;
        Ok(self.db.now_seconds())
    }

    #[pyo3(signature = (memtable_type, *, flush_current=false))]
    fn switch_memtable_type(
        &self,
        py: Python<'_>,
        memtable_type: PyMemtableType,
        flush_current: bool,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        py.detach(move || {
            db.switch_memtable_type(memtable_type.into(), flush_current)
                .map_err(map_error)
        })
    }

    fn load_readonly_files_to_primary(&self, py: Python<'_>) -> PyResult<usize> {
        self.ensure_open()?;
        let db = Arc::clone(&self.db);
        py.detach(move || db.load_readonly_files_to_primary().map_err(map_error))
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        match self
            .lifecycle
            .compare_exchange(OPEN, CLOSING, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {}
            Err(CLOSED) => return Ok(()),
            Err(CLOSING) => return Err(invalid_state("Db is already closing")),
            Err(_) => return Err(invalid_state("Db has an invalid lifecycle state")),
        }
        if Arc::strong_count(&self.db) != 1 {
            self.lifecycle.store(OPEN, Ordering::Release);
            return Err(invalid_state(
                "release all cursors, builders, and priority queues before closing Db",
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
    module.add_class::<PyDb>()
}
