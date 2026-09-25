use super::batch::PyStructuredWriteBatch;
use super::encoding::{CsrbColumns, CsrbRow, buffer_result, prepare};
use super::types::{
    PyStructuredMultiGetResult, PyStructuredReadOptions, PyStructuredRow, PyStructuredScanOptions,
    PyStructuredSchema, PyStructuredSchemaBuilder, StructuredOwner, schema,
};
use crate::buffer::{InputBytes, WritableBuffer};
use crate::error::{input_error, invalid_state, map_error};
use crate::metrics::{PyMetricSample, metrics};
use crate::multi_get::extract_keys;
use crate::snapshot::{
    PyBucketRange, PyGlobalSnapshot, PyPendingShardSnapshot, PyPendingSnapshot, PyShardSnapshot,
    shard_metadata, snapshot,
};
use crate::types::{
    PyBufferResult, PyBufferStatus, PyExpandStorageMode, PyMemtableType, PyRecoveryMode,
};
use cobble_binding::Config;
use cobble_binding::structured::ffi as ds_ffi;
use cobble_binding::structured::{
    StructuredColumnValue, StructuredDb, StructuredDbIterator, StructuredReadOnlyDb,
    StructuredScanSplitScanner, StructuredSingleDb, StructuredWriteOptions,
};
use pyo3::prelude::*;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn write_options(
    options: Option<PyRef<'_, crate::options::PyWriteOptions>>,
) -> StructuredWriteOptions {
    options.map_or_else(StructuredWriteOptions::default, |options| {
        options.inner.clone().into()
    })
}

fn extract_elements(values: &Bound<'_, PyAny>) -> PyResult<Vec<InputBytes>> {
    values
        .try_iter()?
        .map(|value| InputBytes::extract(&value?))
        .collect()
}

pub(super) fn encode_get_into(
    bucket: u16,
    key: &[u8],
    columns: Option<&[Option<StructuredColumnValue>]>,
    output: &mut [u8],
) -> PyResult<PyBufferResult> {
    let rows = [CsrbRow {
        bucket,
        key,
        columns: columns
            .map(CsrbColumns::Structured)
            .unwrap_or(CsrbColumns::Missing),
    }];
    let prepared = prepare(&rows, false, false)?;
    let required = prepared.required_len();
    if output.len() < required {
        return Ok(buffer_result(
            PyBufferStatus::BufferTooSmall,
            0,
            required,
            1,
        ));
    }
    let written = prepared.encode_into(output);
    Ok(buffer_result(
        if columns.is_some() {
            PyBufferStatus::Ok
        } else {
            PyBufferStatus::NotFound
        },
        written,
        written,
        1,
    ))
}

pub(super) fn encode_multi_get_into(
    keys: &[(u16, InputBytes)],
    values: &[Option<Vec<Option<StructuredColumnValue>>>],
    output: &mut [u8],
) -> PyResult<PyBufferResult> {
    if keys.len() != values.len() {
        return Err(invalid_state("structured multi_get result count mismatch"));
    }
    let rows = keys
        .iter()
        .zip(values)
        .map(|((bucket, key), columns)| CsrbRow {
            bucket: *bucket,
            key: key.as_ref(),
            columns: columns
                .as_deref()
                .map(CsrbColumns::Structured)
                .unwrap_or(CsrbColumns::Missing),
        })
        .collect::<Vec<_>>();
    let prepared = prepare(&rows, false, false)?;
    let required = prepared.required_len();
    if output.len() < required {
        return Ok(buffer_result(
            PyBufferStatus::BufferTooSmall,
            0,
            required,
            rows.len(),
        ));
    }
    let written = prepared.encode_into(output);
    Ok(buffer_result(
        PyBufferStatus::Ok,
        written,
        written,
        rows.len(),
    ))
}

enum IteratorOwner {
    Db { _db: Arc<StructuredDb> },
    Single { _db: Arc<StructuredSingleDb> },
    ReadOnly { _db: Arc<StructuredReadOnlyDb> },
}

// Keep the common database iterator inline so ordinary scans do not gain a
// heap allocation; only the larger, cold-path distributed scanner is boxed.
#[allow(clippy::large_enum_variant)]
enum StructuredIterator {
    Db(StructuredDbIterator),
    Split(Box<StructuredScanSplitScanner>),
}

struct StructuredBatchRow {
    bucket: u16,
    key: bytes::Bytes,
    columns: Vec<Option<StructuredColumnValue>>,
}

#[pyclass(name = "StructuredScanRow", module = "pycobble._native", frozen)]
pub(crate) struct PyStructuredScanRow {
    row: StructuredBatchRow,
}

#[pymethods]
impl PyStructuredScanRow {
    #[getter]
    fn bucket(&self) -> u16 {
        self.row.bucket
    }

    #[getter]
    fn key(&self) -> crate::buffer::OwnedBytes {
        crate::buffer::OwnedBytes::new(self.row.key.clone())
    }

    #[getter]
    fn value(&self) -> PyStructuredRow {
        PyStructuredRow::new(Some(self.row.columns.clone()))
    }
}

#[pyclass(name = "StructuredBatch", module = "pycobble._native", frozen)]
pub(crate) struct PyStructuredBatch {
    rows: Vec<StructuredBatchRow>,
    #[pyo3(get)]
    end: bool,
    #[pyo3(get)]
    stopped_at_block_boundary: bool,
}

#[pymethods]
impl PyStructuredBatch {
    fn __len__(&self) -> usize {
        self.rows.len()
    }

    fn row(&self, index: usize) -> PyResult<PyStructuredScanRow> {
        let row = self
            .rows
            .get(index)
            .ok_or_else(|| input_error("scan row index is out of bounds"))?;
        Ok(PyStructuredScanRow {
            row: StructuredBatchRow {
                bucket: row.bucket,
                key: row.key.clone(),
                columns: row.columns.clone(),
            },
        })
    }
}

#[pyclass(name = "StructuredScanCursor", module = "pycobble._native", unsendable)]
pub(crate) struct PyStructuredScanCursor {
    iterator: Option<StructuredIterator>,
    bucket: u16,
    pending: Option<StructuredBatchRow>,
    pending_batch: Option<PyStructuredBatch>,
    owner: Option<IteratorOwner>,
}

impl PyStructuredScanCursor {
    fn new_db(bucket: u16, iterator: StructuredDbIterator, db: Arc<StructuredDb>) -> Self {
        Self {
            iterator: Some(StructuredIterator::Db(iterator)),
            bucket,
            pending: None,
            pending_batch: None,
            owner: Some(IteratorOwner::Db { _db: db }),
        }
    }

    fn new_single(
        bucket: u16,
        iterator: StructuredDbIterator,
        db: Arc<StructuredSingleDb>,
    ) -> Self {
        Self {
            iterator: Some(StructuredIterator::Db(iterator)),
            bucket,
            pending: None,
            pending_batch: None,
            owner: Some(IteratorOwner::Single { _db: db }),
        }
    }

    pub(super) fn new_reader(bucket: u16, iterator: StructuredDbIterator) -> Self {
        Self {
            iterator: Some(StructuredIterator::Db(iterator)),
            bucket,
            pending: None,
            pending_batch: None,
            owner: None,
        }
    }

    pub(super) fn new_read_only(
        bucket: u16,
        iterator: StructuredDbIterator,
        db: Arc<StructuredReadOnlyDb>,
    ) -> Self {
        Self {
            iterator: Some(StructuredIterator::Db(iterator)),
            bucket,
            pending: None,
            pending_batch: None,
            owner: Some(IteratorOwner::ReadOnly { _db: db }),
        }
    }

    pub(super) fn new_split(iterator: StructuredScanSplitScanner) -> Self {
        Self {
            iterator: Some(StructuredIterator::Split(Box::new(iterator))),
            bucket: 0,
            pending: None,
            pending_batch: None,
            owner: None,
        }
    }

    fn next_row(&mut self) -> PyResult<Option<StructuredBatchRow>> {
        let iterator = self
            .iterator
            .as_mut()
            .ok_or_else(|| invalid_state("StructuredScanCursor is closed"))?;
        match iterator {
            StructuredIterator::Db(iterator) => {
                iterator.next().transpose().map_err(map_error).map(|row| {
                    row.map(|(key, columns)| StructuredBatchRow {
                        bucket: self.bucket,
                        key,
                        columns,
                    })
                })
            }
            StructuredIterator::Split(iterator) => {
                iterator.next().transpose().map_err(map_error).map(|row| {
                    row.map(|(bucket, key, columns)| StructuredBatchRow {
                        bucket,
                        key,
                        columns,
                    })
                })
            }
        }
    }

    fn read_batch(&mut self, max_rows: usize) -> PyResult<PyStructuredBatch> {
        if max_rows == 0 {
            return Err(input_error("max_rows must be greater than zero"));
        }
        let mut rows = Vec::with_capacity(max_rows);
        if let Some(row) = self.pending.take() {
            rows.push(row);
        }
        while rows.len() < max_rows {
            let Some(row) = self.next_row()? else {
                break;
            };
            rows.push(row);
        }
        if rows.len() == max_rows {
            self.pending = self.next_row()?;
        }
        let iterator = self
            .iterator
            .as_ref()
            .ok_or_else(|| invalid_state("StructuredScanCursor is closed"))?;
        let stopped = match iterator {
            StructuredIterator::Db(iterator) => {
                ds_ffi::iterator_stopped_at_block_boundary(iterator)
            }
            StructuredIterator::Split(_) => false,
        };
        Ok(PyStructuredBatch {
            end: self.pending.is_none() && !stopped,
            stopped_at_block_boundary: stopped,
            rows,
        })
    }
}

#[pymethods]
impl PyStructuredScanCursor {
    fn next(&mut self, max_rows: usize) -> PyResult<PyStructuredBatch> {
        if let Some(batch) = self.pending_batch.take() {
            return Ok(batch);
        }
        self.read_batch(max_rows)
    }

    fn next_into(
        &mut self,
        max_rows: usize,
        output: &Bound<'_, PyAny>,
    ) -> PyResult<PyBufferResult> {
        if self.pending_batch.is_none() {
            self.pending_batch = Some(self.read_batch(max_rows)?);
        }
        let batch = self
            .pending_batch
            .as_ref()
            .ok_or_else(|| invalid_state("pending structured scan batch is unavailable"))?;
        let rows = batch
            .rows
            .iter()
            .map(|row| CsrbRow {
                bucket: row.bucket,
                key: row.key.as_ref(),
                columns: CsrbColumns::Structured(&row.columns),
            })
            .collect::<Vec<_>>();
        let prepared = prepare(&rows, batch.end, batch.stopped_at_block_boundary)?;
        let required = prepared.required_len();
        let mut output = WritableBuffer::extract(output)?;
        if output.as_mut_slice().len() < required {
            return Ok(buffer_result(
                PyBufferStatus::BufferTooSmall,
                0,
                required,
                rows.len(),
            ));
        }
        let written = prepared.encode_into(output.as_mut_slice());
        let status = if rows.is_empty() && batch.end {
            PyBufferStatus::End
        } else if rows.is_empty() && batch.stopped_at_block_boundary {
            PyBufferStatus::BlockBoundary
        } else {
            PyBufferStatus::Ok
        };
        let row_count = rows.len();
        self.pending_batch = None;
        Ok(buffer_result(status, written, written, row_count))
    }

    fn resume_after_block_boundary(&mut self) -> PyResult<()> {
        if self
            .pending_batch
            .as_ref()
            .is_some_and(|batch| batch.rows.is_empty() && batch.stopped_at_block_boundary)
        {
            self.pending_batch = None;
        }
        let iterator = self
            .iterator
            .as_mut()
            .ok_or_else(|| invalid_state("StructuredScanCursor is closed"))?;
        match iterator {
            StructuredIterator::Db(iterator) => {
                ds_ffi::iterator_clear_stop_at_block_boundary(iterator);
                Ok(())
            }
            StructuredIterator::Split(_) => Err(invalid_state(
                "split scanners do not support block-boundary resume",
            )),
        }
    }

    fn close(&mut self) {
        self.iterator.take();
        self.pending.take();
        self.pending_batch.take();
        self.owner.take();
    }
}

#[pyclass(name = "StructuredSingleDb", module = "pycobble._native")]
pub(crate) struct PyStructuredSingleDb {
    pub(crate) db: Arc<StructuredSingleDb>,
    pub(crate) active_builders: Arc<AtomicUsize>,
}

impl PyStructuredSingleDb {
    fn from_db(db: StructuredSingleDb) -> Self {
        Self {
            db: Arc::new(db),
            active_builders: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[pymethods]
impl PyStructuredSingleDb {
    #[staticmethod]
    fn open(py: Python<'_>, config_json: String) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            StructuredSingleDb::open(Config::from_json_str(&config_json).map_err(map_error)?)
                .map(Self::from_db)
                .map_err(map_error)
        })
    }

    #[staticmethod]
    fn open_file(py: Python<'_>, config_path: PathBuf) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            StructuredSingleDb::open(Config::from_path(config_path).map_err(map_error)?)
                .map(Self::from_db)
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (bucket, key, column, value, options=None))]
    fn put_bytes(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        value: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, crate::options::PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let value = InputBytes::extract(value)?;
        let db = Arc::clone(&self.db);
        let options = write_options(options);
        py.detach(move || {
            ds_ffi::single_db_put_borrowed_bytes_with_options(
                &db,
                bucket,
                key.as_ref(),
                column,
                value.as_ref(),
                &options,
            )
            .map_err(map_error)
        })
    }

    #[pyo3(signature = (bucket, key, column, elements, options=None))]
    fn put_list(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        elements: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, crate::options::PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let elements = extract_elements(elements)?;
        let db = Arc::clone(&self.db);
        let options = write_options(options);
        py.detach(move || {
            let refs = elements.iter().map(AsRef::as_ref).collect::<Vec<_>>();
            ds_ffi::single_db_put_borrowed_list_with_options(
                &db,
                bucket,
                key.as_ref(),
                column,
                &refs,
                &options,
            )
            .map_err(map_error)
        })
    }
    #[pyo3(signature = (bucket, key, column, value, options=None))]
    fn merge_bytes(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        value: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, crate::options::PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let value = InputBytes::extract(value)?;
        let db = Arc::clone(&self.db);
        let options = write_options(options);
        py.detach(move || {
            ds_ffi::single_db_merge_borrowed_bytes_with_options(
                &db,
                bucket,
                key.as_ref(),
                column,
                value.as_ref(),
                &options,
            )
            .map_err(map_error)
        })
    }

    #[pyo3(signature = (bucket, key, column, elements, options=None))]
    fn merge_list(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        elements: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, crate::options::PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let elements = extract_elements(elements)?;
        let db = Arc::clone(&self.db);
        let options = write_options(options);
        py.detach(move || {
            let refs = elements.iter().map(AsRef::as_ref).collect::<Vec<_>>();
            ds_ffi::single_db_merge_borrowed_list_with_options(
                &db,
                bucket,
                key.as_ref(),
                column,
                &refs,
                &options,
            )
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
        options: Option<PyRef<'_, crate::options::PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let db = Arc::clone(&self.db);
        let options = write_options(options);
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
        options: Option<PyRef<'_, PyStructuredReadOptions>>,
    ) -> PyResult<PyStructuredRow> {
        let key = InputBytes::extract(key)?;
        let db = Arc::clone(&self.db);
        let options = options.map_or_else(Default::default, |value| value.inner.clone());
        py.detach(move || {
            db.get_with_options(bucket, key.as_ref(), &options)
                .map(PyStructuredRow::new)
                .map_err(map_error)
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
        let options = options.map_or_else(Default::default, |value| value.inner.clone());
        let columns = py.detach(|| {
            db.get_with_options(bucket, key.as_ref(), &options)
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
        let options = options.map_or_else(Default::default, |value| value.inner.clone());
        py.detach(move || {
            db.multi_get_with_options(&keys, &options)
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
        let options = options.map_or_else(Default::default, |value| value.inner.clone());
        let values = py.detach(|| {
            db.multi_get_with_options(&keys, &options)
                .map_err(map_error)
        })?;
        encode_multi_get_into(&keys, &values, output.as_mut_slice())
    }

    fn write(&self, py: Python<'_>, batch: &PyStructuredWriteBatch) -> PyResult<()> {
        let operations = batch.begin()?;
        let native = PyStructuredWriteBatch::for_single(&self.db, operations.as_slice());
        let result = match native {
            Ok(native) => {
                let db = Arc::clone(&self.db);
                py.detach(move || db.write_batch(native).map_err(map_error))
            }
            Err(error) => Err(error),
        };
        batch.finish(operations, result.is_ok());
        result
    }

    #[pyo3(signature = (bucket, start=None, end=None, options=None))]
    fn scan(
        &self,
        bucket: u16,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        options: Option<PyRef<'_, PyStructuredScanOptions>>,
    ) -> PyResult<PyStructuredScanCursor> {
        let start = start.map(InputBytes::extract).transpose()?;
        let end = end.map(InputBytes::extract).transpose()?;
        let options = options.map_or_else(Default::default, |value| value.inner.clone());
        let db = Arc::clone(&self.db);
        let iterator = ds_ffi::single_db_scan_with_options_bounds(
            &db,
            bucket,
            start.as_ref().map(AsRef::as_ref),
            end.as_ref().map(AsRef::as_ref),
            &options,
        )
        .map_err(map_error)?;
        Ok(PyStructuredScanCursor::new_single(bucket, iterator, db))
    }

    fn current_schema(&self) -> PyStructuredSchema {
        schema(self.db.current_schema())
    }
    fn update_schema(slf: PyRef<'_, Self>) -> PyStructuredSchemaBuilder {
        let count = Arc::clone(&slf.active_builders);
        PyStructuredSchemaBuilder::new(StructuredOwner::Single(slf.into()), count)
    }
    fn new_priority_queue(
        &mut self,
        name: String,
    ) -> PyResult<super::priority_queue::PyPriorityQueue> {
        super::priority_queue::new_single(self, name, false)
    }
    fn get_priority_queue(&self, name: String) -> PyResult<super::priority_queue::PyPriorityQueue> {
        super::priority_queue::get_single(self, name)
    }
    fn get_or_new_priority_queue(
        &mut self,
        name: String,
    ) -> PyResult<super::priority_queue::PyPriorityQueue> {
        super::priority_queue::new_single(self, name, true)
    }
    fn snapshot(&self, py: Python<'_>) -> PyResult<u64> {
        let db = Arc::clone(&self.db);
        py.detach(move || db.snapshot().map_err(map_error))
    }
    fn start_snapshot(&self) -> PyResult<PyPendingSnapshot> {
        PyPendingSnapshot::start_structured(&self.db)
    }
    fn take_snapshot(&self, py: Python<'_>) -> PyResult<PyGlobalSnapshot> {
        PyPendingSnapshot::start_structured(&self.db)?.wait_result(py)
    }
    fn get_snapshot(&self, py: Python<'_>, snapshot_id: u64) -> PyResult<PyGlobalSnapshot> {
        let db = Arc::clone(&self.db);
        py.detach(move || {
            db.db()
                .get_snapshot(snapshot_id)
                .map(snapshot)
                .map_err(map_error)
        })
    }
    fn list_snapshots(&self, py: Python<'_>) -> PyResult<Vec<PyGlobalSnapshot>> {
        let db = Arc::clone(&self.db);
        py.detach(move || {
            db.list_snapshots()
                .map(|values| values.into_iter().map(snapshot).collect())
                .map_err(map_error)
        })
    }
    fn retain_snapshot(&self, py: Python<'_>, snapshot_id: u64) -> PyResult<bool> {
        let db = Arc::clone(&self.db);
        py.detach(move || db.retain_snapshot(snapshot_id).map_err(map_error))
    }
    fn expire_snapshot(&self, py: Python<'_>, snapshot_id: u64) -> PyResult<bool> {
        let db = Arc::clone(&self.db);
        py.detach(move || db.expire_snapshot(snapshot_id).map_err(map_error))
    }
    fn set_time(&self, unix_seconds: u32) {
        self.db.set_time(unix_seconds);
    }
    fn now_seconds(&self) -> u32 {
        self.db.db().now_seconds()
    }
    #[pyo3(signature = (memtable_type, *, flush_current=false))]
    fn switch_memtable_type(
        &self,
        py: Python<'_>,
        memtable_type: PyMemtableType,
        flush_current: bool,
    ) -> PyResult<()> {
        let db = Arc::clone(&self.db);
        py.detach(move || {
            db.switch_memtable_type(memtable_type.into(), flush_current)
                .map_err(map_error)
        })
    }
    fn load_readonly_files_to_primary(&self, py: Python<'_>) -> PyResult<usize> {
        let db = Arc::clone(&self.db);
        py.detach(move || db.load_readonly_files_to_primary().map_err(map_error))
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        if Arc::strong_count(&self.db) != 1 || self.active_builders.load(Ordering::Acquire) != 0 {
            return Err(invalid_state(
                "release all structured cursors, builders, and priority queues before closing",
            ));
        }
        let db = Arc::clone(&self.db);
        py.detach(move || db.close().map_err(map_error))
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

#[pyclass(name = "StructuredDb", module = "pycobble._native")]
pub(crate) struct PyStructuredDb {
    pub(crate) db: Arc<StructuredDb>,
    pub(crate) active_builders: Arc<AtomicUsize>,
}

impl PyStructuredDb {
    fn from_db(db: StructuredDb) -> Self {
        Self {
            db: Arc::new(db),
            active_builders: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn parse_json(config_json: &str) -> PyResult<Config> {
        Config::from_json_str(config_json).map_err(map_error)
    }

    fn parse_file(config_path: PathBuf) -> PyResult<Config> {
        Config::from_path(config_path).map_err(map_error)
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
}

fn full_ranges(
    config: &Config,
    ranges: Option<Vec<PyBucketRange>>,
) -> PyResult<Vec<RangeInclusive<u16>>> {
    if config.total_buckets == 0 || config.total_buckets > u32::from(u16::MAX) + 1 {
        return Err(input_error("total_buckets must be in range 1..=65536"));
    }
    let values = if let Some(values) = ranges {
        values
    } else {
        vec![PyBucketRange {
            start_inclusive: 0,
            end_inclusive: u16::try_from(
                config
                    .total_buckets
                    .checked_sub(1)
                    .ok_or_else(|| input_error("total_buckets must be positive"))?,
            )
            .map_err(|_| input_error("total_buckets exceeds bucket range"))?,
        }]
    };
    if values.is_empty() {
        return Err(input_error("ranges must not be empty"));
    }
    values
        .into_iter()
        .map(|value| {
            if u32::from(value.end_inclusive) >= config.total_buckets {
                return Err(input_error(format!(
                    "range {}..={} exceeds total_buckets {}",
                    value.start_inclusive, value.end_inclusive, config.total_buckets
                )));
            }
            Ok(value.start_inclusive..=value.end_inclusive)
        })
        .collect()
}

#[pymethods]
impl PyStructuredDb {
    #[staticmethod]
    #[pyo3(signature = (config_json, ranges=None))]
    fn open(
        py: Python<'_>,
        config_json: String,
        ranges: Option<Vec<PyBucketRange>>,
    ) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            let config = Config::from_json_str(&config_json).map_err(map_error)?;
            let ranges = full_ranges(&config, ranges)?;
            StructuredDb::open(config, ranges)
                .map(Self::from_db)
                .map_err(map_error)
        })
    }
    #[staticmethod]
    #[pyo3(signature = (config_path, ranges=None))]
    fn open_file(
        py: Python<'_>,
        config_path: PathBuf,
        ranges: Option<Vec<PyBucketRange>>,
    ) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            let config = Self::parse_file(config_path)?;
            let ranges = full_ranges(&config, ranges)?;
            StructuredDb::open(config, ranges)
                .map(Self::from_db)
                .map_err(map_error)
        })
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
            StructuredDb::open_from_snapshot_with_recovery_mode(
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
            StructuredDb::open_from_snapshot_with_recovery_mode(
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
            StructuredDb::open_new_with_snapshot(
                Self::parse_json(&config_json)?,
                snapshot_id,
                source_db_id,
            )
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
            StructuredDb::open_new_with_snapshot(
                Self::parse_file(config_path)?,
                snapshot_id,
                source_db_id,
            )
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
            StructuredDb::open_new_with_manifest_path(
                Self::parse_json(&config_json)?,
                manifest_path,
            )
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
            StructuredDb::open_new_with_manifest_path(Self::parse_file(config_path)?, manifest_path)
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
            StructuredDb::resume_with_recovery_mode(
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
            StructuredDb::resume_with_recovery_mode(
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
            StructuredDb::resume_from_snapshot_with_recovery_mode(
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
            StructuredDb::resume_from_snapshot_with_recovery_mode(
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
    #[pyo3(signature = (bucket,key,column,value,options=None))]
    fn put_bytes(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        value: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, crate::options::PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let value = InputBytes::extract(value)?;
        let db = Arc::clone(&self.db);
        let options = write_options(options);
        py.detach(move || {
            ds_ffi::db_put_borrowed_bytes_with_options(
                &db,
                bucket,
                key.as_ref(),
                column,
                value.as_ref(),
                &options,
            )
            .map_err(map_error)
        })
    }
    #[pyo3(signature = (bucket,key,column,elements,options=None))]
    fn put_list(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        elements: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, crate::options::PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let elements = extract_elements(elements)?;
        let db = Arc::clone(&self.db);
        let options = write_options(options);
        py.detach(move || {
            let refs = elements.iter().map(AsRef::as_ref).collect::<Vec<_>>();
            ds_ffi::db_put_borrowed_list_with_options(
                &db,
                bucket,
                key.as_ref(),
                column,
                &refs,
                &options,
            )
            .map_err(map_error)
        })
    }
    #[pyo3(signature = (bucket,key,column,value,options=None))]
    fn merge_bytes(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        value: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, crate::options::PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let value = InputBytes::extract(value)?;
        let db = Arc::clone(&self.db);
        let options = write_options(options);
        py.detach(move || {
            ds_ffi::db_merge_borrowed_bytes_with_options(
                &db,
                bucket,
                key.as_ref(),
                column,
                value.as_ref(),
                &options,
            )
            .map_err(map_error)
        })
    }
    #[pyo3(signature = (bucket,key,column,elements,options=None))]
    fn merge_list(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        elements: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, crate::options::PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let elements = extract_elements(elements)?;
        let db = Arc::clone(&self.db);
        let options = write_options(options);
        py.detach(move || {
            let refs = elements.iter().map(AsRef::as_ref).collect::<Vec<_>>();
            ds_ffi::db_merge_borrowed_list_with_options(
                &db,
                bucket,
                key.as_ref(),
                column,
                &refs,
                &options,
            )
            .map_err(map_error)
        })
    }
    #[pyo3(signature = (bucket,key,column,options=None))]
    fn delete(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        options: Option<PyRef<'_, crate::options::PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let db = Arc::clone(&self.db);
        let options = write_options(options);
        py.detach(move || {
            db.delete_with_options(bucket, key.as_ref(), column, &options)
                .map_err(map_error)
        })
    }
    #[pyo3(signature = (bucket,key,options=None))]
    fn get(
        &self,
        py: Python<'_>,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyStructuredReadOptions>>,
    ) -> PyResult<PyStructuredRow> {
        let key = InputBytes::extract(key)?;
        let db = Arc::clone(&self.db);
        let options = options.map_or_else(Default::default, |v| v.inner.clone());
        py.detach(move || {
            db.get_with_options(bucket, key.as_ref(), &options)
                .map(PyStructuredRow::new)
                .map_err(map_error)
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
        let options = options.map_or_else(Default::default, |value| value.inner.clone());
        let columns = py.detach(|| {
            db.get_with_options(bucket, key.as_ref(), &options)
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
        let options = options.map_or_else(Default::default, |value| value.inner.clone());
        py.detach(move || {
            db.multi_get_with_options(&keys, &options)
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
        let options = options.map_or_else(Default::default, |value| value.inner.clone());
        let values = py.detach(|| {
            db.multi_get_with_options(&keys, &options)
                .map_err(map_error)
        })?;
        encode_multi_get_into(&keys, &values, output.as_mut_slice())
    }

    fn write(&self, py: Python<'_>, batch: &PyStructuredWriteBatch) -> PyResult<()> {
        let operations = batch.begin()?;
        let native = PyStructuredWriteBatch::for_db(&self.db, operations.as_slice());
        let result = match native {
            Ok(native) => {
                let db = Arc::clone(&self.db);
                py.detach(move || db.write_batch(native).map_err(map_error))
            }
            Err(error) => Err(error),
        };
        batch.finish(operations, result.is_ok());
        result
    }
    #[pyo3(signature = (bucket,start=None,end=None,options=None))]
    fn scan(
        &self,
        bucket: u16,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        options: Option<PyRef<'_, PyStructuredScanOptions>>,
    ) -> PyResult<PyStructuredScanCursor> {
        let start = start.map(InputBytes::extract).transpose()?;
        let end = end.map(InputBytes::extract).transpose()?;
        let options = options.map_or_else(Default::default, |v| v.inner.clone());
        let db = Arc::clone(&self.db);
        let iterator = db
            .scan_with_options_bounds(
                bucket,
                start.as_ref().map(AsRef::as_ref),
                end.as_ref().map(AsRef::as_ref),
                &options,
            )
            .map_err(map_error)?;
        Ok(PyStructuredScanCursor::new_db(bucket, iterator, db))
    }
    fn current_schema(&self) -> PyStructuredSchema {
        schema(self.db.current_schema())
    }
    fn update_schema(slf: PyRef<'_, Self>) -> PyStructuredSchemaBuilder {
        let count = Arc::clone(&slf.active_builders);
        PyStructuredSchemaBuilder::new(StructuredOwner::Db(slf.into()), count)
    }
    fn new_priority_queue(
        &mut self,
        name: String,
    ) -> PyResult<super::priority_queue::PyPriorityQueue> {
        super::priority_queue::new_db(self, name, false)
    }
    fn get_priority_queue(&self, name: String) -> PyResult<super::priority_queue::PyPriorityQueue> {
        super::priority_queue::get_db(self, name)
    }
    fn get_or_new_priority_queue(
        &mut self,
        name: String,
    ) -> PyResult<super::priority_queue::PyPriorityQueue> {
        super::priority_queue::new_db(self, name, true)
    }
    fn snapshot(&self, py: Python<'_>) -> PyResult<u64> {
        let db = Arc::clone(&self.db);
        py.detach(move || db.snapshot().map_err(map_error))
    }
    fn start_snapshot(&self) -> PyResult<PyPendingShardSnapshot> {
        PyPendingShardSnapshot::start_structured(&self.db)
    }
    fn take_snapshot(&self, py: Python<'_>) -> PyResult<PyShardSnapshot> {
        PyPendingShardSnapshot::start_structured(&self.db)?.wait_result(py)
    }
    fn cancel_snapshot(&self, py: Python<'_>, snapshot_id: u64) -> PyResult<bool> {
        let db = Arc::clone(&self.db);
        py.detach(move || db.cancel_snapshot(snapshot_id).map_err(map_error))
    }
    fn get_shard_snapshot(&self, py: Python<'_>, snapshot_id: u64) -> PyResult<PyShardSnapshot> {
        let db = Arc::clone(&self.db);
        py.detach(move || {
            db.shard_snapshot_metadata(snapshot_id)
                .map(shard_metadata)
                .map_err(map_error)
        })
    }
    fn retain_snapshot(&self, snapshot_id: u64) -> bool {
        self.db.retain_snapshot(snapshot_id)
    }
    fn expire_snapshot(&self, py: Python<'_>, snapshot_id: u64) -> PyResult<bool> {
        let db = Arc::clone(&self.db);
        py.detach(move || db.expire_snapshot(snapshot_id).map_err(map_error))
    }
    fn switch_to_snapshot(&mut self, py: Python<'_>, snapshot_id: u64) -> PyResult<()> {
        if self.active_builders.load(Ordering::Acquire) != 0 {
            return Err(invalid_state(
                "switch_to_snapshot requires all structured schema builders to be released",
            ));
        }
        let db = Arc::get_mut(&mut self.db).ok_or_else(|| {
            invalid_state(
                "switch_to_snapshot requires all structured cursors and priority queues to be released",
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
        let ranges = Self::unchecked_ranges(ranges, "shrink")?;
        let db = Arc::clone(&self.db);
        py.detach(move || db.shrink_bucket(ranges).map_err(map_error))
    }
    fn metrics(&self, py: Python<'_>) -> Vec<PyMetricSample> {
        let db = Arc::clone(&self.db);
        py.detach(move || metrics(db.metrics()))
    }
    fn set_time(&self, unix_seconds: u32) {
        self.db.set_time(unix_seconds);
    }
    fn now_seconds(&self) -> u32 {
        self.db.now_seconds()
    }
    #[pyo3(signature = (memtable_type, *, flush_current=false))]
    fn switch_memtable_type(
        &self,
        py: Python<'_>,
        memtable_type: PyMemtableType,
        flush_current: bool,
    ) -> PyResult<()> {
        let db = Arc::clone(&self.db);
        py.detach(move || {
            db.switch_memtable_type(memtable_type.into(), flush_current)
                .map_err(map_error)
        })
    }
    fn load_readonly_files_to_primary(&self, py: Python<'_>) -> PyResult<usize> {
        let db = Arc::clone(&self.db);
        py.detach(move || db.load_readonly_files_to_primary().map_err(map_error))
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        if Arc::strong_count(&self.db) != 1 || self.active_builders.load(Ordering::Acquire) != 0 {
            return Err(invalid_state(
                "release all structured cursors, builders, and priority queues before closing",
            ));
        }
        let db = Arc::clone(&self.db);
        py.detach(move || db.close().map_err(map_error))
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
    module.add_class::<PyStructuredScanRow>()?;
    module.add_class::<PyStructuredBatch>()?;
    module.add_class::<PyStructuredScanCursor>()?;
    module.add_class::<PyStructuredSingleDb>()?;
    module.add_class::<PyStructuredDb>()
}
