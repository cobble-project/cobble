use super::types::{
    PyStructuredReadOptions, PyStructuredRow, PyStructuredScanOptions, PyStructuredSchema,
    PyStructuredSchemaBuilder, StructuredOwner, schema,
};
use crate::buffer::InputBytes;
use crate::error::{input_error, invalid_state, map_error};
use crate::snapshot::{PyBucketRange, PyGlobalSnapshot, PyShardSnapshot, shard_input, snapshot};
use cobble_binding::Config;
use cobble_binding::structured::ffi as ds_ffi;
use cobble_binding::structured::{
    StructuredColumnValue, StructuredDb, StructuredDbIterator, StructuredSingleDb,
    StructuredWriteOptions,
};
use pyo3::prelude::*;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};

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

enum IteratorOwner {
    Db { _db: Arc<StructuredDb> },
    Single { _db: Arc<StructuredSingleDb> },
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
    iterator: Option<StructuredDbIterator>,
    bucket: u16,
    pending: Option<StructuredBatchRow>,
    owner: Option<IteratorOwner>,
}

impl PyStructuredScanCursor {
    fn new_db(bucket: u16, iterator: StructuredDbIterator, db: Arc<StructuredDb>) -> Self {
        Self {
            iterator: Some(iterator),
            bucket,
            pending: None,
            owner: Some(IteratorOwner::Db { _db: db }),
        }
    }

    fn new_single(
        bucket: u16,
        iterator: StructuredDbIterator,
        db: Arc<StructuredSingleDb>,
    ) -> Self {
        Self {
            iterator: Some(iterator),
            bucket,
            pending: None,
            owner: Some(IteratorOwner::Single { _db: db }),
        }
    }

    fn next_row(&mut self) -> PyResult<Option<StructuredBatchRow>> {
        self.iterator
            .as_mut()
            .ok_or_else(|| invalid_state("StructuredScanCursor is closed"))?
            .next()
            .transpose()
            .map_err(map_error)
            .map(|row| {
                row.map(|(key, columns)| StructuredBatchRow {
                    bucket: self.bucket,
                    key,
                    columns,
                })
            })
    }
}

#[pymethods]
impl PyStructuredScanCursor {
    fn next(&mut self, max_rows: usize) -> PyResult<PyStructuredBatch> {
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
        let stopped = ds_ffi::iterator_stopped_at_block_boundary(iterator);
        Ok(PyStructuredBatch {
            end: self.pending.is_none() && !stopped,
            stopped_at_block_boundary: stopped,
            rows,
        })
    }

    fn resume_after_block_boundary(&mut self) -> PyResult<()> {
        let iterator = self
            .iterator
            .as_mut()
            .ok_or_else(|| invalid_state("StructuredScanCursor is closed"))?;
        ds_ffi::iterator_clear_stop_at_block_boundary(iterator);
        Ok(())
    }

    fn close(&mut self) {
        self.iterator.take();
        self.pending.take();
        self.owner.take();
    }
}

#[pyclass(name = "StructuredSingleDb", module = "pycobble._native")]
pub(crate) struct PyStructuredSingleDb {
    pub(crate) db: Arc<StructuredSingleDb>,
    pub(crate) active_builders: Arc<AtomicUsize>,
}

#[pymethods]
impl PyStructuredSingleDb {
    #[staticmethod]
    fn open(py: Python<'_>, config_json: String) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            StructuredSingleDb::open(Config::from_json_str(&config_json).map_err(map_error)?)
                .map(|db| Self {
                    db: Arc::new(db),
                    active_builders: Arc::new(AtomicUsize::new(0)),
                })
                .map_err(map_error)
        })
    }

    #[staticmethod]
    fn open_file(py: Python<'_>, config_path: PathBuf) -> PyResult<Self> {
        py.detach(move || {
            opendal::install_default();
            StructuredSingleDb::open(Config::from_path(config_path).map_err(map_error)?)
                .map(|db| Self {
                    db: Arc::new(db),
                    active_builders: Arc::new(AtomicUsize::new(0)),
                })
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
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        elements: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, crate::options::PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let elements = extract_elements(elements)?;
        let refs = elements.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        ds_ffi::single_db_put_borrowed_list_with_options(
            &self.db,
            bucket,
            key.as_ref(),
            column,
            &refs,
            &write_options(options),
        )
        .map_err(map_error)
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
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        elements: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, crate::options::PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let elements = extract_elements(elements)?;
        let refs = elements.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        ds_ffi::single_db_merge_borrowed_list_with_options(
            &self.db,
            bucket,
            key.as_ref(),
            column,
            &refs,
            &write_options(options),
        )
        .map_err(map_error)
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
    fn take_snapshot(&self, py: Python<'_>) -> PyResult<PyGlobalSnapshot> {
        let (tx, rx) = mpsc::channel();
        self.db
            .snapshot_with_callback(move |result| {
                let _ = tx.send(result);
            })
            .map_err(map_error)?;
        py.detach(move || {
            rx.recv()
                .map_err(|_| invalid_state("snapshot completion channel closed"))?
                .map(snapshot)
                .map_err(map_error)
        })
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
}

#[pyclass(name = "StructuredDb", module = "pycobble._native")]
pub(crate) struct PyStructuredDb {
    pub(crate) db: Arc<StructuredDb>,
    pub(crate) active_builders: Arc<AtomicUsize>,
}

fn full_ranges(
    config: &Config,
    ranges: Option<Vec<PyBucketRange>>,
) -> PyResult<Vec<RangeInclusive<u16>>> {
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
    Ok(values
        .into_iter()
        .map(|value| value.start_inclusive..=value.end_inclusive)
        .collect())
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
                .map(|db| Self {
                    db: Arc::new(db),
                    active_builders: Arc::new(AtomicUsize::new(0)),
                })
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
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        elements: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, crate::options::PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let elements = extract_elements(elements)?;
        let refs = elements.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        ds_ffi::db_put_borrowed_list_with_options(
            &self.db,
            bucket,
            key.as_ref(),
            column,
            &refs,
            &write_options(options),
        )
        .map_err(map_error)
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
    fn take_snapshot(&self, py: Python<'_>) -> PyResult<PyShardSnapshot> {
        let (tx, rx) = mpsc::channel();
        self.db
            .snapshot_with_callback(move |result| {
                let _ = tx.send(result);
            })
            .map_err(map_error)?;
        py.detach(move || {
            rx.recv()
                .map_err(|_| invalid_state("snapshot completion channel closed"))?
                .map(shard_input)
                .map_err(map_error)
        })
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
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyStructuredScanRow>()?;
    module.add_class::<PyStructuredBatch>()?;
    module.add_class::<PyStructuredScanCursor>()?;
    module.add_class::<PyStructuredSingleDb>()?;
    module.add_class::<PyStructuredDb>()
}
