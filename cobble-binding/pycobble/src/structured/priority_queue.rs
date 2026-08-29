use super::database::{PyStructuredDb, PyStructuredSingleDb};
use crate::buffer::{InputBytes, OwnedBytes};
use crate::error::{input_error, invalid_state, map_error};
use cobble_binding::structured::ffi as ds_ffi;
use cobble_binding::structured::{StructuredDb, StructuredSingleDb};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::Arc;
use std::sync::atomic::Ordering;

enum PriorityQueueOwner {
    Db(Arc<StructuredDb>),
    Single(Arc<StructuredSingleDb>),
}

#[pyclass(name = "PriorityQueueEntry", module = "pycobble._native", frozen)]
pub(crate) struct PyPriorityQueueEntry {
    key: bytes::Bytes,
    value: bytes::Bytes,
}

#[pymethods]
impl PyPriorityQueueEntry {
    #[getter]
    fn key(&self) -> OwnedBytes {
        OwnedBytes::new(self.key.clone())
    }

    #[getter]
    fn value(&self) -> OwnedBytes {
        OwnedBytes::new(self.value.clone())
    }
}

#[pyclass(name = "PriorityQueueBatch", module = "pycobble._native", frozen)]
pub(crate) struct PyPriorityQueueBatch {
    rows: Vec<(bytes::Bytes, bytes::Bytes)>,
}

#[pymethods]
impl PyPriorityQueueBatch {
    fn __len__(&self) -> usize {
        self.rows.len()
    }

    fn entry(&self, index: usize) -> PyResult<PyPriorityQueueEntry> {
        self.rows
            .get(index)
            .map(|(key, value)| PyPriorityQueueEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .ok_or_else(|| input_error("priority queue entry index is out of bounds"))
    }
}

#[pyclass(name = "PriorityQueue", module = "pycobble._native", unsendable)]
pub(crate) struct PyPriorityQueue {
    descriptor: ds_ffi::PriorityQueueDescriptor,
    owner: PriorityQueueOwner,
}

impl PyPriorityQueue {
    pub(crate) fn new_db(
        descriptor: ds_ffi::PriorityQueueDescriptor,
        owner: Arc<StructuredDb>,
    ) -> Self {
        Self {
            descriptor,
            owner: PriorityQueueOwner::Db(owner),
        }
    }

    pub(crate) fn new_single(
        descriptor: ds_ffi::PriorityQueueDescriptor,
        owner: Arc<StructuredSingleDb>,
    ) -> Self {
        Self {
            descriptor,
            owner: PriorityQueueOwner::Single(owner),
        }
    }

    fn peek_rows(
        &self,
        bucket: u16,
        limit: Option<usize>,
    ) -> PyResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
        match &self.owner {
            PriorityQueueOwner::Db(db) => {
                ds_ffi::db_priority_queue_peek_batch(db, &self.descriptor, bucket, limit)
            }
            PriorityQueueOwner::Single(db) => {
                ds_ffi::single_db_priority_queue_peek_batch(db, &self.descriptor, bucket, limit)
            }
        }
        .map_err(map_error)
    }

    fn advance_inner(&self, bucket: u16, key: &[u8]) -> PyResult<()> {
        match &self.owner {
            PriorityQueueOwner::Db(db) => {
                ds_ffi::db_priority_queue_advance(db, &self.descriptor, bucket, key)
            }
            PriorityQueueOwner::Single(db) => {
                ds_ffi::single_db_priority_queue_advance(db, &self.descriptor, bucket, key)
            }
        }
        .map_err(map_error)
    }
}

#[pymethods]
impl PyPriorityQueue {
    #[getter]
    fn column_family(&self) -> &str {
        self.descriptor.column_family()
    }

    fn offer(&self, bucket: u16, key: &Bound<'_, PyAny>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let value = InputBytes::extract(value)?;
        match &self.owner {
            PriorityQueueOwner::Db(db) => ds_ffi::db_priority_queue_offer(
                db,
                &self.descriptor,
                bucket,
                key.as_ref(),
                value.as_ref(),
            ),
            PriorityQueueOwner::Single(db) => ds_ffi::single_db_priority_queue_offer(
                db,
                &self.descriptor,
                bucket,
                key.as_ref(),
                value.as_ref(),
            ),
        }
        .map_err(map_error)
    }

    fn delete(&self, bucket: u16, key: &Bound<'_, PyAny>) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        match &self.owner {
            PriorityQueueOwner::Db(db) => {
                ds_ffi::db_priority_queue_delete(db, &self.descriptor, bucket, key.as_ref())
            }
            PriorityQueueOwner::Single(db) => {
                ds_ffi::single_db_priority_queue_delete(db, &self.descriptor, bucket, key.as_ref())
            }
        }
        .map_err(map_error)
    }

    fn peek(&self, bucket: u16) -> PyResult<Option<PyPriorityQueueEntry>> {
        Ok(self
            .peek_rows(bucket, Some(1))?
            .into_iter()
            .next()
            .map(|(key, value)| PyPriorityQueueEntry { key, value }))
    }

    fn poll(&self, bucket: u16) -> PyResult<Option<PyPriorityQueueEntry>> {
        let Some((key, value)) = self.peek_rows(bucket, Some(1))?.into_iter().next() else {
            return Ok(None);
        };
        self.advance_inner(bucket, key.as_ref())?;
        Ok(Some(PyPriorityQueueEntry { key, value }))
    }

    #[pyo3(signature = (bucket, limit=None))]
    fn peek_batch(&self, bucket: u16, limit: Option<usize>) -> PyResult<PyPriorityQueueBatch> {
        Ok(PyPriorityQueueBatch {
            rows: self.peek_rows(bucket, limit)?,
        })
    }

    #[pyo3(signature = (bucket, limit=None))]
    fn poll_batch(&self, bucket: u16, limit: Option<usize>) -> PyResult<PyPriorityQueueBatch> {
        let rows = self.peek_rows(bucket, limit)?;
        if let Some((key, _)) = rows.last() {
            self.advance_inner(bucket, key.as_ref())?;
        }
        Ok(PyPriorityQueueBatch { rows })
    }

    fn advance(&self, bucket: u16, key: &Bound<'_, PyAny>) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        self.advance_inner(bucket, key.as_ref())
    }

    fn cursor(&self, py: Python<'_>, bucket: u16) -> PyResult<Option<Py<PyBytes>>> {
        let value = match &self.owner {
            PriorityQueueOwner::Db(db) => {
                ds_ffi::db_priority_queue_cursor(db, &self.descriptor, bucket)
            }
            PriorityQueueOwner::Single(db) => {
                ds_ffi::single_db_priority_queue_cursor(db, &self.descriptor, bucket)
            }
        }
        .map_err(map_error)?;
        Ok(value.map(|value| PyBytes::new(py, &value).unbind()))
    }
}

pub(crate) fn new_db(
    db: &mut PyStructuredDb,
    name: String,
    get_or_new: bool,
) -> PyResult<PyPriorityQueue> {
    if Arc::strong_count(&db.db) != 1 || db.active_builders.load(Ordering::Acquire) != 0 {
        return Err(invalid_state(
            "priority queue creation requires exclusive structured database ownership",
        ));
    }
    let owner = Arc::get_mut(&mut db.db).expect("strong count checked");
    let descriptor = if get_or_new {
        ds_ffi::db_get_or_new_priority_queue_descriptor(owner, name)
    } else {
        ds_ffi::db_new_priority_queue_descriptor(owner, name)
    }
    .map_err(map_error)?;
    Ok(PyPriorityQueue::new_db(descriptor, Arc::clone(&db.db)))
}

pub(crate) fn get_db(db: &PyStructuredDb, name: String) -> PyResult<PyPriorityQueue> {
    let descriptor = ds_ffi::db_get_priority_queue_descriptor(&db.db, name).map_err(map_error)?;
    Ok(PyPriorityQueue::new_db(descriptor, Arc::clone(&db.db)))
}

pub(crate) fn new_single(
    db: &mut PyStructuredSingleDb,
    name: String,
    get_or_new: bool,
) -> PyResult<PyPriorityQueue> {
    if Arc::strong_count(&db.db) != 1 || db.active_builders.load(Ordering::Acquire) != 0 {
        return Err(invalid_state(
            "priority queue creation requires exclusive structured database ownership",
        ));
    }
    let owner = Arc::get_mut(&mut db.db).expect("strong count checked");
    let descriptor = if get_or_new {
        ds_ffi::single_db_get_or_new_priority_queue_descriptor(owner, name)
    } else {
        ds_ffi::single_db_new_priority_queue_descriptor(owner, name)
    }
    .map_err(map_error)?;
    Ok(PyPriorityQueue::new_single(descriptor, Arc::clone(&db.db)))
}

pub(crate) fn get_single(db: &PyStructuredSingleDb, name: String) -> PyResult<PyPriorityQueue> {
    let descriptor =
        ds_ffi::single_db_get_priority_queue_descriptor(&db.db, name).map_err(map_error)?;
    Ok(PyPriorityQueue::new_single(descriptor, Arc::clone(&db.db)))
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyPriorityQueueEntry>()?;
    module.add_class::<PyPriorityQueueBatch>()?;
    module.add_class::<PyPriorityQueue>()
}
