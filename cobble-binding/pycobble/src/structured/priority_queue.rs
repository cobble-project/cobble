use super::database::{PyStructuredDb, PyStructuredSingleDb};
use super::encoding::{CsrbColumns, CsrbRow, buffer_result, encode_into, encoded_len};
use crate::buffer::{InputBytes, OwnedBytes, WritableBuffer};
use crate::error::{input_error, invalid_state, map_error};
use crate::types::{PyBufferResult, PyBufferStatus};
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum CallerOperation {
    Peek,
    Poll,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct CallerRequest {
    operation: CallerOperation,
    bucket: u16,
    limit: Option<usize>,
    single: bool,
}

struct PendingCallerBatch {
    request: CallerRequest,
    rows: Vec<(bytes::Bytes, bytes::Bytes)>,
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
    pending: Option<PendingCallerBatch>,
}

impl PyPriorityQueue {
    pub(crate) fn new_db(
        descriptor: ds_ffi::PriorityQueueDescriptor,
        owner: Arc<StructuredDb>,
    ) -> Self {
        Self {
            descriptor,
            owner: PriorityQueueOwner::Db(owner),
            pending: None,
        }
    }

    pub(crate) fn new_single(
        descriptor: ds_ffi::PriorityQueueDescriptor,
        owner: Arc<StructuredSingleDb>,
    ) -> Self {
        Self {
            descriptor,
            owner: PriorityQueueOwner::Single(owner),
            pending: None,
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

    fn ensure_idle(&self) -> PyResult<()> {
        if self.pending.is_some() {
            Err(invalid_state(
                "priority queue has a pending caller-buffer result; retry the same operation",
            ))
        } else {
            Ok(())
        }
    }

    fn batch_into(
        &mut self,
        request: CallerRequest,
        output: &Bound<'_, PyAny>,
    ) -> PyResult<PyBufferResult> {
        match self.pending.as_ref() {
            Some(pending) if pending.request != request => {
                return Err(invalid_state(
                    "priority queue caller-buffer retry must use the same operation, bucket, and limit",
                ));
            }
            Some(_) => {}
            None => {
                self.pending = Some(PendingCallerBatch {
                    request,
                    rows: self.peek_rows(request.bucket, request.limit)?,
                });
            }
        }

        let pending = self.pending.as_ref().expect("pending caller batch");
        let rows = pending
            .rows
            .iter()
            .map(|(key, value)| CsrbRow {
                bucket: request.bucket,
                key,
                columns: CsrbColumns::PriorityQueue(value),
            })
            .collect::<Vec<_>>();
        let required = encoded_len(&rows)?;
        let mut output = WritableBuffer::extract(output)?;
        if output.as_mut_slice().len() < required {
            return Ok(buffer_result(
                PyBufferStatus::BufferTooSmall,
                0,
                required,
                rows.len(),
            ));
        }
        if request.operation == CallerOperation::Poll
            && let Some((key, _)) = pending.rows.last()
        {
            self.advance_inner(request.bucket, key.as_ref())?;
        }
        let written = encode_into(&rows, pending.rows.is_empty(), false, output.as_mut_slice())?;
        let row_count = rows.len();
        self.pending = None;
        Ok(buffer_result(
            if row_count == 0 {
                if request.single {
                    PyBufferStatus::NotFound
                } else {
                    PyBufferStatus::End
                }
            } else {
                PyBufferStatus::Ok
            },
            written,
            written,
            row_count,
        ))
    }
}

#[pymethods]
impl PyPriorityQueue {
    #[getter]
    fn column_family(&self) -> &str {
        self.descriptor.column_family()
    }

    fn offer(
        &mut self,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.ensure_idle()?;
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

    fn delete(&mut self, bucket: u16, key: &Bound<'_, PyAny>) -> PyResult<()> {
        self.ensure_idle()?;
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

    fn peek(&mut self, bucket: u16) -> PyResult<Option<PyPriorityQueueEntry>> {
        self.ensure_idle()?;
        Ok(self
            .peek_rows(bucket, Some(1))?
            .into_iter()
            .next()
            .map(|(key, value)| PyPriorityQueueEntry { key, value }))
    }

    fn poll(&mut self, bucket: u16) -> PyResult<Option<PyPriorityQueueEntry>> {
        self.ensure_idle()?;
        let Some((key, value)) = self.peek_rows(bucket, Some(1))?.into_iter().next() else {
            return Ok(None);
        };
        self.advance_inner(bucket, key.as_ref())?;
        Ok(Some(PyPriorityQueueEntry { key, value }))
    }

    #[pyo3(signature = (bucket, limit=None))]
    fn peek_batch(&mut self, bucket: u16, limit: Option<usize>) -> PyResult<PyPriorityQueueBatch> {
        self.ensure_idle()?;
        Ok(PyPriorityQueueBatch {
            rows: self.peek_rows(bucket, limit)?,
        })
    }

    #[pyo3(signature = (bucket, limit=None))]
    fn poll_batch(&mut self, bucket: u16, limit: Option<usize>) -> PyResult<PyPriorityQueueBatch> {
        self.ensure_idle()?;
        let rows = self.peek_rows(bucket, limit)?;
        if let Some((key, _)) = rows.last() {
            self.advance_inner(bucket, key.as_ref())?;
        }
        Ok(PyPriorityQueueBatch { rows })
    }

    fn advance(&mut self, bucket: u16, key: &Bound<'_, PyAny>) -> PyResult<()> {
        self.ensure_idle()?;
        let key = InputBytes::extract(key)?;
        self.advance_inner(bucket, key.as_ref())
    }

    fn cursor(&mut self, py: Python<'_>, bucket: u16) -> PyResult<Option<Py<PyBytes>>> {
        self.ensure_idle()?;
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

    fn peek_into(&mut self, bucket: u16, output: &Bound<'_, PyAny>) -> PyResult<PyBufferResult> {
        self.batch_into(
            CallerRequest {
                operation: CallerOperation::Peek,
                bucket,
                limit: Some(1),
                single: true,
            },
            output,
        )
    }

    fn poll_into(&mut self, bucket: u16, output: &Bound<'_, PyAny>) -> PyResult<PyBufferResult> {
        self.batch_into(
            CallerRequest {
                operation: CallerOperation::Poll,
                bucket,
                limit: Some(1),
                single: true,
            },
            output,
        )
    }

    #[pyo3(signature = (bucket, output, limit=None))]
    fn peek_batch_into(
        &mut self,
        bucket: u16,
        output: &Bound<'_, PyAny>,
        limit: Option<usize>,
    ) -> PyResult<PyBufferResult> {
        self.batch_into(
            CallerRequest {
                operation: CallerOperation::Peek,
                bucket,
                limit,
                single: false,
            },
            output,
        )
    }

    #[pyo3(signature = (bucket, output, limit=None))]
    fn poll_batch_into(
        &mut self,
        bucket: u16,
        output: &Bound<'_, PyAny>,
        limit: Option<usize>,
    ) -> PyResult<PyBufferResult> {
        self.batch_into(
            CallerRequest {
                operation: CallerOperation::Poll,
                bucket,
                limit,
                single: false,
            },
            output,
        )
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
