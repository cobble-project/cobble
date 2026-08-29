use crate::buffer::{OwnedBytes, WritableBuffer};
use crate::encoding::{batch_encoded_len, encode_batch_into};
use crate::error::{input_error, invalid_state, map_error};
use crate::types::{PyBufferResult, PyBufferStatus};
use bytes::Bytes;
use cobble_binding::{Db, DbIterator, SingleDb};
use pyo3::prelude::*;
use std::sync::Arc;

pub(crate) struct BatchRow {
    pub(crate) bucket: u16,
    pub(crate) key: Bytes,
    pub(crate) columns: Vec<Option<Bytes>>,
}

#[pyclass(name = "ScanRow", module = "pycobble._native", frozen)]
pub(crate) struct PyScanRow {
    row: BatchRow,
}

impl PyScanRow {
    fn from_row(row: &BatchRow) -> Self {
        Self {
            row: BatchRow {
                bucket: row.bucket,
                key: row.key.clone(),
                columns: row.columns.clone(),
            },
        }
    }
}

#[pymethods]
impl PyScanRow {
    #[getter]
    fn bucket(&self) -> u16 {
        self.row.bucket
    }

    #[getter]
    fn key(&self) -> OwnedBytes {
        OwnedBytes::new(self.row.key.clone())
    }

    #[getter]
    fn column_count(&self) -> usize {
        self.row.columns.len()
    }

    fn has_column(&self, column: usize) -> bool {
        self.row.columns.get(column).is_some_and(Option::is_some)
    }

    fn column(&self, column: usize) -> Option<OwnedBytes> {
        self.row
            .columns
            .get(column)?
            .as_ref()
            .map(|value| OwnedBytes::new(value.clone()))
    }
}

#[pyclass(name = "OwnedBatch", module = "pycobble._native", frozen)]
pub(crate) struct PyOwnedBatch {
    pub(crate) rows: Vec<BatchRow>,
    pub(crate) end: bool,
    pub(crate) stopped_at_block_boundary: bool,
}

#[pymethods]
impl PyOwnedBatch {
    fn __len__(&self) -> usize {
        self.rows.len()
    }

    fn row(&self, index: usize) -> PyResult<PyScanRow> {
        self.rows
            .get(index)
            .map(PyScanRow::from_row)
            .ok_or_else(|| input_error("scan row index is out of bounds"))
    }

    #[getter]
    fn end(&self) -> bool {
        self.end
    }

    #[getter]
    fn stopped_at_block_boundary(&self) -> bool {
        self.stopped_at_block_boundary
    }
}

struct ScanState {
    bucket: u16,
    iterator: DbIterator,
    pending_row: Option<BatchRow>,
    pending_batch: Option<PyOwnedBatch>,
}

impl ScanState {
    fn next_row(&mut self) -> PyResult<Option<BatchRow>> {
        self.iterator
            .next()
            .transpose()
            .map_err(map_error)
            .map(|row| {
                row.map(|(key, columns)| BatchRow {
                    bucket: self.bucket,
                    key,
                    columns,
                })
            })
    }

    fn read_batch(&mut self, max_rows: usize) -> PyResult<PyOwnedBatch> {
        let mut rows = Vec::with_capacity(max_rows);
        if let Some(row) = self.pending_row.take() {
            rows.push(row);
        }
        while rows.len() < max_rows {
            let Some(row) = self.next_row()? else {
                let stopped = self.iterator.stopped_at_block_boundary();
                return Ok(PyOwnedBatch {
                    rows,
                    end: !stopped,
                    stopped_at_block_boundary: stopped,
                });
            };
            rows.push(row);
        }
        self.pending_row = self.next_row()?;
        let stopped = self.iterator.stopped_at_block_boundary();
        Ok(PyOwnedBatch {
            rows,
            end: self.pending_row.is_none() && !stopped,
            stopped_at_block_boundary: stopped,
        })
    }

    fn take_or_read_batch(&mut self, max_rows: usize) -> PyResult<PyOwnedBatch> {
        if let Some(batch) = self.pending_batch.take() {
            return Ok(batch);
        }
        self.read_batch(max_rows)
    }
}

#[pyclass(name = "ScanCursor", module = "pycobble._native", unsendable)]
pub(crate) struct PyScanCursor {
    // Drop the iterator/access guard before the final database owner.
    state: Option<ScanState>,
    owner: Option<ScanOwner>,
}

enum ScanOwner {
    Single { _db: Arc<SingleDb> },
    Sharded { _db: Arc<Db> },
}

impl PyScanCursor {
    pub(crate) fn new_single(bucket: u16, iterator: DbIterator, owner: Arc<SingleDb>) -> Self {
        Self {
            state: Some(ScanState {
                bucket,
                iterator,
                pending_row: None,
                pending_batch: None,
            }),
            owner: Some(ScanOwner::Single { _db: owner }),
        }
    }

    pub(crate) fn new_sharded(bucket: u16, iterator: DbIterator, owner: Arc<Db>) -> Self {
        Self {
            state: Some(ScanState {
                bucket,
                iterator,
                pending_row: None,
                pending_batch: None,
            }),
            owner: Some(ScanOwner::Sharded { _db: owner }),
        }
    }

    fn state_mut(&mut self) -> PyResult<&mut ScanState> {
        self.state
            .as_mut()
            .ok_or_else(|| invalid_state("ScanCursor is closed"))
    }
}

#[pymethods]
impl PyScanCursor {
    fn next(&mut self, max_rows: usize) -> PyResult<PyOwnedBatch> {
        if max_rows == 0 {
            return Err(input_error("max_rows must be greater than zero"));
        }
        self.state_mut()?.take_or_read_batch(max_rows)
    }

    fn next_batch_into(
        &mut self,
        max_rows: usize,
        output: &Bound<'_, PyAny>,
    ) -> PyResult<PyBufferResult> {
        if max_rows == 0 {
            return Err(input_error("max_rows must be greater than zero"));
        }
        let mut output = WritableBuffer::extract(output)?;
        let state = self.state_mut()?;
        if state.pending_batch.is_none() {
            state.pending_batch = Some(state.read_batch(max_rows)?);
        }
        let batch = state
            .pending_batch
            .as_ref()
            .ok_or_else(|| invalid_state("pending scan batch was not initialized"))?;
        if batch.rows.is_empty() {
            let status = if batch.stopped_at_block_boundary {
                PyBufferStatus::BlockBoundary
            } else {
                PyBufferStatus::End
            };
            state.pending_batch = None;
            return Ok(PyBufferResult::new(status, 0, 0, 0));
        }
        let required = batch_encoded_len(batch)?;
        if output.as_mut_slice().len() < required {
            return Ok(PyBufferResult::new(
                PyBufferStatus::BufferTooSmall,
                0,
                required,
                batch.rows.len(),
            ));
        }
        encode_batch_into(batch, &mut output.as_mut_slice()[..required])?;
        let row_count = batch.rows.len();
        state.pending_batch = None;
        Ok(PyBufferResult::new(
            PyBufferStatus::Ok,
            required,
            required,
            row_count,
        ))
    }

    fn resume_after_block_boundary(&mut self) -> PyResult<()> {
        if self
            .state
            .as_ref()
            .and_then(|state| state.pending_batch.as_ref())
            .is_some_and(|batch| batch.rows.is_empty() && batch.stopped_at_block_boundary)
        {
            self.state_mut()?.pending_batch = None;
        }
        self.state_mut()?.iterator.clear_stop_at_block_boundary();
        Ok(())
    }

    fn close(&mut self) {
        self.state.take();
        self.owner.take();
    }

    fn __enter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exception_type: &Bound<'_, PyAny>,
        _exception: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> bool {
        self.close();
        false
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyScanRow>()?;
    module.add_class::<PyOwnedBatch>()?;
    module.add_class::<PyScanCursor>()
}
