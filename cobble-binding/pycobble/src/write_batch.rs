use crate::buffer::InputBytes;
use crate::error::invalid_state;
use crate::options::PyWriteOptions;
use cobble_binding::{WriteBatch, WriteOptions};
use pyo3::prelude::*;
use std::sync::Mutex;

struct BatchState {
    batch: WriteBatch,
    len: usize,
    in_flight: bool,
}

#[pyclass(
    name = "WriteBatch",
    module = "pycobble._native",
    frozen,
    skip_from_py_object
)]
pub(crate) struct PyWriteBatch {
    state: Mutex<BatchState>,
}

impl PyWriteBatch {
    pub(crate) fn begin_write(&self) -> PyResult<WriteBatch> {
        let mut state = self.state.lock().unwrap();
        if state.in_flight {
            return Err(invalid_state("WriteBatch is already being written"));
        }
        state.in_flight = true;
        Ok(state.batch.clone())
    }

    pub(crate) fn finish_write(&self, success: bool) {
        let mut state = self.state.lock().unwrap();
        if success {
            state.batch = WriteBatch::new();
            state.len = 0;
        }
        state.in_flight = false;
    }

    fn write_options(options: Option<PyRef<'_, PyWriteOptions>>) -> WriteOptions {
        options.map_or_else(WriteOptions::default, |options| options.inner.clone())
    }

    fn with_state(&self, operation: impl FnOnce(&mut BatchState)) -> PyResult<()> {
        let mut state = self.state.lock().unwrap();
        if state.in_flight {
            return Err(invalid_state("WriteBatch is currently being written"));
        }
        operation(&mut state);
        state.len = state.len.saturating_add(1);
        Ok(())
    }
}

#[pymethods]
impl PyWriteBatch {
    #[new]
    fn new() -> Self {
        Self {
            state: Mutex::new(BatchState {
                batch: WriteBatch::new(),
                len: 0,
                in_flight: false,
            }),
        }
    }

    #[pyo3(signature = (bucket, key, column, value, options=None))]
    fn put(
        &self,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        value: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let value = InputBytes::extract(value)?;
        let options = Self::write_options(options);
        self.with_state(move |state| {
            state
                .batch
                .put_with_options(bucket, key.as_ref(), column, value.as_ref(), &options);
        })
    }

    #[pyo3(signature = (bucket, key, column, value, options=None))]
    fn merge(
        &self,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        value: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let value = InputBytes::extract(value)?;
        let options = Self::write_options(options);
        self.with_state(move |state| {
            state
                .batch
                .merge_with_options(bucket, key.as_ref(), column, value.as_ref(), &options);
        })
    }

    #[pyo3(signature = (bucket, key, column, options=None))]
    fn delete(
        &self,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        options: Option<PyRef<'_, PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = InputBytes::extract(key)?;
        let options = Self::write_options(options);
        self.with_state(move |state| {
            state
                .batch
                .delete_with_options(bucket, key.as_ref(), column, &options);
        })
    }

    fn clear(&self) -> PyResult<()> {
        let mut state = self.state.lock().unwrap();
        if state.in_flight {
            return Err(invalid_state("WriteBatch is currently being written"));
        }
        state.batch = WriteBatch::new();
        state.len = 0;
        Ok(())
    }

    fn __len__(&self) -> usize {
        self.state.lock().unwrap().len
    }

    fn __bool__(&self) -> bool {
        self.__len__() != 0
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyWriteBatch>()
}
