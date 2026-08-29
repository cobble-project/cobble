use crate::buffer::InputBytes;
use crate::error::{invalid_state, map_error};
use crate::options::PyWriteOptions;
use cobble_binding::structured::ffi as ds_ffi;
use cobble_binding::structured::{
    StructuredDb, StructuredSingleDb, StructuredWriteBatch, StructuredWriteOptions,
};
use pyo3::prelude::*;
use std::sync::Mutex;

#[derive(Clone)]
pub(super) enum Operation {
    PutBytes(u16, bytes::Bytes, u16, bytes::Bytes, StructuredWriteOptions),
    MergeBytes(u16, bytes::Bytes, u16, bytes::Bytes, StructuredWriteOptions),
    PutList(
        u16,
        bytes::Bytes,
        u16,
        Vec<bytes::Bytes>,
        StructuredWriteOptions,
    ),
    MergeList(
        u16,
        bytes::Bytes,
        u16,
        Vec<bytes::Bytes>,
        StructuredWriteOptions,
    ),
    Delete(u16, bytes::Bytes, u16, StructuredWriteOptions),
}

struct State {
    operations: Vec<Operation>,
    in_flight: bool,
}

#[pyclass(name = "StructuredWriteBatch", module = "pycobble._native", frozen)]
pub(crate) struct PyStructuredWriteBatch {
    state: Mutex<State>,
}

fn write_options(value: Option<PyRef<'_, PyWriteOptions>>) -> StructuredWriteOptions {
    value.map_or_else(StructuredWriteOptions::default, |value| {
        value.inner.clone().into()
    })
}

fn elements(value: &Bound<'_, PyAny>) -> PyResult<Vec<bytes::Bytes>> {
    value
        .try_iter()?
        .map(|item| {
            InputBytes::extract(&item?).map(|value| bytes::Bytes::copy_from_slice(value.as_ref()))
        })
        .collect()
}

impl PyStructuredWriteBatch {
    pub(super) fn begin(&self) -> PyResult<Vec<Operation>> {
        let mut state = self.state.lock().unwrap();
        if state.in_flight {
            return Err(invalid_state(
                "StructuredWriteBatch is already being written",
            ));
        }
        state.in_flight = true;
        Ok(state.operations.clone())
    }
    pub(super) fn finish(&self, success: bool) {
        let mut state = self.state.lock().unwrap();
        if success {
            state.operations.clear();
        }
        state.in_flight = false;
    }
    pub(super) fn for_db(
        db: &StructuredDb,
        operations: Vec<Operation>,
    ) -> PyResult<StructuredWriteBatch> {
        let mut batch = db.new_write_batch();
        apply(&mut batch, operations)?;
        Ok(batch)
    }
    pub(super) fn for_single(
        db: &StructuredSingleDb,
        operations: Vec<Operation>,
    ) -> PyResult<StructuredWriteBatch> {
        let mut batch = db.new_write_batch();
        apply(&mut batch, operations)?;
        Ok(batch)
    }
}

fn apply(batch: &mut StructuredWriteBatch, operations: Vec<Operation>) -> PyResult<()> {
    for operation in operations {
        match operation {
            Operation::PutBytes(bucket, key, column, value, options) => {
                ds_ffi::write_batch_put_borrowed_bytes_with_options(
                    batch,
                    bucket,
                    key.as_ref(),
                    column,
                    value.as_ref(),
                    &options,
                )
            }
            Operation::MergeBytes(bucket, key, column, value, options) => {
                ds_ffi::write_batch_merge_borrowed_bytes_with_options(
                    batch,
                    bucket,
                    key.as_ref(),
                    column,
                    value.as_ref(),
                    &options,
                )
            }
            Operation::PutList(bucket, key, column, values, options) => {
                let refs = values.iter().map(bytes::Bytes::as_ref).collect::<Vec<_>>();
                ds_ffi::write_batch_put_borrowed_list_with_options(
                    batch,
                    bucket,
                    key.as_ref(),
                    column,
                    &refs,
                    &options,
                )
            }
            Operation::MergeList(bucket, key, column, values, options) => {
                let refs = values.iter().map(bytes::Bytes::as_ref).collect::<Vec<_>>();
                ds_ffi::write_batch_merge_borrowed_list_with_options(
                    batch,
                    bucket,
                    key.as_ref(),
                    column,
                    &refs,
                    &options,
                )
            }
            Operation::Delete(bucket, key, column, options) => {
                batch.delete_with_options(bucket, key, column, &options);
                Ok(())
            }
        }
        .map_err(map_error)?;
    }
    Ok(())
}

#[pymethods]
impl PyStructuredWriteBatch {
    #[new]
    fn new() -> Self {
        Self {
            state: Mutex::new(State {
                operations: Vec::new(),
                in_flight: false,
            }),
        }
    }
    #[pyo3(signature=(bucket,key,column,value,options=None))]
    fn put_bytes(
        &self,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        value: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = bytes::Bytes::copy_from_slice(InputBytes::extract(key)?.as_ref());
        let value = bytes::Bytes::copy_from_slice(InputBytes::extract(value)?.as_ref());
        self.state
            .lock()
            .unwrap()
            .operations
            .push(Operation::PutBytes(
                bucket,
                key,
                column,
                value,
                write_options(options),
            ));
        Ok(())
    }
    #[pyo3(signature=(bucket,key,column,value,options=None))]
    fn merge_bytes(
        &self,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        value: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = bytes::Bytes::copy_from_slice(InputBytes::extract(key)?.as_ref());
        let value = bytes::Bytes::copy_from_slice(InputBytes::extract(value)?.as_ref());
        self.state
            .lock()
            .unwrap()
            .operations
            .push(Operation::MergeBytes(
                bucket,
                key,
                column,
                value,
                write_options(options),
            ));
        Ok(())
    }
    #[pyo3(signature=(bucket,key,column,values,options=None))]
    fn put_list(
        &self,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        values: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = bytes::Bytes::copy_from_slice(InputBytes::extract(key)?.as_ref());
        self.state
            .lock()
            .unwrap()
            .operations
            .push(Operation::PutList(
                bucket,
                key,
                column,
                elements(values)?,
                write_options(options),
            ));
        Ok(())
    }
    #[pyo3(signature=(bucket,key,column,values,options=None))]
    fn merge_list(
        &self,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        values: &Bound<'_, PyAny>,
        options: Option<PyRef<'_, PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = bytes::Bytes::copy_from_slice(InputBytes::extract(key)?.as_ref());
        self.state
            .lock()
            .unwrap()
            .operations
            .push(Operation::MergeList(
                bucket,
                key,
                column,
                elements(values)?,
                write_options(options),
            ));
        Ok(())
    }
    #[pyo3(signature=(bucket,key,column,options=None))]
    fn delete(
        &self,
        bucket: u16,
        key: &Bound<'_, PyAny>,
        column: u16,
        options: Option<PyRef<'_, PyWriteOptions>>,
    ) -> PyResult<()> {
        let key = bytes::Bytes::copy_from_slice(InputBytes::extract(key)?.as_ref());
        self.state
            .lock()
            .unwrap()
            .operations
            .push(Operation::Delete(
                bucket,
                key,
                column,
                write_options(options),
            ));
        Ok(())
    }
    fn clear(&self) -> PyResult<()> {
        let mut state = self.state.lock().unwrap();
        if state.in_flight {
            return Err(invalid_state("StructuredWriteBatch is being written"));
        }
        state.operations.clear();
        Ok(())
    }
    fn __len__(&self) -> usize {
        self.state.lock().unwrap().operations.len()
    }
    fn __bool__(&self) -> bool {
        self.__len__() != 0
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyStructuredWriteBatch>()
}
