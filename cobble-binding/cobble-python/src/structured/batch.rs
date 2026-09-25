use crate::buffer::InputBytes;
use crate::error::{input_error, invalid_state, map_error};
use crate::options::PyWriteOptions;
use crate::types::PickleReduction;
use cobble_binding::structured::ffi as ds_ffi;
use cobble_binding::structured::{
    StructuredDb, StructuredSingleDb, StructuredWriteBatch, StructuredWriteOptions,
};
use pyo3::prelude::*;
use std::sync::{Arc, Mutex};

type PickledOperation = (
    u8,
    u16,
    Vec<u8>,
    u16,
    Option<Vec<Vec<u8>>>,
    Option<u32>,
    Option<String>,
    bool,
);

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
    operations: Arc<Vec<Operation>>,
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
    pub(super) fn begin(&self) -> PyResult<Arc<Vec<Operation>>> {
        let mut state = self.state.lock().unwrap();
        if state.in_flight {
            return Err(invalid_state(
                "StructuredWriteBatch is already being written",
            ));
        }
        state.in_flight = true;
        Ok(Arc::clone(&state.operations))
    }
    pub(super) fn finish(&self, in_flight: Arc<Vec<Operation>>, success: bool) {
        // Drop the write-side owner before reopening mutation. This preserves
        // the Arc::get_mut invariant even if another Python thread immediately
        // appends after this method clears `in_flight`.
        drop(in_flight);
        let mut state = self.state.lock().unwrap();
        if success {
            state.operations = Arc::new(Vec::new());
        }
        state.in_flight = false;
    }
    pub(super) fn for_db(
        db: &StructuredDb,
        operations: &[Operation],
    ) -> PyResult<StructuredWriteBatch> {
        let mut batch = db.new_write_batch();
        apply(&mut batch, operations)?;
        Ok(batch)
    }
    pub(super) fn for_single(
        db: &StructuredSingleDb,
        operations: &[Operation],
    ) -> PyResult<StructuredWriteBatch> {
        let mut batch = db.new_write_batch();
        apply(&mut batch, operations)?;
        Ok(batch)
    }

    fn with_operations(&self, operation: impl FnOnce(&mut Vec<Operation>)) -> PyResult<()> {
        let mut state = self.state.lock().unwrap();
        if state.in_flight {
            return Err(invalid_state(
                "StructuredWriteBatch is currently being written",
            ));
        }
        let operations = Arc::get_mut(&mut state.operations)
            .expect("idle structured write batch has unique operations");
        operation(operations);
        Ok(())
    }
}

fn apply(batch: &mut StructuredWriteBatch, operations: &[Operation]) -> PyResult<()> {
    for operation in operations {
        match operation {
            Operation::PutBytes(bucket, key, column, value, options) => {
                ds_ffi::write_batch_put_borrowed_bytes_with_options(
                    batch,
                    *bucket,
                    key.as_ref(),
                    *column,
                    value.as_ref(),
                    options,
                )
            }
            Operation::MergeBytes(bucket, key, column, value, options) => {
                ds_ffi::write_batch_merge_borrowed_bytes_with_options(
                    batch,
                    *bucket,
                    key.as_ref(),
                    *column,
                    value.as_ref(),
                    options,
                )
            }
            Operation::PutList(bucket, key, column, values, options) => {
                let refs = values.iter().map(bytes::Bytes::as_ref).collect::<Vec<_>>();
                ds_ffi::write_batch_put_borrowed_list_with_options(
                    batch,
                    *bucket,
                    key.as_ref(),
                    *column,
                    &refs,
                    options,
                )
            }
            Operation::MergeList(bucket, key, column, values, options) => {
                let refs = values.iter().map(bytes::Bytes::as_ref).collect::<Vec<_>>();
                ds_ffi::write_batch_merge_borrowed_list_with_options(
                    batch,
                    *bucket,
                    key.as_ref(),
                    *column,
                    &refs,
                    options,
                )
            }
            Operation::Delete(bucket, key, column, options) => {
                batch.delete_with_options(*bucket, key, *column, options);
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
                operations: Arc::new(Vec::new()),
                in_flight: false,
            }),
        }
    }
    #[staticmethod]
    fn _restore(operations: Vec<PickledOperation>) -> PyResult<Self> {
        let batch = Self::new();
        for (kind, bucket, key, column, values, ttl_seconds, column_family, await_durable) in
            operations
        {
            if !matches!((kind, values.as_ref()), (0 | 1, Some(values)) if values.len() == 1)
                && !matches!((kind, values.as_ref()), (2 | 3, Some(_)) | (4, None))
            {
                return Err(input_error(
                    "invalid pickled structured write batch operation",
                ));
            }
            let mut raw_options =
                cobble_binding::WriteOptions::default().with_await_durable(await_durable);
            raw_options.ttl_seconds = ttl_seconds;
            raw_options.column_family = column_family;
            let options = StructuredWriteOptions::from(raw_options);
            let key = bytes::Bytes::from(key);
            batch.with_operations(|operations| {
                operations.push(match (kind, values) {
                    (0, Some(values)) => Operation::PutBytes(
                        bucket,
                        key,
                        column,
                        bytes::Bytes::from(values.into_iter().next().unwrap()),
                        options,
                    ),
                    (1, Some(values)) => Operation::MergeBytes(
                        bucket,
                        key,
                        column,
                        bytes::Bytes::from(values.into_iter().next().unwrap()),
                        options,
                    ),
                    (2, Some(values)) => Operation::PutList(
                        bucket,
                        key,
                        column,
                        values.into_iter().map(bytes::Bytes::from).collect(),
                        options,
                    ),
                    (3, Some(values)) => Operation::MergeList(
                        bucket,
                        key,
                        column,
                        values.into_iter().map(bytes::Bytes::from).collect(),
                        options,
                    ),
                    (4, None) => Operation::Delete(bucket, key, column, options),
                    _ => unreachable!("validated above"),
                });
            })?;
        }
        Ok(batch)
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(Vec<PickledOperation>,)>> {
        let state = self.state.lock().unwrap();
        if state.in_flight {
            return Err(invalid_state(
                "StructuredWriteBatch is currently being written",
            ));
        }
        let operations = state
            .operations
            .iter()
            .map(|operation| {
                let (kind, bucket, key, column, values, options) = match operation {
                    Operation::PutBytes(bucket, key, column, value, options) => (
                        0,
                        *bucket,
                        key,
                        *column,
                        Some(vec![value.to_vec()]),
                        options,
                    ),
                    Operation::MergeBytes(bucket, key, column, value, options) => (
                        1,
                        *bucket,
                        key,
                        *column,
                        Some(vec![value.to_vec()]),
                        options,
                    ),
                    Operation::PutList(bucket, key, column, values, options) => (
                        2,
                        *bucket,
                        key,
                        *column,
                        Some(values.iter().map(|value| value.to_vec()).collect()),
                        options,
                    ),
                    Operation::MergeList(bucket, key, column, values, options) => (
                        3,
                        *bucket,
                        key,
                        *column,
                        Some(values.iter().map(|value| value.to_vec()).collect()),
                        options,
                    ),
                    Operation::Delete(bucket, key, column, options) => {
                        (4, *bucket, key, *column, None, options)
                    }
                };
                let raw_options = options.as_cobble();
                (
                    kind,
                    bucket,
                    key.to_vec(),
                    column,
                    values,
                    raw_options.ttl_seconds,
                    raw_options.column_family.clone(),
                    raw_options.await_durable,
                )
            })
            .collect();
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (operations,),
        ))
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
        self.with_operations(|operations| {
            operations.push(Operation::PutBytes(
                bucket,
                key,
                column,
                value,
                write_options(options),
            ));
        })
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
        self.with_operations(|operations| {
            operations.push(Operation::MergeBytes(
                bucket,
                key,
                column,
                value,
                write_options(options),
            ));
        })
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
        let values = elements(values)?;
        self.with_operations(|operations| {
            operations.push(Operation::PutList(
                bucket,
                key,
                column,
                values,
                write_options(options),
            ));
        })
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
        let values = elements(values)?;
        self.with_operations(|operations| {
            operations.push(Operation::MergeList(
                bucket,
                key,
                column,
                values,
                write_options(options),
            ));
        })
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
        self.with_operations(|operations| {
            operations.push(Operation::Delete(
                bucket,
                key,
                column,
                write_options(options),
            ));
        })
    }
    fn clear(&self) -> PyResult<()> {
        let mut state = self.state.lock().unwrap();
        if state.in_flight {
            return Err(invalid_state("StructuredWriteBatch is being written"));
        }
        state.operations = Arc::new(Vec::new());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_flight_operations_are_shared_and_retryable() {
        let batch = PyStructuredWriteBatch::new();
        batch
            .with_operations(|operations| {
                operations.push(Operation::Delete(
                    0,
                    bytes::Bytes::from_static(b"key"),
                    0,
                    StructuredWriteOptions::default(),
                ));
            })
            .unwrap();

        let in_flight = batch.begin().unwrap();
        let state = batch.state.lock().unwrap();
        assert!(Arc::ptr_eq(&in_flight, &state.operations));
        drop(state);
        assert!(batch.with_operations(|_| {}).is_err());

        batch.finish(in_flight, false);
        assert_eq!(batch.__len__(), 1);
        let retry = batch.begin().unwrap();
        assert_eq!(retry.len(), 1);
        batch.finish(retry, true);
        assert_eq!(batch.__len__(), 0);
    }
}
