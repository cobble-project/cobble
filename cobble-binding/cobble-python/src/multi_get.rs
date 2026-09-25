use crate::buffer::InputBytes;
use crate::error::input_error;
use crate::row::{PickledColumns, PyOwnedRow};
use crate::types::PickleReduction;
use bytes::Bytes;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

type PickledRows = Vec<PickledColumns>;

pub(crate) fn extract_keys(keys: &Bound<'_, PyAny>) -> PyResult<Vec<(u16, InputBytes)>> {
    let mut extracted = Vec::new();
    for item in keys.try_iter()? {
        let item = item?;
        let tuple = item
            .cast::<PyTuple>()
            .map_err(|_| input_error("each multi_get key must be a (bucket, key) tuple"))?;
        if tuple.len() != 2 {
            return Err(input_error(
                "each multi_get key must contain exactly two values",
            ));
        }
        let bucket = tuple.get_item(0)?.extract::<u16>()?;
        let key = InputBytes::extract(&tuple.get_item(1)?)?;
        extracted.push((bucket, key));
    }
    Ok(extracted)
}

#[pyclass(name = "OwnedMultiGetResult", module = "pycobble._native", frozen)]
pub(crate) struct PyMultiGetResult {
    rows: Vec<Option<Vec<Option<Bytes>>>>,
}

impl PyMultiGetResult {
    pub(crate) fn new(rows: Vec<Option<Vec<Option<Bytes>>>>) -> Self {
        Self { rows }
    }
}

#[pymethods]
impl PyMultiGetResult {
    #[staticmethod]
    fn _restore(rows: Vec<Option<Vec<Option<Vec<u8>>>>>) -> Self {
        Self::new(
            rows.into_iter()
                .map(|row| {
                    row.map(|columns| {
                        columns
                            .into_iter()
                            .map(|value| value.map(Bytes::from))
                            .collect()
                    })
                })
                .collect(),
        )
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(PickledRows,)>> {
        let rows = self
            .rows
            .iter()
            .map(|row| {
                row.as_ref().map(|columns| {
                    columns
                        .iter()
                        .map(|value| value.as_ref().map(|value| value.to_vec()))
                        .collect()
                })
            })
            .collect();
        Ok((py.get_type::<Self>().getattr("_restore")?.unbind(), (rows,)))
    }

    fn __len__(&self) -> usize {
        self.rows.len()
    }

    fn row(&self, index: usize) -> PyResult<PyOwnedRow> {
        self.rows
            .get(index)
            .cloned()
            .map(PyOwnedRow::new)
            .ok_or_else(|| input_error("multi_get row index is out of bounds"))
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyMultiGetResult>()
}
