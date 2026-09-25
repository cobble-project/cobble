use crate::buffer::OwnedBytes;
use crate::types::PickleReduction;
use bytes::Bytes;
use pyo3::prelude::*;

pub(crate) type PickledColumns = Option<Vec<Option<Vec<u8>>>>;

#[pyclass(name = "OwnedRow", module = "pycobble._native", frozen)]
pub(crate) struct PyOwnedRow {
    columns: Option<Vec<Option<Bytes>>>,
}

impl PyOwnedRow {
    pub(crate) fn new(columns: Option<Vec<Option<Bytes>>>) -> Self {
        Self { columns }
    }
}

#[pymethods]
impl PyOwnedRow {
    #[staticmethod]
    fn _restore(columns: Option<Vec<Option<Vec<u8>>>>) -> Self {
        Self::new(columns.map(|columns| {
            columns
                .into_iter()
                .map(|value| value.map(Bytes::from))
                .collect()
        }))
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(PickledColumns,)>> {
        let columns = self.columns.as_ref().map(|columns| {
            columns
                .iter()
                .map(|value| value.as_ref().map(|value| value.to_vec()))
                .collect()
        });
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (columns,),
        ))
    }

    #[getter]
    fn found(&self) -> bool {
        self.columns.is_some()
    }

    #[getter]
    fn column_count(&self) -> usize {
        self.columns.as_ref().map_or(0, Vec::len)
    }

    fn has_column(&self, column: usize) -> bool {
        self.columns
            .as_ref()
            .and_then(|columns| columns.get(column))
            .is_some_and(Option::is_some)
    }

    fn column(&self, column: usize) -> Option<OwnedBytes> {
        self.columns
            .as_ref()?
            .get(column)?
            .as_ref()
            .map(|bytes| OwnedBytes::new(bytes.clone()))
    }

    fn __bool__(&self) -> bool {
        self.found()
    }

    fn __repr__(&self) -> String {
        match &self.columns {
            Some(columns) => format!("OwnedRow(found=True, columns={})", columns.len()),
            None => "OwnedRow(found=False, columns=0)".to_string(),
        }
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyOwnedRow>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columns_share_the_database_owned_allocation() {
        let value = Bytes::from_static(b"value");
        let pointer = value.as_ptr();
        let row = PyOwnedRow::new(Some(vec![Some(value)]));
        let column = row.column(0).expect("column");
        assert_eq!(column.as_slice().as_ptr(), pointer);
        assert!(row.column(1).is_none());
    }
}
