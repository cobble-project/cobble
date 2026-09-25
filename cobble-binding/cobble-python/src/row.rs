use crate::buffer::OwnedBytes;
use bytes::Bytes;
use pyo3::prelude::*;

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
