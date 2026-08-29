mod batch;
mod database;
mod priority_queue;
mod types;

use pyo3::prelude::*;

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    batch::register(module)?;
    types::register(module)?;
    priority_queue::register(module)?;
    database::register(module)
}
