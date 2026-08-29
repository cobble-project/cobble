mod batch;
mod database;
mod priority_queue;
mod scan_plan;
mod types;

use pyo3::prelude::*;

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    batch::register(module)?;
    types::register(module)?;
    priority_queue::register(module)?;
    scan_plan::register(module)?;
    database::register(module)
}
