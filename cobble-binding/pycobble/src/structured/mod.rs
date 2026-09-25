mod batch;
mod database;
mod encoding;
mod priority_queue;
mod read_only_db;
mod reader;
mod scan_plan;
mod types;

use pyo3::prelude::*;

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    batch::register(module)?;
    types::register(module)?;
    priority_queue::register(module)?;
    read_only_db::register(module)?;
    reader::register(module)?;
    scan_plan::register(module)?;
    database::register(module)
}
