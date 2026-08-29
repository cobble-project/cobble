mod buffer;
mod database;
mod error;
mod options;
mod row;
mod scan;
mod types;

use pyo3::prelude::*;

#[pymodule(gil_used = true)]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    error::register(module)?;
    types::register(module)?;
    buffer::register(module)?;
    row::register(module)?;
    options::register(module)?;
    scan::register(module)?;
    database::register(module)?;
    Ok(())
}
