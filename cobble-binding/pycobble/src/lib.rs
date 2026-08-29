mod buffer;
mod database;
mod encoding;
mod error;
mod metrics;
mod multi_get;
mod options;
mod row;
mod scan;
mod schema;
mod sharded_db;
mod snapshot;
mod types;
mod write_batch;

use pyo3::prelude::*;

#[pymodule(gil_used = true)]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    error::register(module)?;
    types::register(module)?;
    buffer::register(module)?;
    multi_get::register(module)?;
    metrics::register(module)?;
    row::register(module)?;
    options::register(module)?;
    scan::register(module)?;
    write_batch::register(module)?;
    schema::register(module)?;
    snapshot::register(module)?;
    database::register(module)?;
    sharded_db::register(module)?;
    Ok(())
}
