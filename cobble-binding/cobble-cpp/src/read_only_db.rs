use std::sync::Arc;

use cobble_binding::{Config, ReadOnlyDb};

use crate::{
    BridgeResult,
    database::{NativeDatabaseOwner, NativeRow},
    encoding::{STATUS_BUFFER_TOO_SMALL, STATUS_NOT_FOUND, STATUS_OK, buffer_result},
    error::format_cobble_error,
    ffi,
    metrics::metrics,
    multi_get::{NativeMultiGetResult, borrowed_keys},
    options::{checked_u64, to_read_options, to_scan_options, to_single_column_read_options},
    scan::{NativeScanCursor, native_scan_cursor_from_db_iterator},
    schema::native_schema,
};

pub(crate) struct NativeReadOnlyDatabase {
    pub(crate) db: Arc<ReadOnlyDb>,
}

fn open(
    config: Config,
    snapshot_id: u64,
    db_id: &str,
) -> BridgeResult<Box<NativeReadOnlyDatabase>> {
    opendal::install_default();
    ReadOnlyDb::open_with_db_id(config, snapshot_id, db_id)
        .map(|db| Box::new(NativeReadOnlyDatabase { db: Arc::new(db) }))
        .map_err(format_cobble_error)
}

pub(crate) fn native_read_only_database_open(
    config_json: &str,
    snapshot_id: u64,
    db_id: &str,
) -> BridgeResult<Box<NativeReadOnlyDatabase>> {
    open(
        Config::from_json_str(config_json).map_err(format_cobble_error)?,
        snapshot_id,
        db_id,
    )
}

pub(crate) fn native_read_only_database_open_file(
    config_path: &str,
    snapshot_id: u64,
    db_id: &str,
) -> BridgeResult<Box<NativeReadOnlyDatabase>> {
    open(
        Config::from_path(config_path).map_err(format_cobble_error)?,
        snapshot_id,
        db_id,
    )
}

pub(crate) fn native_read_only_database_id(db: &NativeReadOnlyDatabase) -> &str {
    db.db.id()
}

pub(crate) fn native_read_only_database_get(
    db: &NativeReadOnlyDatabase,
    bucket: u16,
    key: &[u8],
    options: &ffi::NativeReadOptions,
) -> BridgeResult<Box<NativeRow>> {
    Ok(Box::new(NativeRow {
        columns: db
            .db
            .get_with_options(bucket, key, &to_read_options(options)?)
            .map_err(format_cobble_error)?,
    }))
}

pub(crate) fn native_read_only_database_get_column_into(
    db: &NativeReadOnlyDatabase,
    bucket: u16,
    key: &[u8],
    output: &mut [u8],
    options: &ffi::NativeReadOptions,
) -> BridgeResult<ffi::NativeBufferResult> {
    let options = to_single_column_read_options(options)?;
    let Some(columns) = db
        .db
        .get_with_options(bucket, key, &options)
        .map_err(format_cobble_error)?
    else {
        return Ok(buffer_result(STATUS_NOT_FOUND, 0, 0, 0));
    };
    let Some(Some(column)) = columns.into_iter().next() else {
        return Ok(buffer_result(STATUS_NOT_FOUND, 0, 0, 0));
    };
    let required = checked_u64(column.len(), "column value length")?;
    if output.len() < column.len() {
        return Ok(buffer_result(STATUS_BUFFER_TOO_SMALL, 0, required, 1));
    }
    output[..column.len()].copy_from_slice(column.as_ref());
    Ok(buffer_result(STATUS_OK, required, required, 1))
}

pub(crate) fn native_read_only_database_multi_get(
    db: &NativeReadOnlyDatabase,
    descriptor_address: usize,
    count: u64,
    options: &ffi::NativeReadOptions,
) -> BridgeResult<Box<NativeMultiGetResult>> {
    // SAFETY: the private C++ wrapper keeps descriptor and key storage alive
    // for this synchronous bridge call.
    let keys = unsafe { borrowed_keys(descriptor_address, count)? };
    Ok(Box::new(NativeMultiGetResult {
        rows: db
            .db
            .multi_get_with_options(&keys, &to_read_options(options)?)
            .map_err(format_cobble_error)?,
    }))
}

pub(crate) fn native_read_only_database_scan(
    db: &NativeReadOnlyDatabase,
    bucket: u16,
    start: &[u8],
    has_start: bool,
    end: &[u8],
    has_end: bool,
    options: &ffi::NativeScanOptions,
) -> BridgeResult<Box<NativeScanCursor>> {
    let iterator = db
        .db
        .scan_with_options_bounds(
            bucket,
            has_start.then_some(start),
            has_end.then_some(end),
            &to_scan_options(options)?,
        )
        .map_err(format_cobble_error)?;
    Ok(native_scan_cursor_from_db_iterator(
        bucket,
        iterator,
        Some(NativeDatabaseOwner::read_only(db)),
    ))
}

pub(crate) fn native_read_only_database_current_schema(
    db: &NativeReadOnlyDatabase,
) -> BridgeResult<ffi::NativeSchema> {
    native_schema(db.db.current_schema().as_ref())
}

pub(crate) fn native_read_only_database_metrics(
    db: &NativeReadOnlyDatabase,
) -> Vec<ffi::NativeMetric> {
    metrics(db.db.metrics())
}
