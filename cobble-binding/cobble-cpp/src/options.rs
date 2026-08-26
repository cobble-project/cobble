use cobble_binding::{ReadOptions, ScanOptions, WriteOptions};
use size::Size;

use crate::{BridgeResult, error::input_error, ffi};

pub(crate) fn to_read_options(options: &ffi::NativeReadOptions) -> BridgeResult<ReadOptions> {
    let columns = decode_columns(&options.columns)?;
    Ok(match (optional_string(&options.column_family), columns) {
        (Some(column_family), Some(columns)) => {
            ReadOptions::for_columns_in_family(column_family, columns)
        }
        (Some(column_family), None) => ReadOptions::default().with_column_family(column_family),
        (None, Some(columns)) => ReadOptions::for_columns(columns),
        (None, None) => ReadOptions::default(),
    })
}

pub(crate) fn to_single_column_read_options(
    options: &ffi::NativeReadOptions,
) -> BridgeResult<ReadOptions> {
    if options.columns.len() != 1 {
        return Err(input_error(
            "get_column_into requires exactly one projected column",
        ));
    }
    to_read_options(options)
}

pub(crate) fn to_write_options(options: &ffi::NativeWriteOptions) -> WriteOptions {
    let mut write_options = WriteOptions::default().with_await_durable(options.await_durable);
    write_options.ttl_seconds = options.has_ttl_seconds.then_some(options.ttl_seconds);
    write_options.column_family = optional_string(&options.column_family);
    write_options
}

pub(crate) fn to_scan_options(options: &ffi::NativeScanOptions) -> BridgeResult<ScanOptions> {
    if options.has_max_rows && options.max_rows == 0 {
        return Err(input_error("scan max_rows must be greater than zero"));
    }
    let columns = decode_columns(&options.columns)?;
    let mut scan_options = match (optional_string(&options.column_family), columns) {
        (Some(column_family), Some(columns)) => {
            ScanOptions::for_columns(columns).with_column_family(column_family)
        }
        (Some(column_family), None) => ScanOptions::default().with_column_family(column_family),
        (None, Some(columns)) => ScanOptions::for_columns(columns),
        (None, None) => ScanOptions::default(),
    };
    let read_ahead_bytes = i64::try_from(options.read_ahead_bytes)
        .map_err(|_| input_error("scan read_ahead_bytes exceeds the supported size"))?;
    scan_options.read_ahead_bytes = Size::from_const(read_ahead_bytes);
    if options.has_max_rows {
        scan_options.set_max_rows(checked_usize(options.max_rows, "scan max_rows")?);
    }
    scan_options.set_preload_scan_cursor_block(options.preload_scan_cursor_block);
    scan_options = scan_options.with_stop_at_block_boundary(options.stop_at_block_boundary);
    Ok(scan_options)
}

pub(crate) fn checked_usize(value: u64, name: &str) -> BridgeResult<usize> {
    usize::try_from(value).map_err(|_| input_error(&format!("{name} exceeds the supported size")))
}

pub(crate) fn checked_u32(value: usize, name: &str) -> BridgeResult<u32> {
    u32::try_from(value).map_err(|_| input_error(&format!("{name} exceeds u32::MAX")))
}

pub(crate) fn checked_u64(value: usize, name: &str) -> BridgeResult<u64> {
    u64::try_from(value).map_err(|_| input_error(&format!("{name} exceeds u64::MAX")))
}

pub(crate) fn checked_nonzero_usize(value: u64, name: &str) -> BridgeResult<usize> {
    if value == 0 {
        return Err(input_error(&format!("{name} must be greater than zero")));
    }
    checked_usize(value, name)
}

fn decode_columns(columns: &[u64]) -> BridgeResult<Option<Vec<usize>>> {
    if columns.is_empty() {
        return Ok(None);
    }
    columns
        .iter()
        .map(|&column| checked_usize(column, "column index"))
        .collect::<BridgeResult<Vec<_>>>()
        .map(Some)
}

fn optional_string(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}
