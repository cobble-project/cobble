use cobble_binding::{Config, Reader, ReaderConfig};

use crate::{
    BridgeResult,
    database::NativeRow,
    encoding::{STATUS_BUFFER_TOO_SMALL, STATUS_NOT_FOUND, STATUS_OK, buffer_result},
    error::{format_cobble_error, input_error},
    ffi,
    multi_get::{NativeMultiGetResult, borrowed_keys},
    options::{checked_u64, to_read_options, to_scan_options, to_single_column_read_options},
    scan::{NativeScanCursor, native_scan_cursor_from_db_iterator},
    snapshot::snapshot,
};

pub(crate) struct NativeReader {
    reader: Reader,
}

fn open(config: Config, snapshot_id: Option<u64>) -> BridgeResult<Box<NativeReader>> {
    opendal::install_default();
    let config = ReaderConfig::from_config(&config);
    let reader = match snapshot_id {
        Some(snapshot_id) => Reader::open(config, snapshot_id),
        None => Reader::open_current(config),
    }
    .map_err(format_cobble_error)?;
    Ok(Box::new(NativeReader { reader }))
}

pub(crate) fn native_reader_open_current(config_json: &str) -> BridgeResult<Box<NativeReader>> {
    open(
        Config::from_json_str(config_json).map_err(format_cobble_error)?,
        None,
    )
}

pub(crate) fn native_reader_open_current_file(
    config_path: &str,
) -> BridgeResult<Box<NativeReader>> {
    open(
        Config::from_path(config_path).map_err(format_cobble_error)?,
        None,
    )
}

pub(crate) fn native_reader_open(
    config_json: &str,
    snapshot_id: u64,
) -> BridgeResult<Box<NativeReader>> {
    open(
        Config::from_json_str(config_json).map_err(format_cobble_error)?,
        Some(snapshot_id),
    )
}

pub(crate) fn native_reader_open_file(
    config_path: &str,
    snapshot_id: u64,
) -> BridgeResult<Box<NativeReader>> {
    open(
        Config::from_path(config_path).map_err(format_cobble_error)?,
        Some(snapshot_id),
    )
}

pub(crate) fn native_reader_refresh(reader: &mut NativeReader) -> BridgeResult<()> {
    if reader.reader.configured_snapshot_id().is_some() {
        return Err("CB_INVALID_STATE: pinned Reader cannot refresh; open current mode to follow the global snapshot pointer".to_string());
    }
    reader.reader.refresh().map_err(format_cobble_error)
}

pub(crate) fn native_reader_get(
    reader: &mut NativeReader,
    bucket: u16,
    key: &[u8],
    options: &ffi::NativeReadOptions,
) -> BridgeResult<Box<NativeRow>> {
    Ok(Box::new(NativeRow {
        columns: reader
            .reader
            .get_with_options(bucket, key, &to_read_options(options)?)
            .map_err(format_cobble_error)?,
    }))
}

pub(crate) fn native_reader_get_column_into(
    reader: &mut NativeReader,
    bucket: u16,
    key: &[u8],
    output: &mut [u8],
    options: &ffi::NativeReadOptions,
) -> BridgeResult<ffi::NativeBufferResult> {
    let options = to_single_column_read_options(options)?;
    let Some(columns) = reader
        .reader
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

pub(crate) fn native_reader_multi_get(
    reader: &mut NativeReader,
    descriptor_address: usize,
    count: u64,
    options: &ffi::NativeReadOptions,
) -> BridgeResult<Box<NativeMultiGetResult>> {
    // SAFETY: the private C++ wrapper keeps descriptor and key storage alive
    // for this synchronous bridge call.
    let keys = unsafe { borrowed_keys(descriptor_address, count)? };
    Ok(Box::new(NativeMultiGetResult {
        rows: reader
            .reader
            .multi_get_with_options(&keys, &to_read_options(options)?)
            .map_err(format_cobble_error)?,
    }))
}

pub(crate) fn native_reader_scan(
    reader: &mut NativeReader,
    bucket: u16,
    start: &[u8],
    end: &[u8],
    options: &ffi::NativeScanOptions,
) -> BridgeResult<Box<NativeScanCursor>> {
    if start > end {
        return Err(input_error("reader scan start must not exceed end"));
    }
    let iterator = reader
        .reader
        .scan_with_options(bucket, start..end, &to_scan_options(options)?)
        .map_err(format_cobble_error)?;
    Ok(native_scan_cursor_from_db_iterator(bucket, iterator, None))
}

pub(crate) fn native_reader_mode(reader: &NativeReader) -> u8 {
    u8::from(reader.reader.configured_snapshot_id().is_some())
}

pub(crate) fn native_reader_has_configured_snapshot(reader: &NativeReader) -> bool {
    reader.reader.configured_snapshot_id().is_some()
}

pub(crate) fn native_reader_configured_snapshot(reader: &NativeReader) -> u64 {
    reader.reader.configured_snapshot_id().unwrap_or(0)
}

pub(crate) fn native_reader_current_global_snapshot(reader: &NativeReader) -> ffi::NativeSnapshot {
    snapshot(reader.reader.current_global_snapshot().clone())
}

pub(crate) fn native_reader_list_global_snapshots(
    reader: &NativeReader,
) -> BridgeResult<Vec<ffi::NativeSnapshot>> {
    reader
        .reader
        .list_global_snapshot_manifests()
        .map(|values| values.into_iter().map(snapshot).collect())
        .map_err(format_cobble_error)
}
