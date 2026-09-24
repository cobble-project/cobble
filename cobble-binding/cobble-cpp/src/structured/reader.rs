use cobble_binding::ReaderConfig;
use cobble_binding::structured::StructuredReader;

use crate::structured_bridge::ffi;

use super::conversion::{
    format_error, input_error, native_schema, parse_config_file, parse_config_json,
};
use super::multi_get::{NativeStructuredMultiGetResult, borrowed_keys, encode_multi_get};
use super::row::encode_get;
use super::scan::{NativeStructuredScanCursor, native_structured_scan_cursor_from_iterator};
use super::{
    BridgeResult, NativeStructuredReadOptions, NativeStructuredRow, NativeStructuredScanOptions,
};

pub(crate) struct NativeStructuredReader {
    reader: StructuredReader,
}

fn open(
    config: cobble_binding::Config,
    snapshot_id: Option<u64>,
) -> BridgeResult<Box<NativeStructuredReader>> {
    opendal::install_default();
    let config = ReaderConfig::from_config(&config);
    let reader = match snapshot_id {
        Some(id) => StructuredReader::open(config, id),
        None => StructuredReader::open_current(config),
    }
    .map_err(format_error)?;
    Ok(Box::new(NativeStructuredReader { reader }))
}

pub(crate) fn native_structured_reader_open_current(
    config_json: &str,
) -> BridgeResult<Box<NativeStructuredReader>> {
    open(parse_config_json(config_json)?, None)
}

pub(crate) fn native_structured_reader_open_current_file(
    config_path: &str,
) -> BridgeResult<Box<NativeStructuredReader>> {
    open(parse_config_file(config_path)?, None)
}

pub(crate) fn native_structured_reader_open(
    config_json: &str,
    snapshot_id: u64,
) -> BridgeResult<Box<NativeStructuredReader>> {
    open(parse_config_json(config_json)?, Some(snapshot_id))
}

pub(crate) fn native_structured_reader_open_file(
    config_path: &str,
    snapshot_id: u64,
) -> BridgeResult<Box<NativeStructuredReader>> {
    open(parse_config_file(config_path)?, Some(snapshot_id))
}

pub(crate) fn native_structured_reader_refresh(
    reader: &mut NativeStructuredReader,
) -> BridgeResult<()> {
    if reader.reader.configured_snapshot_id().is_some() {
        return Err("CB_INVALID_STATE: pinned Reader cannot refresh; open current mode to follow the global snapshot pointer".to_string());
    }
    reader.reader.refresh().map_err(format_error)
}

pub(crate) fn native_structured_reader_get(
    reader: &mut NativeStructuredReader,
    bucket: u16,
    key: &[u8],
    options: &NativeStructuredReadOptions,
) -> BridgeResult<Box<NativeStructuredRow>> {
    let columns = reader
        .reader
        .get_with_options(bucket, key, &options.options)
        .map_err(format_error)?;
    Ok(Box::new(NativeStructuredRow { columns }))
}

pub(crate) fn native_structured_reader_get_into(
    reader: &mut NativeStructuredReader,
    bucket: u16,
    key: &[u8],
    options: &NativeStructuredReadOptions,
    output: &mut [u8],
) -> BridgeResult<ffi::NativeBufferResult> {
    let columns = reader
        .reader
        .get_with_options(bucket, key, &options.options)
        .map_err(format_error)?;
    encode_get(bucket, key, columns.as_deref(), output)
}

pub(crate) fn native_structured_reader_multi_get(
    reader: &mut NativeStructuredReader,
    descriptor_address: usize,
    count: u64,
    options: &NativeStructuredReadOptions,
) -> BridgeResult<Box<NativeStructuredMultiGetResult>> {
    // SAFETY: the C++ wrapper keeps key descriptors alive for this synchronous call.
    let keys = unsafe { borrowed_keys(descriptor_address, count)? };
    let rows = reader
        .reader
        .multi_get_with_options(&keys, &options.options)
        .map_err(format_error)?;
    Ok(Box::new(NativeStructuredMultiGetResult { rows }))
}

pub(crate) fn native_structured_reader_multi_get_into(
    reader: &mut NativeStructuredReader,
    descriptor_address: usize,
    count: u64,
    options: &NativeStructuredReadOptions,
    output: &mut [u8],
) -> BridgeResult<ffi::NativeBufferResult> {
    // SAFETY: the C++ wrapper keeps key descriptors alive for this synchronous call.
    let keys = unsafe { borrowed_keys(descriptor_address, count)? };
    let rows = reader
        .reader
        .multi_get_with_options(&keys, &options.options)
        .map_err(format_error)?;
    encode_multi_get(&keys, &rows, output)
}

pub(crate) fn native_structured_reader_scan(
    reader: &mut NativeStructuredReader,
    bucket: u16,
    start: &[u8],
    end: &[u8],
    options: &NativeStructuredScanOptions,
) -> BridgeResult<Box<NativeStructuredScanCursor>> {
    if start > end {
        return Err(input_error("reader scan start must not exceed end"));
    }
    let iterator = reader
        .reader
        .scan_with_options(bucket, start..end, &options.options)
        .map_err(format_error)?;
    Ok(native_structured_scan_cursor_from_iterator(
        bucket, iterator,
    ))
}

pub(crate) fn native_structured_reader_current_schema(
    reader: &NativeStructuredReader,
) -> ffi::NativeStructuredSchema {
    native_schema(reader.reader.current_schema())
}

pub(crate) fn native_structured_reader_mode(reader: &NativeStructuredReader) -> u8 {
    u8::from(reader.reader.configured_snapshot_id().is_some())
}

pub(crate) fn native_structured_reader_has_configured_snapshot(
    reader: &NativeStructuredReader,
) -> bool {
    reader.reader.configured_snapshot_id().is_some()
}

pub(crate) fn native_structured_reader_configured_snapshot(reader: &NativeStructuredReader) -> u64 {
    reader.reader.configured_snapshot_id().unwrap_or(0)
}

pub(crate) fn native_structured_reader_current_global_snapshot(
    reader: &NativeStructuredReader,
) -> ffi::NativeSnapshot {
    super::lifecycle::native_snapshot(reader.reader.current_global_snapshot().clone())
}

pub(crate) fn native_structured_reader_list_global_snapshots(
    reader: &NativeStructuredReader,
) -> BridgeResult<Vec<ffi::NativeSnapshot>> {
    reader
        .reader
        .list_global_snapshot_manifests()
        .map(|values| {
            values
                .into_iter()
                .map(super::lifecycle::native_snapshot)
                .collect()
        })
        .map_err(format_error)
}
