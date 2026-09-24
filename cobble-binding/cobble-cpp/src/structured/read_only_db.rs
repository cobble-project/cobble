use cobble_binding::structured::StructuredReadOnlyDb;

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

pub(crate) struct NativeStructuredReadOnlyDb {
    db: StructuredReadOnlyDb,
}

fn open(
    config: cobble_binding::Config,
    snapshot_id: u64,
    db_id: &str,
) -> BridgeResult<Box<NativeStructuredReadOnlyDb>> {
    opendal::install_default();
    let db = StructuredReadOnlyDb::open(config, snapshot_id, db_id).map_err(format_error)?;
    Ok(Box::new(NativeStructuredReadOnlyDb { db }))
}

pub(crate) fn native_structured_read_only_db_open(
    config_json: &str,
    snapshot_id: u64,
    db_id: &str,
) -> BridgeResult<Box<NativeStructuredReadOnlyDb>> {
    open(parse_config_json(config_json)?, snapshot_id, db_id)
}

pub(crate) fn native_structured_read_only_db_open_file(
    config_path: &str,
    snapshot_id: u64,
    db_id: &str,
) -> BridgeResult<Box<NativeStructuredReadOnlyDb>> {
    open(parse_config_file(config_path)?, snapshot_id, db_id)
}

pub(crate) fn native_structured_read_only_db_id(db: &NativeStructuredReadOnlyDb) -> &str {
    db.db.id()
}

pub(crate) fn native_structured_read_only_db_current_schema(
    db: &NativeStructuredReadOnlyDb,
) -> ffi::NativeStructuredSchema {
    native_schema(db.db.current_schema())
}

pub(crate) fn native_structured_read_only_db_get(
    db: &NativeStructuredReadOnlyDb,
    bucket: u16,
    key: &[u8],
    options: &NativeStructuredReadOptions,
) -> BridgeResult<Box<NativeStructuredRow>> {
    let columns = db
        .db
        .get_with_options(bucket, key, &options.options)
        .map_err(format_error)?;
    Ok(Box::new(NativeStructuredRow { columns }))
}

pub(crate) fn native_structured_read_only_db_get_into(
    db: &NativeStructuredReadOnlyDb,
    bucket: u16,
    key: &[u8],
    options: &NativeStructuredReadOptions,
    output: &mut [u8],
) -> BridgeResult<ffi::NativeBufferResult> {
    let columns = db
        .db
        .get_with_options(bucket, key, &options.options)
        .map_err(format_error)?;
    encode_get(bucket, key, columns.as_deref(), output)
}

pub(crate) fn native_structured_read_only_db_multi_get(
    db: &NativeStructuredReadOnlyDb,
    descriptor_address: usize,
    count: u64,
    options: &NativeStructuredReadOptions,
) -> BridgeResult<Box<NativeStructuredMultiGetResult>> {
    // SAFETY: the C++ wrapper keeps key descriptors alive for this synchronous call.
    let keys = unsafe { borrowed_keys(descriptor_address, count)? };
    let rows = db
        .db
        .multi_get_with_options(&keys, &options.options)
        .map_err(format_error)?;
    Ok(Box::new(NativeStructuredMultiGetResult { rows }))
}

pub(crate) fn native_structured_read_only_db_multi_get_into(
    db: &NativeStructuredReadOnlyDb,
    descriptor_address: usize,
    count: u64,
    options: &NativeStructuredReadOptions,
    output: &mut [u8],
) -> BridgeResult<ffi::NativeBufferResult> {
    // SAFETY: the C++ wrapper keeps key descriptors alive for this synchronous call.
    let keys = unsafe { borrowed_keys(descriptor_address, count)? };
    let rows = db
        .db
        .multi_get_with_options(&keys, &options.options)
        .map_err(format_error)?;
    encode_multi_get(&keys, &rows, output)
}

pub(crate) fn native_structured_read_only_db_scan(
    db: &NativeStructuredReadOnlyDb,
    bucket: u16,
    start: &[u8],
    end: &[u8],
    options: &NativeStructuredScanOptions,
) -> BridgeResult<Box<NativeStructuredScanCursor>> {
    if start > end {
        return Err(input_error("read-only scan start must not exceed end"));
    }
    let iterator = db
        .db
        .scan_with_options(bucket, start..end, &options.options)
        .map_err(format_error)?;
    Ok(native_structured_scan_cursor_from_iterator(
        bucket, iterator,
    ))
}
