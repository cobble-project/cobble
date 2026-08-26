use bytes::Bytes;
use cobble_binding::structured::StructuredColumnValue;

use super::conversion::format_error;
use super::conversion::input_error;
use super::encoding::{
    CsrbColumns, CsrbRow, STATUS_BUFFER_TOO_SMALL, STATUS_NOT_FOUND, STATUS_OK, encode_into,
    encoded_len,
};
use super::multi_get::buffer_result;
use super::{
    BridgeResult, NativeStructuredDb, NativeStructuredReadOptions, NativeStructuredRow,
    NativeStructuredSingleDb,
};
use crate::structured_bridge::ffi;

fn column(row: &NativeStructuredRow, column: usize) -> BridgeResult<&StructuredColumnValue> {
    row.columns
        .as_ref()
        .ok_or_else(|| input_error("row was not found"))?
        .get(column)
        .ok_or_else(|| input_error("column index is out of range"))?
        .as_ref()
        .ok_or_else(|| input_error("column value is absent"))
}

pub(crate) fn native_structured_db_get_into(
    db: &NativeStructuredDb,
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

pub(crate) fn native_structured_single_db_get_into(
    db: &NativeStructuredSingleDb,
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

fn encode_get(
    bucket: u16,
    key: &[u8],
    columns: Option<&[Option<StructuredColumnValue>]>,
    output: &mut [u8],
) -> BridgeResult<ffi::NativeBufferResult> {
    let rows = [CsrbRow {
        bucket,
        key,
        columns: columns
            .map(CsrbColumns::Structured)
            .unwrap_or(CsrbColumns::Missing),
    }];
    let required = encoded_len(&rows)?;
    if output.len() < required {
        return Ok(buffer_result(STATUS_BUFFER_TOO_SMALL, 0, required, 1));
    }
    let written = encode_into(&rows, false, false, output)?;
    Ok(buffer_result(
        if columns.is_some() {
            STATUS_OK
        } else {
            STATUS_NOT_FOUND
        },
        written,
        written,
        1,
    ))
}
pub(crate) fn native_structured_row_found(row: &NativeStructuredRow) -> bool {
    row.columns.is_some()
}
pub(crate) fn native_structured_row_column_count(row: &NativeStructuredRow) -> usize {
    row.columns.as_ref().map_or(0, Vec::len)
}
pub(crate) fn native_structured_row_has_column(row: &NativeStructuredRow, column: usize) -> bool {
    row.columns
        .as_ref()
        .and_then(|columns| columns.get(column))
        .is_some_and(Option::is_some)
}
pub(crate) fn native_structured_row_kind(
    row: &NativeStructuredRow,
    index: usize,
) -> BridgeResult<u8> {
    Ok(match column(row, index)? {
        StructuredColumnValue::Bytes(_) => 0,
        StructuredColumnValue::List(_) => 1,
    })
}
pub(crate) fn native_structured_row_bytes(
    row: &NativeStructuredRow,
    index: usize,
) -> BridgeResult<&[u8]> {
    match column(row, index)? {
        StructuredColumnValue::Bytes(value) => Ok(value.as_ref()),
        StructuredColumnValue::List(_) => Err(input_error("column is not BYTES")),
    }
}
pub(crate) fn native_structured_row_list_size(
    row: &NativeStructuredRow,
    index: usize,
) -> BridgeResult<usize> {
    match column(row, index)? {
        StructuredColumnValue::List(value) => Ok(value.len()),
        StructuredColumnValue::Bytes(_) => Err(input_error("column is not LIST")),
    }
}
pub(crate) fn native_structured_row_list_element(
    row: &NativeStructuredRow,
    index: usize,
    element: usize,
) -> BridgeResult<&[u8]> {
    match column(row, index)? {
        StructuredColumnValue::List(value) => value
            .get(element)
            .map(Bytes::as_ref)
            .ok_or_else(|| input_error("list element index is out of range")),
        StructuredColumnValue::Bytes(_) => Err(input_error("column is not LIST")),
    }
}
