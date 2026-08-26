use bytes::Bytes;
use cobble_binding::structured::StructuredColumnValue;

use super::conversion::input_error;
use super::{BridgeResult, NativeStructuredRow};

fn column(row: &NativeStructuredRow, column: usize) -> BridgeResult<&StructuredColumnValue> {
    row.columns
        .as_ref()
        .ok_or_else(|| input_error("row was not found"))?
        .get(column)
        .ok_or_else(|| input_error("column index is out of range"))?
        .as_ref()
        .ok_or_else(|| input_error("column value is absent"))
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
