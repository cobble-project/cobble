use std::{mem, slice};

use cobble_binding::structured::StructuredColumnValue;

use crate::structured_bridge::ffi;

use super::conversion::{format_error, input_error};
use super::encoding::{
    CsrbColumns, CsrbRow, STATUS_BUFFER_TOO_SMALL, STATUS_OK, encode_into, encoded_len,
};
use super::{
    BridgeResult, NativeStructuredDb, NativeStructuredReadOptions, NativeStructuredSingleDb,
};

#[repr(C)]
struct KeyDescriptor {
    bucket: u16,
    reserved: u16,
    data: *const u8,
    length: usize,
}

pub(crate) struct NativeStructuredMultiGetResult {
    rows: Vec<Option<Vec<Option<StructuredColumnValue>>>>,
}

/// Borrows all key payloads for one synchronous structured multi-get crossing.
///
/// # Safety
///
/// The descriptor address and every non-empty key pointer must refer to readable caller memory
/// that remains alive until this function returns.
pub(crate) unsafe fn borrowed_keys<'a>(
    descriptor_address: usize,
    count: u64,
) -> BridgeResult<Vec<(u16, &'a [u8])>> {
    let count = usize::try_from(count).map_err(|_| input_error("key count exceeds usize"))?;
    if count == 0 {
        return Ok(Vec::new());
    }
    if descriptor_address == 0 {
        return Err(input_error("key descriptors must not be null"));
    }
    if !descriptor_address.is_multiple_of(mem::align_of::<KeyDescriptor>()) {
        return Err(input_error("key descriptors are not aligned"));
    }
    let bytes = count
        .checked_mul(mem::size_of::<KeyDescriptor>())
        .ok_or_else(|| input_error("key descriptor size overflows"))?;
    if bytes > isize::MAX as usize {
        return Err(input_error("key descriptor array exceeds isize::MAX"));
    }
    descriptor_address
        .checked_add(bytes)
        .ok_or_else(|| input_error("key descriptor address overflows"))?;
    // SAFETY: caller contract plus the array checks above.
    let descriptors =
        unsafe { slice::from_raw_parts(descriptor_address as *const KeyDescriptor, count) };
    descriptors
        .iter()
        .map(|descriptor| {
            if descriptor.reserved != 0 {
                return Err(input_error("key descriptor reserved field must be zero"));
            }
            if descriptor.length == 0 {
                return Ok((descriptor.bucket, &[][..]));
            }
            if descriptor.data.is_null() {
                return Err(input_error("non-empty key data must not be null"));
            }
            if descriptor.length > isize::MAX as usize {
                return Err(input_error("key length exceeds isize::MAX"));
            }
            (descriptor.data as usize)
                .checked_add(descriptor.length)
                .ok_or_else(|| input_error("key address overflows"))?;
            // SAFETY: caller keeps each readable key span alive for this synchronous call.
            Ok((descriptor.bucket, unsafe {
                slice::from_raw_parts(descriptor.data, descriptor.length)
            }))
        })
        .collect()
}

pub(crate) fn native_structured_db_multi_get(
    db: &NativeStructuredDb,
    descriptor_address: usize,
    count: u64,
    options: &NativeStructuredReadOptions,
) -> BridgeResult<Box<NativeStructuredMultiGetResult>> {
    // SAFETY: private C++ wrapper owns and validates the exact descriptor layout.
    let keys = unsafe { borrowed_keys(descriptor_address, count)? };
    db.db
        .multi_get_with_options(&keys, &options.options)
        .map(|rows| Box::new(NativeStructuredMultiGetResult { rows }))
        .map_err(format_error)
}

pub(crate) fn native_structured_single_db_multi_get(
    db: &NativeStructuredSingleDb,
    descriptor_address: usize,
    count: u64,
    options: &NativeStructuredReadOptions,
) -> BridgeResult<Box<NativeStructuredMultiGetResult>> {
    // SAFETY: private C++ wrapper owns and validates the exact descriptor layout.
    let keys = unsafe { borrowed_keys(descriptor_address, count)? };
    db.db
        .multi_get_with_options(&keys, &options.options)
        .map(|rows| Box::new(NativeStructuredMultiGetResult { rows }))
        .map_err(format_error)
}

pub(crate) fn native_structured_db_multi_get_into(
    db: &NativeStructuredDb,
    descriptor_address: usize,
    count: u64,
    options: &NativeStructuredReadOptions,
    output: &mut [u8],
) -> BridgeResult<ffi::NativeBufferResult> {
    // SAFETY: private C++ wrapper owns and validates the exact descriptor layout.
    let keys = unsafe { borrowed_keys(descriptor_address, count)? };
    let rows = db
        .db
        .multi_get_with_options(&keys, &options.options)
        .map_err(format_error)?;
    encode_multi_get(&keys, &rows, output)
}

pub(crate) fn native_structured_single_db_multi_get_into(
    db: &NativeStructuredSingleDb,
    descriptor_address: usize,
    count: u64,
    options: &NativeStructuredReadOptions,
    output: &mut [u8],
) -> BridgeResult<ffi::NativeBufferResult> {
    // SAFETY: private C++ wrapper owns and validates the exact descriptor layout.
    let keys = unsafe { borrowed_keys(descriptor_address, count)? };
    let rows = db
        .db
        .multi_get_with_options(&keys, &options.options)
        .map_err(format_error)?;
    encode_multi_get(&keys, &rows, output)
}

fn encode_multi_get(
    keys: &[(u16, &[u8])],
    rows: &[Option<Vec<Option<StructuredColumnValue>>>],
    output: &mut [u8],
) -> BridgeResult<ffi::NativeBufferResult> {
    if keys.len() != rows.len() {
        return Err(input_error("structured multi-get result count mismatch"));
    }
    let encoded_rows = keys
        .iter()
        .zip(rows)
        .map(|((bucket, key), columns)| CsrbRow {
            bucket: *bucket,
            key,
            columns: columns
                .as_deref()
                .map(CsrbColumns::Structured)
                .unwrap_or(CsrbColumns::Missing),
        })
        .collect::<Vec<_>>();
    let required = encoded_len(&encoded_rows)?;
    if output.len() < required {
        return Ok(buffer_result(
            STATUS_BUFFER_TOO_SMALL,
            0,
            required,
            rows.len(),
        ));
    }
    let written = encode_into(&encoded_rows, false, false, output)?;
    Ok(buffer_result(STATUS_OK, written, written, rows.len()))
}

fn row(
    result: &NativeStructuredMultiGetResult,
    index: usize,
) -> BridgeResult<&Option<Vec<Option<StructuredColumnValue>>>> {
    result
        .rows
        .get(index)
        .ok_or_else(|| input_error("multi-get row index is out of bounds"))
}

fn column(
    result: &NativeStructuredMultiGetResult,
    row_index: usize,
    column_index: usize,
) -> BridgeResult<&StructuredColumnValue> {
    row(result, row_index)?
        .as_ref()
        .and_then(|columns| columns.get(column_index))
        .and_then(Option::as_ref)
        .ok_or_else(|| input_error("multi-get column is absent"))
}

pub(crate) fn native_structured_multi_get_row_count(
    result: &NativeStructuredMultiGetResult,
) -> usize {
    result.rows.len()
}

pub(crate) fn native_structured_multi_get_found(
    result: &NativeStructuredMultiGetResult,
    row_index: usize,
) -> bool {
    row(result, row_index).ok().is_some_and(Option::is_some)
}

pub(crate) fn native_structured_multi_get_column_count(
    result: &NativeStructuredMultiGetResult,
    row_index: usize,
) -> BridgeResult<usize> {
    Ok(row(result, row_index)?.as_ref().map_or(0, Vec::len))
}

pub(crate) fn native_structured_multi_get_has_column(
    result: &NativeStructuredMultiGetResult,
    row_index: usize,
    column_index: usize,
) -> bool {
    column(result, row_index, column_index).is_ok()
}

pub(crate) fn native_structured_multi_get_kind(
    result: &NativeStructuredMultiGetResult,
    row_index: usize,
    column_index: usize,
) -> BridgeResult<u8> {
    Ok(match column(result, row_index, column_index)? {
        StructuredColumnValue::Bytes(_) => 0,
        StructuredColumnValue::List(_) => 1,
    })
}

pub(crate) fn native_structured_multi_get_bytes(
    result: &NativeStructuredMultiGetResult,
    row_index: usize,
    column_index: usize,
) -> BridgeResult<&[u8]> {
    match column(result, row_index, column_index)? {
        StructuredColumnValue::Bytes(value) => Ok(value),
        StructuredColumnValue::List(_) => Err(input_error("multi-get column is not BYTES")),
    }
}

pub(crate) fn native_structured_multi_get_list_size(
    result: &NativeStructuredMultiGetResult,
    row_index: usize,
    column_index: usize,
) -> BridgeResult<usize> {
    match column(result, row_index, column_index)? {
        StructuredColumnValue::List(elements) => Ok(elements.len()),
        StructuredColumnValue::Bytes(_) => Err(input_error("multi-get column is not LIST")),
    }
}

pub(crate) fn native_structured_multi_get_list_element(
    result: &NativeStructuredMultiGetResult,
    row_index: usize,
    column_index: usize,
    element_index: usize,
) -> BridgeResult<&[u8]> {
    match column(result, row_index, column_index)? {
        StructuredColumnValue::List(elements) => elements
            .get(element_index)
            .map(AsRef::as_ref)
            .ok_or_else(|| input_error("LIST element index is out of bounds")),
        StructuredColumnValue::Bytes(_) => Err(input_error("multi-get column is not LIST")),
    }
}

pub(crate) fn buffer_result(
    status: u8,
    bytes_written: usize,
    bytes_required: usize,
    row_count: usize,
) -> ffi::NativeBufferResult {
    ffi::NativeBufferResult {
        status,
        bytes_written: bytes_written as u64,
        bytes_required: bytes_required as u64,
        row_count: row_count as u64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_validation_rejects_null_misaligned_and_reserved() {
        // SAFETY: all cases are rejected before an invalid pointer is dereferenced.
        unsafe {
            assert!(
                borrowed_keys(0, 1)
                    .unwrap_err()
                    .contains("must not be null")
            );
            assert!(borrowed_keys(1, 1).unwrap_err().contains("not aligned"));
        }
        let descriptor = KeyDescriptor {
            bucket: 0,
            reserved: 1,
            data: std::ptr::null(),
            length: 0,
        };
        // SAFETY: descriptor points to a live, correctly aligned local value.
        let error = unsafe { borrowed_keys((&descriptor as *const KeyDescriptor) as usize, 1) }
            .unwrap_err();
        assert!(error.contains("reserved"));
    }

    #[test]
    fn descriptor_validation_accepts_empty_and_binary_keys() {
        let bytes = [0, 0xff, 1];
        let descriptors = [
            KeyDescriptor {
                bucket: 2,
                reserved: 0,
                data: std::ptr::null(),
                length: 0,
            },
            KeyDescriptor {
                bucket: 3,
                reserved: 0,
                data: bytes.as_ptr(),
                length: bytes.len(),
            },
        ];
        // SAFETY: descriptors and key payload remain live for the call.
        let keys = unsafe { borrowed_keys(descriptors.as_ptr() as usize, 2) }.unwrap();
        assert_eq!(keys, vec![(2, &[][..]), (3, &bytes[..])]);
    }
}
