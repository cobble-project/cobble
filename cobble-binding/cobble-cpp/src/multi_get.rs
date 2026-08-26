use bytes::Bytes;
use std::{mem, slice};

use crate::{
    BridgeResult,
    database::NativeDatabase,
    error::{format_cobble_error, input_error},
    ffi,
    options::{checked_u64, checked_usize, to_read_options},
};

#[repr(C)]
struct KeyDescriptor {
    bucket: u16,
    _padding: u16,
    data: *const u8,
    len: usize,
}

pub(crate) struct NativeMultiGetResult {
    rows: Vec<Option<Vec<Option<Bytes>>>>,
}

/// Borrows the caller's key buffers for one synchronous `multi_get` call.
///
/// # Safety
///
/// `descriptor_address` must point to `count` naturally aligned `KeyDescriptor`
/// values created by the private C++ wrapper. Every non-empty descriptor must
/// point to a readable key buffer that remains alive until this function
/// returns. No borrowed address escapes this call.
unsafe fn borrowed_keys<'a>(
    descriptor_address: usize,
    count: u64,
) -> BridgeResult<Vec<(u16, &'a [u8])>> {
    let count = checked_usize(count, "multi-get key count")?;
    if count == 0 {
        return Ok(Vec::new());
    }
    if descriptor_address == 0 {
        return Err(input_error("multi-get descriptors must not be null"));
    }
    if !descriptor_address.is_multiple_of(mem::align_of::<KeyDescriptor>()) {
        return Err(input_error("multi-get descriptors are not aligned"));
    }
    let byte_len = count
        .checked_mul(mem::size_of::<KeyDescriptor>())
        .ok_or_else(|| input_error("multi-get descriptor size overflows"))?;
    if byte_len > isize::MAX as usize {
        return Err(input_error("multi-get descriptor array is too large"));
    }
    descriptor_address
        .checked_add(byte_len)
        .ok_or_else(|| input_error("multi-get descriptor address overflows"))?;

    // SAFETY: the private C++ wrapper satisfies the function contract above;
    // alignment, null, byte-size, and address overflow were checked first.
    let descriptors =
        unsafe { slice::from_raw_parts(descriptor_address as *const KeyDescriptor, count) };
    descriptors
        .iter()
        .map(|entry| {
            if entry.len == 0 {
                return Ok((entry.bucket, &[][..]));
            }
            if entry.data.is_null() {
                return Err(input_error("multi-get key data must not be null"));
            }
            (entry.data as usize)
                .checked_add(entry.len)
                .ok_or_else(|| input_error("multi-get key address overflows"))?;
            // `isize::MAX` is also the maximum length accepted by
            // `slice::from_raw_parts` even when `usize` is wider.
            if entry.len > isize::MAX as usize {
                return Err(input_error("multi-get key length is too large"));
            }
            // SAFETY: the private wrapper keeps each span alive and readable
            // for the duration of this synchronous bridge call.
            Ok((entry.bucket, unsafe {
                slice::from_raw_parts(entry.data, entry.len)
            }))
        })
        .collect()
}

pub(crate) fn native_database_multi_get(
    db: &NativeDatabase,
    descriptor_address: usize,
    count: u64,
    options: &ffi::NativeReadOptions,
) -> BridgeResult<Box<NativeMultiGetResult>> {
    // SAFETY: this function is called only by the private C++ wrapper, and the
    // borrowed descriptors are consumed before returning across the bridge.
    let keys = unsafe { borrowed_keys(descriptor_address, count)? };
    Ok(Box::new(NativeMultiGetResult {
        rows: db
            .db
            .multi_get_with_options(&keys, &to_read_options(options)?)
            .map_err(format_cobble_error)?,
    }))
}

pub(crate) fn native_sharded_database_multi_get(
    db: &crate::sharded_db::NativeShardedDatabase,
    descriptor_address: usize,
    count: u64,
    options: &ffi::NativeReadOptions,
) -> BridgeResult<Box<NativeMultiGetResult>> {
    // SAFETY: this function is called only by the private C++ wrapper, and the
    // borrowed descriptors are consumed before returning across the bridge.
    let keys = unsafe { borrowed_keys(descriptor_address, count)? };
    Ok(Box::new(NativeMultiGetResult {
        rows: db
            .db
            .multi_get_with_options(&keys, &to_read_options(options)?)
            .map_err(format_cobble_error)?,
    }))
}

fn row(rows: &NativeMultiGetResult, index: u64) -> BridgeResult<&Option<Vec<Option<Bytes>>>> {
    rows.rows
        .get(checked_usize(index, "multi-get row index")?)
        .ok_or_else(|| input_error("multi-get row index is out of bounds"))
}

pub(crate) fn native_multi_get_row_count(rows: &NativeMultiGetResult) -> u64 {
    u64::try_from(rows.rows.len()).unwrap_or(u64::MAX)
}

pub(crate) fn native_multi_get_found(rows: &NativeMultiGetResult, index: u64) -> bool {
    row(rows, index).ok().is_some_and(Option::is_some)
}

pub(crate) fn native_multi_get_column_count(
    rows: &NativeMultiGetResult,
    index: u64,
) -> BridgeResult<u64> {
    checked_u64(
        row(rows, index)?.as_ref().map_or(0, Vec::len),
        "multi-get column count",
    )
}

pub(crate) fn native_multi_get_has_column(
    rows: &NativeMultiGetResult,
    index: u64,
    column: u64,
) -> bool {
    checked_usize(column, "column index")
        .ok()
        .and_then(|column| row(rows, index).ok()?.as_ref()?.get(column))
        .is_some_and(Option::is_some)
}

pub(crate) fn native_multi_get_column(
    rows: &NativeMultiGetResult,
    index: u64,
    column: u64,
) -> BridgeResult<&[u8]> {
    let column = checked_usize(column, "column index")?;
    row(rows, index)?
        .as_ref()
        .and_then(|row| row.get(column))
        .and_then(Option::as_ref)
        .map(Bytes::as_ref)
        .ok_or_else(|| input_error("requested multi-get column is absent"))
}
