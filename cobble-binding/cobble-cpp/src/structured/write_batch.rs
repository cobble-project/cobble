use std::{mem, slice, str};

use cobble_binding::structured::{StructuredWriteBatch, StructuredWriteOptions};

use super::conversion::{format_error, input_error};
use super::{BridgeResult, NativeStructuredDb, NativeStructuredSingleDb};

const OP_PUT: u8 = 0;
const OP_MERGE: u8 = 1;
const OP_DELETE: u8 = 2;
const KIND_BYTES: u8 = 0;
const KIND_LIST: u8 = 1;
const FLAG_HAS_TTL: u16 = 1;
const FLAG_HAS_FAMILY: u16 = 2;
const FLAG_AWAIT_DURABLE: u16 = 4;
const KNOWN_FLAGS: u16 = FLAG_HAS_TTL | FLAG_HAS_FAMILY | FLAG_AWAIT_DURABLE;

#[repr(C)]
struct BytesDescriptor {
    data: usize,
    length: usize,
}

#[repr(C)]
struct WriteOperationDescriptor {
    operation: u8,
    kind: u8,
    bucket: u16,
    column: u16,
    flags: u16,
    ttl_seconds: u32,
    reserved: u32,
    key_data: usize,
    key_length: usize,
    value_data: usize,
    value_length: usize,
    elements_data: usize,
    element_count: usize,
    family_data: usize,
    family_length: usize,
}

struct BorrowedOperation<'a> {
    operation: u8,
    kind: u8,
    bucket: u16,
    column: u16,
    key: &'a [u8],
    value: &'a [u8],
    elements: Vec<&'a [u8]>,
    options: StructuredWriteOptions,
}

pub(crate) fn native_structured_db_write(
    db: &NativeStructuredDb,
    descriptor_address: usize,
    count: u64,
) -> BridgeResult<()> {
    // SAFETY: the private C++ wrapper retains every operation and nested payload for this call.
    let operations = unsafe { borrowed_operations(descriptor_address, count)? };
    let mut batch = db.db.new_write_batch();
    apply_operations(&mut batch, operations)?;
    db.db.write_batch(batch).map_err(format_error)
}

pub(crate) fn native_structured_single_db_write(
    db: &NativeStructuredSingleDb,
    descriptor_address: usize,
    count: u64,
) -> BridgeResult<()> {
    // SAFETY: the private C++ wrapper retains every operation and nested payload for this call.
    let operations = unsafe { borrowed_operations(descriptor_address, count)? };
    let mut batch = db.db.new_write_batch();
    apply_operations(&mut batch, operations)?;
    db.db.write_batch(batch).map_err(format_error)
}

fn apply_operations(
    batch: &mut StructuredWriteBatch,
    operations: Vec<BorrowedOperation<'_>>,
) -> BridgeResult<()> {
    for operation in operations {
        match (operation.operation, operation.kind) {
            (OP_PUT, KIND_BYTES) => {
                cobble_binding::structured::ffi::write_batch_put_borrowed_bytes_with_options(
                    batch,
                    operation.bucket,
                    operation.key,
                    operation.column,
                    operation.value,
                    &operation.options,
                )
                .map_err(format_error)?;
            }
            (OP_PUT, KIND_LIST) => {
                cobble_binding::structured::ffi::write_batch_put_borrowed_list_with_options(
                    batch,
                    operation.bucket,
                    operation.key,
                    operation.column,
                    &operation.elements,
                    &operation.options,
                )
                .map_err(format_error)?;
            }
            (OP_MERGE, KIND_BYTES) => {
                cobble_binding::structured::ffi::write_batch_merge_borrowed_bytes_with_options(
                    batch,
                    operation.bucket,
                    operation.key,
                    operation.column,
                    operation.value,
                    &operation.options,
                )
                .map_err(format_error)?;
            }
            (OP_MERGE, KIND_LIST) => {
                cobble_binding::structured::ffi::write_batch_merge_borrowed_list_with_options(
                    batch,
                    operation.bucket,
                    operation.key,
                    operation.column,
                    &operation.elements,
                    &operation.options,
                )
                .map_err(format_error)?;
            }
            (OP_DELETE, _) => batch.delete_with_options(
                operation.bucket,
                operation.key,
                operation.column,
                &operation.options,
            ),
            _ => return Err(input_error("unknown structured write operation")),
        }
    }
    Ok(())
}

/// Decodes exact-layout operation descriptors while borrowing all payload bytes.
///
/// # Safety
///
/// The descriptor array, nested element arrays, and every non-empty byte span must remain readable
/// until this function and the synchronous write call return.
unsafe fn borrowed_operations<'a>(
    descriptor_address: usize,
    count: u64,
) -> BridgeResult<Vec<BorrowedOperation<'a>>> {
    let descriptors = unsafe {
        borrowed_descriptor_slice::<WriteOperationDescriptor>(
            descriptor_address,
            count,
            "write operation",
        )?
    };
    descriptors
        .iter()
        .map(|descriptor| {
            if descriptor.reserved != 0 {
                return Err(input_error("write operation reserved field must be zero"));
            }
            if descriptor.flags & !KNOWN_FLAGS != 0 {
                return Err(input_error("write operation contains unknown option flags"));
            }
            if descriptor.flags & FLAG_HAS_TTL == 0 && descriptor.ttl_seconds != 0 {
                return Err(input_error("TTL seconds require the TTL option flag"));
            }
            if descriptor.operation > OP_DELETE {
                return Err(input_error("unknown structured write operation"));
            }
            if descriptor.kind > KIND_LIST {
                return Err(input_error("unknown structured write value kind"));
            }
            let key =
                unsafe { borrowed_bytes(descriptor.key_data, descriptor.key_length, "write key")? };
            let value = unsafe {
                borrowed_bytes(
                    descriptor.value_data,
                    descriptor.value_length,
                    "write value",
                )?
            };
            let element_descriptors = unsafe {
                borrowed_descriptor_slice::<BytesDescriptor>(
                    descriptor.elements_data,
                    u64::try_from(descriptor.element_count)
                        .map_err(|_| input_error("LIST element count exceeds u64"))?,
                    "LIST element",
                )?
            };
            let elements = element_descriptors
                .iter()
                .map(|element| unsafe {
                    borrowed_bytes(element.data, element.length, "LIST element")
                })
                .collect::<BridgeResult<Vec<_>>>()?;
            let family_bytes = unsafe {
                borrowed_bytes(
                    descriptor.family_data,
                    descriptor.family_length,
                    "column family",
                )?
            };

            if descriptor.operation == OP_DELETE && (!value.is_empty() || !elements.is_empty()) {
                return Err(input_error("delete operation must not contain a value"));
            }
            if descriptor.operation == OP_DELETE && descriptor.kind != KIND_BYTES {
                return Err(input_error("delete operation kind must be BYTES"));
            }
            if descriptor.kind == KIND_BYTES && !elements.is_empty() {
                return Err(input_error(
                    "BYTES operation must not contain LIST elements",
                ));
            }
            if descriptor.kind == KIND_LIST && !value.is_empty() {
                return Err(input_error("LIST operation must not contain a BYTES value"));
            }
            if descriptor.flags & FLAG_HAS_FAMILY == 0 && !family_bytes.is_empty() {
                return Err(input_error(
                    "column family payload requires the family flag",
                ));
            }
            if descriptor.flags & FLAG_HAS_FAMILY != 0 && family_bytes.is_empty() {
                return Err(input_error("column family must not be empty"));
            }

            let mut raw = if descriptor.flags & FLAG_HAS_TTL != 0 {
                cobble_binding::WriteOptions::with_ttl(descriptor.ttl_seconds)
            } else {
                cobble_binding::WriteOptions::default()
            }
            .with_await_durable(descriptor.flags & FLAG_AWAIT_DURABLE != 0);
            if descriptor.flags & FLAG_HAS_FAMILY != 0 {
                raw.column_family = Some(
                    str::from_utf8(family_bytes)
                        .map_err(|_| input_error("column family must be UTF-8"))?
                        .to_owned(),
                );
            }
            Ok(BorrowedOperation {
                operation: descriptor.operation,
                kind: descriptor.kind,
                bucket: descriptor.bucket,
                column: descriptor.column,
                key,
                value,
                elements,
                options: raw.into(),
            })
        })
        .collect()
}

unsafe fn borrowed_descriptor_slice<'a, T>(
    address: usize,
    count: u64,
    name: &str,
) -> BridgeResult<&'a [T]> {
    let count =
        usize::try_from(count).map_err(|_| input_error(format!("{name} count exceeds usize")))?;
    if count == 0 {
        return Ok(&[]);
    }
    if address == 0 {
        return Err(input_error(format!("{name} descriptors must not be null")));
    }
    if !address.is_multiple_of(mem::align_of::<T>()) {
        return Err(input_error(format!("{name} descriptors are not aligned")));
    }
    let byte_len = count
        .checked_mul(mem::size_of::<T>())
        .ok_or_else(|| input_error(format!("{name} descriptor size overflows")))?;
    if byte_len > isize::MAX as usize {
        return Err(input_error(format!(
            "{name} descriptor array exceeds isize::MAX"
        )));
    }
    address
        .checked_add(byte_len)
        .ok_or_else(|| input_error(format!("{name} descriptor address overflows")))?;
    // SAFETY: caller contract plus null/alignment/size/address checks above.
    Ok(unsafe { slice::from_raw_parts(address as *const T, count) })
}

unsafe fn borrowed_bytes<'a>(address: usize, length: usize, name: &str) -> BridgeResult<&'a [u8]> {
    if length == 0 {
        return Ok(&[]);
    }
    if address == 0 {
        return Err(input_error(format!("non-empty {name} must not be null")));
    }
    if length > isize::MAX as usize {
        return Err(input_error(format!("{name} length exceeds isize::MAX")));
    }
    address
        .checked_add(length)
        .ok_or_else(|| input_error(format!("{name} address overflows")))?;
    // SAFETY: caller retains this readable span for the synchronous crossing.
    Ok(unsafe { slice::from_raw_parts(address as *const u8, length) })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_descriptor() -> WriteOperationDescriptor {
        WriteOperationDescriptor {
            operation: OP_PUT,
            kind: KIND_BYTES,
            bucket: 0,
            column: 0,
            flags: 0,
            ttl_seconds: 0,
            reserved: 0,
            key_data: 0,
            key_length: 0,
            value_data: 0,
            value_length: 0,
            elements_data: 0,
            element_count: 0,
            family_data: 0,
            family_length: 0,
        }
    }

    #[test]
    fn write_descriptor_rejects_unknown_flags_and_null_nested_array() {
        let mut descriptor = valid_descriptor();
        descriptor.flags = 8;
        // SAFETY: descriptor points to a live local value.
        let error = unsafe { borrowed_operations((&descriptor as *const _) as usize, 1) }
            .err()
            .unwrap();
        assert!(error.contains("unknown option flags"));

        descriptor.flags = 0;
        descriptor.kind = KIND_LIST;
        descriptor.element_count = 1;
        // SAFETY: descriptor points to a live local value; null nested array is rejected.
        let error = unsafe { borrowed_operations((&descriptor as *const _) as usize, 1) }
            .err()
            .unwrap();
        assert!(error.contains("must not be null"));
    }

    #[test]
    fn write_descriptor_rejects_unflagged_ttl_and_list_delete() {
        let mut descriptor = valid_descriptor();
        descriptor.ttl_seconds = 1;
        // SAFETY: descriptor points to a live local value.
        let error = unsafe { borrowed_operations((&descriptor as *const _) as usize, 1) }
            .err()
            .unwrap();
        assert!(error.contains("TTL option flag"));

        descriptor.ttl_seconds = 0;
        descriptor.operation = OP_DELETE;
        descriptor.kind = KIND_LIST;
        // SAFETY: descriptor points to a live local value.
        let error = unsafe { borrowed_operations((&descriptor as *const _) as usize, 1) }
            .err()
            .unwrap();
        assert!(error.contains("kind must be BYTES"));
    }
}
