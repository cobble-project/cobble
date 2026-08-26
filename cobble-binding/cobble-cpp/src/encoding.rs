use crate::{
    BridgeResult,
    error::input_error,
    options::{checked_u32, checked_u64},
    scan::NativeBatch,
};

pub(crate) const STATUS_OK: u8 = 0;
pub(crate) const STATUS_NOT_FOUND: u8 = 1;
pub(crate) const STATUS_END: u8 = 2;
pub(crate) const STATUS_BUFFER_TOO_SMALL: u8 = 3;
pub(crate) const STATUS_BLOCK_BOUNDARY: u8 = 4;

const BATCH_MAGIC: [u8; 4] = *b"CBRB";
const BATCH_VERSION: u16 = 1;
const BATCH_HEADER_SIZE: usize = 24;
const BATCH_FLAG_END: u32 = 1;
const BATCH_FLAG_BOUNDARY: u32 = 2;
const NONE_COLUMN_LENGTH: u64 = u64::MAX;

pub(crate) fn buffer_result(
    status: u8,
    bytes_written: u64,
    bytes_required: u64,
    row_count: u64,
) -> crate::ffi::NativeBufferResult {
    crate::ffi::NativeBufferResult {
        status,
        bytes_written,
        bytes_required,
        row_count,
    }
}

pub(crate) fn batch_status(batch: &NativeBatch) -> u8 {
    if batch.rows.is_empty() {
        if batch.stopped_at_block_boundary {
            STATUS_BLOCK_BOUNDARY
        } else {
            STATUS_END
        }
    } else {
        STATUS_OK
    }
}

pub(crate) fn batch_encoded_len(batch: &NativeBatch) -> BridgeResult<usize> {
    let mut total = BATCH_HEADER_SIZE;
    checked_u32(batch.rows.len(), "batch row count")?;
    for row in &batch.rows {
        checked_u32(row.key.len(), "row key length")?;
        checked_u32(row.columns.len(), "row column count")?;
        total = checked_add(total, 12, "row header size")?;
        total = checked_add(total, row.key.len(), "row key length")?;
        for column in &row.columns {
            total = checked_add(total, 8, "column length field")?;
            if let Some(column) = column {
                total = checked_add(total, column.len(), "column payload length")?;
            }
        }
    }
    let _ = checked_u64(total, "batch encoded length")?;
    Ok(total)
}

pub(crate) fn encode_batch_into(batch: &NativeBatch, output: &mut [u8]) -> BridgeResult<()> {
    let total = batch_encoded_len(batch)?;
    if output.len() != total {
        return Err(input_error(
            "internal batch encoding buffer length mismatch",
        ));
    }
    output[..4].copy_from_slice(&BATCH_MAGIC);
    write_u16(&mut output[4..6], BATCH_VERSION);
    write_u16(&mut output[6..8], BATCH_HEADER_SIZE as u16);
    let mut flags = 0;
    if batch.end {
        flags |= BATCH_FLAG_END;
    }
    if batch.stopped_at_block_boundary {
        flags |= BATCH_FLAG_BOUNDARY;
    }
    write_u32(&mut output[8..12], flags);
    write_u32(
        &mut output[12..16],
        checked_u32(batch.rows.len(), "batch row count")?,
    );
    write_u64(
        &mut output[16..24],
        checked_u64(total, "batch encoded length")?,
    );
    let mut offset = BATCH_HEADER_SIZE;
    for row in &batch.rows {
        write_u16(&mut output[offset..offset + 2], row.bucket);
        write_u16(&mut output[offset + 2..offset + 4], 0);
        write_u32(
            &mut output[offset + 4..offset + 8],
            checked_u32(row.key.len(), "row key length")?,
        );
        write_u32(
            &mut output[offset + 8..offset + 12],
            checked_u32(row.columns.len(), "row column count")?,
        );
        offset += 12;
        output[offset..offset + row.key.len()].copy_from_slice(row.key.as_ref());
        offset += row.key.len();
        for column in &row.columns {
            match column {
                Some(column) => {
                    write_u64(
                        &mut output[offset..offset + 8],
                        checked_u64(column.len(), "column payload length")?,
                    );
                    offset += 8;
                    output[offset..offset + column.len()].copy_from_slice(column.as_ref());
                    offset += column.len();
                }
                None => {
                    write_u64(&mut output[offset..offset + 8], NONE_COLUMN_LENGTH);
                    offset += 8;
                }
            }
        }
    }
    debug_assert_eq!(offset, total);
    Ok(())
}

fn checked_add(left: usize, right: usize, name: &str) -> BridgeResult<usize> {
    left.checked_add(right)
        .ok_or_else(|| input_error(&format!("{name} overflows the batch encoding size")))
}

fn write_u16(output: &mut [u8], value: u16) {
    output.copy_from_slice(&value.to_le_bytes());
}
fn write_u32(output: &mut [u8], value: u32) {
    output.copy_from_slice(&value.to_le_bytes());
}
fn write_u64(output: &mut [u8], value: u64) {
    output.copy_from_slice(&value.to_le_bytes());
}
