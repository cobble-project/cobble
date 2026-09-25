use crate::error::input_error;
use crate::scan::PyOwnedBatch;

const BATCH_MAGIC: [u8; 4] = *b"CBRB";
const BATCH_VERSION: u16 = 1;
const BATCH_HEADER_SIZE: usize = 24;
const BATCH_FLAG_END: u32 = 1;
const BATCH_FLAG_BOUNDARY: u32 = 2;
const NONE_COLUMN_LENGTH: u64 = u64::MAX;

pub(crate) struct PreparedBatch<'a> {
    batch: &'a PyOwnedBatch,
    required: usize,
}

impl PreparedBatch<'_> {
    pub(crate) fn required_len(&self) -> usize {
        self.required
    }

    /// Encode a batch whose sizes were validated by [`prepare_batch`].
    ///
    /// Keeping this stage infallible lets stateful cursors commit their pending
    /// batch only after buffer sizing has succeeded, without walking every row
    /// a second time just to repeat validation.
    pub(crate) fn encode_into(self, output: &mut [u8]) -> usize {
        assert!(output.len() >= self.required, "preflighted CBRB buffer");
        let output = &mut output[..self.required];
        let batch = self.batch;
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
            u32::try_from(batch.rows.len()).expect("preflighted scan row count"),
        );
        write_u64(&mut output[16..24], self.required as u64);

        let mut offset = BATCH_HEADER_SIZE;
        for row in &batch.rows {
            write_u16(&mut output[offset..offset + 2], row.bucket);
            write_u16(&mut output[offset + 2..offset + 4], 0);
            write_u32(
                &mut output[offset + 4..offset + 8],
                u32::try_from(row.key.len()).expect("preflighted scan key length"),
            );
            write_u32(
                &mut output[offset + 8..offset + 12],
                u32::try_from(row.columns.len()).expect("preflighted scan column count"),
            );
            offset += 12;
            output[offset..offset + row.key.len()].copy_from_slice(&row.key);
            offset += row.key.len();
            for column in &row.columns {
                match column {
                    Some(column) => {
                        write_u64(&mut output[offset..offset + 8], column.len() as u64);
                        offset += 8;
                        output[offset..offset + column.len()].copy_from_slice(column);
                        offset += column.len();
                    }
                    None => {
                        write_u64(&mut output[offset..offset + 8], NONE_COLUMN_LENGTH);
                        offset += 8;
                    }
                }
            }
        }
        debug_assert_eq!(offset, self.required);
        self.required
    }
}

pub(crate) fn prepare_batch(batch: &PyOwnedBatch) -> pyo3::PyResult<PreparedBatch<'_>> {
    u32::try_from(batch.rows.len()).map_err(|_| input_error("scan row count exceeds u32"))?;
    let mut total = BATCH_HEADER_SIZE;
    for row in &batch.rows {
        u32::try_from(row.key.len()).map_err(|_| input_error("scan key length exceeds u32"))?;
        u32::try_from(row.columns.len())
            .map_err(|_| input_error("scan column count exceeds u32"))?;
        total = checked_add(total, 12, "row header")?;
        total = checked_add(total, row.key.len(), "row key")?;
        for column in &row.columns {
            total = checked_add(total, 8, "column length")?;
            if let Some(column) = column {
                total = checked_add(total, column.len(), "column payload")?;
            }
        }
    }
    u64::try_from(total).map_err(|_| input_error("encoded batch length exceeds u64"))?;
    Ok(PreparedBatch {
        batch,
        required: total,
    })
}

fn checked_add(left: usize, right: usize, name: &str) -> pyo3::PyResult<usize> {
    left.checked_add(right)
        .ok_or_else(|| input_error(format!("{name} overflows encoded batch size")))
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
