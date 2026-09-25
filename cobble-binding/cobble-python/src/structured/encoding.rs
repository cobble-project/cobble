use crate::error::input_error;
use crate::types::{PyBufferResult, PyBufferStatus};
use cobble_binding::structured::StructuredColumnValue;

const MAGIC: [u8; 4] = *b"CSRB";
const VERSION: u16 = 1;
const HEADER_SIZE: usize = 24;
const ROW_HEADER_SIZE: usize = 16;
const COLUMN_HEADER_SIZE: usize = 16;
const FLAG_END: u32 = 1;
const FLAG_BLOCK_BOUNDARY: u32 = 2;
const ROW_FLAG_FOUND: u16 = 1;
const COLUMN_NULL: u8 = 0;
const COLUMN_BYTES: u8 = 1;
const COLUMN_LIST: u8 = 2;

pub(super) struct CsrbRow<'a> {
    pub(super) bucket: u16,
    pub(super) key: &'a [u8],
    pub(super) columns: CsrbColumns<'a>,
}

#[derive(Clone, Copy)]
pub(super) enum CsrbColumns<'a> {
    Missing,
    Structured(&'a [Option<StructuredColumnValue>]),
    PriorityQueue(&'a bytes::Bytes),
}

enum CsrbColumn<'a> {
    Null,
    Bytes(&'a bytes::Bytes),
    List(&'a [bytes::Bytes]),
}

pub(super) struct PreparedCsrb<'rows, 'data> {
    rows: &'rows [CsrbRow<'data>],
    required: usize,
    end: bool,
    stopped_at_block_boundary: bool,
}

impl PreparedCsrb<'_, '_> {
    pub(super) fn required_len(&self) -> usize {
        self.required
    }

    /// Encode metadata that was fully validated by [`prepare`].
    ///
    /// This stage is infallible so a stateful scan or priority-queue operation
    /// can commit its pending result after sizing succeeds without repeating
    /// the complete CSRB length walk.
    pub(super) fn encode_into(self, output: &mut [u8]) -> usize {
        assert!(output.len() >= self.required, "preflighted CSRB buffer");
        let output = &mut output[..self.required];
        output[..4].copy_from_slice(&MAGIC);
        write_u16(&mut output[4..6], VERSION);
        write_u16(&mut output[6..8], HEADER_SIZE as u16);
        let flags = (u32::from(self.end) * FLAG_END)
            | (u32::from(self.stopped_at_block_boundary) * FLAG_BLOCK_BOUNDARY);
        write_u32(&mut output[8..12], flags);
        write_u32(
            &mut output[12..16],
            u32::try_from(self.rows.len()).expect("preflighted row count"),
        );
        write_u64(&mut output[16..24], self.required as u64);

        let mut offset = HEADER_SIZE;
        for row in self.rows {
            write_u16(&mut output[offset..offset + 2], row.bucket);
            write_u16(
                &mut output[offset + 2..offset + 4],
                if row.columns.found() {
                    ROW_FLAG_FOUND
                } else {
                    0
                },
            );
            write_u32(
                &mut output[offset + 4..offset + 8],
                u32::try_from(row.key.len()).expect("preflighted row key length"),
            );
            write_u32(
                &mut output[offset + 8..offset + 12],
                u32::try_from(row.columns.len()).expect("preflighted row column count"),
            );
            write_u32(&mut output[offset + 12..offset + 16], 0);
            offset += ROW_HEADER_SIZE;
            output[offset..offset + row.key.len()].copy_from_slice(row.key);
            offset += row.key.len();
            row.columns.for_each(|column| {
                let (tag, element_count, payload_size) =
                    column_metadata(&column).expect("preflighted column metadata");
                output[offset] = tag;
                output[offset + 1] = 0;
                write_u16(&mut output[offset + 2..offset + 4], 0);
                write_u32(&mut output[offset + 4..offset + 8], element_count);
                write_u64(&mut output[offset + 8..offset + 16], payload_size);
                offset += COLUMN_HEADER_SIZE;
                match column {
                    CsrbColumn::Null => {}
                    CsrbColumn::Bytes(value) => {
                        output[offset..offset + value.len()].copy_from_slice(value);
                        offset += value.len();
                    }
                    CsrbColumn::List(elements) => {
                        for element in elements {
                            write_u64(&mut output[offset..offset + 8], element.len() as u64);
                            offset += 8;
                            output[offset..offset + element.len()].copy_from_slice(element);
                            offset += element.len();
                        }
                    }
                }
            });
        }
        debug_assert_eq!(offset, self.required);
        self.required
    }
}

impl CsrbColumns<'_> {
    fn found(self) -> bool {
        !matches!(self, Self::Missing)
    }

    fn len(self) -> usize {
        match self {
            Self::Missing => 0,
            Self::Structured(columns) => columns.len(),
            Self::PriorityQueue(_) => 1,
        }
    }

    fn for_each(self, mut callback: impl FnMut(CsrbColumn<'_>)) {
        match self {
            Self::Missing => {}
            Self::Structured(columns) => columns.iter().for_each(|column| {
                callback(match column {
                    None => CsrbColumn::Null,
                    Some(StructuredColumnValue::Bytes(value)) => CsrbColumn::Bytes(value),
                    Some(StructuredColumnValue::List(elements)) => CsrbColumn::List(elements),
                });
            }),
            Self::PriorityQueue(value) => callback(CsrbColumn::Bytes(value)),
        }
    }
}

fn checked_add(left: usize, right: usize, name: &str) -> pyo3::PyResult<usize> {
    left.checked_add(right)
        .ok_or_else(|| input_error(format!("{name} overflows CSRB size")))
}

fn checked_u32(value: usize, name: &str) -> pyo3::PyResult<u32> {
    u32::try_from(value).map_err(|_| input_error(format!("{name} exceeds u32")))
}

fn column_metadata(column: &CsrbColumn<'_>) -> pyo3::PyResult<(u8, u32, u64)> {
    match column {
        CsrbColumn::Null => Ok((COLUMN_NULL, 0, 0)),
        CsrbColumn::Bytes(value) => Ok((
            COLUMN_BYTES,
            1,
            u64::try_from(value.len()).map_err(|_| input_error("BYTES length exceeds u64"))?,
        )),
        CsrbColumn::List(elements) => {
            let mut payload = 0usize;
            for element in *elements {
                payload = checked_add(payload, 8, "LIST element length")?;
                payload = checked_add(payload, element.len(), "LIST element payload")?;
            }
            Ok((
                COLUMN_LIST,
                checked_u32(elements.len(), "LIST element count")?,
                u64::try_from(payload).map_err(|_| input_error("LIST payload exceeds u64"))?,
            ))
        }
    }
}

fn encoded_len(rows: &[CsrbRow<'_>]) -> pyo3::PyResult<usize> {
    checked_u32(rows.len(), "row count")?;
    let mut total = HEADER_SIZE;
    for row in rows {
        checked_u32(row.key.len(), "row key length")?;
        checked_u32(row.columns.len(), "row column count")?;
        total = checked_add(total, ROW_HEADER_SIZE, "row header")?;
        total = checked_add(total, row.key.len(), "row key")?;
        match row.columns {
            CsrbColumns::Missing => {}
            CsrbColumns::Structured(columns) => {
                for column in columns {
                    total = checked_add(total, COLUMN_HEADER_SIZE, "column header")?;
                    match column {
                        None => {}
                        Some(StructuredColumnValue::Bytes(value)) => {
                            total = checked_add(total, value.len(), "BYTES payload")?;
                        }
                        Some(StructuredColumnValue::List(elements)) => {
                            checked_u32(elements.len(), "LIST element count")?;
                            for element in elements {
                                total = checked_add(total, 8, "LIST element length")?;
                                total = checked_add(total, element.len(), "LIST element payload")?;
                            }
                        }
                    }
                }
            }
            CsrbColumns::PriorityQueue(value) => {
                total = checked_add(total, COLUMN_HEADER_SIZE, "column header")?;
                total = checked_add(total, value.len(), "BYTES payload")?;
            }
        }
    }
    u64::try_from(total).map_err(|_| input_error("CSRB encoded size exceeds u64"))?;
    Ok(total)
}

pub(super) fn prepare<'rows, 'data>(
    rows: &'rows [CsrbRow<'data>],
    end: bool,
    stopped_at_block_boundary: bool,
) -> pyo3::PyResult<PreparedCsrb<'rows, 'data>> {
    Ok(PreparedCsrb {
        rows,
        required: encoded_len(rows)?,
        end,
        stopped_at_block_boundary,
    })
}

pub(super) fn buffer_result(
    status: PyBufferStatus,
    bytes_written: usize,
    bytes_required: usize,
    row_count: usize,
) -> PyBufferResult {
    PyBufferResult::new(status, bytes_written, bytes_required, row_count)
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn csrb_v1_distinguishes_missing_bytes_and_lists() {
        let columns = vec![
            None,
            Some(StructuredColumnValue::Bytes(Bytes::from_static(b"xy"))),
            Some(StructuredColumnValue::List(vec![
                Bytes::new(),
                Bytes::from_static(b"z"),
            ])),
        ];
        let rows = [CsrbRow {
            bucket: 7,
            key: b"k",
            columns: CsrbColumns::Structured(&columns),
        }];
        let prepared = prepare(&rows, true, false).unwrap();
        let mut output = vec![0; prepared.required_len()];
        let written = prepared.encode_into(&mut output);
        assert_eq!(written, 108);
        assert_eq!(&output[..4], b"CSRB");
        assert_eq!(u32::from_le_bytes(output[8..12].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(output[12..16].try_into().unwrap()), 1);
        assert_eq!(u16::from_le_bytes(output[24..26].try_into().unwrap()), 7);
        assert_eq!(output[41], COLUMN_NULL);
        assert_eq!(output[57], COLUMN_BYTES);
        assert_eq!(output[75], COLUMN_LIST);
    }
}
