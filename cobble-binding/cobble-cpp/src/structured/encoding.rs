use cobble_binding::structured::StructuredColumnValue;

use super::BridgeResult;
use super::conversion::input_error;

pub(crate) const STATUS_OK: u8 = 0;
pub(crate) const STATUS_NOT_FOUND: u8 = 1;
pub(crate) const STATUS_END: u8 = 2;
pub(crate) const STATUS_BUFFER_TOO_SMALL: u8 = 3;
pub(crate) const STATUS_BLOCK_BOUNDARY: u8 = 4;

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

pub(crate) struct CsrbRow<'a> {
    pub(crate) bucket: u16,
    pub(crate) key: &'a [u8],
    pub(crate) columns: Option<&'a [Option<StructuredColumnValue>]>,
}

pub(crate) fn encoded_len(rows: &[CsrbRow<'_>]) -> BridgeResult<usize> {
    let mut total = HEADER_SIZE;
    checked_u32(rows.len(), "row count")?;
    for row in rows {
        checked_u32(row.key.len(), "row key length")?;
        let columns = row.columns.unwrap_or_default();
        checked_u32(columns.len(), "row column count")?;
        total = checked_add(total, ROW_HEADER_SIZE, "row header")?;
        total = checked_add(total, row.key.len(), "row key")?;
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
    u64::try_from(total).map_err(|_| input_error("CSRB encoded size exceeds u64"))?;
    Ok(total)
}

pub(crate) fn encode_into(
    rows: &[CsrbRow<'_>],
    end: bool,
    stopped_at_block_boundary: bool,
    output: &mut [u8],
) -> BridgeResult<usize> {
    let required = encoded_len(rows)?;
    if output.len() < required {
        return Err(input_error("CSRB output buffer is too small"));
    }
    let output = &mut output[..required];
    output[..4].copy_from_slice(&MAGIC);
    write_u16(&mut output[4..6], VERSION);
    write_u16(&mut output[6..8], HEADER_SIZE as u16);
    let mut flags = 0;
    if end {
        flags |= FLAG_END;
    }
    if stopped_at_block_boundary {
        flags |= FLAG_BLOCK_BOUNDARY;
    }
    write_u32(&mut output[8..12], flags);
    write_u32(&mut output[12..16], checked_u32(rows.len(), "row count")?);
    write_u64(&mut output[16..24], required as u64);

    let mut offset = HEADER_SIZE;
    for row in rows {
        let columns = row.columns.unwrap_or_default();
        write_u16(&mut output[offset..offset + 2], row.bucket);
        write_u16(
            &mut output[offset + 2..offset + 4],
            if row.columns.is_some() {
                ROW_FLAG_FOUND
            } else {
                0
            },
        );
        write_u32(
            &mut output[offset + 4..offset + 8],
            checked_u32(row.key.len(), "row key length")?,
        );
        write_u32(
            &mut output[offset + 8..offset + 12],
            checked_u32(columns.len(), "row column count")?,
        );
        write_u32(&mut output[offset + 12..offset + 16], 0);
        offset += ROW_HEADER_SIZE;
        output[offset..offset + row.key.len()].copy_from_slice(row.key);
        offset += row.key.len();
        for column in columns {
            let (tag, element_count, payload_size) = column_metadata(column)?;
            output[offset] = tag;
            output[offset + 1] = 0;
            write_u16(&mut output[offset + 2..offset + 4], 0);
            write_u32(&mut output[offset + 4..offset + 8], element_count);
            write_u64(&mut output[offset + 8..offset + 16], payload_size);
            offset += COLUMN_HEADER_SIZE;
            match column {
                None => {}
                Some(StructuredColumnValue::Bytes(value)) => {
                    output[offset..offset + value.len()].copy_from_slice(value);
                    offset += value.len();
                }
                Some(StructuredColumnValue::List(elements)) => {
                    for element in elements {
                        write_u64(
                            &mut output[offset..offset + 8],
                            u64::try_from(element.len())
                                .map_err(|_| input_error("LIST element length exceeds u64"))?,
                        );
                        offset += 8;
                        output[offset..offset + element.len()].copy_from_slice(element);
                        offset += element.len();
                    }
                }
            }
        }
    }
    debug_assert_eq!(offset, required);
    Ok(required)
}

fn column_metadata(column: &Option<StructuredColumnValue>) -> BridgeResult<(u8, u32, u64)> {
    match column {
        None => Ok((COLUMN_NULL, 0, 0)),
        Some(StructuredColumnValue::Bytes(value)) => Ok((
            COLUMN_BYTES,
            1,
            u64::try_from(value.len()).map_err(|_| input_error("BYTES length exceeds u64"))?,
        )),
        Some(StructuredColumnValue::List(elements)) => {
            let mut payload = 0usize;
            for element in elements {
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

fn checked_add(left: usize, right: usize, name: &str) -> BridgeResult<usize> {
    left.checked_add(right)
        .ok_or_else(|| input_error(format!("{name} overflows CSRB size")))
}

fn checked_u32(value: usize, name: &str) -> BridgeResult<u32> {
    u32::try_from(value).map_err(|_| input_error(format!("{name} exceeds u32")))
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
    fn csrb_v1_encodes_null_bytes_and_list_exactly() {
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
            columns: Some(&columns),
        }];
        let mut output = vec![0; encoded_len(&rows).unwrap()];
        let written = encode_into(&rows, true, false, &mut output).unwrap();
        assert_eq!(written, 108);
        assert_eq!(&output[0..4], b"CSRB");
        assert_eq!(u16::from_le_bytes(output[4..6].try_into().unwrap()), 1);
        assert_eq!(u16::from_le_bytes(output[6..8].try_into().unwrap()), 24);
        assert_eq!(u32::from_le_bytes(output[8..12].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(output[12..16].try_into().unwrap()), 1);
        assert_eq!(u64::from_le_bytes(output[16..24].try_into().unwrap()), 108);
        assert_eq!(u16::from_le_bytes(output[24..26].try_into().unwrap()), 7);
        assert_eq!(u16::from_le_bytes(output[26..28].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(output[28..32].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(output[32..36].try_into().unwrap()), 3);
        assert_eq!(output[40], b'k');
        assert_eq!(output[41], COLUMN_NULL);
        assert_eq!(output[57], COLUMN_BYTES);
        assert_eq!(u64::from_le_bytes(output[65..73].try_into().unwrap()), 2);
        assert_eq!(&output[73..75], b"xy");
        assert_eq!(output[75], COLUMN_LIST);
        assert_eq!(u32::from_le_bytes(output[79..83].try_into().unwrap()), 2);
        assert_eq!(u64::from_le_bytes(output[83..91].try_into().unwrap()), 17);
        assert_eq!(u64::from_le_bytes(output[91..99].try_into().unwrap()), 0);
        assert_eq!(output[99], 1); // first byte of the second element length
        assert_eq!(output[107], b'z');
    }

    #[test]
    fn too_small_output_is_immutable() {
        let columns = vec![Some(StructuredColumnValue::Bytes(Bytes::from_static(
            b"value",
        )))];
        let rows = [CsrbRow {
            bucket: 0,
            key: b"key",
            columns: Some(&columns),
        }];
        let mut output = vec![0xa5; encoded_len(&rows).unwrap() - 1];
        let before = output.clone();
        assert!(encode_into(&rows, false, false, &mut output).is_err());
        assert_eq!(output, before);
    }

    #[test]
    fn missing_and_empty_row_are_distinct() {
        let empty = Vec::new();
        let rows = [
            CsrbRow {
                bucket: 1,
                key: b"a",
                columns: None,
            },
            CsrbRow {
                bucket: 1,
                key: b"b",
                columns: Some(&empty),
            },
        ];
        let mut output = vec![0; encoded_len(&rows).unwrap()];
        encode_into(&rows, false, true, &mut output).unwrap();
        assert_eq!(u32::from_le_bytes(output[8..12].try_into().unwrap()), 2);
        assert_eq!(u16::from_le_bytes(output[26..28].try_into().unwrap()), 0);
        let second = 24 + 16 + 1;
        assert_eq!(
            u16::from_le_bytes(output[second + 2..second + 4].try_into().unwrap()),
            1
        );
    }
}
