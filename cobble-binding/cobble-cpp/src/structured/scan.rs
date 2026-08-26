use std::sync::Arc;

use bytes::Bytes;
use cobble_binding::structured::{
    StructuredColumnValue, StructuredDb, StructuredDbIterator, StructuredScanSplitScanner,
    StructuredSingleDb,
};

use crate::structured_bridge::ffi;

use super::conversion::{format_error, input_error};
use super::encoding::{
    CsrbRow, STATUS_BLOCK_BOUNDARY, STATUS_BUFFER_TOO_SMALL, STATUS_END, STATUS_OK, encode_into,
    encoded_len,
};
use super::multi_get::buffer_result;
use super::{
    BridgeResult, NativeStructuredDb, NativeStructuredScanOptions, NativeStructuredSingleDb,
};

pub(crate) struct NativeStructuredBatchRow {
    bucket: u16,
    key: Bytes,
    columns: Vec<Option<StructuredColumnValue>>,
}

pub(crate) struct NativeStructuredBatch {
    rows: Vec<NativeStructuredBatchRow>,
    end: bool,
    stopped_at_block_boundary: bool,
}

enum NativeStructuredOwner {
    Db { _owner: Arc<StructuredDb> },
    SingleDb { _owner: Arc<StructuredSingleDb> },
}

#[allow(clippy::large_enum_variant)]
enum NativeStructuredIterator {
    Db {
        bucket: u16,
        iterator: StructuredDbIterator,
    },
    Split(StructuredScanSplitScanner),
}

pub(crate) struct NativeStructuredScanCursor {
    iterator: NativeStructuredIterator,
    pending_row: Option<NativeStructuredBatchRow>,
    pending_batch: Option<NativeStructuredBatch>,
    // Drop iterator/access guard before the final database owner.
    _owner: Option<NativeStructuredOwner>,
}

impl NativeStructuredIterator {
    fn next_row(&mut self) -> BridgeResult<Option<NativeStructuredBatchRow>> {
        match self {
            Self::Db { bucket, iterator } => {
                iterator
                    .next()
                    .transpose()
                    .map_err(format_error)
                    .map(|row| {
                        row.map(|(key, columns)| NativeStructuredBatchRow {
                            bucket: *bucket,
                            key,
                            columns,
                        })
                    })
            }
            Self::Split(scanner) => scanner.next().transpose().map_err(format_error).map(|row| {
                row.map(|(bucket, key, columns)| NativeStructuredBatchRow {
                    bucket,
                    key,
                    columns,
                })
            }),
        }
    }

    fn stopped_at_block_boundary(&self) -> bool {
        match self {
            Self::Db { iterator, .. } => {
                cobble_binding::structured::ffi::iterator_stopped_at_block_boundary(iterator)
            }
            Self::Split(_) => false,
        }
    }

    fn clear_stop_at_block_boundary(&mut self) -> BridgeResult<()> {
        match self {
            Self::Db { iterator, .. } => {
                cobble_binding::structured::ffi::iterator_clear_stop_at_block_boundary(iterator);
                Ok(())
            }
            Self::Split(_) => Err(input_error(
                "block-boundary resume is not supported for structured split scanners",
            )),
        }
    }
}

pub(crate) fn native_structured_db_scan(
    db: &NativeStructuredDb,
    bucket: u16,
    start: &[u8],
    has_start: bool,
    end: &[u8],
    has_end: bool,
    options: &NativeStructuredScanOptions,
) -> BridgeResult<Box<NativeStructuredScanCursor>> {
    let iterator = db
        .db
        .scan_with_options_bounds(
            bucket,
            has_start.then_some(start),
            has_end.then_some(end),
            &options.options,
        )
        .map_err(format_error)?;
    Ok(Box::new(NativeStructuredScanCursor {
        iterator: NativeStructuredIterator::Db { bucket, iterator },
        pending_row: None,
        pending_batch: None,
        _owner: Some(NativeStructuredOwner::Db {
            _owner: Arc::clone(&db.db),
        }),
    }))
}

pub(crate) fn native_structured_single_db_scan(
    db: &NativeStructuredSingleDb,
    bucket: u16,
    start: &[u8],
    has_start: bool,
    end: &[u8],
    has_end: bool,
    options: &NativeStructuredScanOptions,
) -> BridgeResult<Box<NativeStructuredScanCursor>> {
    let iterator = cobble_binding::structured::ffi::single_db_scan_with_options_bounds(
        &db.db,
        bucket,
        has_start.then_some(start),
        has_end.then_some(end),
        &options.options,
    )
    .map_err(format_error)?;
    Ok(Box::new(NativeStructuredScanCursor {
        iterator: NativeStructuredIterator::Db { bucket, iterator },
        pending_row: None,
        pending_batch: None,
        _owner: Some(NativeStructuredOwner::SingleDb {
            _owner: Arc::clone(&db.db),
        }),
    }))
}

pub(crate) fn native_structured_scan_cursor_from_split(
    scanner: StructuredScanSplitScanner,
) -> Box<NativeStructuredScanCursor> {
    Box::new(NativeStructuredScanCursor {
        iterator: NativeStructuredIterator::Split(scanner),
        pending_row: None,
        pending_batch: None,
        _owner: None,
    })
}

pub(crate) fn native_structured_scan_cursor_next_owned(
    cursor: &mut NativeStructuredScanCursor,
    max_rows: u64,
) -> BridgeResult<Box<NativeStructuredBatch>> {
    Ok(Box::new(take_or_read_batch(cursor, max_rows)?))
}

pub(crate) fn native_structured_scan_cursor_next_batch_into(
    cursor: &mut NativeStructuredScanCursor,
    max_rows: u64,
    output: &mut [u8],
) -> BridgeResult<ffi::NativeBufferResult> {
    if cursor.pending_batch.is_none() {
        cursor.pending_batch = Some(read_batch(cursor, max_rows)?);
    }
    let batch = cursor
        .pending_batch
        .as_ref()
        .ok_or_else(|| input_error("pending structured scan batch is unavailable"))?;
    let rows = csrb_rows(&batch.rows);
    let required = encoded_len(&rows)?;
    if output.len() < required {
        return Ok(buffer_result(
            STATUS_BUFFER_TOO_SMALL,
            0,
            required,
            batch.rows.len(),
        ));
    }
    let written = encode_into(&rows, batch.end, batch.stopped_at_block_boundary, output)?;
    let row_count = batch.rows.len();
    let status = if row_count == 0 && batch.end {
        STATUS_END
    } else if row_count == 0 && batch.stopped_at_block_boundary {
        STATUS_BLOCK_BOUNDARY
    } else {
        STATUS_OK
    };
    cursor.pending_batch = None;
    Ok(buffer_result(status, written, written, row_count))
}

pub(crate) fn native_structured_scan_cursor_resume_after_block_boundary(
    cursor: &mut NativeStructuredScanCursor,
) -> BridgeResult<()> {
    if cursor
        .pending_batch
        .as_ref()
        .is_some_and(|batch| batch.rows.is_empty() && batch.stopped_at_block_boundary)
    {
        cursor.pending_batch = None;
    }
    cursor.iterator.clear_stop_at_block_boundary()
}

fn take_or_read_batch(
    cursor: &mut NativeStructuredScanCursor,
    max_rows: u64,
) -> BridgeResult<NativeStructuredBatch> {
    if let Some(batch) = cursor.pending_batch.take() {
        return Ok(batch);
    }
    read_batch(cursor, max_rows)
}

fn read_batch(
    cursor: &mut NativeStructuredScanCursor,
    max_rows: u64,
) -> BridgeResult<NativeStructuredBatch> {
    let max_rows = usize::try_from(max_rows)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| input_error("structured scan max_rows must be positive and fit usize"))?;
    let mut rows = Vec::new();
    if let Some(row) = cursor.pending_row.take() {
        rows.push(row);
    }
    while rows.len() < max_rows {
        let Some(row) = cursor.iterator.next_row()? else {
            let stopped = cursor.iterator.stopped_at_block_boundary();
            return Ok(NativeStructuredBatch {
                rows,
                end: !stopped,
                stopped_at_block_boundary: stopped,
            });
        };
        rows.push(row);
    }
    if let Some(row) = cursor.iterator.next_row()? {
        cursor.pending_row = Some(row);
    }
    let stopped = cursor.iterator.stopped_at_block_boundary();
    Ok(NativeStructuredBatch {
        rows,
        end: cursor.pending_row.is_none() && !stopped,
        stopped_at_block_boundary: stopped,
    })
}

fn csrb_rows(rows: &[NativeStructuredBatchRow]) -> Vec<CsrbRow<'_>> {
    rows.iter()
        .map(|row| CsrbRow {
            bucket: row.bucket,
            key: &row.key,
            columns: Some(&row.columns),
        })
        .collect()
}

fn batch_row(batch: &NativeStructuredBatch, row: usize) -> BridgeResult<&NativeStructuredBatchRow> {
    batch
        .rows
        .get(row)
        .ok_or_else(|| input_error("structured batch row index is out of bounds"))
}

fn batch_column(
    batch: &NativeStructuredBatch,
    row: usize,
    column: usize,
) -> BridgeResult<&StructuredColumnValue> {
    batch_row(batch, row)?
        .columns
        .get(column)
        .and_then(Option::as_ref)
        .ok_or_else(|| input_error("structured batch column is absent"))
}

pub(crate) fn native_structured_batch_row_count(batch: &NativeStructuredBatch) -> usize {
    batch.rows.len()
}

pub(crate) fn native_structured_batch_end(batch: &NativeStructuredBatch) -> bool {
    batch.end
}

pub(crate) fn native_structured_batch_stopped_at_block_boundary(
    batch: &NativeStructuredBatch,
) -> bool {
    batch.stopped_at_block_boundary
}

pub(crate) fn native_structured_batch_bucket(
    batch: &NativeStructuredBatch,
    row: usize,
) -> BridgeResult<u16> {
    Ok(batch_row(batch, row)?.bucket)
}

pub(crate) fn native_structured_batch_key(
    batch: &NativeStructuredBatch,
    row: usize,
) -> BridgeResult<&[u8]> {
    Ok(&batch_row(batch, row)?.key)
}

pub(crate) fn native_structured_batch_column_count(
    batch: &NativeStructuredBatch,
    row: usize,
) -> BridgeResult<usize> {
    Ok(batch_row(batch, row)?.columns.len())
}

pub(crate) fn native_structured_batch_has_column(
    batch: &NativeStructuredBatch,
    row: usize,
    column: usize,
) -> bool {
    batch_column(batch, row, column).is_ok()
}

pub(crate) fn native_structured_batch_kind(
    batch: &NativeStructuredBatch,
    row: usize,
    column: usize,
) -> BridgeResult<u8> {
    Ok(match batch_column(batch, row, column)? {
        StructuredColumnValue::Bytes(_) => 0,
        StructuredColumnValue::List(_) => 1,
    })
}

pub(crate) fn native_structured_batch_bytes(
    batch: &NativeStructuredBatch,
    row: usize,
    column: usize,
) -> BridgeResult<&[u8]> {
    match batch_column(batch, row, column)? {
        StructuredColumnValue::Bytes(value) => Ok(value),
        StructuredColumnValue::List(_) => Err(input_error("structured batch column is not BYTES")),
    }
}

pub(crate) fn native_structured_batch_list_size(
    batch: &NativeStructuredBatch,
    row: usize,
    column: usize,
) -> BridgeResult<usize> {
    match batch_column(batch, row, column)? {
        StructuredColumnValue::List(elements) => Ok(elements.len()),
        StructuredColumnValue::Bytes(_) => Err(input_error("structured batch column is not LIST")),
    }
}

pub(crate) fn native_structured_batch_list_element(
    batch: &NativeStructuredBatch,
    row: usize,
    column: usize,
    element: usize,
) -> BridgeResult<&[u8]> {
    match batch_column(batch, row, column)? {
        StructuredColumnValue::List(elements) => elements
            .get(element)
            .map(AsRef::as_ref)
            .ok_or_else(|| input_error("LIST element index is out of bounds")),
        StructuredColumnValue::Bytes(_) => Err(input_error("structured batch column is not LIST")),
    }
}
