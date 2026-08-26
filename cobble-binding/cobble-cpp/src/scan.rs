use crate::{
    BridgeResult,
    database::{NativeDatabase, NativeDatabaseOwner, NativeRow},
    encoding::{
        STATUS_BUFFER_TOO_SMALL, batch_encoded_len, batch_status, buffer_result, encode_batch_into,
    },
    error::{format_cobble_error, input_error},
    ffi,
    options::{checked_nonzero_usize, checked_u64, checked_usize, to_scan_options},
};
use bytes::Bytes;
use cobble_binding::{DbIterator, ScanSplitScanner};

pub(crate) struct NativeBatchRow {
    pub(crate) bucket: u16,
    pub(crate) key: Bytes,
    pub(crate) columns: Vec<Option<Bytes>>,
}

pub(crate) struct NativeBatch {
    pub(crate) rows: Vec<NativeBatchRow>,
    pub(crate) end: bool,
    pub(crate) stopped_at_block_boundary: bool,
}

pub(crate) struct NativeScanCursor {
    iterator: NativeScanIterator,
    pending_row: Option<NativeBatchRow>,
    pending_batch: Option<NativeBatch>,
    // Drop the iterator/access guard before the final database owner.
    _owner: Option<NativeDatabaseOwner>,
}

// The cursor itself is already heap allocated for the opaque CXX handle. Keep
// both iterators inline to avoid another allocation and pointer chase per scan.
#[allow(clippy::large_enum_variant)]
enum NativeScanIterator {
    Db { bucket: u16, iterator: DbIterator },
    Split(ScanSplitScanner),
}

impl NativeScanIterator {
    fn next_row(&mut self) -> BridgeResult<Option<NativeBatchRow>> {
        match self {
            Self::Db { bucket, iterator } => iterator
                .next()
                .transpose()
                .map_err(format_cobble_error)
                .map(|row| {
                    row.map(|(key, columns)| NativeBatchRow {
                        bucket: *bucket,
                        key,
                        columns,
                    })
                }),
            Self::Split(scanner) => {
                scanner
                    .next()
                    .transpose()
                    .map_err(format_cobble_error)
                    .map(|row| {
                        row.map(|(bucket, key, columns)| NativeBatchRow {
                            bucket,
                            key,
                            columns,
                        })
                    })
            }
        }
    }

    fn stopped_at_block_boundary(&self) -> bool {
        match self {
            Self::Db { iterator, .. } => iterator.stopped_at_block_boundary(),
            Self::Split(_) => false,
        }
    }

    fn clear_stop_at_block_boundary(&mut self) -> BridgeResult<()> {
        match self {
            Self::Db { iterator, .. } => {
                iterator.clear_stop_at_block_boundary();
                Ok(())
            }
            Self::Split(_) => Err(input_error(
                "block-boundary resume is not supported for split scanners",
            )),
        }
    }
}

pub(crate) fn native_scan_cursor_from_db_iterator(
    bucket: u16,
    iterator: DbIterator,
    owner: Option<NativeDatabaseOwner>,
) -> Box<NativeScanCursor> {
    Box::new(NativeScanCursor {
        iterator: NativeScanIterator::Db { bucket, iterator },
        pending_row: None,
        pending_batch: None,
        _owner: owner,
    })
}

pub(crate) fn native_scan_cursor_from_split_scanner(
    scanner: ScanSplitScanner,
) -> Box<NativeScanCursor> {
    Box::new(NativeScanCursor {
        iterator: NativeScanIterator::Split(scanner),
        pending_row: None,
        pending_batch: None,
        _owner: None,
    })
}

pub(crate) fn native_database_scan(
    db: &NativeDatabase,
    bucket: u16,
    start: &[u8],
    has_start: bool,
    end: &[u8],
    has_end: bool,
    options: &ffi::NativeScanOptions,
) -> BridgeResult<Box<NativeScanCursor>> {
    let scan_options = to_scan_options(options)?;
    let iterator = db
        .db
        .db()
        .scan_with_options_bounds(
            bucket,
            has_start.then_some(start),
            has_end.then_some(end),
            &scan_options,
        )
        .map_err(format_cobble_error)?;
    Ok(native_scan_cursor_from_db_iterator(
        bucket,
        iterator,
        Some(NativeDatabaseOwner::single(db)),
    ))
}

pub(crate) fn native_sharded_database_scan(
    db: &crate::sharded_db::NativeShardedDatabase,
    bucket: u16,
    start: &[u8],
    has_start: bool,
    end: &[u8],
    has_end: bool,
    options: &ffi::NativeScanOptions,
) -> BridgeResult<Box<NativeScanCursor>> {
    let iterator = db
        .db
        .scan_with_options_bounds(
            bucket,
            has_start.then_some(start),
            has_end.then_some(end),
            &to_scan_options(options)?,
        )
        .map_err(format_cobble_error)?;
    Ok(native_scan_cursor_from_db_iterator(
        bucket,
        iterator,
        Some(NativeDatabaseOwner::sharded(db)),
    ))
}

pub(crate) fn native_scan_cursor_next_owned(
    cursor: &mut NativeScanCursor,
    max_rows: u64,
) -> BridgeResult<Box<NativeBatch>> {
    Ok(Box::new(take_or_read_batch(cursor, max_rows)?))
}

pub(crate) fn native_scan_cursor_next_batch_into(
    cursor: &mut NativeScanCursor,
    max_rows: u64,
    output: &mut [u8],
) -> BridgeResult<ffi::NativeBufferResult> {
    if cursor.pending_batch.is_none() {
        cursor.pending_batch = Some(read_batch(cursor, max_rows)?);
    }
    let batch = cursor
        .pending_batch
        .as_ref()
        .ok_or_else(|| "CB_INVALID_STATE: pending scan batch was not initialized".to_string())?;
    if batch.rows.is_empty() {
        let status = batch_status(batch);
        cursor.pending_batch = None;
        return Ok(buffer_result(status, 0, 0, 0));
    }
    let required = batch_encoded_len(batch)?;
    let row_count = checked_u64(batch.rows.len(), "batch row count")?;
    if output.len() < required {
        return Ok(buffer_result(
            STATUS_BUFFER_TOO_SMALL,
            0,
            checked_u64(required, "batch encoded length")?,
            row_count,
        ));
    }
    encode_batch_into(batch, &mut output[..required])?;
    let status = batch_status(batch);
    cursor.pending_batch = None;
    Ok(buffer_result(
        status,
        checked_u64(required, "batch encoded length")?,
        checked_u64(required, "batch encoded length")?,
        row_count,
    ))
}

pub(crate) fn native_scan_cursor_resume_after_block_boundary(
    cursor: &mut NativeScanCursor,
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

pub(crate) fn native_row_found(row: &NativeRow) -> bool {
    row.columns.is_some()
}
pub(crate) fn native_row_column_count(row: &NativeRow) -> u64 {
    row.columns.as_ref().map_or(0, |columns| {
        u64::try_from(columns.len()).unwrap_or(u64::MAX)
    })
}
pub(crate) fn native_row_has_column(row: &NativeRow, column: u64) -> bool {
    checked_usize(column, "column index")
        .ok()
        .and_then(|index| row.columns.as_ref()?.get(index))
        .is_some_and(Option::is_some)
}
pub(crate) fn native_row_column(row: &NativeRow, column: u64) -> BridgeResult<&[u8]> {
    let index = checked_usize(column, "column index")?;
    let column = row
        .columns
        .as_ref()
        .and_then(|columns| columns.get(index))
        .and_then(Option::as_ref)
        .ok_or_else(|| input_error("requested row column is absent"))?;
    Ok(column.as_ref())
}
pub(crate) fn native_batch_row_count(batch: &NativeBatch) -> u64 {
    u64::try_from(batch.rows.len()).unwrap_or(u64::MAX)
}
pub(crate) fn native_batch_end(batch: &NativeBatch) -> bool {
    batch.end
}
pub(crate) fn native_batch_stopped_at_block_boundary(batch: &NativeBatch) -> bool {
    batch.stopped_at_block_boundary
}
pub(crate) fn native_batch_bucket(batch: &NativeBatch, row: u64) -> BridgeResult<u16> {
    Ok(batch_row(batch, row)?.bucket)
}
pub(crate) fn native_batch_key(batch: &NativeBatch, row: u64) -> BridgeResult<&[u8]> {
    Ok(batch_row(batch, row)?.key.as_ref())
}
pub(crate) fn native_batch_column_count(batch: &NativeBatch, row: u64) -> BridgeResult<u64> {
    checked_u64(batch_row(batch, row)?.columns.len(), "batch column count")
}
pub(crate) fn native_batch_has_column(batch: &NativeBatch, row: u64, column: u64) -> bool {
    checked_usize(column, "column index")
        .ok()
        .and_then(|index| batch_row(batch, row).ok()?.columns.get(index))
        .is_some_and(Option::is_some)
}
pub(crate) fn native_batch_column(
    batch: &NativeBatch,
    row: u64,
    column: u64,
) -> BridgeResult<&[u8]> {
    let column = batch_row(batch, row)?
        .columns
        .get(checked_usize(column, "column index")?)
        .and_then(Option::as_ref)
        .ok_or_else(|| input_error("requested batch column is absent"))?;
    Ok(column.as_ref())
}

fn take_or_read_batch(cursor: &mut NativeScanCursor, max_rows: u64) -> BridgeResult<NativeBatch> {
    if let Some(batch) = cursor.pending_batch.take() {
        return Ok(batch);
    }
    read_batch(cursor, max_rows)
}
fn read_batch(cursor: &mut NativeScanCursor, max_rows: u64) -> BridgeResult<NativeBatch> {
    let max_rows = checked_nonzero_usize(max_rows, "scan batch max_rows")?;
    let mut rows = Vec::new();
    if let Some(row) = cursor.pending_row.take() {
        rows.push(row);
    }
    while rows.len() < max_rows {
        let Some(row) = next_cursor_row(cursor)? else {
            return Ok(NativeBatch {
                rows,
                end: !cursor.iterator.stopped_at_block_boundary(),
                stopped_at_block_boundary: cursor.iterator.stopped_at_block_boundary(),
            });
        };
        rows.push(row);
    }
    if let Some(row) = next_cursor_row(cursor)? {
        cursor.pending_row = Some(row);
    }
    let stopped_at_block_boundary = cursor.iterator.stopped_at_block_boundary();
    Ok(NativeBatch {
        rows,
        end: cursor.pending_row.is_none() && !stopped_at_block_boundary,
        stopped_at_block_boundary,
    })
}
fn next_cursor_row(cursor: &mut NativeScanCursor) -> BridgeResult<Option<NativeBatchRow>> {
    cursor.iterator.next_row()
}
fn batch_row(batch: &NativeBatch, row: u64) -> BridgeResult<&NativeBatchRow> {
    batch
        .rows
        .get(checked_usize(row, "row index")?)
        .ok_or_else(|| input_error("row index is out of bounds"))
}
