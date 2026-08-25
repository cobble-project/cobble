//! Private Rust surface for the C++ raw-KV binding.
//!
//! The public C++ API deliberately lives behind a PImpl wrapper. This bridge
//! exposes only opaque Rust owners, shared option/result values, and borrowed
//! byte slices to that wrapper.

use bytes::Bytes;
use cobble::{
    Config, DbIterator, Error, ReadOptions, RecoveryMode, ScanOptions, SingleDb, WriteBatch,
    WriteOptions,
};
use size::Size;
use std::sync::Arc;

const STATUS_OK: u8 = 0;
const STATUS_NOT_FOUND: u8 = 1;
const STATUS_END: u8 = 2;
const STATUS_BUFFER_TOO_SMALL: u8 = 3;
const STATUS_BLOCK_BOUNDARY: u8 = 4;

const BATCH_MAGIC: [u8; 4] = *b"CBRB";
const BATCH_VERSION: u16 = 1;
const BATCH_HEADER_SIZE: usize = 24;
const BATCH_FLAG_END: u32 = 1;
const BATCH_FLAG_BOUNDARY: u32 = 2;
const NONE_COLUMN_LENGTH: u64 = u64::MAX;

#[cxx::bridge(namespace = "cobble::ffi")]
mod ffi {
    struct NativeReadOptions {
        column_family: String,
        columns: Vec<u64>,
    }

    struct NativeWriteOptions {
        has_ttl_seconds: bool,
        ttl_seconds: u32,
        column_family: String,
        await_durable: bool,
    }

    struct NativeScanOptions {
        column_family: String,
        columns: Vec<u64>,
        read_ahead_bytes: u64,
        has_max_rows: bool,
        max_rows: u64,
        preload_scan_cursor_block: bool,
        stop_at_block_boundary: bool,
    }

    struct NativeBufferResult {
        status: u8,
        bytes_written: u64,
        bytes_required: u64,
        row_count: u64,
    }

    extern "Rust" {
        type NativeDatabase;
        type NativeWriteBatch;
        type NativeRow;
        type NativeScanCursor;
        type NativeBatch;

        fn native_database_open(config_json: &str) -> Result<Box<NativeDatabase>>;
        fn native_database_open_file(config_path: &str) -> Result<Box<NativeDatabase>>;
        fn native_database_resume(
            config_json: &str,
            snapshot_id: u64,
            recovery_mode: u8,
        ) -> Result<Box<NativeDatabase>>;
        fn native_database_resume_file(
            config_path: &str,
            snapshot_id: u64,
            recovery_mode: u8,
        ) -> Result<Box<NativeDatabase>>;
        fn native_database_close(db: &NativeDatabase) -> Result<()>;
        fn native_database_version() -> String;

        fn native_database_put(
            db: &NativeDatabase,
            bucket: u16,
            key: &[u8],
            column: u16,
            value: &[u8],
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_database_delete(
            db: &NativeDatabase,
            bucket: u16,
            key: &[u8],
            column: u16,
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_database_merge(
            db: &NativeDatabase,
            bucket: u16,
            key: &[u8],
            column: u16,
            value: &[u8],
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_database_write_batch(
            db: &NativeDatabase,
            batch: Box<NativeWriteBatch>,
            await_durable: bool,
        ) -> Result<()>;

        fn native_database_get(
            db: &NativeDatabase,
            bucket: u16,
            key: &[u8],
            options: &NativeReadOptions,
        ) -> Result<Box<NativeRow>>;
        fn native_database_get_column_into(
            db: &NativeDatabase,
            bucket: u16,
            key: &[u8],
            output: &mut [u8],
            options: &NativeReadOptions,
        ) -> Result<NativeBufferResult>;

        fn native_database_scan(
            db: &NativeDatabase,
            bucket: u16,
            start: &[u8],
            has_start: bool,
            end: &[u8],
            has_end: bool,
            options: &NativeScanOptions,
        ) -> Result<Box<NativeScanCursor>>;
        fn native_scan_cursor_next_owned(
            cursor: &mut NativeScanCursor,
            max_rows: u64,
        ) -> Result<Box<NativeBatch>>;
        fn native_scan_cursor_next_batch_into(
            cursor: &mut NativeScanCursor,
            max_rows: u64,
            output: &mut [u8],
        ) -> Result<NativeBufferResult>;
        fn native_scan_cursor_resume_after_block_boundary(cursor: &mut NativeScanCursor);

        fn native_write_batch_new() -> Box<NativeWriteBatch>;
        fn native_write_batch_len(batch: &NativeWriteBatch) -> u64;
        fn native_write_batch_put(
            batch: &mut NativeWriteBatch,
            bucket: u16,
            key: &[u8],
            column: u16,
            value: &[u8],
            options: &NativeWriteOptions,
        );
        fn native_write_batch_delete(
            batch: &mut NativeWriteBatch,
            bucket: u16,
            key: &[u8],
            column: u16,
            options: &NativeWriteOptions,
        );
        fn native_write_batch_merge(
            batch: &mut NativeWriteBatch,
            bucket: u16,
            key: &[u8],
            column: u16,
            value: &[u8],
            options: &NativeWriteOptions,
        );

        fn native_row_found(row: &NativeRow) -> bool;
        fn native_row_column_count(row: &NativeRow) -> u64;
        fn native_row_has_column(row: &NativeRow, column: u64) -> bool;
        fn native_row_column(row: &NativeRow, column: u64) -> Result<&[u8]>;

        fn native_batch_row_count(batch: &NativeBatch) -> u64;
        fn native_batch_end(batch: &NativeBatch) -> bool;
        fn native_batch_stopped_at_block_boundary(batch: &NativeBatch) -> bool;
        fn native_batch_bucket(batch: &NativeBatch, row: u64) -> Result<u16>;
        fn native_batch_key(batch: &NativeBatch, row: u64) -> Result<&[u8]>;
        fn native_batch_column_count(batch: &NativeBatch, row: u64) -> Result<u64>;
        fn native_batch_has_column(batch: &NativeBatch, row: u64, column: u64) -> bool;
        fn native_batch_column(batch: &NativeBatch, row: u64, column: u64) -> Result<&[u8]>;

        fn native_database_snapshot(db: &NativeDatabase) -> Result<u64>;
        fn native_database_retain_snapshot(db: &NativeDatabase, snapshot_id: u64) -> Result<bool>;
        fn native_database_expire_snapshot(db: &NativeDatabase, snapshot_id: u64) -> Result<bool>;
        fn native_database_list_snapshots(db: &NativeDatabase) -> Result<Vec<u64>>;
        fn native_database_snapshot_manifest_json(
            db: &NativeDatabase,
            snapshot_id: u64,
        ) -> Result<String>;
        fn native_database_set_time(db: &NativeDatabase, unix_seconds: u32);
    }
}

type BridgeResult<T> = std::result::Result<T, String>;

pub struct NativeDatabase {
    db: Arc<SingleDb>,
}

pub struct NativeWriteBatch {
    batch: WriteBatch,
    count: u64,
}

pub struct NativeRow {
    columns: Option<Vec<Option<Bytes>>>,
}

struct NativeBatchRow {
    bucket: u16,
    key: Bytes,
    columns: Vec<Option<Bytes>>,
}

pub struct NativeBatch {
    rows: Vec<NativeBatchRow>,
    end: bool,
    stopped_at_block_boundary: bool,
}

pub struct NativeScanCursor {
    // The owner must outlive the iterator. In particular, it prevents a C++
    // Database wrapper from dropping the last SingleDb while its cursor holds
    // an active lifecycle access guard.
    _owner: Arc<SingleDb>,
    bucket: u16,
    iterator: DbIterator,
    pending_row: Option<NativeBatchRow>,
    pending_batch: Option<NativeBatch>,
}

fn native_database_open(config_json: &str) -> BridgeResult<Box<NativeDatabase>> {
    open_database(parse_config_json(config_json)?)
}

fn native_database_open_file(config_path: &str) -> BridgeResult<Box<NativeDatabase>> {
    open_database(Config::from_path(config_path).map_err(format_cobble_error)?)
}

fn native_database_resume(
    config_json: &str,
    snapshot_id: u64,
    recovery_mode: u8,
) -> BridgeResult<Box<NativeDatabase>> {
    resume_database(
        parse_config_json(config_json)?,
        snapshot_id,
        decode_recovery_mode(recovery_mode)?,
    )
}

fn native_database_resume_file(
    config_path: &str,
    snapshot_id: u64,
    recovery_mode: u8,
) -> BridgeResult<Box<NativeDatabase>> {
    resume_database(
        Config::from_path(config_path).map_err(format_cobble_error)?,
        snapshot_id,
        decode_recovery_mode(recovery_mode)?,
    )
}

fn native_database_close(db: &NativeDatabase) -> BridgeResult<()> {
    db.db.close().map_err(format_cobble_error)
}

fn native_database_version() -> String {
    cobble::ffi::build_version_string().to_owned()
}

fn native_database_put(
    db: &NativeDatabase,
    bucket: u16,
    key: &[u8],
    column: u16,
    value: &[u8],
    options: &ffi::NativeWriteOptions,
) -> BridgeResult<()> {
    db.db
        .put_with_options(bucket, key, column, value, &to_write_options(options))
        .map_err(format_cobble_error)
}

fn native_database_delete(
    db: &NativeDatabase,
    bucket: u16,
    key: &[u8],
    column: u16,
    options: &ffi::NativeWriteOptions,
) -> BridgeResult<()> {
    db.db
        .delete_with_options(bucket, key, column, &to_write_options(options))
        .map_err(format_cobble_error)
}

fn native_database_merge(
    db: &NativeDatabase,
    bucket: u16,
    key: &[u8],
    column: u16,
    value: &[u8],
    options: &ffi::NativeWriteOptions,
) -> BridgeResult<()> {
    db.db
        .merge_with_options(bucket, key, column, value, &to_write_options(options))
        .map_err(format_cobble_error)
}

#[allow(clippy::boxed_local)]
fn native_database_write_batch(
    db: &NativeDatabase,
    batch: Box<NativeWriteBatch>,
    await_durable: bool,
) -> BridgeResult<()> {
    let options = WriteOptions::default().with_await_durable(await_durable);
    db.db
        .write_batch_with_options(batch.batch, &options)
        .map_err(format_cobble_error)
}

fn native_database_get(
    db: &NativeDatabase,
    bucket: u16,
    key: &[u8],
    options: &ffi::NativeReadOptions,
) -> BridgeResult<Box<NativeRow>> {
    Ok(Box::new(NativeRow {
        columns: db
            .db
            .get_with_options(bucket, key, &to_read_options(options)?)
            .map_err(format_cobble_error)?,
    }))
}

fn native_database_get_column_into(
    db: &NativeDatabase,
    bucket: u16,
    key: &[u8],
    output: &mut [u8],
    options: &ffi::NativeReadOptions,
) -> BridgeResult<ffi::NativeBufferResult> {
    let read_options = to_single_column_read_options(options)?;
    let Some(columns) = db
        .db
        .get_with_options(bucket, key, &read_options)
        .map_err(format_cobble_error)?
    else {
        return Ok(buffer_result(STATUS_NOT_FOUND, 0, 0, 0));
    };
    let Some(Some(column)) = columns.into_iter().next() else {
        return Ok(buffer_result(STATUS_NOT_FOUND, 0, 0, 0));
    };
    let required = checked_u64(column.len(), "column value length")?;
    if output.len() < column.len() {
        return Ok(buffer_result(STATUS_BUFFER_TOO_SMALL, 0, required, 1));
    }
    output[..column.len()].copy_from_slice(column.as_ref());
    Ok(buffer_result(STATUS_OK, required, required, 1))
}

fn native_database_scan(
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
    Ok(Box::new(NativeScanCursor {
        _owner: Arc::clone(&db.db),
        bucket,
        iterator,
        pending_row: None,
        pending_batch: None,
    }))
}

fn native_scan_cursor_next_owned(
    cursor: &mut NativeScanCursor,
    max_rows: u64,
) -> BridgeResult<Box<NativeBatch>> {
    Ok(Box::new(take_or_read_batch(cursor, max_rows)?))
}

fn native_scan_cursor_next_batch_into(
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

fn native_scan_cursor_resume_after_block_boundary(cursor: &mut NativeScanCursor) {
    if cursor
        .pending_batch
        .as_ref()
        .is_some_and(|batch| batch.rows.is_empty() && batch.stopped_at_block_boundary)
    {
        cursor.pending_batch = None;
    }
    cursor.iterator.clear_stop_at_block_boundary();
}

fn native_write_batch_new() -> Box<NativeWriteBatch> {
    Box::new(NativeWriteBatch {
        batch: WriteBatch::new(),
        count: 0,
    })
}

fn native_write_batch_len(batch: &NativeWriteBatch) -> u64 {
    batch.count
}

fn native_write_batch_put(
    batch: &mut NativeWriteBatch,
    bucket: u16,
    key: &[u8],
    column: u16,
    value: &[u8],
    options: &ffi::NativeWriteOptions,
) {
    batch
        .batch
        .put_with_options(bucket, key, column, value, &to_write_options(options));
    batch.count = batch.count.saturating_add(1);
}

fn native_write_batch_delete(
    batch: &mut NativeWriteBatch,
    bucket: u16,
    key: &[u8],
    column: u16,
    options: &ffi::NativeWriteOptions,
) {
    batch
        .batch
        .delete_with_options(bucket, key, column, &to_write_options(options));
    batch.count = batch.count.saturating_add(1);
}

fn native_write_batch_merge(
    batch: &mut NativeWriteBatch,
    bucket: u16,
    key: &[u8],
    column: u16,
    value: &[u8],
    options: &ffi::NativeWriteOptions,
) {
    batch
        .batch
        .merge_with_options(bucket, key, column, value, &to_write_options(options));
    batch.count = batch.count.saturating_add(1);
}

fn native_row_found(row: &NativeRow) -> bool {
    row.columns.is_some()
}

fn native_row_column_count(row: &NativeRow) -> u64 {
    row.columns.as_ref().map_or(0, |columns| {
        u64::try_from(columns.len()).unwrap_or(u64::MAX)
    })
}

fn native_row_has_column(row: &NativeRow, column: u64) -> bool {
    checked_usize(column, "column index")
        .ok()
        .and_then(|index| row.columns.as_ref()?.get(index))
        .is_some_and(Option::is_some)
}

fn native_row_column(row: &NativeRow, column: u64) -> BridgeResult<&[u8]> {
    let index = checked_usize(column, "column index")?;
    let column = row
        .columns
        .as_ref()
        .and_then(|columns| columns.get(index))
        .and_then(Option::as_ref)
        .ok_or_else(|| input_error("requested row column is absent"))?;
    Ok(column.as_ref())
}

fn native_batch_row_count(batch: &NativeBatch) -> u64 {
    u64::try_from(batch.rows.len()).unwrap_or(u64::MAX)
}

fn native_batch_end(batch: &NativeBatch) -> bool {
    batch.end
}

fn native_batch_stopped_at_block_boundary(batch: &NativeBatch) -> bool {
    batch.stopped_at_block_boundary
}

fn native_batch_bucket(batch: &NativeBatch, row: u64) -> BridgeResult<u16> {
    Ok(batch_row(batch, row)?.bucket)
}

fn native_batch_key(batch: &NativeBatch, row: u64) -> BridgeResult<&[u8]> {
    Ok(batch_row(batch, row)?.key.as_ref())
}

fn native_batch_column_count(batch: &NativeBatch, row: u64) -> BridgeResult<u64> {
    checked_u64(batch_row(batch, row)?.columns.len(), "batch column count")
}

fn native_batch_has_column(batch: &NativeBatch, row: u64, column: u64) -> bool {
    checked_usize(column, "column index")
        .ok()
        .and_then(|index| batch_row(batch, row).ok()?.columns.get(index))
        .is_some_and(Option::is_some)
}

fn native_batch_column(batch: &NativeBatch, row: u64, column: u64) -> BridgeResult<&[u8]> {
    let column = batch_row(batch, row)?
        .columns
        .get(checked_usize(column, "column index")?)
        .and_then(Option::as_ref)
        .ok_or_else(|| input_error("requested batch column is absent"))?;
    Ok(column.as_ref())
}

fn native_database_snapshot(db: &NativeDatabase) -> BridgeResult<u64> {
    db.db.snapshot().map_err(format_cobble_error)
}

fn native_database_retain_snapshot(db: &NativeDatabase, snapshot_id: u64) -> BridgeResult<bool> {
    db.db
        .retain_snapshot(snapshot_id)
        .map_err(format_cobble_error)
}

fn native_database_expire_snapshot(db: &NativeDatabase, snapshot_id: u64) -> BridgeResult<bool> {
    db.db
        .expire_snapshot(snapshot_id)
        .map_err(format_cobble_error)
}

fn native_database_list_snapshots(db: &NativeDatabase) -> BridgeResult<Vec<u64>> {
    db.db
        .list_snapshots()
        .map(|snapshots| snapshots.into_iter().map(|snapshot| snapshot.id).collect())
        .map_err(format_cobble_error)
}

fn native_database_snapshot_manifest_json(
    db: &NativeDatabase,
    snapshot_id: u64,
) -> BridgeResult<String> {
    let manifest = db
        .db
        .get_snapshot(snapshot_id)
        .map_err(format_cobble_error)?;
    serde_json::to_string(&manifest)
        .map_err(|error| format!("CB_CONFIGURATION: cannot encode snapshot manifest: {error}"))
}

fn native_database_set_time(db: &NativeDatabase, unix_seconds: u32) {
    db.db.set_time(unix_seconds);
}

fn open_database(config: Config) -> BridgeResult<Box<NativeDatabase>> {
    // Static libraries do not reliably run OpenDAL's constructor-based
    // registration. Initialize the enabled storage services and default HTTP
    // transport before URI-backed storage is first used.
    opendal::install_default();
    SingleDb::open(config)
        .map(|db| Box::new(NativeDatabase { db: Arc::new(db) }))
        .map_err(format_cobble_error)
}

fn resume_database(
    config: Config,
    snapshot_id: u64,
    recovery_mode: RecoveryMode,
) -> BridgeResult<Box<NativeDatabase>> {
    opendal::install_default();
    SingleDb::resume_with_recovery_mode(config, snapshot_id, recovery_mode)
        .map(|db| Box::new(NativeDatabase { db: Arc::new(db) }))
        .map_err(format_cobble_error)
}

fn parse_config_json(config_json: &str) -> BridgeResult<Config> {
    Config::from_json_str(config_json).map_err(format_cobble_error)
}

fn decode_recovery_mode(mode: u8) -> BridgeResult<RecoveryMode> {
    match mode {
        0 => Ok(RecoveryMode::SnapshotOnly),
        1 => Ok(RecoveryMode::LatestWithWal),
        _ => Err(input_error(
            "recovery mode must be 0 (snapshot-only) or 1 (latest-with-wal)",
        )),
    }
}

fn to_read_options(options: &ffi::NativeReadOptions) -> BridgeResult<ReadOptions> {
    let columns = decode_columns(&options.columns)?;
    Ok(match (optional_string(&options.column_family), columns) {
        (Some(column_family), Some(columns)) => {
            ReadOptions::for_columns_in_family(column_family, columns)
        }
        (Some(column_family), None) => ReadOptions::default().with_column_family(column_family),
        (None, Some(columns)) => ReadOptions::for_columns(columns),
        (None, None) => ReadOptions::default(),
    })
}

fn to_single_column_read_options(options: &ffi::NativeReadOptions) -> BridgeResult<ReadOptions> {
    if options.columns.len() != 1 {
        return Err(input_error(
            "get_column_into requires exactly one projected column",
        ));
    }
    to_read_options(options)
}

fn to_write_options(options: &ffi::NativeWriteOptions) -> WriteOptions {
    let mut write_options = WriteOptions::default().with_await_durable(options.await_durable);
    write_options.ttl_seconds = options.has_ttl_seconds.then_some(options.ttl_seconds);
    write_options.column_family = optional_string(&options.column_family);
    write_options
}

fn to_scan_options(options: &ffi::NativeScanOptions) -> BridgeResult<ScanOptions> {
    if options.has_max_rows && options.max_rows == 0 {
        return Err(input_error("scan max_rows must be greater than zero"));
    }
    let columns = decode_columns(&options.columns)?;
    let mut scan_options = match (optional_string(&options.column_family), columns) {
        (Some(column_family), Some(columns)) => {
            ScanOptions::for_columns(columns).with_column_family(column_family)
        }
        (Some(column_family), None) => ScanOptions::default().with_column_family(column_family),
        (None, Some(columns)) => ScanOptions::for_columns(columns),
        (None, None) => ScanOptions::default(),
    };
    let read_ahead_bytes = i64::try_from(options.read_ahead_bytes)
        .map_err(|_| input_error("scan read_ahead_bytes exceeds the supported size"))?;
    scan_options.read_ahead_bytes = Size::from_const(read_ahead_bytes);
    if options.has_max_rows {
        scan_options.set_max_rows(checked_usize(options.max_rows, "scan max_rows")?);
    }
    scan_options.set_preload_scan_cursor_block(options.preload_scan_cursor_block);
    scan_options = scan_options.with_stop_at_block_boundary(options.stop_at_block_boundary);
    Ok(scan_options)
}

fn decode_columns(columns: &[u64]) -> BridgeResult<Option<Vec<usize>>> {
    if columns.is_empty() {
        return Ok(None);
    }
    columns
        .iter()
        .map(|&column| checked_usize(column, "column index"))
        .collect::<BridgeResult<Vec<_>>>()
        .map(Some)
}

fn optional_string(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
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

    // Look one row ahead so the batch's end flag is precise without losing a
    // row. The held row is consumed before reading the iterator again.
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
    cursor
        .iterator
        .next()
        .transpose()
        .map_err(format_cobble_error)
        .map(|row| {
            row.map(|(key, columns)| NativeBatchRow {
                bucket: cursor.bucket,
                key,
                columns,
            })
        })
}

fn batch_status(batch: &NativeBatch) -> u8 {
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

fn batch_row(batch: &NativeBatch, row: u64) -> BridgeResult<&NativeBatchRow> {
    batch
        .rows
        .get(checked_usize(row, "row index")?)
        .ok_or_else(|| input_error("row index is out of bounds"))
}

fn batch_encoded_len(batch: &NativeBatch) -> BridgeResult<usize> {
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

fn encode_batch_into(batch: &NativeBatch, output: &mut [u8]) -> BridgeResult<()> {
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

fn write_u16(output: &mut [u8], value: u16) {
    output.copy_from_slice(&value.to_le_bytes());
}

fn write_u32(output: &mut [u8], value: u32) {
    output.copy_from_slice(&value.to_le_bytes());
}

fn write_u64(output: &mut [u8], value: u64) {
    output.copy_from_slice(&value.to_le_bytes());
}

fn buffer_result(
    status: u8,
    bytes_written: u64,
    bytes_required: u64,
    row_count: u64,
) -> ffi::NativeBufferResult {
    ffi::NativeBufferResult {
        status,
        bytes_written,
        bytes_required,
        row_count,
    }
}

fn checked_nonzero_usize(value: u64, name: &str) -> BridgeResult<usize> {
    if value == 0 {
        return Err(input_error(&format!("{name} must be greater than zero")));
    }
    checked_usize(value, name)
}

fn checked_usize(value: u64, name: &str) -> BridgeResult<usize> {
    usize::try_from(value).map_err(|_| input_error(&format!("{name} exceeds the supported size")))
}

fn checked_u32(value: usize, name: &str) -> BridgeResult<u32> {
    u32::try_from(value).map_err(|_| input_error(&format!("{name} exceeds u32::MAX")))
}

fn checked_u64(value: usize, name: &str) -> BridgeResult<u64> {
    u64::try_from(value).map_err(|_| input_error(&format!("{name} exceeds u64::MAX")))
}

fn checked_add(left: usize, right: usize, name: &str) -> BridgeResult<usize> {
    left.checked_add(right)
        .ok_or_else(|| input_error(&format!("{name} overflows the batch encoding size")))
}

fn input_error(message: &str) -> String {
    format!("CB_INPUT: {message}")
}

fn format_cobble_error(error: Error) -> String {
    let prefix = match error {
        Error::UrlParseError(_) => "CB_URL",
        Error::FileSystemError(_) => "CB_FILE_SYSTEM",
        Error::IoError(_) => "CB_IO",
        Error::MemtableFull { .. } => "CB_MEMTABLE_FULL",
        Error::ConfigError(_) => "CB_CONFIGURATION",
        Error::InputError(_) => "CB_INPUT",
        Error::CoordinationError(_) => "CB_COORDINATION",
        Error::InvalidState(_) => "CB_INVALID_STATE",
        Error::FileFormatError(_) => "CB_FILE_FORMAT",
        Error::ChecksumMismatch(_) => "CB_CHECKSUM",
        Error::CancelledError(_) => "CB_CANCELLED",
    };
    format!("{prefix}: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    fn batch(rows: Vec<NativeBatchRow>, end: bool, boundary: bool) -> NativeBatch {
        NativeBatch {
            rows,
            end,
            stopped_at_block_boundary: boundary,
        }
    }

    #[test]
    fn encodes_little_endian_versioned_batch_with_null_columns() {
        let batch = batch(
            vec![NativeBatchRow {
                bucket: 7,
                key: Bytes::from_static(b"key"),
                columns: vec![Some(Bytes::from_static(b"value")), None],
            }],
            true,
            false,
        );
        let mut encoded = vec![0; batch_encoded_len(&batch).unwrap()];
        encode_batch_into(&batch, &mut encoded).unwrap();

        assert_eq!(&encoded[..4], b"CBRB");
        assert_eq!(u16::from_le_bytes(encoded[4..6].try_into().unwrap()), 1);
        assert_eq!(u16::from_le_bytes(encoded[6..8].try_into().unwrap()), 24);
        assert_eq!(u32::from_le_bytes(encoded[8..12].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(encoded[12..16].try_into().unwrap()), 1);
        assert_eq!(
            u64::from_le_bytes(encoded[16..24].try_into().unwrap()) as usize,
            encoded.len()
        );
        assert_eq!(u16::from_le_bytes(encoded[24..26].try_into().unwrap()), 7);
        assert_eq!(u32::from_le_bytes(encoded[28..32].try_into().unwrap()), 3);
        assert_eq!(u32::from_le_bytes(encoded[32..36].try_into().unwrap()), 2);
        assert_eq!(&encoded[36..39], b"key");
        assert_eq!(u64::from_le_bytes(encoded[39..47].try_into().unwrap()), 5);
        assert_eq!(&encoded[47..52], b"value");
        assert_eq!(
            u64::from_le_bytes(encoded[52..60].try_into().unwrap()),
            u64::MAX
        );
    }

    #[test]
    fn too_small_batch_buffer_can_be_checked_without_mutating_output() {
        let batch = batch(Vec::new(), true, false);
        let required = batch_encoded_len(&batch).unwrap();
        let output = vec![0xa5; required - 1];
        assert!(output.len() < required);
        assert_eq!(output, vec![0xa5; required - 1]);
    }

    #[test]
    fn error_prefixes_cover_every_cobble_error_variant() {
        let cases = [
            (Error::FileSystemError("x".to_string()), "CB_FILE_SYSTEM:"),
            (Error::IoError("x".to_string()), "CB_IO:"),
            (
                Error::MemtableFull {
                    needed: 2,
                    remaining: 1,
                },
                "CB_MEMTABLE_FULL:",
            ),
            (Error::ConfigError("x".to_string()), "CB_CONFIGURATION:"),
            (Error::InputError("x".to_string()), "CB_INPUT:"),
            (
                Error::CoordinationError("x".to_string()),
                "CB_COORDINATION:",
            ),
            (Error::InvalidState("x".to_string()), "CB_INVALID_STATE:"),
            (Error::FileFormatError("x".to_string()), "CB_FILE_FORMAT:"),
            (Error::ChecksumMismatch("x".to_string()), "CB_CHECKSUM:"),
            (Error::CancelledError("x".to_string()), "CB_CANCELLED:"),
        ];
        for (error, prefix) in cases {
            assert!(format_cobble_error(error).starts_with(prefix));
        }
    }

    #[test]
    fn caller_buffer_scan_keeps_pending_batch_after_small_buffer() {
        let root = format!(
            "/tmp/cobble_cpp_pending_batch_{}_{}",
            std::process::id(),
            NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)
        );
        let _ = std::fs::remove_dir_all(&root);
        let config = Config {
            volumes: cobble::VolumeDescriptor::single_volume(format!("file://{root}")),
            num_columns: 1,
            total_buckets: 1,
            ..Config::default()
        };
        let db = open_database(config).unwrap();
        let write_options = ffi::NativeWriteOptions {
            has_ttl_seconds: false,
            ttl_seconds: 0,
            column_family: String::new(),
            await_durable: true,
        };
        native_database_put(&db, 0, b"key", 0, b"value", &write_options).unwrap();
        let scan_options = ffi::NativeScanOptions {
            column_family: String::new(),
            columns: Vec::new(),
            read_ahead_bytes: 0,
            has_max_rows: false,
            max_rows: 0,
            preload_scan_cursor_block: false,
            stop_at_block_boundary: false,
        };
        let mut cursor =
            native_database_scan(&db, 0, &[], false, &[], false, &scan_options).unwrap();

        let mut too_small = [0xa5; 1];
        let result = native_scan_cursor_next_batch_into(&mut cursor, 1, &mut too_small).unwrap();
        assert_eq!(result.status, STATUS_BUFFER_TOO_SMALL);
        assert_eq!(too_small, [0xa5; 1]);

        let mut output = vec![0; usize::try_from(result.bytes_required).unwrap()];
        let written = native_scan_cursor_next_batch_into(&mut cursor, 1, &mut output).unwrap();
        assert_eq!(written.status, STATUS_OK);
        assert_eq!(written.row_count, 1);
        assert_eq!(&output[..4], b"CBRB");

        let end = native_scan_cursor_next_batch_into(&mut cursor, 1, &mut []).unwrap();
        assert_eq!(end.status, STATUS_END);
        drop(cursor);
        native_database_close(&db).unwrap();
        drop(db);
        let _ = std::fs::remove_dir_all(root);
    }
}
