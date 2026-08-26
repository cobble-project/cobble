use cobble_binding::{Config, Db, RecoveryMode, WriteOptions};
use std::{ops::RangeInclusive, sync::Arc, time::Duration};

use crate::{
    BridgeResult,
    database::NativeRow,
    encoding::{STATUS_BUFFER_TOO_SMALL, STATUS_NOT_FOUND, STATUS_OK, buffer_result},
    error::{format_cobble_error, input_error},
    ffi,
    options::{checked_u64, to_read_options, to_single_column_read_options, to_write_options},
    write_batch::NativeWriteBatch,
};

pub(crate) struct NativeShardedDatabase {
    pub(crate) db: Arc<Db>,
}

fn parse_config_json(config_json: &str) -> BridgeResult<Config> {
    Config::from_json_str(config_json).map_err(format_cobble_error)
}

fn parse_config_file(config_path: &str) -> BridgeResult<Config> {
    Config::from_path(config_path).map_err(format_cobble_error)
}

fn recovery_mode(mode: u8) -> BridgeResult<RecoveryMode> {
    match mode {
        0 => Ok(RecoveryMode::SnapshotOnly),
        1 => Ok(RecoveryMode::LatestWithWal),
        _ => Err(input_error(
            "recovery mode must be 0 (snapshot-only) or 1 (latest-with-wal)",
        )),
    }
}

fn total_bucket_range(config: &Config) -> BridgeResult<Vec<RangeInclusive<u16>>> {
    if config.total_buckets == 0 || config.total_buckets > u32::from(u16::MAX) + 1 {
        return Err(input_error("total_buckets must be in range 1..=65536"));
    }
    let last = u16::try_from(config.total_buckets - 1)
        .map_err(|_| input_error("total_buckets does not fit the bucket id range"))?;
    Ok(vec![0..=last])
}

fn bucket_ranges(
    config: &Config,
    ranges: Vec<ffi::NativeRange>,
    operation: &str,
) -> BridgeResult<Vec<RangeInclusive<u16>>> {
    total_bucket_range(config)?;
    if ranges.is_empty() {
        return Err(input_error(&format!(
            "{operation} ranges must not be empty"
        )));
    }
    let mut converted = Vec::with_capacity(ranges.len());
    for range in ranges {
        if range.first > range.last {
            return Err(input_error(&format!(
                "{operation} range {}..={} is reversed",
                range.first, range.last
            )));
        }
        if u32::from(range.last) >= config.total_buckets {
            return Err(input_error(&format!(
                "{operation} range {}..={} exceeds total_buckets {}",
                range.first, range.last, config.total_buckets
            )));
        }
        converted.push(range.first..=range.last);
    }
    Ok(converted)
}

fn open(
    config: Config,
    ranges: Vec<RangeInclusive<u16>>,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    opendal::install_default();
    Db::open(config, ranges)
        .map(|db| Box::new(NativeShardedDatabase { db: Arc::new(db) }))
        .map_err(format_cobble_error)
}

fn owned(db: Db) -> Box<NativeShardedDatabase> {
    Box::new(NativeShardedDatabase { db: Arc::new(db) })
}

pub(crate) fn native_sharded_database_open(
    config_json: &str,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    let config = parse_config_json(config_json)?;
    let ranges = total_bucket_range(&config)?;
    open(config, ranges)
}

pub(crate) fn native_sharded_database_open_ranges(
    config_json: &str,
    ranges: Vec<ffi::NativeRange>,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    let config = parse_config_json(config_json)?;
    let ranges = bucket_ranges(&config, ranges, "open")?;
    open(config, ranges)
}

pub(crate) fn native_sharded_database_open_file(
    config_path: &str,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    let config = parse_config_file(config_path)?;
    let ranges = total_bucket_range(&config)?;
    open(config, ranges)
}

pub(crate) fn native_sharded_database_open_file_ranges(
    config_path: &str,
    ranges: Vec<ffi::NativeRange>,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    let config = parse_config_file(config_path)?;
    let ranges = bucket_ranges(&config, ranges, "open")?;
    open(config, ranges)
}

pub(crate) fn native_sharded_database_open_from_snapshot(
    config_json: &str,
    snapshot_id: u64,
    db_id: &str,
    mode: u8,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    opendal::install_default();
    Db::open_from_snapshot_with_recovery_mode(
        parse_config_json(config_json)?,
        snapshot_id,
        db_id,
        recovery_mode(mode)?,
    )
    .map(owned)
    .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_open_from_snapshot_file(
    config_path: &str,
    snapshot_id: u64,
    db_id: &str,
    mode: u8,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    opendal::install_default();
    Db::open_from_snapshot_with_recovery_mode(
        parse_config_file(config_path)?,
        snapshot_id,
        db_id,
        recovery_mode(mode)?,
    )
    .map(owned)
    .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_restore_new(
    config_json: &str,
    snapshot_id: u64,
    source_db_id: &str,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    opendal::install_default();
    Db::open_new_with_snapshot(parse_config_json(config_json)?, snapshot_id, source_db_id)
        .map(owned)
        .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_restore_new_file(
    config_path: &str,
    snapshot_id: u64,
    source_db_id: &str,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    opendal::install_default();
    Db::open_new_with_snapshot(parse_config_file(config_path)?, snapshot_id, source_db_id)
        .map(owned)
        .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_restore_new_from_manifest(
    config_json: &str,
    manifest_path: &str,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    opendal::install_default();
    Db::open_new_with_manifest_path(parse_config_json(config_json)?, manifest_path)
        .map(owned)
        .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_restore_new_from_manifest_file(
    config_path: &str,
    manifest_path: &str,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    opendal::install_default();
    Db::open_new_with_manifest_path(parse_config_file(config_path)?, manifest_path)
        .map(owned)
        .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_resume(
    config_json: &str,
    db_id: &str,
    mode: u8,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    opendal::install_default();
    Db::resume_with_recovery_mode(parse_config_json(config_json)?, db_id, recovery_mode(mode)?)
        .map(owned)
        .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_resume_file(
    config_path: &str,
    db_id: &str,
    mode: u8,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    opendal::install_default();
    Db::resume_with_recovery_mode(parse_config_file(config_path)?, db_id, recovery_mode(mode)?)
        .map(owned)
        .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_resume_from_snapshot(
    config_json: &str,
    snapshot_id: u64,
    db_id: &str,
    mode: u8,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    opendal::install_default();
    Db::resume_from_snapshot_with_recovery_mode(
        parse_config_json(config_json)?,
        snapshot_id,
        db_id,
        recovery_mode(mode)?,
    )
    .map(owned)
    .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_resume_from_snapshot_file(
    config_path: &str,
    snapshot_id: u64,
    db_id: &str,
    mode: u8,
) -> BridgeResult<Box<NativeShardedDatabase>> {
    opendal::install_default();
    Db::resume_from_snapshot_with_recovery_mode(
        parse_config_file(config_path)?,
        snapshot_id,
        db_id,
        recovery_mode(mode)?,
    )
    .map(owned)
    .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_id(db: &NativeShardedDatabase) -> &str {
    db.db.id()
}

pub(crate) fn native_sharded_database_close(db: &NativeShardedDatabase) -> BridgeResult<()> {
    db.db.close().map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_put(
    db: &NativeShardedDatabase,
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

pub(crate) fn native_sharded_database_delete(
    db: &NativeShardedDatabase,
    bucket: u16,
    key: &[u8],
    column: u16,
    options: &ffi::NativeWriteOptions,
) -> BridgeResult<()> {
    db.db
        .delete_with_options(bucket, key, column, &to_write_options(options))
        .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_merge(
    db: &NativeShardedDatabase,
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
pub(crate) fn native_sharded_database_write_batch(
    db: &NativeShardedDatabase,
    batch: Box<NativeWriteBatch>,
    await_durable: bool,
) -> BridgeResult<()> {
    db.db
        .write_batch_with_options(
            batch.batch,
            &WriteOptions::default().with_await_durable(await_durable),
        )
        .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_get(
    db: &NativeShardedDatabase,
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

pub(crate) fn native_sharded_database_get_column_into(
    db: &NativeShardedDatabase,
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

pub(crate) fn native_sharded_database_switch_to_snapshot(
    db: &mut NativeShardedDatabase,
    snapshot_id: u64,
) -> BridgeResult<()> {
    let db = Arc::get_mut(&mut db.db).ok_or_else(|| {
        "CB_INVALID_STATE: SwitchToSnapshot requires exclusive ownership; release all scan cursors and schema builders first".to_string()
    })?;
    db.switch_to_snapshot(snapshot_id)
        .map_err(format_cobble_error)
}

fn expand_storage_mode(mode: u8) -> BridgeResult<cobble_binding::ExpandStorageMode> {
    match mode {
        0 => Ok(cobble_binding::ExpandStorageMode::AdoptAsync),
        1 => Ok(cobble_binding::ExpandStorageMode::ReferencePersistent),
        2 => Ok(cobble_binding::ExpandStorageMode::ReferencePersistentWithCache),
        _ => Err(input_error("unknown expand storage mode")),
    }
}

pub(crate) fn native_sharded_database_expand_bucket(
    db: &NativeShardedDatabase,
    source_db_id: &str,
    has_snapshot_id: bool,
    snapshot_id: u64,
    has_ranges: bool,
    ranges: Vec<ffi::NativeRange>,
    storage_mode: u8,
) -> BridgeResult<u64> {
    let ranges = has_ranges
        .then(|| bucket_ranges_from_values(ranges, "expand"))
        .transpose()?;
    db.db
        .expand_bucket_with_storage_mode(
            source_db_id,
            has_snapshot_id.then_some(snapshot_id),
            ranges,
            expand_storage_mode(storage_mode)?,
        )
        .map_err(format_cobble_error)
}

fn bucket_ranges_from_values(
    ranges: Vec<ffi::NativeRange>,
    operation: &str,
) -> BridgeResult<Vec<RangeInclusive<u16>>> {
    if ranges.is_empty() {
        return Err(input_error(&format!(
            "{operation} ranges must not be empty"
        )));
    }
    ranges
        .into_iter()
        .map(|range| {
            if range.first > range.last {
                return Err(input_error(&format!(
                    "{operation} range {}..={} is reversed",
                    range.first, range.last
                )));
            }
            Ok(range.first..=range.last)
        })
        .collect()
}

pub(crate) fn native_sharded_database_wait_for_expand_adoption(
    db: &NativeShardedDatabase,
    timeout_millis: i64,
) -> BridgeResult<()> {
    let timeout_millis = u64::try_from(timeout_millis)
        .map_err(|_| input_error("expand adoption timeout must not be negative"))?;
    db.db
        .wait_for_expand_adoption(Duration::from_millis(timeout_millis))
        .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_shrink_bucket(
    db: &NativeShardedDatabase,
    ranges: Vec<ffi::NativeRange>,
) -> BridgeResult<u64> {
    db.db
        .shrink_bucket(bucket_ranges_from_values(ranges, "shrink")?)
        .map_err(format_cobble_error)
}
