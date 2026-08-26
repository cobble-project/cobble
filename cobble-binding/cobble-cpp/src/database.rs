use bytes::Bytes;
use cobble_binding::{Config, RecoveryMode, SingleDb, WriteOptions};
use std::sync::Arc;

use crate::{
    BridgeResult,
    encoding::{STATUS_BUFFER_TOO_SMALL, STATUS_NOT_FOUND, STATUS_OK, buffer_result},
    error::format_cobble_error,
    ffi,
    options::{checked_u64, to_read_options, to_single_column_read_options, to_write_options},
    write_batch::NativeWriteBatch,
};

pub(crate) struct NativeDatabase {
    pub(crate) db: Arc<SingleDb>,
}

pub(crate) struct NativeRow {
    pub(crate) columns: Option<Vec<Option<Bytes>>>,
}

pub(crate) fn native_database_open(config_json: &str) -> BridgeResult<Box<NativeDatabase>> {
    open_database(parse_config_json(config_json)?)
}

pub(crate) fn native_database_open_file(config_path: &str) -> BridgeResult<Box<NativeDatabase>> {
    open_database(Config::from_path(config_path).map_err(format_cobble_error)?)
}

pub(crate) fn native_database_resume(
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

pub(crate) fn native_database_resume_file(
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

pub(crate) fn native_database_close(db: &NativeDatabase) -> BridgeResult<()> {
    db.db.close().map_err(format_cobble_error)
}

pub(crate) fn native_database_version() -> String {
    cobble_binding::ffi::build_version_string().to_owned()
}

pub(crate) fn native_database_put(
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

pub(crate) fn native_database_delete(
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

pub(crate) fn native_database_merge(
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
pub(crate) fn native_database_write_batch(
    db: &NativeDatabase,
    batch: Box<NativeWriteBatch>,
    await_durable: bool,
) -> BridgeResult<()> {
    let options = WriteOptions::default().with_await_durable(await_durable);
    db.db
        .write_batch_with_options(batch.batch, &options)
        .map_err(format_cobble_error)
}

pub(crate) fn native_database_get(
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

pub(crate) fn native_database_get_column_into(
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

pub(crate) fn native_database_snapshot(db: &NativeDatabase) -> BridgeResult<u64> {
    db.db.snapshot().map_err(format_cobble_error)
}
pub(crate) fn native_database_retain_snapshot(
    db: &NativeDatabase,
    snapshot_id: u64,
) -> BridgeResult<bool> {
    db.db
        .retain_snapshot(snapshot_id)
        .map_err(format_cobble_error)
}
pub(crate) fn native_database_expire_snapshot(
    db: &NativeDatabase,
    snapshot_id: u64,
) -> BridgeResult<bool> {
    db.db
        .expire_snapshot(snapshot_id)
        .map_err(format_cobble_error)
}
pub(crate) fn native_database_list_snapshots(db: &NativeDatabase) -> BridgeResult<Vec<u64>> {
    db.db
        .list_snapshots()
        .map(|snapshots| snapshots.into_iter().map(|snapshot| snapshot.id).collect())
        .map_err(format_cobble_error)
}
pub(crate) fn native_database_snapshot_manifest_json(
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
pub(crate) fn native_database_set_time(db: &NativeDatabase, unix_seconds: u32) {
    db.db.set_time(unix_seconds);
}

pub(crate) fn open_database(config: Config) -> BridgeResult<Box<NativeDatabase>> {
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
        _ => Err(crate::error::input_error(
            "recovery mode must be 0 (snapshot-only) or 1 (latest-with-wal)",
        )),
    }
}
