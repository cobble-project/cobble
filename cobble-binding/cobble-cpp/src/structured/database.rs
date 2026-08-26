use cobble_binding::structured::StructuredDb;

use crate::structured_bridge::ffi;

use super::conversion::*;
use super::{BridgeResult, NativeStructuredDb, NativeStructuredReadOptions, NativeStructuredRow};

pub(crate) fn native_structured_db_open(
    config_json: &str,
) -> BridgeResult<Box<NativeStructuredDb>> {
    let config = parse_config_json(config_json)?;
    let bucket_ranges = full_range(&config)?;
    opendal::install_default();
    StructuredDb::open(config, bucket_ranges)
        .map(|db| Box::new(NativeStructuredDb { db }))
        .map_err(format_error)
}

pub(crate) fn native_structured_db_open_ranges(
    config_json: &str,
    values: Vec<ffi::NativeBucketRange>,
) -> BridgeResult<Box<NativeStructuredDb>> {
    let config = parse_config_json(config_json)?;
    let bucket_ranges = ranges(&config, values)?;
    opendal::install_default();
    StructuredDb::open(config, bucket_ranges)
        .map(|db| Box::new(NativeStructuredDb { db }))
        .map_err(format_error)
}

pub(crate) fn native_structured_db_open_file(
    config_path: &str,
) -> BridgeResult<Box<NativeStructuredDb>> {
    let config = parse_config_file(config_path)?;
    let bucket_ranges = full_range(&config)?;
    opendal::install_default();
    StructuredDb::open(config, bucket_ranges)
        .map(|db| Box::new(NativeStructuredDb { db }))
        .map_err(format_error)
}

pub(crate) fn native_structured_db_open_file_ranges(
    config_path: &str,
    values: Vec<ffi::NativeBucketRange>,
) -> BridgeResult<Box<NativeStructuredDb>> {
    let config = parse_config_file(config_path)?;
    let bucket_ranges = ranges(&config, values)?;
    opendal::install_default();
    StructuredDb::open(config, bucket_ranges)
        .map(|db| Box::new(NativeStructuredDb { db }))
        .map_err(format_error)
}

fn boxed_db(result: cobble_binding::Result<StructuredDb>) -> BridgeResult<Box<NativeStructuredDb>> {
    result
        .map(|db| Box::new(NativeStructuredDb { db }))
        .map_err(format_error)
}

pub(crate) fn native_structured_db_open_from_snapshot(
    config_json: &str,
    snapshot_id: u64,
    db_id: &str,
    mode: u8,
) -> BridgeResult<Box<NativeStructuredDb>> {
    opendal::install_default();
    boxed_db(StructuredDb::open_from_snapshot_with_recovery_mode(
        parse_config_json(config_json)?,
        snapshot_id,
        db_id,
        recovery_mode(mode)?,
    ))
}

pub(crate) fn native_structured_db_open_from_snapshot_file(
    config_path: &str,
    snapshot_id: u64,
    db_id: &str,
    mode: u8,
) -> BridgeResult<Box<NativeStructuredDb>> {
    opendal::install_default();
    boxed_db(StructuredDb::open_from_snapshot_with_recovery_mode(
        parse_config_file(config_path)?,
        snapshot_id,
        db_id,
        recovery_mode(mode)?,
    ))
}

pub(crate) fn native_structured_db_restore_new(
    config_json: &str,
    snapshot_id: u64,
    db_id: &str,
) -> BridgeResult<Box<NativeStructuredDb>> {
    opendal::install_default();
    boxed_db(StructuredDb::open_new_with_snapshot(
        parse_config_json(config_json)?,
        snapshot_id,
        db_id,
    ))
}

pub(crate) fn native_structured_db_restore_new_file(
    config_path: &str,
    snapshot_id: u64,
    db_id: &str,
) -> BridgeResult<Box<NativeStructuredDb>> {
    opendal::install_default();
    boxed_db(StructuredDb::open_new_with_snapshot(
        parse_config_file(config_path)?,
        snapshot_id,
        db_id,
    ))
}

pub(crate) fn native_structured_db_restore_new_from_manifest(
    config_json: &str,
    manifest_path: &str,
) -> BridgeResult<Box<NativeStructuredDb>> {
    opendal::install_default();
    boxed_db(StructuredDb::open_new_with_manifest_path(
        parse_config_json(config_json)?,
        manifest_path,
    ))
}

pub(crate) fn native_structured_db_restore_new_from_manifest_file(
    config_path: &str,
    manifest_path: &str,
) -> BridgeResult<Box<NativeStructuredDb>> {
    opendal::install_default();
    boxed_db(StructuredDb::open_new_with_manifest_path(
        parse_config_file(config_path)?,
        manifest_path,
    ))
}

pub(crate) fn native_structured_db_resume(
    config_json: &str,
    db_id: &str,
    mode: u8,
) -> BridgeResult<Box<NativeStructuredDb>> {
    opendal::install_default();
    boxed_db(StructuredDb::resume_with_recovery_mode(
        parse_config_json(config_json)?,
        db_id,
        recovery_mode(mode)?,
    ))
}

pub(crate) fn native_structured_db_resume_file(
    config_path: &str,
    db_id: &str,
    mode: u8,
) -> BridgeResult<Box<NativeStructuredDb>> {
    opendal::install_default();
    boxed_db(StructuredDb::resume_with_recovery_mode(
        parse_config_file(config_path)?,
        db_id,
        recovery_mode(mode)?,
    ))
}

pub(crate) fn native_structured_db_resume_from_snapshot(
    config_json: &str,
    snapshot_id: u64,
    db_id: &str,
    mode: u8,
) -> BridgeResult<Box<NativeStructuredDb>> {
    opendal::install_default();
    boxed_db(StructuredDb::resume_from_snapshot_with_recovery_mode(
        parse_config_json(config_json)?,
        snapshot_id,
        db_id,
        recovery_mode(mode)?,
    ))
}

pub(crate) fn native_structured_db_resume_from_snapshot_file(
    config_path: &str,
    snapshot_id: u64,
    db_id: &str,
    mode: u8,
) -> BridgeResult<Box<NativeStructuredDb>> {
    opendal::install_default();
    boxed_db(StructuredDb::resume_from_snapshot_with_recovery_mode(
        parse_config_file(config_path)?,
        snapshot_id,
        db_id,
        recovery_mode(mode)?,
    ))
}

pub(crate) fn native_structured_db_id(db: &NativeStructuredDb) -> &str {
    db.db.id()
}

pub(crate) fn native_structured_db_put_bytes(
    db: &NativeStructuredDb,
    bucket: u16,
    key: &[u8],
    column: u16,
    value: &[u8],
    options: &ffi::NativeWriteOptions,
) -> BridgeResult<()> {
    cobble_binding::structured::ffi::db_put_borrowed_bytes_with_options(
        &db.db,
        bucket,
        key,
        column,
        value,
        &write_options(options)?,
    )
    .map_err(format_error)
}
pub(crate) fn native_structured_db_merge_bytes(
    db: &NativeStructuredDb,
    bucket: u16,
    key: &[u8],
    column: u16,
    value: &[u8],
    options: &ffi::NativeWriteOptions,
) -> BridgeResult<()> {
    cobble_binding::structured::ffi::db_merge_borrowed_bytes_with_options(
        &db.db,
        bucket,
        key,
        column,
        value,
        &write_options(options)?,
    )
    .map_err(format_error)
}
pub(crate) fn native_structured_db_put_list(
    db: &NativeStructuredDb,
    bucket: u16,
    key: &[u8],
    column: u16,
    values: Vec<ffi::NativeBytesDescriptor>,
    options: &ffi::NativeWriteOptions,
) -> BridgeResult<()> {
    let elements = borrowed_elements(&values)?;
    cobble_binding::structured::ffi::db_put_borrowed_list_with_options(
        &db.db,
        bucket,
        key,
        column,
        &elements,
        &write_options(options)?,
    )
    .map_err(format_error)
}
pub(crate) fn native_structured_db_merge_list(
    db: &NativeStructuredDb,
    bucket: u16,
    key: &[u8],
    column: u16,
    values: Vec<ffi::NativeBytesDescriptor>,
    options: &ffi::NativeWriteOptions,
) -> BridgeResult<()> {
    let elements = borrowed_elements(&values)?;
    cobble_binding::structured::ffi::db_merge_borrowed_list_with_options(
        &db.db,
        bucket,
        key,
        column,
        &elements,
        &write_options(options)?,
    )
    .map_err(format_error)
}
pub(crate) fn native_structured_db_delete(
    db: &NativeStructuredDb,
    bucket: u16,
    key: &[u8],
    column: u16,
    options: &ffi::NativeWriteOptions,
) -> BridgeResult<()> {
    db.db
        .delete_with_options(bucket, key, column, &write_options(options)?)
        .map_err(format_error)
}
pub(crate) fn native_structured_db_get(
    db: &NativeStructuredDb,
    bucket: u16,
    key: &[u8],
    options: &NativeStructuredReadOptions,
) -> BridgeResult<Box<NativeStructuredRow>> {
    db.db
        .get_with_options(bucket, key, &options.options)
        .map(|columns| Box::new(NativeStructuredRow { columns }))
        .map_err(format_error)
}
