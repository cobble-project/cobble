use cobble_binding::structured::StructuredSingleDb;
use std::sync::Arc;

use crate::structured_bridge::ffi;

use super::conversion::*;
use super::{
    BridgeResult, NativeStructuredReadOptions, NativeStructuredRow, NativeStructuredSingleDb,
};

pub(crate) fn native_structured_single_db_open(
    config_json: &str,
) -> BridgeResult<Box<NativeStructuredSingleDb>> {
    opendal::install_default();
    StructuredSingleDb::open(parse_config_json(config_json)?)
        .map(|db| Box::new(NativeStructuredSingleDb { db: Arc::new(db) }))
        .map_err(format_error)
}

pub(crate) fn native_structured_single_db_open_file(
    config_path: &str,
) -> BridgeResult<Box<NativeStructuredSingleDb>> {
    opendal::install_default();
    StructuredSingleDb::open(parse_config_file(config_path)?)
        .map(|db| Box::new(NativeStructuredSingleDb { db: Arc::new(db) }))
        .map_err(format_error)
}

pub(crate) fn native_structured_single_db_put_bytes(
    db: &NativeStructuredSingleDb,
    bucket: u16,
    key: &[u8],
    column: u16,
    value: &[u8],
    options: &ffi::NativeWriteOptions,
) -> BridgeResult<()> {
    cobble_binding::structured::ffi::single_db_put_borrowed_bytes_with_options(
        &db.db,
        bucket,
        key,
        column,
        value,
        &write_options(options)?,
    )
    .map_err(format_error)
}
pub(crate) fn native_structured_single_db_merge_bytes(
    db: &NativeStructuredSingleDb,
    bucket: u16,
    key: &[u8],
    column: u16,
    value: &[u8],
    options: &ffi::NativeWriteOptions,
) -> BridgeResult<()> {
    cobble_binding::structured::ffi::single_db_merge_borrowed_bytes_with_options(
        &db.db,
        bucket,
        key,
        column,
        value,
        &write_options(options)?,
    )
    .map_err(format_error)
}
pub(crate) fn native_structured_single_db_put_list(
    db: &NativeStructuredSingleDb,
    bucket: u16,
    key: &[u8],
    column: u16,
    values: Vec<ffi::NativeBytesDescriptor>,
    options: &ffi::NativeWriteOptions,
) -> BridgeResult<()> {
    let elements = borrowed_elements(&values)?;
    cobble_binding::structured::ffi::single_db_put_borrowed_list_with_options(
        &db.db,
        bucket,
        key,
        column,
        &elements,
        &write_options(options)?,
    )
    .map_err(format_error)
}
pub(crate) fn native_structured_single_db_merge_list(
    db: &NativeStructuredSingleDb,
    bucket: u16,
    key: &[u8],
    column: u16,
    values: Vec<ffi::NativeBytesDescriptor>,
    options: &ffi::NativeWriteOptions,
) -> BridgeResult<()> {
    let elements = borrowed_elements(&values)?;
    cobble_binding::structured::ffi::single_db_merge_borrowed_list_with_options(
        &db.db,
        bucket,
        key,
        column,
        &elements,
        &write_options(options)?,
    )
    .map_err(format_error)
}
pub(crate) fn native_structured_single_db_delete(
    db: &NativeStructuredSingleDb,
    bucket: u16,
    key: &[u8],
    column: u16,
    options: &ffi::NativeWriteOptions,
) -> BridgeResult<()> {
    db.db
        .delete_with_options(bucket, key, column, &write_options(options)?)
        .map_err(format_error)
}
pub(crate) fn native_structured_single_db_get(
    db: &NativeStructuredSingleDb,
    bucket: u16,
    key: &[u8],
    options: &NativeStructuredReadOptions,
) -> BridgeResult<Box<NativeStructuredRow>> {
    db.db
        .get_with_options(bucket, key, &options.options)
        .map(|columns| Box::new(NativeStructuredRow { columns }))
        .map_err(format_error)
}
