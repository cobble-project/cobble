use crate::db::db_arc_from_handle_or_throw;
use crate::metrics::metrics_json;
use crate::read_only_db::{
    read_only_db_arc_from_handle_or_throw, read_only_db_from_handle_or_throw,
};
use crate::read_options::read_options_from_handle_or_throw;
use crate::table_direct::{encode_direct_get, take_direct_overflow};
use crate::util::{
    decode_java_bytes, decode_java_string, decode_multi_get_keys, decode_optional_java_bytes,
    decode_u16, decode_u64_from_jlong, throw_illegal_argument, throw_illegal_state,
    to_java_optional_bytes_2d, to_java_optional_bytes_3d, to_java_string_or_throw,
};
use crate::write_options::write_options_from_handle_or_throw;
use crate::{
    read_options::bind_to_table_schema as bind_read_options,
    scan::bind_to_table_schema as bind_scan_options,
};
use cobble_binding::ColumnFamilyOptions;
use cobble_table::{ReadOnlyTable, Table, TableError, TableSchema};
use jni::JNIEnv;
use jni::objects::{
    JByteArray, JByteBuffer, JClass, JIntArray, JObject, JObjectArray, JString, JValue,
};
use jni::sys::{jint, jlong, jobject, jstring};
use std::sync::Arc;

struct TableReadHandle {
    access: cobble_table::ffi::RawTableAccess,
}

pub(crate) fn table_handle_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static cobble_table::ffi::TableHandle> {
    if native_handle == 0 {
        throw_illegal_state(env, "table is closed".to_string());
        return None;
    }
    // SAFETY: Java owns this Box until disposeNative.
    Some(unsafe { &*(native_handle as *const cobble_table::ffi::TableHandle) })
}

pub(crate) fn table_handle_from_handle_mut_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static mut cobble_table::ffi::TableHandle> {
    if native_handle == 0 {
        throw_illegal_state(env, "table is closed".to_string());
        return None;
    }
    // SAFETY: refresh is serialized by Java Table.
    Some(unsafe { &mut *(native_handle as *mut cobble_table::ffi::TableHandle) })
}

fn read_handle_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static TableReadHandle> {
    if native_handle == 0 {
        throw_illegal_state(env, "table read view is closed".to_string());
        return None;
    }
    // SAFETY: Java owns this Box until disposeNative.
    Some(unsafe { &*(native_handle as *const TableReadHandle) })
}

pub(crate) fn table_to_java(env: &mut JNIEnv, table: Table) -> jobject {
    let table = cobble_table::ffi::TableHandle::new(table);
    let response = table_open_response(
        env,
        table.total_buckets(),
        table.schema(),
        table.schema_binding(),
    );
    if response.is_null() {
        return std::ptr::null_mut();
    }
    let response = unsafe { JObject::from_raw(response as jobject) };
    let name = match env.new_string(table.name()) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(env, error.to_string());
            return std::ptr::null_mut();
        }
    };
    let handle = Box::into_raw(Box::new(table)) as jlong;
    match env.call_static_method(
        "io/cobble/table/Table",
        "fromNativeHandle",
        "(JLjava/lang/String;Ljava/lang/String;)Lio/cobble/table/Table;",
        &[
            JValue::Long(handle),
            JValue::Object(&name),
            JValue::Object(&response),
        ],
    ) {
        Ok(value) => value.l().map_or_else(
            |error| {
                // SAFETY: Java did not receive this handle.
                drop(unsafe { Box::from_raw(handle as *mut cobble_table::ffi::TableHandle) });
                throw_illegal_state(env, error.to_string());
                std::ptr::null_mut()
            },
            |value| value.into_raw(),
        ),
        Err(_) => {
            // SAFETY: Java did not receive this handle.
            drop(unsafe { Box::from_raw(handle as *mut cobble_table::ffi::TableHandle) });
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_createNative(
    mut env: JNIEnv,
    _class: JClass,
    db_handle: jlong,
    name: JString,
    schema_json: JString,
) -> jobject {
    let Some(db) = db_arc_from_handle_or_throw(&mut env, db_handle) else {
        return std::ptr::null_mut();
    };
    let name = match decode_java_string(&mut env, name) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    let schema_json = match decode_java_string(&mut env, schema_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    let schema = match serde_json::from_str::<TableSchema>(&schema_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, format!("invalid table schema JSON: {error}"));
            return std::ptr::null_mut();
        }
    };
    let table = match Table::create(Arc::clone(db), name, schema) {
        Ok(value) => value,
        Err(error) => {
            throw_table_error(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    table_to_java(&mut env, table)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_openNative(
    mut env: JNIEnv,
    _class: JClass,
    db_handle: jlong,
    name: JString,
) -> jobject {
    let Some(db) = db_arc_from_handle_or_throw(&mut env, db_handle) else {
        return std::ptr::null_mut();
    };
    let name = match decode_java_string(&mut env, name) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    let table = match Table::open(Arc::clone(db), name) {
        Ok(value) => value,
        Err(error) => {
            throw_table_error(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    table_to_java(&mut env, table)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_ReadOnlyTable_openNative(
    mut env: JNIEnv,
    _class: JClass,
    db_handle: jlong,
    name: JString,
) -> jstring {
    let Some(db) = read_only_db_arc_from_handle_or_throw(&mut env, db_handle) else {
        return std::ptr::null_mut();
    };
    let name = match decode_java_string(&mut env, name) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    let table = match ReadOnlyTable::open(Arc::clone(db), name) {
        Ok(value) => value,
        Err(error) => {
            throw_table_error(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    table_open_response(
        &mut env,
        db.total_buckets(),
        table.schema(),
        cobble_table::ffi::read_only_table_schema_binding(&table),
    )
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_bindOptionsNative(
    mut env: JNIEnv,
    _class: JClass,
    read_options_handle: jlong,
    scan_options_handle: jlong,
    column_family_options_json: JString,
    physical_columns: jint,
) {
    let options_json = match decode_java_string(&mut env, column_family_options_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(&mut env, error);
            return;
        }
    };
    let options = match serde_json::from_str::<ColumnFamilyOptions>(&options_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(
                &mut env,
                format!("invalid captured table column-family options: {error}"),
            );
            return;
        }
    };
    let physical_columns = match usize::try_from(physical_columns) {
        Ok(value) if value > 0 => value,
        _ => {
            throw_illegal_state(
                &mut env,
                "invalid captured table physical column count".to_string(),
            );
            return;
        }
    };
    if !bind_read_options(&mut env, read_options_handle, &options, physical_columns) {
        return;
    }
    let _ = bind_scan_options(&mut env, scan_options_handle, &options, physical_columns);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_disposeNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(&mut env, "table is already closed".to_string());
        return;
    }
    // SAFETY: Java calls this once after invalidating its handle.
    drop(unsafe { Box::from_raw(native_handle as *mut cobble_table::ffi::TableHandle) });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_refreshNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    let Some(table) = table_handle_from_handle_mut_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    if let Err(error) = table.refresh() {
        throw_table_error(&mut env, error);
        return std::ptr::null_mut();
    }
    table_open_response(
        &mut env,
        table.total_buckets(),
        table.schema(),
        table.schema_binding(),
    )
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_metricsJson(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    let Some(table) = table_handle_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    to_java_string_or_throw(&mut env, metrics_json(table.metrics()))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_asyncSnapshotNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    snapshot_future_json: JObject,
) -> jlong {
    let Some(table) = table_handle_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    if snapshot_future_json.is_null() {
        throw_illegal_argument(&mut env, "snapshotFutureJson must not be null".to_string());
        return 0;
    }
    let future = match env.new_global_ref(snapshot_future_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            return 0;
        }
    };
    let vm = match env.get_java_vm() {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            return 0;
        }
    };
    match table.snapshot_with_callback(move |result| {
        crate::db::complete_snapshot_json_future(
            &vm,
            &future,
            result.map(|snapshot| crate::db::shard_snapshot_json(&snapshot)),
        );
    }) {
        Ok(snapshot_id) => snapshot_id as jlong,
        Err(error) => {
            throw_table_error(&mut env, error);
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_getShardSnapshotJsonNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    snapshot_id: jlong,
) -> jstring {
    let Some(table) = table_handle_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    match table.shard_snapshot_metadata(snapshot_id) {
        Ok(snapshot) => {
            to_java_string_or_throw(&mut env, crate::db::shard_snapshot_json(&snapshot))
        }
        Err(error) => {
            throw_table_error(&mut env, error);
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_getNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
) -> jobject {
    let Some(table) = table_handle_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    let key = match decode_java_bytes(&mut env, key) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    match table.access().get(bucket, &key) {
        Ok(Some(columns)) => {
            to_java_optional_bytes_2d(&mut env, &columns).unwrap_or_else(|error| {
                throw_illegal_state(&mut env, error);
                std::ptr::null_mut()
            })
        }
        Ok(None) => std::ptr::null_mut(),
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_ReadOnlyTable_getEncodedDirectNative(
    mut env: JNIEnv,
    _class: JClass,
    db_handle: jlong,
    bucket: jint,
    buffer: JByteBuffer,
    key_length: jint,
    read_options_handle: jlong,
) -> jint {
    let Some(db) = read_only_db_from_handle_or_throw(&mut env, db_handle) else {
        return 0;
    };
    let Some(options) = read_options_from_handle_or_throw(&mut env, read_options_handle) else {
        return 0;
    };
    encode_direct_get(&mut env, bucket, buffer, key_length, |bucket, key| {
        db.get_with_options(bucket, key, options.read_options())
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_ReadOnlyTable_takeDirectOverflowNative(
    mut env: JNIEnv,
    _class: JClass,
) -> jobject {
    take_direct_overflow(&mut env)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_multiGetNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    native_handle: jlong,
    buckets: JIntArray<'local>,
    keys: JObjectArray<'local>,
) -> jobject {
    let Some(table) = table_handle_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let keys = match decode_multi_get_keys(&mut env, &buckets, &keys) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    let borrowed = keys
        .iter()
        .map(|(bucket, key)| (*bucket, key.as_slice()))
        .collect::<Vec<_>>();
    match table.access().multi_get(&borrowed) {
        Ok(rows) => to_java_optional_bytes_3d(&mut env, &rows).unwrap_or_else(|error| {
            throw_illegal_state(&mut env, error);
            std::ptr::null_mut()
        }),
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_createReadViewNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    native_handle: jlong,
    field_names: JObjectArray<'local>,
) -> jlong {
    let Some(table) = table_handle_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let access = if field_names.is_null() {
        Ok(table.access().clone())
    } else {
        let field_names = match decode_string_array(&mut env, &field_names) {
            Ok(value) => value,
            Err(error) => return throw_argument_and_zero(&mut env, error),
        };
        table.projection(&field_names)
    };
    match access {
        Ok(access) => Box::into_raw(Box::new(TableReadHandle { access })) as jlong,
        Err(error) => throw_table_error_and_zero(&mut env, error),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReadView_cloneNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jlong {
    let Some(view) = read_handle_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    Box::into_raw(Box::new(TableReadHandle {
        access: view.access.clone(),
    })) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReadView_disposeInternal(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(&mut env, "table read view is already closed".to_string());
        return;
    }
    // SAFETY: Java calls this once after invalidating its handle.
    drop(unsafe { Box::from_raw(native_handle as *mut TableReadHandle) });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_putNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    row_payload: JByteArray,
    write_options_handle: jlong,
) {
    let Some(table) = table_handle_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let key = match decode_java_bytes(&mut env, key) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let payload = match decode_java_bytes(&mut env, row_payload) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    put_raw(
        &mut env,
        table,
        bucket,
        &key,
        &payload,
        write_options_handle,
    );
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_putDirectNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key_buffer: JByteBuffer,
    key_offset: jint,
    key_length: jint,
    row_buffer: JByteBuffer,
    row_offset: jint,
    row_length: jint,
    write_options_handle: jlong,
) {
    let Some(table) = table_handle_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let key = match direct_range(&mut env, &key_buffer, key_offset, key_length, "key") {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let payload = match direct_range(&mut env, &row_buffer, row_offset, row_length, "rowPayload") {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    put_raw(&mut env, table, bucket, key, payload, write_options_handle);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_deleteTableNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
) {
    let Some(table) = table_handle_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let key = match decode_java_bytes(&mut env, key) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    if let Err(error) = table.delete(bucket, &key) {
        throw_illegal_state(&mut env, error.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_deleteBatchTableNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    native_handle: jlong,
    buckets: JIntArray<'local>,
    keys: JObjectArray<'local>,
) {
    let Some(table) = table_handle_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let keys = match decode_multi_get_keys(&mut env, &buckets, &keys) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let borrowed = keys
        .iter()
        .map(|(bucket, key)| (*bucket, key.as_slice()))
        .collect::<Vec<_>>();
    if let Err(error) = table.delete_batch(&borrowed) {
        throw_illegal_state(&mut env, error.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReadView_get(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
) -> jobject {
    let Some(view) = read_handle_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    let key = match decode_java_bytes(&mut env, key) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    match view.access.get(bucket, &key) {
        Ok(Some(columns)) => {
            to_java_optional_bytes_2d(&mut env, &columns).unwrap_or_else(|error| {
                throw_illegal_state(&mut env, error);
                std::ptr::null_mut()
            })
        }
        Ok(None) => std::ptr::null_mut(),
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReadView_multiGet<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    native_handle: jlong,
    buckets: JIntArray<'local>,
    keys: JObjectArray<'local>,
) -> jobject {
    let Some(view) = read_handle_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let keys = match decode_multi_get_keys(&mut env, &buckets, &keys) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    let borrowed = keys
        .iter()
        .map(|(bucket, key)| (*bucket, key.as_slice()))
        .collect::<Vec<_>>();
    match view.access.multi_get(&borrowed) {
        Ok(rows) => to_java_optional_bytes_3d(&mut env, &rows).unwrap_or_else(|error| {
            throw_illegal_state(&mut env, error);
            std::ptr::null_mut()
        }),
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReadView_openScanCursor(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    start: JByteArray,
    end: JByteArray,
) -> jobject {
    let Some(view) = read_handle_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    let start = match decode_optional_java_bytes(&mut env, start) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    let end = match decode_optional_java_bytes(&mut env, end) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    match view.access.scan(bucket, start.as_deref(), end.as_deref()) {
        Ok(iter) => new_direct_scan_cursor(&mut env, iter),
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            std::ptr::null_mut()
        }
    }
}

pub(crate) fn table_open_response(
    env: &mut JNIEnv,
    total_buckets: u32,
    schema: &TableSchema,
    binding: cobble_table::ffi::TableSchemaBinding,
) -> jstring {
    let response = match serde_json::to_string(&serde_json::json!({
        "schema": schema,
        "total_buckets": total_buckets,
        "column_family_options": binding.column_family_options(),
        "physical_columns": binding.physical_columns(),
    })) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(env, error.to_string());
            return std::ptr::null_mut();
        }
    };
    to_java_string_or_throw(env, response)
}

fn put_raw(
    env: &mut JNIEnv,
    table: &cobble_table::ffi::TableHandle,
    bucket: u16,
    key: &[u8],
    payload: &[u8],
    write_options_handle: jlong,
) {
    let columns = match decode_row_payload(payload) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(env, error);
            return;
        }
    };
    let result = if write_options_handle == 0 {
        table.put_columns(bucket, key, &columns)
    } else {
        let Some(options) = write_options_from_handle_or_throw(env, write_options_handle) else {
            return;
        };
        table.put_columns_with_options(bucket, key, &columns, options.write_options())
    };
    if let Err(error) = result {
        throw_illegal_state(env, error.to_string());
    }
}

fn new_direct_scan_cursor(env: &mut JNIEnv, iter: cobble_binding::DbIterator) -> jobject {
    let handle = Box::into_raw(Box::new(crate::scan::ScanCursorHandle::from_static_iter(
        iter,
    ))) as jlong;
    match env.new_object(
        "io/cobble/DirectScanCursor",
        "(J)V",
        &[JValue::Long(handle)],
    ) {
        Ok(cursor) => cursor.into_raw(),
        Err(error) => {
            // SAFETY: Java did not receive this cursor handle.
            drop(unsafe { Box::from_raw(handle as *mut crate::scan::ScanCursorHandle) });
            throw_illegal_state(env, error.to_string());
            std::ptr::null_mut()
        }
    }
}

fn decode_row_payload(payload: &[u8]) -> Result<Vec<&[u8]>, String> {
    if payload.len() < 4 {
        return Err("table row payload is missing its column count".to_string());
    }
    let count = u32::from_be_bytes(payload[..4].try_into().unwrap()) as usize;
    if count > (payload.len() - 4) / 5 {
        return Err("table row column count exceeds its payload".to_string());
    }
    let mut columns = Vec::with_capacity(count);
    let mut offset = 4usize;
    for column in 0..count {
        if payload.len() - offset < 5 {
            return Err(format!("table row column {column} is truncated"));
        }
        if payload[offset] != 1 {
            return Err(format!(
                "table row column {column} must contain a complete encoded value"
            ));
        }
        offset += 1;
        let length_end = offset + 4;
        let length = u32::from_be_bytes(payload[offset..length_end].try_into().unwrap()) as usize;
        offset = length_end;
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= payload.len())
            .ok_or_else(|| format!("table row column {column} exceeds its payload"))?;
        columns.push(&payload[offset..end]);
        offset = end;
    }
    if offset != payload.len() {
        return Err("table row payload has trailing bytes".to_string());
    }
    Ok(columns)
}

fn direct_range<'a>(
    env: &mut JNIEnv,
    buffer: &'a JByteBuffer,
    offset: jint,
    length: jint,
    name: &str,
) -> Result<&'a [u8], String> {
    let offset = usize::try_from(offset).map_err(|_| format!("{name}Offset must be >= 0"))?;
    let length = usize::try_from(length).map_err(|_| format!("{name}Length must be >= 0"))?;
    let capacity = env
        .get_direct_buffer_capacity(buffer)
        .map_err(|_| format!("{name} must be a direct ByteBuffer"))?;
    let end = offset
        .checked_add(length)
        .filter(|end| *end <= capacity)
        .ok_or_else(|| format!("{name} range exceeds its buffer"))?;
    let address = env
        .get_direct_buffer_address(buffer)
        .map_err(|_| format!("{name} must be a direct ByteBuffer"))?;
    Ok(unsafe { std::slice::from_raw_parts(address.add(offset), end - offset) })
}

fn throw_table_error(env: &mut JNIEnv, error: TableError) {
    match error {
        TableError::InvalidSchema(message) | TableError::Codec(message) => {
            throw_illegal_argument(env, message)
        }
        other => throw_illegal_state(env, other.to_string()),
    }
}

fn throw_argument_and_null<T: Into<String>>(env: &mut JNIEnv, error: T) -> jobject {
    throw_illegal_argument(env, error.into());
    std::ptr::null_mut()
}

fn throw_argument_and_zero<T: Into<String>>(env: &mut JNIEnv, error: T) -> jlong {
    throw_illegal_argument(env, error.into());
    0
}

fn throw_table_error_and_zero(env: &mut JNIEnv, error: TableError) -> jlong {
    throw_table_error(env, error);
    0
}

fn decode_string_array(env: &mut JNIEnv, values: &JObjectArray) -> Result<Vec<String>, String> {
    let length = env
        .get_array_length(values)
        .map_err(|error| error.to_string())?;
    let mut decoded = Vec::with_capacity(length as usize);
    for index in 0..length {
        let value = env
            .get_object_array_element(values, index)
            .map_err(|error| error.to_string())?;
        if value.is_null() {
            return Err(format!("fieldNames[{index}] must not be null"));
        }
        decoded.push(decode_java_string(env, JString::from(value))?);
    }
    Ok(decoded)
}
