use crate::read_options::read_options_from_handle_or_throw;
use crate::scan::{
    ScanCursorHandle, decode_scan_open_bounds_args, scan_options_from_handle_or_throw,
};
use crate::table::table_open_response;
use crate::util::{
    decode_java_bytes, decode_java_string, decode_multi_get_keys, decode_u16, parse_config_json,
    throw_illegal_argument, throw_illegal_state, to_java_optional_bytes_2d,
    to_java_optional_bytes_3d,
};
use cobble_binding::Config;
use cobble_table::{TableReader, TableReaderBuilder, ffi};
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JIntArray, JObject, JObjectArray, JString, JValue};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint, jlong, jobject, jstring};

struct TableReaderHandle(TableReader);

pub(crate) fn into_table_reader_handle(reader: TableReader) -> jlong {
    Box::into_raw(Box::new(TableReaderHandle(reader))) as jlong
}

fn reader_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static TableReaderHandle> {
    if native_handle == 0 {
        throw_illegal_state(env, "table reader handle is disposed".to_string());
        return None;
    }
    // SAFETY: the handle is created below and remains valid until disposal.
    Some(unsafe { &*(native_handle as *const TableReaderHandle) })
}

fn view_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static ffi::TableReaderView> {
    if native_handle == 0 {
        throw_illegal_state(env, "table reader view is disposed".to_string());
        return None;
    }
    // SAFETY: the handle is created below and remains valid until disposal.
    Some(unsafe { &*(native_handle as *const ffi::TableReaderView) })
}

fn open_reader(env: &mut JNIEnv, config: Config, name: String, snapshot_id: Option<u64>) -> jlong {
    let builder = TableReaderBuilder::new(config).table_name(name);
    let result = match snapshot_id {
        Some(snapshot_id) => builder.global_snapshot(snapshot_id).open(),
        None => builder.current_global_snapshot().open(),
    };
    match result {
        Ok(reader) => into_table_reader_handle(reader),
        Err(error) => {
            throw_illegal_state(env, error.to_string());
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReader_openCurrentNative(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    name: JString,
) -> jlong {
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return 0;
        }
    };
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return 0;
    };
    let name = match decode_java_string(&mut env, name) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return 0;
        }
    };
    open_reader(&mut env, config, name, None)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReader_openNative(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    name: JString,
    snapshot_id: jlong,
) -> jlong {
    let snapshot_id = match u64::try_from(snapshot_id) {
        Ok(value) => value,
        Err(_) => {
            throw_illegal_argument(&mut env, "snapshotId must be >= 0".to_string());
            return 0;
        }
    };
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return 0;
        }
    };
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return 0;
    };
    let name = match decode_java_string(&mut env, name) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return 0;
        }
    };
    open_reader(&mut env, config, name, Some(snapshot_id))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReader_disposeInternal(
    mut env: JNIEnv,
    _object: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "table reader handle is already disposed".to_string(),
        );
        return;
    }
    // SAFETY: this consumes the allocation created by open_reader.
    drop(unsafe { Box::from_raw(native_handle as *mut TableReaderHandle) });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReader_disposeHandleNative(
    _env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    if native_handle != 0 {
        // SAFETY: this consumes an unopened Java facade's native allocation.
        drop(unsafe { Box::from_raw(native_handle as *mut TableReaderHandle) });
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReader_refreshIntervalNanosNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jlong {
    let Some(reader) = reader_from_handle_or_throw(&mut env, native_handle) else {
        return -1;
    };
    reader
        .0
        .auto_refresh_interval_nanos()
        .map_or(-1, |nanos| nanos.min(i64::MAX as u64) as jlong)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReader_acquireViewNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    current_view_handle: jlong,
) -> jlong {
    let Some(reader) = reader_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let view = if current_view_handle == 0 {
        Some(ffi::acquire_table_reader_view(&reader.0))
    } else {
        let Some(current) = view_from_handle_or_throw(&mut env, current_view_handle) else {
            return 0;
        };
        ffi::acquire_table_reader_view_if_changed(&reader.0, current)
    };
    view.map_or(0, |view| Box::into_raw(Box::new(view)) as jlong)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReader_describeViewNative(
    mut env: JNIEnv,
    _class: JClass,
    view_handle: jlong,
) -> jstring {
    let Some(view) = view_from_handle_or_throw(&mut env, view_handle) else {
        return std::ptr::null_mut();
    };
    table_open_response(
        &mut env,
        view.total_buckets(),
        view.schema(),
        view.schema_binding(),
    )
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReader_refreshNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jboolean {
    let Some(reader) = reader_from_handle_or_throw(&mut env, native_handle) else {
        return JNI_FALSE;
    };
    match reader.0.refresh() {
        Ok(true) => JNI_TRUE,
        Ok(false) => JNI_FALSE,
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            JNI_FALSE
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReaderView_disposeInternal(
    mut env: JNIEnv,
    _class: JClass,
    view_handle: jlong,
) {
    if view_handle == 0 {
        throw_illegal_state(
            &mut env,
            "table reader view is already disposed".to_string(),
        );
        return;
    }
    // SAFETY: this consumes the allocation created by acquireViewNative.
    drop(unsafe { Box::from_raw(view_handle as *mut ffi::TableReaderView) });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReader_cloneViewNative(
    mut env: JNIEnv,
    _class: JClass,
    view_handle: jlong,
) -> jlong {
    let Some(view) = view_from_handle_or_throw(&mut env, view_handle) else {
        return 0;
    };
    Box::into_raw(Box::new(view.clone())) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableReaderView_get(
    mut env: JNIEnv,
    _class: JClass,
    view_handle: jlong,
    bucket: jint,
    key: JByteArray,
    read_options_handle: jlong,
) -> jobject {
    let Some(view) = view_from_handle_or_throw(&mut env, view_handle) else {
        return std::ptr::null_mut();
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    let key = match decode_java_bytes(&mut env, key) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    let Some(options) = read_options_from_handle_or_throw(&mut env, read_options_handle) else {
        return std::ptr::null_mut();
    };
    match view.get(bucket, &key, options.read_options()) {
        Ok(Some(columns)) => {
            to_java_optional_bytes_2d(&mut env, &columns).unwrap_or_else(|error| {
                throw_illegal_state(&mut env, error.to_string());
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
pub extern "system" fn Java_io_cobble_table_TableReaderView_multiGet<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    view_handle: jlong,
    buckets: JIntArray<'local>,
    keys: JObjectArray<'local>,
    read_options_handle: jlong,
) -> jobject {
    let Some(view) = view_from_handle_or_throw(&mut env, view_handle) else {
        return std::ptr::null_mut();
    };
    let keys = match decode_multi_get_keys(&mut env, &buckets, &keys) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    let Some(options) = read_options_from_handle_or_throw(&mut env, read_options_handle) else {
        return std::ptr::null_mut();
    };
    let borrowed = keys
        .iter()
        .map(|(bucket, key)| (*bucket, key.as_slice()))
        .collect::<Vec<_>>();
    match view.multi_get(&borrowed, options.read_options()) {
        Ok(columns) => to_java_optional_bytes_3d(&mut env, &columns).unwrap_or_else(|error| {
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
pub extern "system" fn Java_io_cobble_table_TableReaderView_openScanCursor(
    mut env: JNIEnv,
    _class: JClass,
    view_handle: jlong,
    bucket: jint,
    start: JByteArray,
    end: JByteArray,
    scan_options_handle: jlong,
) -> jobject {
    let Some(view) = view_from_handle_or_throw(&mut env, view_handle) else {
        return std::ptr::null_mut();
    };
    let Some(args) =
        decode_scan_open_bounds_args(&mut env, bucket, start, end, scan_options_handle)
    else {
        return std::ptr::null_mut();
    };
    let Some(options) = scan_options_from_handle_or_throw(&mut env, scan_options_handle) else {
        return std::ptr::null_mut();
    };
    match view.scan(
        args.bucket,
        args.start_key_inclusive.as_deref(),
        args.end_key_exclusive.as_deref(),
        options.scan_options(),
    ) {
        Ok(iter) => {
            let handle = Box::into_raw(Box::new(ScanCursorHandle::from_static_iter(iter))) as jlong;
            match env.new_object(
                "io/cobble/DirectScanCursor",
                "(J)V",
                &[JValue::Long(handle)],
            ) {
                Ok(cursor) => cursor.into_raw(),
                Err(error) => {
                    // SAFETY: Java has not received this cursor handle.
                    drop(unsafe { Box::from_raw(handle as *mut ScanCursorHandle) });
                    throw_illegal_state(&mut env, error.to_string());
                    std::ptr::null_mut()
                }
            }
        }
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            std::ptr::null_mut()
        }
    }
}
