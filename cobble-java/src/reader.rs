use crate::scan::{ScanCursorHandle, decode_scan_open_args};
use crate::util::{
    decode_column_index, decode_java_bytes, decode_java_string, decode_u16, decode_u32,
    throw_illegal_argument, throw_illegal_state, to_java_string_or_throw,
};
use cobble::{Config, ReadOptions, Reader, ReaderConfig};
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jbyteArray, jint, jlong, jstring};

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Reader_openCurrentHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
) -> jlong {
    let path = match decode_java_string(&mut env, config_path) {
        Ok(path) => path,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let config = match Config::from_path(path) {
        Ok(config) => config,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    let reader_config = ReaderConfig::from_config(&config);
    let reader = match Reader::open_current(reader_config) {
        Ok(reader) => reader,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(reader)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Reader_openCurrentHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
) -> jlong {
    let json = match decode_java_string(&mut env, config_json) {
        Ok(json) => json,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let config = match serde_json::from_str::<Config>(&json) {
        Ok(config) => config,
        Err(err) => {
            throw_illegal_argument(&mut env, format!("invalid config json: {}", err));
            return 0;
        }
    };
    let reader_config = ReaderConfig::from_config(&config);
    let reader = match Reader::open_current(reader_config) {
        Ok(reader) => reader,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(reader)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Reader_openHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    global_snapshot_id: jint,
) -> jlong {
    let path = match decode_java_string(&mut env, config_path) {
        Ok(path) => path,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let snapshot_id = match decode_u32("globalSnapshotId", global_snapshot_id) {
        Ok(v) => v as u64,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let config = match Config::from_path(path) {
        Ok(config) => config,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    let reader_config = ReaderConfig::from_config(&config);
    let reader = match Reader::open(reader_config, snapshot_id) {
        Ok(reader) => reader,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(reader)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Reader_openHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    global_snapshot_id: jint,
) -> jlong {
    let json = match decode_java_string(&mut env, config_json) {
        Ok(json) => json,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let snapshot_id = match decode_u32("globalSnapshotId", global_snapshot_id) {
        Ok(v) => v as u64,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let config = match serde_json::from_str::<Config>(&json) {
        Ok(config) => config,
        Err(err) => {
            throw_illegal_argument(&mut env, format!("invalid config json: {}", err));
            return 0;
        }
    };
    let reader_config = ReaderConfig::from_config(&config);
    let reader = match Reader::open(reader_config, snapshot_id) {
        Ok(reader) => reader,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(reader)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Reader_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(&mut env, "reader handle is already disposed".to_string());
        return;
    }
    let ptr = native_handle as *mut Reader;
    // SAFETY: `native_handle` is returned by `Reader.open*Handle` from `Box<Reader>`.
    let _boxed = unsafe { Box::from_raw(ptr) };
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Reader_refresh(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    let Some(reader) = reader_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    if let Err(err) = reader.refresh() {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Reader_get(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
) -> jbyteArray {
    let Some(reader) = reader_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let column_index = match decode_column_index(column) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let key = match decode_java_bytes(&mut env, key) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let options = ReadOptions::for_column(column_index);
    let values = match reader.get(bucket, &key, &options) {
        Ok(values) => values,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    let Some(columns) = values else {
        return std::ptr::null_mut();
    };
    let Some(Some(value)) = columns.get(column_index) else {
        return std::ptr::null_mut();
    };
    match env.byte_array_from_slice(value) {
        Ok(bytes) => bytes.into_raw(),
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Reader_openScanCursor(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    start_key_inclusive: JByteArray,
    end_key_exclusive: JByteArray,
    scan_options_handle: jlong,
) -> jlong {
    let Some(reader) = reader_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let Some(args) = decode_scan_open_args(
        &mut env,
        bucket,
        start_key_inclusive,
        end_key_exclusive,
        scan_options_handle,
    ) else {
        return 0;
    };
    let iter = match reader.scan(
        args.bucket,
        args.start_key_inclusive.as_slice()..args.end_key_exclusive.as_slice(),
        &args.scan_options,
    ) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(ScanCursorHandle::from_static_iter(
        iter,
        args.batch_size,
    ))) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Reader_readMode(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    let Some(reader) = reader_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    match env.new_string(reader.read_mode()) {
        Ok(s) => s.into_raw(),
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Reader_configuredSnapshotId(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jlong {
    let Some(reader) = reader_from_handle_or_throw(&mut env, native_handle) else {
        return -1;
    };
    match reader.configured_snapshot_id() {
        Some(id) => id as jlong,
        None => -1,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Reader_listGlobalSnapshotsJson(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    let Some(reader) = reader_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let manifests = match reader.list_global_snapshot_manifests() {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    let json = match serde_json::to_string(&manifests) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    to_java_string_or_throw(&mut env, json)
}

fn reader_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static mut Reader> {
    if native_handle == 0 {
        throw_illegal_state(env, "reader handle is disposed".to_string());
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<Reader>` and valid until `disposeInternal`.
    Some(unsafe { &mut *(native_handle as *mut Reader) })
}
