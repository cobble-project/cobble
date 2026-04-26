use crate::read_options::read_options_from_handle_or_throw;
use crate::scan::{ScanCursorHandle, decode_scan_open_args};
use crate::util::{
    complete_future_exceptionally, complete_future_with_string, decode_bucket_ranges,
    decode_java_bytes, decode_java_string, decode_u16, decode_u32, decode_u64_from_jlong,
    parse_config_json, take_last_overflow_direct_buffer, throw_illegal_argument,
    throw_illegal_state, to_java_optional_bytes_2d, to_java_string_or_throw,
    write_payload_to_io_or_cached_overflow,
};
use crate::write_options::write_options_from_handle_or_throw;
use cobble::{Config, Db};
use jni::JNIEnv;
use jni::JavaVM;
use jni::objects::{GlobalRef, JByteArray, JClass, JIntArray, JObject, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint, jintArray, jlong, jobject, jstring};
use serde_json::json;

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_openHandle(
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
    open_db(&mut env, config)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_openHandleWithRange(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    range_start: jint,
    range_end: jint,
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
    open_db_with_range(&mut env, config, range_start, range_end)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_openHandleFromJson(
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
    let Some(config) = parse_config_json(&mut env, &json) else {
        return 0;
    };
    open_db(&mut env, config)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_openHandleFromJsonWithRange(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    range_start: jint,
    range_end: jint,
) -> jlong {
    let json = match decode_java_string(&mut env, config_json) {
        Ok(json) => json,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let Some(config) = parse_config_json(&mut env, &json) else {
        return 0;
    };
    open_db_with_range(&mut env, config, range_start, range_end)
}

fn restore_db(
    env: &mut JNIEnv,
    config: Config,
    snapshot_id: jlong,
    db_id: JString,
    new_db_id: jboolean,
) -> jlong {
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return 0;
        }
    };
    let db_id = match decode_java_string(env, db_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return 0;
        }
    };
    let db = if new_db_id == JNI_TRUE {
        Db::open_new_with_snapshot(config, snapshot_id, db_id)
    } else {
        Db::open_from_snapshot(config, snapshot_id, db_id)
    };
    match db {
        Ok(v) => Box::into_raw(Box::new(v)) as jlong,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            0
        }
    }
}

fn restore_db_from_manifest_path(
    env: &mut JNIEnv,
    config: Config,
    manifest_path: JString,
) -> jlong {
    let manifest_path = match decode_java_string(env, manifest_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return 0;
        }
    };
    match Db::open_new_with_manifest_path(config, manifest_path) {
        Ok(v) => Box::into_raw(Box::new(v)) as jlong,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_restoreHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    snapshot_id: jlong,
    db_id: JString,
    new_db_id: jboolean,
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
    restore_db(&mut env, config, snapshot_id, db_id, new_db_id)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_restoreHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    snapshot_id: jlong,
    db_id: JString,
    new_db_id: jboolean,
) -> jlong {
    let json = match decode_java_string(&mut env, config_json) {
        Ok(json) => json,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let Some(config) = parse_config_json(&mut env, &json) else {
        return 0;
    };
    restore_db(&mut env, config, snapshot_id, db_id, new_db_id)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_restoreWithManifestHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    manifest_path: JString,
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
    restore_db_from_manifest_path(&mut env, config, manifest_path)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_restoreWithManifestHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    manifest_path: JString,
) -> jlong {
    let json = match decode_java_string(&mut env, config_json) {
        Ok(json) => json,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let Some(config) = parse_config_json(&mut env, &json) else {
        return 0;
    };
    restore_db_from_manifest_path(&mut env, config, manifest_path)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_resumeHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    db_id: JString,
) -> jlong {
    let path = match decode_java_string(&mut env, config_path) {
        Ok(path) => path,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let db_id = match decode_java_string(&mut env, db_id) {
        Ok(v) => v,
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
    let db = match Db::resume(config, db_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(db)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_resumeHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    db_id: JString,
) -> jlong {
    let json = match decode_java_string(&mut env, config_json) {
        Ok(json) => json,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let db_id = match decode_java_string(&mut env, db_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let Some(config) = parse_config_json(&mut env, &json) else {
        return 0;
    };
    let db = match Db::resume(config, db_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(db)) as jlong
}

fn open_db(env: &mut JNIEnv, config: Config) -> jlong {
    let max_bucket = match validate_total_buckets(env, &config) {
        Some(v) => v,
        None => return 0,
    };
    open_db_with_owned_range(env, config, 0, max_bucket)
}

fn open_db_with_range(
    env: &mut JNIEnv,
    config: Config,
    range_start: jint,
    range_end: jint,
) -> jlong {
    let _ = match validate_total_buckets(env, &config) {
        Some(v) => v,
        None => return 0,
    };
    let range_start = match decode_u16("rangeStartInclusive", range_start) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return 0;
        }
    };
    let range_end = match decode_u16("rangeEndInclusive", range_end) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return 0;
        }
    };
    open_db_with_owned_range(env, config, range_start, range_end)
}

fn open_db_with_owned_range(
    env: &mut JNIEnv,
    config: Config,
    range_start: u16,
    range_end: u16,
) -> jlong {
    let max_bucket = (config.total_buckets - 1) as u16;
    if range_start > range_end {
        throw_illegal_argument(
            env,
            format!(
                "rangeStartInclusive must be <= rangeEndInclusive, got {}..={}",
                range_start, range_end
            ),
        );
        return 0;
    }
    if range_end > max_bucket {
        throw_illegal_argument(
            env,
            format!(
                "rangeEndInclusive must be <= {}, got {}",
                max_bucket, range_end
            ),
        );
        return 0;
    }
    let db = match Db::open(config, vec![range_start..=range_end]) {
        Ok(db) => db,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(db)) as jlong
}

fn validate_total_buckets(env: &mut JNIEnv, config: &Config) -> Option<u16> {
    let total_buckets = config.total_buckets;
    if total_buckets == 0 || total_buckets > (u16::MAX as u32) + 1 {
        throw_illegal_argument(
            env,
            format!("invalid total_buckets in config: {}", total_buckets),
        );
        return None;
    }
    Some((total_buckets - 1) as u16)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(&mut env, "db handle is already disposed".to_string());
        return;
    }
    let ptr = native_handle as *mut Db;
    // SAFETY: `native_handle` is returned by `Db.openHandle` from `Box<Db>`.
    let boxed = unsafe { Box::from_raw(ptr) };
    if let Err(err) = boxed.close() {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_directBufferPoolConfig(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jintArray {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let (buffer_size_bytes, pool_size) = match db.jni_direct_buffer_pool_config() {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    direct_buffer_pool_config_array(&mut env, buffer_size_bytes, pool_size)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_put(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
) {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let column = match decode_u16("column", column) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let key = match decode_java_bytes(&mut env, key) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let value = match decode_java_bytes(&mut env, value) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    if let Err(err) = db.put(bucket, key, column, value) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_putWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
    write_options_handle: jlong,
) {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let column = match decode_u16("column", column) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let key = match decode_java_bytes(&mut env, key) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let value = match decode_java_bytes(&mut env, value) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let Some(write_options_handle) =
        write_options_from_handle_or_throw(&mut env, write_options_handle)
    else {
        return;
    };
    if let Err(err) = db.put_with_options(
        bucket,
        key,
        column,
        value,
        write_options_handle.write_options(),
    ) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_putDirectWithOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key_address: jlong,
    key_capacity: jint,
    key_length: jint,
    column: jint,
    value_address: jlong,
    value_capacity: jint,
    value_length: jint,
    write_options_handle: jlong,
) {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let column = match decode_u16("column", column) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let key_length = match usize::try_from(key_length) {
        Ok(v) => v,
        Err(_) => {
            throw_illegal_argument(&mut env, "keyLength must be >= 0".to_string());
            return;
        }
    };
    let key_capacity = match usize::try_from(key_capacity) {
        Ok(v) => v,
        Err(_) => {
            throw_illegal_argument(&mut env, "keyCapacity must be >= 0".to_string());
            return;
        }
    };
    let value_length = match usize::try_from(value_length) {
        Ok(v) => v,
        Err(_) => {
            throw_illegal_argument(&mut env, "valueLength must be >= 0".to_string());
            return;
        }
    };
    let value_capacity = match usize::try_from(value_capacity) {
        Ok(v) => v,
        Err(_) => {
            throw_illegal_argument(&mut env, "valueCapacity must be >= 0".to_string());
            return;
        }
    };
    let Some(write_options_handle) =
        write_options_from_handle_or_throw(&mut env, write_options_handle)
    else {
        return;
    };

    let key_addr = match usize::try_from(key_address) {
        Ok(v) if v != 0 => v as *mut u8,
        _ => {
            throw_illegal_argument(&mut env, "keyAddress must be > 0".to_string());
            return;
        }
    };
    if key_length > key_capacity {
        throw_illegal_argument(
            &mut env,
            format!(
                "keyLength {} exceeds keyBuffer capacity {}",
                key_length, key_capacity
            ),
        );
        return;
    }

    let value_addr = match usize::try_from(value_address) {
        Ok(v) if v != 0 => v as *mut u8,
        _ => {
            throw_illegal_argument(&mut env, "valueAddress must be > 0".to_string());
            return;
        }
    };
    if value_length > value_capacity {
        throw_illegal_argument(
            &mut env,
            format!(
                "valueLength {} exceeds valueBuffer capacity {}",
                value_length, value_capacity
            ),
        );
        return;
    }

    let key = unsafe { std::slice::from_raw_parts(key_addr as *const u8, key_length) };
    let value = unsafe { std::slice::from_raw_parts(value_addr as *const u8, value_length) };
    if let Err(err) = db.put_with_options(
        bucket,
        key,
        column,
        value,
        write_options_handle.write_options(),
    ) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_merge(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
) {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let column = match decode_u16("column", column) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let key = match decode_java_bytes(&mut env, key) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let value = match decode_java_bytes(&mut env, value) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    if let Err(err) = db.merge(bucket, key, column, value) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_mergeWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
    write_options_handle: jlong,
) {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let column = match decode_u16("column", column) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let key = match decode_java_bytes(&mut env, key) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let value = match decode_java_bytes(&mut env, value) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let Some(write_options_handle) =
        write_options_from_handle_or_throw(&mut env, write_options_handle)
    else {
        return;
    };
    if let Err(err) = db.merge_with_options(
        bucket,
        key,
        column,
        value,
        write_options_handle.write_options(),
    ) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_get(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    read_options_handle: jlong,
) -> jobject {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let bucket = match decode_u16("bucket", bucket) {
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
    let Some(read_options_handle) =
        read_options_from_handle_or_throw(&mut env, read_options_handle)
    else {
        return std::ptr::null_mut();
    };
    let values = match db.get_with_options(bucket, &key, read_options_handle.read_options()) {
        Ok(values) => values,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    let Some(columns) = values else {
        return std::ptr::null_mut();
    };
    match to_java_optional_bytes_2d(&mut env, columns.as_slice()) {
        Ok(array) => array,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_getEncodedDirectWithOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    io_address: jlong,
    io_capacity: jint,
    key_length: jint,
    read_options_handle: jlong,
) -> jint {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let key_length = match usize::try_from(key_length) {
        Ok(v) => v,
        Err(_) => {
            throw_illegal_argument(&mut env, "keyLength must be >= 0".to_string());
            return 0;
        }
    };
    let io_capacity = match usize::try_from(io_capacity) {
        Ok(v) => v,
        Err(_) => {
            throw_illegal_argument(&mut env, "ioCapacity must be >= 0".to_string());
            return 0;
        }
    };
    let Some(read_options_handle) =
        read_options_from_handle_or_throw(&mut env, read_options_handle)
    else {
        return 0;
    };
    let direct_addr = match usize::try_from(io_address) {
        Ok(v) if v != 0 => v as *mut u8,
        _ => {
            throw_illegal_argument(&mut env, "ioAddress must be > 0".to_string());
            return 0;
        }
    };
    if key_length > io_capacity {
        throw_illegal_argument(
            &mut env,
            format!(
                "keyLength {} exceeds ioBuffer capacity {}",
                key_length, io_capacity
            ),
        );
        return 0;
    }
    let key = unsafe { std::slice::from_raw_parts(direct_addr as *const u8, key_length) };
    let values = match db.get_with_options(bucket, key, read_options_handle.read_options()) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    encode_optional_columns_to_direct_buffer(&mut env, values, direct_addr, io_capacity)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_getLastDirectOverflowBuffer(
    mut env: JNIEnv,
    _class: JClass,
) -> jobject {
    match take_last_overflow_direct_buffer(&mut env) {
        Ok(obj) => obj,
        Err(err) => {
            throw_illegal_state(&mut env, err);
            std::ptr::null_mut()
        }
    }
}

fn encode_optional_columns_to_direct_buffer<'local>(
    env: &mut JNIEnv<'local>,
    values: Option<Vec<Option<bytes::Bytes>>>,
    direct_addr: *mut u8,
    direct_capacity: usize,
) -> jint {
    let Some(values) = values else {
        return 0;
    };
    let encoded = match encode_optional_columns_payload(values.as_slice()) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(env, err);
            return 0;
        }
    };
    match write_payload_to_io_or_cached_overflow(
        env,
        direct_addr,
        direct_capacity,
        encoded.as_slice(),
    ) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(env, err);
            0
        }
    }
}

fn encode_optional_columns_payload(columns: &[Option<bytes::Bytes>]) -> Result<Vec<u8>, String> {
    let mut total = 4usize;
    for column in columns {
        total = total
            .checked_add(1)
            .ok_or_else(|| "encoded columns size overflow".to_string())?;
        if let Some(bytes) = column {
            total = total
                .checked_add(4)
                .and_then(|v| v.checked_add(bytes.len()))
                .ok_or_else(|| "encoded column size overflow".to_string())?;
        }
    }
    let mut encoded = Vec::with_capacity(total);
    encoded.extend_from_slice(&(columns.len() as u32).to_be_bytes());
    for column in columns {
        match column {
            None => encoded.push(0),
            Some(bytes) => {
                encoded.push(1);
                encoded.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                encoded.extend_from_slice(bytes.as_ref());
            }
        }
    }
    Ok(encoded)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_openScanCursor(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    start_key_inclusive: JByteArray,
    end_key_exclusive: JByteArray,
    scan_options_handle: jlong,
) -> jlong {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
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
    let iter = match db.scan_with_options(
        args.bucket,
        args.start_key_inclusive.as_slice()..args.end_key_exclusive.as_slice(),
        args.scan_options_handle.scan_options(),
    ) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(ScanCursorHandle::from_static_iter(
        iter,
        args.scan_options_handle.batch_size(),
    ))) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_delete(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
) {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let column = match decode_u16("column", column) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let key = match decode_java_bytes(&mut env, key) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    if let Err(err) = db.delete(bucket, key, column) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_deleteWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    write_options_handle: jlong,
) {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let column = match decode_u16("column", column) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let key = match decode_java_bytes(&mut env, key) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let Some(write_options_handle) =
        write_options_from_handle_or_throw(&mut env, write_options_handle)
    else {
        return;
    };
    if let Err(err) =
        db.delete_with_options(bucket, key, column, write_options_handle.write_options())
    {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_setTime(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    next_seconds: jint,
) {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let next = match decode_u32("nextSeconds", next_seconds) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    db.set_time(next);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_id(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    to_java_string_or_throw(&mut env, db.id().to_string())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_nowSeconds(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jint {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    db.now_seconds() as jint
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_asyncSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    snapshot_future_json: JObject,
) {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    if snapshot_future_json.is_null() {
        throw_illegal_argument(&mut env, "snapshotFutureJson must not be null".to_string());
        return;
    }
    let future = match env.new_global_ref(snapshot_future_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return;
        }
    };
    let vm = match env.get_java_vm() {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return;
        }
    };
    if let Err(err) = db.snapshot_with_callback(move |result| {
        complete_snapshot_json_future(
            &vm,
            &future,
            result.map(|input| shard_snapshot_json(&input)),
        );
    }) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_expireSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    snapshot_id: jlong,
) -> jboolean {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return JNI_FALSE;
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return JNI_FALSE;
        }
    };
    match db.expire_snapshot(snapshot_id) {
        Ok(expired) => {
            if expired {
                JNI_TRUE
            } else {
                JNI_FALSE
            }
        }
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            JNI_FALSE
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_retainSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    snapshot_id: jlong,
) -> jboolean {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return JNI_FALSE;
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return JNI_FALSE;
        }
    };
    if db.retain_snapshot(snapshot_id) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_getShardSnapshotJson(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    snapshot_id: jlong,
) -> jstring {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let payload = match build_shard_snapshot_payload(db, snapshot_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    to_java_string_or_throw(&mut env, payload)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_expandBucket(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    source_db_id: JString,
    snapshot_id: jlong,
    range_starts: JIntArray,
    range_ends: JIntArray,
) -> jlong {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let source_db_id = match decode_java_string(&mut env, source_db_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let snapshot_id = if snapshot_id < 0 {
        None
    } else {
        match decode_u64_from_jlong("snapshotId", snapshot_id) {
            Ok(v) => Some(v),
            Err(err) => {
                throw_illegal_argument(&mut env, err);
                return 0;
            }
        }
    };
    let ranges = match decode_bucket_ranges(&mut env, range_starts, range_ends) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let ranges = if ranges.is_empty() {
        None
    } else {
        Some(ranges)
    };
    match db.expand_bucket(source_db_id, snapshot_id, ranges) {
        Ok(v) => v as jlong,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_shrinkBucket(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    range_starts: JIntArray,
    range_ends: JIntArray,
) -> jlong {
    let Some(db) = db_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let ranges = match decode_bucket_ranges(&mut env, range_starts, range_ends) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    if ranges.is_empty() {
        throw_illegal_argument(&mut env, "shrink ranges must not be empty".to_string());
        return 0;
    }
    match db.shrink_bucket(ranges) {
        Ok(v) => v as jlong,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            0
        }
    }
}

fn db_from_handle_or_throw(env: &mut JNIEnv, native_handle: jlong) -> Option<&'static Db> {
    if native_handle == 0 {
        throw_illegal_state(env, "db handle is disposed".to_string());
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<Db>` and valid until `disposeInternal`.
    Some(unsafe { &*(native_handle as *const Db) })
}

fn direct_buffer_pool_config_array(
    env: &mut JNIEnv,
    buffer_size_bytes: usize,
    pool_size: usize,
) -> jintArray {
    let buffer_size_jint: jint = match jint::try_from(buffer_size_bytes) {
        Ok(v) => v,
        Err(_) => {
            throw_illegal_state(
                env,
                format!("jni_direct_buffer_size too large: {buffer_size_bytes}"),
            );
            return std::ptr::null_mut();
        }
    };
    let pool_size_jint: jint = match jint::try_from(pool_size) {
        Ok(v) => v,
        Err(_) => {
            throw_illegal_state(
                env,
                format!("jni_direct_buffer_pool_size too large: {pool_size}"),
            );
            return std::ptr::null_mut();
        }
    };
    let arr = match env.new_int_array(2) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    if let Err(err) = env.set_int_array_region(&arr, 0, &[buffer_size_jint, pool_size_jint]) {
        throw_illegal_state(env, err.to_string());
        return std::ptr::null_mut();
    }
    arr.into_raw()
}

fn build_shard_snapshot_payload(db: &Db, snapshot_id: u64) -> std::result::Result<String, String> {
    let input = db
        .shard_snapshot_input(snapshot_id)
        .map_err(|err| err.to_string())?;
    Ok(shard_snapshot_json(&input))
}

fn shard_snapshot_json(input: &cobble::ShardSnapshotInput) -> String {
    let ranges: Vec<serde_json::Value> = input
        .ranges
        .iter()
        .map(|range| {
            json!({
                "start": *range.start(),
                "end": *range.end(),
            })
        })
        .collect();
    json!({
        "ranges": ranges,
        "column_family_ids": &input.column_family_ids,
        "db_id": &input.db_id,
        "snapshot_id": input.snapshot_id,
        "manifest_path": &input.manifest_path,
        "timestamp_seconds": input.timestamp_seconds,
    })
    .to_string()
}

fn complete_snapshot_json_future(vm: &JavaVM, future: &GlobalRef, result: cobble::Result<String>) {
    let mut env = match vm.attach_current_thread() {
        Ok(v) => v,
        Err(_) => return,
    };
    match result {
        Ok(json) => {
            let _ = complete_future_with_string(&mut env, future.as_obj(), &json).or_else(|_| {
                complete_future_exceptionally(
                    &mut env,
                    future.as_obj(),
                    "failed to allocate json string",
                )
            });
        }
        Err(err) => {
            let _ = complete_future_exceptionally(&mut env, future.as_obj(), &err.to_string());
        }
    }
}
