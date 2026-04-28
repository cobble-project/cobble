// JNI bridge for io.cobble.structured.Db and io.cobble.structured.ScanCursor
//
// Typed put/merge accept both bytes (byte[]) and list (byte[][]).
// Typed get returns Object[] where each element is null | byte[] | byte[][].
// Typed scan cursor yields structured batches with mixed column types.

use crate::structured_read_options::structured_read_options_from_handle_or_throw;
use crate::structured_scan_options::{
    DEFAULT_SCAN_BATCH_SIZE, decode_structured_scan_open_args,
    decode_structured_scan_open_direct_args, structured_scan_options_from_handle_or_throw,
};
use crate::structured_write_options::structured_write_options_from_handle_or_throw;
use crate::util::{
    byte_array_class, complete_future_exceptionally, complete_future_with_string,
    decode_bucket_ranges, decode_java_bytes, decode_java_bytes_ref, decode_java_string,
    decode_optional_java_string, decode_u16, decode_u64_from_jlong, new_object_array,
    new_structured_scan_batch, object_array_class, object_class, parse_config_json,
    take_last_overflow_direct_buffer, throw_illegal_argument, throw_illegal_state,
    to_java_string_or_throw, write_payload_to_io_or_cached_overflow,
};
use bytes::Bytes;
use cobble::Config;
use cobble_data_structure::{
    DataStructureDb, StructuredColumnValue, StructuredDbIterator, StructuredScanSplit,
    StructuredScanSplitScanner,
};
use jni::JNIEnv;
use jni::JavaVM;
use jni::objects::{GlobalRef, JByteArray, JClass, JIntArray, JObject, JObjectArray, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint, jintArray, jlong, jobject, jstring};
use serde_json::json;

pub(crate) enum StructuredSchemaBuilderHandle {
    Db(cobble_data_structure::StructuredSchemaBuilder<'static, DataStructureDb>),
    SingleDb(
        cobble_data_structure::StructuredSchemaBuilder<
            'static,
            cobble_data_structure::StructuredSingleDb,
        >,
    ),
}

// ── open ────────────────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_openHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
) -> jlong {
    let config_path = match decode_java_string(&mut env, config_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let config = match Config::from_path(&config_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    open_structured_db(&mut env, config)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_openHandleWithRange(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    range_start: jint,
    range_end: jint,
) -> jlong {
    let config_path = match decode_java_string(&mut env, config_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let config = match Config::from_path(&config_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    open_structured_db_with_range(&mut env, config, range_start, range_end)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_openHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
) -> jlong {
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return 0;
    };
    open_structured_db(&mut env, config)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_openHandleFromJsonWithRange(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    range_start: jint,
    range_end: jint,
) -> jlong {
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return 0;
    };
    open_structured_db_with_range(&mut env, config, range_start, range_end)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_directBufferPoolConfig(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jintArray {
    let Some(db) = db_from_handle(&mut env, handle) else {
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

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_currentSchemaJson(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jstring {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return std::ptr::null_mut();
    };
    match serde_json::to_string(&db.current_schema()) {
        Ok(v) => to_java_string_or_throw(&mut env, v),
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_createSchemaBuilder(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jlong {
    let Some(db) = db_from_handle_mut(&mut env, handle) else {
        return 0;
    };
    let builder = unsafe {
        // JNI builder handle owns the lifetime; Java must commit/dispose before Db disposal.
        let builder = (&mut *(db as *mut DataStructureDb)).update_schema();
        std::mem::transmute::<
            cobble_data_structure::StructuredSchemaBuilder<'_, DataStructureDb>,
            cobble_data_structure::StructuredSchemaBuilder<'static, DataStructureDb>,
        >(builder)
    };
    let handle = StructuredSchemaBuilderHandle::Db(builder);
    Box::into_raw(Box::new(handle)) as jlong
}

fn open_structured_db(env: &mut JNIEnv, config: Config) -> jlong {
    let total_buckets = config.total_buckets;
    if total_buckets == 0 || total_buckets > (u16::MAX as u32) + 1 {
        throw_illegal_argument(
            env,
            format!(
                "total_buckets must be in [1, {}], got {}",
                (u16::MAX as u32) + 1,
                total_buckets
            ),
        );
        return 0;
    }
    let range = 0..=((total_buckets - 1) as u16);
    match DataStructureDb::open(config, vec![range]) {
        Ok(db) => Box::into_raw(Box::new(db)) as jlong,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            0
        }
    }
}

fn open_structured_db_with_range(
    env: &mut JNIEnv,
    config: Config,
    range_start: jint,
    range_end: jint,
) -> jlong {
    let total_buckets = config.total_buckets;
    if total_buckets == 0 || total_buckets > (u16::MAX as u32) + 1 {
        throw_illegal_argument(
            env,
            format!(
                "total_buckets must be in [1, {}], got {}",
                (u16::MAX as u32) + 1,
                total_buckets
            ),
        );
        return 0;
    }
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
    let max_bucket = (total_buckets - 1) as u16;
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
    match DataStructureDb::open(config, vec![range_start..=range_end]) {
        Ok(db) => Box::into_raw(Box::new(db)) as jlong,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            0
        }
    }
}

// ── restore / resume ────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_restoreHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    snapshot_id: jlong,
    db_id: JString,
    new_db_id: jboolean,
) -> jlong {
    let config_path = match decode_java_string(&mut env, config_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let config = match Config::from_path(&config_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    restore_structured_db(&mut env, config, snapshot_id, db_id, new_db_id)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_restoreHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    snapshot_id: jlong,
    db_id: JString,
    new_db_id: jboolean,
) -> jlong {
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return 0;
    };
    restore_structured_db(&mut env, config, snapshot_id, db_id, new_db_id)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_restoreWithManifestHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    manifest_path: JString,
) -> jlong {
    let config_path = match decode_java_string(&mut env, config_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let config = match Config::from_path(&config_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    restore_structured_db_from_manifest_path(&mut env, config, manifest_path)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_restoreWithManifestHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    manifest_path: JString,
) -> jlong {
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return 0;
    };
    restore_structured_db_from_manifest_path(&mut env, config, manifest_path)
}

fn restore_structured_db(
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
    let resolver = cobble_data_structure::structured_merge_operator_resolver();
    let db = if new_db_id == JNI_TRUE {
        DataStructureDb::open_new_with_snapshot_with_resolver(
            config,
            snapshot_id,
            db_id,
            Some(resolver),
        )
    } else {
        DataStructureDb::open_from_snapshot_with_resolver(
            config,
            snapshot_id,
            db_id,
            Some(resolver),
        )
    };
    match db {
        Ok(db) => Box::into_raw(Box::new(db)) as jlong,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            0
        }
    }
}

fn restore_structured_db_from_manifest_path(
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
    let resolver = cobble_data_structure::structured_merge_operator_resolver();
    match DataStructureDb::open_new_with_manifest_path_with_resolver(
        config,
        manifest_path,
        Some(resolver),
    ) {
        Ok(db) => Box::into_raw(Box::new(db)) as jlong,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_resumeHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    db_id: JString,
) -> jlong {
    let config_path = match decode_java_string(&mut env, config_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let config = match Config::from_path(&config_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    resume_structured_db(&mut env, config, db_id)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_resumeHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    db_id: JString,
) -> jlong {
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return 0;
    };
    resume_structured_db(&mut env, config, db_id)
}

fn resume_structured_db(env: &mut JNIEnv, config: Config, db_id: JString) -> jlong {
    let db_id = match decode_java_string(env, db_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return 0;
        }
    };
    let resolver = cobble_data_structure::structured_merge_operator_resolver();
    match DataStructureDb::resume_with_resolver(config, db_id, Some(resolver)) {
        Ok(db) => Box::into_raw(Box::new(db)) as jlong,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            0
        }
    }
}

// ── dispose ─────────────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "structured db handle is already disposed".to_string(),
        );
        return;
    }
    let db = unsafe { Box::from_raw(native_handle as *mut DataStructureDb) };
    if let Err(err) = db.close() {
        throw_illegal_state(&mut env, err.to_string());
    }
}

// ── bytes put / merge ───────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_putBytes(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
) {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return;
    };
    let Some((bucket, column, key, value_bytes)) =
        decode_write_bytes_args(&mut env, bucket, column, key, value)
    else {
        return;
    };
    if let Err(err) = db.put(
        bucket,
        key,
        column,
        StructuredColumnValue::Bytes(Bytes::from(value_bytes)),
    ) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_putBytesWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
    write_options_handle: jlong,
) {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return;
    };
    let Some((bucket, col, key, value_bytes)) =
        decode_write_bytes_args(&mut env, bucket, column, key, value)
    else {
        return;
    };
    let result = if write_options_handle == 0 {
        db.put(
            bucket,
            key,
            col,
            StructuredColumnValue::Bytes(Bytes::from(value_bytes)),
        )
    } else {
        let Some(wo) =
            structured_write_options_from_handle_or_throw(&mut env, write_options_handle)
        else {
            return;
        };
        db.put_with_options(
            bucket,
            key,
            col,
            StructuredColumnValue::Bytes(Bytes::from(value_bytes)),
            wo.write_options(),
        )
    };
    if let Err(err) = result {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_putBytesDirectWithOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    handle: jlong,
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
    let Some(db) = db_from_handle(&mut env, handle) else {
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
    let result = if write_options_handle == 0 {
        db.put(
            bucket,
            key,
            column,
            StructuredColumnValue::Bytes(Bytes::copy_from_slice(value)),
        )
    } else {
        let Some(wo) =
            structured_write_options_from_handle_or_throw(&mut env, write_options_handle)
        else {
            return;
        };
        db.put_with_options(
            bucket,
            key,
            column,
            StructuredColumnValue::Bytes(Bytes::copy_from_slice(value)),
            wo.write_options(),
        )
    };
    if let Err(err) = result {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_mergeBytes(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
) {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return;
    };
    let Some((bucket, column, key, value_bytes)) =
        decode_write_bytes_args(&mut env, bucket, column, key, value)
    else {
        return;
    };
    if let Err(err) = db.merge(
        bucket,
        key,
        column,
        StructuredColumnValue::Bytes(Bytes::from(value_bytes)),
    ) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_mergeBytesWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
    write_options_handle: jlong,
) {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return;
    };
    let Some((bucket, col, key, value_bytes)) =
        decode_write_bytes_args(&mut env, bucket, column, key, value)
    else {
        return;
    };
    let result = if write_options_handle == 0 {
        db.merge(
            bucket,
            key,
            col,
            StructuredColumnValue::Bytes(Bytes::from(value_bytes)),
        )
    } else {
        let Some(wo) =
            structured_write_options_from_handle_or_throw(&mut env, write_options_handle)
        else {
            return;
        };
        db.merge_with_options(
            bucket,
            key,
            col,
            StructuredColumnValue::Bytes(Bytes::from(value_bytes)),
            wo.write_options(),
        )
    };
    if let Err(err) = result {
        throw_illegal_state(&mut env, err.to_string());
    }
}

// ── list put / merge ────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_putList(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    elements: JObjectArray,
) {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return;
    };
    let Some((bucket, column, key, list)) =
        decode_write_list_args(&mut env, bucket, column, key, elements)
    else {
        return;
    };
    if let Err(err) = db.put(bucket, key, column, StructuredColumnValue::List(list)) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_putListWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    elements: JObjectArray,
    write_options_handle: jlong,
) {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return;
    };
    let Some((bucket, col, key, list)) =
        decode_write_list_args(&mut env, bucket, column, key, elements)
    else {
        return;
    };
    let result = if write_options_handle == 0 {
        db.put(bucket, key, col, StructuredColumnValue::List(list))
    } else {
        let Some(wo) =
            structured_write_options_from_handle_or_throw(&mut env, write_options_handle)
        else {
            return;
        };
        db.put_with_options(
            bucket,
            key,
            col,
            StructuredColumnValue::List(list),
            wo.write_options(),
        )
    };
    if let Err(err) = result {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_mergeList(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    elements: JObjectArray,
) {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return;
    };
    let Some((bucket, column, key, list)) =
        decode_write_list_args(&mut env, bucket, column, key, elements)
    else {
        return;
    };
    if let Err(err) = db.merge(bucket, key, column, StructuredColumnValue::List(list)) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_mergeListWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    elements: JObjectArray,
    write_options_handle: jlong,
) {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return;
    };
    let Some((bucket, col, key, list)) =
        decode_write_list_args(&mut env, bucket, column, key, elements)
    else {
        return;
    };
    let result = if write_options_handle == 0 {
        db.merge(bucket, key, col, StructuredColumnValue::List(list))
    } else {
        let Some(wo) =
            structured_write_options_from_handle_or_throw(&mut env, write_options_handle)
        else {
            return;
        };
        db.merge_with_options(
            bucket,
            key,
            col,
            StructuredColumnValue::List(list),
            wo.write_options(),
        )
    };
    if let Err(err) = result {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_putEncodedListDirectWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key_address: jlong,
    key_capacity: jint,
    key_length: jint,
    column: jint,
    encoded_list_address: jlong,
    encoded_list_capacity: jint,
    encoded_list_length: jint,
    write_options_handle: jlong,
) {
    apply_encoded_list_direct_with_options(
        &mut env,
        handle,
        bucket,
        key_address,
        key_capacity,
        key_length,
        column,
        encoded_list_address,
        encoded_list_capacity,
        encoded_list_length,
        write_options_handle,
        false,
    );
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_mergeEncodedListDirectWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key_address: jlong,
    key_capacity: jint,
    key_length: jint,
    column: jint,
    encoded_list_address: jlong,
    encoded_list_capacity: jint,
    encoded_list_length: jint,
    write_options_handle: jlong,
) {
    apply_encoded_list_direct_with_options(
        &mut env,
        handle,
        bucket,
        key_address,
        key_capacity,
        key_length,
        column,
        encoded_list_address,
        encoded_list_capacity,
        encoded_list_length,
        write_options_handle,
        true,
    );
}

// ── delete ──────────────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_deleteWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    write_options_handle: jlong,
) {
    let Some(db) = db_from_handle(&mut env, handle) else {
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
    let result = if write_options_handle == 0 {
        db.delete(bucket, key, column)
    } else {
        let Some(wo) =
            structured_write_options_from_handle_or_throw(&mut env, write_options_handle)
        else {
            return;
        };
        db.delete_with_options(bucket, key, column, wo.write_options())
    };
    if let Err(err) = result {
        throw_illegal_state(&mut env, err.to_string());
    }
}

// ── typed get ───────────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_getTyped<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray<'local>,
) -> jobject {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return std::ptr::null_mut();
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let key_bytes = match decode_java_bytes_ref(&mut env, &key) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let values = match db.get(bucket, &key_bytes) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    let Some(columns) = values else {
        return std::ptr::null_mut();
    };
    match to_java_typed_columns(&mut env, columns) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err);
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_getTypedWithOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray<'local>,
    read_options_handle: jlong,
) -> jobject {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return std::ptr::null_mut();
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let key_bytes = match decode_java_bytes_ref(&mut env, &key) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let values = match if read_options_handle == 0 {
        db.get(bucket, &key_bytes)
    } else {
        let Some(ro) = structured_read_options_from_handle_or_throw(&mut env, read_options_handle)
        else {
            return std::ptr::null_mut();
        };
        db.get_with_options(bucket, &key_bytes, ro.read_options())
    } {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    let Some(columns) = values else {
        return std::ptr::null_mut();
    };
    match to_java_typed_columns(&mut env, columns) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err);
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_getEncodedDirectWithOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    io_address: jlong,
    io_capacity: jint,
    key_length: jint,
    read_options_handle: jlong,
) -> jint {
    let Some(db) = db_from_handle(&mut env, handle) else {
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
                "keyLength {} exceeds direct buffer capacity {}",
                key_length, io_capacity
            ),
        );
        return 0;
    }

    let key = unsafe { std::slice::from_raw_parts(direct_addr as *const u8, key_length) };
    let values = match if read_options_handle == 0 {
        db.get(bucket, key)
    } else {
        let Some(ro) = structured_read_options_from_handle_or_throw(&mut env, read_options_handle)
        else {
            return 0;
        };
        db.get_with_options(bucket, key, ro.read_options())
    } {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    encode_structured_columns_to_direct_buffer(&mut env, values, direct_addr, io_capacity)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_getLastDirectOverflowBuffer(
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

fn encode_structured_columns_to_direct_buffer<'local>(
    env: &mut JNIEnv<'local>,
    values: Option<Vec<Option<StructuredColumnValue>>>,
    direct_addr: *mut u8,
    direct_capacity: usize,
) -> jint {
    let Some(columns) = values else {
        return 0;
    };
    let encoded_size = match encoded_row_size(&columns) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(env, err);
            return 0;
        }
    };
    let mut encoded = vec![0u8; encoded_size];
    let out = encoded.as_mut_slice();
    if let Err(err) = encode_row_payload(&columns, out) {
        throw_illegal_state(env, err);
        return 0;
    }
    match write_payload_to_io_or_cached_overflow(env, direct_addr, direct_capacity, out) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(env, err);
            0
        }
    }
}

fn encoded_row_size(columns: &[Option<StructuredColumnValue>]) -> Result<usize, String> {
    let mut total = 4usize;
    for column in columns {
        total = total
            .checked_add(1)
            .ok_or_else(|| "encoded row size overflow".to_string())?;
        match column {
            None => {}
            Some(StructuredColumnValue::Bytes(bytes)) => {
                total = total
                    .checked_add(4)
                    .and_then(|v| v.checked_add(bytes.len()))
                    .ok_or_else(|| "encoded bytes column size overflow".to_string())?;
            }
            Some(StructuredColumnValue::List(list)) => {
                total = total
                    .checked_add(4)
                    .ok_or_else(|| "encoded list size overflow".to_string())?;
                for element in list {
                    total = total
                        .checked_add(4)
                        .and_then(|v| v.checked_add(element.len()))
                        .ok_or_else(|| "encoded list element size overflow".to_string())?;
                }
            }
        }
    }
    Ok(total)
}

fn encode_row_payload(
    columns: &[Option<StructuredColumnValue>],
    out: &mut [u8],
) -> Result<(), String> {
    if out.len() < 4 {
        return Err("output buffer too small for row header".to_string());
    }
    let mut offset = 0usize;
    offset = write_i32_be(out, offset, columns.len())?;
    for column in columns {
        match column {
            None => {
                offset = write_u8(out, offset, 0)?;
            }
            Some(StructuredColumnValue::Bytes(bytes)) => {
                offset = write_u8(out, offset, 1)?;
                offset = write_i32_be(out, offset, bytes.len())?;
                offset = write_slice(out, offset, bytes)?;
            }
            Some(StructuredColumnValue::List(list)) => {
                offset = write_u8(out, offset, 2)?;
                offset = write_i32_be(out, offset, list.len())?;
                for element in list {
                    offset = write_i32_be(out, offset, element.len())?;
                    offset = write_slice(out, offset, element)?;
                }
            }
        }
    }
    if offset != out.len() {
        return Err("encoded row size mismatch".to_string());
    }
    Ok(())
}

fn write_u8(out: &mut [u8], offset: usize, value: u8) -> Result<usize, String> {
    let end = offset
        .checked_add(1)
        .ok_or_else(|| "output offset overflow".to_string())?;
    if end > out.len() {
        return Err("output buffer too small".to_string());
    }
    out[offset] = value;
    Ok(end)
}

fn write_i32_be(out: &mut [u8], offset: usize, value: usize) -> Result<usize, String> {
    let value_u32 = u32::try_from(value).map_err(|_| "value does not fit into u32".to_string())?;
    let end = offset
        .checked_add(4)
        .ok_or_else(|| "output offset overflow".to_string())?;
    if end > out.len() {
        return Err("output buffer too small".to_string());
    }
    out[offset..end].copy_from_slice(&value_u32.to_be_bytes());
    Ok(end)
}

fn write_slice(out: &mut [u8], offset: usize, value: &[u8]) -> Result<usize, String> {
    let end = offset
        .checked_add(value.len())
        .ok_or_else(|| "output offset overflow".to_string())?;
    if end > out.len() {
        return Err("output buffer too small".to_string());
    }
    out[offset..end].copy_from_slice(value);
    Ok(end)
}

#[allow(clippy::too_many_arguments)]
fn apply_encoded_list_direct_with_options(
    env: &mut JNIEnv,
    handle: jlong,
    bucket: jint,
    key_address: jlong,
    key_capacity: jint,
    key_length: jint,
    column: jint,
    encoded_list_address: jlong,
    encoded_list_capacity: jint,
    encoded_list_length: jint,
    write_options_handle: jlong,
    merge: bool,
) {
    let Some(db) = db_from_handle(env, handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return;
        }
    };
    let column = match decode_u16("column", column) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return;
        }
    };
    let Some(key) = decode_direct_buffer_slice(env, "key", key_address, key_capacity, key_length)
    else {
        return;
    };
    let Some(encoded_list) = decode_direct_buffer_slice(
        env,
        "encodedList",
        encoded_list_address,
        encoded_list_capacity,
        encoded_list_length,
    ) else {
        return;
    };
    let encoded = Bytes::copy_from_slice(encoded_list);
    let result = if write_options_handle == 0 {
        if merge {
            db.merge_encoded_list(bucket, key, column, encoded)
        } else {
            db.put_encoded_list(bucket, key, column, encoded)
        }
    } else {
        let Some(wo) = structured_write_options_from_handle_or_throw(env, write_options_handle)
        else {
            return;
        };
        if merge {
            db.merge_encoded_list_with_options(bucket, key, column, encoded, wo.write_options())
        } else {
            db.put_encoded_list_with_options(bucket, key, column, encoded, wo.write_options())
        }
    };
    if let Err(err) = result {
        throw_illegal_state(env, err.to_string());
    }
}

fn decode_direct_buffer_slice<'a>(
    env: &mut JNIEnv,
    label: &str,
    address: jlong,
    capacity: jint,
    length: jint,
) -> Option<&'a [u8]> {
    let capacity = match usize::try_from(capacity) {
        Ok(v) => v,
        Err(_) => {
            throw_illegal_argument(env, format!("{label}Capacity must be >= 0"));
            return None;
        }
    };
    let length = match usize::try_from(length) {
        Ok(v) => v,
        Err(_) => {
            throw_illegal_argument(env, format!("{label}Length must be >= 0"));
            return None;
        }
    };
    let address = match usize::try_from(address) {
        Ok(v) if v != 0 => v as *const u8,
        _ => {
            throw_illegal_argument(env, format!("{label}Address must be > 0"));
            return None;
        }
    };
    if length > capacity {
        throw_illegal_argument(
            env,
            format!("{label}Length {length} exceeds direct buffer capacity {capacity}"),
        );
        return None;
    }
    Some(unsafe { std::slice::from_raw_parts(address, length) })
}

// ── typed scan ──────────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_openStructuredScanCursor(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    start_key_inclusive: JByteArray,
    end_key_exclusive: JByteArray,
    scan_options_handle: jlong,
) -> jlong {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return 0;
    };
    let Some(args) = decode_structured_scan_open_args(
        &mut env,
        bucket,
        start_key_inclusive,
        end_key_exclusive,
        scan_options_handle,
    ) else {
        return 0;
    };
    let batch_size = args.batch_size;
    let range = args.start_key_inclusive.as_slice()..args.end_key_exclusive.as_slice();
    let iter = if let Some(scan_options_handle) = args.scan_options_handle {
        match db.scan_with_options(args.bucket, range, scan_options_handle.scan_options()) {
            Ok(iter) => iter,
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                return 0;
            }
        }
    } else {
        match db.scan(args.bucket, range) {
            Ok(iter) => iter,
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                return 0;
            }
        }
    };
    // SAFETY: StructuredDbIterator borrows from DataStructureDb which is alive as long as
    // the Java Db object keeps the native handle.
    let iter = unsafe {
        std::mem::transmute::<StructuredDbIterator<'_>, StructuredDbIterator<'static>>(iter)
    };
    let cursor = StructuredScanCursorHandle::new(iter, batch_size);
    Box::into_raw(Box::new(cursor)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_openStructuredDirectScanCursor(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    start_key_address: jlong,
    start_key_length: jint,
    end_key_address: jlong,
    end_key_length: jint,
    scan_options_handle: jlong,
) -> jlong {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return 0;
    };
    let Some(args) = decode_structured_scan_open_direct_args(
        &mut env,
        bucket,
        start_key_address,
        start_key_length,
        end_key_address,
        end_key_length,
        scan_options_handle,
    ) else {
        return 0;
    };
    let batch_size = args.batch_size;
    let range = args.start_key_inclusive.as_slice()..args.end_key_exclusive.as_slice();
    let iter = if let Some(scan_options_handle) = args.scan_options_handle {
        match db.scan_with_options(args.bucket, range, scan_options_handle.scan_options()) {
            Ok(iter) => iter,
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                return 0;
            }
        }
    } else {
        match db.scan(args.bucket, range) {
            Ok(iter) => iter,
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                return 0;
            }
        }
    };
    let iter = unsafe {
        std::mem::transmute::<StructuredDbIterator<'_>, StructuredDbIterator<'static>>(iter)
    };
    let cursor = StructuredScanCursorHandle::new(iter, batch_size);
    Box::into_raw(Box::new(cursor)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_StructuredScanSplit_openStructuredSplitScanCursor(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    split_json: JString,
    scan_options_handle: jlong,
) -> jlong {
    let config_path = match decode_java_string(&mut env, config_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let config = match Config::from_path(&config_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    open_structured_split_scan_cursor(&mut env, config, split_json, scan_options_handle)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_StructuredScanSplit_openStructuredSplitScanCursorFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    split_json: JString,
    scan_options_handle: jlong,
) -> jlong {
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return 0;
    };
    open_structured_split_scan_cursor(&mut env, config, split_json, scan_options_handle)
}

fn open_structured_split_scan_cursor(
    env: &mut JNIEnv,
    config: Config,
    split_json: JString,
    scan_options_handle: jlong,
) -> jlong {
    let split_json = match decode_java_string(env, split_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return 0;
        }
    };
    let split = match serde_json::from_str::<StructuredScanSplit>(&split_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, format!("invalid structured scan split json: {}", err));
            return 0;
        }
    };
    let (scanner, batch_size) = if scan_options_handle == 0 {
        match split.create_scanner_without_options(config) {
            Ok(v) => (v, DEFAULT_SCAN_BATCH_SIZE),
            Err(err) => {
                throw_illegal_state(env, err.to_string());
                return 0;
            }
        }
    } else {
        let Some(options_handle) =
            structured_scan_options_from_handle_or_throw(env, scan_options_handle)
        else {
            return 0;
        };
        match split.create_scanner(config, options_handle.scan_options()) {
            Ok(v) => (v, options_handle.batch_size()),
            Err(err) => {
                throw_illegal_state(env, err.to_string());
                return 0;
            }
        }
    };
    let cursor = StructuredScanCursorHandle::new_from_split_scanner(scanner, batch_size);
    Box::into_raw(Box::new(cursor)) as jlong
}

// ── metadata / time ─────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_id(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jstring {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return std::ptr::null_mut();
    };
    to_java_string_or_throw(&mut env, db.id().to_string())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_nowSeconds(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jint {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return 0;
    };
    db.now_seconds() as jint
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_setTime(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    next_seconds: jint,
) {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return;
    };
    let next = match crate::util::decode_u32("nextSeconds", next_seconds) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    db.set_time(next);
}

// ── snapshot lifecycle ──────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_asyncSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    future: JObject,
) {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return;
    };
    let vm = match env.get_java_vm() {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, format!("failed to get java vm: {}", err));
            return;
        }
    };
    let future_ref = match env.new_global_ref(future) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, format!("failed to create global ref: {}", err));
            return;
        }
    };
    match db.snapshot_with_callback(move |result| {
        complete_snapshot_json_future(
            &vm,
            &future_ref,
            result.map(|input| shard_snapshot_json(&input)),
        );
    }) {
        Ok(_) => {}
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_expireSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    snapshot_id: jlong,
) -> jboolean {
    let Some(db) = db_from_handle(&mut env, handle) else {
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
        Ok(true) => JNI_TRUE,
        Ok(false) => JNI_FALSE,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            JNI_FALSE
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_retainSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    snapshot_id: jlong,
) -> jboolean {
    let Some(db) = db_from_handle(&mut env, handle) else {
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
pub extern "system" fn Java_io_cobble_structured_Db_getShardSnapshotJson(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    snapshot_id: jlong,
) -> jstring {
    let Some(db) = db_from_handle(&mut env, handle) else {
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
pub extern "system" fn Java_io_cobble_structured_Db_expandBucket(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    source_db_id: JString,
    snapshot_id: jlong,
    range_starts: JIntArray,
    range_ends: JIntArray,
) -> jlong {
    let Some(db) = db_from_handle(&mut env, handle) else {
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
pub extern "system" fn Java_io_cobble_structured_Db_shrinkBucket(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    range_starts: JIntArray,
    range_ends: JIntArray,
) -> jlong {
    let Some(db) = db_from_handle(&mut env, handle) else {
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

// ── structured scan cursor ──────────────────────────────────────────────────

pub(crate) struct StructuredScanCursorHandle {
    iter: StructuredScanCursorIter,
    batch_size: usize,
    pending: Option<(Bytes, Vec<Option<StructuredColumnValue>>)>,
    exhausted: bool,
}

pub(crate) enum StructuredScanCursorIter {
    Db(Box<StructuredDbIterator<'static>>),
    Split(Box<StructuredScanSplitScanner>),
}

impl StructuredScanCursorHandle {
    pub(crate) fn new(iter: StructuredDbIterator<'static>, batch_size: usize) -> Self {
        Self {
            iter: StructuredScanCursorIter::Db(Box::new(iter)),
            batch_size,
            pending: None,
            exhausted: false,
        }
    }

    pub(crate) fn new_from_split_scanner(
        scanner: StructuredScanSplitScanner,
        batch_size: usize,
    ) -> Self {
        Self {
            iter: StructuredScanCursorIter::Split(Box::new(scanner)),
            batch_size,
            pending: None,
            exhausted: false,
        }
    }

    fn next_batch(&mut self) -> cobble::Result<StructuredScanBatch> {
        if self.exhausted {
            return Ok(StructuredScanBatch::empty());
        }
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(self.batch_size);
        let mut rows: Vec<Vec<Option<StructuredColumnValue>>> = Vec::with_capacity(self.batch_size);

        if let Some((key, cols)) = self.pending.take() {
            keys.push(key.to_vec());
            rows.push(cols);
        }
        while keys.len() < self.batch_size {
            match self.next_row() {
                Some(Ok((key, cols))) => {
                    keys.push(key.to_vec());
                    rows.push(cols);
                }
                Some(Err(err)) => return Err(err),
                None => {
                    self.exhausted = true;
                    break;
                }
            }
        }
        if keys.is_empty() {
            self.exhausted = true;
            return Ok(StructuredScanBatch::empty());
        }
        let mut has_more = false;
        if !self.exhausted {
            match self.next_row() {
                Some(Ok((key, cols))) => {
                    self.pending = Some((key, cols));
                    has_more = true;
                }
                Some(Err(err)) => return Err(err),
                None => {
                    self.exhausted = true;
                }
            }
        }
        Ok(StructuredScanBatch {
            next_start_after_exclusive: keys.last().cloned(),
            keys,
            rows,
            has_more,
        })
    }

    fn next_batch_direct<'local>(
        &mut self,
        env: &mut JNIEnv<'local>,
        io_addr: *mut u8,
        io_capacity: usize,
    ) -> jint {
        if self.exhausted {
            return 0;
        }
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&0u32.to_be_bytes());
        encoded.push(0);
        let mut row_count = 0usize;

        if let Some((key, cols)) = self.pending.take() {
            if let Err(err) = append_direct_scan_row_payload(&mut encoded, &key, &cols) {
                throw_illegal_state(env, err);
                return 0;
            }
            row_count += 1;
        }

        while row_count < self.batch_size {
            match self.next_row() {
                Some(Ok((key, cols))) => {
                    if let Err(err) = append_direct_scan_row_payload(&mut encoded, &key, &cols) {
                        throw_illegal_state(env, err);
                        return 0;
                    }
                    row_count += 1;
                }
                Some(Err(err)) => {
                    throw_illegal_state(env, err.to_string());
                    return 0;
                }
                None => {
                    self.exhausted = true;
                    break;
                }
            }
        }

        if row_count == 0 {
            self.exhausted = true;
            return 0;
        }

        let mut has_more = false;
        if !self.exhausted {
            match self.next_row() {
                Some(Ok((key, cols))) => {
                    self.pending = Some((key, cols));
                    has_more = true;
                }
                Some(Err(err)) => {
                    throw_illegal_state(env, err.to_string());
                    return 0;
                }
                None => {
                    self.exhausted = true;
                }
            }
        }

        encoded[0..4].copy_from_slice(&(row_count as u32).to_be_bytes());
        encoded[4] = if has_more { 1 } else { 0 };
        match write_payload_to_io_or_cached_overflow(env, io_addr, io_capacity, &encoded) {
            Ok(v) => v,
            Err(err) => {
                throw_illegal_state(env, err);
                0
            }
        }
    }

    fn next_row(&mut self) -> Option<cobble::Result<(Bytes, Vec<Option<StructuredColumnValue>>)>> {
        match &mut self.iter {
            StructuredScanCursorIter::Db(iter) => iter.as_mut().next(),
            StructuredScanCursorIter::Split(iter) => iter.as_mut().next(),
        }
    }
}

struct StructuredScanBatch {
    keys: Vec<Vec<u8>>,
    rows: Vec<Vec<Option<StructuredColumnValue>>>,
    next_start_after_exclusive: Option<Vec<u8>>,
    has_more: bool,
}

impl StructuredScanBatch {
    fn empty() -> Self {
        Self {
            keys: Vec::new(),
            rows: Vec::new(),
            next_start_after_exclusive: None,
            has_more: false,
        }
    }
}

fn append_direct_scan_row_payload(
    encoded: &mut Vec<u8>,
    key: &[u8],
    columns: &[Option<StructuredColumnValue>],
) -> Result<(), String> {
    let row_size = encoded_row_size(columns)?;
    let needed = encoded
        .len()
        .checked_add(4)
        .and_then(|v| v.checked_add(key.len()))
        .and_then(|v| v.checked_add(4))
        .and_then(|v| v.checked_add(row_size))
        .ok_or_else(|| "encoded direct scan batch size overflow".to_string())?;
    let previous_len = encoded.len();
    encoded.resize(needed, 0);
    encoded[previous_len..previous_len + 4].copy_from_slice(&(key.len() as u32).to_be_bytes());
    let mut cursor = previous_len + 4;
    encoded[cursor..cursor + key.len()].copy_from_slice(key);
    cursor += key.len();
    encoded[cursor..cursor + 4].copy_from_slice(&(row_size as u32).to_be_bytes());
    cursor += 4;
    encode_row_payload(columns, &mut encoded[cursor..cursor + row_size])?;
    Ok(())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ScanCursor_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "structured scan cursor handle is already disposed".to_string(),
        );
        return;
    }
    let _boxed = unsafe { Box::from_raw(native_handle as *mut StructuredScanCursorHandle) };
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_DirectScanCursor_disposeInternal(
    env: JNIEnv,
    obj: JObject,
    native_handle: jlong,
) {
    Java_io_cobble_structured_ScanCursor_disposeInternal(env, obj, native_handle);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ScanCursor_nextBatchInternal(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jobject {
    let Some(cursor) = structured_scan_cursor_from_handle(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let batch = match cursor.next_batch() {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    match to_java_structured_scan_batch(&mut env, batch) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err);
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_DirectScanCursor_nextBatchDirectInternal(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    io_address: jlong,
    io_capacity: jint,
) -> jint {
    let Some(cursor) = structured_scan_cursor_from_handle(&mut env, native_handle) else {
        return 0;
    };
    let io_capacity = match usize::try_from(io_capacity) {
        Ok(v) => v,
        Err(_) => {
            throw_illegal_argument(&mut env, "ioCapacity must be >= 0".to_string());
            return 0;
        }
    };
    let io_address = match usize::try_from(io_address) {
        Ok(v) if v != 0 => v as *mut u8,
        _ => {
            throw_illegal_argument(&mut env, "ioAddress must be > 0".to_string());
            return 0;
        }
    };
    cursor.next_batch_direct(&mut env, io_address, io_capacity)
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn db_from_handle(env: &mut JNIEnv, handle: jlong) -> Option<&'static DataStructureDb> {
    if handle == 0 {
        throw_illegal_state(env, "structured db handle is disposed".to_string());
        return None;
    }
    Some(unsafe { &*(handle as *const DataStructureDb) })
}

fn db_from_handle_mut(env: &mut JNIEnv, handle: jlong) -> Option<&'static mut DataStructureDb> {
    if handle == 0 {
        throw_illegal_state(env, "structured db handle is disposed".to_string());
        return None;
    }
    Some(unsafe { &mut *(handle as *mut DataStructureDb) })
}

fn structured_builder_from_handle(
    env: &mut JNIEnv,
    handle: jlong,
) -> Option<&'static mut StructuredSchemaBuilderHandle> {
    if handle == 0 {
        throw_illegal_state(
            env,
            "structured schema builder handle is disposed".to_string(),
        );
        return None;
    }
    Some(unsafe { &mut *(handle as *mut StructuredSchemaBuilderHandle) })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_StructuredSchemaBuilder_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "structured schema builder handle is already disposed".to_string(),
        );
        return;
    }
    let _boxed = unsafe { Box::from_raw(native_handle as *mut StructuredSchemaBuilderHandle) };
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_StructuredSchemaBuilder_nativeAddBytesColumn(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column_family: JString,
    column_idx: jint,
) {
    let Some(builder) = structured_builder_from_handle(&mut env, native_handle) else {
        return;
    };
    let column_family = match decode_optional_java_string(&mut env, column_family) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let column = match decode_u16("column", column_idx) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    match builder {
        StructuredSchemaBuilderHandle::Db(b) => {
            b.add_bytes_column(column_family, column);
        }
        StructuredSchemaBuilderHandle::SingleDb(b) => {
            b.add_bytes_column(column_family, column);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_StructuredSchemaBuilder_nativeAddListColumn(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column_family: JString,
    column_idx: jint,
    max_elements: jint,
    retain_mode: JString,
    preserve_element_ttl: jboolean,
) {
    let Some(builder) = structured_builder_from_handle(&mut env, native_handle) else {
        return;
    };
    let column_family = match decode_optional_java_string(&mut env, column_family) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let column = match decode_u16("column", column_idx) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let retain_mode = match decode_java_string(&mut env, retain_mode) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let retain_mode = match retain_mode.as_str() {
        "first" => cobble_data_structure::ListRetainMode::First,
        "last" => cobble_data_structure::ListRetainMode::Last,
        _ => {
            throw_illegal_argument(
                &mut env,
                format!(
                    "retainMode must be 'first' or 'last', got '{}'",
                    retain_mode
                ),
            );
            return;
        }
    };
    let max_elements = if max_elements < 0 {
        None
    } else {
        Some(max_elements as usize)
    };
    let config = cobble_data_structure::ListConfig {
        max_elements,
        retain_mode,
        preserve_element_ttl: preserve_element_ttl == JNI_TRUE,
    };
    match builder {
        StructuredSchemaBuilderHandle::Db(b) => {
            b.add_list_column(column_family.clone(), column, config.clone());
        }
        StructuredSchemaBuilderHandle::SingleDb(b) => {
            b.add_list_column(column_family, column, config);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_StructuredSchemaBuilder_nativeDeleteColumn(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column_family: JString,
    column_idx: jint,
) {
    let Some(builder) = structured_builder_from_handle(&mut env, native_handle) else {
        return;
    };
    let column_family = match decode_optional_java_string(&mut env, column_family) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let column = match decode_u16("column", column_idx) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    match builder {
        StructuredSchemaBuilderHandle::Db(b) => {
            b.delete_column(column_family.clone(), column);
        }
        StructuredSchemaBuilderHandle::SingleDb(b) => {
            b.delete_column(column_family, column);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_StructuredSchemaBuilder_nativeCommit(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "structured schema builder handle is disposed".to_string(),
        );
        return std::ptr::null_mut();
    }
    let handle = unsafe { Box::from_raw(native_handle as *mut StructuredSchemaBuilderHandle) };
    let commit_result = match *handle {
        StructuredSchemaBuilderHandle::Db(mut builder) => builder.commit(),
        StructuredSchemaBuilderHandle::SingleDb(mut builder) => builder.commit(),
    };
    match commit_result {
        Ok(schema) => match serde_json::to_string(&schema) {
            Ok(v) => to_java_string_or_throw(&mut env, v),
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                std::ptr::null_mut()
            }
        },
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            std::ptr::null_mut()
        }
    }
}

fn structured_scan_cursor_from_handle(
    env: &mut JNIEnv,
    handle: jlong,
) -> Option<&'static mut StructuredScanCursorHandle> {
    if handle == 0 {
        throw_illegal_state(env, "structured scan cursor handle is disposed".to_string());
        return None;
    }
    Some(unsafe { &mut *(handle as *mut StructuredScanCursorHandle) })
}

pub(crate) fn decode_write_bytes_args(
    env: &mut JNIEnv,
    bucket: jint,
    column: jint,
    key: JByteArray,
    value: JByteArray,
) -> Option<(u16, u16, Vec<u8>, Vec<u8>)> {
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    let column = match decode_u16("column", column) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    let key = match decode_java_bytes(env, key) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    let value = match decode_java_bytes(env, value) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    Some((bucket, column, key, value))
}

pub(crate) fn decode_write_list_args(
    env: &mut JNIEnv,
    bucket: jint,
    column: jint,
    key: JByteArray,
    elements: JObjectArray,
) -> Option<(u16, u16, Vec<u8>, Vec<Bytes>)> {
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    let column = match decode_u16("column", column) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    let key = match decode_java_bytes(env, key) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    let list = match decode_java_bytes_2d(env, elements) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    Some((bucket, column, key, list))
}

fn decode_java_bytes_2d(
    env: &mut JNIEnv,
    array: JObjectArray,
) -> std::result::Result<Vec<Bytes>, String> {
    let len = env
        .get_array_length(&array)
        .map_err(|err| format!("invalid list elements array: {}", err))?;
    let mut result = Vec::with_capacity(len as usize);
    for i in 0..len {
        let element = env
            .get_object_array_element(&array, i)
            .map_err(|err| format!("failed to read list element {}: {}", i, err))?;
        let byte_array: JByteArray = element.into();
        let bytes = env
            .convert_byte_array(byte_array)
            .map_err(|err| format!("failed to convert list element {}: {}", i, err))?;
        result.push(Bytes::from(bytes));
    }
    Ok(result)
}

pub(crate) fn to_java_typed_columns(
    env: &mut JNIEnv,
    columns: Vec<Option<StructuredColumnValue>>,
) -> std::result::Result<jobject, String> {
    let object_class = object_class(env)?;
    let byte_array_class = byte_array_class(env)?;
    let array = new_object_array(env, columns.len() as i32, object_class)?;
    for (i, col) in columns.into_iter().enumerate() {
        match col {
            None => {} // null already
            Some(StructuredColumnValue::Bytes(b)) => {
                let byte_arr = env
                    .byte_array_from_slice(&b)
                    .map_err(|err| err.to_string())?;
                env.set_object_array_element(&array, i as i32, byte_arr)
                    .map_err(|err| err.to_string())?;
            }
            Some(StructuredColumnValue::List(elements)) => {
                let inner = new_object_array(env, elements.len() as i32, byte_array_class)?;
                for (j, elem) in elements.into_iter().enumerate() {
                    let elem_arr = env
                        .byte_array_from_slice(&elem)
                        .map_err(|err| err.to_string())?;
                    env.set_object_array_element(&inner, j as i32, elem_arr)
                        .map_err(|err| err.to_string())?;
                }
                env.set_object_array_element(&array, i as i32, inner)
                    .map_err(|err| err.to_string())?;
            }
        }
    }
    Ok(array.into_raw() as jobject)
}

fn to_java_structured_scan_batch(
    env: &mut JNIEnv,
    batch: StructuredScanBatch,
) -> std::result::Result<jobject, String> {
    // keys: byte[][]
    let byte_array_class = byte_array_class(env)?;
    let object_array_class = object_array_class(env)?;
    let object_class = object_class(env)?;
    let keys_array = new_object_array(env, batch.keys.len() as i32, byte_array_class)?;
    for (i, key) in batch.keys.iter().enumerate() {
        let arr = env
            .byte_array_from_slice(key)
            .map_err(|err| err.to_string())?;
        env.set_object_array_element(&keys_array, i as i32, arr)
            .map_err(|err| err.to_string())?;
    }

    // rawColumns: Object[][] where each row is Object[] with null | byte[] | byte[][]
    let rows_array = new_object_array(env, batch.rows.len() as i32, object_array_class)?;
    for (i, row) in batch.rows.into_iter().enumerate() {
        let col_array = new_object_array(env, row.len() as i32, object_class)?;
        for (j, col) in row.into_iter().enumerate() {
            match col {
                None => {} // null already
                Some(StructuredColumnValue::Bytes(b)) => {
                    let arr = env
                        .byte_array_from_slice(&b)
                        .map_err(|err| err.to_string())?;
                    env.set_object_array_element(&col_array, j as i32, arr)
                        .map_err(|err| err.to_string())?;
                }
                Some(StructuredColumnValue::List(elements)) => {
                    let inner = new_object_array(env, elements.len() as i32, byte_array_class)?;
                    for (k, elem) in elements.into_iter().enumerate() {
                        let elem_arr = env
                            .byte_array_from_slice(&elem)
                            .map_err(|err| err.to_string())?;
                        env.set_object_array_element(&inner, k as i32, elem_arr)
                            .map_err(|err| err.to_string())?;
                    }
                    env.set_object_array_element(&col_array, j as i32, inner)
                        .map_err(|err| err.to_string())?;
                }
            }
        }
        env.set_object_array_element(&rows_array, i as i32, col_array)
            .map_err(|err| err.to_string())?;
    }

    // next_start_after_exclusive: byte[]
    let next_raw = match batch.next_start_after_exclusive {
        Some(v) => env
            .byte_array_from_slice(&v)
            .map(|arr| arr.into_raw() as jobject)
            .map_err(|err| err.to_string())?,
        None => std::ptr::null_mut(),
    };

    let keys_obj = unsafe { JObject::from_raw(keys_array.into_raw() as jobject) };
    let rows_obj = unsafe { JObject::from_raw(rows_array.into_raw() as jobject) };
    let next_obj = unsafe { JObject::from_raw(next_raw) };

    let result = new_structured_scan_batch(env, &keys_obj, &rows_obj, &next_obj, batch.has_more)?;
    Ok(result.into_raw())
}

fn build_shard_snapshot_payload(
    db: &DataStructureDb,
    snapshot_id: u64,
) -> std::result::Result<String, String> {
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
