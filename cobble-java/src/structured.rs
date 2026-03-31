// JNI bridge for io.cobble.structured.Db and io.cobble.structured.ScanCursor
//
// Typed put/merge accept both bytes (byte[]) and list (byte[][]).
// Typed get returns Object[] where each element is null | byte[] | byte[][].
// Typed scan cursor yields structured batches with mixed column types.

use crate::read_options::read_options_from_handle_or_throw;
use crate::scan::decode_scan_open_args;
use crate::util::{
    decode_java_bytes, decode_java_string, decode_u16, decode_u64_from_jlong,
    throw_illegal_argument, throw_illegal_state, to_java_string_or_throw,
};
use crate::write_options::write_options_from_handle_or_throw;
use bytes::Bytes;
use cobble::Config;
use cobble_data_structure::{
    DataStructureDb, StructuredColumnValue, StructuredDbIterator, StructuredSchema,
};
use jni::JNIEnv;
use jni::JavaVM;
use jni::objects::{GlobalRef, JByteArray, JClass, JObject, JObjectArray, JString, JValue};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint, jlong, jobject, jstring};
use serde_json::json;

// ── open ────────────────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_openHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    schema_json: JString,
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
    open_structured_db(&mut env, config, schema_json)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_openHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    schema_json: JString,
) -> jlong {
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let config: Config = match serde_json::from_str(&config_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    open_structured_db(&mut env, config, schema_json)
}

fn open_structured_db(env: &mut JNIEnv, config: Config, schema_json: JString) -> jlong {
    let schema = match open_structured_schema(env, schema_json) {
        Some(v) => v,
        None => return 0,
    };
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
    match DataStructureDb::open(config, vec![range], schema) {
        Ok(db) => Box::into_raw(Box::new(db)) as jlong,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            0
        }
    }
}

/// Parse a StructuredSchema from a JNI string (null → default).
/// Returns None if an error was thrown.
pub(crate) fn open_structured_schema(
    env: &mut JNIEnv,
    schema_json: JString,
) -> Option<StructuredSchema> {
    if env
        .is_same_object(&schema_json, JObject::null())
        .unwrap_or(true)
    {
        Some(StructuredSchema::default())
    } else {
        match decode_java_string(env, schema_json) {
            Ok(s) if s.trim().is_empty() => Some(StructuredSchema::default()),
            Ok(s) => match serde_json::from_str(&s) {
                Ok(v) => Some(v),
                Err(err) => {
                    throw_illegal_argument(env, format!("invalid schema json: {}", err));
                    None
                }
            },
            Err(err) => {
                throw_illegal_argument(env, err);
                None
            }
        }
    }
}

// ── restore / resume ────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_openFromSnapshotHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    snapshot_id: jlong,
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
    restore_structured_db(&mut env, config, snapshot_id, db_id)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_openFromSnapshotHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    snapshot_id: jlong,
    db_id: JString,
) -> jlong {
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let config: Config = match serde_json::from_str(&config_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    restore_structured_db(&mut env, config, snapshot_id, db_id)
}

fn restore_structured_db(
    env: &mut JNIEnv,
    config: Config,
    snapshot_id: jlong,
    db_id: JString,
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
    match DataStructureDb::open_from_snapshot_with_resolver(
        config,
        snapshot_id,
        db_id,
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
    let config: Config = match serde_json::from_str(&config_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
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
    let Some(wo) = write_options_from_handle_or_throw(&mut env, write_options_handle) else {
        return;
    };
    if let Err(err) = db.put_with_options(
        bucket,
        key,
        col,
        StructuredColumnValue::Bytes(Bytes::from(value_bytes)),
        wo.write_options(),
    ) {
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
    let Some(wo) = write_options_from_handle_or_throw(&mut env, write_options_handle) else {
        return;
    };
    if let Err(err) = db.merge_with_options(
        bucket,
        key,
        col,
        StructuredColumnValue::Bytes(Bytes::from(value_bytes)),
        wo.write_options(),
    ) {
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
    let Some(wo) = write_options_from_handle_or_throw(&mut env, write_options_handle) else {
        return;
    };
    if let Err(err) = db.put_with_options(
        bucket,
        key,
        col,
        StructuredColumnValue::List(list),
        wo.write_options(),
    ) {
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
    let Some(wo) = write_options_from_handle_or_throw(&mut env, write_options_handle) else {
        return;
    };
    if let Err(err) = db.merge_with_options(
        bucket,
        key,
        col,
        StructuredColumnValue::List(list),
        wo.write_options(),
    ) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

// ── delete ──────────────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_delete(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
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
    if let Err(err) = db.delete(bucket, key, column) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

// ── typed get ───────────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_getTyped(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
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
    let key = match decode_java_bytes(&mut env, key) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let values = match db.get(bucket, &key) {
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
pub extern "system" fn Java_io_cobble_structured_Db_getTypedWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
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
    let key = match decode_java_bytes(&mut env, key) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let Some(ro) = read_options_from_handle_or_throw(&mut env, read_options_handle) else {
        return std::ptr::null_mut();
    };
    let values = match db.get_with_options(bucket, &key, ro.read_options()) {
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
    let Some(args) = decode_scan_open_args(
        &mut env,
        bucket,
        start_key_inclusive,
        end_key_exclusive,
        scan_options_handle,
    ) else {
        return 0;
    };
    let batch_size = args.scan_options_handle.batch_size();
    let range = args.start_key_inclusive.as_slice()..args.end_key_exclusive.as_slice();
    let iter =
        match db.scan_with_options(args.bucket, range, args.scan_options_handle.scan_options()) {
            Ok(iter) => iter,
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                return 0;
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
        complete_snapshot_id_future(&vm, &future_ref, result);
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

// ── structured scan cursor ──────────────────────────────────────────────────

pub(crate) struct StructuredScanCursorHandle {
    iter: StructuredDbIterator<'static>,
    batch_size: usize,
    pending: Option<(Bytes, Vec<Option<StructuredColumnValue>>)>,
    exhausted: bool,
}

impl StructuredScanCursorHandle {
    pub(crate) fn new(iter: StructuredDbIterator<'static>, batch_size: usize) -> Self {
        Self {
            iter,
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
            match self.iter.next() {
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
            match self.iter.next() {
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

// ── helpers ─────────────────────────────────────────────────────────────────

fn db_from_handle(env: &mut JNIEnv, handle: jlong) -> Option<&'static DataStructureDb> {
    if handle == 0 {
        throw_illegal_state(env, "structured db handle is disposed".to_string());
        return None;
    }
    Some(unsafe { &*(handle as *const DataStructureDb) })
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
    let array = env
        .new_object_array(columns.len() as i32, "java/lang/Object", JObject::null())
        .map_err(|err| err.to_string())?;
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
                let inner = env
                    .new_object_array(elements.len() as i32, "[B", JObject::null())
                    .map_err(|err| err.to_string())?;
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
    let keys_array = env
        .new_object_array(batch.keys.len() as i32, "[B", JObject::null())
        .map_err(|err| err.to_string())?;
    for (i, key) in batch.keys.iter().enumerate() {
        let arr = env
            .byte_array_from_slice(key)
            .map_err(|err| err.to_string())?;
        env.set_object_array_element(&keys_array, i as i32, arr)
            .map_err(|err| err.to_string())?;
    }

    // rawColumns: Object[][] where each row is Object[] with null | byte[] | byte[][]
    let rows_array = env
        .new_object_array(
            batch.rows.len() as i32,
            "[Ljava/lang/Object;",
            JObject::null(),
        )
        .map_err(|err| err.to_string())?;
    for (i, row) in batch.rows.into_iter().enumerate() {
        let col_array = env
            .new_object_array(row.len() as i32, "java/lang/Object", JObject::null())
            .map_err(|err| err.to_string())?;
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
                    let inner = env
                        .new_object_array(elements.len() as i32, "[B", JObject::null())
                        .map_err(|err| err.to_string())?;
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

    let result = env
        .new_object(
            "io/cobble/structured/ScanBatch",
            "([[B[[Ljava/lang/Object;[BZ)V",
            &[
                JValue::Object(&keys_obj),
                JValue::Object(&rows_obj),
                JValue::Object(&next_obj),
                JValue::Bool(if batch.has_more { 1 } else { 0 }),
            ],
        )
        .map_err(|err| err.to_string())?;
    Ok(result.into_raw())
}

fn build_shard_snapshot_payload(
    db: &DataStructureDb,
    snapshot_id: u64,
) -> std::result::Result<String, String> {
    let input = db
        .shard_snapshot_input(snapshot_id)
        .map_err(|err| err.to_string())?;
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
    Ok(json!({
        "ranges": ranges,
        "db_id": input.db_id,
        "snapshot_id": input.snapshot_id,
        "manifest_path": input.manifest_path,
    })
    .to_string())
}

fn complete_snapshot_id_future(vm: &JavaVM, future: &GlobalRef, result: cobble::Result<u64>) {
    let mut env = match vm.attach_current_thread() {
        Ok(v) => v,
        Err(_) => return,
    };
    match result {
        Ok(snapshot_id) => {
            let snapshot_id_obj = match env.new_object(
                "java/lang/Long",
                "(J)V",
                &[JValue::Long(snapshot_id as jlong)],
            ) {
                Ok(v) => v,
                Err(_) => {
                    complete_future_exceptionally(
                        &mut env,
                        future.as_obj(),
                        "failed to allocate snapshot id object",
                    );
                    return;
                }
            };
            let _ = env.call_method(
                future.as_obj(),
                "complete",
                "(Ljava/lang/Object;)Z",
                &[JValue::Object(&snapshot_id_obj)],
            );
        }
        Err(err) => {
            complete_future_exceptionally(&mut env, future.as_obj(), &err.to_string());
        }
    }
}

fn complete_future_exceptionally(env: &mut JNIEnv, future: &JObject, message: &str) {
    let exception_obj = match env.new_string(message) {
        Ok(msg) => {
            let msg_obj = JObject::from(msg);
            match env.new_object(
                "java/lang/IllegalStateException",
                "(Ljava/lang/String;)V",
                &[JValue::Object(&msg_obj)],
            ) {
                Ok(v) => v,
                Err(_) => return,
            }
        }
        Err(_) => return,
    };
    let _ = env.call_method(
        future,
        "completeExceptionally",
        "(Ljava/lang/Throwable;)Z",
        &[JValue::Object(&exception_obj)],
    );
}
