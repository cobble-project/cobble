// JNI bridge for io.cobble.structured.SingleDb
//
// Wraps StructuredSingleDb from cobble-data-structure.
// Typed put/merge accept both bytes (byte[]) and list (byte[][]).
// Typed get returns Object[] where each element is null | byte[] | byte[][].

use crate::read_options::read_options_from_handle_or_throw;
use crate::scan::decode_scan_open_args;
use crate::structured::{
    StructuredScanCursorHandle, decode_write_bytes_args, decode_write_list_args,
    open_structured_schema, to_java_typed_columns,
};
use crate::util::{
    decode_java_string, decode_u16, decode_u32, decode_u64_from_jlong, throw_illegal_argument,
    throw_illegal_state, to_java_string_or_throw,
};
use crate::write_options::write_options_from_handle_or_throw;
use bytes::Bytes;
use cobble::Config;
use cobble_data_structure::{StructuredColumnValue, StructuredSingleDb};
use jni::JNIEnv;
use jni::JavaVM;
use jni::objects::{GlobalRef, JByteArray, JClass, JObject, JObjectArray, JString, JValue};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint, jlong, jobject, jstring};

// ── open ────────────────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_SingleDb_openHandle(
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
    open_structured_single_db(&mut env, config, schema_json)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_SingleDb_openHandleFromJson(
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
    open_structured_single_db(&mut env, config, schema_json)
}

fn open_structured_single_db(env: &mut JNIEnv, config: Config, schema_json: JString) -> jlong {
    let schema = match open_structured_schema(env, schema_json) {
        Some(v) => v,
        None => return 0,
    };
    match StructuredSingleDb::open(config, schema) {
        Ok(db) => Box::into_raw(Box::new(db)) as jlong,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            0
        }
    }
}

// ── dispose ─────────────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_SingleDb_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "structured single db handle is already disposed".to_string(),
        );
        return;
    }
    let db = unsafe { Box::from_raw(native_handle as *mut StructuredSingleDb) };
    if let Err(err) = db.close() {
        throw_illegal_state(&mut env, err.to_string());
    }
}

// ── bytes put / merge ───────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_SingleDb_putBytes(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
) {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
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
pub extern "system" fn Java_io_cobble_structured_SingleDb_putBytesWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
    write_options_handle: jlong,
) {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
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
pub extern "system" fn Java_io_cobble_structured_SingleDb_mergeBytes(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
) {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
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
pub extern "system" fn Java_io_cobble_structured_SingleDb_mergeBytesWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
    write_options_handle: jlong,
) {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
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
pub extern "system" fn Java_io_cobble_structured_SingleDb_putList(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    elements: JObjectArray,
) {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
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
pub extern "system" fn Java_io_cobble_structured_SingleDb_putListWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    elements: JObjectArray,
    write_options_handle: jlong,
) {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
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
pub extern "system" fn Java_io_cobble_structured_SingleDb_mergeList(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    elements: JObjectArray,
) {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
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
pub extern "system" fn Java_io_cobble_structured_SingleDb_mergeListWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    elements: JObjectArray,
    write_options_handle: jlong,
) {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
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
pub extern "system" fn Java_io_cobble_structured_SingleDb_delete(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
) {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
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
    let key = match crate::util::decode_java_bytes(&mut env, key) {
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
pub extern "system" fn Java_io_cobble_structured_SingleDb_getTyped(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
) -> jobject {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
        return std::ptr::null_mut();
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let key = match crate::util::decode_java_bytes(&mut env, key) {
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
pub extern "system" fn Java_io_cobble_structured_SingleDb_getTypedWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    key: JByteArray,
    read_options_handle: jlong,
) -> jobject {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
        return std::ptr::null_mut();
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let key = match crate::util::decode_java_bytes(&mut env, key) {
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
pub extern "system" fn Java_io_cobble_structured_SingleDb_openStructuredScanCursor(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    start_key_inclusive: JByteArray,
    end_key_exclusive: JByteArray,
    scan_options_handle: jlong,
) -> jlong {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
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
    // SAFETY: StructuredDbIterator borrows from StructuredSingleDb which is alive as long as
    // the Java SingleDb object keeps the native handle.
    let iter = unsafe {
        std::mem::transmute::<
            cobble_data_structure::StructuredDbIterator<'_>,
            cobble_data_structure::StructuredDbIterator<'static>,
        >(iter)
    };
    let cursor = StructuredScanCursorHandle::new(iter, batch_size);
    Box::into_raw(Box::new(cursor)) as jlong
}

// ── metadata / time ─────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_SingleDb_nowSeconds(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jint {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
        return 0;
    };
    db.db().db().now_seconds() as jint
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_SingleDb_setTime(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    next_seconds: jint,
) {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
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

// ── snapshot lifecycle ──────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_SingleDb_asyncSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    future: JObject,
) {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
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
pub extern "system" fn Java_io_cobble_structured_SingleDb_retainSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    snapshot_id: jlong,
) -> jboolean {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
        return JNI_FALSE;
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return JNI_FALSE;
        }
    };
    match db.retain_snapshot(snapshot_id) {
        Ok(true) => JNI_TRUE,
        Ok(false) => JNI_FALSE,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            JNI_FALSE
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_SingleDb_expireSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    snapshot_id: jlong,
) -> jboolean {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
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
pub extern "system" fn Java_io_cobble_structured_SingleDb_listSnapshotsJson(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jstring {
    let Some(db) = single_db_from_handle(&mut env, handle) else {
        return std::ptr::null_mut();
    };
    let snapshots = match db.list_snapshots() {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    let json = match serde_json::to_string(&snapshots) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    to_java_string_or_throw(&mut env, json)
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn single_db_from_handle(env: &mut JNIEnv, handle: jlong) -> Option<&'static StructuredSingleDb> {
    if handle == 0 {
        throw_illegal_state(env, "structured single db handle is disposed".to_string());
        return None;
    }
    Some(unsafe { &*(handle as *const StructuredSingleDb) })
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
