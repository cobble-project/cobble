use crate::read_options::read_options_from_handle_or_throw;
use crate::scan::{ScanCursorHandle, decode_scan_open_args};
use crate::util::{
    decode_java_bytes, decode_java_string, decode_u16, decode_u32, decode_u64_from_jlong,
    throw_illegal_argument, throw_illegal_state, to_java_optional_bytes_2d,
    to_java_string_or_throw,
};
use crate::write_options::write_options_from_handle_or_throw;
use cobble::{Config, SingleDb};
use jni::JNIEnv;
use jni::JavaVM;
use jni::objects::{GlobalRef, JByteArray, JClass, JObject, JString, JValue};
use jni::sys::{jint, jlong, jobject};

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SingleDb_openHandle(
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
    let db = match SingleDb::open(config) {
        Ok(db) => db,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(db)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SingleDb_openHandleFromJson(
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
    let db = match SingleDb::open(config) {
        Ok(db) => db,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(db)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SingleDb_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(&mut env, "single db handle is already disposed".to_string());
        return;
    }
    let ptr = native_handle as *mut SingleDb;
    // SAFETY: `native_handle` is returned by `SingleDb.openHandle` from `Box<SingleDb>`.
    let boxed = unsafe { Box::from_raw(ptr) };
    if let Err(err) = boxed.close() {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SingleDb_put(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
) {
    let Some(db) = single_db_from_handle_or_throw(&mut env, native_handle) else {
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
pub extern "system" fn Java_io_cobble_SingleDb_putWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
    write_options_handle: jlong,
) {
    let Some(db) = single_db_from_handle_or_throw(&mut env, native_handle) else {
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
pub extern "system" fn Java_io_cobble_SingleDb_merge(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
) {
    let Some(db) = single_db_from_handle_or_throw(&mut env, native_handle) else {
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
pub extern "system" fn Java_io_cobble_SingleDb_mergeWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
    value: JByteArray,
    write_options_handle: jlong,
) {
    let Some(db) = single_db_from_handle_or_throw(&mut env, native_handle) else {
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
pub extern "system" fn Java_io_cobble_SingleDb_get(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    read_options_handle: jlong,
) -> jobject {
    let Some(db) = single_db_from_handle_or_throw(&mut env, native_handle) else {
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
pub extern "system" fn Java_io_cobble_SingleDb_delete(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
) {
    let Some(db) = single_db_from_handle_or_throw(&mut env, native_handle) else {
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
pub extern "system" fn Java_io_cobble_SingleDb_setTime(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    next_seconds: jint,
) {
    let Some(db) = single_db_from_handle_or_throw(&mut env, native_handle) else {
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
pub extern "system" fn Java_io_cobble_SingleDb_asyncSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    snapshot_future_json: JObject,
) {
    let Some(db) = single_db_from_handle_or_throw(&mut env, native_handle) else {
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
        complete_snapshot_id_future(&vm, &future, result);
    }) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SingleDb_retainSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    snapshot_id: jlong,
) -> jni::sys::jboolean {
    let Some(db) = single_db_from_handle_or_throw(&mut env, native_handle) else {
        return jni::sys::JNI_FALSE;
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return jni::sys::JNI_FALSE;
        }
    };
    match db.retain_snapshot(snapshot_id) {
        Ok(true) => jni::sys::JNI_TRUE,
        Ok(false) => jni::sys::JNI_FALSE,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            jni::sys::JNI_FALSE
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SingleDb_expireSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    snapshot_id: jlong,
) -> jni::sys::jboolean {
    let Some(db) = single_db_from_handle_or_throw(&mut env, native_handle) else {
        return jni::sys::JNI_FALSE;
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return jni::sys::JNI_FALSE;
        }
    };
    match db.expire_snapshot(snapshot_id) {
        Ok(true) => jni::sys::JNI_TRUE,
        Ok(false) => jni::sys::JNI_FALSE,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            jni::sys::JNI_FALSE
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SingleDb_listSnapshotsJson(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jni::sys::jstring {
    let Some(db) = single_db_from_handle_or_throw(&mut env, native_handle) else {
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

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SingleDb_openScanCursor(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    start_key_inclusive: JByteArray,
    end_key_exclusive: JByteArray,
    scan_options_handle: jlong,
) -> jlong {
    let Some(db) = single_db_from_handle_or_throw(&mut env, native_handle) else {
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

fn single_db_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static SingleDb> {
    if native_handle == 0 {
        throw_illegal_state(env, "single db handle is disposed".to_string());
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<SingleDb>` and valid until `disposeInternal`.
    Some(unsafe { &*(native_handle as *const SingleDb) })
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
