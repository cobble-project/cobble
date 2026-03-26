use crate::util::{
    decode_column_index, decode_java_bytes, decode_java_string, decode_u16, decode_u64_from_jlong,
    throw_illegal_argument, throw_illegal_state, to_java_string_or_throw,
};
use cobble::{Config, Db, ReadOptions};
use jni::JNIEnv;
use jni::JavaVM;
use jni::objects::{GlobalRef, JByteArray, JClass, JObject, JString, JValue};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jbyteArray, jint, jlong, jstring};
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
    let total_buckets = config.total_buckets;
    if total_buckets == 0 || total_buckets > (u16::MAX as u32) + 1 {
        throw_illegal_argument(
            &mut env,
            format!("invalid total_buckets in config: {}", total_buckets),
        );
        return 0;
    }
    let range = 0..=((total_buckets - 1) as u16);
    let db = match Db::open(config, vec![range]) {
        Ok(db) => db,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(db)) as jlong
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
    let config = match serde_json::from_str::<Config>(&json) {
        Ok(config) => config,
        Err(err) => {
            throw_illegal_argument(&mut env, format!("invalid config json: {}", err));
            return 0;
        }
    };
    let total_buckets = config.total_buckets;
    if total_buckets == 0 || total_buckets > (u16::MAX as u32) + 1 {
        throw_illegal_argument(
            &mut env,
            format!("invalid total_buckets in config: {}", total_buckets),
        );
        return 0;
    }
    let range = 0..=((total_buckets - 1) as u16);
    let db = match Db::open(config, vec![range]) {
        Ok(db) => db,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(db)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_openFromSnapshotHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    snapshot_id: jlong,
    db_id: JString,
) -> jlong {
    let path = match decode_java_string(&mut env, config_path) {
        Ok(path) => path,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(v) => v,
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
    let db = match Db::open_from_snapshot(config, snapshot_id, db_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(db)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_openFromSnapshotHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    snapshot_id: jlong,
    db_id: JString,
) -> jlong {
    let json = match decode_java_string(&mut env, config_json) {
        Ok(json) => json,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(v) => v,
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
    let config = match serde_json::from_str::<Config>(&json) {
        Ok(config) => config,
        Err(err) => {
            throw_illegal_argument(&mut env, format!("invalid config json: {}", err));
            return 0;
        }
    };
    let db = match Db::open_from_snapshot(config, snapshot_id, db_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(db)) as jlong
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
    let config = match serde_json::from_str::<Config>(&json) {
        Ok(config) => config,
        Err(err) => {
            throw_illegal_argument(&mut env, format!("invalid config json: {}", err));
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
pub extern "system" fn Java_io_cobble_Db_get(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    column: jint,
) -> jbyteArray {
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
    let values = match db.get(bucket, &key, &options) {
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
        complete_snapshot_id_future(&vm, &future, result);
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

fn db_from_handle_or_throw(env: &mut JNIEnv, native_handle: jlong) -> Option<&'static Db> {
    if native_handle == 0 {
        throw_illegal_state(env, "db handle is disposed".to_string());
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<Db>` and valid until `disposeInternal`.
    Some(unsafe { &*(native_handle as *const Db) })
}

fn build_shard_snapshot_payload(db: &Db, snapshot_id: u64) -> std::result::Result<String, String> {
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
