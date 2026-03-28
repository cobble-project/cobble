use crate::read_options::read_options_from_handle_or_throw;
use crate::util::{
    decode_java_bytes, decode_java_string, decode_u16, throw_illegal_argument, throw_illegal_state,
    to_java_optional_bytes_2d,
};
use cobble::{Config, SingleDb};
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JObject, JString};
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
