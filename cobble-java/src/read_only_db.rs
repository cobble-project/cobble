use crate::read_options::read_options_from_handle_or_throw;
use crate::scan::{ScanCursorHandle, decode_scan_open_args};
use crate::util::{
    decode_java_bytes, decode_java_string, decode_u16, decode_u64_from_jlong, parse_config_json,
    throw_illegal_argument, throw_illegal_state, to_java_optional_bytes_2d,
    to_java_string_or_throw,
};
use cobble::{Config, ReadOnlyDb};
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jint, jlong, jobject, jstring};

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ReadOnlyDb_openHandle(
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
    let db = match ReadOnlyDb::open_with_db_id(config, snapshot_id, db_id) {
        Ok(db) => db,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(db)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ReadOnlyDb_openHandleFromJson(
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
    let Some(config) = parse_config_json(&mut env, &json) else {
        return 0;
    };
    let db = match ReadOnlyDb::open_with_db_id(config, snapshot_id, db_id) {
        Ok(db) => db,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(db)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ReadOnlyDb_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "readonly db handle is already disposed".to_string(),
        );
        return;
    }
    let ptr = native_handle as *mut ReadOnlyDb;
    // SAFETY: `native_handle` is returned by `ReadOnlyDb.openHandle` from `Box<ReadOnlyDb>`.
    let _boxed = unsafe { Box::from_raw(ptr) };
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ReadOnlyDb_get(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    key: JByteArray,
    read_options_handle: jlong,
) -> jobject {
    let Some(db) = read_only_db_from_handle_or_throw(&mut env, native_handle) else {
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
    let values = match if read_options_handle == 0 {
        db.get(bucket, &key)
    } else {
        let Some(read_options_handle) =
            read_options_from_handle_or_throw(&mut env, read_options_handle)
        else {
            return std::ptr::null_mut();
        };
        db.get_with_options(bucket, &key, read_options_handle.read_options())
    } {
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
pub extern "system" fn Java_io_cobble_ReadOnlyDb_openScanCursor(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    start_key_inclusive: JByteArray,
    end_key_exclusive: JByteArray,
    scan_options_handle: jlong,
) -> jlong {
    let Some(db) = read_only_db_from_handle_or_throw(&mut env, native_handle) else {
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
    let iter = match args.scan_options_handle {
        Some(scan_options_handle) => match db.scan_with_options(
            args.bucket,
            args.start_key_inclusive.as_slice()..args.end_key_exclusive.as_slice(),
            scan_options_handle.scan_options(),
        ) {
            Ok(v) => v,
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                return 0;
            }
        },
        None => match db.scan(
            args.bucket,
            args.start_key_inclusive.as_slice()..args.end_key_exclusive.as_slice(),
        ) {
            Ok(v) => v,
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                return 0;
            }
        },
    };
    Box::into_raw(Box::new(ScanCursorHandle::from_static_iter(
        iter,
        args.batch_size,
    ))) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ReadOnlyDb_id(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    let Some(db) = read_only_db_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    to_java_string_or_throw(&mut env, db.id().to_string())
}

fn read_only_db_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static ReadOnlyDb> {
    if native_handle == 0 {
        throw_illegal_state(env, "readonly db handle is disposed".to_string());
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<ReadOnlyDb>` and valid until `disposeInternal`.
    Some(unsafe { &*(native_handle as *const ReadOnlyDb) })
}
