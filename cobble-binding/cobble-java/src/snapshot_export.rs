use crate::db::{complete_snapshot_json_future, shard_snapshot_json};
use crate::util::{
    decode_java_string, decode_u64_from_jlong, throw_illegal_argument, throw_illegal_state,
};
use cobble_binding::{ExportSnapshotHandle, VolumeDescriptor};
use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jlong};

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ExportSnapshotHandle_startNative(
    mut env: JNIEnv,
    _class: JClass,
    db_handle: jlong,
    snapshot_id: jlong,
    destination_json: JString,
    structured: jboolean,
    future_json: JObject,
) -> jlong {
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return 0;
        }
    };
    let destination = match decode_java_string(&mut env, destination_json).and_then(|json| {
        serde_json::from_str::<VolumeDescriptor>(&json).map_err(|error| error.to_string())
    }) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return 0;
        }
    };
    if future_json.is_null() {
        throw_illegal_argument(&mut env, "snapshot future must not be null".into());
        return 0;
    }
    let future = match env.new_global_ref(future_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            return 0;
        }
    };
    let vm = match env.get_java_vm() {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            return 0;
        }
    };
    let result = if structured != JNI_FALSE {
        let Some(db) = crate::structured::db_from_handle(&mut env, db_handle) else {
            return 0;
        };
        db.export_snapshot(snapshot_id, destination)
    } else {
        let Some(db) = crate::db::db_from_handle_or_throw(&mut env, db_handle) else {
            return 0;
        };
        db.export_snapshot(snapshot_id, destination)
    };
    let handle = match result {
        Ok(handle) => handle,
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            return 0;
        }
    };
    let native_handle = Box::into_raw(Box::new(handle.clone())) as jlong;
    handle.on_complete(move |result| {
        complete_snapshot_json_future(
            &vm,
            &future,
            result.map(|snapshot| shard_snapshot_json(&snapshot)),
        )
    });
    native_handle
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ExportSnapshotHandle_cancelNative(
    _env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jboolean {
    if native_handle != 0 && unsafe { &*(native_handle as *const ExportSnapshotHandle) }.cancel() {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ExportSnapshotHandle_disposeNative(
    _env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    if native_handle != 0 {
        drop(unsafe { Box::from_raw(native_handle as *mut ExportSnapshotHandle) });
    }
}
