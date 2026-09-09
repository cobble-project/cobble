use crate::util::{
    decode_java_string, decode_u32, decode_u64_from_jlong, parse_config_json,
    throw_illegal_argument, throw_illegal_state, to_java_string_or_throw,
};
use cobble_binding::{Config, CoordinatorConfig, DbCoordinator, ShardSnapshotMetadata};
use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint, jlong, jstring};

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DbCoordinator_openHandle(
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
    let coordinator = match DbCoordinator::open(CoordinatorConfig::from_config(&config)) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(coordinator)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DbCoordinator_openHandleFromJson(
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
    let coordinator = match DbCoordinator::open(CoordinatorConfig::from_config(&config)) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(coordinator)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DbCoordinator_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "coordinator handle is already disposed".to_string(),
        );
        return;
    }
    let ptr = native_handle as *mut DbCoordinator;
    // SAFETY: `native_handle` is returned by `DbCoordinator.openHandle` from `Box<DbCoordinator>`.
    let _boxed = unsafe { Box::from_raw(ptr) };
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DbCoordinator_materializeGlobalSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    total_buckets: jint,
    snapshot_id: jlong,
    shard_inputs_json: JString,
) -> jstring {
    let Some(coordinator) = coordinator_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let total_buckets = match decode_u32("totalBuckets", total_buckets) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    if snapshot_id < 0 {
        throw_illegal_argument(
            &mut env,
            format!("snapshotId out of range: {}", snapshot_id),
        );
        return std::ptr::null_mut();
    }
    let snapshot_id = snapshot_id as u64;
    let json = match decode_java_string(&mut env, shard_inputs_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let shard_snapshots = match parse_shard_snapshots(&json) {
        Ok(inputs) => inputs,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    if shard_snapshots.is_empty() {
        throw_illegal_argument(&mut env, "shard inputs must not be empty".to_string());
        return std::ptr::null_mut();
    }

    let global =
        match coordinator.take_global_snapshot_with_id(total_buckets, shard_snapshots, snapshot_id)
        {
            Ok(v) => v,
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                return std::ptr::null_mut();
            }
        };
    if let Err(err) = coordinator.materialize_global_snapshot(&global) {
        throw_illegal_state(&mut env, err.to_string());
        return std::ptr::null_mut();
    }
    let json = match serde_json::to_string(&global) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    to_java_string_or_throw(&mut env, json)
}

pub(crate) fn parse_shard_snapshots(json: &str) -> Result<Vec<ShardSnapshotMetadata>, String> {
    serde_json::from_str(json).map_err(|err| format!("invalid shard metadata json: {err}"))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DbCoordinator_getGlobalSnapshotJson(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    snapshot_id: jlong,
) -> jstring {
    let Some(coordinator) = coordinator_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let snapshot = match coordinator.load_global_snapshot(snapshot_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    let json = match serde_json::to_string(&snapshot) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    to_java_string_or_throw(&mut env, json)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DbCoordinator_listGlobalSnapshotsJson(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    let Some(coordinator) = coordinator_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let snapshots = match coordinator.list_global_snapshots() {
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
pub extern "system" fn Java_io_cobble_DbCoordinator_loadCurrentGlobalSnapshotJson(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    let Some(coordinator) = coordinator_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let snapshot = match coordinator.load_current_global_snapshot() {
        Ok(Some(v)) => v,
        Ok(None) => return std::ptr::null_mut(),
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    let json = match serde_json::to_string(&snapshot) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    to_java_string_or_throw(&mut env, json)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DbCoordinator_retainSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    snapshot_id: jlong,
) -> jboolean {
    let Some(coordinator) = coordinator_from_handle_or_throw(&mut env, native_handle) else {
        return JNI_FALSE;
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return JNI_FALSE;
        }
    };
    if coordinator.retain_snapshot(snapshot_id) {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DbCoordinator_expireSnapshot(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    snapshot_id: jlong,
) -> jboolean {
    let Some(coordinator) = coordinator_from_handle_or_throw(&mut env, native_handle) else {
        return JNI_FALSE;
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return JNI_FALSE;
        }
    };
    match coordinator.expire_snapshot(snapshot_id) {
        Ok(true) => JNI_TRUE,
        Ok(false) => JNI_FALSE,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            JNI_FALSE
        }
    }
}

fn coordinator_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static mut DbCoordinator> {
    if native_handle == 0 {
        throw_illegal_state(env, "coordinator handle is disposed".to_string());
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<DbCoordinator>` and valid until `disposeInternal`.
    Some(unsafe { &mut *(native_handle as *mut DbCoordinator) })
}
