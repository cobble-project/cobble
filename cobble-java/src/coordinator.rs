use crate::util::{
    decode_java_string, decode_u32, decode_u64_from_jlong, throw_illegal_argument,
    throw_illegal_state, to_java_string_or_throw,
};
use cobble::{Config, CoordinatorConfig, DbCoordinator, ShardSnapshotInput};
use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jint, jlong, jstring};
use serde::Deserialize;
use std::ops::RangeInclusive;

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
    let config = match serde_json::from_str::<Config>(&json) {
        Ok(config) => config,
        Err(err) => {
            throw_illegal_argument(&mut env, format!("invalid config json: {}", err));
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

#[derive(Deserialize)]
struct JavaRange {
    start: u16,
    end: u16,
}

#[derive(Deserialize)]
struct JavaShardSnapshot {
    ranges: Vec<JavaRange>,
    db_id: String,
    snapshot_id: u64,
    manifest_path: String,
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
    let parsed = match serde_json::from_str::<Vec<JavaShardSnapshot>>(&json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, format!("invalid shard inputs json: {}", err));
            return std::ptr::null_mut();
        }
    };
    if parsed.is_empty() {
        throw_illegal_argument(&mut env, "shard inputs must not be empty".to_string());
        return std::ptr::null_mut();
    }
    let mut shard_snapshots: Vec<ShardSnapshotInput> = Vec::with_capacity(parsed.len());
    for input in parsed {
        if input.ranges.is_empty() {
            throw_illegal_argument(&mut env, "shard input ranges must not be empty".to_string());
            return std::ptr::null_mut();
        }
        let mut ranges: Vec<RangeInclusive<u16>> = Vec::with_capacity(input.ranges.len());
        for range in input.ranges {
            if range.start > range.end {
                throw_illegal_argument(
                    &mut env,
                    format!("invalid range: start {} > end {}", range.start, range.end),
                );
                return std::ptr::null_mut();
            }
            ranges.push(range.start..=range.end);
        }
        shard_snapshots.push(ShardSnapshotInput {
            ranges,
            db_id: input.db_id,
            snapshot_id: input.snapshot_id,
            manifest_path: input.manifest_path,
        });
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
