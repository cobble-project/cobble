use crate::coordinator::parse_shard_snapshot_inputs;
use crate::util::{
    decode_java_string, decode_u32, decode_u64_from_jlong, parse_config_json,
    throw_illegal_argument, throw_illegal_state, to_java_string_or_throw,
};
use cobble_binding::{Config, CoordinatorConfig, DbCoordinator, GlobalSnapshotManifest};
use cobble_table::snapshot::TableSnapshotCommitter;
use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jint, jlong, jstring};
use std::sync::Arc;

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableSnapshotCommitter_openHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    total_buckets: jint,
    max_pending_commits: jint,
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
    open_committer(&mut env, config, total_buckets, max_pending_commits)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableSnapshotCommitter_openHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    total_buckets: jint,
    max_pending_commits: jint,
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
    open_committer(&mut env, config, total_buckets, max_pending_commits)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableSnapshotCommitter_submitJson(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    commit_id: jlong,
    shard_snapshot_json: JString,
) -> jstring {
    let Some(committer) = committer_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let commit_id = match decode_u64_from_jlong("commitId", commit_id) {
        Ok(commit_id) => commit_id,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let json = match decode_java_string(&mut env, shard_snapshot_json) {
        Ok(json) => json,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let mut snapshots = match parse_shard_snapshot_inputs(&json) {
        Ok(snapshots) if snapshots.len() == 1 => snapshots,
        Ok(_) => {
            throw_illegal_argument(
                &mut env,
                "submit requires exactly one shard snapshot".to_string(),
            );
            return std::ptr::null_mut();
        }
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let snapshot = snapshots.pop().expect("exactly one snapshot was parsed");
    snapshot_result_to_java(&mut env, committer.submit(commit_id, snapshot))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableSnapshotCommitter_commitBatchJson(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    commit_id: jlong,
    shard_snapshots_json: JString,
) -> jstring {
    let Some(committer) = committer_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let commit_id = match decode_u64_from_jlong("commitId", commit_id) {
        Ok(commit_id) => commit_id,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let json = match decode_java_string(&mut env, shard_snapshots_json) {
        Ok(json) => json,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let snapshots = match parse_shard_snapshot_inputs(&json) {
        Ok(snapshots) if !snapshots.is_empty() => snapshots,
        Ok(_) => {
            throw_illegal_argument(&mut env, "shard snapshots must not be empty".to_string());
            return std::ptr::null_mut();
        }
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    snapshot_result_to_java(&mut env, committer.commit_batch(commit_id, snapshots))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableSnapshotCommitter_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "table snapshot committer handle is already disposed".to_string(),
        );
        return;
    }
    // SAFETY: The handle is created by `open_committer` and Java serializes close with operations.
    let _boxed = unsafe { Box::from_raw(native_handle as *mut TableSnapshotCommitter) };
}

fn open_committer(
    env: &mut JNIEnv,
    config: Config,
    total_buckets: jint,
    max_pending_commits: jint,
) -> jlong {
    let total_buckets = match decode_u32("totalBuckets", total_buckets) {
        Ok(total_buckets) => total_buckets,
        Err(err) => {
            throw_illegal_argument(env, err);
            return 0;
        }
    };
    let max_pending_commits = match decode_u32("maxPendingCommits", max_pending_commits) {
        Ok(max_pending_commits) => max_pending_commits as usize,
        Err(err) => {
            throw_illegal_argument(env, err);
            return 0;
        }
    };
    let coordinator = match DbCoordinator::open(CoordinatorConfig::from_config(&config)) {
        Ok(coordinator) => Arc::new(coordinator),
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            return 0;
        }
    };
    let committer =
        match TableSnapshotCommitter::new(coordinator, total_buckets, max_pending_commits) {
            Ok(committer) => committer,
            Err(err) => {
                throw_illegal_argument(env, err.to_string());
                return 0;
            }
        };
    Box::into_raw(Box::new(committer)) as jlong
}

fn snapshot_result_to_java(
    env: &mut JNIEnv,
    result: cobble_table::Result<Option<GlobalSnapshotManifest>>,
) -> jstring {
    let snapshot = match result {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => return std::ptr::null_mut(),
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    match serde_json::to_string(&snapshot) {
        Ok(json) => to_java_string_or_throw(env, json),
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            std::ptr::null_mut()
        }
    }
}

fn committer_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static TableSnapshotCommitter> {
    if native_handle == 0 {
        throw_illegal_state(env, "table snapshot committer is disposed".to_string());
        return None;
    }
    // SAFETY: The handle is created by `open_committer` and remains valid until dispose.
    Some(unsafe { &*(native_handle as *const TableSnapshotCommitter) })
}
