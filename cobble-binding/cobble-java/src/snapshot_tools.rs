use crate::util::to_java_string_or_throw;
use crate::util::{
    decode_java_string, decode_u64_from_jlong, parse_config_json, throw_illegal_argument,
    throw_illegal_state,
};
use cobble_binding::{
    DbCoordinator, ShardSnapshotMetadata, SnapshotCleanupOptions, VolumeDescriptor,
    cleanup_global_snapshot, cleanup_shard_snapshot, load_global_snapshot_metadata,
    load_shard_snapshot_metadata, prune_shard_snapshot,
};
use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jlong, jstring};

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SnapshotTools_cleanupShardSnapshotFromJson(
    mut env: JNIEnv,
    _class: JClass,
    metadata_volume_json: JString,
    other_volumes_json: JString,
    db_id: JString,
    snapshot_id: jlong,
    preserve_dependencies: jboolean,
    include_older_snapshots: jboolean,
) -> jstring {
    let db_id = match decode_java_string(&mut env, db_id) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    cleanup_snapshot_from_json(
        &mut env,
        (metadata_volume_json, other_volumes_json),
        Some(&db_id),
        snapshot_id,
        SnapshotCleanupOptions {
            preserve_dependencies: preserve_dependencies != JNI_FALSE,
            include_older_snapshots: include_older_snapshots != JNI_FALSE,
        },
    )
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SnapshotTools_cleanupGlobalSnapshotFromJson(
    mut env: JNIEnv,
    _class: JClass,
    metadata_volume_json: JString,
    other_volumes_json: JString,
    snapshot_id: jlong,
    preserve_dependencies: jboolean,
    include_older_snapshots: jboolean,
) -> jstring {
    cleanup_snapshot_from_json(
        &mut env,
        (metadata_volume_json, other_volumes_json),
        None,
        snapshot_id,
        SnapshotCleanupOptions {
            preserve_dependencies: preserve_dependencies != JNI_FALSE,
            include_older_snapshots: include_older_snapshots != JNI_FALSE,
        },
    )
}

fn cleanup_snapshot_from_json(
    env: &mut JNIEnv,
    volume_json: (JString, JString),
    db_id: Option<&str>,
    snapshot_id: jlong,
    options: SnapshotCleanupOptions,
) -> jstring {
    let inputs = (|| {
        let metadata_json = decode_java_string(env, volume_json.0)?;
        let others_json = decode_java_string(env, volume_json.1)?;
        let metadata: VolumeDescriptor = serde_json::from_str(&metadata_json)
            .map_err(|err| format!("invalid metadata volume json: {err}"))?;
        let others: Vec<VolumeDescriptor> = serde_json::from_str(&others_json)
            .map_err(|err| format!("invalid other volumes json: {err}"))?;
        let snapshot_id = decode_u64_from_jlong("snapshotId", snapshot_id)?;
        Ok::<_, String>((metadata, others, snapshot_id))
    })();
    let (metadata, others, snapshot_id) = match inputs {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(env, err);
            return std::ptr::null_mut();
        }
    };
    let result = match db_id {
        Some(db_id) => cleanup_shard_snapshot(&metadata, &others, db_id, snapshot_id, options),
        None => cleanup_global_snapshot(&metadata, &others, snapshot_id, options),
    };
    match result {
        Ok(result) => {
            let preserved: Vec<_> = result
                .preserved_snapshots
                .iter()
                .map(|snapshot| {
                    serde_json::json!({
                        "db_id": snapshot.db_id,
                        "snapshot_id": snapshot.snapshot_id,
                        "manifest_path": snapshot.manifest_path,
                    })
                })
                .collect();
            to_java_string_or_throw(
                env,
                serde_json::json!({
                    "deleted_files": result.deleted_files,
                    "deleted_shard_snapshots": result.deleted_shard_snapshots,
                    "deleted_global_snapshots": result.deleted_global_snapshots,
                    "preserved_snapshots": preserved,
                })
                .to_string(),
            )
        }
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SnapshotTools_pruneShardSnapshotFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    db_id: JString,
    snapshot_id: jlong,
) -> jboolean {
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return JNI_FALSE;
        }
    };
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return JNI_FALSE;
    };
    let db_id = match decode_java_string(&mut env, db_id) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return JNI_FALSE;
        }
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return JNI_FALSE;
        }
    };
    match prune_shard_snapshot(config, db_id, snapshot_id) {
        Ok(true) => JNI_TRUE,
        Ok(false) => JNI_FALSE,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            JNI_FALSE
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SnapshotTools_loadShardSnapshotFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    db_id: JString,
    manifest_path: JString,
) -> jstring {
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return std::ptr::null_mut();
    };
    let db_id = match decode_java_string(&mut env, db_id) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let manifest_path = match decode_java_string(&mut env, manifest_path) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    match load_shard_snapshot_metadata(&config, &db_id, &manifest_path) {
        Ok(snapshot) => match serde_json::to_string(&snapshot) {
            Ok(json) => to_java_string_or_throw(&mut env, json),
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                std::ptr::null_mut()
            }
        },
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SnapshotTools_loadGlobalSnapshotFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    manifest_path: JString,
) -> jstring {
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return std::ptr::null_mut();
    };
    let manifest_path = match decode_java_string(&mut env, manifest_path) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    match load_global_snapshot_metadata(&config, &manifest_path) {
        Ok(snapshot) => match serde_json::to_string(&snapshot) {
            Ok(json) => to_java_string_or_throw(&mut env, json),
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                std::ptr::null_mut()
            }
        },
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SnapshotTools_buildGlobalSnapshotFromJson(
    mut env: JNIEnv,
    _class: JClass,
    total_buckets: jni::sys::jint,
    snapshot_id: jlong,
    shards_json: JString,
) -> jstring {
    let total_buckets = match crate::util::decode_u32("totalBuckets", total_buckets) {
        Ok(value) if value > 0 => value,
        Ok(_) => {
            throw_illegal_argument(&mut env, "totalBuckets must be > 0".to_string());
            return std::ptr::null_mut();
        }
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let shards_json = match decode_java_string(&mut env, shards_json) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return std::ptr::null_mut();
        }
    };
    let shards: Vec<ShardSnapshotMetadata> =
        match serde_json::from_str::<Vec<ShardSnapshotMetadata>>(&shards_json) {
            Ok(value) if !value.is_empty() => value,
            Ok(_) => {
                throw_illegal_argument(&mut env, "shards must not be empty".to_string());
                return std::ptr::null_mut();
            }
            Err(err) => {
                throw_illegal_argument(&mut env, format!("invalid shard metadata json: {err}"));
                return std::ptr::null_mut();
            }
        };
    match DbCoordinator::build_global_snapshot(total_buckets, shards, snapshot_id) {
        Ok(snapshot) => match serde_json::to_string(&snapshot) {
            Ok(json) => to_java_string_or_throw(&mut env, json),
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                std::ptr::null_mut()
            }
        },
        Err(err) => {
            throw_illegal_argument(&mut env, err.to_string());
            std::ptr::null_mut()
        }
    }
}
