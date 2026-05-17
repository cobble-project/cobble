use crate::util::{
    decode_java_string, decode_u64_from_jlong, parse_config_json, throw_illegal_argument,
    throw_illegal_state,
};
use cobble::prune_shard_snapshot;
use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jlong};

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
