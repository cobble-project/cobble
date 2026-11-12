use crate::db::{shard_snapshot_json, validate_total_buckets};
use crate::table::{decode_table_schema, table_to_java, throw_table_error};
use crate::util::{
    decode_bucket_ranges, decode_java_string, decode_optional_bucket_ranges,
    decode_optional_java_string, decode_u64_from_jlong, import_storage_mode, parse_config_json,
    throw_illegal_argument, throw_illegal_state, to_java_string_or_throw,
};
use cobble_binding::RescaleOptions;
use cobble_table::TableStore;
use jni::JNIEnv;
use jni::objects::{JClass, JIntArray, JString};
use jni::sys::{JNI_FALSE, jboolean, jint, jlong, jobject, jstring};
use std::time::Duration;

fn store_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static mut TableStore> {
    if native_handle == 0 {
        throw_illegal_state(env, "table store is closed".to_string());
        return None;
    }
    // SAFETY: The owning Java TableStore serializes all calls on this Box, including
    // disposal. Derived handles retain only the underlying Arc<Db>, not the store.
    Some(unsafe { &mut *(native_handle as *mut TableStore) })
}

fn input<T>(env: &mut JNIEnv, result: Result<T, String>) -> Option<T> {
    result
        .map_err(|error| throw_illegal_argument(env, error))
        .ok()
}

fn store_result<T>(env: &mut JNIEnv, result: cobble_table::Result<T>) -> Option<T> {
    result.map_err(|error| throw_table_error(env, error)).ok()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableStore_openNative(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    range_starts: JIntArray,
    range_ends: JIntArray,
    db_id: JString,
) -> jlong {
    let args = (|| {
        let config_json = decode_java_string(&mut env, config_json)?;
        let ranges = decode_optional_bucket_ranges(&mut env, range_starts, range_ends)?;
        let db_id = decode_optional_java_string(&mut env, db_id)?;
        Ok((config_json, ranges, db_id))
    })();
    let Some((config_json, ranges, db_id)) = input(&mut env, args) else {
        return 0;
    };
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return 0;
    };
    let Some(max_bucket) = validate_total_buckets(&mut env, &config) else {
        return 0;
    };
    let ranges = ranges.unwrap_or_else(|| std::iter::once(0..=max_bucket).collect());
    let mut builder = TableStore::builder(config).bucket_ranges(ranges);
    if let Some(db_id) = db_id {
        builder = builder.db_id(db_id);
    }
    store_result(&mut env, builder.open())
        .map_or(0, |store| Box::into_raw(Box::new(store)) as jlong)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableStore_idNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    let Some(store) = store_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    to_java_string_or_throw(&mut env, store.id().to_owned())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableStore_createTableNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    name: JString,
    schema_json: JString,
) -> jobject {
    let Some(store) = store_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let args = (|| {
        let name = decode_java_string(&mut env, name)?;
        let schema = decode_table_schema(&mut env, schema_json)?;
        Ok((name, schema))
    })();
    let Some((name, schema)) = input(&mut env, args) else {
        return std::ptr::null_mut();
    };
    match store_result(&mut env, store.create_table(name, schema)) {
        Some(table) => table_to_java(&mut env, table),
        None => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableStore_openTableNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    name: JString,
) -> jobject {
    let Some(store) = store_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let name = decode_java_string(&mut env, name);
    let Some(name) = input(&mut env, name) else {
        return std::ptr::null_mut();
    };
    match store_result(&mut env, store.open_table(name)) {
        Some(table) => table_to_java(&mut env, table),
        None => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableStore_snapshotNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    let Some(store) = store_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    match store_result(&mut env, store.snapshot_and_wait()) {
        Some(snapshot) => to_java_string_or_throw(&mut env, shard_snapshot_json(&snapshot)),
        None => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableStore_getShardSnapshotNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    snapshot_id: jlong,
) -> jstring {
    let Some(store) = store_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let Some(snapshot_id) = input(&mut env, decode_u64_from_jlong("snapshotId", snapshot_id))
    else {
        return std::ptr::null_mut();
    };
    match store_result(&mut env, store.shard_snapshot_metadata(snapshot_id)) {
        Some(snapshot) => to_java_string_or_throw(&mut env, shard_snapshot_json(&snapshot)),
        None => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableStore_expandBucketFromManifestNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    source_db_id: JString,
    source_manifest_path: JString,
    range_starts: JIntArray,
    range_ends: JIntArray,
    storage_mode: jint,
    snapshot_before: jboolean,
    snapshot_after: jboolean,
) -> jlong {
    let Some(store) = store_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let args = (|| {
        let source_db_id = decode_java_string(&mut env, source_db_id)?;
        let source_manifest_path = decode_java_string(&mut env, source_manifest_path)?;
        let ranges = decode_bucket_ranges(&mut env, range_starts, range_ends)?;
        let storage_mode = import_storage_mode(storage_mode)?;
        Ok((source_db_id, source_manifest_path, ranges, storage_mode))
    })();
    let Some((source_db_id, source_manifest_path, ranges, storage_mode)) = input(&mut env, args)
    else {
        return 0;
    };
    let result = store.expand_bucket_from_manifest_with_options(
        source_db_id,
        source_manifest_path,
        (!ranges.is_empty()).then_some(ranges),
        storage_mode,
        RescaleOptions {
            snapshot_before: snapshot_before != JNI_FALSE,
            snapshot_after: snapshot_after != JNI_FALSE,
        },
    );
    store_result(&mut env, result).map_or(0, |snapshot_id| snapshot_id as jlong)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableStore_shrinkBucketNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    range_starts: JIntArray,
    range_ends: JIntArray,
    snapshot_before: jboolean,
    snapshot_after: jboolean,
) -> jlong {
    let Some(store) = store_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let ranges = decode_bucket_ranges(&mut env, range_starts, range_ends);
    let Some(ranges) = input(&mut env, ranges) else {
        return 0;
    };
    if ranges.is_empty() {
        throw_illegal_argument(&mut env, "shrink ranges must not be empty".to_string());
        return 0;
    }
    let result = store.shrink_bucket_with_options(
        ranges,
        RescaleOptions {
            snapshot_before: snapshot_before != JNI_FALSE,
            snapshot_after: snapshot_after != JNI_FALSE,
        },
    );
    match store_result(&mut env, result) {
        Some(Some(snapshot_id)) => snapshot_id as jlong,
        Some(None) => -1,
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableStore_waitForImportAdoptionNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    timeout_millis: jlong,
) {
    let Some(store) = store_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let Some(timeout_millis) = input(
        &mut env,
        decode_u64_from_jlong("timeoutMillis", timeout_millis),
    ) else {
        return;
    };
    store_result(
        &mut env,
        store.wait_for_import_adoption(Duration::from_millis(timeout_millis)),
    );
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableStore_closeNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    let Some(store) = store_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    store_result(&mut env, store.close());
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableStore_disposeNative(
    _env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    // SAFETY: NativeObject.close clears the handle exactly once, under the TableStore monitor.
    drop(unsafe { Box::from_raw(native_handle as *mut TableStore) });
}
