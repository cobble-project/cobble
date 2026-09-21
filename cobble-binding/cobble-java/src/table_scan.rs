use crate::scan::ScanCursorHandle;
use crate::util::{
    decode_java_string, decode_u64_from_jlong, parse_config_json, throw_illegal_argument,
    throw_illegal_state, to_java_string_or_throw,
};
use cobble_table::{TableReaderBuilder, TableScanPlan, TableScanSplit};
use jni::JNIEnv;
use jni::objects::{AutoLocal, JClass, JObjectArray, JString, JValue};
use jni::sys::{jboolean, jint, jlong, jobject, jstring};

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableScanPlan_openSnapshotNative(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    table_name: JString,
    snapshot_id: jlong,
) -> jstring {
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    open_plan(&mut env, config_json, table_name, snapshot_id)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableScanPlan_splitsNative(
    mut env: JNIEnv,
    _class: JClass,
    plan_json: JString,
) -> jstring {
    let plan = match decode_plan(&mut env, plan_json) {
        Some(value) => value,
        None => return std::ptr::null_mut(),
    };
    let splits = match plan.splits() {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            return std::ptr::null_mut();
        }
    };
    match serde_json::to_string(&splits) {
        Ok(value) => to_java_string_or_throw(&mut env, value),
        Err(error) => {
            throw_illegal_state(
                &mut env,
                format!("failed to serialize table scan splits: {error}"),
            );
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableScanSplit_openScannerNative<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_json: JString<'local>,
    split_json: JString<'local>,
    field_names: JObjectArray<'local>,
    read_ahead_bytes: jint,
    direct: jboolean,
) -> jobject {
    if read_ahead_bytes < 0 {
        throw_illegal_argument(
            &mut env,
            format!("readAheadBytes out of range: {read_ahead_bytes}"),
        );
        return std::ptr::null_mut();
    }
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return std::ptr::null_mut();
    };
    let split_json = match decode_java_string(&mut env, split_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    let split = match serde_json::from_str::<TableScanSplit>(&split_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, format!("invalid table scan split JSON: {error}"));
            return std::ptr::null_mut();
        }
    };
    let field_names = match decode_string_array(&mut env, &field_names) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    let scanner = match cobble_table::ffi::open_projected_scan(
        &split,
        config,
        &field_names,
        i64::from(read_ahead_bytes),
    ) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            return std::ptr::null_mut();
        }
    };
    let handle = Box::into_raw(Box::new(ScanCursorHandle::from_split_scanner(scanner))) as jlong;
    let cursor_class = if direct != 0 {
        "io/cobble/DirectScanCursor"
    } else {
        "io/cobble/ScanCursor"
    };
    match env.new_object(cursor_class, "(J)V", &[JValue::Long(handle)]) {
        Ok(value) => value.into_raw(),
        Err(error) => {
            // SAFETY: `handle` was allocated immediately above and was not passed to Java.
            let _ = unsafe { Box::from_raw(handle as *mut ScanCursorHandle) };
            throw_illegal_state(&mut env, format!("failed to create scan cursor: {error}"));
            std::ptr::null_mut()
        }
    }
}

fn open_plan(
    env: &mut JNIEnv,
    config_json: JString,
    table_name: JString,
    snapshot_id: u64,
) -> jstring {
    let config_json = match decode_java_string(env, config_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(env, error);
            return std::ptr::null_mut();
        }
    };
    let Some(config) = parse_config_json(env, &config_json) else {
        return std::ptr::null_mut();
    };
    let table_name = match decode_java_string(env, table_name) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(env, error);
            return std::ptr::null_mut();
        }
    };
    let builder = TableReaderBuilder::new(config).table_name(table_name);
    let reader = builder.global_snapshot(snapshot_id).open();
    let plan = match reader.and_then(|reader| reader.scan_plan()) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(env, error.to_string());
            return std::ptr::null_mut();
        }
    };
    scan_plan_response(env, &plan)
}

pub(crate) fn scan_plan_response(env: &mut JNIEnv, plan: &TableScanPlan) -> jstring {
    let response = serde_json::json!({
        "plan": plan,
        "schema": plan.schema(),
        "snapshot_id": plan.snapshot_id(),
        "total_buckets": plan.total_buckets(),
        "data_size_bytes": plan.data_size_bytes(),
    });
    match serde_json::to_string(&response) {
        Ok(value) => to_java_string_or_throw(env, value),
        Err(error) => {
            throw_illegal_state(env, format!("failed to serialize table scan plan: {error}"));
            std::ptr::null_mut()
        }
    }
}

fn decode_plan(env: &mut JNIEnv, plan_json: JString) -> Option<TableScanPlan> {
    let plan_json = match decode_java_string(env, plan_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(env, error);
            return None;
        }
    };
    match serde_json::from_str::<TableScanPlan>(&plan_json) {
        Ok(value) => Some(value),
        Err(error) => {
            throw_illegal_argument(env, format!("invalid table scan plan JSON: {error}"));
            None
        }
    }
}

fn decode_string_array(env: &mut JNIEnv, values: &JObjectArray) -> Result<Vec<String>, String> {
    let length = env
        .get_array_length(values)
        .map_err(|error| format!("invalid field names array: {error}"))?;
    let mut decoded = Vec::with_capacity(length as usize);
    for index in 0..length {
        // Release every array element's local ref immediately. A worker may
        // project many fields, and retaining them until this JNI call returns
        // can exhaust the JVM local-reference table.
        let value = env
            .get_object_array_element(values, index)
            .map_err(|error| format!("invalid field name at index {index}: {error}"))?;
        let value = AutoLocal::new(value, env);
        if value.as_ref().is_null() {
            return Err(format!("field name at index {index} must not be null"));
        }
        let value: &JString = value.as_ref().into();
        decoded.push(
            env.get_string(value)
                .map(|value| value.into())
                .map_err(|error| format!("invalid field name at index {index}: {error}"))?,
        );
    }
    Ok(decoded)
}
