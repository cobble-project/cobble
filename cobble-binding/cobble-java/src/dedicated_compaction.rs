use crate::util::{
    decode_java_bytes, decode_java_string, parse_config_json, throw_illegal_argument,
    throw_illegal_state, to_java_string_or_throw,
};
use cobble_binding::{
    DedicatedCompactionExecution, DedicatedCompactionExecutor, DedicatedCompactionMonitor,
    DedicatedCompactionPlan,
};
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JString};
use jni::sys::{jint, jlong, jobject, jstring};

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DedicatedCompactionMonitor_openScanHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    root: JString,
) -> jlong {
    let Some(config) = decode_config_path(&mut env, config_path) else {
        return 0;
    };
    let root = match decode_java_string(&mut env, root) {
        Ok(root) => root,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    open_scan(&mut env, config, root)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DedicatedCompactionMonitor_openScanHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    root: JString,
) -> jlong {
    let Some(config) = decode_config(&mut env, config_json) else {
        return 0;
    };
    let root = match decode_java_string(&mut env, root) {
        Ok(root) => root,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    open_scan(&mut env, config, root)
}

fn open_scan(env: &mut JNIEnv, config: cobble_binding::Config, root: String) -> jlong {
    match DedicatedCompactionMonitor::scan(config, root) {
        Ok(monitor) => Box::into_raw(Box::new(monitor)) as jlong,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DedicatedCompactionMonitor_openWatchHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    paths: JObjectArray,
) -> jlong {
    let Some(config) = decode_config_path(&mut env, config_path) else {
        return 0;
    };
    let paths = match decode_java_strings(&mut env, paths) {
        Ok(paths) => paths,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    open_watch(&mut env, config, paths)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DedicatedCompactionMonitor_openWatchHandleFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    paths: JObjectArray,
) -> jlong {
    let Some(config) = decode_config(&mut env, config_json) else {
        return 0;
    };
    let paths = match decode_java_strings(&mut env, paths) {
        Ok(paths) => paths,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    open_watch(&mut env, config, paths)
}

fn open_watch(env: &mut JNIEnv, config: cobble_binding::Config, paths: Vec<String>) -> jlong {
    match DedicatedCompactionMonitor::watch_databases(config, paths) {
        Ok(monitor) => Box::into_raw(Box::new(monitor)) as jlong,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DedicatedCompactionMonitor_disposeInternal(
    _env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    if native_handle == 0 {
        return;
    }
    // SAFETY: the handle was created by one of the open methods above and NativeObject closes it
    // at most once.
    unsafe {
        drop(Box::from_raw(
            native_handle as *mut DedicatedCompactionMonitor,
        ));
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DedicatedCompactionMonitor_pollEncoded(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jobject {
    let Some(monitor) = monitor_from_handle(native_handle) else {
        throw_illegal_state(
            &mut env,
            "Dedicated compaction monitor is closed".to_string(),
        );
        return std::ptr::null_mut();
    };
    let plans = match monitor.poll() {
        Ok(plans) => plans,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    let byte_array_class = match env.find_class("[B") {
        Ok(class) => class,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    let output = match env.new_object_array(plans.len() as i32, byte_array_class, JObject::null()) {
        Ok(output) => output,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    for (index, plan) in plans.into_iter().enumerate() {
        let encoded = match plan.encode() {
            Ok(encoded) => encoded,
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                return std::ptr::null_mut();
            }
        };
        let encoded = match env.byte_array_from_slice(&encoded) {
            Ok(encoded) => encoded,
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                return std::ptr::null_mut();
            }
        };
        if let Err(err) = env.set_object_array_element(&output, index as i32, encoded) {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    }
    output.into_raw() as jobject
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DedicatedCompactionMonitor_completeInternal(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    job_id: JString,
) {
    let Some(monitor) = monitor_from_handle(native_handle) else {
        throw_illegal_state(
            &mut env,
            "Dedicated compaction monitor is closed".to_string(),
        );
        return;
    };
    match decode_java_string(&mut env, job_id) {
        Ok(job_id) => monitor.complete(&job_id),
        Err(err) => throw_illegal_argument(&mut env, err),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DedicatedCompactionPlan_jobIdInternal(
    mut env: JNIEnv,
    _class: JClass,
    encoded: JByteArray,
) -> jstring {
    let Some(plan) = decode_plan(&mut env, encoded) else {
        return std::ptr::null_mut();
    };
    to_java_string_or_throw(&mut env, plan.job_id().to_string())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DedicatedCompactionPlan_dbIdInternal(
    mut env: JNIEnv,
    _class: JClass,
    encoded: JByteArray,
) -> jstring {
    let Some(plan) = decode_plan(&mut env, encoded) else {
        return std::ptr::null_mut();
    };
    to_java_string_or_throw(&mut env, plan.db_id().to_string())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DedicatedCompactionExecutor_openHandle(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
) -> jlong {
    let Some(config) = decode_config(&mut env, config_json) else {
        return 0;
    };
    open_executor(&mut env, config)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DedicatedCompactionExecutor_openHandleFromPath(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
) -> jlong {
    let Some(config) = decode_config_path(&mut env, config_path) else {
        return 0;
    };
    open_executor(&mut env, config)
}

fn open_executor(env: &mut JNIEnv, config: cobble_binding::Config) -> jlong {
    match DedicatedCompactionExecutor::open(config) {
        Ok(executor) => Box::into_raw(Box::new(executor)) as jlong,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DedicatedCompactionExecutor_disposeInternal(
    _env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    if native_handle == 0 {
        return;
    }
    // SAFETY: the handle was created by an open method above and NativeObject closes it once.
    unsafe {
        drop(Box::from_raw(
            native_handle as *mut DedicatedCompactionExecutor,
        ));
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DedicatedCompactionExecutor_executeInternal(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    encoded: JByteArray,
) -> jint {
    let Some(executor) = executor_from_handle(native_handle) else {
        throw_illegal_state(
            &mut env,
            "Dedicated compaction executor is closed".to_string(),
        );
        return -1;
    };
    let Some(plan) = decode_plan(&mut env, encoded) else {
        return -1;
    };
    match executor.execute(&plan) {
        Ok(DedicatedCompactionExecution::ResultPublished { .. }) => 0,
        Ok(DedicatedCompactionExecution::WaitingForResult) => 1,
        Ok(DedicatedCompactionExecution::Stale) => 2,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            -1
        }
    }
}

fn decode_config(env: &mut JNIEnv, config_json: JString) -> Option<cobble_binding::Config> {
    let json = match decode_java_string(env, config_json) {
        Ok(json) => json,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    parse_config_json(env, &json)
}

fn decode_config_path(env: &mut JNIEnv, config_path: JString) -> Option<cobble_binding::Config> {
    let path = match decode_java_string(env, config_path) {
        Ok(path) => path,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    match cobble_binding::Config::from_path(path) {
        Ok(config) => Some(config),
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            None
        }
    }
}

fn decode_plan(env: &mut JNIEnv, encoded: JByteArray) -> Option<DedicatedCompactionPlan> {
    let bytes = match decode_java_bytes(env, encoded) {
        Ok(bytes) => bytes,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    match DedicatedCompactionPlan::decode(&bytes) {
        Ok(plan) => Some(plan),
        Err(err) => {
            throw_illegal_argument(env, err.to_string());
            None
        }
    }
}

fn decode_java_strings(env: &mut JNIEnv, values: JObjectArray) -> Result<Vec<String>, String> {
    let length = env
        .get_array_length(&values)
        .map_err(|err| format!("invalid paths array: {err}"))?;
    let mut decoded = Vec::with_capacity(length as usize);
    for index in 0..length {
        let value = env
            .get_object_array_element(&values, index)
            .map_err(|err| format!("invalid path at index {index}: {err}"))?;
        if value.is_null() {
            return Err(format!("path at index {index} must not be null"));
        }
        decoded.push(decode_java_string(env, JString::from(value))?);
    }
    Ok(decoded)
}

fn monitor_from_handle(native_handle: jlong) -> Option<&'static mut DedicatedCompactionMonitor> {
    if native_handle == 0 {
        return None;
    }
    // SAFETY: the handle is owned by one Java monitor and all mutating Java methods are
    // synchronized.
    Some(unsafe { &mut *(native_handle as *mut DedicatedCompactionMonitor) })
}

fn executor_from_handle(native_handle: jlong) -> Option<&'static DedicatedCompactionExecutor> {
    if native_handle == 0 {
        return None;
    }
    // SAFETY: the handle is owned by one Java executor and remains valid until close.
    Some(unsafe { &*(native_handle as *const DedicatedCompactionExecutor) })
}
