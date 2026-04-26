use crate::util::{decode_java_string, decode_u32, throw_illegal_argument, throw_illegal_state};
use cobble::WriteOptions;
use cobble_data_structure::StructuredWriteOptions;
use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jint, jlong};

pub(crate) struct StructuredWriteOptionsHandle {
    write_options: StructuredWriteOptions,
}

impl StructuredWriteOptionsHandle {
    fn new() -> StructuredWriteOptionsHandle {
        StructuredWriteOptionsHandle {
            write_options: StructuredWriteOptions::default(),
        }
    }

    pub(crate) fn write_options(&self) -> &StructuredWriteOptions {
        &self.write_options
    }
}

pub(crate) fn structured_write_options_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static StructuredWriteOptionsHandle> {
    structured_write_options_from_handle_or_throw_impl(env, native_handle)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_WriteOptions_createHandle(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    Box::into_raw(Box::new(StructuredWriteOptionsHandle::new())) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_WriteOptions_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "structured write options handle is already disposed".to_string(),
        );
        return;
    }
    let ptr = native_handle as *mut StructuredWriteOptionsHandle;
    // SAFETY: `native_handle` is returned by `structured.WriteOptions.createHandle`.
    let _boxed = unsafe { Box::from_raw(ptr) };
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_WriteOptions_setTtlSeconds(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    ttl_seconds: jint,
) {
    let Some(write_options) =
        structured_write_options_from_handle_mut_or_throw(&mut env, native_handle)
    else {
        return;
    };
    let ttl_seconds = match decode_u32("ttlSeconds", ttl_seconds) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let mut raw = write_options.write_options.clone().into_cobble();
    raw.ttl_seconds = Some(ttl_seconds);
    write_options.write_options = StructuredWriteOptions::from(raw);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_WriteOptions_clearTtlSeconds(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    let Some(write_options) =
        structured_write_options_from_handle_mut_or_throw(&mut env, native_handle)
    else {
        return;
    };
    let mut raw = write_options.write_options.clone().into_cobble();
    raw.ttl_seconds = None;
    write_options.write_options = StructuredWriteOptions::from(raw);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_WriteOptions_setColumnFamily(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column_family: JString,
) {
    let Some(write_options) =
        structured_write_options_from_handle_mut_or_throw(&mut env, native_handle)
    else {
        return;
    };
    let column_family = match decode_java_string(&mut env, column_family) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let ttl_seconds = write_options.write_options.ttl_seconds();
    let mut raw = WriteOptions::with_column_family(column_family);
    raw.ttl_seconds = ttl_seconds;
    write_options.write_options = StructuredWriteOptions::from(raw);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_WriteOptions_clearColumnFamily(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    let Some(write_options) =
        structured_write_options_from_handle_mut_or_throw(&mut env, native_handle)
    else {
        return;
    };
    let ttl_seconds = write_options.write_options.ttl_seconds();
    let mut raw = WriteOptions::default();
    raw.ttl_seconds = ttl_seconds;
    write_options.write_options = StructuredWriteOptions::from(raw);
}

fn structured_write_options_from_handle_or_throw_impl(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static StructuredWriteOptionsHandle> {
    if native_handle == 0 {
        throw_illegal_state(
            env,
            "structured write options handle is disposed".to_string(),
        );
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<StructuredWriteOptionsHandle>`.
    Some(unsafe { &*(native_handle as *const StructuredWriteOptionsHandle) })
}

fn structured_write_options_from_handle_mut_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static mut StructuredWriteOptionsHandle> {
    if native_handle == 0 {
        throw_illegal_state(
            env,
            "structured write options handle is disposed".to_string(),
        );
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<StructuredWriteOptionsHandle>`.
    Some(unsafe { &mut *(native_handle as *mut StructuredWriteOptionsHandle) })
}
