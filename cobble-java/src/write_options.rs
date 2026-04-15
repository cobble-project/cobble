use crate::util::{decode_java_string, decode_u32, throw_illegal_argument, throw_illegal_state};
use cobble::WriteOptions;
use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jint, jlong};
use std::sync::OnceLock;

pub(crate) struct WriteOptionsHandle {
    write_options: WriteOptions,
}

impl WriteOptionsHandle {
    fn new() -> WriteOptionsHandle {
        WriteOptionsHandle {
            write_options: WriteOptions::default(),
        }
    }

    pub(crate) fn write_options(&self) -> &WriteOptions {
        &self.write_options
    }
}

pub(crate) fn write_options_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static WriteOptionsHandle> {
    if native_handle == 0 {
        return Some(default_write_options_handle());
    }
    write_options_from_handle_or_throw_impl(env, native_handle)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_WriteOptions_createHandle(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    Box::into_raw(Box::new(WriteOptionsHandle::new())) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_WriteOptions_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "write options handle is already disposed".to_string(),
        );
        return;
    }
    let ptr = native_handle as *mut WriteOptionsHandle;
    // SAFETY: `native_handle` is returned by `WriteOptions.createHandle` from `Box<...>`.
    let _boxed = unsafe { Box::from_raw(ptr) };
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_WriteOptions_setTtlSeconds(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    ttl_seconds: jint,
) {
    let Some(write_options) = write_options_from_handle_mut_or_throw(&mut env, native_handle)
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
    write_options.write_options.ttl_seconds = Some(ttl_seconds);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_WriteOptions_clearTtlSeconds(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    let Some(write_options) = write_options_from_handle_mut_or_throw(&mut env, native_handle)
    else {
        return;
    };
    write_options.write_options.ttl_seconds = None;
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_WriteOptions_setColumnFamily(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column_family: JString,
) {
    let Some(write_options) = write_options_from_handle_mut_or_throw(&mut env, native_handle)
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
    write_options.write_options.column_family = Some(column_family);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_WriteOptions_clearColumnFamily(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    let Some(write_options) = write_options_from_handle_mut_or_throw(&mut env, native_handle)
    else {
        return;
    };
    write_options.write_options.column_family = None;
}

fn write_options_from_handle_or_throw_impl(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static WriteOptionsHandle> {
    if native_handle == 0 {
        throw_illegal_state(env, "write options handle is disposed".to_string());
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<WriteOptionsHandle>` and valid until dispose.
    Some(unsafe { &*(native_handle as *const WriteOptionsHandle) })
}

fn write_options_from_handle_mut_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static mut WriteOptionsHandle> {
    if native_handle == 0 {
        throw_illegal_state(env, "write options handle is disposed".to_string());
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<WriteOptionsHandle>` and valid until dispose.
    Some(unsafe { &mut *(native_handle as *mut WriteOptionsHandle) })
}

fn default_write_options_handle() -> &'static WriteOptionsHandle {
    static DEFAULT_WRITE_OPTIONS_HANDLE: OnceLock<WriteOptionsHandle> = OnceLock::new();
    DEFAULT_WRITE_OPTIONS_HANDLE.get_or_init(WriteOptionsHandle::new)
}
