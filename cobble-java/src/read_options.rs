use crate::util::{
    decode_column_index, decode_java_string, throw_illegal_argument, throw_illegal_state,
};
use cobble::ReadOptions;
use jni::JNIEnv;
use jni::objects::{JClass, JIntArray, JObject, JString};
use jni::sys::{jint, jlong};

pub(crate) struct ReadOptionsHandle {
    read_options: ReadOptions,
}

impl ReadOptionsHandle {
    fn new() -> ReadOptionsHandle {
        ReadOptionsHandle {
            read_options: ReadOptions::for_column(0),
        }
    }

    pub(crate) fn read_options(&self) -> &ReadOptions {
        &self.read_options
    }
}

fn rebuild_read_options(
    column_family: Option<String>,
    column_indices: Option<Vec<usize>>,
) -> ReadOptions {
    match column_indices {
        Some(column_indices) => match column_family {
            Some(column_family) => {
                ReadOptions::for_columns_in_family(column_family, column_indices)
            }
            None => ReadOptions::for_columns(column_indices),
        },
        None => match column_family {
            Some(column_family) => ReadOptions::default().with_column_family(column_family),
            None => ReadOptions::default(),
        },
    }
}

fn replace_read_options(handle: &mut ReadOptionsHandle, read_options: ReadOptions) {
    handle.read_options = read_options;
}

pub(crate) fn read_options_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static ReadOptionsHandle> {
    read_options_from_handle_or_throw_impl(env, native_handle)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ReadOptions_createHandle(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    Box::into_raw(Box::new(ReadOptionsHandle::new())) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ReadOptions_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "read options handle is already disposed".to_string(),
        );
        return;
    }
    let ptr = native_handle as *mut ReadOptionsHandle;
    // SAFETY: `native_handle` is returned by `ReadOptions.createHandle` from `Box<...>`.
    let _boxed = unsafe { Box::from_raw(ptr) };
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ReadOptions_setColumn(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column: jint,
) {
    let Some(read_options) = read_options_from_handle_mut_or_throw(&mut env, native_handle) else {
        return;
    };
    let column_index = match decode_column_index(column) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let next = match read_options.read_options.column_family.clone() {
        Some(column_family) => ReadOptions::for_column_in_family(column_family, column_index),
        None => ReadOptions::for_column(column_index),
    };
    replace_read_options(read_options, next);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ReadOptions_setColumns(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    columns: JIntArray,
) {
    let Some(read_options) = read_options_from_handle_mut_or_throw(&mut env, native_handle) else {
        return;
    };
    let len = match env.get_array_length(&columns) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, format!("invalid columns array: {}", err));
            return;
        }
    };
    if len <= 0 {
        throw_illegal_argument(&mut env, "columns must not be empty".to_string());
        return;
    }
    let mut raw = vec![0i32; len as usize];
    if let Err(err) = env.get_int_array_region(&columns, 0, &mut raw) {
        throw_illegal_argument(&mut env, format!("invalid columns array: {}", err));
        return;
    }
    let mut decoded = Vec::with_capacity(raw.len());
    for value in raw {
        match decode_column_index(value) {
            Ok(v) => decoded.push(v),
            Err(err) => {
                throw_illegal_argument(&mut env, err);
                return;
            }
        }
    }
    let next = match read_options.read_options.column_family.clone() {
        Some(column_family) => ReadOptions::for_columns_in_family(column_family, decoded),
        None => ReadOptions::for_columns(decoded),
    };
    replace_read_options(read_options, next);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ReadOptions_setColumnFamily(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column_family: JString,
) {
    let Some(read_options) = read_options_from_handle_mut_or_throw(&mut env, native_handle) else {
        return;
    };
    let column_family = match decode_java_string(&mut env, column_family) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let next = rebuild_read_options(
        Some(column_family),
        read_options.read_options.column_indices.clone(),
    );
    replace_read_options(read_options, next);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ReadOptions_clearColumnFamily(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    let Some(read_options) = read_options_from_handle_mut_or_throw(&mut env, native_handle) else {
        return;
    };
    let next = rebuild_read_options(None, read_options.read_options.column_indices.clone());
    replace_read_options(read_options, next);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ReadOptions_clearColumns(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    let Some(read_options) = read_options_from_handle_mut_or_throw(&mut env, native_handle) else {
        return;
    };
    let next = rebuild_read_options(read_options.read_options.column_family.clone(), None);
    replace_read_options(read_options, next);
}

fn read_options_from_handle_or_throw_impl(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static ReadOptionsHandle> {
    if native_handle == 0 {
        throw_illegal_state(env, "read options handle is disposed".to_string());
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<ReadOptionsHandle>` and valid until dispose.
    Some(unsafe { &*(native_handle as *const ReadOptionsHandle) })
}

fn read_options_from_handle_mut_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static mut ReadOptionsHandle> {
    if native_handle == 0 {
        throw_illegal_state(env, "read options handle is disposed".to_string());
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<ReadOptionsHandle>` and valid until dispose.
    Some(unsafe { &mut *(native_handle as *mut ReadOptionsHandle) })
}
