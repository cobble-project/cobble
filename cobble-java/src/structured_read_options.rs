use crate::util::{
    decode_column_index, decode_java_string, throw_illegal_argument, throw_illegal_state,
};
use cobble::ReadOptions;
use cobble_data_structure::StructuredReadOptions;
use jni::JNIEnv;
use jni::objects::{JClass, JIntArray, JObject, JString};
use jni::sys::{jint, jlong};

pub(crate) struct StructuredReadOptionsHandle {
    read_options: StructuredReadOptions,
}

impl StructuredReadOptionsHandle {
    fn new() -> StructuredReadOptionsHandle {
        StructuredReadOptionsHandle {
            read_options: StructuredReadOptions::for_column(0),
        }
    }

    pub(crate) fn read_options(&self) -> &StructuredReadOptions {
        &self.read_options
    }
}

fn rebuild_read_options(
    column_family: Option<String>,
    column_indices: Option<Vec<usize>>,
) -> StructuredReadOptions {
    let read_options = match column_indices {
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
    };
    StructuredReadOptions::from(read_options)
}

pub(crate) fn structured_read_options_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static StructuredReadOptionsHandle> {
    structured_read_options_from_handle_or_throw_impl(env, native_handle)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ReadOptions_createHandle(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    Box::into_raw(Box::new(StructuredReadOptionsHandle::new())) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ReadOptions_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "structured read options handle is already disposed".to_string(),
        );
        return;
    }
    let ptr = native_handle as *mut StructuredReadOptionsHandle;
    // SAFETY: `native_handle` is returned by `structured.ReadOptions.createHandle`.
    let _boxed = unsafe { Box::from_raw(ptr) };
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ReadOptions_setColumn(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column: jint,
) {
    let Some(read_options) =
        structured_read_options_from_handle_mut_or_throw(&mut env, native_handle)
    else {
        return;
    };
    let column_index = match decode_column_index(column) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let current = read_options.read_options.clone().into_cobble();
    let next = match current.column_family {
        Some(column_family) => ReadOptions::for_column_in_family(column_family, column_index),
        None => ReadOptions::for_column(column_index),
    };
    read_options.read_options = StructuredReadOptions::from(next);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ReadOptions_setColumns(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    columns: JIntArray,
) {
    let Some(read_options) =
        structured_read_options_from_handle_mut_or_throw(&mut env, native_handle)
    else {
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
    let current = read_options.read_options.clone().into_cobble();
    let next = match current.column_family {
        Some(column_family) => ReadOptions::for_columns_in_family(column_family, decoded),
        None => ReadOptions::for_columns(decoded),
    };
    read_options.read_options = StructuredReadOptions::from(next);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ReadOptions_setColumnFamily(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column_family: JString,
) {
    let Some(read_options) =
        structured_read_options_from_handle_mut_or_throw(&mut env, native_handle)
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
    let current = read_options.read_options.clone().into_cobble();
    read_options.read_options = rebuild_read_options(Some(column_family), current.column_indices);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ReadOptions_clearColumnFamily(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    let Some(read_options) =
        structured_read_options_from_handle_mut_or_throw(&mut env, native_handle)
    else {
        return;
    };
    let current = read_options.read_options.clone().into_cobble();
    read_options.read_options = rebuild_read_options(None, current.column_indices);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ReadOptions_clearColumns(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    let Some(read_options) =
        structured_read_options_from_handle_mut_or_throw(&mut env, native_handle)
    else {
        return;
    };
    let current = read_options.read_options.clone().into_cobble();
    read_options.read_options = rebuild_read_options(current.column_family, None);
}

fn structured_read_options_from_handle_or_throw_impl(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static StructuredReadOptionsHandle> {
    if native_handle == 0 {
        throw_illegal_state(
            env,
            "structured read options handle is disposed".to_string(),
        );
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<StructuredReadOptionsHandle>`.
    Some(unsafe { &*(native_handle as *const StructuredReadOptionsHandle) })
}

fn structured_read_options_from_handle_mut_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static mut StructuredReadOptionsHandle> {
    if native_handle == 0 {
        throw_illegal_state(
            env,
            "structured read options handle is disposed".to_string(),
        );
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<StructuredReadOptionsHandle>`.
    Some(unsafe { &mut *(native_handle as *mut StructuredReadOptionsHandle) })
}
