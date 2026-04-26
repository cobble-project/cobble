use crate::util::{
    decode_column_index, decode_java_bytes, decode_java_string, decode_u16, throw_illegal_argument,
    throw_illegal_state,
};
use cobble::ScanOptions;
use cobble_data_structure::StructuredScanOptions;
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JIntArray, JObject, JString};
use jni::sys::{jint, jlong};
use size::Size;

pub(crate) const DEFAULT_SCAN_BATCH_SIZE: usize = 256;

pub(crate) struct StructuredScanOptionsHandle {
    scan_options: StructuredScanOptions,
    batch_size: usize,
}

impl StructuredScanOptionsHandle {
    fn new() -> StructuredScanOptionsHandle {
        StructuredScanOptionsHandle {
            scan_options: StructuredScanOptions::default(),
            batch_size: DEFAULT_SCAN_BATCH_SIZE,
        }
    }

    pub(crate) fn scan_options(&self) -> &StructuredScanOptions {
        &self.scan_options
    }

    pub(crate) fn batch_size(&self) -> usize {
        self.batch_size
    }
}

fn rebuild_scan_options(
    column_family: Option<String>,
    column_indices: Option<Vec<usize>>,
    read_ahead_bytes: Size,
) -> StructuredScanOptions {
    let mut scan_options = match column_indices {
        Some(column_indices) => match column_family {
            Some(column_family) => {
                ScanOptions::for_columns(column_indices).with_column_family(column_family)
            }
            None => ScanOptions::for_columns(column_indices),
        },
        None => match column_family {
            Some(column_family) => ScanOptions::default().with_column_family(column_family),
            None => ScanOptions::default(),
        },
    };
    scan_options.read_ahead_bytes = read_ahead_bytes;
    StructuredScanOptions::from(scan_options)
}

pub(crate) struct StructuredScanOpenArgs {
    pub(crate) bucket: u16,
    pub(crate) start_key_inclusive: Vec<u8>,
    pub(crate) end_key_exclusive: Vec<u8>,
    pub(crate) scan_options_handle: Option<&'static StructuredScanOptionsHandle>,
    pub(crate) batch_size: usize,
}

pub(crate) fn decode_structured_scan_open_args(
    env: &mut JNIEnv,
    bucket: jint,
    start_key_inclusive: JByteArray,
    end_key_exclusive: JByteArray,
    scan_options_handle: jlong,
) -> Option<StructuredScanOpenArgs> {
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    let start_key_inclusive = match decode_java_bytes(env, start_key_inclusive) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    let end_key_exclusive = match decode_java_bytes(env, end_key_exclusive) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    let (scan_options_handle, batch_size) = if scan_options_handle == 0 {
        (None, DEFAULT_SCAN_BATCH_SIZE)
    } else {
        let handle = structured_scan_options_from_handle_or_throw(env, scan_options_handle)?;
        (Some(handle), handle.batch_size())
    };
    Some(StructuredScanOpenArgs {
        bucket,
        start_key_inclusive,
        end_key_exclusive,
        scan_options_handle,
        batch_size,
    })
}

pub(crate) fn structured_scan_options_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static StructuredScanOptionsHandle> {
    if native_handle == 0 {
        throw_illegal_state(
            env,
            "structured scan options handle is disposed".to_string(),
        );
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<StructuredScanOptionsHandle>`.
    Some(unsafe { &*(native_handle as *const StructuredScanOptionsHandle) })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ScanOptions_createHandle(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    Box::into_raw(Box::new(StructuredScanOptionsHandle::new())) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ScanOptions_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "structured scan options handle is already disposed".to_string(),
        );
        return;
    }
    let ptr = native_handle as *mut StructuredScanOptionsHandle;
    // SAFETY: `native_handle` is returned by `structured.ScanOptions.createHandle`.
    let _boxed = unsafe { Box::from_raw(ptr) };
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ScanOptions_setReadAheadBytes(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    read_ahead_bytes: jint,
) {
    let Some(scan_options) =
        structured_scan_options_from_handle_mut_or_throw(&mut env, native_handle)
    else {
        return;
    };
    if read_ahead_bytes < 0 {
        throw_illegal_argument(
            &mut env,
            format!("readAheadBytes out of range: {}", read_ahead_bytes),
        );
        return;
    }
    let mut raw = scan_options.scan_options.clone().into_cobble();
    raw.read_ahead_bytes = Size::from_const(read_ahead_bytes as i64);
    scan_options.scan_options = StructuredScanOptions::from(raw);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ScanOptions_setBatchSize(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    batch_size: jint,
) {
    let Some(scan_options) =
        structured_scan_options_from_handle_mut_or_throw(&mut env, native_handle)
    else {
        return;
    };
    if batch_size <= 0 {
        throw_illegal_argument(&mut env, format!("batchSize must be > 0: {}", batch_size));
        return;
    }
    scan_options.batch_size = batch_size as usize;
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ScanOptions_setColumns(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    columns: JIntArray,
) {
    let Some(scan_options) =
        structured_scan_options_from_handle_mut_or_throw(&mut env, native_handle)
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
    let current = scan_options.scan_options.clone().into_cobble();
    scan_options.scan_options = rebuild_scan_options(
        current.column_family,
        Some(decoded),
        current.read_ahead_bytes,
    );
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ScanOptions_setColumnFamily(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column_family: JString,
) {
    let Some(scan_options) =
        structured_scan_options_from_handle_mut_or_throw(&mut env, native_handle)
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
    let current = scan_options.scan_options.clone().into_cobble();
    scan_options.scan_options = rebuild_scan_options(
        Some(column_family),
        current.column_indices,
        current.read_ahead_bytes,
    );
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_ScanOptions_clearColumnFamily(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    let Some(scan_options) =
        structured_scan_options_from_handle_mut_or_throw(&mut env, native_handle)
    else {
        return;
    };
    let current = scan_options.scan_options.clone().into_cobble();
    scan_options.scan_options =
        rebuild_scan_options(None, current.column_indices, current.read_ahead_bytes);
}

fn structured_scan_options_from_handle_mut_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static mut StructuredScanOptionsHandle> {
    if native_handle == 0 {
        throw_illegal_state(
            env,
            "structured scan options handle is disposed".to_string(),
        );
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<StructuredScanOptionsHandle>`.
    Some(unsafe { &mut *(native_handle as *mut StructuredScanOptionsHandle) })
}
