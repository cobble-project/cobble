use crate::util::{
    decode_column_index, decode_java_bytes, decode_java_string, decode_u16, throw_illegal_argument,
    throw_illegal_state,
};
use bytes::Bytes;
use cobble::{Config, DbIterator, Result, ScanOptions, ScanSplit, ScanSplitScanner};
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JIntArray, JObject, JString, JValue};
use jni::sys::{jint, jlong, jobject};
use std::sync::OnceLock;

const DEFAULT_SCAN_BATCH_SIZE: usize = 256;

pub(crate) struct ScanOptionsHandle {
    scan_options: ScanOptions,
    batch_size: usize,
}

impl ScanOptionsHandle {
    fn new() -> ScanOptionsHandle {
        ScanOptionsHandle {
            scan_options: ScanOptions::default(),
            batch_size: DEFAULT_SCAN_BATCH_SIZE,
        }
    }

    pub(crate) fn scan_options(&self) -> &ScanOptions {
        &self.scan_options
    }

    pub(crate) fn batch_size(&self) -> usize {
        self.batch_size
    }
}

pub(crate) struct ScanOpenArgs {
    pub(crate) bucket: u16,
    pub(crate) start_key_inclusive: Vec<u8>,
    pub(crate) end_key_exclusive: Vec<u8>,
    pub(crate) scan_options_handle: &'static ScanOptionsHandle,
}

pub(crate) fn decode_scan_open_args(
    env: &mut JNIEnv,
    bucket: jint,
    start_key_inclusive: JByteArray,
    end_key_exclusive: JByteArray,
    scan_options_handle: jlong,
) -> Option<ScanOpenArgs> {
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
    let scan_options_handle = scan_options_from_handle_or_throw(env, scan_options_handle)?;
    Some(ScanOpenArgs {
        bucket,
        start_key_inclusive,
        end_key_exclusive,
        scan_options_handle,
    })
}

pub(crate) struct ScanCursorHandle {
    inner: Box<StaticScanCursorInner>,
}

type ScanValues = Vec<Option<Vec<u8>>>;
type ScanRow = (Vec<u8>, ScanValues);

enum ScanCursorIter {
    Db(DbIterator<'static>),
    Split(ScanSplitScanner),
}

impl ScanCursorHandle {
    pub(crate) fn from_static_iter(
        iter: DbIterator<'static>,
        batch_size: usize,
    ) -> ScanCursorHandle {
        ScanCursorHandle {
            inner: Box::new(StaticScanCursorInner {
                iter: ScanCursorIter::Db(iter),
                batch_size,
                pending: None,
                exhausted: false,
            }),
        }
    }

    pub(crate) fn from_split_scanner(
        iter: ScanSplitScanner,
        batch_size: usize,
    ) -> ScanCursorHandle {
        ScanCursorHandle {
            inner: Box::new(StaticScanCursorInner {
                iter: ScanCursorIter::Split(iter),
                batch_size,
                pending: None,
                exhausted: false,
            }),
        }
    }

    fn next_batch(&mut self) -> Result<ScanBatch> {
        self.inner.next_batch()
    }
}

struct StaticScanCursorInner {
    iter: ScanCursorIter,
    batch_size: usize,
    pending: Option<ScanRow>,
    exhausted: bool,
}

impl StaticScanCursorInner {
    fn next_batch(&mut self) -> Result<ScanBatch> {
        if self.exhausted {
            return Ok(ScanBatch::empty());
        }
        let mut keys = Vec::with_capacity(self.batch_size);
        let mut values = Vec::with_capacity(self.batch_size);
        if let Some((key, value)) = self.pending.take() {
            keys.push(key);
            values.push(value);
        }
        while keys.len() < self.batch_size {
            let Some(row) = self.next_row() else {
                self.exhausted = true;
                break;
            };
            let Some((key, value)) = convert_row(row?) else {
                continue;
            };
            keys.push(key);
            values.push(value);
        }
        if keys.is_empty() {
            self.exhausted = true;
            return Ok(ScanBatch::empty());
        }
        let mut has_more = false;
        if !self.exhausted {
            loop {
                match self.next_row() {
                    Some(Ok(row)) => {
                        if let Some(next_row) = convert_row(row) {
                            self.pending = Some(next_row);
                            has_more = true;
                            break;
                        }
                    }
                    Some(Err(err)) => return Err(err),
                    None => {
                        self.exhausted = true;
                        break;
                    }
                }
            }
        }
        Ok(ScanBatch {
            next_start_after_exclusive: keys.last().cloned(),
            keys,
            values,
            has_more,
        })
    }

    fn next_row(&mut self) -> Option<cobble::Result<(Bytes, Vec<Option<Bytes>>)>> {
        match &mut self.iter {
            ScanCursorIter::Db(iter) => iter.next(),
            ScanCursorIter::Split(iter) => iter.next(),
        }
    }
}

fn convert_row(row: (Bytes, Vec<Option<Bytes>>)) -> Option<(Vec<u8>, ScanValues)> {
    let (key, columns) = row;
    // Skip rows where all columns are None (fully deleted/empty rows)
    if columns.iter().all(|c| c.is_none()) {
        return None;
    }
    let values: ScanValues = columns
        .into_iter()
        .map(|col| col.map(|b| b.to_vec()))
        .collect();
    Some((key.to_vec(), values))
}

struct ScanBatch {
    keys: Vec<Vec<u8>>,
    values: Vec<ScanValues>,
    next_start_after_exclusive: Option<Vec<u8>>,
    has_more: bool,
}

impl ScanBatch {
    fn empty() -> ScanBatch {
        ScanBatch {
            keys: Vec::new(),
            values: Vec::new(),
            next_start_after_exclusive: None,
            has_more: false,
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ScanOptions_createHandle(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    Box::into_raw(Box::new(ScanOptionsHandle::new())) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ScanOptions_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "scan options handle is already disposed".to_string(),
        );
        return;
    }
    let ptr = native_handle as *mut ScanOptionsHandle;
    // SAFETY: `native_handle` is returned by `ScanOptions.createHandle` from `Box<...>`.
    let _boxed = unsafe { Box::from_raw(ptr) };
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ScanOptions_setReadAheadBytes(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    read_ahead_bytes: jint,
) {
    let Some(scan_options) = scan_options_from_handle_mut_or_throw(&mut env, native_handle) else {
        return;
    };
    if read_ahead_bytes < 0 {
        throw_illegal_argument(
            &mut env,
            format!("readAheadBytes out of range: {}", read_ahead_bytes),
        );
        return;
    }
    scan_options.scan_options.read_ahead_bytes = read_ahead_bytes as usize;
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ScanOptions_setBatchSize(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    batch_size: jint,
) {
    let Some(scan_options) = scan_options_from_handle_mut_or_throw(&mut env, native_handle) else {
        return;
    };
    if batch_size <= 0 {
        throw_illegal_argument(&mut env, format!("batchSize must be > 0: {}", batch_size));
        return;
    }
    scan_options.batch_size = batch_size as usize;
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ScanOptions_setColumns(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    columns: JIntArray,
) {
    let Some(scan_options) = scan_options_from_handle_mut_or_throw(&mut env, native_handle) else {
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
    scan_options.scan_options.column_indices = Some(decoded);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ScanCursor_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "scan cursor handle is already disposed".to_string(),
        );
        return;
    }
    let ptr = native_handle as *mut ScanCursorHandle;
    // SAFETY: `native_handle` is returned by openScanCursor methods from `Box<...>`.
    let _boxed = unsafe { Box::from_raw(ptr) };
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ScanCursor_nextBatchInternal(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jobject {
    let Some(cursor) = scan_cursor_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let batch = match cursor.next_batch() {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return std::ptr::null_mut();
        }
    };
    match to_java_scan_batch(&mut env, batch) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err);
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ScanSplit_openSplitScanCursor(
    mut env: JNIEnv,
    _class: JClass,
    config_path: JString,
    split_json: JString,
    scan_options_handle: jlong,
) -> jlong {
    let config_path = match decode_java_string(&mut env, config_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let config = match Config::from_path(&config_path) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(&mut env, err.to_string());
            return 0;
        }
    };
    open_split_scan_cursor(&mut env, config, split_json, scan_options_handle)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ScanSplit_openSplitScanCursorFromJson(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    split_json: JString,
    scan_options_handle: jlong,
) -> jlong {
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return 0;
        }
    };
    let config = match serde_json::from_str::<Config>(&config_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, format!("invalid config json: {}", err));
            return 0;
        }
    };
    open_split_scan_cursor(&mut env, config, split_json, scan_options_handle)
}

fn open_split_scan_cursor(
    env: &mut JNIEnv,
    config: Config,
    split_json: JString,
    scan_options_handle: jlong,
) -> jlong {
    let split_json = match decode_java_string(env, split_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return 0;
        }
    };
    let split = match serde_json::from_str::<ScanSplit>(&split_json) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, format!("invalid scan split json: {}", err));
            return 0;
        }
    };
    let Some(options_handle) = scan_options_from_handle_or_throw(env, scan_options_handle) else {
        return 0;
    };
    let scanner = match split.create_scanner(config, options_handle.scan_options()) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            return 0;
        }
    };
    Box::into_raw(Box::new(ScanCursorHandle::from_split_scanner(
        scanner,
        options_handle.batch_size(),
    ))) as jlong
}

pub(crate) fn scan_options_from_handle_or_throw(
    _env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static ScanOptionsHandle> {
    if native_handle == 0 {
        return Some(default_scan_options_handle());
    }
    // SAFETY: `native_handle` is created from `Box<ScanOptionsHandle>` and valid until dispose.
    Some(unsafe { &*(native_handle as *const ScanOptionsHandle) })
}

fn scan_options_from_handle_mut_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static mut ScanOptionsHandle> {
    if native_handle == 0 {
        throw_illegal_state(env, "scan options handle is disposed".to_string());
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<ScanOptionsHandle>` and valid until dispose.
    Some(unsafe { &mut *(native_handle as *mut ScanOptionsHandle) })
}

fn scan_cursor_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static mut ScanCursorHandle> {
    if native_handle == 0 {
        throw_illegal_state(env, "scan cursor handle is disposed".to_string());
        return None;
    }
    // SAFETY: `native_handle` is created from `Box<ScanCursorHandle>` and valid until dispose.
    Some(unsafe { &mut *(native_handle as *mut ScanCursorHandle) })
}

fn to_java_scan_batch(env: &mut JNIEnv, batch: ScanBatch) -> std::result::Result<jobject, String> {
    let keys_raw = to_java_bytes_array(env, batch.keys)?;
    let values_raw = to_java_3d_bytes_array(env, batch.values)?;
    let next_raw = match batch.next_start_after_exclusive {
        Some(v) => env
            .byte_array_from_slice(&v)
            .map(|arr| arr.into_raw() as jobject)
            .map_err(|err| err.to_string())?,
        None => std::ptr::null_mut(),
    };
    let keys_obj = unsafe { JObject::from_raw(keys_raw) };
    let values_obj = unsafe { JObject::from_raw(values_raw) };
    let next_obj = unsafe { JObject::from_raw(next_raw) };
    let result = env
        .new_object(
            "io/cobble/ScanBatch",
            "([[B[[[B[BZ)V",
            &[
                JValue::Object(&keys_obj),
                JValue::Object(&values_obj),
                JValue::Object(&next_obj),
                JValue::Bool(if batch.has_more { 1 } else { 0 }),
            ],
        )
        .map_err(|err| err.to_string())?;
    Ok(result.into_raw())
}

fn default_scan_options_handle() -> &'static ScanOptionsHandle {
    static DEFAULT_SCAN_OPTIONS_HANDLE: OnceLock<ScanOptionsHandle> = OnceLock::new();
    DEFAULT_SCAN_OPTIONS_HANDLE.get_or_init(ScanOptionsHandle::new)
}

fn to_java_bytes_array(
    env: &mut JNIEnv,
    items: Vec<Vec<u8>>,
) -> std::result::Result<jobject, String> {
    let array = env
        .new_object_array(items.len() as i32, "[B", JObject::null())
        .map_err(|err| err.to_string())?;
    for (index, value) in items.into_iter().enumerate() {
        let row = env
            .byte_array_from_slice(&value)
            .map_err(|err| err.to_string())?;
        env.set_object_array_element(&array, index as i32, row)
            .map_err(|err| err.to_string())?;
    }
    Ok(array.into_raw() as jobject)
}

fn to_java_3d_bytes_array(
    env: &mut JNIEnv,
    items: Vec<ScanValues>,
) -> std::result::Result<jobject, String> {
    let array = env
        .new_object_array(items.len() as i32, "[[B", JObject::null())
        .map_err(|err| err.to_string())?;
    for (index, columns) in items.into_iter().enumerate() {
        let column_array = env
            .new_object_array(columns.len() as i32, "[B", JObject::null())
            .map_err(|err| err.to_string())?;
        for (column_index, bytes) in columns.into_iter().enumerate() {
            if let Some(bytes) = bytes {
                let column_bytes = env
                    .byte_array_from_slice(&bytes)
                    .map_err(|err| err.to_string())?;
                env.set_object_array_element(&column_array, column_index as i32, column_bytes)
                    .map_err(|err| err.to_string())?;
            }
            // None columns remain null in the Java array
        }
        env.set_object_array_element(&array, index as i32, column_array)
            .map_err(|err| err.to_string())?;
    }
    Ok(array.into_raw() as jobject)
}
