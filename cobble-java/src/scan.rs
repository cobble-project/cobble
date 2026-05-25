use crate::util::{
    decode_column_index, decode_java_bytes, decode_java_string, decode_optional_java_bytes,
    decode_u16, parse_config_json, throw_illegal_argument, throw_illegal_state,
    write_payload_to_io_or_cached_overflow,
};
use bytes::Bytes;
use cobble::{Config, DbIterator, Result, ScanOptions, ScanSplit, ScanSplitScanner};
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JIntArray, JObject, JString};
use jni::sys::{jint, jlong};
use size::Size;

pub(crate) struct ScanOptionsHandle {
    scan_options: ScanOptions,
}

impl ScanOptionsHandle {
    fn new() -> ScanOptionsHandle {
        ScanOptionsHandle {
            scan_options: ScanOptions::default(),
        }
    }

    pub(crate) fn scan_options(&self) -> &ScanOptions {
        &self.scan_options
    }
}

fn rebuild_scan_options(
    column_family: Option<String>,
    column_indices: Option<Vec<usize>>,
    read_ahead_bytes: Size,
    max_rows: Option<usize>,
) -> ScanOptions {
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
    if let Some(max_rows) = max_rows {
        scan_options.set_max_rows(max_rows);
    }
    scan_options
}

pub(crate) struct ScanOpenArgs {
    pub(crate) bucket: u16,
    pub(crate) start_key_inclusive: Vec<u8>,
    pub(crate) end_key_exclusive: Vec<u8>,
    pub(crate) scan_options_handle: Option<&'static ScanOptionsHandle>,
}

pub(crate) struct ScanOpenBoundsArgs {
    pub(crate) bucket: u16,
    pub(crate) start_key_inclusive: Option<Vec<u8>>,
    pub(crate) end_key_exclusive: Option<Vec<u8>>,
    pub(crate) scan_options_handle: Option<&'static ScanOptionsHandle>,
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
    let scan_options_handle = if scan_options_handle == 0 {
        None
    } else {
        Some(scan_options_from_handle_or_throw(env, scan_options_handle)?)
    };
    Some(ScanOpenArgs {
        bucket,
        start_key_inclusive,
        end_key_exclusive,
        scan_options_handle,
    })
}

pub(crate) fn decode_scan_open_bounds_args(
    env: &mut JNIEnv,
    bucket: jint,
    start_key_inclusive: JByteArray,
    end_key_exclusive: JByteArray,
    scan_options_handle: jlong,
) -> Option<ScanOpenBoundsArgs> {
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    let start_key_inclusive = match decode_optional_java_bytes(env, start_key_inclusive) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    let end_key_exclusive = match decode_optional_java_bytes(env, end_key_exclusive) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    let scan_options_handle = if scan_options_handle == 0 {
        None
    } else {
        Some(scan_options_from_handle_or_throw(env, scan_options_handle)?)
    };
    Some(ScanOpenBoundsArgs {
        bucket,
        start_key_inclusive,
        end_key_exclusive,
        scan_options_handle,
    })
}

pub(crate) fn decode_scan_open_direct_bounds_args(
    env: &mut JNIEnv,
    bucket: jint,
    start_key_address: jlong,
    start_key_length: jint,
    end_key_address: jlong,
    end_key_length: jint,
    scan_options_handle: jlong,
) -> Option<ScanOpenBoundsArgs> {
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    let start_key_inclusive = decode_optional_direct_bytes(
        env,
        "startKeyInclusive",
        start_key_address,
        start_key_length,
    )?;
    let end_key_exclusive =
        decode_optional_direct_bytes(env, "endKeyExclusive", end_key_address, end_key_length)?;
    let scan_options_handle = if scan_options_handle == 0 {
        None
    } else {
        Some(scan_options_from_handle_or_throw(env, scan_options_handle)?)
    };
    Some(ScanOpenBoundsArgs {
        bucket,
        start_key_inclusive,
        end_key_exclusive,
        scan_options_handle,
    })
}

fn decode_optional_direct_bytes(
    env: &mut JNIEnv,
    name: &str,
    address: jlong,
    length: jint,
) -> Option<Option<Vec<u8>>> {
    let length = match usize::try_from(length) {
        Ok(v) => v,
        Err(_) => {
            throw_illegal_argument(env, format!("{name} length must be >= 0"));
            return None;
        }
    };
    if address == -1 {
        if length != 0 {
            throw_illegal_argument(
                env,
                format!("{name} length must be 0 when {name} address is -1"),
            );
            return None;
        }
        return Some(None);
    }
    let address = match usize::try_from(address) {
        Ok(v) if v != 0 => v as *const u8,
        _ => {
            throw_illegal_argument(env, format!("{name} address must be > 0 or -1"));
            return None;
        }
    };
    let bytes = unsafe { std::slice::from_raw_parts(address, length) };
    Some(Some(bytes.to_vec()))
}

pub(crate) struct ScanCursorHandle {
    inner: Box<StaticScanCursorInner>,
}

enum ScanCursorIter {
    Db(Box<DbIterator<'static>>),
    Split(Box<ScanSplitScanner>),
}

impl ScanCursorHandle {
    pub(crate) fn from_static_iter(iter: DbIterator<'static>) -> ScanCursorHandle {
        ScanCursorHandle {
            inner: Box::new(StaticScanCursorInner {
                iter: ScanCursorIter::Db(Box::new(iter)),
                exhausted: false,
            }),
        }
    }

    pub(crate) fn from_split_scanner(iter: ScanSplitScanner) -> ScanCursorHandle {
        ScanCursorHandle {
            inner: Box::new(StaticScanCursorInner {
                iter: ScanCursorIter::Split(Box::new(iter)),
                exhausted: false,
            }),
        }
    }

    fn next_entry_direct<'local>(
        &mut self,
        env: &mut JNIEnv<'local>,
        io_addr: *mut u8,
        io_capacity: usize,
    ) -> jint {
        self.inner.next_entry_direct(env, io_addr, io_capacity)
    }
}

struct StaticScanCursorInner {
    iter: ScanCursorIter,
    exhausted: bool,
}

impl StaticScanCursorInner {
    fn next_entry_direct<'local>(
        &mut self,
        env: &mut JNIEnv<'local>,
        io_addr: *mut u8,
        io_capacity: usize,
    ) -> jint {
        if self.exhausted {
            return 0;
        }
        let encoded = match self.consume_next_live_row(|bucket, key, columns| {
            write_direct_scan_entry_payload(
                env,
                io_addr,
                io_capacity,
                bucket,
                key.as_ref(),
                columns,
            )
            .map_err(cobble::Error::IoError)
        }) {
            Ok(row) => row,
            Err(err) => {
                throw_illegal_state(env, err.to_string());
                return 0;
            }
        };
        let Some(encoded) = encoded else {
            self.exhausted = true;
            return 0;
        };
        encoded
    }

    fn consume_next_live_row<T, F>(&mut self, mut consumer: F) -> Result<Option<T>>
    where
        F: FnMut(u16, &Bytes, &[Option<Bytes>]) -> Result<T>,
    {
        match &mut self.iter {
            ScanCursorIter::Db(iter) => {
                iter.as_mut()
                    .consume_next_row_with_bucket(|bucket, key, columns| {
                        consumer(bucket, key, columns)
                    })
            }
            ScanCursorIter::Split(iter) => {
                iter.as_mut()
                    .consume_next_row_with_bucket(|bucket, key, columns| {
                        consumer(bucket, key, columns)
                    })
            }
        }
    }
}

fn encoded_direct_scan_entry_payload_size<T: AsRef<[u8]>>(
    _bucket: u16,
    key: &[u8],
    columns: &[Option<T>],
) -> Result<usize, String> {
    let columns_len = encoded_optional_columns_payload_size(columns)?;
    4usize
        .checked_add(key.len())
        .and_then(|v| v.checked_add(4))
        .and_then(|v| v.checked_add(columns_len))
        .and_then(|v| v.checked_add(4))
        .ok_or_else(|| "encoded direct scan entry size overflow".to_string())
}

fn encode_direct_scan_entry_payload_into<T: AsRef<[u8]>>(
    bucket: u16,
    key: &[u8],
    columns: &[Option<T>],
    dst: &mut [u8],
) -> Result<(), String> {
    let columns_len = encoded_optional_columns_payload_size(columns)?;
    let expected_len = encoded_direct_scan_entry_payload_size(bucket, key, columns)?;
    if dst.len() != expected_len {
        return Err(format!(
            "direct scan entry payload size mismatch: expected {}, got {}",
            expected_len,
            dst.len()
        ));
    }
    dst[0..4].copy_from_slice(&(key.len() as u32).to_be_bytes());
    let key_end = 4 + key.len();
    dst[4..key_end].copy_from_slice(key);
    dst[key_end..key_end + 4].copy_from_slice(&(columns_len as u32).to_be_bytes());
    let row_end = key_end + 4 + columns_len;
    encode_optional_columns_payload_into(columns, &mut dst[key_end + 4..row_end])?;
    dst[row_end..row_end + 4].copy_from_slice(&(bucket as u32).to_be_bytes());
    Ok(())
}

fn write_direct_scan_entry_payload<'local, T: AsRef<[u8]>>(
    env: &mut JNIEnv<'local>,
    io_addr: *mut u8,
    io_capacity: usize,
    bucket: u16,
    key: &[u8],
    columns: &[Option<T>],
) -> Result<jint, String> {
    let payload_len = encoded_direct_scan_entry_payload_size(bucket, key, columns)?;
    if payload_len <= io_capacity {
        let dst = unsafe { std::slice::from_raw_parts_mut(io_addr, payload_len) };
        encode_direct_scan_entry_payload_into(bucket, key, columns, dst)?;
        return Ok(payload_len as jint);
    }
    let mut encoded = vec![0; payload_len];
    encode_direct_scan_entry_payload_into(bucket, key, columns, &mut encoded)?;
    write_payload_to_io_or_cached_overflow(env, io_addr, io_capacity, &encoded)
}

pub(crate) fn encoded_optional_columns_payload_size<T: AsRef<[u8]>>(
    columns: &[Option<T>],
) -> Result<usize, String> {
    let mut total = 4usize;
    for column in columns {
        total = total
            .checked_add(1)
            .ok_or_else(|| "encoded columns size overflow".to_string())?;
        if let Some(bytes) = column {
            total = total
                .checked_add(4)
                .and_then(|v| v.checked_add(bytes.as_ref().len()))
                .ok_or_else(|| "encoded column size overflow".to_string())?;
        }
    }
    Ok(total)
}

pub(crate) fn encode_optional_columns_payload_into<T: AsRef<[u8]>>(
    columns: &[Option<T>],
    dst: &mut [u8],
) -> Result<(), String> {
    let expected_len = encoded_optional_columns_payload_size(columns)?;
    if dst.len() != expected_len {
        return Err(format!(
            "direct columns payload size mismatch: expected {}, got {}",
            expected_len,
            dst.len()
        ));
    }
    dst[0..4].copy_from_slice(&(columns.len() as u32).to_be_bytes());
    let mut cursor = 4usize;
    for column in columns {
        match column {
            None => {
                dst[cursor] = 0;
                cursor += 1;
            }
            Some(bytes) => {
                let bytes = bytes.as_ref();
                dst[cursor] = 1;
                cursor += 1;
                dst[cursor..cursor + 4].copy_from_slice(&(bytes.len() as u32).to_be_bytes());
                cursor += 4;
                dst[cursor..cursor + bytes.len()].copy_from_slice(bytes);
                cursor += bytes.len();
            }
        }
    }
    Ok(())
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
    scan_options.scan_options.read_ahead_bytes = Size::from_const(read_ahead_bytes as i64);
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
    scan_options.scan_options = rebuild_scan_options(
        scan_options.scan_options.column_family.clone(),
        Some(decoded),
        scan_options.scan_options.read_ahead_bytes,
        scan_options.scan_options.max_rows(),
    );
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ScanOptions_setColumnFamily(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column_family: JString,
) {
    let Some(scan_options) = scan_options_from_handle_mut_or_throw(&mut env, native_handle) else {
        return;
    };
    let column_family = match decode_java_string(&mut env, column_family) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    scan_options.scan_options = rebuild_scan_options(
        Some(column_family),
        scan_options.scan_options.column_indices.clone(),
        scan_options.scan_options.read_ahead_bytes,
        scan_options.scan_options.max_rows(),
    );
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ScanOptions_clearColumnFamily(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    let Some(scan_options) = scan_options_from_handle_mut_or_throw(&mut env, native_handle) else {
        return;
    };
    scan_options.scan_options = rebuild_scan_options(
        None,
        scan_options.scan_options.column_indices.clone(),
        scan_options.scan_options.read_ahead_bytes,
        scan_options.scan_options.max_rows(),
    );
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ScanOptions_setMaxRows(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    max_rows: jint,
) {
    let Some(scan_options) = scan_options_from_handle_mut_or_throw(&mut env, native_handle) else {
        return;
    };
    if max_rows <= 0 {
        throw_illegal_argument(&mut env, format!("maxRows must be > 0: {}", max_rows));
        return;
    }
    scan_options.scan_options.set_max_rows(max_rows as usize);
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
pub extern "system" fn Java_io_cobble_DirectScanCursor_disposeInternal(
    env: JNIEnv,
    obj: JObject,
    native_handle: jlong,
) {
    Java_io_cobble_ScanCursor_disposeInternal(env, obj, native_handle);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_ScanCursor_nextEntryDirectInternal(
    env: JNIEnv,
    class: JClass,
    native_handle: jlong,
    io_address: jlong,
    io_capacity: jint,
) -> jint {
    Java_io_cobble_DirectScanCursor_nextEntryDirectInternal(
        env,
        class,
        native_handle,
        io_address,
        io_capacity,
    )
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_DirectScanCursor_nextEntryDirectInternal(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    io_address: jlong,
    io_capacity: jint,
) -> jint {
    let Some(cursor) = scan_cursor_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let io_capacity = match usize::try_from(io_capacity) {
        Ok(v) => v,
        Err(_) => {
            throw_illegal_argument(&mut env, "ioCapacity must be >= 0".to_string());
            return 0;
        }
    };
    let io_address = match usize::try_from(io_address) {
        Ok(v) if v != 0 => v as *mut u8,
        _ => {
            throw_illegal_argument(&mut env, "ioAddress must be > 0".to_string());
            return 0;
        }
    };
    cursor.next_entry_direct(&mut env, io_address, io_capacity)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_openDirectScanCursor(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    bucket: jint,
    start_key_address: jlong,
    start_key_length: jint,
    end_key_address: jlong,
    end_key_length: jint,
    scan_options_handle: jlong,
) -> jlong {
    let Some(db) = super::db::db_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let Some(args) = decode_scan_open_direct_bounds_args(
        &mut env,
        bucket,
        start_key_address,
        start_key_length,
        end_key_address,
        end_key_length,
        scan_options_handle,
    ) else {
        return 0;
    };
    let iter = match args.scan_options_handle {
        Some(scan_options_handle) => match db.scan_with_options_bounds(
            args.bucket,
            args.start_key_inclusive.as_deref(),
            args.end_key_exclusive.as_deref(),
            scan_options_handle.scan_options(),
        ) {
            Ok(v) => v,
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                return 0;
            }
        },
        None => match db.scan_bounds(
            args.bucket,
            args.start_key_inclusive.as_deref(),
            args.end_key_exclusive.as_deref(),
        ) {
            Ok(v) => v,
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                return 0;
            }
        },
    };
    Box::into_raw(Box::new(ScanCursorHandle::from_static_iter(iter))) as jlong
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
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return 0;
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
    let scanner = if scan_options_handle == 0 {
        match split.create_scanner_without_options(config) {
            Ok(v) => v,
            Err(err) => {
                throw_illegal_state(env, err.to_string());
                return 0;
            }
        }
    } else {
        let Some(options_handle) = scan_options_from_handle_or_throw(env, scan_options_handle)
        else {
            return 0;
        };
        match split.create_scanner(config, options_handle.scan_options()) {
            Ok(v) => v,
            Err(err) => {
                throw_illegal_state(env, err.to_string());
                return 0;
            }
        }
    };
    Box::into_raw(Box::new(ScanCursorHandle::from_split_scanner(scanner))) as jlong
}

pub(crate) fn scan_options_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static ScanOptionsHandle> {
    if native_handle == 0 {
        throw_illegal_state(env, "scan options handle is disposed".to_string());
        return None;
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
