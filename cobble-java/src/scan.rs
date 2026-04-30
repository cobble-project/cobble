use crate::util::{
    byte_array_2d_class, byte_array_class, decode_column_index, decode_java_bytes,
    decode_java_string, decode_u16, new_object_array, new_scan_batch, parse_config_json,
    throw_illegal_argument, throw_illegal_state, write_payload_to_io_or_cached_overflow,
};
use bytes::Bytes;
use cobble::{Config, DbIterator, Result, ScanOptions, ScanSplit, ScanSplitScanner};
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JIntArray, JObject, JString};
use jni::sys::{jint, jlong, jobject};
use size::Size;

const DEFAULT_SCAN_BATCH_SIZE: usize = 16;

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
    pub(crate) batch_size: usize,
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
    let (scan_options_handle, batch_size) = if scan_options_handle == 0 {
        (None, DEFAULT_SCAN_BATCH_SIZE)
    } else {
        let handle = scan_options_from_handle_or_throw(env, scan_options_handle)?;
        (Some(handle), handle.batch_size())
    };
    Some(ScanOpenArgs {
        bucket,
        start_key_inclusive,
        end_key_exclusive,
        scan_options_handle,
        batch_size,
    })
}

pub(crate) fn decode_scan_open_direct_args(
    env: &mut JNIEnv,
    bucket: jint,
    start_key_address: jlong,
    start_key_length: jint,
    end_key_address: jlong,
    end_key_length: jint,
    scan_options_handle: jlong,
) -> Option<ScanOpenArgs> {
    let bucket = match decode_u16("bucket", bucket) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(env, err);
            return None;
        }
    };
    let start_key_inclusive = decode_direct_bytes(
        env,
        "startKeyInclusive",
        start_key_address,
        start_key_length,
    )?;
    let end_key_exclusive =
        decode_direct_bytes(env, "endKeyExclusive", end_key_address, end_key_length)?;
    let (scan_options_handle, batch_size) = if scan_options_handle == 0 {
        (None, DEFAULT_SCAN_BATCH_SIZE)
    } else {
        let handle = scan_options_from_handle_or_throw(env, scan_options_handle)?;
        (Some(handle), handle.batch_size())
    };
    Some(ScanOpenArgs {
        bucket,
        start_key_inclusive,
        end_key_exclusive,
        scan_options_handle,
        batch_size,
    })
}

fn decode_direct_bytes(
    env: &mut JNIEnv,
    name: &str,
    address: jlong,
    length: jint,
) -> Option<Vec<u8>> {
    let length = match usize::try_from(length) {
        Ok(v) => v,
        Err(_) => {
            throw_illegal_argument(env, format!("{name} length must be >= 0"));
            return None;
        }
    };
    let address = match usize::try_from(address) {
        Ok(v) if v != 0 => v as *const u8,
        _ => {
            throw_illegal_argument(env, format!("{name} address must be > 0"));
            return None;
        }
    };
    let bytes = unsafe { std::slice::from_raw_parts(address, length) };
    Some(bytes.to_vec())
}

pub(crate) struct ScanCursorHandle {
    inner: Box<StaticScanCursorInner>,
}

type ScanValues = Vec<Option<Vec<u8>>>;
type ScanRow = (Vec<u8>, ScanValues);

enum ScanCursorIter {
    Db(Box<DbIterator<'static>>),
    Split(Box<ScanSplitScanner>),
}

impl ScanCursorHandle {
    pub(crate) fn from_static_iter(
        iter: DbIterator<'static>,
        batch_size: usize,
    ) -> ScanCursorHandle {
        ScanCursorHandle {
            inner: Box::new(StaticScanCursorInner {
                iter: ScanCursorIter::Db(Box::new(iter)),
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
                iter: ScanCursorIter::Split(Box::new(iter)),
                batch_size,
                pending: None,
                exhausted: false,
            }),
        }
    }

    fn next_batch(&mut self) -> Result<ScanBatch> {
        self.inner.next_batch()
    }

    fn next_batch_direct<'local>(
        &mut self,
        env: &mut JNIEnv<'local>,
        io_addr: *mut u8,
        io_capacity: usize,
    ) -> jint {
        self.inner.next_batch_direct(env, io_addr, io_capacity)
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

    fn next_batch_direct<'local>(
        &mut self,
        env: &mut JNIEnv<'local>,
        io_addr: *mut u8,
        io_capacity: usize,
    ) -> jint {
        if self.exhausted {
            return 0;
        }
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&0u32.to_be_bytes());
        encoded.push(0);
        let mut row_count = 0usize;

        if let Some((key, value)) = self.pending.take() {
            if let Err(err) = append_direct_scan_row_payload(&mut encoded, &key, &value) {
                throw_illegal_state(env, err);
                return 0;
            }
            row_count += 1;
        }

        while row_count < self.batch_size {
            match self.next_row() {
                Some(Ok(row)) => {
                    if let Some((key, value)) = convert_row(row) {
                        if let Err(err) = append_direct_scan_row_payload(&mut encoded, &key, &value)
                        {
                            throw_illegal_state(env, err);
                            return 0;
                        }
                        row_count += 1;
                    }
                }
                Some(Err(err)) => {
                    throw_illegal_state(env, err.to_string());
                    return 0;
                }
                None => {
                    self.exhausted = true;
                    break;
                }
            }
        }

        if row_count == 0 {
            self.exhausted = true;
            return 0;
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
                    Some(Err(err)) => {
                        throw_illegal_state(env, err.to_string());
                        return 0;
                    }
                    None => {
                        self.exhausted = true;
                        break;
                    }
                }
            }
        }

        encoded[0..4].copy_from_slice(&(row_count as u32).to_be_bytes());
        encoded[4] = if has_more { 1 } else { 0 };
        match write_payload_to_io_or_cached_overflow(env, io_addr, io_capacity, &encoded) {
            Ok(v) => v,
            Err(err) => {
                throw_illegal_state(env, err);
                0
            }
        }
    }

    fn next_row(&mut self) -> Option<cobble::Result<(Bytes, Vec<Option<Bytes>>)>> {
        match &mut self.iter {
            ScanCursorIter::Db(iter) => iter.as_mut().next(),
            ScanCursorIter::Split(iter) => iter.as_mut().next(),
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

fn append_direct_scan_row_payload(
    encoded: &mut Vec<u8>,
    key: &[u8],
    columns: &[Option<Vec<u8>>],
) -> Result<(), String> {
    let row_size = encoded_optional_columns_payload_size(columns)?;
    let needed = encoded
        .len()
        .checked_add(4)
        .and_then(|v| v.checked_add(key.len()))
        .and_then(|v| v.checked_add(4))
        .and_then(|v| v.checked_add(row_size))
        .ok_or_else(|| "encoded direct scan batch size overflow".to_string())?;
    let previous_len = encoded.len();
    encoded.resize(needed, 0);
    encoded[previous_len..previous_len + 4].copy_from_slice(&(key.len() as u32).to_be_bytes());
    let mut cursor = previous_len + 4;
    encoded[cursor..cursor + key.len()].copy_from_slice(key);
    cursor += key.len();
    encoded[cursor..cursor + 4].copy_from_slice(&(row_size as u32).to_be_bytes());
    cursor += 4;
    encode_optional_columns_payload_into(columns, &mut encoded[cursor..cursor + row_size])?;
    Ok(())
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
pub extern "system" fn Java_io_cobble_DirectScanCursor_nextBatchDirectInternal(
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
    cursor.next_batch_direct(&mut env, io_address, io_capacity)
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
    let Some(args) = decode_scan_open_direct_args(
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
        Some(scan_options_handle) => match db.scan_with_options(
            args.bucket,
            args.start_key_inclusive.as_slice()..args.end_key_exclusive.as_slice(),
            scan_options_handle.scan_options(),
        ) {
            Ok(v) => v,
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                return 0;
            }
        },
        None => match db.scan(
            args.bucket,
            args.start_key_inclusive.as_slice()..args.end_key_exclusive.as_slice(),
        ) {
            Ok(v) => v,
            Err(err) => {
                throw_illegal_state(&mut env, err.to_string());
                return 0;
            }
        },
    };
    Box::into_raw(Box::new(ScanCursorHandle::from_static_iter(
        iter,
        args.batch_size,
    ))) as jlong
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
    let (scanner, batch_size) = if scan_options_handle == 0 {
        match split.create_scanner_without_options(config) {
            Ok(v) => (v, DEFAULT_SCAN_BATCH_SIZE),
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
            Ok(v) => (v, options_handle.batch_size()),
            Err(err) => {
                throw_illegal_state(env, err.to_string());
                return 0;
            }
        }
    };
    Box::into_raw(Box::new(ScanCursorHandle::from_split_scanner(
        scanner, batch_size,
    ))) as jlong
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
    let result = new_scan_batch(env, &keys_obj, &values_obj, &next_obj, batch.has_more)?;
    Ok(result.into_raw())
}

fn to_java_bytes_array(
    env: &mut JNIEnv,
    items: Vec<Vec<u8>>,
) -> std::result::Result<jobject, String> {
    let byte_array_class = byte_array_class(env)?;
    let array = new_object_array(env, items.len() as i32, byte_array_class)?;
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
    let byte_array_2d_class = byte_array_2d_class(env)?;
    let byte_array_class = byte_array_class(env)?;
    let array = new_object_array(env, items.len() as i32, byte_array_2d_class)?;
    for (index, columns) in items.into_iter().enumerate() {
        let column_array = new_object_array(env, columns.len() as i32, byte_array_class)?;
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
