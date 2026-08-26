use bytes::Bytes;
use cobble_binding::{Config, ExpandStorageMode, RecoveryMode};
use jni::JNIEnv;
use jni::descriptors::Desc;
use jni::objects::{
    AutoLocal, GlobalRef, JByteArray, JByteBuffer, JClass, JIntArray, JMethodID, JObject,
    JObjectArray, JString, JThrowable, JValue,
};
use jni::signature::{Primitive, ReturnType};
use jni::sys::{jint, jlong, jobject, jstring};
use std::cell::RefCell;
use std::ops::RangeInclusive;
use std::sync::OnceLock;

/// Error type that bridges JNI [`Error`]s and plain strings, so local-frame
/// closures can also surface string diagnostics.
pub(crate) struct FrameError(pub(crate) String);

impl From<jni::errors::Error> for FrameError {
    fn from(err: jni::errors::Error) -> Self {
        FrameError(err.to_string())
    }
}

impl From<String> for FrameError {
    fn from(err: String) -> Self {
        FrameError(err)
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Utils_versionString(env: JNIEnv, class: JClass) -> jstring {
    native_string(env, class, cobble_binding::ffi::build_version_string())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Utils_buildCommitId(env: JNIEnv, class: JClass) -> jstring {
    native_string(env, class, cobble_binding::ffi::build_commit_short_id())
}

fn native_string(env: JNIEnv, _class: JClass, value: &'static str) -> jstring {
    match env.new_string(value) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

pub(crate) fn decode_java_string(env: &mut JNIEnv, value: JString) -> Result<String, String> {
    env.get_string(&value)
        .map(|s| s.into())
        .map_err(|err| format!("invalid java string: {}", err))
}

pub(crate) fn decode_optional_java_string(
    env: &mut JNIEnv,
    value: JString,
) -> Result<Option<String>, String> {
    if value.is_null() {
        return Ok(None);
    }
    decode_java_string(env, value).map(Some)
}

pub(crate) fn decode_java_bytes(env: &mut JNIEnv, value: JByteArray) -> Result<Vec<u8>, String> {
    env.convert_byte_array(value)
        .map_err(|err| format!("invalid java byte array: {}", err))
}

pub(crate) fn decode_optional_java_bytes(
    env: &mut JNIEnv,
    value: JByteArray,
) -> Result<Option<Vec<u8>>, String> {
    if value.is_null() {
        return Ok(None);
    }
    decode_java_bytes(env, value).map(Some)
}

pub(crate) fn decode_java_bytes_ref(
    env: &mut JNIEnv,
    value: &JByteArray,
) -> Result<Vec<u8>, String> {
    env.convert_byte_array(value)
        .map_err(|err| format!("invalid java byte array: {}", err))
}

pub(crate) fn decode_u16(name: &str, value: jint) -> Result<u16, String> {
    if value < 0 || value > u16::MAX as jint {
        return Err(format!("{} out of range: {}", name, value));
    }
    Ok(value as u16)
}

pub(crate) fn decode_u32(name: &str, value: jint) -> Result<u32, String> {
    if value < 0 {
        return Err(format!("{} out of range: {}", name, value));
    }
    Ok(value as u32)
}

pub(crate) fn expand_storage_mode(value: jint) -> Result<ExpandStorageMode, String> {
    match value {
        0 => Ok(ExpandStorageMode::AdoptAsync),
        1 => Ok(ExpandStorageMode::ReferencePersistent),
        2 => Ok(ExpandStorageMode::ReferencePersistentWithCache),
        _ => Err(format!("unknown expand storage mode {value}")),
    }
}

pub(crate) fn decode_recovery_mode(value: jint) -> Result<RecoveryMode, String> {
    match value {
        0 => Ok(RecoveryMode::SnapshotOnly),
        1 => Ok(RecoveryMode::LatestWithWal),
        _ => Err(format!("unknown recovery mode {value}")),
    }
}

pub(crate) fn decode_u64_from_jlong(name: &str, value: jlong) -> Result<u64, String> {
    if value < 0 {
        return Err(format!("{} out of range: {}", name, value));
    }
    Ok(value as u64)
}

pub(crate) fn decode_column_index(value: jint) -> Result<usize, String> {
    if value < 0 {
        return Err(format!("column out of range: {}", value));
    }
    Ok(value as usize)
}

pub(crate) fn decode_bucket_ranges(
    env: &mut JNIEnv,
    starts: JIntArray,
    ends: JIntArray,
) -> Result<Vec<RangeInclusive<u16>>, String> {
    let starts_len = env
        .get_array_length(&starts)
        .map_err(|err| format!("invalid range start array: {}", err))?;
    let ends_len = env
        .get_array_length(&ends)
        .map_err(|err| format!("invalid range end array: {}", err))?;
    if starts_len != ends_len {
        return Err(format!(
            "range start/end array length mismatch: {} != {}",
            starts_len, ends_len
        ));
    }
    let mut raw_starts = vec![0; starts_len as usize];
    let mut raw_ends = vec![0; ends_len as usize];
    env.get_int_array_region(&starts, 0, &mut raw_starts)
        .map_err(|err| format!("invalid range start array: {}", err))?;
    env.get_int_array_region(&ends, 0, &mut raw_ends)
        .map_err(|err| format!("invalid range end array: {}", err))?;

    let mut ranges = Vec::with_capacity(raw_starts.len());
    for (index, (start, end)) in raw_starts.into_iter().zip(raw_ends).enumerate() {
        let start = decode_u16(&format!("rangeStarts[{}]", index), start)?;
        let end = decode_u16(&format!("rangeEnds[{}]", index), end)?;
        if start > end {
            return Err(format!(
                "rangeStarts[{}] must be <= rangeEnds[{}], but got {}..={}",
                index, index, start, end
            ));
        }
        ranges.push(start..=end);
    }
    Ok(ranges)
}

pub(crate) fn throw_illegal_state(env: &mut JNIEnv, message: String) {
    let class = match illegal_state_exception_class(env) {
        Ok(v) => v,
        Err(_) => return,
    };
    let ctor = match illegal_state_exception_ctor(env) {
        Ok(v) => v,
        Err(_) => return,
    };
    let exception = match new_exception(env, class, ctor, message) {
        Ok(v) => v,
        Err(_) => return,
    };
    let _ = env.throw(JThrowable::from(exception));
}

pub(crate) fn throw_illegal_argument(env: &mut JNIEnv, message: String) {
    let class = match illegal_argument_exception_class(env) {
        Ok(v) => v,
        Err(_) => return,
    };
    let ctor = match illegal_argument_exception_ctor(env) {
        Ok(v) => v,
        Err(_) => return,
    };
    let exception = match new_exception(env, class, ctor, message) {
        Ok(v) => v,
        Err(_) => return,
    };
    let _ = env.throw(JThrowable::from(exception));
}

pub(crate) fn parse_config_json(env: &mut JNIEnv, json: &str) -> Option<Config> {
    match Config::from_json_str(json) {
        Ok(config) => Some(config),
        Err(err) => {
            throw_illegal_argument(env, format!("invalid config json: {}", err));
            None
        }
    }
}

pub(crate) fn to_java_string_or_throw(env: &mut JNIEnv, value: String) -> jstring {
    match env.new_string(value) {
        Ok(s) => s.into_raw(),
        Err(err) => {
            throw_illegal_state(env, err.to_string());
            std::ptr::null_mut()
        }
    }
}

pub(crate) fn to_java_optional_bytes_2d(
    env: &mut JNIEnv,
    columns: &[Option<Bytes>],
) -> Result<jobject, String> {
    let byte_array_class = byte_array_class(env)?;
    let array = new_object_array(env, columns.len() as i32, byte_array_class)?;
    for (index, column) in columns.iter().enumerate() {
        let Some(bytes) = column.as_ref() else {
            continue;
        };
        let value = env
            .byte_array_from_slice(bytes)
            .map_err(|err| err.to_string())?;
        env.set_object_array_element(&array, index as i32, value)
            .map_err(|err| err.to_string())?;
    }
    Ok(array.into_raw() as jobject)
}

/// Builds a Java `byte[][][]` from a multi-get result.
///
/// Each outer element is `null` (key not found) or a `byte[][]` produced by
/// [`to_java_optional_bytes_2d`] (the column values for that key).
///
/// The outer array lives in the caller's frame; each non-null row is built
/// inside its own local reference frame so the per-row `byte[]`/`byte[][]`
/// intermediates are released as soon as the row is stored. Only the promoted
/// row reference survives, so large batches never accumulate unbounded local
/// refs.
pub(crate) fn to_java_optional_bytes_3d(
    env: &mut JNIEnv,
    rows: &[Option<Vec<Option<Bytes>>>],
) -> Result<jobject, String> {
    let byte_array_2d = byte_array_2d_class(env)?;
    let array = new_object_array(env, rows.len() as i32, byte_array_2d)?;
    for (index, row) in rows.iter().enumerate() {
        let Some(columns) = row.as_ref() else {
            continue;
        };
        // Build and store this row in a temporary frame. The outer array keeps
        // the row alive after every local ref created for the row is released.
        env.with_local_frame(16, |env| {
            let row = to_java_optional_bytes_2d(env, columns.as_slice())?;
            // SAFETY: `row` is a freshly created `byte[][]` local ref.
            let row = unsafe { JObject::from_raw(row) };
            env.set_object_array_element(&array, index as i32, &row)?;
            Ok(())
        })
        .map_err(|err: FrameError| err.0)?;
    }
    Ok(array.into_raw() as jobject)
}

/// Decodes parallel `int[] buckets` + `byte[][] keys` into a `Vec<(u16, Vec<u8>)>`.
///
/// The two arrays must have the same length. Each bucket must fit in `u16`.
pub(crate) fn decode_multi_get_keys<'local>(
    env: &mut JNIEnv<'local>,
    buckets: &JIntArray<'local>,
    keys: &JObjectArray<'local>,
) -> Result<Vec<(u16, Vec<u8>)>, String> {
    let bucket_count = env
        .get_array_length(buckets)
        .map_err(|err| format!("invalid buckets array: {}", err))?;
    let key_count = env
        .get_array_length(keys)
        .map_err(|err| format!("invalid keys array: {}", err))?;
    if bucket_count != key_count {
        return Err(format!(
            "buckets length {} does not match keys length {}",
            bucket_count, key_count
        ));
    }
    let mut raw_buckets = vec![0i32; bucket_count as usize];
    env.get_int_array_region(buckets, 0, &mut raw_buckets)
        .map_err(|err| format!("invalid buckets array: {}", err))?;
    let mut result = Vec::with_capacity(raw_buckets.len());
    for (i, &bucket) in raw_buckets.iter().enumerate() {
        if bucket < 0 || bucket > u16::MAX as i32 {
            return Err(format!("bucket out of range at index {}: {}", i, bucket));
        }
        // `AutoLocal` releases each key's local ref after we copy the bytes out,
        // so large batches don't exhaust the JNI local-reference table.
        let key_obj = env
            .get_object_array_element(keys, i as i32)
            .map_err(|err| format!("failed to get key at index {}: {}", i, err))?;
        let key_obj = AutoLocal::new(key_obj, env);
        let key: &JByteArray = key_obj.as_ref().into();
        let key_bytes = env
            .convert_byte_array(key)
            .map_err(|err| format!("invalid key byte array at index {}: {}", i, err))?;
        result.push((bucket as u16, key_bytes));
    }
    Ok(result)
}

/// Decodes packed keys from a direct buffer for multi-get direct I/O.
///
/// Format: `i32 num_keys`, then per key: `i32 bucket`, `i32 key_length`, `key_bytes`.
///
/// # Safety
///
/// The caller must ensure `addr` points to at least `capacity` readable bytes.
pub(crate) fn decode_packed_multi_get_keys(
    addr: *const u8,
    capacity: usize,
) -> Result<Vec<(u16, Vec<u8>)>, String> {
    if capacity < 4 {
        return Err("IO buffer too small for multi-get key header".to_string());
    }
    // SAFETY: caller guarantees `addr..addr+capacity` is valid.
    let buf = unsafe { std::slice::from_raw_parts(addr, capacity) };
    let num_keys = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    // Reject malformed headers before allocating. Each key needs at least 8 bytes
    // (i32 bucket + i32 key_length) in addition to the 4-byte count prefix.
    let max_keys = (capacity - 4) / 8;
    if num_keys > max_keys {
        return Err(format!(
            "multi-get num_keys {} exceeds buffer capacity {} (max {} keys)",
            num_keys, capacity, max_keys
        ));
    }
    let mut offset = 4usize;
    let mut keys = Vec::with_capacity(num_keys);
    for i in 0..num_keys {
        if offset + 8 > capacity {
            return Err(format!(
                "IO buffer too small for multi-get key {} header",
                i
            ));
        }
        let bucket = u32::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);
        if bucket > u16::MAX as u32 {
            return Err(format!("bucket out of range at key {}: {}", i, bucket));
        }
        offset += 4;
        let key_len = u32::from_be_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]) as usize;
        offset += 4;
        if offset
            .checked_add(key_len)
            .map(|end| end > capacity)
            .unwrap_or(true)
        {
            return Err(format!("IO buffer too small for multi-get key {} bytes", i));
        }
        let key = buf[offset..offset + key_len].to_vec();
        offset += key_len;
        keys.push((bucket as u16, key));
    }
    Ok(keys)
}

pub(crate) fn new_object_array<'local>(
    env: &mut JNIEnv<'local>,
    length: i32,
    element_class: &GlobalRef,
) -> Result<JObjectArray<'local>, String> {
    env.new_object_array(length, element_class, JObject::null())
        .map_err(|err| err.to_string())
}

pub(crate) fn complete_future_with_string(
    env: &mut JNIEnv,
    future: &JObject,
    value: &str,
) -> Result<(), String> {
    let value = env.new_string(value).map_err(|err| err.to_string())?;
    let value_obj = JObject::from(value);
    let args = [JValue::Object(&value_obj).as_jni()];
    let method = completable_future_complete_method(env)?;
    unsafe {
        env.call_method_unchecked(
            future,
            method,
            ReturnType::Primitive(Primitive::Boolean),
            &args,
        )
    }
    .map(|_| ())
    .map_err(|err| err.to_string())
}

pub(crate) fn complete_future_exceptionally(
    env: &mut JNIEnv,
    future: &JObject,
    message: &str,
) -> Result<(), String> {
    complete_future_exceptionally_with_throwable(env, future, message, false)
}

pub(crate) fn complete_future_with_cobble_error(
    env: &mut JNIEnv,
    future: &JObject,
    err: &cobble_binding::Error,
) -> Result<(), String> {
    complete_future_exceptionally_with_throwable(
        env,
        future,
        &err.to_string(),
        matches!(err, cobble_binding::Error::CancelledError(_)),
    )
}

fn complete_future_exceptionally_with_throwable(
    env: &mut JNIEnv,
    future: &JObject,
    message: &str,
    cancelled: bool,
) -> Result<(), String> {
    let (class, ctor) = if cancelled {
        (cancelled_error_class(env)?, cancelled_error_ctor(env)?)
    } else {
        (
            illegal_state_exception_class(env)?,
            illegal_state_exception_ctor(env)?,
        )
    };
    let exception = new_exception(env, class, ctor, message.to_string())?;
    let throwable = JThrowable::from(exception);
    let args = [JValue::Object(&throwable).as_jni()];
    let method = completable_future_complete_exceptionally_method(env)?;
    unsafe {
        env.call_method_unchecked(
            future,
            method,
            ReturnType::Primitive(Primitive::Boolean),
            &args,
        )
    }
    .map(|_| ())
    .map_err(|err| err.to_string())
}

pub(crate) fn write_payload_to_io_or_cached_overflow<'local>(
    env: &mut JNIEnv<'local>,
    io_addr: *mut u8,
    io_capacity: usize,
    payload: &[u8],
) -> Result<jint, String> {
    if payload.len() > i32::MAX as usize {
        return Err("encoded payload too large".to_string());
    }
    let encoded_len = payload.len() as jint;
    if payload.len() <= io_capacity {
        let out = unsafe { std::slice::from_raw_parts_mut(io_addr, payload.len()) };
        out.copy_from_slice(payload);
        return Ok(encoded_len);
    }
    let overflow = get_or_grow_overflow_direct_buffer(env, encoded_len)?;
    let overflow_addr = env
        .get_direct_buffer_address(&overflow)
        .map_err(|err| format!("failed to access overflow direct buffer: {err}"))?;
    let out = unsafe { std::slice::from_raw_parts_mut(overflow_addr, payload.len()) };
    out.copy_from_slice(payload);
    Ok(-encoded_len)
}

pub(crate) fn take_last_overflow_direct_buffer<'local>(
    env: &mut JNIEnv<'local>,
) -> Result<jobject, String> {
    LAST_OVERFLOW_DIRECT_BUFFER.with(|slot| {
        let guard = slot.borrow();
        let Some(global) = guard.as_ref() else {
            return Ok(std::ptr::null_mut());
        };
        let local = env
            .new_local_ref(global.as_obj())
            .map_err(|err| err.to_string())?;
        Ok(local.into_raw())
    })
}

thread_local! {
    static LAST_OVERFLOW_DIRECT_BUFFER: RefCell<Option<GlobalRef>> = const { RefCell::new(None) };
}

fn get_or_grow_overflow_direct_buffer<'local>(
    env: &mut JNIEnv<'local>,
    required: jint,
) -> Result<JByteBuffer<'local>, String> {
    LAST_OVERFLOW_DIRECT_BUFFER.with(|slot| {
        if let Some(existing) = slot.borrow().as_ref() {
            let local = env
                .new_local_ref(existing.as_obj())
                .map_err(|err| err.to_string())?;
            let buffer = JByteBuffer::from(local);
            let capacity = env
                .get_direct_buffer_capacity(&buffer)
                .map_err(|err| format!("failed to get overflow direct buffer capacity: {err}"))?;
            if capacity >= required as usize {
                return Ok(buffer);
            }
        }

        let fresh = allocate_direct_byte_buffer(env, required)?;
        let global = env.new_global_ref(&fresh).map_err(|err| err.to_string())?;
        *slot.borrow_mut() = Some(global);
        Ok(fresh)
    })
}

fn allocate_direct_byte_buffer<'local>(
    env: &mut JNIEnv<'local>,
    size: jint,
) -> Result<JByteBuffer<'local>, String> {
    let value = env
        .call_static_method(
            "java/nio/ByteBuffer",
            "allocateDirect",
            "(I)Ljava/nio/ByteBuffer;",
            &[JValue::Int(size)],
        )
        .map_err(|err| err.to_string())?;
    let obj = value.l().map_err(|err| err.to_string())?;
    Ok(JByteBuffer::from(obj))
}

pub(crate) fn object_class(env: &mut JNIEnv) -> Result<&'static GlobalRef, String> {
    static CLASS: OnceLock<GlobalRef> = OnceLock::new();
    cached_class(env, &CLASS, "java/lang/Object")
}

pub(crate) fn object_array_class(env: &mut JNIEnv) -> Result<&'static GlobalRef, String> {
    static CLASS: OnceLock<GlobalRef> = OnceLock::new();
    cached_class(env, &CLASS, "[Ljava/lang/Object;")
}

pub(crate) fn byte_array_class(env: &mut JNIEnv) -> Result<&'static GlobalRef, String> {
    static CLASS: OnceLock<GlobalRef> = OnceLock::new();
    cached_class(env, &CLASS, "[B")
}

pub(crate) fn byte_array_2d_class(env: &mut JNIEnv) -> Result<&'static GlobalRef, String> {
    static CLASS: OnceLock<GlobalRef> = OnceLock::new();
    cached_class(env, &CLASS, "[[B")
}

fn illegal_state_exception_class(env: &mut JNIEnv) -> Result<&'static GlobalRef, String> {
    static CLASS: OnceLock<GlobalRef> = OnceLock::new();
    cached_class(env, &CLASS, "java/lang/IllegalStateException")
}

fn illegal_argument_exception_class(env: &mut JNIEnv) -> Result<&'static GlobalRef, String> {
    static CLASS: OnceLock<GlobalRef> = OnceLock::new();
    cached_class(env, &CLASS, "java/lang/IllegalArgumentException")
}

fn cancelled_error_class(env: &mut JNIEnv) -> Result<&'static GlobalRef, String> {
    static CLASS: OnceLock<GlobalRef> = OnceLock::new();
    cached_class(env, &CLASS, "io/cobble/CancelledError")
}

fn completable_future_class(env: &mut JNIEnv) -> Result<&'static GlobalRef, String> {
    static CLASS: OnceLock<GlobalRef> = OnceLock::new();
    cached_class(env, &CLASS, "java/util/concurrent/CompletableFuture")
}

fn illegal_state_exception_ctor(env: &mut JNIEnv) -> Result<JMethodID, String> {
    static METHOD: OnceLock<JMethodID> = OnceLock::new();
    let class = illegal_state_exception_class(env)?;
    cached_constructor_id(env, &METHOD, class, "(Ljava/lang/String;)V")
}

fn illegal_argument_exception_ctor(env: &mut JNIEnv) -> Result<JMethodID, String> {
    static METHOD: OnceLock<JMethodID> = OnceLock::new();
    let class = illegal_argument_exception_class(env)?;
    cached_constructor_id(env, &METHOD, class, "(Ljava/lang/String;)V")
}

fn cancelled_error_ctor(env: &mut JNIEnv) -> Result<JMethodID, String> {
    static METHOD: OnceLock<JMethodID> = OnceLock::new();
    let class = cancelled_error_class(env)?;
    cached_constructor_id(env, &METHOD, class, "(Ljava/lang/String;)V")
}

fn completable_future_complete_method(env: &mut JNIEnv) -> Result<JMethodID, String> {
    static METHOD: OnceLock<JMethodID> = OnceLock::new();
    let class = completable_future_class(env)?;
    cached_instance_method_id(env, &METHOD, class, "complete", "(Ljava/lang/Object;)Z")
}

fn completable_future_complete_exceptionally_method(env: &mut JNIEnv) -> Result<JMethodID, String> {
    static METHOD: OnceLock<JMethodID> = OnceLock::new();
    let class = completable_future_class(env)?;
    cached_instance_method_id(
        env,
        &METHOD,
        class,
        "completeExceptionally",
        "(Ljava/lang/Throwable;)Z",
    )
}

fn cached_class(
    env: &mut JNIEnv,
    cache: &'static OnceLock<GlobalRef>,
    class_name: &str,
) -> Result<&'static GlobalRef, String> {
    if let Some(class) = cache.get() {
        return Ok(class);
    }
    let local = env.find_class(class_name).map_err(|err| err.to_string())?;
    let global = env.new_global_ref(local).map_err(|err| err.to_string())?;
    let _ = cache.set(global);
    cache
        .get()
        .ok_or_else(|| format!("failed to cache class {}", class_name))
}

fn cached_constructor_id(
    env: &mut JNIEnv,
    cache: &'static OnceLock<JMethodID>,
    class: &GlobalRef,
    signature: &str,
) -> Result<JMethodID, String> {
    if let Some(method) = cache.get() {
        return Ok(*method);
    }
    let method =
        Desc::<JMethodID>::lookup((class, signature), env).map_err(|err| err.to_string())?;
    let _ = cache.set(method);
    cache
        .get()
        .copied()
        .ok_or_else(|| format!("failed to cache constructor {}", signature))
}

fn cached_instance_method_id(
    env: &mut JNIEnv,
    cache: &'static OnceLock<JMethodID>,
    class: &GlobalRef,
    name: &str,
    signature: &str,
) -> Result<JMethodID, String> {
    if let Some(method) = cache.get() {
        return Ok(*method);
    }
    let method =
        Desc::<JMethodID>::lookup((class, name, signature), env).map_err(|err| err.to_string())?;
    let _ = cache.set(method);
    cache
        .get()
        .copied()
        .ok_or_else(|| format!("failed to cache method {}{}", name, signature))
}

fn new_exception<'local>(
    env: &mut JNIEnv<'local>,
    class: &GlobalRef,
    ctor: JMethodID,
    message: String,
) -> Result<JObject<'local>, String> {
    let message = env.new_string(message).map_err(|err| err.to_string())?;
    let message_obj = JObject::from(message);
    let args = [JValue::Object(&message_obj).as_jni()];
    unsafe { env.new_object_unchecked(class, ctor, &args) }.map_err(|err| err.to_string())
}
