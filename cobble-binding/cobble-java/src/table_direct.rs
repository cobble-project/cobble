use crate::db::{
    direct_buffer_pool_config_array, encode_multi_get_payload,
    encode_optional_columns_to_direct_buffer,
};
use crate::table::table_handle_from_handle_or_throw;
use crate::util::{
    decode_packed_multi_get_keys, decode_u16, take_owned_overflow_direct_buffer,
    throw_illegal_argument, throw_illegal_state, write_payload_to_io_or_cached_overflow,
};
use bytes::Bytes;
use jni::JNIEnv;
use jni::objects::{JByteBuffer, JClass};
use jni::sys::{jint, jintArray, jlong, jobject};
use std::fmt::Display;

pub(crate) fn encode_direct_multi_get<E>(
    env: &mut JNIEnv,
    buffer: JByteBuffer,
    get: impl FnOnce(&[(u16, &[u8])]) -> Result<Vec<Option<Vec<Option<Bytes>>>>, E>,
) -> jint
where
    E: Display,
{
    let decoded = (|| {
        let capacity = env
            .get_direct_buffer_capacity(&buffer)
            .map_err(|e| e.to_string())?;
        let address = env
            .get_direct_buffer_address(&buffer)
            .map_err(|e| e.to_string())?;
        let input = unsafe { std::slice::from_raw_parts(address, capacity) };
        let keys = decode_packed_multi_get_keys(input)?;
        Ok::<_, String>((capacity, address, keys))
    })();
    let (capacity, address, keys) = match decoded {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(env, error);
            return 0;
        }
    };
    let rows = match get(&keys) {
        Ok(rows) => rows,
        Err(error) => {
            throw_illegal_state(env, error.to_string());
            return 0;
        }
    };
    drop(keys);
    let payload = match encode_multi_get_payload(&rows) {
        Ok(payload) => payload,
        Err(error) => {
            throw_illegal_state(env, error);
            return 0;
        }
    };
    match write_payload_to_io_or_cached_overflow(env, address, capacity, &payload) {
        Ok(length) => length,
        Err(error) => {
            throw_illegal_state(env, error);
            0
        }
    }
}

pub(crate) fn encode_direct_get<E>(
    env: &mut JNIEnv,
    bucket: jint,
    buffer: JByteBuffer,
    key_length: jint,
    get: impl FnOnce(u16, &[u8]) -> Result<Option<Vec<Option<Bytes>>>, E>,
) -> jint
where
    E: Display,
{
    let decoded = (|| {
        let bucket = decode_u16("bucket", bucket)?;
        let capacity = env
            .get_direct_buffer_capacity(&buffer)
            .map_err(|e| e.to_string())?;
        let address = env
            .get_direct_buffer_address(&buffer)
            .map_err(|e| e.to_string())?;
        let length = usize::try_from(key_length)
            .ok()
            .filter(|length| *length <= capacity)
            .ok_or_else(|| "keyLength is outside the direct buffer".to_string())?;
        Ok::<_, String>((bucket, capacity, address, length))
    })();
    let (bucket, capacity, address, length) = match decoded {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(env, error);
            return 0;
        }
    };
    // The Java pooled buffer is retained throughout this synchronous call. Finish reading the
    // key before overwriting the same buffer with encoded columns.
    let key = unsafe { std::slice::from_raw_parts(address, length) };
    match get(bucket, key) {
        Ok(columns) => encode_optional_columns_to_direct_buffer(env, columns, address, capacity),
        Err(error) => {
            throw_illegal_state(env, error.to_string());
            0
        }
    }
}

pub(crate) fn take_direct_overflow(env: &mut JNIEnv) -> jobject {
    match take_owned_overflow_direct_buffer(env) {
        Ok(buffer) => buffer,
        Err(error) => {
            throw_illegal_state(env, error);
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_directBufferPoolConfigNative(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jintArray {
    let Some(table) = table_handle_from_handle_or_throw(&mut env, handle) else {
        return std::ptr::null_mut();
    };
    match table.direct_buffer_pool_config() {
        Ok((size, count)) => direct_buffer_pool_config_array(&mut env, size, count),
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_getEncodedDirectNative(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    buffer: JByteBuffer,
    key_length: jint,
) -> jint {
    let Some(table) = table_handle_from_handle_or_throw(&mut env, handle) else {
        return 0;
    };
    encode_direct_get(&mut env, bucket, buffer, key_length, |bucket, key| {
        table.access().get(bucket, key)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_multiGetEncodedDirectNative(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    buffer: JByteBuffer,
) -> jint {
    let Some(table) = table_handle_from_handle_or_throw(&mut env, handle) else {
        return 0;
    };
    encode_direct_multi_get(&mut env, buffer, |keys| table.access().multi_get(keys))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_takeDirectOverflowNative(
    mut env: JNIEnv,
    _class: JClass,
) -> jobject {
    take_direct_overflow(&mut env)
}
