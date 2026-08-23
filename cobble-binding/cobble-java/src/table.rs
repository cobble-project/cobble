use crate::db::db_from_handle_or_throw;
use crate::util::{
    decode_java_bytes, decode_java_string, decode_u16, throw_illegal_argument, throw_illegal_state,
    to_java_string_or_throw,
};
use crate::write_options::write_options_from_handle_or_throw;
use cobble_binding::Db;
use cobble_table::{Table, TableError, TableSchema};
use jni::JNIEnv;
use jni::objects::{JByteArray, JByteBuffer, JClass, JString};
use jni::sys::{jint, jlong, jstring};

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_createNative(
    mut env: JNIEnv,
    _class: JClass,
    db_handle: jlong,
    name: JString,
    schema_json: JString,
) -> jstring {
    let Some(db) = db_from_handle_or_throw(&mut env, db_handle) else {
        return std::ptr::null_mut();
    };
    let name = match decode_java_string(&mut env, name) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    let schema_json = match decode_java_string(&mut env, schema_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    let schema = match serde_json::from_str::<TableSchema>(&schema_json) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, format!("invalid table schema JSON: {error}"));
            return std::ptr::null_mut();
        }
    };
    let table = match Table::create(db, name, schema) {
        Ok(value) => value,
        Err(error) => {
            throw_table_error(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    table_open_response(&mut env, db, table.schema())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_openNative(
    mut env: JNIEnv,
    _class: JClass,
    db_handle: jlong,
    name: JString,
) -> jstring {
    let Some(db) = db_from_handle_or_throw(&mut env, db_handle) else {
        return std::ptr::null_mut();
    };
    let name = match decode_java_string(&mut env, name) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    let table = match Table::open(db, name) {
        Ok(value) => value,
        Err(error) => {
            throw_table_error(&mut env, error);
            return std::ptr::null_mut();
        }
    };
    table_open_response(&mut env, db, table.schema())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_putEncodedNative(
    mut env: JNIEnv,
    _class: JClass,
    db_handle: jlong,
    bucket: jint,
    key: JByteArray,
    row_payload: JByteArray,
    write_options_handle: jlong,
) {
    let Some(db) = db_from_handle_or_throw(&mut env, db_handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let key = match decode_java_bytes(&mut env, key) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let payload = match decode_java_bytes(&mut env, row_payload) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    put_encoded(&mut env, db, bucket, &key, &payload, write_options_handle);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_Table_putEncodedDirectNative(
    mut env: JNIEnv,
    _class: JClass,
    db_handle: jlong,
    bucket: jint,
    key_buffer: JByteBuffer,
    key_offset: jint,
    key_length: jint,
    row_buffer: JByteBuffer,
    row_offset: jint,
    row_length: jint,
    write_options_handle: jlong,
) {
    let Some(db) = db_from_handle_or_throw(&mut env, db_handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let key = match direct_range(&mut env, &key_buffer, key_offset, key_length, "key") {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let payload = match direct_range(&mut env, &row_buffer, row_offset, row_length, "rowPayload") {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    put_encoded(&mut env, db, bucket, key, payload, write_options_handle);
}

fn table_open_response(env: &mut JNIEnv, db: &Db, schema: &TableSchema) -> jstring {
    let response = match serde_json::to_string(&serde_json::json!({
        "schema": schema,
        "total_buckets": db.total_buckets(),
    })) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(env, error.to_string());
            return std::ptr::null_mut();
        }
    };
    to_java_string_or_throw(env, response)
}

fn put_encoded(
    env: &mut JNIEnv,
    db: &Db,
    bucket: u16,
    key: &[u8],
    payload: &[u8],
    write_options_handle: jlong,
) {
    let Some(write_options) = write_options_from_handle_or_throw(env, write_options_handle) else {
        return;
    };
    let columns = match decode_row_payload(payload) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(env, error);
            return;
        }
    };
    if let Err(error) =
        db.put_columns_with_options(bucket, key, &columns, write_options.write_options())
    {
        throw_illegal_state(env, error.to_string());
    }
}

fn decode_row_payload(payload: &[u8]) -> Result<Vec<&[u8]>, String> {
    if payload.len() < 4 {
        return Err("table row payload is missing its column count".to_string());
    }
    let count = u32::from_be_bytes(payload[..4].try_into().unwrap()) as usize;
    if count > (payload.len() - 4) / 5 {
        return Err("table row column count exceeds its payload".to_string());
    }
    let mut columns = Vec::with_capacity(count);
    let mut offset = 4usize;
    for column in 0..count {
        if payload.len() - offset < 5 {
            return Err(format!("table row column {column} is truncated"));
        }
        if payload[offset] != 1 {
            return Err(format!(
                "table row column {column} must contain a complete encoded value"
            ));
        }
        offset += 1;
        let length_end = offset + 4;
        let length = u32::from_be_bytes(payload[offset..length_end].try_into().unwrap()) as usize;
        offset = length_end;
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= payload.len())
            .ok_or_else(|| format!("table row column {column} exceeds its payload"))?;
        columns.push(&payload[offset..end]);
        offset = end;
    }
    if offset != payload.len() {
        return Err("table row payload has trailing bytes".to_string());
    }
    Ok(columns)
}

fn direct_range<'a>(
    env: &mut JNIEnv,
    buffer: &'a JByteBuffer,
    offset: jint,
    length: jint,
    name: &str,
) -> Result<&'a [u8], String> {
    let offset = usize::try_from(offset).map_err(|_| format!("{name}Offset must be >= 0"))?;
    let length = usize::try_from(length).map_err(|_| format!("{name}Length must be >= 0"))?;
    let capacity = env
        .get_direct_buffer_capacity(buffer)
        .map_err(|_| format!("{name} must be a direct ByteBuffer"))?;
    let end = offset
        .checked_add(length)
        .filter(|end| *end <= capacity)
        .ok_or_else(|| format!("{name} range exceeds its buffer"))?;
    let address = env
        .get_direct_buffer_address(buffer)
        .map_err(|_| format!("{name} must be a direct ByteBuffer"))?;
    Ok(unsafe { std::slice::from_raw_parts(address.add(offset), end - offset) })
}

fn throw_table_error(env: &mut JNIEnv, error: TableError) {
    match error {
        TableError::InvalidSchema(message) | TableError::Codec(message) => {
            throw_illegal_argument(env, message)
        }
        other => throw_illegal_state(env, other.to_string()),
    }
}
