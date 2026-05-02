use crate::util::{
    decode_java_bytes, decode_java_string, decode_optional_java_string, throw_illegal_argument,
    throw_illegal_state, to_java_string_or_throw,
};
use bytes::Bytes;
use cobble::{ColumnFamilyOptions, Db, Schema, SchemaBuilder, SingleDb, merge_operator_by_id};
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jint, jlong, jstring};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;

// ── Schema JSON ─────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct SchemaColumnFamilyJson {
    id: u8,
    name: String,
    num_columns: usize,
    operator_ids: Vec<String>,
    options: ColumnFamilyOptions,
    value_has_ttl: bool,
}

#[derive(Serialize)]
struct SchemaJson {
    version: u64,
    column_family_ids: BTreeMap<String, u8>,
    column_families: Vec<SchemaColumnFamilyJson>,
}

fn schema_to_json_string(schema: &Schema) -> Result<String, String> {
    let column_family_ids = schema.column_family_ids();
    let mut column_families = Vec::with_capacity(column_family_ids.len());
    for (name, num_columns) in schema.column_families() {
        let id = *column_family_ids
            .get(&name)
            .ok_or_else(|| format!("missing column family id for '{}'", name))?;
        let operator_ids = schema
            .operator_ids_in_family(name.as_str())
            .map_err(|err| err.to_string())?;
        column_families.push(SchemaColumnFamilyJson {
            id,
            name,
            num_columns,
            operator_ids,
            options: schema.column_family_options_in_family(id),
            value_has_ttl: schema.value_has_ttl_in_family(id),
        });
    }
    let payload = SchemaJson {
        version: schema.version(),
        column_family_ids,
        column_families,
    };
    serde_json::to_string(&payload).map_err(|e| e.to_string())
}

// ── SchemaBuilder handle ────────────────────────────────────────────────────

fn builder_from_handle(native_handle: jlong) -> Option<&'static mut SchemaBuilder> {
    if native_handle == 0 {
        return None;
    }
    Some(unsafe { &mut *(native_handle as *mut SchemaBuilder) })
}

fn resolve_operator_or_default(
    operator_id: Option<&str>,
    metadata_json: Option<&str>,
) -> Result<Option<Arc<dyn cobble::MergeOperator>>, String> {
    let Some(id) = operator_id else {
        return Ok(None);
    };
    let metadata: Option<serde_json::Value> = match metadata_json {
        Some(json) => {
            Some(serde_json::from_str(json).map_err(|e| format!("invalid metadata json: {}", e))?)
        }
        None => None,
    };
    merge_operator_by_id(id, metadata.as_ref(), None)
        .map(Some)
        .map_err(|e| e.to_string())
}

// ── Db JNI ──────────────────────────────────────────────────────────────────

fn db_from_handle(native_handle: jlong) -> Option<&'static Db> {
    if native_handle == 0 {
        return None;
    }
    Some(unsafe { &*(native_handle as *const Db) })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_currentSchemaJson(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    let Some(db) = db_from_handle(native_handle) else {
        throw_illegal_state(&mut env, "db handle is disposed".to_string());
        return std::ptr::null_mut();
    };
    let schema = db.current_schema();
    match schema_to_json_string(&schema) {
        Ok(json) => to_java_string_or_throw(&mut env, json),
        Err(err) => {
            throw_illegal_state(&mut env, err);
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_Db_createSchemaBuilder(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jlong {
    let Some(db) = db_from_handle(native_handle) else {
        throw_illegal_state(&mut env, "db handle is disposed".to_string());
        return 0;
    };
    let builder = db.update_schema();
    Box::into_raw(Box::new(builder)) as jlong
}

// ── SingleDb JNI ────────────────────────────────────────────────────────────

fn single_db_from_handle(native_handle: jlong) -> Option<&'static SingleDb> {
    if native_handle == 0 {
        return None;
    }
    Some(unsafe { &*(native_handle as *const SingleDb) })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SingleDb_currentSchemaJson(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    let Some(db) = single_db_from_handle(native_handle) else {
        throw_illegal_state(&mut env, "single db handle is disposed".to_string());
        return std::ptr::null_mut();
    };
    let schema = db.current_schema();
    match schema_to_json_string(&schema) {
        Ok(json) => to_java_string_or_throw(&mut env, json),
        Err(err) => {
            throw_illegal_state(&mut env, err);
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SingleDb_createSchemaBuilder(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jlong {
    let Some(db) = single_db_from_handle(native_handle) else {
        throw_illegal_state(&mut env, "single db handle is disposed".to_string());
        return 0;
    };
    let builder = db.update_schema();
    Box::into_raw(Box::new(builder)) as jlong
}

// ── SchemaBuilder JNI ───────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SchemaBuilder_disposeInternal(
    mut env: JNIEnv,
    _obj: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(
            &mut env,
            "schema builder handle is already disposed".to_string(),
        );
        return;
    }
    let _boxed = unsafe { Box::from_raw(native_handle as *mut SchemaBuilder) };
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SchemaBuilder_nativeSetColumnOperator(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column_family: JString,
    column_idx: jint,
    operator_id: JString,
    metadata_json: JString,
) {
    let Some(builder) = builder_from_handle(native_handle) else {
        throw_illegal_state(&mut env, "schema builder handle is disposed".to_string());
        return;
    };
    let col = column_idx as usize;
    let op_id = match decode_java_string(&mut env, operator_id) {
        Ok(v) => v,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let column_family = match decode_optional_java_string(&mut env, column_family) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let meta = if metadata_json.is_null() {
        None
    } else {
        match decode_java_string(&mut env, metadata_json) {
            Ok(v) => Some(v),
            Err(err) => {
                throw_illegal_argument(&mut env, err);
                return;
            }
        }
    };
    let operator = match resolve_operator_or_default(Some(&op_id), meta.as_deref()) {
        Ok(Some(op)) => op,
        Ok(None) => {
            throw_illegal_argument(&mut env, "operator_id is required".to_string());
            return;
        }
        Err(err) => {
            throw_illegal_state(&mut env, err);
            return;
        }
    };
    if let Err(err) = builder.set_column_operator(column_family, col, operator) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SchemaBuilder_nativeAddColumn(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column_family: JString,
    column_idx: jint,
    operator_id: JString,
    metadata_json: JString,
    default_value: JByteArray,
) {
    let Some(builder) = builder_from_handle(native_handle) else {
        throw_illegal_state(&mut env, "schema builder handle is disposed".to_string());
        return;
    };
    let col = column_idx as usize;
    let column_family = match decode_optional_java_string(&mut env, column_family) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let op_id = if operator_id.is_null() {
        None
    } else {
        match decode_java_string(&mut env, operator_id) {
            Ok(v) => Some(v),
            Err(err) => {
                throw_illegal_argument(&mut env, err);
                return;
            }
        }
    };
    let meta = if metadata_json.is_null() {
        None
    } else {
        match decode_java_string(&mut env, metadata_json) {
            Ok(v) => Some(v),
            Err(err) => {
                throw_illegal_argument(&mut env, err);
                return;
            }
        }
    };
    let operator = match resolve_operator_or_default(op_id.as_deref(), meta.as_deref()) {
        Ok(op) => op,
        Err(err) => {
            throw_illegal_state(&mut env, err);
            return;
        }
    };
    let default_bytes = if default_value.is_null() {
        None
    } else {
        match decode_java_bytes(&mut env, default_value) {
            Ok(v) => Some(Bytes::from(v)),
            Err(err) => {
                throw_illegal_argument(&mut env, err);
                return;
            }
        }
    };
    if let Err(err) = builder.add_column(col, operator, default_bytes, column_family) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SchemaBuilder_nativeDeleteColumn(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column_family: JString,
    column_idx: jint,
) {
    let Some(builder) = builder_from_handle(native_handle) else {
        throw_illegal_state(&mut env, "schema builder handle is disposed".to_string());
        return;
    };
    let column_family = match decode_optional_java_string(&mut env, column_family) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    if let Err(err) = builder.delete_column(column_family, column_idx as usize) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SchemaBuilder_nativeSetColumnFamilyOptions(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    column_family: JString,
    options_json: JString,
) {
    let Some(builder) = builder_from_handle(native_handle) else {
        throw_illegal_state(&mut env, "schema builder handle is disposed".to_string());
        return;
    };
    let column_family = match decode_optional_java_string(&mut env, column_family) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let options_json = match decode_java_string(&mut env, options_json) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(&mut env, err);
            return;
        }
    };
    let options = match serde_json::from_str::<ColumnFamilyOptions>(&options_json) {
        Ok(value) => value,
        Err(err) => {
            throw_illegal_argument(
                &mut env,
                format!("invalid column family options json: {}", err),
            );
            return;
        }
    };
    if let Err(err) = builder.set_column_family_options(column_family, options) {
        throw_illegal_state(&mut env, err.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_SchemaBuilder_nativeCommit(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    if native_handle == 0 {
        throw_illegal_state(&mut env, "schema builder handle is disposed".to_string());
        return std::ptr::null_mut();
    }
    let builder = unsafe { Box::from_raw(native_handle as *mut SchemaBuilder) };
    let new_schema = builder.commit();
    match schema_to_json_string(&new_schema) {
        Ok(json) => to_java_string_or_throw(&mut env, json),
        Err(err) => {
            throw_illegal_state(&mut env, err);
            std::ptr::null_mut()
        }
    }
}
