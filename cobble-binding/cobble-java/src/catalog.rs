use crate::db::db_arc_from_handle_or_throw;
use crate::table::table_open_response;
use crate::util::{
    decode_java_string, parse_config_json, throw_illegal_argument, throw_illegal_state,
    to_java_string_or_throw,
};
use cobble_table::SchemaChange;
use cobble_table::catalog::{
    Catalog, CatalogSchemaId, CatalogTable as RustCatalogTable, FileCatalog as RustFileCatalog,
    FileCatalogConfig,
};
use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jlong, jstring};
use std::sync::Arc;

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_FileCatalog_openNative(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
    storage_id: JString,
) -> jlong {
    let config_json = match decode_java_string(&mut env, config_json) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_zero(&mut env, error),
    };
    let Some(config) = parse_config_json(&mut env, &config_json) else {
        return 0;
    };
    let storage_id = match decode_java_string(&mut env, storage_id) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_zero(&mut env, error),
    };
    match RustFileCatalog::open(&config, FileCatalogConfig::new(storage_id)) {
        Ok(catalog) => Box::into_raw(Box::new(catalog)) as jlong,
        Err(error) => throw_state_and_zero(&mut env, error),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_FileCatalog_disposeInternal(
    mut env: JNIEnv,
    _object: JObject,
    native_handle: jlong,
) {
    if native_handle == 0 {
        throw_illegal_state(&mut env, "file catalog is already closed".to_string());
        return;
    }
    // SAFETY: Java serializes close with catalog operations.
    drop(unsafe { Box::from_raw(native_handle as *mut RustFileCatalog) });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_FileCatalog_createNamespaceNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    namespace_json: JString,
) {
    let Some(catalog) = file_catalog_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let Some(namespace) = parse_json(&mut env, namespace_json, "namespace") else {
        return;
    };
    if let Err(error) = catalog.create_namespace(namespace) {
        throw_illegal_state(&mut env, error.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_FileCatalog_listNamespacesNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    let Some(catalog) = file_catalog_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    json_result(&mut env, catalog.list_namespaces())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_FileCatalog_dropNamespaceNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    namespace_json: JString,
) {
    let Some(catalog) = file_catalog_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let Some(namespace): Option<Vec<String>> = parse_json(&mut env, namespace_json, "namespace")
    else {
        return;
    };
    if let Err(error) = catalog.drop_namespace(&namespace) {
        throw_illegal_state(&mut env, error.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_FileCatalog_createTableNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    identifier_json: JString,
    schema_json: JString,
) -> jlong {
    let Some(catalog) = file_catalog_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let Some(identifier) = parse_json(&mut env, identifier_json, "table identifier") else {
        return 0;
    };
    let Some(schema) = parse_json(&mut env, schema_json, "table schema") else {
        return 0;
    };
    catalog_table_handle(&mut env, catalog.create_table(identifier, schema))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_FileCatalog_loadTableNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    identifier_json: JString,
) -> jlong {
    let Some(catalog) = file_catalog_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let Some(identifier) = parse_json(&mut env, identifier_json, "table identifier") else {
        return 0;
    };
    catalog_table_handle(&mut env, catalog.load_table(&identifier))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_FileCatalog_loadTableSchemaNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    identifier_json: JString,
    catalog_schema_id: jlong,
) -> jstring {
    let Some(catalog) = file_catalog_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let Some(identifier) = parse_json(&mut env, identifier_json, "table identifier") else {
        return std::ptr::null_mut();
    };
    let catalog_schema_id = match u32::try_from(catalog_schema_id) {
        Ok(value) => CatalogSchemaId::from(value),
        Err(_) => {
            throw_illegal_argument(&mut env, "catalogSchemaId out of range".to_string());
            return std::ptr::null_mut();
        }
    };
    json_result(
        &mut env,
        catalog.load_table_schema(&identifier, catalog_schema_id),
    )
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_FileCatalog_evolveSchemaNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    identifier_json: JString,
    changes_json: JString,
) -> jlong {
    let Some(catalog) = file_catalog_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let Some(identifier) = parse_json(&mut env, identifier_json, "table identifier") else {
        return 0;
    };
    let Some(changes): Option<Vec<SchemaChange>> =
        parse_json(&mut env, changes_json, "schema changes")
    else {
        return 0;
    };
    catalog_table_handle(&mut env, catalog.evolve_schema(&identifier, changes))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_FileCatalog_listTablesNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    namespace_json: JString,
) -> jstring {
    let Some(catalog) = file_catalog_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let Some(namespace): Option<Vec<String>> = parse_json(&mut env, namespace_json, "namespace")
    else {
        return std::ptr::null_mut();
    };
    json_result(&mut env, catalog.list_tables(&namespace))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_FileCatalog_tableExistsNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    identifier_json: JString,
) -> jboolean {
    let Some(catalog) = file_catalog_from_handle_or_throw(&mut env, native_handle) else {
        return JNI_FALSE;
    };
    let Some(identifier) = parse_json(&mut env, identifier_json, "table identifier") else {
        return JNI_FALSE;
    };
    match catalog.table_exists(&identifier) {
        Ok(value) => {
            if value {
                JNI_TRUE
            } else {
                JNI_FALSE
            }
        }
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            JNI_FALSE
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_FileCatalog_renameTableNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    identifier_json: JString,
    new_name: JString,
) -> jlong {
    let Some(catalog) = file_catalog_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let Some(identifier) = parse_json(&mut env, identifier_json, "table identifier") else {
        return 0;
    };
    let new_name = match decode_java_string(&mut env, new_name) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_zero(&mut env, error),
    };
    catalog_table_handle(&mut env, catalog.rename_table(&identifier, new_name))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_FileCatalog_dropTableNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    identifier_json: JString,
) {
    let Some(catalog) = file_catalog_from_handle_or_throw(&mut env, native_handle) else {
        return;
    };
    let Some(identifier) = parse_json(&mut env, identifier_json, "table identifier") else {
        return;
    };
    if let Err(error) = catalog.drop_table(&identifier) {
        throw_illegal_state(&mut env, error.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_CatalogTable_descriptorNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) -> jstring {
    let Some(table) = catalog_table_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let value = serde_json::json!({
        "identifier": table.identifier(),
        "table_id": table.table_id().as_u32(),
        "catalog_schema_id": table.catalog_schema_id().as_u32(),
        "schema": table.schema(),
        "physical_name": table.physical_name(),
    });
    match serde_json::to_string(&value) {
        Ok(value) => to_java_string_or_throw(&mut env, value),
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_CatalogTable_materializeNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    db_handle: jlong,
) -> jstring {
    let Some(table) = catalog_table_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let Some(db) = db_arc_from_handle_or_throw(&mut env, db_handle) else {
        return std::ptr::null_mut();
    };
    materialize_response(&mut env, table, db)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_CatalogTable_refreshWriterNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    db_handle: jlong,
    table_name: JString,
) -> jstring {
    let Some(table) = catalog_table_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let Some(db) = db_arc_from_handle_or_throw(&mut env, db_handle) else {
        return std::ptr::null_mut();
    };
    let table_name = match decode_java_string(&mut env, table_name) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    if table_name != table.physical_name() {
        throw_illegal_state(
            &mut env,
            "Table does not belong to this catalog table".to_string(),
        );
        return std::ptr::null_mut();
    }
    materialize_response(&mut env, table, db)
}

fn materialize_response(
    env: &mut JNIEnv,
    table: &RustCatalogTable,
    db: &Arc<cobble_binding::Db>,
) -> jstring {
    let materialized = match table.materialize_table(Arc::clone(db)) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(env, error.to_string());
            return std::ptr::null_mut();
        }
    };
    table_open_response(
        env,
        db.total_buckets(),
        materialized.schema(),
        cobble_table::ffi::table_schema_binding(&materialized),
    )
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_CatalogTable_disposeInternal(
    mut env: JNIEnv,
    _object: JObject,
    native_handle: jlong,
) {
    dispose_catalog_table(&mut env, native_handle);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_CatalogTable_disposeNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
) {
    dispose_catalog_table(&mut env, native_handle);
}

fn dispose_catalog_table(env: &mut JNIEnv, native_handle: jlong) {
    if native_handle == 0 {
        throw_illegal_state(env, "catalog table is already closed".to_string());
        return;
    }
    // SAFETY: Java serializes close with descriptor operations.
    drop(unsafe { Box::from_raw(native_handle as *mut RustCatalogTable) });
}

fn file_catalog_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static RustFileCatalog> {
    if native_handle == 0 {
        throw_illegal_state(env, "file catalog is closed".to_string());
        return None;
    }
    // SAFETY: The handle is returned by `FileCatalog_openNative` and Java serializes close.
    Some(unsafe { &*(native_handle as *const RustFileCatalog) })
}

fn catalog_table_from_handle_or_throw(
    env: &mut JNIEnv,
    native_handle: jlong,
) -> Option<&'static RustCatalogTable> {
    if native_handle == 0 {
        throw_illegal_state(env, "catalog table is closed".to_string());
        return None;
    }
    // SAFETY: The handle is returned by a catalog operation and Java serializes close.
    Some(unsafe { &*(native_handle as *const RustCatalogTable) })
}

fn parse_json<T: serde::de::DeserializeOwned>(
    env: &mut JNIEnv,
    value: JString,
    name: &str,
) -> Option<T> {
    let value = match decode_java_string(env, value) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(env, error);
            return None;
        }
    };
    match serde_json::from_str(&value) {
        Ok(value) => Some(value),
        Err(error) => {
            throw_illegal_argument(env, format!("invalid {name} JSON: {error}"));
            None
        }
    }
}

fn catalog_table_handle(
    env: &mut JNIEnv,
    result: cobble_table::catalog::CatalogResult<RustCatalogTable>,
) -> jlong {
    match result {
        Ok(table) => Box::into_raw(Box::new(table)) as jlong,
        Err(error) => throw_state_and_zero(env, error),
    }
}

fn json_result<T: serde::Serialize>(
    env: &mut JNIEnv,
    result: cobble_table::catalog::CatalogResult<T>,
) -> jstring {
    match result.and_then(|value| {
        serde_json::to_string(&value).map_err(|error| {
            cobble_table::catalog::CatalogError::InvalidMetadata(error.to_string())
        })
    }) {
        Ok(value) => to_java_string_or_throw(env, value),
        Err(error) => {
            throw_illegal_state(env, error.to_string());
            std::ptr::null_mut()
        }
    }
}

fn throw_state_and_zero(env: &mut JNIEnv, error: impl std::fmt::Display) -> jlong {
    throw_illegal_state(env, error.to_string());
    0
}

fn throw_argument_and_zero(env: &mut JNIEnv, error: impl Into<String>) -> jlong {
    throw_illegal_argument(env, error.into());
    0
}

fn throw_argument_and_null(env: &mut JNIEnv, error: impl Into<String>) -> jstring {
    throw_illegal_argument(env, error.into());
    std::ptr::null_mut()
}
