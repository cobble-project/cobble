use crate::db::db_arc_from_handle_or_throw;
use crate::table::table_open_response;
use crate::util::{
    decode_bucket_ranges, decode_java_string, decode_optional_java_string, decode_u32,
    decode_u64_from_jlong, parse_config_json, throw_illegal_argument, throw_illegal_state,
    to_java_string_or_throw,
};
use cobble_table::catalog::{
    Catalog, CatalogSchemaId, CatalogTable as RustCatalogTable, FileCatalog as RustFileCatalog,
    FileCatalogConfig,
};
use cobble_table::{SchemaChange, TableWritePlan, TableWriterBuilder};
use jni::JNIEnv;
use jni::objects::{JClass, JIntArray, JObject, JString, JValue};
use jni::sys::{JNI_FALSE, JNI_TRUE, jboolean, jint, jlong, jobject, jstring};
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
) -> jobject {
    let Some(table) = catalog_table_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let Some(db) = db_arc_from_handle_or_throw(&mut env, db_handle) else {
        return std::ptr::null_mut();
    };
    match table.materialize_table(Arc::clone(db)) {
        Ok(materialized) => crate::table::table_to_java(&mut env, materialized),
        Err(error) => throw_state_and_null(&mut env, error),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_CatalogTable_refreshWriterNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    table_handle: jlong,
) -> jstring {
    let Some(table) = catalog_table_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let Some(writer) = crate::table::table_handle_from_handle_mut_or_throw(&mut env, table_handle)
    else {
        return std::ptr::null_mut();
    };
    match writer.refresh_from_catalog(table) {
        Ok(_) => table_open_response(
            &mut env,
            writer.total_buckets(),
            writer.schema(),
            writer.schema_binding(),
        ),
        Err(error) => throw_state_and_null(&mut env, error),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_CatalogTable_writerOpenNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    runtime_json: JString,
    db_id: JString,
    range_starts: JIntArray,
    range_ends: JIntArray,
    mode: jint,
    snapshot_id: jlong,
) -> jobject {
    let Some(table) = catalog_table_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let runtime_json = match decode_java_string(&mut env, runtime_json) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    let Some(runtime) = parse_config_json(&mut env, &runtime_json) else {
        return std::ptr::null_mut();
    };
    let builder = match table.writer_builder(runtime) {
        Ok(builder) => builder,
        Err(error) => return throw_state_and_null(&mut env, error),
    };
    writer_open(
        &mut env,
        builder,
        db_id,
        range_starts,
        range_ends,
        mode,
        snapshot_id,
    )
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_CatalogTable_buildWritePlanNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    total_buckets: jint,
) -> jstring {
    let Some(table) = catalog_table_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let builder = table.new_write_builder();
    let result = if total_buckets == -1 {
        builder.build()
    } else {
        match decode_u32("totalBuckets", total_buckets) {
            Ok(total_buckets) => builder.total_buckets(total_buckets).build(),
            Err(error) => return throw_argument_and_null(&mut env, error),
        }
    };
    match result {
        Ok(plan) => match serde_json::to_string(&plan) {
            Ok(json) => to_java_string_or_throw(&mut env, json),
            Err(error) => throw_state_and_null(&mut env, error),
        },
        Err(error) => throw_state_and_null(&mut env, error),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_TableWritePlan_writerOpenNative(
    mut env: JNIEnv,
    _class: JClass,
    plan_json: JString,
    runtime_json: JString,
    db_id: JString,
    range_starts: JIntArray,
    range_ends: JIntArray,
    mode: jint,
    snapshot_id: jlong,
) -> jobject {
    let plan: TableWritePlan = match parse_json(&mut env, plan_json, "table write plan") {
        Some(plan) => plan,
        None => return std::ptr::null_mut(),
    };
    let runtime_json = match decode_java_string(&mut env, runtime_json) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    let Some(runtime) = parse_config_json(&mut env, &runtime_json) else {
        return std::ptr::null_mut();
    };
    let builder = match plan.writer_builder(runtime) {
        Ok(builder) => builder,
        Err(error) => return throw_state_and_null(&mut env, error),
    };
    writer_open(
        &mut env,
        builder,
        db_id,
        range_starts,
        range_ends,
        mode,
        snapshot_id,
    )
}

fn writer_open(
    env: &mut JNIEnv,
    builder: TableWriterBuilder,
    db_id: JString,
    range_starts: JIntArray,
    range_ends: JIntArray,
    mode: jint,
    snapshot_id: jlong,
) -> jobject {
    let db_id = match decode_optional_java_string(env, db_id) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(env, error),
    };
    let ranges = match decode_bucket_ranges(env, range_starts, range_ends) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(env, error),
    };
    let builder = match db_id {
        Some(db_id) => builder.db_id(db_id).bucket_ranges(ranges),
        None => builder.bucket_ranges(ranges),
    };
    let opened = match mode {
        0 => builder.open(),
        1 => builder.resume(),
        2 => match decode_u64_from_jlong("snapshotId", snapshot_id) {
            Ok(snapshot_id) => builder.open_from_snapshot(snapshot_id),
            Err(error) => return throw_argument_and_null(env, error),
        },
        3 => match decode_u64_from_jlong("snapshotId", snapshot_id) {
            Ok(snapshot_id) => builder.resume_from_snapshot(snapshot_id),
            Err(error) => return throw_argument_and_null(env, error),
        },
        _ => return throw_argument_and_null(env, "invalid catalog writer open mode"),
    };
    match opened {
        Ok(table) => crate::table::table_to_java(env, table),
        Err(error) => throw_state_and_null(env, error),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_CatalogTable_readerOpenNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    runtime_json: JString,
    snapshot_id: jlong,
) -> jlong {
    let Some(table) = catalog_table_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let runtime_json = match decode_java_string(&mut env, runtime_json) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_zero(&mut env, error),
    };
    let Some(runtime) = parse_config_json(&mut env, &runtime_json) else {
        return 0;
    };
    let builder = match table.reader_builder(runtime) {
        Ok(builder) => builder,
        Err(error) => return throw_state_and_zero(&mut env, error),
    };
    let reader = if snapshot_id == -1 {
        builder.current_global_snapshot().open()
    } else {
        match decode_u64_from_jlong("snapshotId", snapshot_id) {
            Ok(snapshot_id) => builder.global_snapshot(snapshot_id).open(),
            Err(error) => return throw_argument_and_zero(&mut env, error),
        }
    };
    match reader {
        Ok(reader) => crate::table_reader::into_table_reader_handle(reader),
        Err(error) => throw_state_and_zero(&mut env, error),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_CatalogTable_readonlyTableOpenNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    runtime_json: JString,
    db_id: JString,
    snapshot_id: jlong,
) -> jobject {
    let Some(table) = catalog_table_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let runtime_json = match decode_java_string(&mut env, runtime_json) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    let Some(runtime) = parse_config_json(&mut env, &runtime_json) else {
        return std::ptr::null_mut();
    };
    let db_id = match decode_java_string(&mut env, db_id) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    let snapshot_id = match decode_u64_from_jlong("snapshotId", snapshot_id) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    let read_only_table = match table
        .readonly_table_builder(runtime)
        .and_then(|builder| Ok(builder.shard_snapshot(db_id, snapshot_id).open()?))
    {
        Ok(value) => value,
        Err(error) => return throw_state_and_null(&mut env, error),
    };
    let db = cobble_table::ffi::read_only_table_db(&read_only_table);
    read_only_db_to_java(&mut env, db)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_CatalogTable_snapshotCommitterNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    runtime_json: JString,
    max_pending_commits: jint,
) -> jlong {
    let Some(table) = catalog_table_from_handle_or_throw(&mut env, native_handle) else {
        return 0;
    };
    let runtime_json = match decode_java_string(&mut env, runtime_json) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_zero(&mut env, error),
    };
    let Some(runtime) = parse_config_json(&mut env, &runtime_json) else {
        return 0;
    };
    let max_pending_commits = match decode_u32("maxPendingCommits", max_pending_commits) {
        Ok(value) => value as usize,
        Err(error) => return throw_argument_and_zero(&mut env, error),
    };
    match table.snapshot_committer(runtime, max_pending_commits) {
        Ok(committer) => crate::table_snapshot::into_table_snapshot_committer_handle(committer),
        Err(error) => throw_state_and_zero(&mut env, error),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_table_CatalogTable_coordinatorNative(
    mut env: JNIEnv,
    _class: JClass,
    native_handle: jlong,
    runtime_json: JString,
) -> jobject {
    let Some(table) = catalog_table_from_handle_or_throw(&mut env, native_handle) else {
        return std::ptr::null_mut();
    };
    let runtime_json = match decode_java_string(&mut env, runtime_json) {
        Ok(value) => value,
        Err(error) => return throw_argument_and_null(&mut env, error),
    };
    let Some(runtime) = parse_config_json(&mut env, &runtime_json) else {
        return std::ptr::null_mut();
    };
    let coordinator = match table.coordinator(runtime) {
        Ok(value) => value,
        Err(error) => return throw_state_and_null(&mut env, error),
    };
    let handle = Box::into_raw(Box::new(coordinator));
    match env.new_object(
        "io/cobble/DbCoordinator",
        "(J)V",
        &[JValue::Long(handle as jlong)],
    ) {
        Ok(object) => object.into_raw(),
        Err(error) => {
            // Java did not acquire ownership when construction failed.
            unsafe {
                drop(Box::from_raw(handle));
            }
            throw_state_and_null(&mut env, error)
        }
    }
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

fn read_only_db_to_java(env: &mut JNIEnv, db: Arc<cobble_binding::ReadOnlyDb>) -> jobject {
    let handle = Box::into_raw(Box::new(db)) as jlong;
    match env.new_object(
        "io/cobble/ReadOnlyDb",
        "(J)V",
        &[jni::objects::JValue::Long(handle)],
    ) {
        Ok(db) => db.into_raw(),
        Err(error) => {
            // SAFETY: Java did not receive this handle.
            drop(unsafe { Box::from_raw(handle as *mut Arc<cobble_binding::ReadOnlyDb>) });
            throw_illegal_state(env, error.to_string());
            std::ptr::null_mut()
        }
    }
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

fn throw_state_and_null(env: &mut JNIEnv, error: impl std::fmt::Display) -> jobject {
    throw_illegal_state(env, error.to_string());
    std::ptr::null_mut()
}

fn throw_argument_and_zero(env: &mut JNIEnv, error: impl Into<String>) -> jlong {
    throw_illegal_argument(env, error.into());
    0
}

fn throw_argument_and_null(env: &mut JNIEnv, error: impl Into<String>) -> jstring {
    throw_illegal_argument(env, error.into());
    std::ptr::null_mut()
}
