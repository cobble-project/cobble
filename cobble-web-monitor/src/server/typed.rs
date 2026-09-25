use super::{AppState, Error, Result};
use axum::Json;
use axum::extract::{Query, State};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use cobble::load_shard_snapshot_metadata;
use cobble_table::{
    LogicalType, LogicalTypeKind, TableKey, TableReader, TableReaderBuilder, TableSchema, Value,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone, Serialize)]
struct TableSummary {
    name: String,
    schema: TableSchema,
}

pub(super) struct TableCache {
    snapshot_id: u64,
    total_buckets: u32,
    tables: Vec<TableSummary>,
    readers: HashMap<String, Arc<TableReader>>,
}

#[derive(Serialize)]
pub(super) struct TablesResponse {
    snapshot_id: u64,
    tables: Vec<TableSummary>,
}

pub(super) async fn tables_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<TablesResponse>> {
    let state = Arc::clone(&state);
    tokio::task::spawn_blocking(move || list_tables(&state))
        .await
        .map_err(|err| Error::HttpServerError(format!("table list task failed: {err}")))?
        .map(Json)
}

fn selected_snapshot(state: &AppState) -> Result<(u64, u32)> {
    let mut reader = state
        .proxy
        .lock()
        .map_err(|_| Error::HttpServerError("monitor proxy lock poisoned".into()))?;
    if reader.read_mode() == "current" {
        reader.refresh()?;
    }
    let snapshot = reader.current_global_snapshot();
    Ok((snapshot.id, snapshot.total_buckets))
}

fn load_tables(state: &AppState, snapshot_id: u64) -> Result<Vec<TableSummary>> {
    let reader = state
        .proxy
        .lock()
        .map_err(|_| Error::HttpServerError("monitor proxy lock poisoned".into()))?;
    let snapshot = reader.current_global_snapshot();
    if snapshot.id != snapshot_id {
        return Err(Error::InputError("snapshot changed; try again".into()));
    }
    let shard = snapshot
        .shard_snapshots
        .iter()
        .find(|shard| !shard.ranges.is_empty())
        .ok_or_else(|| Error::CobbleError("snapshot has no buckets".into()))?;
    let metadata =
        load_shard_snapshot_metadata(reader.config(), &shard.db_id, &shard.manifest_path)?;
    if metadata.snapshot_id != shard.snapshot_id {
        return Err(Error::CobbleError(
            "shard snapshot changed unexpectedly".into(),
        ));
    }
    let mut tables = Vec::new();
    for (name, family) in metadata.column_families {
        let Some(table_metadata) = family.options.metadata else {
            continue;
        };
        if table_metadata.get("format").and_then(JsonValue::as_str) != Some("cobble-table") {
            continue;
        }
        let schema = serde_json::from_value::<TableSchema>(
            table_metadata
                .get("schema")
                .cloned()
                .ok_or_else(|| Error::CobbleError(format!("table '{name}' has no schema")))?,
        )
        .map_err(|err| Error::CobbleError(format!("table '{name}' schema is invalid: {err}")))?;
        tables.push(TableSummary { name, schema });
    }
    Ok(tables)
}

fn list_tables(state: &AppState) -> Result<TablesResponse> {
    let (snapshot_id, total_buckets) = selected_snapshot(state)?;
    let mut cache = state
        .table_cache
        .lock()
        .map_err(|_| Error::HttpServerError("table cache lock poisoned".into()))?;
    if cache
        .as_ref()
        .is_none_or(|cached| cached.snapshot_id != snapshot_id)
    {
        *cache = Some(TableCache {
            snapshot_id,
            total_buckets,
            tables: load_tables(state, snapshot_id)?,
            readers: HashMap::new(),
        });
    }
    let tables = cache.as_ref().expect("initialized cache").tables.clone();
    Ok(TablesResponse {
        snapshot_id,
        tables,
    })
}

#[derive(Deserialize)]
pub(super) struct InspectParams {
    table: String,
    mode: String,
    key: Option<String>,
    bucket: Option<u16>,
    start_after: Option<String>,
    fields: Option<String>,
    limit: Option<usize>,
    snapshot_id: Option<u64>,
}

#[derive(Serialize)]
struct Row {
    values: Vec<JsonValue>,
}

#[derive(Serialize)]
pub(super) struct InspectResponse {
    snapshot_id: u64,
    table: String,
    fields: Vec<String>,
    lookup: Option<Row>,
    scan: Option<ScanResponse>,
}

#[derive(Serialize)]
struct ScanResponse {
    bucket: u16,
    limit: usize,
    has_more: bool,
    next_start_after: Option<ScanCursor>,
    items: Vec<Row>,
}

#[derive(Serialize, Deserialize)]
struct ScanCursor {
    snapshot_id: u64,
    table: String,
    bucket: u16,
    fields: Vec<String>,
    key: Vec<JsonValue>,
}

pub(super) async fn inspect_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<InspectParams>,
) -> Result<Json<InspectResponse>> {
    let state = Arc::clone(&state);
    tokio::task::spawn_blocking(move || run_inspect(&state, params))
        .await
        .map_err(|err| Error::HttpServerError(format!("table inspect task failed: {err}")))?
        .map(Json)
}

fn run_inspect(state: &AppState, params: InspectParams) -> Result<InspectResponse> {
    let (snapshot_id, total_buckets) = selected_snapshot(state)?;
    if let Some(expected) = params.snapshot_id
        && expected != snapshot_id
    {
        return Err(Error::InputError(
            "snapshot changed; restart the scan".into(),
        ));
    }
    let (schema, table_name, reader, total_buckets) = {
        let mut cache = state
            .table_cache
            .lock()
            .map_err(|_| Error::HttpServerError("table cache lock poisoned".into()))?;
        if cache
            .as_ref()
            .is_none_or(|cached| cached.snapshot_id != snapshot_id)
        {
            *cache = Some(TableCache {
                snapshot_id,
                total_buckets,
                tables: load_tables(state, snapshot_id)?,
                readers: HashMap::new(),
            });
        }
        let cache = cache.as_mut().expect("initialized cache");
        let table = cache
            .tables
            .iter()
            .find(|table| table.name == params.table)
            .ok_or_else(|| {
                Error::InputError(format!("table '{}' is not in this snapshot", params.table))
            })?;
        let schema = table.schema.clone();
        let table_name = table.name.clone();
        let reader = if let Some(reader) = cache.readers.get(&table_name) {
            Arc::clone(reader)
        } else {
            let reader = Arc::new(
                TableReaderBuilder::new(state.table_config.clone())
                    .table_name(&table_name)
                    .global_snapshot(snapshot_id)
                    .open()
                    .map_err(table_error)?,
            );
            cache
                .readers
                .insert(table_name.clone(), Arc::clone(&reader));
            reader
        };
        (schema, table_name, reader, cache.total_buckets)
    };
    let schema = &schema;
    let requested = parse_fields(params.fields.as_deref(), schema)?;
    let mut read_fields = requested.clone();
    if params.mode == "scan" {
        for id in &schema.primary_key {
            let field = schema
                .fields
                .iter()
                .find(|field| field.id == *id)
                .expect("valid table schema");
            if !read_fields.contains(&field.name) {
                read_fields.push(field.name.clone());
            }
        }
    }
    let projection = reader
        .project_by_names(&read_fields)
        .map_err(|err| Error::InputError(err.to_string()))?;
    let field_plan = requested
        .iter()
        .map(|name| {
            let index = read_fields
                .iter()
                .position(|field| field == name)
                .expect("projected field");
            let field = schema
                .fields
                .iter()
                .find(|field| field.name == *name)
                .expect("schema field");
            (index, &field.logical_type)
        })
        .collect::<Vec<_>>();
    let key_plan = schema
        .primary_key
        .iter()
        .map(|id| {
            let field = schema
                .fields
                .iter()
                .find(|field| field.id == *id)
                .expect("schema key");
            read_fields
                .iter()
                .position(|name| name == &field.name)
                .map(|index| (index, &field.logical_type))
        })
        .collect::<Option<Vec<_>>>();
    let mut response = InspectResponse {
        snapshot_id,
        table: table_name,
        fields: requested.clone(),
        lookup: None,
        scan: None,
    };
    match params.mode.as_str() {
        "lookup" => {
            let key = parse_key(params.key.as_deref(), schema, &reader)?;
            let values = projection.get(&key).map_err(table_error)?;
            response.lookup = values.map(|values| Row {
                values: render_row(&values, &field_plan),
            });
        }
        "scan" => {
            let bucket = params
                .bucket
                .ok_or_else(|| Error::InputError("bucket is required".into()))?;
            if u32::from(bucket) >= total_buckets {
                return Err(Error::InputError("bucket is outside this snapshot".into()));
            }
            let limit = params.limit.unwrap_or(state.inspect_default_limit);
            if limit == 0 || limit > state.inspect_max_limit {
                return Err(Error::InputError(format!(
                    "limit must be between 1 and {}",
                    state.inspect_max_limit
                )));
            }
            let cursor = params
                .start_after
                .as_deref()
                .map(|raw| {
                    serde_json::from_str::<ScanCursor>(raw)
                        .map_err(|_| Error::InputError("invalid scan cursor".into()))
                })
                .transpose()?;
            if let Some(cursor) = &cursor
                && (cursor.snapshot_id != snapshot_id
                    || cursor.table != response.table
                    || cursor.bucket != bucket
                    || cursor.fields != requested)
            {
                return Err(Error::InputError(
                    "scan settings changed; restart the scan".into(),
                ));
            }
            let start_after = cursor
                .as_ref()
                .map(|cursor| parse_key_parts(&cursor.key, schema, &reader))
                .transpose()?;
            let normalized_start_after = cursor
                .as_ref()
                .map(|cursor| normalize_key_parts(&cursor.key, schema))
                .transpose()?;
            if let Some(key) = &start_after
                && key.bucket() != bucket
            {
                return Err(Error::InputError("cursor belongs to another bucket".into()));
            }
            let mut scan = projection
                .scan_bounds(bucket, start_after.as_ref(), None)
                .map_err(table_error)?;
            let key_plan = key_plan.expect("scan projection includes key fields");
            let mut rows = Vec::with_capacity(limit + 1);
            let mut cursors = Vec::with_capacity(limit + 1);
            for item in &mut scan {
                let values = item.map_err(table_error)?;
                let cursor = key_values(&values, &key_plan);
                if let Some(start_after) = normalized_start_after.as_ref()
                    && cursors.is_empty()
                    && cursor == *start_after
                {
                    continue;
                }
                rows.push(Row {
                    values: render_row(&values, &field_plan),
                });
                cursors.push(cursor);
                if rows.len() > limit {
                    break;
                }
            }
            let has_more = rows.len() > limit;
            if has_more {
                rows.pop();
                cursors.pop();
            }
            response.scan = Some(ScanResponse {
                bucket,
                limit,
                has_more,
                next_start_after: if has_more {
                    cursors.pop().map(|key| ScanCursor {
                        snapshot_id,
                        table: response.table.clone(),
                        bucket,
                        fields: requested,
                        key,
                    })
                } else {
                    None
                },
                items: rows,
            });
        }
        _ => return Err(Error::InputError("mode must be lookup or scan".into())),
    }
    Ok(response)
}

fn table_error(error: cobble_table::TableError) -> Error {
    Error::CobbleError(error.to_string())
}

fn parse_fields(raw: Option<&str>, schema: &TableSchema) -> Result<Vec<String>> {
    let fields = if let Some(raw) = raw {
        let fields: Vec<String> = serde_json::from_str(raw)
            .map_err(|err| Error::InputError(format!("fields must be a JSON array: {err}")))?;
        if fields.is_empty() {
            return Err(Error::InputError("select at least one field".into()));
        }
        fields
    } else {
        schema
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect()
    };
    let mut seen = HashSet::new();
    for name in &fields {
        if !seen.insert(name) || !schema.fields.iter().any(|field| field.name == *name) {
            return Err(Error::InputError(format!(
                "duplicate or unknown field '{name}'"
            )));
        }
    }
    Ok(fields)
}

fn parse_json_array(raw: &str, label: &str) -> Result<Vec<JsonValue>> {
    serde_json::from_str(raw)
        .map_err(|err| Error::InputError(format!("{label} must be a JSON array: {err}")))
}

fn parse_key(raw: Option<&str>, schema: &TableSchema, reader: &TableReader) -> Result<TableKey> {
    let raw = raw.ok_or_else(|| Error::InputError("key is required".into()))?;
    let parts = parse_json_array(raw, "key")?;
    parse_key_parts(&parts, schema, reader)
}

fn parse_key_parts(
    parts: &[JsonValue],
    schema: &TableSchema,
    reader: &TableReader,
) -> Result<TableKey> {
    if parts.len() != schema.primary_key.len() {
        return Err(Error::InputError(format!(
            "key requires {} values",
            schema.primary_key.len()
        )));
    }
    let mut builder = reader.key_builder();
    for (id, part) in schema.primary_key.iter().zip(parts.iter()) {
        let field = schema
            .fields
            .iter()
            .find(|field| field.id == *id)
            .expect("valid table schema");
        builder.push(parse_value(part, &field.logical_type, &field.name)?);
    }
    builder
        .build()
        .map_err(|err| Error::InputError(err.to_string()))
}

fn normalize_key_parts(parts: &[JsonValue], schema: &TableSchema) -> Result<Vec<JsonValue>> {
    if parts.len() != schema.primary_key.len() {
        return Err(Error::InputError(format!(
            "key requires {} values",
            schema.primary_key.len()
        )));
    }
    schema
        .primary_key
        .iter()
        .zip(parts)
        .map(|(id, part)| {
            let field = schema
                .fields
                .iter()
                .find(|field| field.id == *id)
                .expect("valid table schema");
            let value = parse_value(part, &field.logical_type, &field.name)?;
            Ok(render_value(&value, &field.logical_type))
        })
        .collect()
}

fn text_value<'a>(value: &'a JsonValue, name: &str) -> Result<&'a str> {
    value
        .as_str()
        .ok_or_else(|| Error::InputError(format!("{name} must be text")))
}

fn parse_number<T: std::str::FromStr>(value: &JsonValue, name: &str) -> Result<T> {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_number().map(ToString::to_string))
        .ok_or_else(|| Error::InputError(format!("{name} must be a number")))?
        .parse()
        .map_err(|_| Error::InputError(format!("{name} is out of range or invalid")))
}

fn parse_value(value: &JsonValue, ty: &LogicalType, name: &str) -> Result<Value> {
    if value.is_null() && ty.nullable {
        return Ok(Value::Null);
    }
    match &ty.kind {
        LogicalTypeKind::Boolean => value
            .as_bool()
            .map(Value::Boolean)
            .ok_or_else(|| Error::InputError(format!("{name} must be true or false"))),
        LogicalTypeKind::Int8 => Ok(Value::Int8(parse_number(value, name)?)),
        LogicalTypeKind::Int16 => Ok(Value::Int16(parse_number(value, name)?)),
        LogicalTypeKind::Int32 => Ok(Value::Int32(parse_number(value, name)?)),
        LogicalTypeKind::Int64 => Ok(Value::Int64(parse_number(value, name)?)),
        LogicalTypeKind::Float32 => Ok(Value::Float32(parse_number(value, name)?)),
        LogicalTypeKind::Float64 => Ok(Value::Float64(parse_number(value, name)?)),
        LogicalTypeKind::Decimal { precision, scale } => {
            let text = text_value(value, name)?;
            let negative = text.starts_with('-');
            let unsigned = text.strip_prefix('-').unwrap_or(text);
            let mut halves = unsigned.split('.');
            let whole = halves.next().unwrap_or("");
            let fraction = halves.next().unwrap_or("");
            if whole.is_empty()
                || halves.next().is_some()
                || fraction.len() > usize::from(*scale)
                || !whole.bytes().all(|byte| byte.is_ascii_digit())
                || !fraction.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(Error::InputError(format!(
                    "{name} must be a decimal with scale {scale}"
                )));
            }
            let digits = format!(
                "{whole}{fraction}{}",
                "0".repeat(usize::from(*scale) - fraction.len())
            );
            if digits.trim_start_matches('0').len() > usize::from(*precision) {
                return Err(Error::InputError(format!(
                    "{name} exceeds decimal precision {precision}"
                )));
            }
            let magnitude: i128 = digits
                .parse()
                .map_err(|_| Error::InputError(format!("{name} is invalid")))?;
            Ok(Value::Decimal {
                precision: *precision,
                scale: *scale,
                unscaled: if negative { -magnitude } else { magnitude },
            })
        }
        LogicalTypeKind::Date => Ok(Value::Date(parse_number(value, name)?)),
        LogicalTypeKind::Time { .. } => Ok(Value::Time(parse_number(value, name)?)),
        LogicalTypeKind::Timestamp {
            precision,
            timestamp_kind,
        } => {
            let seconds: i64 = parse_number(
                value
                    .get("seconds")
                    .ok_or_else(|| Error::InputError(format!("{name}.seconds is required")))?,
                name,
            )?;
            let nanos: u32 = parse_number(
                value
                    .get("nanos")
                    .ok_or_else(|| Error::InputError(format!("{name}.nanos is required")))?,
                name,
            )?;
            Ok(Value::Timestamp {
                precision: *precision,
                timestamp_kind: *timestamp_kind,
                seconds,
                nanos,
            })
        }
        LogicalTypeKind::String => Ok(Value::String(text_value(value, name)?.to_owned())),
        LogicalTypeKind::Binary => {
            let encoded = value
                .as_str()
                .or_else(|| value.get("base64").and_then(JsonValue::as_str))
                .ok_or_else(|| Error::InputError(format!("{name} must be base64 text")))?;
            Ok(Value::from(STANDARD.decode(encoded).map_err(|_| {
                Error::InputError(format!("{name} is not valid base64"))
            })?))
        }
        _ => Err(Error::InputError(format!(
            "{name} is not supported as an input field"
        ))),
    }
}

fn render_row(values: &[Value], plan: &[(usize, &LogicalType)]) -> Vec<JsonValue> {
    plan.iter()
        .map(|(index, ty)| render_value(&values[*index], ty))
        .collect()
}

fn key_values(values: &[Value], plan: &[(usize, &LogicalType)]) -> Vec<JsonValue> {
    plan.iter()
        .map(|(index, ty)| render_value(&values[*index], ty))
        .collect()
}

fn render_value(value: &Value, ty: &LogicalType) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Boolean(value) => json!(value),
        Value::Int8(value) => json!(value),
        Value::Int16(value) => json!(value),
        Value::Int32(value) => json!(value),
        Value::Int64(value) => json!(value.to_string()),
        Value::Float32(value) => render_float(f64::from(*value)),
        Value::Float64(value) => render_float(*value),
        Value::Decimal {
            scale, unscaled, ..
        } => {
            let negative = *unscaled < 0;
            let mut digits = unscaled.unsigned_abs().to_string();
            if *scale > 0 {
                let scale = usize::from(*scale);
                if digits.len() <= scale {
                    digits = format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits);
                }
                digits.insert(digits.len() - scale, '.');
            }
            json!(format!("{}{digits}", if negative { "-" } else { "" }))
        }
        Value::Date(value) => json!(value),
        Value::Time(value) => json!(value.to_string()),
        Value::Timestamp { seconds, nanos, .. } => {
            json!({"seconds": seconds.to_string(), "nanos": nanos})
        }
        Value::String(value) => json!(value),
        Value::Binary(value) => json!({"base64": STANDARD.encode(value)}),
        Value::List(values) => {
            let LogicalTypeKind::List { element_type } = &ty.kind else {
                unreachable!()
            };
            json!(
                values
                    .iter()
                    .map(|value| render_value(value, element_type))
                    .collect::<Vec<_>>()
            )
        }
        Value::Map(entries) => {
            let LogicalTypeKind::Map {
                key_type,
                value_type,
            } = &ty.kind
            else {
                unreachable!()
            };
            json!(
                entries
                    .iter()
                    .map(|(key, value)| [
                        render_value(key, key_type),
                        render_value(value, value_type)
                    ])
                    .collect::<Vec<_>>()
            )
        }
        Value::Struct(values) => {
            let LogicalTypeKind::Struct { fields } = &ty.kind else {
                unreachable!()
            };
            JsonValue::Object(
                fields
                    .iter()
                    .zip(values)
                    .map(|(field, value)| {
                        (field.name.clone(), render_value(value, &field.logical_type))
                    })
                    .collect(),
            )
        }
        Value::Extension { type_id, value } => {
            let LogicalTypeKind::Extension { extension } = &ty.kind else {
                unreachable!()
            };
            json!({"type_id": type_id, "value": render_value(value, &extension.physical_type)})
        }
    }
}

fn render_float(value: f64) -> JsonValue {
    if value.is_nan() {
        json!("NaN")
    } else if value == f64::INFINITY {
        json!("Infinity")
    } else if value == f64::NEG_INFINITY {
        json!("-Infinity")
    } else {
        json!(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_json_keeps_precision_and_special_values() {
        assert_eq!(
            render_value(&Value::Int64(9_007_199_254_740_993), &LogicalType::int64()),
            json!("9007199254740993")
        );
        assert_eq!(
            render_value(
                &Value::Decimal {
                    precision: 18,
                    scale: 2,
                    unscaled: -125050
                },
                &LogicalType::decimal(18, 2)
            ),
            json!("-1250.50")
        );
        assert_eq!(
            render_value(&Value::Float64(f64::NAN), &LogicalType::float64()),
            json!("NaN")
        );
        assert_eq!(
            render_value(&Value::Float64(f64::INFINITY), &LogicalType::float64()),
            json!("Infinity")
        );
        assert_eq!(
            render_value(&Value::Float64(f64::NEG_INFINITY), &LogicalType::float64()),
            json!("-Infinity")
        );
        assert_eq!(
            render_value(&Value::from(vec![0, 255]), &LogicalType::binary()),
            json!({"base64":"AP8="})
        );
        assert_eq!(
            render_value(
                &Value::List(vec![Value::Int64(9_007_199_254_740_993)]),
                &LogicalType::list(LogicalType::int64())
            ),
            json!(["9007199254740993"])
        );
        assert!(parse_value(&json!("--1.00"), &LogicalType::decimal(18, 2), "price").is_err());
    }
}
