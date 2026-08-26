use bytes::Bytes;
use cobble_binding::{MergeOperator, Schema, SchemaBuilder, SingleDb, merge_operator_by_id};
use serde_json::Value;
use std::sync::Arc;

use crate::{
    BridgeResult,
    database::NativeDatabase,
    error::{format_cobble_error, input_error},
    ffi,
    options::checked_usize,
};

pub(crate) struct NativeSchemaBuilder {
    // Field order is intentional: release the builder's DB access guard before
    // dropping the last possible database owner.
    builder: SchemaBuilder,
    _owner: Arc<SingleDb>,
}

fn optional_family(has_family: bool, family: &str) -> BridgeResult<Option<String>> {
    if has_family {
        if family.is_empty() {
            return Err(input_error("column family must not be empty"));
        }
        Ok(Some(family.to_owned()))
    } else {
        Ok(None)
    }
}

fn resolve_operator(
    operator_id: &str,
    has_metadata: bool,
    metadata_json: &str,
) -> BridgeResult<Arc<dyn MergeOperator>> {
    if operator_id.is_empty() {
        return Err(input_error("merge operator id must not be empty"));
    }
    let metadata: Option<Value> =
        if has_metadata {
            Some(serde_json::from_str(metadata_json).map_err(|error| {
                input_error(&format!("invalid operator metadata JSON: {error}"))
            })?)
        } else {
            None
        };
    merge_operator_by_id(operator_id, metadata.as_ref(), None).map_err(format_cobble_error)
}

fn native_schema(schema: &Schema) -> BridgeResult<ffi::NativeSchema> {
    let family_ids = schema.column_family_ids();
    let mut column_families = Vec::with_capacity(family_ids.len());
    for (name, column_count) in schema.column_families() {
        let id = family_ids
            .get(&name)
            .copied()
            .ok_or_else(|| input_error(&format!("schema is missing id for family '{name}'")))?;
        let operator_ids = schema
            .operator_ids_in_family(&name)
            .map_err(format_cobble_error)?;
        let mut merge_operators = Vec::with_capacity(operator_ids.len());
        for (column, operator_id) in operator_ids.into_iter().enumerate() {
            let metadata = schema
                .column_metadata_at(Some(&name), column)
                .map_err(format_cobble_error)?;
            merge_operators.push(ffi::NativeMergeOperator {
                id: operator_id,
                has_metadata: metadata.is_some(),
                metadata_json: metadata.map_or_else(String::new, Value::to_string),
            });
        }
        column_families.push(ffi::NativeSchemaFamily {
            name,
            id,
            column_count: u64::try_from(column_count)
                .map_err(|_| input_error("schema column count does not fit in u64"))?,
            value_has_ttl: schema.value_has_ttl_in_family(id),
            merge_operators,
        });
    }
    Ok(ffi::NativeSchema {
        version: schema.version(),
        column_families,
    })
}

pub(crate) fn native_database_current_schema(
    db: &NativeDatabase,
) -> BridgeResult<ffi::NativeSchema> {
    native_schema(db.db.current_schema().as_ref())
}

pub(crate) fn native_database_update_schema(db: &NativeDatabase) -> Box<NativeSchemaBuilder> {
    Box::new(NativeSchemaBuilder {
        builder: db.db.update_schema(),
        _owner: Arc::clone(&db.db),
    })
}

pub(crate) fn native_schema_builder_set_column_operator(
    builder: &mut NativeSchemaBuilder,
    has_family: bool,
    family: &str,
    column: u64,
    operator_id: &str,
    has_metadata: bool,
    metadata_json: &str,
) -> BridgeResult<()> {
    builder
        .builder
        .set_column_operator(
            optional_family(has_family, family)?,
            checked_usize(column, "schema column index")?,
            resolve_operator(operator_id, has_metadata, metadata_json)?,
        )
        .map_err(format_cobble_error)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn native_schema_builder_add_column(
    builder: &mut NativeSchemaBuilder,
    column: u64,
    has_operator: bool,
    operator_id: &str,
    has_metadata: bool,
    metadata_json: &str,
    has_default_value: bool,
    default_value: &[u8],
    has_family: bool,
    family: &str,
) -> BridgeResult<()> {
    if !has_operator && has_metadata {
        return Err(input_error(
            "operator metadata requires an explicit merge operator",
        ));
    }
    let operator = has_operator
        .then(|| resolve_operator(operator_id, has_metadata, metadata_json))
        .transpose()?;
    let default_value = has_default_value.then(|| Bytes::copy_from_slice(default_value));
    builder
        .builder
        .add_column(
            checked_usize(column, "schema column index")?,
            operator,
            default_value,
            optional_family(has_family, family)?,
        )
        .map_err(format_cobble_error)
}

pub(crate) fn native_schema_builder_delete_column(
    builder: &mut NativeSchemaBuilder,
    has_family: bool,
    family: &str,
    column: u64,
) -> BridgeResult<()> {
    builder
        .builder
        .delete_column(
            optional_family(has_family, family)?,
            checked_usize(column, "schema column index")?,
        )
        .map_err(format_cobble_error)
}

pub(crate) fn native_schema_builder_set_column_family_ttl(
    builder: &mut NativeSchemaBuilder,
    has_family: bool,
    family: &str,
    value_has_ttl: bool,
) -> BridgeResult<()> {
    builder
        .builder
        .set_column_family_value_has_ttl(optional_family(has_family, family)?, value_has_ttl)
        .map_err(format_cobble_error)
}

#[allow(clippy::boxed_local)]
pub(crate) fn native_schema_builder_commit(
    builder: Box<NativeSchemaBuilder>,
) -> BridgeResult<ffi::NativeSchema> {
    let schema = builder.builder.commit();
    native_schema(schema.as_ref())
}
