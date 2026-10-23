use super::model::{CatalogSchemaId, CatalogSchemaVersion, TableId};
use super::runtime::physical_table_name;
use super::{CatalogError, CatalogResult};
use crate::evolution::compile_column_evolution;
use crate::metadata::TableMetadata;
use crate::{LogicalTypeKind, TableError, TableSchema};
use cobble::{BytesMergeOperator, ColumnFamilyOptions, Db, MergeOperator};
use std::sync::Arc;

pub(crate) fn materialize_table_definition(
    db: &Db,
    table_id: TableId,
    catalog_schema_id: CatalogSchemaId,
    schema: &TableSchema,
    mut load_version: impl FnMut(CatalogSchemaId) -> CatalogResult<CatalogSchemaVersion>,
    mut record_mapping: impl FnMut(CatalogSchemaId, u64) -> CatalogResult<()>,
) -> CatalogResult<(String, TableMetadata)> {
    let physical_name = physical_table_name(table_id);
    let target = TableMetadata::compile_catalog(schema.clone(), table_id, catalog_schema_id)?;
    let current = db.current_schema();
    let materialized = if let Some(column_family_id) =
        current.column_family_ids().get(&physical_name).copied()
    {
        let options = current.column_family_options_in_family(column_family_id);
        let existing = options.metadata.as_ref().ok_or_else(|| {
            TableError::InvalidSchema(format!(
                "column family '{physical_name}' is not a catalog table"
            ))
        })?;
        let existing = TableMetadata::from_value(existing)?;
        let binding = existing.catalog_binding.ok_or_else(|| {
            TableError::InvalidSchema(format!(
                "column family '{physical_name}' is not a catalog table"
            ))
        })?;
        if binding.table_id != table_id {
            return Err(TableError::InvalidSchema(format!(
                "column family '{physical_name}' belongs to another catalog table"
            ))
            .into());
        }
        if binding.catalog_schema_id > catalog_schema_id {
            return Err(TableError::InvalidSchema(format!(
                "catalog schema {} cannot replace newer materialized schema {}",
                catalog_schema_id, binding.catalog_schema_id
            ))
            .into());
        }
        let source_record = load_version(binding.catalog_schema_id)?;
        let mut materialized = TableMetadata::compile_catalog(
            source_record.schema().clone(),
            table_id,
            binding.catalog_schema_id,
        )?;
        if existing != materialized
            || current.num_columns_in_family(column_family_id)
                != Some(materialized.layout.value_columns.len().max(1))
        {
            return Err(TableError::InvalidSchema(
                "materialized table metadata does not match the catalog".to_string(),
            )
            .into());
        }
        let core_schema_id =
            crate::append::prepare_list_columns(db, &physical_name, &materialized.schema)?;
        record_mapping(binding.catalog_schema_id, core_schema_id)?;
        let mut materialized_catalog_schema_id = binding.catalog_schema_id;
        while materialized_catalog_schema_id < catalog_schema_id {
            let next_catalog_schema_id =
                materialized_catalog_schema_id.next().ok_or_else(|| {
                    CatalogError::InvalidSchemaEvolution(
                        "catalog schema id space exhausted".to_string(),
                    )
                })?;
            let next_record = load_version(next_catalog_schema_id)?;
            let next = TableMetadata::compile_catalog(
                next_record.schema().clone(),
                table_id,
                next_catalog_schema_id,
            )?;
            let remap =
                compile_column_evolution(&materialized, &next, next_record.field_transforms())?;
            let mut builder = db.update_schema();
            builder.remap_columns(Some(physical_name.clone()), remap)?;
            for column in &next.layout.value_columns {
                let field = next
                    .schema
                    .fields
                    .iter()
                    .find(|field| field.id == column.field_id)
                    .expect("compiled field");
                let operator =
                    if let Some(operator) = crate::append::list_operator(&field.logical_type) {
                        Some(operator)
                    } else if materialized.schema.fields.iter().any(|previous| {
                        previous.id == field.id
                            && matches!(previous.logical_type.kind, LogicalTypeKind::List { .. })
                    }) {
                        builder.clear_column_metadata(
                            Some(physical_name.clone()),
                            column.column_index as usize,
                        )?;
                        Some(Arc::new(BytesMergeOperator) as Arc<dyn MergeOperator>)
                    } else {
                        None
                    };
                if let Some(operator) = operator {
                    builder.set_column_operator(
                        Some(physical_name.clone()),
                        column.column_index as usize,
                        operator,
                    )?;
                }
            }
            builder.set_column_family_options(
                Some(physical_name.clone()),
                ColumnFamilyOptions {
                    metadata: Some(next.to_value()?),
                    ..ColumnFamilyOptions::default()
                },
            )?;
            let core_schema_id = builder.commit().version();
            record_mapping(next_catalog_schema_id, core_schema_id)?;
            materialized = next;
            materialized_catalog_schema_id = next_catalog_schema_id;
        }
        materialized
    } else {
        let mut builder = db.update_schema();
        builder.ensure_column_family_exists(physical_name.clone())?;
        for column in 0..target.layout.value_columns.len().max(1) {
            let operator = target
                .layout
                .value_columns
                .get(column)
                .and_then(|column| {
                    target
                        .schema
                        .fields
                        .iter()
                        .find(|field| field.id == column.field_id)
                })
                .and_then(|field| crate::append::list_operator(&field.logical_type));
            builder.add_column(column, operator, None, Some(physical_name.clone()))?;
        }
        builder.set_column_family_options(
            Some(physical_name.clone()),
            ColumnFamilyOptions {
                metadata: Some(target.to_value()?),
                ..ColumnFamilyOptions::default()
            },
        )?;
        let core_schema_id = builder.commit().version();
        record_mapping(catalog_schema_id, core_schema_id)?;
        return Ok((physical_name, target));
    };
    if materialized != target {
        return Err(TableError::InvalidSchema(
            "materialized table metadata does not match the requested catalog schema".to_string(),
        )
        .into());
    }
    Ok((physical_name, materialized))
}
