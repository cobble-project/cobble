use cobble_binding::ColumnFamilyOptions;
use cobble_binding::structured::{
    StructuredDb, StructuredSchema, StructuredSchemaBuilder, StructuredSingleDb,
};

use crate::structured_bridge::ffi;

use super::conversion::{format_error, input_error, list_config, native_schema};
use super::{
    BridgeResult, NativeStructuredDb, NativeStructuredSchemaEdit, NativeStructuredSingleDb,
    SchemaOperation,
};
use std::sync::Arc;

pub(crate) fn native_structured_db_current_schema(
    db: &NativeStructuredDb,
) -> ffi::NativeStructuredSchema {
    native_schema(db.db.current_schema())
}
pub(crate) fn native_structured_single_db_current_schema(
    db: &NativeStructuredSingleDb,
) -> ffi::NativeStructuredSchema {
    native_schema(db.db.current_schema())
}

enum SchemaBuilderDispatch<'a> {
    Db(StructuredSchemaBuilder<'a, StructuredDb>),
    SingleDb(StructuredSchemaBuilder<'a, StructuredSingleDb>),
}

impl SchemaBuilderDispatch<'_> {
    fn apply(&mut self, operation: SchemaOperation) {
        match operation {
            SchemaOperation::AddBytes(family, column) => match self {
                Self::Db(builder) => {
                    builder.add_bytes_column(family, column);
                }
                Self::SingleDb(builder) => {
                    builder.add_bytes_column(family, column);
                }
            },
            SchemaOperation::AddList(family, column, config) => match self {
                Self::Db(builder) => {
                    builder.add_list_column(family, column, config);
                }
                Self::SingleDb(builder) => {
                    builder.add_list_column(family, column, config);
                }
            },
            SchemaOperation::Delete(family, column) => match self {
                Self::Db(builder) => {
                    builder.delete_column(family, column);
                }
                Self::SingleDb(builder) => {
                    builder.delete_column(family, column);
                }
            },
            SchemaOperation::SetFamilyTtl(family, value_has_ttl) => {
                let options = ColumnFamilyOptions {
                    value_has_ttl,
                    metadata: None,
                };
                match self {
                    Self::Db(builder) => {
                        builder.set_column_family_options(family, options);
                    }
                    Self::SingleDb(builder) => {
                        builder.set_column_family_options(family, options);
                    }
                }
            }
        }
    }

    fn commit(self) -> cobble_binding::Result<StructuredSchema> {
        match self {
            Self::Db(mut builder) => builder.commit(),
            Self::SingleDb(mut builder) => builder.commit(),
        }
    }
}

fn apply_schema_operations(
    mut builder: SchemaBuilderDispatch<'_>,
    operations: Vec<SchemaOperation>,
) -> BridgeResult<StructuredSchema> {
    for operation in operations {
        builder.apply(operation);
    }
    builder.commit().map_err(format_error)
}

pub(crate) fn native_structured_db_commit_schema(
    db: &mut NativeStructuredDb,
    edit: &mut NativeStructuredSchemaEdit,
) -> BridgeResult<ffi::NativeStructuredSchema> {
    let owner = Arc::get_mut(&mut db.db).ok_or_else(|| {
        "CB_INVALID_STATE: schema commit requires releasing every structured scan cursor first"
            .to_owned()
    })?;
    apply_schema_operations(
        SchemaBuilderDispatch::Db(owner.update_schema()),
        edit.operations.clone(),
    )
    .map(native_schema)
}
pub(crate) fn native_structured_single_db_commit_schema(
    db: &mut NativeStructuredSingleDb,
    edit: &mut NativeStructuredSchemaEdit,
) -> BridgeResult<ffi::NativeStructuredSchema> {
    let owner = Arc::get_mut(&mut db.db).ok_or_else(|| {
        "CB_INVALID_STATE: schema commit requires releasing every structured scan cursor first"
            .to_owned()
    })?;
    apply_schema_operations(
        SchemaBuilderDispatch::SingleDb(owner.update_schema()),
        edit.operations.clone(),
    )
    .map(native_schema)
}

pub(crate) fn native_structured_schema_edit_new() -> Box<NativeStructuredSchemaEdit> {
    Box::new(NativeStructuredSchemaEdit {
        operations: Vec::new(),
    })
}
fn family(has_family: bool, family: &str) -> BridgeResult<Option<String>> {
    if has_family && family.is_empty() {
        Err(input_error("column family must not be empty"))
    } else {
        Ok(has_family.then(|| family.to_string()))
    }
}
pub(crate) fn native_structured_schema_edit_add_bytes(
    edit: &mut NativeStructuredSchemaEdit,
    has_family: bool,
    value: &str,
    column: u16,
) -> BridgeResult<()> {
    edit.operations.push(SchemaOperation::AddBytes(
        family(has_family, value)?,
        column,
    ));
    Ok(())
}
pub(crate) fn native_structured_schema_edit_add_list(
    edit: &mut NativeStructuredSchemaEdit,
    has_family: bool,
    value: &str,
    column: u16,
    config: &ffi::NativeListConfig,
) -> BridgeResult<()> {
    edit.operations.push(SchemaOperation::AddList(
        family(has_family, value)?,
        column,
        list_config(config)?,
    ));
    Ok(())
}
pub(crate) fn native_structured_schema_edit_delete(
    edit: &mut NativeStructuredSchemaEdit,
    has_family: bool,
    value: &str,
    column: u16,
) -> BridgeResult<()> {
    edit.operations
        .push(SchemaOperation::Delete(family(has_family, value)?, column));
    Ok(())
}
pub(crate) fn native_structured_schema_edit_set_family_ttl(
    edit: &mut NativeStructuredSchemaEdit,
    has_family: bool,
    value: &str,
    value_has_ttl: bool,
) -> BridgeResult<()> {
    edit.operations.push(SchemaOperation::SetFamilyTtl(
        family(has_family, value)?,
        value_has_ttl,
    ));
    Ok(())
}
