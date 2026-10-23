use crate::{LogicalType, LogicalTypeKind, Result, TableError, TableSchema};
use bytes::Bytes;
use cobble::{
    BytesMergeOperator, Db, MergeOperator, MergeOperatorResolver, TimeProvider, ValueType,
};
use cobble_data_structure::list::{concat_list_payloads, transform_list_payload};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const OPERATOR_ID: &str = "cobble.table.list/v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ListEncoding {
    nullable: bool,
}

impl ListEncoding {
    fn body(self, value: Bytes) -> cobble::Result<Bytes> {
        if value.is_empty() {
            return Ok(value);
        }
        if !self.nullable {
            return Ok(value);
        }
        match value.first() {
            Some(0) if value.len() == 1 => Ok(Bytes::new()),
            Some(1) if value.len() >= 5 => Ok(value.slice(1..)),
            _ => Err(cobble::Error::FileFormatError(
                "invalid nullable table List".into(),
            )),
        }
    }

    fn prefix(self) -> &'static [u8] {
        if self.nullable { &[1] } else { &[] }
    }
}

struct TableListOperator {
    encoding: ListEncoding,
}

impl MergeOperator for TableListOperator {
    fn id(&self) -> String {
        OPERATOR_ID.into()
    }
    fn metadata(&self) -> Option<serde_json::Value> {
        Some(serde_json::to_value(self.encoding).expect("serializable List encoding"))
    }
    fn merge(
        &self,
        existing: Bytes,
        operand: Bytes,
        clock: Option<&dyn TimeProvider>,
    ) -> cobble::Result<(Bytes, Option<ValueType>)> {
        self.merge_batch(existing, vec![operand], clock)
    }
    fn merge_batch(
        &self,
        existing: Bytes,
        operands: Vec<Bytes>,
        _: Option<&dyn TimeProvider>,
    ) -> cobble::Result<(Bytes, Option<ValueType>)> {
        if operands.is_empty() {
            return Ok((existing, None));
        }
        let existing = self.encoding.body(existing)?;
        let operands = operands
            .into_iter()
            .map(|value| {
                if value.is_empty() {
                    Err(cobble::Error::FileFormatError(
                        "empty List merge operand".into(),
                    ))
                } else {
                    self.encoding.body(value)
                }
            })
            .collect::<cobble::Result<Vec<_>>>()?;
        Ok((
            concat_list_payloads(&existing, &operands, self.encoding.prefix(), None)?
                .expect("uncapped concatenation"),
            None,
        ))
    }
}

pub(crate) fn list_operator(ty: &LogicalType) -> Option<Arc<dyn MergeOperator>> {
    matches!(ty.kind, LogicalTypeKind::List { .. }).then(|| {
        Arc::new(TableListOperator {
            encoding: ListEncoding {
                nullable: ty.nullable,
            },
        }) as Arc<dyn MergeOperator>
    })
}

/// Resolve common-format table List operators, composing with optional custom operators.
pub fn table_merge_operator_resolver(
    custom: Option<Arc<dyn MergeOperatorResolver>>,
) -> Arc<dyn MergeOperatorResolver> {
    Arc::new(move |id: &str, metadata: Option<&serde_json::Value>| {
        if id == OPERATOR_ID {
            return serde_json::from_value::<ListEncoding>(metadata?.clone())
                .ok()
                .map(|encoding| {
                    Arc::new(TableListOperator { encoding }) as Arc<dyn MergeOperator>
                });
        }
        custom
            .as_ref()
            .and_then(|resolver| resolver.resolve(id, metadata))
    })
}

/// Operator capabilities for standalone compaction workers.
pub fn table_merge_operator_ids() -> Vec<String> {
    vec![OPERATOR_ID.into()]
}

/// Adapt an element-byte transform to the ordinary ValueCodec List column format.
/// The nullable flags describe the source and target outer List, not its elements.
pub fn list_element_transform<F>(
    source_nullable: bool,
    target_nullable: bool,
    transform: F,
) -> impl Fn(Option<Bytes>) -> cobble::Result<Option<Bytes>> + Send + Sync + 'static
where
    F: Fn(Bytes) -> cobble::Result<Bytes> + Send + Sync + 'static,
{
    move |value| {
        value
            .map(|value| {
                if value.is_empty() {
                    return Err(cobble::Error::FileFormatError(
                        "empty stored List value".into(),
                    ));
                }
                if source_nullable && value.as_ref() == [0] {
                    return if target_nullable {
                        Ok(value)
                    } else {
                        Err(cobble::Error::InputError(
                            "cannot transform Null into a non-null List".into(),
                        ))
                    };
                }
                let source = ListEncoding {
                    nullable: source_nullable,
                }
                .body(value)?;
                transform_list_payload(
                    source,
                    &transform,
                    ListEncoding {
                        nullable: target_nullable,
                    }
                    .prefix(),
                )
            })
            .transpose()
    }
}

/// Install common-format List operators once when preparing a writable table. Legacy Bytes
/// columns need only an operator schema upgrade; persisted values and logical/catalog schemas
/// stay unchanged. Exact snapshot restore can therefore advance the core operator schema here.
pub(crate) fn prepare_list_columns(db: &Db, name: &str, schema: &TableSchema) -> Result<u64> {
    let current = db.current_schema();
    let ids = current.operator_ids_in_family(name)?;
    let mut upgrades = Vec::new();
    let mut column = 0;
    for field in &schema.fields {
        if schema.primary_key.contains(&field.id) {
            continue;
        }
        if let Some(operator) = list_operator(&field.logical_type) {
            let id = &ids[column];
            let expected = operator.metadata();
            if id == OPERATOR_ID {
                if current.column_metadata_at(Some(name), column)? != expected.as_ref() {
                    return Err(TableError::InvalidSchema(format!(
                        "List operator encoding disagrees with field '{}'",
                        field.name
                    )));
                }
            } else if id == &BytesMergeOperator.id() {
                upgrades.push((column, operator));
            } else {
                return Err(TableError::InvalidSchema(format!(
                    "List field '{}' has an unexpected merge operator '{}'",
                    field.name, id
                )));
            }
        }
        column += 1;
    }
    if upgrades.is_empty() {
        return Ok(current.version());
    }
    let mut update = db.update_schema();
    for (column, operator) in upgrades {
        update.set_column_operator(Some(name.into()), column, operator)?;
    }
    Ok(update.commit().version())
}
