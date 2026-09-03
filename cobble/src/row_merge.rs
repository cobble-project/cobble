use crate::TimeProvider;
use crate::error::{Error, Result};
use crate::merge_operator::MergeOperator;
use crate::schema::{Schema, SchemaManager};
use crate::r#type::{Column, Value, ValueType, decode_merge_separated_array};
use crate::vlog::VlogPointer;
use bytes::Bytes;
use std::sync::Arc;

/// A physical value together with the schema that encoded it.
///
/// Point reads retain this association until values from each schema epoch have
/// been merged with that epoch's operators.
pub(crate) struct SchemaValue {
    pub(crate) schema_id: u64,
    pub(crate) value: Value,
}

/// How to evolve an already-merged value between schema versions.
///
/// Builtin-compatible transitions preserve encoded columns, including VLOG
/// pointers. Defaults, nulls, and custom transforms instead need logical bytes,
/// so their route is compiled once and applied after VLOG materialization.
enum EvolutionPlan {
    Builtin,
    Materialized(crate::schema::SchemaProjectionRoute),
}

enum SchemaMergeRef<'a, T> {
    Borrowed(&'a T),
    Shared(Arc<T>),
}

impl<T> SchemaMergeRef<'_, T> {
    fn get(&self) -> &T {
        match self {
            Self::Borrowed(value) => value,
            Self::Shared(value) => value.as_ref(),
        }
    }
}

/// Reusable schema-aware merge state.
///
/// Reads and compaction reuse one plan across keys, so each schema pair resolves
/// transforms and compatibility segments at most once.
pub(crate) struct SchemaMergePlan<'a> {
    target_schema: SchemaMergeRef<'a, Schema>,
    schema_manager: SchemaMergeRef<'a, SchemaManager>,
    column_family_id: u8,
    evolutions: Vec<((u64, u64), EvolutionPlan)>,
    segment_targets: Vec<(u64, u64)>,
}

impl<'a> SchemaMergePlan<'a> {
    pub(crate) fn new(
        target_schema: &'a Schema,
        schema_manager: &'a SchemaManager,
        column_family_id: u8,
    ) -> Self {
        Self {
            target_schema: SchemaMergeRef::Borrowed(target_schema),
            schema_manager: SchemaMergeRef::Borrowed(schema_manager),
            column_family_id,
            evolutions: Vec::new(),
            segment_targets: Vec::new(),
        }
    }

    pub(crate) fn new_shared(
        target_schema: Arc<Schema>,
        schema_manager: Arc<SchemaManager>,
        column_family_id: u8,
    ) -> SchemaMergePlan<'static> {
        SchemaMergePlan {
            target_schema: SchemaMergeRef::Shared(target_schema),
            schema_manager: SchemaMergeRef::Shared(schema_manager),
            column_family_id,
            evolutions: Vec::new(),
            segment_targets: Vec::new(),
        }
    }

    fn target_schema(&self) -> &Schema {
        self.target_schema.get()
    }

    pub(crate) fn schema_manager(&self) -> &SchemaManager {
        self.schema_manager.get()
    }

    pub(crate) fn column_family_id(&self) -> u8 {
        self.column_family_id
    }

    fn evolution(&mut self, from_schema_id: u64, to_schema_id: u64) -> Result<&EvolutionPlan> {
        let key = (from_schema_id, to_schema_id);
        if let Some(index) = self.evolutions.iter().position(|(pair, _)| *pair == key) {
            return Ok(&self.evolutions[index].1);
        }
        let evolution = if self.schema_manager().is_builtin_compatible_transition(
            from_schema_id,
            to_schema_id,
            self.column_family_id,
        )? {
            EvolutionPlan::Builtin
        } else {
            EvolutionPlan::Materialized(self.schema_manager().compile_projection_route(
                from_schema_id,
                to_schema_id,
                self.column_family_id,
            )?)
        };
        self.evolutions.push((key, evolution));
        Ok(&self.evolutions.last().expect("schema evolution inserted").1)
    }

    /// The last schema reachable from `schema_id` without crossing an
    /// incompatible boundary on the way to the fixed task target.
    fn segment_target(&mut self, schema_id: u64) -> Result<u64> {
        if schema_id > self.target_schema().version() {
            return Err(Error::InvalidState(format!(
                "cannot merge schema {} value into target schema {}",
                schema_id,
                self.target_schema().version()
            )));
        }
        if let Some((_, target)) = self
            .segment_targets
            .iter()
            .find(|(source, _)| *source == schema_id)
        {
            return Ok(*target);
        }
        let mut target = schema_id;
        while target < self.target_schema().version()
            && self.schema_manager().is_builtin_compatible_transition(
                target,
                target + 1,
                self.column_family_id,
            )?
        {
            target += 1;
        }
        self.segment_targets.push((schema_id, target));
        Ok(target)
    }
}

pub(crate) fn resolve_column_with_vlog<F>(
    column: Column,
    resolve_pointer: &mut F,
    merge_operator: &dyn MergeOperator,
    time_provider: Option<&dyn TimeProvider>,
) -> Result<Option<Bytes>>
where
    F: FnMut(VlogPointer) -> Result<Bytes>,
{
    match column.value_type() {
        ValueType::Delete => Ok(None),
        ValueType::Put => Ok(Some(Bytes::from(column))),
        ValueType::Merge => Ok(Some(
            merge_operator
                .merge(Bytes::new(), Bytes::from(column), time_provider)?
                .0,
        )),
        ValueType::PutSeparated | ValueType::MergeSeparated => {
            let pointer = VlogPointer::from_bytes(column.data())?;
            let resolved = resolve_pointer(pointer)?;
            if *column.value_type() == ValueType::MergeSeparated {
                Ok(Some(
                    merge_operator
                        .merge(Bytes::new(), resolved, time_provider)?
                        .0,
                ))
            } else {
                Ok(Some(resolved))
            }
        }
        ValueType::MergeSeparatedArray | ValueType::PutSeparatedArray => {
            let mut merged = Bytes::new();
            let items = decode_merge_separated_array(column.data())?;
            let mut operands = Vec::with_capacity(items.len());
            for item in items {
                let item_value = match item.value_type {
                    ValueType::Put | ValueType::Merge => Bytes::copy_from_slice(item.data()),
                    ValueType::PutSeparated | ValueType::MergeSeparated => {
                        resolve_pointer(VlogPointer::from_bytes(item.data())?)?
                    }
                    _ => {
                        return Err(Error::IoError(format!(
                            "Invalid value type in MergeSeparatedArray: {:?}",
                            item.value_type
                        )));
                    }
                };
                match item.value_type {
                    ValueType::Put | ValueType::PutSeparated => {
                        merged = item_value;
                        operands.clear();
                    }
                    ValueType::Merge | ValueType::MergeSeparated => operands.push(item_value),
                    _ => unreachable!(),
                }
            }
            if !operands.is_empty() {
                merged = merge_operator
                    .merge_batch(merged, operands, time_provider)?
                    .0;
            }
            Ok(Some(merged))
        }
    }
}

/// Materializes a value into logical column bytes, resolving VLOG pointers.
pub(crate) fn value_to_vec_of_columns_with_vlog<F>(
    value: Value,
    mut resolve_pointer: F,
    schema: &Schema,
    column_family_id: u8,
    time_provider: Option<&dyn TimeProvider>,
) -> Result<Option<Vec<Option<Bytes>>>>
where
    F: FnMut(VlogPointer) -> Result<Bytes>,
{
    let columns = materialize_columns_with_vlog(
        value,
        &mut resolve_pointer,
        schema,
        column_family_id,
        time_provider,
    )?;
    if columns.iter().all(Option::is_none) {
        Ok(None)
    } else {
        Ok(Some(columns))
    }
}

fn materialize_columns_with_vlog<F>(
    value: Value,
    resolve_pointer: &mut F,
    schema: &Schema,
    column_family_id: u8,
    time_provider: Option<&dyn TimeProvider>,
) -> Result<Vec<Option<Bytes>>>
where
    F: FnMut(VlogPointer) -> Result<Bytes>,
{
    let mut columns = Vec::with_capacity(value.columns.len());
    for (column_idx, column) in value.columns.into_iter().enumerate() {
        columns.push(match column {
            Some(column) => resolve_column_with_vlog(
                column,
                resolve_pointer,
                schema.operator_in_family(column_family_id, column_idx),
                time_provider,
            )?,
            None => None,
        });
    }
    Ok(columns)
}

fn whole_row_delete(value: &Value) -> bool {
    !value.columns().is_empty()
        && value.columns().iter().all(|column| {
            column
                .as_ref()
                .is_some_and(|column| *column.value_type() == ValueType::Delete)
        })
}

fn evolve_merged_value<F>(
    value: Value,
    source_schema_id: u64,
    target_schema_id: u64,
    plan: &mut SchemaMergePlan<'_>,
    time_provider: Option<&dyn TimeProvider>,
    resolve_pointer: &mut F,
) -> Result<Value>
where
    F: FnMut(VlogPointer) -> Result<Bytes>,
{
    if source_schema_id == target_schema_id {
        return Ok(value);
    }
    if matches!(
        plan.evolution(source_schema_id, target_schema_id)?,
        EvolutionPlan::Builtin
    ) {
        return plan.schema_manager().evolve_value_in_family(
            value,
            source_schema_id,
            target_schema_id,
            plan.column_family_id,
        );
    }

    let target_schema = plan.schema_manager().schema(target_schema_id)?;
    let target_columns = target_schema
        .num_columns_in_family(plan.column_family_id)
        .unwrap_or(0);
    let expired_at = value.expired_at();
    if whole_row_delete(&value) {
        return Ok(Value::new_with_expired_at(
            (0..target_columns)
                .map(|_| Some(Column::new(ValueType::Delete, Bytes::new())))
                .collect(),
            expired_at,
        ));
    }
    let source_schema = plan.schema_manager().schema(source_schema_id)?;
    let columns = materialize_columns_with_vlog(
        value,
        resolve_pointer,
        source_schema.as_ref(),
        plan.column_family_id,
        time_provider,
    )?;
    let EvolutionPlan::Materialized(route) = plan.evolution(source_schema_id, target_schema_id)?
    else {
        unreachable!("builtin evolution returned above");
    };
    let columns = route.apply(&columns)?;
    Ok(Value::new_with_expired_at(
        columns
            .into_iter()
            .map(|column| column.map(|value| Column::new(ValueType::Put, value)))
            .collect(),
        expired_at,
    ))
}

/// Merges values ordered from oldest to newest, grouping maximal builtin-compatible
/// schema segments before crossing a schema barrier.
///
/// A segment is normalized to its last compatible schema and merged once. Its
/// complete result is then materialized and projected only when the next input
/// segment crosses an incompatible boundary. Planning ensures
/// compaction never presents an incompatible schema decrease here.
pub(crate) fn merge_schema_values<F>(
    values: Vec<SchemaValue>,
    plan: &mut SchemaMergePlan<'_>,
    time_provider: Option<&dyn TimeProvider>,
    mut resolve_pointer: F,
) -> Result<Option<Value>>
where
    F: FnMut(VlogPointer) -> Result<Bytes>,
{
    if values.is_empty() {
        return Ok(None);
    }
    // Compaction may have rewritten an older stratum to the latest schema while a newer
    // old-schema value still exists, so the fast path must inspect the whole chain.
    if values
        .iter()
        .all(|value| value.schema_id == plan.target_schema().version())
    {
        return Ok(Some(Value::merge_all_in_column_family(
            values.into_iter().map(|value| value.value),
            plan.target_schema(),
            plan.column_family_id,
            time_provider,
        )?));
    }

    let mut values = values.into_iter().peekable();
    let mut merged: Option<SchemaValue> = None;
    while let Some(first) = values.next() {
        let segment_schema_id = plan.segment_target(first.schema_id)?;
        let mut segment = vec![evolve_merged_value(
            first.value,
            first.schema_id,
            segment_schema_id,
            plan,
            time_provider,
            &mut resolve_pointer,
        )?];
        while let Some(next) = values.peek() {
            if plan.segment_target(next.schema_id)? != segment_schema_id {
                break;
            }
            let value = values.next().expect("peeked schema segment");
            segment.push(evolve_merged_value(
                value.value,
                value.schema_id,
                segment_schema_id,
                plan,
                time_provider,
                &mut resolve_pointer,
            )?);
        }
        let (schema_id, value) = match merged.take() {
            Some(previous) if previous.schema_id <= segment_schema_id => {
                let schema = plan.schema_manager().schema(segment_schema_id)?;
                let previous = evolve_merged_value(
                    previous.value,
                    previous.schema_id,
                    segment_schema_id,
                    plan,
                    time_provider,
                    &mut resolve_pointer,
                )?;
                (
                    segment_schema_id,
                    Value::merge_all_in_column_family(
                        std::iter::once(previous).chain(segment),
                        schema.as_ref(),
                        plan.column_family_id,
                        time_provider,
                    )?,
                )
            }
            Some(previous) => {
                return Err(Error::InvalidState(format!(
                    "cannot merge schema {} value after schema {} value: compaction must close incompatible schema transitions",
                    segment_schema_id, previous.schema_id
                )));
            }
            None => {
                let schema = plan.schema_manager().schema(segment_schema_id)?;
                (
                    segment_schema_id,
                    Value::merge_all_in_column_family(
                        segment,
                        schema.as_ref(),
                        plan.column_family_id,
                        time_provider,
                    )?,
                )
            }
        };
        merged = Some(SchemaValue { schema_id, value });
    }
    let Some(merged) = merged else {
        return Ok(None);
    };
    Ok(Some(evolve_merged_value(
        merged.value,
        merged.schema_id,
        plan.target_schema().version(),
        plan,
        time_provider,
        &mut resolve_pointer,
    )?))
}

/// Merges schema-versioned values and materializes the final logical columns.
///
/// One resolver is shared between transition materialization and final VLOG
/// resolution so point-read callers cannot accidentally use different snapshots.
pub(crate) fn merge_schema_values_to_columns<F>(
    values: Vec<SchemaValue>,
    plan: &mut SchemaMergePlan<'_>,
    time_provider: Option<&dyn TimeProvider>,
    mut resolve_pointer: F,
) -> Result<Option<Vec<Option<Bytes>>>>
where
    F: FnMut(VlogPointer) -> Result<Bytes>,
{
    let Some(merged) = merge_schema_values(values, plan, time_provider, &mut resolve_pointer)?
    else {
        return Ok(None);
    };
    value_to_vec_of_columns_with_vlog(
        merged,
        &mut resolve_pointer,
        plan.target_schema(),
        plan.column_family_id,
        time_provider,
    )
}
