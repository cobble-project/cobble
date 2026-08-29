use crate::buffer::InputBytes;
use crate::error::{input_error, invalid_state, map_error};
use bytes::Bytes;
use cobble_binding::{MergeOperator, Schema, SchemaBuilder, SingleDb, merge_operator_by_id};
use pyo3::prelude::*;
use serde_json::Value;
use std::sync::Arc;

#[pyclass(
    name = "MergeOperatorSpec",
    module = "pycobble._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyMergeOperatorSpec {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    metadata_json: Option<String>,
}

#[pyclass(
    name = "ColumnFamily",
    module = "pycobble._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyColumnFamily {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    id: u8,
    #[pyo3(get)]
    column_count: usize,
    #[pyo3(get)]
    value_has_ttl: bool,
    merge_operators: Vec<PyMergeOperatorSpec>,
}

#[pymethods]
impl PyColumnFamily {
    #[getter]
    fn merge_operators(&self) -> Vec<PyMergeOperatorSpec> {
        self.merge_operators.clone()
    }
}

#[pyclass(name = "Schema", module = "pycobble._native", frozen)]
pub(crate) struct PySchema {
    #[pyo3(get)]
    version: u64,
    column_families: Vec<PyColumnFamily>,
}

#[pymethods]
impl PySchema {
    #[getter]
    fn column_families(&self) -> Vec<PyColumnFamily> {
        self.column_families.clone()
    }
}

pub(crate) fn schema(value: &Schema) -> PyResult<PySchema> {
    let family_ids = value.column_family_ids();
    let mut column_families = Vec::with_capacity(family_ids.len());
    for (name, column_count) in value.column_families() {
        let id = family_ids
            .get(&name)
            .copied()
            .ok_or_else(|| input_error(format!("schema is missing id for family '{name}'")))?;
        let operator_ids = value.operator_ids_in_family(&name).map_err(map_error)?;
        let mut merge_operators = Vec::with_capacity(operator_ids.len());
        for (column, operator_id) in operator_ids.into_iter().enumerate() {
            let metadata = value
                .column_metadata_at(Some(&name), column)
                .map_err(map_error)?;
            merge_operators.push(PyMergeOperatorSpec {
                id: operator_id,
                metadata_json: metadata.map(Value::to_string),
            });
        }
        column_families.push(PyColumnFamily {
            name,
            id,
            column_count,
            value_has_ttl: value.value_has_ttl_in_family(id),
            merge_operators,
        });
    }
    Ok(PySchema {
        version: value.version(),
        column_families,
    })
}

fn resolve_operator(
    operator_id: &str,
    metadata_json: Option<&str>,
) -> PyResult<Arc<dyn MergeOperator>> {
    if operator_id.is_empty() {
        return Err(input_error("merge operator id must not be empty"));
    }
    let metadata = metadata_json
        .map(|json| {
            serde_json::from_str::<Value>(json)
                .map_err(|error| input_error(format!("invalid operator metadata JSON: {error}")))
        })
        .transpose()?;
    merge_operator_by_id(operator_id, metadata.as_ref(), None).map_err(map_error)
}

#[pyclass(name = "SchemaBuilder", module = "pycobble._native", unsendable)]
pub(crate) struct PySchemaBuilder {
    // Drop the core builder/access guard before the final database owner.
    builder: Option<SchemaBuilder>,
    owner: Option<Arc<SingleDb>>,
}

impl PySchemaBuilder {
    pub(crate) fn new(owner: Arc<SingleDb>) -> Self {
        Self {
            builder: Some(owner.db().update_schema()),
            owner: Some(owner),
        }
    }

    fn builder(&mut self) -> PyResult<&mut SchemaBuilder> {
        self.builder
            .as_mut()
            .ok_or_else(|| invalid_state("schema builder was already committed"))
    }
}

#[pymethods]
impl PySchemaBuilder {
    #[pyo3(signature = (column, operator_id, *, metadata_json=None, column_family=None))]
    fn set_column_operator(
        &mut self,
        column: usize,
        operator_id: &str,
        metadata_json: Option<&str>,
        column_family: Option<String>,
    ) -> PyResult<()> {
        let operator = resolve_operator(operator_id, metadata_json)?;
        self.builder()?
            .set_column_operator(column_family, column, operator)
            .map_err(map_error)
    }

    #[pyo3(signature = (column, *, operator_id=None, metadata_json=None, default_value=None, column_family=None))]
    fn add_column(
        &mut self,
        column: usize,
        operator_id: Option<&str>,
        metadata_json: Option<&str>,
        default_value: Option<&Bound<'_, PyAny>>,
        column_family: Option<String>,
    ) -> PyResult<()> {
        if operator_id.is_none() && metadata_json.is_some() {
            return Err(input_error(
                "operator metadata requires an explicit merge operator",
            ));
        }
        let operator = operator_id
            .map(|id| resolve_operator(id, metadata_json))
            .transpose()?;
        // Schema defaults are persisted, so the core must own this payload.
        let default_value = default_value
            .map(InputBytes::extract)
            .transpose()?
            .map(|value| Bytes::copy_from_slice(value.as_ref()));
        self.builder()?
            .add_column(column, operator, default_value, column_family)
            .map_err(map_error)
    }

    #[pyo3(signature = (column, *, column_family=None))]
    fn delete_column(&mut self, column: usize, column_family: Option<String>) -> PyResult<()> {
        self.builder()?
            .delete_column(column_family, column)
            .map_err(map_error)
    }

    #[pyo3(signature = (value_has_ttl, *, column_family=None))]
    fn set_column_family_ttl(
        &mut self,
        value_has_ttl: bool,
        column_family: Option<String>,
    ) -> PyResult<()> {
        self.builder()?
            .set_column_family_value_has_ttl(column_family, value_has_ttl)
            .map_err(map_error)
    }

    fn commit(&mut self) -> PyResult<PySchema> {
        let builder = self
            .builder
            .take()
            .ok_or_else(|| invalid_state("schema builder was already committed"))?;
        let result = schema(builder.commit().as_ref());
        self.owner.take();
        result
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyMergeOperatorSpec>()?;
    module.add_class::<PyColumnFamily>()?;
    module.add_class::<PySchema>()?;
    module.add_class::<PySchemaBuilder>()
}
