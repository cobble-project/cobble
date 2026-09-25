use super::database::{PyStructuredDb, PyStructuredSingleDb};
use crate::buffer::OwnedBytes;
use crate::error::{input_error, invalid_state, map_error};
use crate::types::{PickleReduction, enum_reduce};
use cobble_binding::ColumnFamilyOptions;
use cobble_binding::structured::{
    ListConfig, ListRetainMode, StructuredColumnType, StructuredColumnValue, StructuredDb,
    StructuredReadOptions, StructuredScanOptions, StructuredSchema, StructuredSingleDb,
};
use pyo3::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

type PickledReadOptions = (Option<String>, Option<Vec<usize>>);
type PickledScanOptions = (Option<String>, Option<Vec<usize>>, bool, bool);

#[pyclass(
    name = "StructuredColumnKind",
    module = "pycobble._native",
    eq,
    eq_int,
    skip_from_py_object
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PyStructuredColumnKind {
    #[pyo3(name = "BYTES")]
    Bytes = 0,
    #[pyo3(name = "LIST")]
    List = 1,
}

#[pymethods]
impl PyStructuredColumnKind {
    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(Py<PyAny>, &'static str)>> {
        enum_reduce::<Self>(
            py,
            match self {
                Self::Bytes => "BYTES",
                Self::List => "LIST",
            },
        )
    }
}

#[pyclass(
    name = "ListRetainMode",
    module = "pycobble._native",
    eq,
    eq_int,
    from_py_object
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PyListRetainMode {
    #[pyo3(name = "FIRST")]
    First = 0,
    #[pyo3(name = "LAST")]
    Last = 1,
}

#[pymethods]
impl PyListRetainMode {
    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(Py<PyAny>, &'static str)>> {
        enum_reduce::<Self>(
            py,
            match self {
                Self::First => "FIRST",
                Self::Last => "LAST",
            },
        )
    }
}

#[pyclass(
    name = "ListConfig",
    module = "pycobble._native",
    frozen,
    from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyListConfig {
    #[pyo3(get)]
    max_elements: Option<usize>,
    #[pyo3(get)]
    retain_mode: PyListRetainMode,
    #[pyo3(get)]
    preserve_element_ttl: bool,
}

#[pymethods]
impl PyListConfig {
    #[staticmethod]
    fn _restore(
        max_elements: Option<usize>,
        retain_mode: PyListRetainMode,
        preserve_element_ttl: bool,
    ) -> Self {
        Self::new(max_elements, retain_mode, preserve_element_ttl)
    }

    fn __reduce__(
        &self,
        py: Python<'_>,
    ) -> PyResult<PickleReduction<(Option<usize>, PyListRetainMode, bool)>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (
                self.max_elements,
                self.retain_mode,
                self.preserve_element_ttl,
            ),
        ))
    }

    #[new]
    #[pyo3(signature = (*, max_elements=None, retain_mode=PyListRetainMode::Last, preserve_element_ttl=false))]
    fn new(
        max_elements: Option<usize>,
        retain_mode: PyListRetainMode,
        preserve_element_ttl: bool,
    ) -> Self {
        Self {
            max_elements,
            retain_mode,
            preserve_element_ttl,
        }
    }
}

impl From<PyListConfig> for ListConfig {
    fn from(value: PyListConfig) -> Self {
        Self {
            max_elements: value.max_elements,
            retain_mode: match value.retain_mode {
                PyListRetainMode::First => ListRetainMode::First,
                PyListRetainMode::Last => ListRetainMode::Last,
            },
            preserve_element_ttl: value.preserve_element_ttl,
        }
    }
}

#[pyclass(
    name = "StructuredReadOptions",
    module = "pycobble._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyStructuredReadOptions {
    pub(crate) inner: StructuredReadOptions,
}

#[pymethods]
impl PyStructuredReadOptions {
    #[staticmethod]
    fn _restore(column_family: Option<String>, columns: Option<Vec<usize>>) -> Self {
        Self::new(column_family, columns)
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<PickledReadOptions>> {
        let raw = self.inner.as_cobble();
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (raw.column_family.clone(), raw.column_indices.clone()),
        ))
    }

    #[new]
    #[pyo3(signature = (*, column_family=None, columns=None))]
    fn new(column_family: Option<String>, columns: Option<Vec<usize>>) -> Self {
        let mut raw = columns.map_or_else(
            cobble_binding::ReadOptions::default,
            cobble_binding::ReadOptions::for_columns,
        );
        raw.column_family = column_family;
        Self { inner: raw.into() }
    }
}

#[pyclass(
    name = "StructuredScanOptions",
    module = "pycobble._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyStructuredScanOptions {
    pub(crate) inner: StructuredScanOptions,
}

#[pymethods]
impl PyStructuredScanOptions {
    #[staticmethod]
    fn _restore(
        column_family: Option<String>,
        columns: Option<Vec<usize>>,
        preload_scan_cursor_block: bool,
        stop_at_block_boundary: bool,
    ) -> Self {
        Self::new(
            column_family,
            columns,
            preload_scan_cursor_block,
            stop_at_block_boundary,
        )
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<PickledScanOptions>> {
        let raw = self.inner.as_cobble();
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (
                raw.column_family.clone(),
                raw.column_indices.clone(),
                raw.preload_scan_cursor_block(),
                raw.should_stop_at_block_boundary(),
            ),
        ))
    }

    #[new]
    #[pyo3(signature = (*, column_family=None, columns=None, preload_scan_cursor_block=false, stop_at_block_boundary=false))]
    fn new(
        column_family: Option<String>,
        columns: Option<Vec<usize>>,
        preload_scan_cursor_block: bool,
        stop_at_block_boundary: bool,
    ) -> Self {
        let mut raw = columns.map_or_else(
            cobble_binding::ScanOptions::default,
            cobble_binding::ScanOptions::for_columns,
        );
        raw.column_family = column_family;
        let inner = StructuredScanOptions::from(raw)
            .with_preload_scan_cursor_block(preload_scan_cursor_block)
            .with_stop_at_block_boundary(stop_at_block_boundary);
        Self { inner }
    }
}

#[pyclass(name = "StructuredRow", module = "pycobble._native", frozen)]
pub(crate) struct PyStructuredRow {
    pub(crate) columns: Option<Vec<Option<StructuredColumnValue>>>,
}

pub(crate) type PickledColumn = (u8, Vec<Vec<u8>>);
pub(crate) type PickledColumns = Vec<Option<PickledColumn>>;

pub(crate) fn pickle_columns(columns: &[Option<StructuredColumnValue>]) -> PickledColumns {
    columns
        .iter()
        .map(|column| {
            column.as_ref().map(|value| match value {
                StructuredColumnValue::Bytes(value) => (0, vec![value.to_vec()]),
                StructuredColumnValue::List(values) => {
                    (1, values.iter().map(|value| value.to_vec()).collect())
                }
            })
        })
        .collect()
}

pub(crate) fn restore_columns(
    columns: PickledColumns,
) -> PyResult<Vec<Option<StructuredColumnValue>>> {
    columns
        .into_iter()
        .map(|column| {
            column
                .map(|(kind, values)| match kind {
                    0 if values.len() == 1 => Ok(StructuredColumnValue::Bytes(bytes::Bytes::from(
                        values.into_iter().next().unwrap(),
                    ))),
                    1 => Ok(StructuredColumnValue::List(
                        values.into_iter().map(bytes::Bytes::from).collect(),
                    )),
                    _ => Err(input_error("invalid pickled structured column")),
                })
                .transpose()
        })
        .collect()
}

#[pyclass(name = "StructuredMultiGetResult", module = "pycobble._native", frozen)]
pub(crate) struct PyStructuredMultiGetResult {
    pub(crate) rows: Vec<Option<Vec<Option<StructuredColumnValue>>>>,
}

#[pymethods]
impl PyStructuredMultiGetResult {
    #[staticmethod]
    fn _restore(rows: Vec<Option<PickledColumns>>) -> PyResult<Self> {
        Ok(Self {
            rows: rows
                .into_iter()
                .map(|row| row.map(restore_columns).transpose())
                .collect::<PyResult<_>>()?,
        })
    }

    fn __reduce__(
        &self,
        py: Python<'_>,
    ) -> PyResult<PickleReduction<(Vec<Option<PickledColumns>>,)>> {
        let rows = self
            .rows
            .iter()
            .map(|row| row.as_ref().map(|columns| pickle_columns(columns)))
            .collect();
        Ok((py.get_type::<Self>().getattr("_restore")?.unbind(), (rows,)))
    }

    fn __len__(&self) -> usize {
        self.rows.len()
    }
    fn row(&self, index: usize) -> PyResult<PyStructuredRow> {
        self.rows
            .get(index)
            .cloned()
            .map(PyStructuredRow::new)
            .ok_or_else(|| input_error("multi_get row index is out of bounds"))
    }
}

impl PyStructuredRow {
    pub(crate) fn new(columns: Option<Vec<Option<StructuredColumnValue>>>) -> Self {
        Self { columns }
    }

    fn column(&self, index: usize) -> PyResult<&StructuredColumnValue> {
        self.columns
            .as_ref()
            .ok_or_else(|| input_error("row was not found"))?
            .get(index)
            .ok_or_else(|| input_error("column index is out of range"))?
            .as_ref()
            .ok_or_else(|| input_error("column value is absent"))
    }
}

#[pymethods]
impl PyStructuredRow {
    #[staticmethod]
    fn _restore(columns: Option<PickledColumns>) -> PyResult<Self> {
        Ok(Self {
            columns: columns.map(restore_columns).transpose()?,
        })
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(Option<PickledColumns>,)>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (self.columns.as_ref().map(|columns| pickle_columns(columns)),),
        ))
    }

    #[getter]
    fn found(&self) -> bool {
        self.columns.is_some()
    }

    #[getter]
    fn column_count(&self) -> usize {
        self.columns.as_ref().map_or(0, Vec::len)
    }

    fn has_column(&self, column: usize) -> bool {
        self.columns
            .as_ref()
            .and_then(|columns| columns.get(column))
            .is_some_and(Option::is_some)
    }

    fn kind(&self, column: usize) -> PyResult<PyStructuredColumnKind> {
        Ok(match self.column(column)? {
            StructuredColumnValue::Bytes(_) => PyStructuredColumnKind::Bytes,
            StructuredColumnValue::List(_) => PyStructuredColumnKind::List,
        })
    }

    fn bytes(&self, column: usize) -> PyResult<OwnedBytes> {
        match self.column(column)? {
            StructuredColumnValue::Bytes(value) => Ok(OwnedBytes::new(value.clone())),
            StructuredColumnValue::List(_) => Err(input_error("column is not BYTES")),
        }
    }

    fn list_size(&self, column: usize) -> PyResult<usize> {
        match self.column(column)? {
            StructuredColumnValue::List(value) => Ok(value.len()),
            StructuredColumnValue::Bytes(_) => Err(input_error("column is not LIST")),
        }
    }

    fn list_element(&self, column: usize, element: usize) -> PyResult<OwnedBytes> {
        match self.column(column)? {
            StructuredColumnValue::List(value) => value
                .get(element)
                .cloned()
                .map(OwnedBytes::new)
                .ok_or_else(|| input_error("list element index is out of range")),
            StructuredColumnValue::Bytes(_) => Err(input_error("column is not LIST")),
        }
    }

    fn __bool__(&self) -> bool {
        self.found()
    }
}

#[pyclass(
    name = "StructuredColumn",
    module = "pycobble._native",
    frozen,
    from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyStructuredColumn {
    #[pyo3(get)]
    index: u16,
    #[pyo3(get)]
    kind: PyStructuredColumnKind,
    #[pyo3(get)]
    list_config: Option<PyListConfig>,
}

#[pymethods]
impl PyStructuredColumn {
    #[staticmethod]
    fn _restore(
        index: u16,
        kind: PyRef<'_, PyStructuredColumnKind>,
        list_config: Option<PyListConfig>,
    ) -> Self {
        Self {
            index,
            kind: *kind,
            list_config,
        }
    }
    fn __reduce__(
        &self,
        py: Python<'_>,
    ) -> PyResult<PickleReduction<(u16, PyStructuredColumnKind, Option<PyListConfig>)>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (self.index, self.kind, self.list_config.clone()),
        ))
    }
}

#[pyclass(
    name = "StructuredFamily",
    module = "pycobble._native",
    frozen,
    from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyStructuredFamily {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    id: u8,
    columns: Vec<PyStructuredColumn>,
}

#[pymethods]
impl PyStructuredFamily {
    #[staticmethod]
    fn _restore(name: String, id: u8, columns: Vec<PyStructuredColumn>) -> Self {
        Self { name, id, columns }
    }
    fn __reduce__(
        &self,
        py: Python<'_>,
    ) -> PyResult<PickleReduction<(String, u8, Vec<PyStructuredColumn>)>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (self.name.clone(), self.id, self.columns.clone()),
        ))
    }

    #[getter]
    fn columns(&self) -> Vec<PyStructuredColumn> {
        self.columns.clone()
    }
}

#[pyclass(name = "StructuredSchema", module = "pycobble._native", frozen)]
pub(crate) struct PyStructuredSchema {
    families: Vec<PyStructuredFamily>,
}

#[pymethods]
impl PyStructuredSchema {
    #[staticmethod]
    fn _restore(families: Vec<PyStructuredFamily>) -> Self {
        Self { families }
    }
    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(Vec<PyStructuredFamily>,)>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (self.families.clone(),),
        ))
    }

    #[getter]
    fn families(&self) -> Vec<PyStructuredFamily> {
        self.families.clone()
    }
}

pub(crate) fn schema(value: StructuredSchema) -> PyStructuredSchema {
    let families = value
        .column_families()
        .into_iter()
        .map(|(name, family)| PyStructuredFamily {
            id: value.column_family_ids.get(&name).copied().unwrap_or(0),
            name,
            columns: family
                .columns
                .into_iter()
                .map(|(index, kind)| match kind {
                    StructuredColumnType::Bytes => PyStructuredColumn {
                        index,
                        kind: PyStructuredColumnKind::Bytes,
                        list_config: None,
                    },
                    StructuredColumnType::List(config) => PyStructuredColumn {
                        index,
                        kind: PyStructuredColumnKind::List,
                        list_config: Some(PyListConfig {
                            max_elements: config.max_elements,
                            retain_mode: match config.retain_mode {
                                ListRetainMode::First => PyListRetainMode::First,
                                ListRetainMode::Last => PyListRetainMode::Last,
                            },
                            preserve_element_ttl: config.preserve_element_ttl,
                        }),
                    },
                })
                .collect(),
        })
        .collect();
    PyStructuredSchema { families }
}

#[derive(Clone)]
enum SchemaOperation {
    AddBytes(Option<String>, u16),
    AddList(Option<String>, u16, ListConfig),
    Delete(Option<String>, u16),
    SetFamilyTtl(Option<String>, bool),
}

pub(crate) enum StructuredOwner {
    Db(Py<PyStructuredDb>),
    Single(Py<PyStructuredSingleDb>),
}

#[pyclass(
    name = "StructuredSchemaBuilder",
    module = "pycobble._native",
    unsendable
)]
pub(crate) struct PyStructuredSchemaBuilder {
    operations: Option<Vec<SchemaOperation>>,
    pub(crate) owner: Option<StructuredOwner>,
    child_count: Option<Arc<AtomicUsize>>,
}

impl PyStructuredSchemaBuilder {
    pub(crate) fn new(owner: StructuredOwner, child_count: Arc<AtomicUsize>) -> Self {
        child_count.fetch_add(1, Ordering::AcqRel);
        Self {
            operations: Some(Vec::new()),
            owner: Some(owner),
            child_count: Some(child_count),
        }
    }

    fn operations(&mut self) -> PyResult<&mut Vec<SchemaOperation>> {
        self.operations
            .as_mut()
            .ok_or_else(|| invalid_state("structured schema builder was already committed"))
    }

    fn release_child(&mut self) {
        if let Some(count) = self.child_count.take() {
            count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl Drop for PyStructuredSchemaBuilder {
    fn drop(&mut self) {
        self.release_child();
    }
}

#[pymethods]
impl PyStructuredSchemaBuilder {
    #[pyo3(signature = (column, *, column_family=None))]
    fn add_bytes_column(&mut self, column: u16, column_family: Option<String>) -> PyResult<()> {
        self.operations()?
            .push(SchemaOperation::AddBytes(column_family, column));
        Ok(())
    }

    #[pyo3(signature = (column, config, *, column_family=None))]
    fn add_list_column(
        &mut self,
        column: u16,
        config: PyListConfig,
        column_family: Option<String>,
    ) -> PyResult<()> {
        self.operations()?.push(SchemaOperation::AddList(
            column_family,
            column,
            config.into(),
        ));
        Ok(())
    }

    #[pyo3(signature = (column, *, column_family=None))]
    fn delete_column(&mut self, column: u16, column_family: Option<String>) -> PyResult<()> {
        self.operations()?
            .push(SchemaOperation::Delete(column_family, column));
        Ok(())
    }

    #[pyo3(signature = (value_has_ttl, *, column_family=None))]
    fn set_column_family_ttl(
        &mut self,
        value_has_ttl: bool,
        column_family: Option<String>,
    ) -> PyResult<()> {
        self.operations()?
            .push(SchemaOperation::SetFamilyTtl(column_family, value_has_ttl));
        Ok(())
    }

    fn commit(&mut self, py: Python<'_>) -> PyResult<PyStructuredSchema> {
        let operations = self
            .operations
            .take()
            .ok_or_else(|| invalid_state("structured schema builder was already committed"))?;
        let owner = self
            .owner
            .as_mut()
            .ok_or_else(|| invalid_state("structured schema builder lost its owner"))?;
        let result = match owner {
            StructuredOwner::Db(owner) => {
                let mut bound = owner.bind(py).try_borrow_mut()?;
                let owner = Arc::get_mut(&mut bound.db).ok_or_else(|| {
                    invalid_state("schema commit requires releasing every structured scan cursor and priority queue first")
                })?;
                let mut builder = owner.update_schema();
                for operation in operations {
                    apply_db_operation(&mut builder, operation);
                }
                builder.commit()
            }
            StructuredOwner::Single(owner) => {
                let mut bound = owner.bind(py).try_borrow_mut()?;
                let owner = Arc::get_mut(&mut bound.db).ok_or_else(|| {
                    invalid_state("schema commit requires releasing every structured scan cursor and priority queue first")
                })?;
                let mut builder = owner.update_schema();
                for operation in operations {
                    apply_single_operation(&mut builder, operation);
                }
                builder.commit()
            }
        }
        .map(schema)
        .map_err(map_error);
        if result.is_ok() {
            self.owner.take();
            self.release_child();
        }
        result
    }
}

fn apply_db_operation(
    builder: &mut cobble_binding::structured::StructuredSchemaBuilder<'_, StructuredDb>,
    operation: SchemaOperation,
) {
    match operation {
        SchemaOperation::AddBytes(family, column) => {
            builder.add_bytes_column(family, column);
        }
        SchemaOperation::AddList(family, column, config) => {
            builder.add_list_column(family, column, config);
        }
        SchemaOperation::Delete(family, column) => {
            builder.delete_column(family, column);
        }
        SchemaOperation::SetFamilyTtl(family, value_has_ttl) => {
            builder.set_column_family_options(
                family,
                ColumnFamilyOptions {
                    value_has_ttl,
                    metadata: None,
                },
            );
        }
    }
}

fn apply_single_operation(
    builder: &mut cobble_binding::structured::StructuredSchemaBuilder<'_, StructuredSingleDb>,
    operation: SchemaOperation,
) {
    match operation {
        SchemaOperation::AddBytes(family, column) => {
            builder.add_bytes_column(family, column);
        }
        SchemaOperation::AddList(family, column, config) => {
            builder.add_list_column(family, column, config);
        }
        SchemaOperation::Delete(family, column) => {
            builder.delete_column(family, column);
        }
        SchemaOperation::SetFamilyTtl(family, value_has_ttl) => {
            builder.set_column_family_options(
                family,
                ColumnFamilyOptions {
                    value_has_ttl,
                    metadata: None,
                },
            );
        }
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyStructuredColumnKind>()?;
    module.add_class::<PyListRetainMode>()?;
    module.add_class::<PyListConfig>()?;
    module.add_class::<PyStructuredReadOptions>()?;
    module.add_class::<PyStructuredScanOptions>()?;
    module.add_class::<PyStructuredRow>()?;
    module.add_class::<PyStructuredMultiGetResult>()?;
    module.add_class::<PyStructuredColumn>()?;
    module.add_class::<PyStructuredFamily>()?;
    module.add_class::<PyStructuredSchema>()?;
    module.add_class::<PyStructuredSchemaBuilder>()
}
