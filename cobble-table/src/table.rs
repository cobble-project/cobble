use crate::codec::KeyCodec;
use crate::metadata::TableMetadata;
use crate::{BucketHash, FieldId, LogicalType, Result, TableError, TableSchema, Value, ValueCodec};
use bytes::Bytes;
use cobble::{ColumnFamilyOptions, Db, DbIterator, ReadOptions, ScanOptions, WriteOptions};
use std::collections::HashMap;
use std::sync::Arc;

struct CompiledTable {
    schema: TableSchema,
    key_positions: Vec<usize>,
    key_types: Vec<LogicalType>,
    bucket_key_fields: usize,
    value_positions: Vec<usize>,
    value_types: Vec<LogicalType>,
    physical_columns: usize,
    bucket_hash: BucketHash,
}

struct TableKeyData {
    values: Vec<Value>,
    bucket: u16,
    encoded: Vec<u8>,
}

/// A validated and encoded primary key for a table.
///
/// Cloning a key is cheap and shares its encoded bytes and typed values.
#[derive(Clone)]
pub struct TableKey {
    inner: Arc<TableKeyData>,
}

impl TableKey {
    /// Return the bucket selected for this key.
    #[must_use]
    pub fn bucket(&self) -> u16 {
        self.inner.bucket
    }
}

/// Incrementally builds one table primary key in schema order.
pub struct TableKeyBuilder {
    compiled: Arc<CompiledTable>,
    values: Vec<Value>,
}

enum ProjectedFieldSource {
    Key(usize),
    Value {
        /// Position in the compact column list returned by the projection read options.
        projected_column: usize,
        /// Position in the table's physical value columns, used to select its logical type.
        physical_column: usize,
    },
}

struct ProjectionPlan {
    sources: Vec<ProjectedFieldSource>,
    has_key_fields: bool,
}

/// A reusable typed projection over one table.
pub struct TableProjection<'table, 'db> {
    table: &'table Table<'db>,
    plan: Arc<ProjectionPlan>,
    read_options: ReadOptions,
    scan_options: ScanOptions,
}

impl TableKeyBuilder {
    /// Append the next primary-key field.
    pub fn push(&mut self, value: Value) -> &mut Self {
        self.values.push(value);
        self
    }

    /// Validate and encode the complete primary key.
    pub fn build(self) -> Result<TableKey> {
        let mut encoded = Vec::new();
        let prefix_end = KeyCodec::encode_row_with_prefix_validated(
            &self.compiled.key_types,
            &self.values,
            self.compiled.bucket_key_fields,
            &mut encoded,
        )?;
        let bucket = self.compiled.bucket_hash.bucket(&encoded[..prefix_end]);
        Ok(TableKey {
            inner: Arc::new(TableKeyData {
                values: self.values,
                bucket,
                encoded,
            }),
        })
    }
}

/// Typed access to one table-backed Cobble column family.
pub struct Table<'db> {
    db: &'db Db,
    name: String,
    compiled: Arc<CompiledTable>,
    read_options: ReadOptions,
    scan_options: ScanOptions,
    write_options: WriteOptions,
}

impl<'db> Table<'db> {
    /// Create a table or reopen it when its persisted schema is identical.
    pub fn create(db: &'db Db, name: impl Into<String>, schema: TableSchema) -> Result<Self> {
        let name = validate_name(name.into())?;
        let metadata = TableMetadata::compile(schema)?;
        let expected_columns = metadata.layout.value_columns.len().max(1);
        let current = db.current_schema();
        if let Some(id) = current.column_family_ids().get(&name).copied() {
            let existing = load_metadata(&current.column_family_options_in_family(id))?;
            if existing != metadata || current.num_columns_in_family(id) != Some(expected_columns) {
                return Err(TableError::InvalidSchema(format!(
                    "column family '{name}' is not this table"
                )));
            }
        } else {
            let mut builder = db.update_schema();
            builder.ensure_column_family_exists(name.clone())?;
            for column in 0..expected_columns {
                builder.add_column(column, None, None, Some(name.clone()))?;
            }
            builder.set_column_family_options(
                Some(name.clone()),
                ColumnFamilyOptions {
                    metadata: Some(metadata.to_value()?),
                    ..ColumnFamilyOptions::default()
                },
            )?;
            builder.commit();
        }
        Self::from_metadata(db, name, metadata)
    }

    /// Open a table from metadata stored in its column-family options.
    pub fn open(db: &'db Db, name: impl Into<String>) -> Result<Self> {
        let name = validate_name(name.into())?;
        let current = db.current_schema();
        let id = current
            .column_family_ids()
            .get(&name)
            .copied()
            .ok_or_else(|| TableError::InvalidSchema(format!("unknown table '{name}'")))?;
        let metadata = load_metadata(&current.column_family_options_in_family(id))?;
        if current.num_columns_in_family(id) != Some(metadata.layout.value_columns.len().max(1)) {
            return Err(TableError::InvalidSchema(format!(
                "table '{name}' has an incompatible physical column count"
            )));
        }
        Self::from_metadata(db, name, metadata)
    }

    /// Return the persisted semantic schema of this table.
    pub fn schema(&self) -> &TableSchema {
        &self.compiled.schema
    }

    /// Start building one primary key in schema order.
    pub fn key_builder(&self) -> TableKeyBuilder {
        TableKeyBuilder {
            compiled: Arc::clone(&self.compiled),
            values: Vec::with_capacity(self.compiled.key_positions.len()),
        }
    }

    /// Compile a reusable read projection from top-level field names.
    pub fn project_by_names<S: AsRef<str>>(
        &self,
        field_names: &[S],
    ) -> Result<TableProjection<'_, 'db>> {
        if field_names.is_empty() {
            return Err(TableError::InvalidSchema(
                "table projection must contain at least one field".to_string(),
            ));
        }
        let mut seen = std::collections::HashSet::with_capacity(field_names.len());
        let mut sources = Vec::with_capacity(field_names.len());
        let mut physical_columns = Vec::new();
        let mut has_key_fields = false;
        for field_name in field_names {
            let field_name = field_name.as_ref();
            if !seen.insert(field_name) {
                return Err(TableError::InvalidSchema(format!(
                    "duplicate projection field: '{field_name}'"
                )));
            }
            let schema_position = self
                .compiled
                .schema
                .fields
                .iter()
                .position(|field| field.name == field_name)
                .ok_or_else(|| {
                    TableError::InvalidSchema(format!(
                        "projection field '{field_name}' does not exist"
                    ))
                })?;
            if let Some(key_index) = self
                .compiled
                .key_positions
                .iter()
                .position(|position| *position == schema_position)
            {
                sources.push(ProjectedFieldSource::Key(key_index));
                has_key_fields = true;
            } else {
                let physical_column = self
                    .compiled
                    .value_positions
                    .iter()
                    .position(|position| *position == schema_position)
                    .expect("compiled table maps every non-key field");
                let projected_column = physical_columns.len();
                physical_columns.push(physical_column);
                sources.push(ProjectedFieldSource::Value {
                    projected_column,
                    physical_column,
                });
            }
        }
        if physical_columns.is_empty() {
            physical_columns.push(0);
        }
        let plan = Arc::new(ProjectionPlan {
            sources,
            has_key_fields,
        });
        Ok(TableProjection {
            table: self,
            plan,
            read_options: ReadOptions::for_columns_in_family(
                self.name.clone(),
                physical_columns.clone(),
            ),
            scan_options: ScanOptions::for_columns(physical_columns)
                .with_column_family(self.name.clone()),
        })
    }

    /// Write one full row in schema field order.
    pub fn put(&self, row: &[Value]) -> Result<()> {
        self.put_bound(row, &self.write_options)
    }

    /// Write one full row with caller options safely rebound to this table.
    pub fn put_with_options(&self, row: &[Value], options: &WriteOptions) -> Result<()> {
        let options = options.bound_to_column_family(self.name.clone());
        self.put_bound(row, &options)
    }

    /// Delete one complete row.
    pub fn delete(&self, key: &TableKey) -> Result<()> {
        self.db.delete_row_with_options(
            key.inner.bucket,
            key.inner.encoded.as_slice(),
            &self.write_options,
        )?;
        Ok(())
    }

    /// Delete complete rows in one batch without cloning encoded keys.
    pub fn delete_batch(&self, keys: &[TableKey]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let requests = keys
            .iter()
            .map(|key| (key.inner.bucket, key.inner.encoded.as_slice()))
            .collect::<Vec<_>>();
        self.db
            .delete_rows_with_options(&requests, &self.write_options)?;
        Ok(())
    }

    fn put_bound(&self, row: &[Value], options: &WriteOptions) -> Result<()> {
        let (bucket, key) = self.encode_row_key(row)?;
        let mut values = Vec::with_capacity(self.compiled.physical_columns);
        for (position, logical_type) in self
            .compiled
            .value_positions
            .iter()
            .zip(&self.compiled.value_types)
        {
            values.push(ValueCodec::encode_validated(logical_type, &row[*position])?);
        }
        if values.is_empty() {
            values.push(vec![1]);
        }
        self.db
            .put_columns_with_options(bucket, key, &values, options)?;
        Ok(())
    }

    /// Read one row by primary key.
    pub fn get(&self, key: &TableKey) -> Result<Option<Vec<Value>>> {
        self.db
            .get_with_options(key.inner.bucket, &key.inner.encoded, &self.read_options)?
            .map(|columns| self.assemble_row_from_key_values(&key.inner.values, &columns))
            .transpose()
    }

    /// Read many primary keys with one core multi-get, preserving order and duplicates.
    pub fn multi_get(&self, keys: &[TableKey]) -> Result<Vec<Option<Vec<Value>>>> {
        let mut requests = Vec::with_capacity(keys.len());
        for key in keys {
            requests.push((key.inner.bucket, key.inner.encoded.as_slice()));
        }
        self.db
            .multi_get_with_options(&requests, &self.read_options)?
            .into_iter()
            .zip(keys)
            .map(|(columns, key)| {
                columns
                    .map(|columns| self.assemble_row_from_key_values(&key.inner.values, &columns))
                    .transpose()
            })
            .collect()
    }

    /// Scan all rows in one bucket.
    pub fn scan(&self, bucket: u16) -> Result<TableScan<'db>> {
        self.scan_bounds(bucket, None, None)
    }

    /// Scan one bucket from an inclusive primary-key bound to an exclusive bound.
    pub fn scan_bounds(
        &self,
        bucket: u16,
        start_key_inclusive: Option<&TableKey>,
        end_key_exclusive: Option<&TableKey>,
    ) -> Result<TableScan<'db>> {
        self.validate_bound(bucket, start_key_inclusive)?;
        self.validate_bound(bucket, end_key_exclusive)?;
        Ok(TableScan {
            inner: self.db.scan_with_options_bounds(
                bucket,
                start_key_inclusive.map(|key| key.inner.encoded.as_slice()),
                end_key_exclusive.map(|key| key.inner.encoded.as_slice()),
                &self.scan_options,
            )?,
            compiled: Arc::clone(&self.compiled),
        })
    }

    fn from_metadata(db: &'db Db, name: String, metadata: TableMetadata) -> Result<Self> {
        metadata.validate()?;
        let positions = metadata
            .schema
            .fields
            .iter()
            .enumerate()
            .map(|(position, field)| (field.id, position))
            .collect::<HashMap<FieldId, usize>>();
        let key_positions = metadata
            .layout
            .key_fields
            .iter()
            .map(|id| positions[id])
            .collect::<Vec<_>>();
        let key_types = key_positions
            .iter()
            .map(|position| metadata.schema.fields[*position].logical_type.clone())
            .collect::<Vec<_>>();
        let value_positions = metadata
            .layout
            .value_columns
            .iter()
            .map(|column| positions[&column.field_id])
            .collect::<Vec<_>>();
        let value_types = value_positions
            .iter()
            .map(|position| metadata.schema.fields[*position].logical_type.clone())
            .collect::<Vec<_>>();
        let compiled = Arc::new(CompiledTable {
            schema: metadata.schema,
            key_positions,
            key_types,
            bucket_key_fields: metadata.layout.bucket_fields.len(),
            value_positions,
            value_types,
            physical_columns: metadata.layout.value_columns.len().max(1),
            bucket_hash: BucketHash::new(db.total_buckets())?,
        });
        Ok(Self {
            db,
            name: name.clone(),
            compiled,
            read_options: ReadOptions::default().with_column_family(name.clone()),
            scan_options: ScanOptions::default().with_column_family(name.clone()),
            write_options: WriteOptions::with_column_family(name),
        })
    }

    fn encode_row_key(&self, row: &[Value]) -> Result<(u16, Vec<u8>)> {
        if row.len() != self.compiled.schema.fields.len() {
            return Err(TableError::codec("row field count does not match schema"));
        }
        let (encoded, prefix_end) = KeyCodec::encode_row_from_positions_validated(
            &self.compiled.key_types,
            row,
            &self.compiled.key_positions,
            self.compiled.bucket_key_fields,
        )?;
        Ok((
            self.compiled.bucket_hash.bucket(&encoded[..prefix_end]),
            encoded,
        ))
    }

    fn validate_bound(&self, bucket: u16, key: Option<&TableKey>) -> Result<()> {
        let Some(key) = key else {
            return Ok(());
        };
        if key.inner.bucket != bucket {
            return Err(TableError::codec(
                "table scan bound belongs to a different bucket",
            ));
        }
        Ok(())
    }

    fn assemble_row_from_key_values(
        &self,
        primary_key: &[Value],
        columns: &[Option<Bytes>],
    ) -> Result<Vec<Value>> {
        assemble_row_from_key_values(&self.compiled, primary_key, columns)
    }
}

impl TableProjection<'_, '_> {
    /// Read one projected row.
    pub fn get(&self, key: &TableKey) -> Result<Option<Vec<Value>>> {
        self.table
            .db
            .get_with_options(key.inner.bucket, &key.inner.encoded, &self.read_options)?
            .map(|columns| {
                assemble_projected_row(
                    &self.table.compiled,
                    &self.plan,
                    Some(&key.inner.values),
                    &columns,
                )
            })
            .transpose()
    }

    /// Read projected rows in input order, preserving duplicates and misses.
    pub fn multi_get(&self, keys: &[TableKey]) -> Result<Vec<Option<Vec<Value>>>> {
        let requests = keys
            .iter()
            .map(|key| (key.inner.bucket, key.inner.encoded.as_slice()))
            .collect::<Vec<_>>();
        self.table
            .db
            .multi_get_with_options(&requests, &self.read_options)?
            .into_iter()
            .zip(keys)
            .map(|(columns, key)| {
                columns
                    .map(|columns| {
                        assemble_projected_row(
                            &self.table.compiled,
                            &self.plan,
                            Some(&key.inner.values),
                            &columns,
                        )
                    })
                    .transpose()
            })
            .collect()
    }

    /// Scan projected rows in one bucket.
    pub fn scan(&self, bucket: u16) -> Result<ProjectedTableScan<'_>> {
        self.scan_bounds(bucket, None, None)
    }

    /// Scan projected rows over cached primary-key bounds.
    pub fn scan_bounds(
        &self,
        bucket: u16,
        start_key_inclusive: Option<&TableKey>,
        end_key_exclusive: Option<&TableKey>,
    ) -> Result<ProjectedTableScan<'_>> {
        self.table.validate_bound(bucket, start_key_inclusive)?;
        self.table.validate_bound(bucket, end_key_exclusive)?;
        Ok(ProjectedTableScan {
            inner: self.table.db.scan_with_options_bounds(
                bucket,
                start_key_inclusive.map(|key| key.inner.encoded.as_slice()),
                end_key_exclusive.map(|key| key.inner.encoded.as_slice()),
                &self.scan_options,
            )?,
            compiled: Arc::clone(&self.table.compiled),
            plan: Arc::clone(&self.plan),
        })
    }
}

/// Iterator over typed rows from a bucket-scoped table scan.
pub struct TableScan<'db> {
    inner: DbIterator<'db>,
    compiled: Arc<CompiledTable>,
}

/// Iterator over projected typed rows from a bucket-scoped scan.
pub struct ProjectedTableScan<'db> {
    inner: DbIterator<'db>,
    compiled: Arc<CompiledTable>,
    plan: Arc<ProjectionPlan>,
}

impl Iterator for ProjectedTableScan<'_> {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|row| {
            let (key, columns) = row?;
            let key_values = self
                .plan
                .has_key_fields
                .then(|| KeyCodec::decode_row_validated(&self.compiled.key_types, &key))
                .transpose()?;
            assemble_projected_row(&self.compiled, &self.plan, key_values.as_deref(), &columns)
        })
    }
}

impl Iterator for TableScan<'_> {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|row| {
            let (key, columns) = row?;
            let mut row = vec![Value::Null; self.compiled.schema.fields.len()];
            KeyCodec::decode_row_into_positions_validated(
                &self.compiled.key_types,
                &key,
                &self.compiled.key_positions,
                &mut row,
            )?;
            decode_value_columns(&self.compiled, &mut row, &columns)?;
            Ok(row)
        })
    }
}

fn assemble_row_from_key_values(
    compiled: &CompiledTable,
    key_values: &[Value],
    columns: &[Option<Bytes>],
) -> Result<Vec<Value>> {
    debug_assert_eq!(key_values.len(), compiled.key_positions.len());
    let mut row = vec![Value::Null; compiled.schema.fields.len()];
    for (position, value) in compiled.key_positions.iter().zip(key_values) {
        row[*position] = value.clone();
    }
    decode_value_columns(compiled, &mut row, columns)?;
    Ok(row)
}

fn assemble_projected_row(
    compiled: &CompiledTable,
    plan: &ProjectionPlan,
    key_values: Option<&[Value]>,
    columns: &[Option<Bytes>],
) -> Result<Vec<Value>> {
    let mut row = Vec::with_capacity(plan.sources.len());
    for source in &plan.sources {
        match source {
            ProjectedFieldSource::Key(key_index) => {
                row.push(key_values.expect("projection decoded key fields")[*key_index].clone());
            }
            ProjectedFieldSource::Value {
                projected_column,
                physical_column,
            } => {
                let value = columns
                    .get(*projected_column)
                    .and_then(|value| value.as_ref())
                    .ok_or_else(|| TableError::codec("table row is missing a value column"))?;
                row.push(ValueCodec::decode_bytes_validated(
                    &compiled.value_types[*physical_column],
                    value.clone(),
                )?);
            }
        }
    }
    Ok(row)
}

fn decode_value_columns(
    compiled: &CompiledTable,
    row: &mut [Value],
    columns: &[Option<Bytes>],
) -> Result<()> {
    for (column, (logical_type, position)) in compiled
        .value_types
        .iter()
        .zip(&compiled.value_positions)
        .enumerate()
    {
        let value = columns
            .get(column)
            .and_then(|value| value.as_ref())
            .ok_or_else(|| TableError::codec("table row is missing a value column"))?;
        row[*position] = ValueCodec::decode_bytes_validated(logical_type, value.clone())?;
    }
    Ok(())
}

fn load_metadata(options: &ColumnFamilyOptions) -> Result<TableMetadata> {
    let metadata = options
        .metadata
        .as_ref()
        .ok_or_else(|| TableError::InvalidSchema("column family is not a table".to_string()))?;
    TableMetadata::from_value(metadata)
}

fn validate_name(name: String) -> Result<String> {
    if name.is_empty() || name != name.trim() {
        return Err(TableError::InvalidSchema(
            "table name must be non-empty without surrounding whitespace".to_string(),
        ));
    }
    Ok(name)
}
