use crate::codec::KeyCodec;
use crate::metadata::TableMetadata;
use crate::{BucketHash, FieldId, LogicalType, Result, TableError, TableSchema, Value, ValueCodec};
use bytes::Bytes;
use cobble::{
    ColumnFamilyOptions, Config, Db, DbIterator, ReadOnlyDb, ReadOptions, ScanOptions, Schema,
    ShardSnapshotMetadata, ShardSnapshotRef, WriteOptions,
};
use std::collections::HashMap;
use std::sync::{Arc, mpsc};

pub(crate) struct CompiledTable {
    schema: TableSchema,
    column_family_options: ColumnFamilyOptions,
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

#[derive(Clone)]
enum ReadBackend {
    Writable(Arc<Db>),
    Shard(Arc<ReadOnlyDb>),
    Global(Arc<crate::runtime::GlobalReaderState>),
}

impl ReadBackend {
    fn get_with_options(
        &self,
        bucket: u16,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<Bytes>>>> {
        match self {
            Self::Writable(db) => Ok(db.get_with_options(bucket, key, options)?),
            Self::Shard(db) => Ok(db.get_with_options(bucket, key, options)?),
            Self::Global(state) => state.get(bucket, key, options),
        }
    }

    fn multi_get_with_options(
        &self,
        keys: &[(u16, &[u8])],
        options: &ReadOptions,
    ) -> Result<Vec<Option<Vec<Option<Bytes>>>>> {
        match self {
            Self::Writable(db) => Ok(db.multi_get_with_options(keys, options)?),
            Self::Shard(db) => Ok(db.multi_get_with_options(keys, options)?),
            Self::Global(state) => state.multi_get(keys, options),
        }
    }

    fn scan_with_options_bounds(
        &self,
        bucket: u16,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator> {
        match self {
            Self::Writable(db) => Ok(db.scan_with_options_bounds(bucket, start, end, options)?),
            Self::Shard(db) => Ok(db.scan_with_options_bounds(bucket, start, end, options)?),
            Self::Global(state) => state.scan(bucket, start, end, options),
        }
    }
}

/// A reusable typed projection over one table or fixed snapshot table.
pub struct TableProjection {
    backend: ReadBackend,
    compiled: Arc<CompiledTable>,
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
pub struct Table {
    db: Arc<Db>,
    read_backend: ReadBackend,
    name: String,
    compiled: Arc<CompiledTable>,
    read_options: ReadOptions,
    scan_options: ScanOptions,
    write_options: WriteOptions,
}

/// Typed read-only access to one table in a fixed shard snapshot.
pub struct ReadOnlyTable {
    typed: TypedRead,
}

pub(crate) struct TypedRead {
    name: String,
    compiled: Arc<CompiledTable>,
    read_backend: ReadBackend,
    read_options: ReadOptions,
    scan_options: ScanOptions,
}

impl Table {
    /// Create a table or reopen it when its persisted schema is identical.
    pub fn create(db: Arc<Db>, name: impl Into<String>, schema: TableSchema) -> Result<Self> {
        let name = validate_name(name.into())?;
        let metadata = ensure_table_schema(db.as_ref(), &name, schema)?;
        Self::from_metadata(db, name, metadata)
    }

    /// Open a table from metadata stored in its column-family options.
    pub fn open(db: Arc<Db>, name: impl Into<String>) -> Result<Self> {
        let name = validate_name(name.into())?;
        let current = db.current_schema();
        let metadata = load_table_metadata(&current, &name)?;
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
    pub fn project_by_names<S: AsRef<str>>(&self, field_names: &[S]) -> Result<TableProjection> {
        build_projection(
            self.read_backend.clone(),
            &self.name,
            Arc::clone(&self.compiled),
            field_names,
        )
    }

    /// Write one full row in schema field order.
    pub fn put(&self, row: &[Value]) -> Result<()> {
        self.put_bound(row, &self.write_options)
    }

    /// Write one full row with caller options safely rebound to this table.
    pub fn put_with_options(&self, row: &[Value], options: &WriteOptions) -> Result<()> {
        let mut bound = self.write_options.clone();
        bound.ttl_seconds = options.ttl_seconds;
        bound.await_durable = options.await_durable;
        self.put_bound(row, &bound)
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
        let (bucket, key, values) = encode_table_row(&self.compiled, row)?;
        self.db
            .put_columns_with_options(bucket, key, &values, options)?;
        Ok(())
    }

    /// Read one row by primary key.
    pub fn get(&self, key: &TableKey) -> Result<Option<Vec<Value>>> {
        self.db
            .get_with_options(key.inner.bucket, &key.inner.encoded, &self.read_options)?
            .map(|columns| {
                assemble_row_from_key_values(&self.compiled, &key.inner.values, &columns)
            })
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
                    .map(|columns| {
                        assemble_row_from_key_values(&self.compiled, &key.inner.values, &columns)
                    })
                    .transpose()
            })
            .collect()
    }

    /// Scan all rows in one bucket.
    pub fn scan(&self, bucket: u16) -> Result<TableScan> {
        self.scan_bounds(bucket, None, None)
    }

    /// Scan one bucket from an inclusive primary-key bound to an exclusive bound.
    pub fn scan_bounds(
        &self,
        bucket: u16,
        start_key_inclusive: Option<&TableKey>,
        end_key_exclusive: Option<&TableKey>,
    ) -> Result<TableScan> {
        validate_bound(bucket, start_key_inclusive)?;
        validate_bound(bucket, end_key_exclusive)?;
        Ok(TableScan {
            inner: self.read_backend.scan_with_options_bounds(
                bucket,
                start_key_inclusive.map(|key| key.inner.encoded.as_slice()),
                end_key_exclusive.map(|key| key.inner.encoded.as_slice()),
                &self.scan_options,
            )?,
            _read_backend: self.read_backend.clone(),
            compiled: Arc::clone(&self.compiled),
        })
    }

    /// Start an asynchronous shard snapshot, returning its id immediately.
    pub fn snapshot(&self) -> Result<u64> {
        Ok(self.db.snapshot()?)
    }

    /// Receive either the completed shard input or its publication error.
    pub fn snapshot_with_callback<F>(&self, callback: F) -> Result<u64>
    where
        F: Fn(cobble::Result<ShardSnapshotMetadata>) + Send + Sync + 'static,
    {
        Ok(self.db.snapshot_with_callback(callback)?)
    }

    /// Create a snapshot and wait for its callback, without polling.
    pub fn snapshot_and_wait(&self) -> Result<ShardSnapshotMetadata> {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.snapshot_with_callback(move |result| {
            let _ = sender.send(result);
        })?;
        receiver
            .recv()
            .map_err(|err| TableError::internal(format!("snapshot callback disconnected: {err}")))?
            .map_err(Into::into)
    }

    /// Return complete metadata for a completed shard snapshot.
    pub fn shard_snapshot_metadata(&self, snapshot_id: u64) -> Result<ShardSnapshotMetadata> {
        Ok(self.db.shard_snapshot_metadata(snapshot_id)?)
    }

    /// Refresh this writable handle from the local database schema.
    ///
    /// This does not consult a catalog or track a moving catalog version. Existing projections
    /// remain bound to the layout they were compiled with and must be rebuilt after a change.
    pub fn refresh_schema(&mut self) -> Result<bool> {
        let metadata = load_table_metadata(&self.db.current_schema(), &self.name)?;
        if self.compiled.column_family_options.metadata.as_ref() == Some(&metadata.to_value()?) {
            return Ok(false);
        }
        self.compiled = compile_table(metadata, self.db.total_buckets())?;
        (self.read_options, self.scan_options, self.write_options) =
            build_bound_options(&self.name, &self.compiled);
        Ok(true)
    }

    pub(crate) fn db(&self) -> &Db {
        self.db.as_ref()
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn from_metadata(
        db: Arc<Db>,
        name: String,
        metadata: TableMetadata,
    ) -> Result<Self> {
        let compiled = compile_table(metadata, db.total_buckets())?;
        let (read_options, scan_options, write_options) = build_bound_options(&name, &compiled);
        Ok(Self {
            read_backend: ReadBackend::Writable(Arc::clone(&db)),
            db,
            name,
            compiled,
            read_options,
            scan_options,
            write_options,
        })
    }
}

fn build_bound_options(
    name: &str,
    compiled: &CompiledTable,
) -> (ReadOptions, ScanOptions, WriteOptions) {
    (
        ReadOptions::default()
            .with_column_family(name)
            .bound_to_column_family_schema(
                compiled.column_family_options.clone(),
                compiled.physical_columns,
            ),
        ScanOptions::default()
            .with_column_family(name)
            .bound_to_column_family_schema(
                compiled.column_family_options.clone(),
                compiled.physical_columns,
            ),
        WriteOptions::with_column_family(name).bound_to_column_family_schema(
            compiled.column_family_options.clone(),
            compiled.physical_columns,
        ),
    )
}

fn ensure_table_schema(db: &Db, name: &str, schema: TableSchema) -> Result<TableMetadata> {
    let metadata = TableMetadata::compile(schema)?;
    let expected_columns = metadata.layout.value_columns.len().max(1);
    let current = db.current_schema();
    if let Some(id) = current.column_family_ids().get(name).copied() {
        let existing = load_metadata(&current.column_family_options_in_family(id))?;
        if existing != metadata || current.num_columns_in_family(id) != Some(expected_columns) {
            return Err(TableError::InvalidSchema(format!(
                "column family '{name}' is not this table"
            )));
        }
    } else {
        let mut builder = db.update_schema();
        builder.ensure_column_family_exists(name.to_string())?;
        for column in 0..expected_columns {
            builder.add_column(column, None, None, Some(name.to_string()))?;
        }
        builder.set_column_family_options(
            Some(name.to_string()),
            ColumnFamilyOptions {
                metadata: Some(metadata.to_value()?),
                ..ColumnFamilyOptions::default()
            },
        )?;
        builder.commit();
    }
    Ok(metadata)
}

impl ReadOnlyTable {
    /// Open a table from metadata stored in this snapshot's schema.
    pub fn open(db: Arc<ReadOnlyDb>, name: impl Into<String>) -> Result<Self> {
        let name = validate_name(name.into())?;
        let current = db.current_schema();
        let metadata = load_table_metadata(&current, &name)?;
        Self::from_shard_metadata(db, name, metadata)
    }

    pub(crate) fn from_shard_metadata(
        db: Arc<ReadOnlyDb>,
        name: String,
        metadata: TableMetadata,
    ) -> Result<Self> {
        Ok(Self {
            typed: TypedRead::from_shard_metadata(db, name, metadata)?,
        })
    }

    /// Return the persisted semantic schema of this table.
    pub fn schema(&self) -> &TableSchema {
        self.typed.schema()
    }

    /// Start building one primary key in schema order.
    pub fn key_builder(&self) -> TableKeyBuilder {
        self.typed.key_builder()
    }

    /// Compile a reusable read projection from top-level field names.
    pub fn project_by_names<S: AsRef<str>>(&self, field_names: &[S]) -> Result<TableProjection> {
        self.typed.project_by_names(field_names)
    }

    /// Read one row by primary key.
    pub fn get(&self, key: &TableKey) -> Result<Option<Vec<Value>>> {
        self.typed.get(key)
    }

    /// Read many primary keys while preserving order and duplicates.
    pub fn multi_get(&self, keys: &[TableKey]) -> Result<Vec<Option<Vec<Value>>>> {
        self.typed.multi_get(keys)
    }

    /// Scan all rows in one bucket.
    pub fn scan(&self, bucket: u16) -> Result<TableScan> {
        self.typed.scan(bucket)
    }

    /// Scan one bucket from an inclusive primary-key bound to an exclusive bound.
    pub fn scan_bounds(
        &self,
        bucket: u16,
        start_key_inclusive: Option<&TableKey>,
        end_key_exclusive: Option<&TableKey>,
    ) -> Result<TableScan> {
        self.typed
            .scan_bounds(bucket, start_key_inclusive, end_key_exclusive)
    }
}

impl TypedRead {
    pub(crate) fn from_shard_metadata(
        db: Arc<ReadOnlyDb>,
        name: String,
        metadata: TableMetadata,
    ) -> Result<Self> {
        let compiled = compile_table(metadata, db.total_buckets())?;
        Ok(Self::new(name, compiled, ReadBackend::Shard(db)))
    }

    pub(crate) fn from_global_metadata(
        state: Arc<crate::runtime::GlobalReaderState>,
        name: String,
        metadata: TableMetadata,
    ) -> Result<Self> {
        let total_buckets = state.total_buckets();
        let compiled = compile_table(metadata, total_buckets)?;
        Ok(Self::new(name, compiled, ReadBackend::Global(state)))
    }

    fn new(name: String, compiled: Arc<CompiledTable>, read_backend: ReadBackend) -> Self {
        Self {
            name: name.clone(),
            compiled,
            read_backend,
            read_options: ReadOptions::default().with_column_family(name.clone()),
            scan_options: ScanOptions::default().with_column_family(name),
        }
    }

    pub(crate) fn global_state(&self) -> Option<&Arc<crate::runtime::GlobalReaderState>> {
        match &self.read_backend {
            ReadBackend::Global(state) => Some(state),
            ReadBackend::Writable(_) | ReadBackend::Shard(_) => None,
        }
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
    pub fn project_by_names<S: AsRef<str>>(&self, field_names: &[S]) -> Result<TableProjection> {
        build_projection(
            self.read_backend.clone(),
            &self.name,
            Arc::clone(&self.compiled),
            field_names,
        )
    }

    /// Read one row by primary key.
    pub fn get(&self, key: &TableKey) -> Result<Option<Vec<Value>>> {
        self.read_backend
            .get_with_options(key.inner.bucket, &key.inner.encoded, &self.read_options)?
            .map(|columns| {
                assemble_row_from_key_values(&self.compiled, &key.inner.values, &columns)
            })
            .transpose()
    }

    /// Read many primary keys while preserving order and duplicates.
    pub fn multi_get(&self, keys: &[TableKey]) -> Result<Vec<Option<Vec<Value>>>> {
        let requests = keys
            .iter()
            .map(|key| (key.inner.bucket, key.inner.encoded.as_slice()))
            .collect::<Vec<_>>();
        self.read_backend
            .multi_get_with_options(&requests, &self.read_options)?
            .into_iter()
            .zip(keys)
            .map(|(columns, key)| {
                columns
                    .map(|columns| {
                        assemble_row_from_key_values(&self.compiled, &key.inner.values, &columns)
                    })
                    .transpose()
            })
            .collect()
    }

    /// Scan all rows in one bucket.
    pub fn scan(&self, bucket: u16) -> Result<TableScan> {
        self.scan_bounds(bucket, None, None)
    }

    /// Scan one bucket from an inclusive primary-key bound to an exclusive bound.
    pub fn scan_bounds(
        &self,
        bucket: u16,
        start_key_inclusive: Option<&TableKey>,
        end_key_exclusive: Option<&TableKey>,
    ) -> Result<TableScan> {
        validate_bound(bucket, start_key_inclusive)?;
        validate_bound(bucket, end_key_exclusive)?;
        Ok(TableScan {
            inner: self.read_backend.scan_with_options_bounds(
                bucket,
                start_key_inclusive.map(|key| key.inner.encoded.as_slice()),
                end_key_exclusive.map(|key| key.inner.encoded.as_slice()),
                &self.scan_options,
            )?,
            _read_backend: self.read_backend.clone(),
            compiled: Arc::clone(&self.compiled),
        })
    }
}

impl TableProjection {
    /// Read one projected row.
    pub fn get(&self, key: &TableKey) -> Result<Option<Vec<Value>>> {
        self.backend
            .get_with_options(key.inner.bucket, &key.inner.encoded, &self.read_options)?
            .map(|columns| {
                assemble_projected_row(
                    &self.compiled,
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
        self.backend
            .multi_get_with_options(&requests, &self.read_options)?
            .into_iter()
            .zip(keys)
            .map(|(columns, key)| {
                columns
                    .map(|columns| {
                        assemble_projected_row(
                            &self.compiled,
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
    pub fn scan(&self, bucket: u16) -> Result<ProjectedTableScan> {
        self.scan_bounds(bucket, None, None)
    }

    /// Scan projected rows over cached primary-key bounds.
    pub fn scan_bounds(
        &self,
        bucket: u16,
        start_key_inclusive: Option<&TableKey>,
        end_key_exclusive: Option<&TableKey>,
    ) -> Result<ProjectedTableScan> {
        validate_bound(bucket, start_key_inclusive)?;
        validate_bound(bucket, end_key_exclusive)?;
        Ok(ProjectedTableScan {
            inner: self.backend.scan_with_options_bounds(
                bucket,
                start_key_inclusive.map(|key| key.inner.encoded.as_slice()),
                end_key_exclusive.map(|key| key.inner.encoded.as_slice()),
                &self.scan_options,
            )?,
            _read_backend: self.backend.clone(),
            compiled: Arc::clone(&self.compiled),
            plan: Arc::clone(&self.plan),
        })
    }
}

/// Iterator over typed rows from a bucket-scoped table scan.
pub struct TableScan {
    inner: DbIterator,
    // `inner` drops first, releasing its owned access guard before this can
    // release the backend that owns the underlying read route.
    _read_backend: ReadBackend,
    compiled: Arc<CompiledTable>,
}

/// Iterator over projected typed rows from a bucket-scoped scan.
pub struct ProjectedTableScan {
    inner: DbIterator,
    _read_backend: ReadBackend,
    compiled: Arc<CompiledTable>,
    plan: Arc<ProjectionPlan>,
}

impl Iterator for ProjectedTableScan {
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

impl Iterator for TableScan {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|row| {
            let (key, columns) = row?;
            decode_table_scan_row(&self.compiled, &key, &columns)
        })
    }
}

pub(crate) fn decode_table_scan_row(
    compiled: &CompiledTable,
    key: &[u8],
    columns: &[Option<Bytes>],
) -> Result<Vec<Value>> {
    let mut row = vec![Value::Null; compiled.schema.fields.len()];
    KeyCodec::decode_row_into_positions_validated(
        &compiled.key_types,
        key,
        &compiled.key_positions,
        &mut row,
    )?;
    decode_value_columns(compiled, &mut row, columns)?;
    Ok(row)
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

fn encode_table_row(
    compiled: &CompiledTable,
    row: &[Value],
) -> Result<(u16, Vec<u8>, Vec<Vec<u8>>)> {
    if row.len() != compiled.schema.fields.len() {
        return Err(TableError::codec("row field count does not match schema"));
    }
    let (encoded, prefix_end) = KeyCodec::encode_row_from_positions_validated(
        &compiled.key_types,
        row,
        &compiled.key_positions,
        compiled.bucket_key_fields,
    )?;
    let mut values = Vec::with_capacity(compiled.physical_columns);
    for (position, logical_type) in compiled.value_positions.iter().zip(&compiled.value_types) {
        values.push(ValueCodec::encode_validated(logical_type, &row[*position])?);
    }
    if values.is_empty() {
        values.push(vec![1]);
    }
    Ok((
        compiled.bucket_hash.bucket(&encoded[..prefix_end]),
        encoded,
        values,
    ))
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

fn build_projection<S: AsRef<str>>(
    backend: ReadBackend,
    name: &str,
    compiled: Arc<CompiledTable>,
    field_names: &[S],
) -> Result<TableProjection> {
    let (plan, read_options, scan_options) =
        build_projection_parts(name, Arc::clone(&compiled), field_names)?;
    Ok(TableProjection {
        backend,
        compiled,
        plan,
        read_options,
        scan_options,
    })
}

fn build_projection_parts<S: AsRef<str>>(
    name: &str,
    compiled: Arc<CompiledTable>,
    field_names: &[S],
) -> Result<(Arc<ProjectionPlan>, ReadOptions, ScanOptions)> {
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
        let schema_position = compiled
            .schema
            .fields
            .iter()
            .position(|field| field.name == field_name)
            .ok_or_else(|| {
                TableError::InvalidSchema(format!("projection field '{field_name}' does not exist"))
            })?;
        if let Some(key_index) = compiled
            .key_positions
            .iter()
            .position(|position| *position == schema_position)
        {
            sources.push(ProjectedFieldSource::Key(key_index));
            has_key_fields = true;
        } else {
            let physical_column = compiled
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
    Ok((
        Arc::new(ProjectionPlan {
            sources,
            has_key_fields,
        }),
        ReadOptions::for_columns_in_family(name.to_string(), physical_columns.clone())
            .bound_to_column_family_schema(
                compiled.column_family_options.clone(),
                compiled.physical_columns,
            ),
        ScanOptions::for_columns(physical_columns)
            .with_column_family(name.to_string())
            .bound_to_column_family_schema(
                compiled.column_family_options.clone(),
                compiled.physical_columns,
            ),
    ))
}

pub(crate) fn compile_table(
    metadata: TableMetadata,
    total_buckets: u32,
) -> Result<Arc<CompiledTable>> {
    metadata.validate()?;
    let column_family_options = ColumnFamilyOptions {
        metadata: Some(metadata.to_value()?),
        ..ColumnFamilyOptions::default()
    };
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
    Ok(Arc::new(CompiledTable {
        schema: metadata.schema,
        column_family_options,
        key_positions,
        key_types,
        bucket_key_fields: metadata.layout.bucket_fields.len(),
        value_positions,
        value_types,
        physical_columns: metadata.layout.value_columns.len().max(1),
        bucket_hash: BucketHash::new(total_buckets)?,
    }))
}

pub(crate) fn load_table_metadata(schema: &Schema, name: &str) -> Result<TableMetadata> {
    let id = schema
        .column_family_ids()
        .get(name)
        .copied()
        .ok_or_else(|| TableError::InvalidSchema(format!("unknown table '{name}'")))?;
    load_table_metadata_from_options(
        &schema.column_family_options_in_family(id),
        schema.num_columns_in_family(id),
        name,
    )
}

pub(crate) fn load_table_metadata_from_snapshot(
    snapshot: &ShardSnapshotMetadata,
    name: &str,
) -> Result<TableMetadata> {
    let family = snapshot
        .column_families
        .get(name)
        .ok_or_else(|| TableError::InvalidSchema(format!("unknown table '{name}'")))?;
    load_table_metadata_from_options(&family.options, Some(family.num_columns), name)
}

pub(crate) fn load_table_metadata_for_shard(
    config: &Config,
    shard: &ShardSnapshotRef,
    name: &str,
) -> Result<TableMetadata> {
    let snapshot =
        cobble::load_shard_snapshot_metadata(config, &shard.db_id, &shard.manifest_path)?;
    if snapshot.snapshot_id != shard.snapshot_id {
        return Err(TableError::InvalidSchema(format!(
            "shard manifest snapshot {} does not match assigned snapshot {}",
            snapshot.snapshot_id, shard.snapshot_id
        )));
    }
    load_table_metadata_from_snapshot(&snapshot, name)
}

pub(crate) fn table_metadata_from_shard_snapshot(
    snapshot: &ShardSnapshotMetadata,
) -> Result<std::collections::BTreeMap<String, TableMetadata>> {
    let mut tables = std::collections::BTreeMap::new();
    for (name, family) in &snapshot.column_families {
        let is_table = family
            .options
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("format"))
            .and_then(serde_json::Value::as_str)
            == Some(crate::metadata::TABLE_METADATA_FORMAT);
        if !is_table {
            continue;
        }
        tables.insert(
            name.clone(),
            load_table_metadata_from_options(&family.options, Some(family.num_columns), name)?,
        );
    }
    if tables.is_empty() {
        return Err(TableError::InvalidSchema(
            "table shard snapshot contains no table metadata".into(),
        ));
    }
    Ok(tables)
}

fn load_table_metadata_from_options(
    options: &ColumnFamilyOptions,
    num_columns: Option<usize>,
    name: &str,
) -> Result<TableMetadata> {
    let metadata = load_metadata(options)?;
    if num_columns != Some(metadata.layout.value_columns.len().max(1)) {
        return Err(TableError::InvalidSchema(format!(
            "table '{name}' has an incompatible physical column count"
        )));
    }
    Ok(metadata)
}

fn validate_bound(bucket: u16, key: Option<&TableKey>) -> Result<()> {
    if key.is_some_and(|key| key.inner.bucket != bucket) {
        return Err(TableError::codec(
            "table scan bound belongs to a different bucket",
        ));
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

pub(crate) fn validate_name(name: String) -> Result<String> {
    if name.is_empty() || name != name.trim() {
        return Err(TableError::InvalidSchema(
            "table name must be non-empty without surrounding whitespace".to_string(),
        ));
    }
    Ok(name)
}
