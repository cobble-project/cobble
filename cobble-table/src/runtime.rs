use crate::table::{
    TableKey, TableKeyBuilder, TableScan, assemble_row_from_key_values, compile_table,
    encode_table_row, ensure_table_schema, load_table_metadata, validate_bound, validate_name,
};
use crate::{Result, TableError, TableSchema, Value};
use cobble::{
    Config, Db, DbBuilder, DbIterator, ReadOnlyDb, ReadOptions, Reader, ReaderConfig, ScanOptions,
    ShardSnapshotInput, WriteOptions,
};
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex, mpsc};

/// Builder for a standalone writable typed table shard.
pub struct TableWriterBuilder {
    config: Config,
    table_name: Option<String>,
    db_id: Option<String>,
    bucket_ranges: Vec<RangeInclusive<u16>>,
}

impl TableWriterBuilder {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            table_name: None,
            db_id: None,
            bucket_ranges: Vec::new(),
        }
    }

    /// Select the physical column-family name for this typed table.
    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = Some(table_name.into());
        self
    }

    /// Set the durable shard database identity.
    pub fn db_id(mut self, db_id: impl Into<String>) -> Self {
        self.db_id = Some(db_id.into());
        self
    }

    /// Set the bucket ranges owned by this shard.
    pub fn bucket_ranges(mut self, bucket_ranges: Vec<RangeInclusive<u16>>) -> Self {
        self.bucket_ranges = bucket_ranges;
        self
    }

    /// Create a new database and create the typed table schema.
    pub fn create(self, schema: TableSchema) -> Result<TableWriter> {
        let name = self.required_table_name()?;
        let db = Arc::new(self.db_builder().open()?);
        TableWriter::create(db, name, schema)
    }

    /// Resume a writable shard and load its persisted table schema.
    pub fn resume(self) -> Result<TableWriter> {
        let name = self.required_table_name()?;
        let db = Arc::new(self.db_builder().resume()?);
        TableWriter::open(db, name)
    }

    /// Open a writable shard at a selected snapshot boundary.
    pub fn open_from_snapshot(self, snapshot_id: u64) -> Result<TableWriter> {
        let name = self.required_table_name()?;
        let db = Arc::new(self.db_builder().open_from_snapshot(snapshot_id)?);
        TableWriter::open(db, name)
    }

    /// Resume a writable shard from a selected snapshot boundary.
    pub fn resume_from_snapshot(self, snapshot_id: u64) -> Result<TableWriter> {
        let name = self.required_table_name()?;
        let db = Arc::new(self.db_builder().resume_from_snapshot(snapshot_id)?);
        TableWriter::open(db, name)
    }

    fn required_table_name(&self) -> Result<String> {
        self.table_name
            .clone()
            .ok_or_else(|| {
                TableError::InvalidSchema("TableWriterBuilder requires table_name".into())
            })
            .and_then(validate_name)
    }

    fn db_builder(&self) -> DbBuilder {
        let mut builder =
            DbBuilder::new(self.config.clone()).bucket_ranges(self.bucket_ranges.clone());
        if let Some(db_id) = &self.db_id {
            builder = builder.db_id(db_id.clone());
        }
        builder
    }
}

/// An owned writable typed table. Borrowed [`crate::Table`] remains appropriate
/// when multiple tables share an application-owned shard database.
pub struct TableWriter {
    db: Arc<Db>,
    name: String,
    compiled: Arc<crate::table::CompiledTable>,
    read_options: ReadOptions,
    scan_options: ScanOptions,
    write_options: WriteOptions,
}

impl TableWriter {
    fn create(db: Arc<Db>, name: String, schema: TableSchema) -> Result<Self> {
        let metadata = ensure_table_schema(db.as_ref(), &name, schema)?;
        Self::from_table_metadata(db, name, metadata)
    }

    fn open(db: Arc<Db>, name: String) -> Result<Self> {
        Self::from_metadata(db, name)
    }

    fn from_metadata(db: Arc<Db>, name: String) -> Result<Self> {
        let metadata = load_table_metadata(&db.current_schema(), &name)?;
        Self::from_table_metadata(db, name, metadata)
    }

    fn from_table_metadata(
        db: Arc<Db>,
        name: String,
        metadata: crate::metadata::TableMetadata,
    ) -> Result<Self> {
        let compiled = compile_table(metadata, db.total_buckets())?;
        Ok(Self {
            db,
            name: name.clone(),
            compiled,
            read_options: ReadOptions::default().with_column_family(name.clone()),
            scan_options: ScanOptions::default().with_column_family(name.clone()),
            write_options: WriteOptions::with_column_family(name),
        })
    }

    pub fn schema(&self) -> &TableSchema {
        &self.compiled.schema
    }

    pub fn key_builder(&self) -> TableKeyBuilder {
        TableKeyBuilder {
            compiled: Arc::clone(&self.compiled),
            values: Vec::with_capacity(self.compiled.key_positions.len()),
        }
    }

    pub fn put(&self, row: &[Value]) -> Result<()> {
        let (bucket, key, values) = encode_table_row(&self.compiled, row)?;
        self.db
            .put_columns_with_options(bucket, key, &values, &self.write_options)?;
        Ok(())
    }

    pub fn put_with_options(&self, row: &[Value], options: &WriteOptions) -> Result<()> {
        let (bucket, key, values) = encode_table_row(&self.compiled, row)?;
        self.db.put_columns_with_options(
            bucket,
            key,
            &values,
            &options.bound_to_column_family(self.name.clone()),
        )?;
        Ok(())
    }

    pub fn delete(&self, key: &TableKey) -> Result<()> {
        self.db.delete_row_with_options(
            key.inner.bucket,
            &key.inner.encoded,
            &self.write_options,
        )?;
        Ok(())
    }

    pub fn delete_batch(&self, keys: &[TableKey]) -> Result<()> {
        let requests = keys
            .iter()
            .map(|key| (key.inner.bucket, key.inner.encoded.as_slice()))
            .collect::<Vec<_>>();
        if !requests.is_empty() {
            self.db
                .delete_rows_with_options(&requests, &self.write_options)?;
        }
        Ok(())
    }

    pub fn get(&self, key: &TableKey) -> Result<Option<Vec<Value>>> {
        self.db
            .get_with_options(key.inner.bucket, &key.inner.encoded, &self.read_options)?
            .map(|columns| {
                assemble_row_from_key_values(&self.compiled, &key.inner.values, &columns)
            })
            .transpose()
    }

    pub fn multi_get(&self, keys: &[TableKey]) -> Result<Vec<Option<Vec<Value>>>> {
        let requests = keys
            .iter()
            .map(|key| (key.inner.bucket, key.inner.encoded.as_slice()))
            .collect::<Vec<_>>();
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

    pub fn scan(&self, bucket: u16) -> Result<TableScan> {
        self.scan_bounds(bucket, None, None)
    }

    pub fn scan_bounds(
        &self,
        bucket: u16,
        start_key_inclusive: Option<&TableKey>,
        end_key_exclusive: Option<&TableKey>,
    ) -> Result<TableScan> {
        validate_bound(bucket, start_key_inclusive)?;
        validate_bound(bucket, end_key_exclusive)?;
        Ok(TableScan {
            inner: self.db.scan_with_options_bounds(
                bucket,
                start_key_inclusive.map(|key| key.inner.encoded.as_slice()),
                end_key_exclusive.map(|key| key.inner.encoded.as_slice()),
                &self.scan_options,
            )?,
            _writer_backend: Some(Arc::clone(&self.db)),
            compiled: Arc::clone(&self.compiled),
        })
    }

    pub fn project_by_names<S: AsRef<str>>(
        &self,
        field_names: &[S],
    ) -> Result<crate::TableProjection<'static>> {
        crate::TableProjection::from_runtime(
            Arc::new(RuntimeReadBackend::Writer(Arc::clone(&self.db))),
            self.name.clone(),
            Arc::clone(&self.compiled),
            field_names,
        )
    }

    /// Start an asynchronous shard snapshot, returning its id immediately.
    pub fn snapshot(&self) -> Result<u64> {
        Ok(self.db.snapshot()?)
    }

    /// Receive either the completed shard input or its publication error.
    pub fn snapshot_with_callback<F>(&self, callback: F) -> Result<u64>
    where
        F: Fn(cobble::Result<ShardSnapshotInput>) + Send + Sync + 'static,
    {
        Ok(self.db.snapshot_with_callback(callback)?)
    }

    /// Create a snapshot and wait for its callback, without polling.
    pub fn snapshot_and_wait(&self) -> Result<ShardSnapshotInput> {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.snapshot_with_callback(move |result| {
            let _ = sender.send(result);
        })?;
        receiver
            .recv()
            .map_err(|err| TableError::internal(format!("snapshot callback disconnected: {err}")))?
            .map_err(Into::into)
    }

    pub fn shard_snapshot_input(&self, snapshot_id: u64) -> Result<ShardSnapshotInput> {
        Ok(self.db.shard_snapshot_input(snapshot_id)?)
    }

    /// Close this writer after all owned scan iterators have been dropped.
    pub fn close(&self) -> Result<()> {
        Ok(self.db.close()?)
    }
}

/// Builder for a standalone typed reader pinned to one snapshot selection.
pub struct TableReaderBuilder {
    config: Config,
    table_name: Option<String>,
    selection: Option<TableReaderSelection>,
}

enum TableReaderSelection {
    Shard { db_id: String, snapshot_id: u64 },
    Global { snapshot_id: u64 },
    CurrentGlobal,
}

impl TableReaderBuilder {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            table_name: None,
            selection: None,
        }
    }

    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = Some(table_name.into());
        self
    }

    pub fn shard_snapshot(mut self, db_id: impl Into<String>, snapshot_id: u64) -> Self {
        self.selection = Some(TableReaderSelection::Shard {
            db_id: db_id.into(),
            snapshot_id,
        });
        self
    }

    pub fn global_snapshot(mut self, snapshot_id: u64) -> Self {
        self.selection = Some(TableReaderSelection::Global { snapshot_id });
        self
    }

    /// Capture CURRENT once and freeze that manifest as a fixed view.
    pub fn current_global_snapshot(mut self) -> Self {
        self.selection = Some(TableReaderSelection::CurrentGlobal);
        self
    }

    pub fn open(self) -> Result<TableReader> {
        let name = self
            .table_name
            .ok_or_else(|| {
                TableError::InvalidSchema("TableReaderBuilder requires table_name".into())
            })
            .and_then(validate_name)?;
        match self.selection.ok_or_else(|| {
            TableError::InvalidSchema("TableReaderBuilder requires a snapshot selection".into())
        })? {
            TableReaderSelection::Shard { db_id, snapshot_id } => {
                let db = Arc::new(ReadOnlyDb::open_with_db_id(
                    self.config,
                    snapshot_id,
                    db_id,
                )?);
                TableReader::from_shard(db, name)
            }
            TableReaderSelection::Global { snapshot_id } => {
                let reader = Reader::open(ReaderConfig::from_config(&self.config), snapshot_id)?;
                TableReader::from_global(reader, name)
            }
            TableReaderSelection::CurrentGlobal => {
                let reader_config = ReaderConfig::from_config(&self.config);
                let mut reader = Reader::open_current(reader_config)?;
                reader.pin_current_snapshot();
                TableReader::from_global(reader, name)
            }
        }
    }
}

/// An owned typed reader fixed to either a shard or a global snapshot.
pub struct TableReader {
    name: String,
    compiled: Arc<crate::table::CompiledTable>,
    backend: Arc<RuntimeReadBackend>,
    read_options: ReadOptions,
    scan_options: ScanOptions,
}

impl TableReader {
    fn from_shard(db: Arc<ReadOnlyDb>, name: String) -> Result<Self> {
        let metadata = load_table_metadata(&db.current_schema(), &name)?;
        let compiled = compile_table(metadata, db.total_buckets())?;
        Ok(Self::new(
            name,
            compiled,
            Arc::new(RuntimeReadBackend::Shard(db)),
        ))
    }

    fn from_global(mut reader: Reader, name: String) -> Result<Self> {
        let manifest = reader.current_global_snapshot();
        let bucket = manifest
            .shard_snapshots
            .iter()
            .find_map(|shard| shard.ranges.first().map(|range| *range.start()))
            .ok_or_else(|| {
                TableError::InvalidSchema("global snapshot has no shard buckets".into())
            })?;
        let total_buckets = manifest.total_buckets;
        let metadata = load_table_metadata(reader.schema_for_bucket(bucket)?.as_ref(), &name)?;
        let compiled = compile_table(metadata.clone(), total_buckets)?;
        let state_name = name.clone();
        Ok(Self::new(
            name,
            compiled,
            Arc::new(RuntimeReadBackend::Global {
                state: Arc::new(Mutex::new(GlobalReaderState {
                    reader,
                    name: state_name,
                    expected_metadata: metadata,
                    validated_buckets: vec![false; total_buckets as usize],
                })),
            }),
        ))
    }

    fn new(
        name: String,
        compiled: Arc<crate::table::CompiledTable>,
        backend: Arc<RuntimeReadBackend>,
    ) -> Self {
        Self {
            name: name.clone(),
            compiled,
            backend,
            read_options: ReadOptions::default().with_column_family(name.clone()),
            scan_options: ScanOptions::default().with_column_family(name),
        }
    }

    pub fn schema(&self) -> &TableSchema {
        &self.compiled.schema
    }

    pub fn key_builder(&self) -> TableKeyBuilder {
        TableKeyBuilder {
            compiled: Arc::clone(&self.compiled),
            values: Vec::with_capacity(self.compiled.key_positions.len()),
        }
    }

    pub fn get(&self, key: &TableKey) -> Result<Option<Vec<Value>>> {
        self.backend
            .get(key.inner.bucket, &key.inner.encoded, &self.read_options)?
            .map(|columns| {
                assemble_row_from_key_values(&self.compiled, &key.inner.values, &columns)
            })
            .transpose()
    }

    pub fn multi_get(&self, keys: &[TableKey]) -> Result<Vec<Option<Vec<Value>>>> {
        let requests = keys
            .iter()
            .map(|key| (key.inner.bucket, key.inner.encoded.as_slice()))
            .collect::<Vec<_>>();
        self.backend
            .multi_get(&requests, &self.read_options)?
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

    pub fn scan(&self, bucket: u16) -> Result<TableScan> {
        self.scan_bounds(bucket, None, None)
    }

    pub fn scan_bounds(
        &self,
        bucket: u16,
        start: Option<&TableKey>,
        end: Option<&TableKey>,
    ) -> Result<TableScan> {
        validate_bound(bucket, start)?;
        validate_bound(bucket, end)?;
        Ok(TableScan {
            inner: self.backend.scan(
                bucket,
                start.map(|key| key.inner.encoded.as_slice()),
                end.map(|key| key.inner.encoded.as_slice()),
                &self.scan_options,
            )?,
            _writer_backend: None,
            compiled: Arc::clone(&self.compiled),
        })
    }

    pub fn project_by_names<S: AsRef<str>>(
        &self,
        field_names: &[S],
    ) -> Result<crate::TableProjection<'static>> {
        crate::TableProjection::from_runtime(
            Arc::clone(&self.backend),
            self.name.clone(),
            Arc::clone(&self.compiled),
            field_names,
        )
    }
}

pub(crate) enum RuntimeReadBackend {
    Writer(Arc<Db>),
    Shard(Arc<ReadOnlyDb>),
    // Core Reader routing and cache access are serialized for this simple
    // pinned table view; shard-only readers remain fully concurrent.
    Global {
        state: Arc<Mutex<GlobalReaderState>>,
    },
}

pub(crate) struct GlobalReaderState {
    reader: Reader,
    name: String,
    expected_metadata: crate::metadata::TableMetadata,
    validated_buckets: Vec<bool>,
}

impl RuntimeReadBackend {
    pub(crate) fn writer_backend(&self) -> Option<Arc<Db>> {
        match self {
            Self::Writer(db) => Some(Arc::clone(db)),
            Self::Shard(_) | Self::Global { .. } => None,
        }
    }

    pub(crate) fn get(
        &self,
        bucket: u16,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<bytes::Bytes>>>> {
        Ok(match self {
            Self::Writer(db) => db.get_with_options(bucket, key, options)?,
            Self::Shard(db) => db.get_with_options(bucket, key, options)?,
            Self::Global { state } => {
                let mut state = lock_global_state(state)?;
                validate_global_bucket(&mut state, bucket)?;
                state.reader.get_with_options(bucket, key, options)?
            }
        })
    }

    pub(crate) fn multi_get(
        &self,
        keys: &[(u16, &[u8])],
        options: &ReadOptions,
    ) -> Result<Vec<Option<Vec<Option<bytes::Bytes>>>>> {
        Ok(match self {
            Self::Writer(db) => db.multi_get_with_options(keys, options)?,
            Self::Shard(db) => db.multi_get_with_options(keys, options)?,
            Self::Global { state } => {
                let mut state = lock_global_state(state)?;
                for (bucket, _) in keys {
                    validate_global_bucket(&mut state, *bucket)?;
                }
                state.reader.multi_get_with_options(keys, options)?
            }
        })
    }

    pub(crate) fn scan(
        &self,
        bucket: u16,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator> {
        Ok(match self {
            Self::Writer(db) => db.scan_with_options_bounds(bucket, start, end, options)?,
            Self::Shard(db) => db.scan_with_options_bounds(bucket, start, end, options)?,
            Self::Global { state } => {
                let mut state = lock_global_state(state)?;
                validate_global_bucket(&mut state, bucket)?;
                state
                    .reader
                    .scan_with_options_bounds(bucket, start, end, options)?
            }
        })
    }
}

fn lock_global_state(
    state: &Mutex<GlobalReaderState>,
) -> Result<std::sync::MutexGuard<'_, GlobalReaderState>> {
    state
        .lock()
        .map_err(|_| TableError::internal("table reader core lock poisoned"))
}

fn validate_global_bucket(state: &mut GlobalReaderState, bucket: u16) -> Result<()> {
    let bucket_index = usize::from(bucket);
    if state
        .validated_buckets
        .get(bucket_index)
        .copied()
        .unwrap_or(false)
    {
        return Ok(());
    }
    let schema = state.reader.schema_for_bucket(bucket)?;
    let metadata = load_table_metadata(&schema, &state.name)?;
    if metadata != state.expected_metadata {
        return Err(TableError::InvalidSchema(format!(
            "table '{}' has incompatible schema in bucket {bucket}",
            state.name
        )));
    }
    let shard = state
        .reader
        .current_global_snapshot()
        .shard_snapshots
        .iter()
        .find(|shard| shard.ranges.iter().any(|range| range.contains(&bucket)))
        .ok_or_else(|| {
            TableError::InvalidSchema(format!("bucket {bucket} has no shard snapshot"))
        })?;
    for range in &shard.ranges {
        for bucket in *range.start()..=*range.end() {
            state.validated_buckets[usize::from(bucket)] = true;
        }
    }
    Ok(())
}
