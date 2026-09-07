use crate::catalog::{TableId, physical_table_name};
use crate::table::{
    TableKey, TableKeyBuilder, TableScan, assemble_row_from_key_values, compile_table,
    load_table_metadata, validate_bound, validate_name,
};
use crate::{Result, Table, TableError, TableSchema, TableWritePlan, Value};
use cobble::{
    Config, Db, DbBuilder, DbIterator, ReadOnlyDb, ReadOptions, Reader, ReaderConfig, ScanOptions,
};
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};

/// Builder for a standalone writable typed table shard.
pub struct TableWriterBuilder {
    config: Config,
    table_name: Option<String>,
    db_id: Option<String>,
    bucket_ranges: Vec<RangeInclusive<u16>>,
    catalog_binding: Option<(TableWritePlan, Config)>,
}

impl TableWriterBuilder {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            table_name: None,
            db_id: None,
            bucket_ranges: Vec::new(),
            catalog_binding: None,
        }
    }

    pub(crate) fn from_write_plan(
        config: Config,
        table_name: String,
        plan: TableWritePlan,
        catalog_store_config: Config,
    ) -> Self {
        Self {
            config,
            table_name: Some(table_name),
            db_id: None,
            bucket_ranges: Vec::new(),
            catalog_binding: Some((plan, catalog_store_config)),
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
    pub fn create(self, schema: TableSchema) -> Result<Table> {
        if self.catalog_binding.is_some() {
            return Err(TableError::InvalidSchema(
                "catalog-bound TableWriterBuilder requires open()".into(),
            ));
        }
        let name = self.required_table_name()?;
        let db = Arc::new(self.db_builder().open()?);
        Table::create(db, name, schema)
    }

    /// Open a new writable shard for a catalog-bound table.
    pub fn open(self) -> Result<Table> {
        let (plan, store_config) = self.catalog_binding.clone().ok_or_else(|| {
            TableError::InvalidSchema("TableWriterBuilder::open requires a catalog table".into())
        })?;
        self.required_table_name()?;
        let db = Arc::new(self.db_builder().open()?);
        open_materialized_catalog_table(db, &plan, &store_config)
    }

    /// Resume a writable shard, materializing the loaded catalog definition when bound.
    pub fn resume(self) -> Result<Table> {
        let name = self.required_table_name()?;
        let db = Arc::new(self.db_builder().resume()?);
        match self.catalog_binding {
            Some((plan, store_config)) => open_materialized_catalog_table(db, &plan, &store_config),
            None => Table::open(db, name),
        }
    }

    /// Open a writable shard at a selected snapshot boundary.
    pub fn open_from_snapshot(self, snapshot_id: u64) -> Result<Table> {
        let name = self.required_table_name()?;
        let db = Arc::new(self.db_builder().open_from_snapshot(snapshot_id)?);
        match self.catalog_binding {
            Some((plan, _)) => open_catalog_snapshot(db, name, plan.table_id()),
            None => Table::open(db, name),
        }
    }

    /// Resume a writable shard from a selected snapshot boundary.
    pub fn resume_from_snapshot(self, snapshot_id: u64) -> Result<Table> {
        let name = self.required_table_name()?;
        let db = Arc::new(self.db_builder().resume_from_snapshot(snapshot_id)?);
        match self.catalog_binding {
            Some((plan, _)) => open_catalog_snapshot(db, name, plan.table_id()),
            None => Table::open(db, name),
        }
    }

    fn required_table_name(&self) -> Result<String> {
        let name = self
            .table_name
            .clone()
            .ok_or_else(|| {
                TableError::InvalidSchema("TableWriterBuilder requires table_name".into())
            })
            .and_then(validate_name)?;
        if let Some((plan, _)) = &self.catalog_binding
            && name != physical_table_name(plan.table_id())
        {
            return Err(TableError::InvalidSchema(
                "catalog-bound TableWriterBuilder cannot change table_name".into(),
            ));
        }
        Ok(name)
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

fn open_materialized_catalog_table(
    db: Arc<Db>,
    plan: &TableWritePlan,
    store_config: &Config,
) -> Result<Table> {
    let (name, metadata) = crate::catalog::materialize_write_plan(store_config, db.as_ref(), plan)?;
    Table::from_metadata(db, name, metadata)
}

fn open_catalog_snapshot(db: Arc<Db>, name: String, table_id: TableId) -> Result<Table> {
    let metadata = load_table_metadata(&db.current_schema(), &name)?;
    validate_catalog_binding(&metadata, table_id)?;
    Table::from_metadata(db, name, metadata)
}

/// Builder for a standalone typed reader pinned to one snapshot selection.
pub struct TableReaderBuilder {
    config: Config,
    table_name: Option<String>,
    selection: Option<TableReaderSelection>,
    catalog_table_id: Option<TableId>,
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
            catalog_table_id: None,
        }
    }

    pub(crate) fn from_catalog(config: Config, table_name: String, table_id: TableId) -> Self {
        Self {
            config,
            table_name: Some(table_name),
            selection: None,
            catalog_table_id: Some(table_id),
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
        let name = self.required_table_name()?;
        match self.selection.ok_or_else(|| {
            TableError::InvalidSchema("TableReaderBuilder requires a snapshot selection".into())
        })? {
            TableReaderSelection::Shard { db_id, snapshot_id } => {
                let db = Arc::new(ReadOnlyDb::open_with_db_id(
                    self.config,
                    snapshot_id,
                    db_id,
                )?);
                TableReader::from_shard(db, name, self.catalog_table_id)
            }
            TableReaderSelection::Global { snapshot_id } => {
                let reader = Reader::open(ReaderConfig::from_config(&self.config), snapshot_id)?;
                TableReader::from_global(reader, name, self.catalog_table_id)
            }
            TableReaderSelection::CurrentGlobal => {
                let reader_config = ReaderConfig::from_config(&self.config);
                let mut reader = Reader::open_current(reader_config)?;
                reader.pin_current_snapshot();
                TableReader::from_global(reader, name, self.catalog_table_id)
            }
        }
    }

    fn required_table_name(&self) -> Result<String> {
        let name = self
            .table_name
            .clone()
            .ok_or_else(|| {
                TableError::InvalidSchema("TableReaderBuilder requires table_name".into())
            })
            .and_then(validate_name)?;
        if let Some(table_id) = self.catalog_table_id
            && name != physical_table_name(table_id)
        {
            return Err(TableError::InvalidSchema(
                "catalog-bound TableReaderBuilder cannot change table_name".into(),
            ));
        }
        Ok(name)
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
    fn from_shard(
        db: Arc<ReadOnlyDb>,
        name: String,
        catalog_table_id: Option<TableId>,
    ) -> Result<Self> {
        let metadata = load_table_metadata(&db.current_schema(), &name)?;
        if let Some(table_id) = catalog_table_id {
            validate_catalog_binding(&metadata, table_id)?;
        }
        let compiled = compile_table(metadata, db.total_buckets())?;
        Ok(Self::new(
            name,
            compiled,
            Arc::new(RuntimeReadBackend::Shard(db)),
        ))
    }

    fn from_global(
        mut reader: Reader,
        name: String,
        catalog_table_id: Option<TableId>,
    ) -> Result<Self> {
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
        if let Some(table_id) = catalog_table_id {
            validate_catalog_binding(&metadata, table_id)?;
        }
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
    pub(crate) fn get(
        &self,
        bucket: u16,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<bytes::Bytes>>>> {
        Ok(match self {
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

fn validate_catalog_binding(
    metadata: &crate::metadata::TableMetadata,
    table_id: TableId,
) -> Result<()> {
    let binding = metadata.catalog_binding.ok_or_else(|| {
        TableError::InvalidSchema("table metadata is not bound to a catalog table".into())
    })?;
    if binding.table_id != table_id {
        return Err(TableError::InvalidSchema(
            "table metadata belongs to another catalog table".into(),
        ));
    }
    Ok(())
}
