use crate::catalog::{TableId, physical_table_name};
use crate::metadata::TableMetadata;
use crate::table::{TypedRead, load_table_metadata, load_table_metadata_for_shard, validate_name};
use crate::{
    ReadOnlyTable, Result, Table, TableError, TableKey, TableKeyBuilder, TableProjection,
    TableScan, TableSchema, TableWritePlan, Value,
};
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
    validate_catalog_binding(&metadata, Some(table_id))?;
    Table::from_metadata(db, name, metadata)
}

/// A typed global snapshot read proxy.
pub struct TableReader {
    typed: TypedRead,
}

impl TableReader {
    /// Open and pin the current snapshot carried by this core reader.
    pub fn open(mut reader: Reader, name: impl Into<String>) -> Result<Self> {
        reader.pin_current_snapshot();
        let name = validate_name(name.into())?;
        let metadata = global_metadata(&reader, &name)?;
        Self::from_metadata(reader, name, metadata)
    }

    fn from_metadata(reader: Reader, name: String, metadata: TableMetadata) -> Result<Self> {
        let state = Arc::new(GlobalReaderState::new(
            reader,
            name.clone(),
            metadata.clone(),
        ));
        Ok(Self {
            typed: TypedRead::from_global_metadata(state, name, metadata)?,
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

    /// Build a portable full-scan plan pinned to this reader's current snapshot.
    pub fn scan_plan(&self) -> Result<crate::TableScanPlan> {
        let state = self
            .typed
            .global_state()
            .ok_or_else(|| TableError::internal("table reader is missing its global read state"))?;
        state.scan_plan()
    }
}

/// Builder for a global typed reader pinned to one snapshot selection.
pub struct TableReaderBuilder {
    config: Config,
    table_name: Option<String>,
    selection: Option<TableReaderSelection>,
    catalog_table_id: Option<TableId>,
}

enum TableReaderSelection {
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
        let mut reader = match self.selection.ok_or_else(|| {
            TableError::InvalidSchema("TableReaderBuilder requires a snapshot selection".into())
        })? {
            TableReaderSelection::Global { snapshot_id } => {
                Reader::open(ReaderConfig::from_config(&self.config), snapshot_id)?
            }
            TableReaderSelection::CurrentGlobal => {
                let reader_config = ReaderConfig::from_config(&self.config);
                Reader::open_current(reader_config)?
            }
        };
        reader.pin_current_snapshot();
        let metadata = global_metadata(&reader, &name)?;
        validate_catalog_binding(&metadata, self.catalog_table_id)?;
        TableReader::from_metadata(reader, name, metadata)
    }

    fn required_table_name(&self) -> Result<String> {
        required_reader_table_name(&self.table_name, self.catalog_table_id)
    }
}

/// Builder for a typed table over one shard snapshot.
pub struct ReadOnlyTableBuilder {
    config: Config,
    table_name: Option<String>,
    snapshot: Option<(String, u64)>,
    catalog_table_id: Option<TableId>,
}

impl ReadOnlyTableBuilder {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            table_name: None,
            snapshot: None,
            catalog_table_id: None,
        }
    }

    pub(crate) fn from_catalog(config: Config, table_name: String, table_id: TableId) -> Self {
        Self {
            config,
            table_name: Some(table_name),
            snapshot: None,
            catalog_table_id: Some(table_id),
        }
    }

    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = Some(table_name.into());
        self
    }

    pub fn shard_snapshot(mut self, db_id: impl Into<String>, snapshot_id: u64) -> Self {
        self.snapshot = Some((db_id.into(), snapshot_id));
        self
    }

    pub fn open(self) -> Result<ReadOnlyTable> {
        let name = self.required_table_name()?;
        let (db_id, snapshot_id) = self.snapshot.ok_or_else(|| {
            TableError::InvalidSchema(
                "ReadOnlyTableBuilder requires a shard snapshot selection".into(),
            )
        })?;
        let db = Arc::new(ReadOnlyDb::open_with_db_id(
            self.config,
            snapshot_id,
            db_id,
        )?);
        let metadata = load_table_metadata(&db.current_schema(), &name)?;
        validate_catalog_binding(&metadata, self.catalog_table_id)?;
        ReadOnlyTable::from_shard_metadata(db, name, metadata)
    }

    fn required_table_name(&self) -> Result<String> {
        required_reader_table_name(&self.table_name, self.catalog_table_id)
    }
}

fn required_reader_table_name(
    table_name: &Option<String>,
    catalog_table_id: Option<TableId>,
) -> Result<String> {
    let name = table_name
        .clone()
        .ok_or_else(|| TableError::InvalidSchema("table reader requires table_name".into()))
        .and_then(validate_name)?;
    if let Some(table_id) = catalog_table_id
        && name != physical_table_name(table_id)
    {
        return Err(TableError::InvalidSchema(
            "catalog-bound table reader cannot change table_name".into(),
        ));
    }
    Ok(name)
}

pub(crate) struct GlobalReaderState {
    total_buckets: u32,
    state: Mutex<GlobalReaderInner>,
}

struct GlobalReaderInner {
    reader: Reader,
    name: String,
    metadata: TableMetadata,
}

impl GlobalReaderState {
    fn new(reader: Reader, name: String, metadata: TableMetadata) -> Self {
        let total_buckets = reader.current_global_snapshot().total_buckets;
        Self {
            total_buckets,
            state: Mutex::new(GlobalReaderInner {
                reader,
                name,
                metadata,
            }),
        }
    }

    pub(crate) fn total_buckets(&self) -> u32 {
        self.total_buckets
    }

    pub(crate) fn get(
        &self,
        bucket: u16,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<bytes::Bytes>>>> {
        let mut state = lock_global_state(&self.state)?;
        Ok(state.reader.get_with_options(bucket, key, options)?)
    }

    pub(crate) fn multi_get(
        &self,
        keys: &[(u16, &[u8])],
        options: &ReadOptions,
    ) -> Result<Vec<Option<Vec<Option<bytes::Bytes>>>>> {
        let mut state = lock_global_state(&self.state)?;
        Ok(state.reader.multi_get_with_options(keys, options)?)
    }

    pub(crate) fn scan(
        &self,
        bucket: u16,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator> {
        let mut state = lock_global_state(&self.state)?;
        Ok(state
            .reader
            .scan_with_options_bounds(bucket, start, end, options)?)
    }

    pub(crate) fn scan_plan(&self) -> Result<crate::TableScanPlan> {
        let state = lock_global_state(&self.state)?;
        let snapshot = state.reader.current_global_snapshot().clone();
        crate::TableScanPlan::from_global_reader(
            state.name.clone(),
            state.metadata.clone(),
            snapshot.id,
            snapshot.total_buckets,
            snapshot.shard_snapshots,
            state.reader.config().clone(),
        )
    }
}

fn global_metadata(reader: &Reader, name: &str) -> Result<TableMetadata> {
    let shard = reader
        .current_global_snapshot()
        .shard_snapshots
        .iter()
        .find(|shard| !shard.ranges.is_empty())
        .ok_or_else(|| TableError::InvalidSchema("global snapshot has no shard buckets".into()))?;
    load_table_metadata_for_shard(reader.config(), shard, name)
}

fn lock_global_state(
    state: &Mutex<GlobalReaderInner>,
) -> Result<std::sync::MutexGuard<'_, GlobalReaderInner>> {
    state
        .lock()
        .map_err(|_| TableError::internal("table reader core lock poisoned"))
}

fn validate_catalog_binding(
    metadata: &crate::metadata::TableMetadata,
    table_id: Option<TableId>,
) -> Result<()> {
    let Some(table_id) = table_id else {
        return Ok(());
    };
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
