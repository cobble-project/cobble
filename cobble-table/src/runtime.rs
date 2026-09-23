use crate::catalog::{TableId, physical_table_name};
use crate::metadata::TableMetadata;
use crate::table::{
    TypedRead, load_table_metadata, load_table_metadata_for_shard,
    load_table_metadata_from_snapshot, validate_name,
};
use crate::transform::TABLE_TRANSFORM_TYPE;
use crate::{
    ReadOnlyTable, Result, Table, TableError, TableKey, TableKeyBuilder, TableProjection,
    TableScan, TableSchema, TableWritePlan, Value, register_schema_transforms,
};
use arc_swap::{ArcSwap, Guard};
use bytes::Bytes;
use cobble::{
    Config, Db, DbBuilder, DbGovernance, DbIterator, FileSystemDbGovernance, GovernanceMode,
    NoopDbGovernance, ReadOnlyDbBuilder, ReadOptions, Reader, ReaderBuilder, ReaderConfig,
    ScanOptions, SchemaTransformRegistrar, VolumeUsageKind, bucket_snapshot_manifest_path,
};
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use url::Url;

type SchemaTransformCallback =
    Box<dyn Fn(Option<Bytes>) -> cobble::Result<Option<Bytes>> + Send + Sync>;
type SchemaTransformFactory =
    Arc<dyn Fn(&[u8]) -> cobble::Result<SchemaTransformCallback> + Send + Sync>;

#[derive(Default)]
pub(crate) struct TableSchemaTransformFactories(Vec<(String, SchemaTransformFactory)>);

impl TableSchemaTransformFactories {
    pub(crate) fn register<F, T>(
        &mut self,
        transform_type: impl Into<String>,
        factory: F,
    ) -> Result<()>
    where
        F: Fn(&[u8]) -> cobble::Result<T> + Send + Sync + 'static,
        T: Fn(Option<Bytes>) -> cobble::Result<Option<Bytes>> + Send + Sync + 'static,
    {
        let transform_type = transform_type.into();
        if transform_type == TABLE_TRANSFORM_TYPE {
            return Err(TableError::Storage(cobble::Error::InvalidState(format!(
                "Schema transform '{TABLE_TRANSFORM_TYPE}' is reserved by cobble-table"
            ))));
        }
        if transform_type.trim().is_empty() {
            return Err(TableError::Storage(cobble::Error::InvalidState(
                "Schema transform type must not be empty".to_string(),
            )));
        }
        if self
            .0
            .iter()
            .any(|(existing, _)| existing == &transform_type)
        {
            return Err(TableError::Storage(cobble::Error::InvalidState(format!(
                "Schema transform '{}' is already registered",
                transform_type
            ))));
        }
        let factory: SchemaTransformFactory = Arc::new(move |spec| Ok(Box::new(factory(spec)?)));
        self.0.push((transform_type, factory));
        Ok(())
    }

    pub(crate) fn apply_to<B: SchemaTransformRegistrar>(&self, builder: B) -> Result<B> {
        register_schema_transforms(&builder)?;
        for (transform_type, factory) in &self.0 {
            let factory = Arc::clone(factory);
            builder.register_schema_transform(transform_type.clone(), move |spec| factory(spec))?;
        }
        Ok(builder)
    }
}

/// Builder for a standalone writable typed table shard.
pub struct TableWriterBuilder {
    config: Config,
    table_name: Option<String>,
    bucket: Option<u16>,
    catalog_binding: Option<(TableWritePlan, Config)>,
    transforms: TableSchemaTransformFactories,
}

impl TableWriterBuilder {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            table_name: None,
            bucket: None,
            catalog_binding: None,
            transforms: TableSchemaTransformFactories::default(),
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
            bucket: None,
            catalog_binding: Some((plan, catalog_store_config)),
            transforms: TableSchemaTransformFactories::default(),
        }
    }

    /// Select the physical column-family name for this typed table.
    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = Some(table_name.into());
        self
    }

    /// Own exactly one physical bucket using the stable database identity `bucket-N`.
    ///
    /// The writer keeps a local-filesystem advisory lock for that database until its underlying
    /// [`Db`] is dropped.
    pub fn bucket(mut self, bucket: u16) -> Self {
        self.bucket = Some(bucket);
        self
    }

    /// Register a factory for persisted schema transform specifications before opening.
    pub fn register_schema_transform<F, T>(
        mut self,
        transform_type: impl Into<String>,
        factory: F,
    ) -> Result<Self>
    where
        F: Fn(&[u8]) -> cobble::Result<T> + Send + Sync + 'static,
        T: Fn(Option<Bytes>) -> cobble::Result<Option<Bytes>> + Send + Sync + 'static,
    {
        self.transforms.register(transform_type, factory)?;
        Ok(self)
    }

    /// Initialize the bucket's empty baseline, then create or reopen this standalone table.
    ///
    /// A subsequent call starts again from that empty baseline, for overwrite semantics.
    pub fn create(self, schema: TableSchema) -> Result<Table> {
        if self.catalog_binding.is_some() {
            return Err(TableError::InvalidSchema(
                "catalog-bound TableWriterBuilder requires open()".into(),
            ));
        }
        let name = self.required_table_name()?;
        let bucket = self.required_bucket()?;
        let (db, needs_baseline) =
            self.open_or_resume_bucket_baseline(&name, Some(&schema), None)?;
        let table = Table::create(db, name, schema)?;
        if needs_baseline {
            ensure_empty_baseline(&table)?;
        } else {
            ensure_empty_bucket(&table, bucket)?;
        }
        Ok(table)
    }

    /// Initialize the bucket's empty baseline, then open a writable catalog-bound table.
    ///
    /// A subsequent call starts again from that empty baseline, for overwrite semantics.
    pub fn open(self) -> Result<Table> {
        let (plan, store_config) = self.catalog_binding.as_ref().ok_or_else(|| {
            TableError::InvalidSchema("TableWriterBuilder::open requires a catalog table".into())
        })?;
        let name = self.required_table_name()?;
        let bucket = self.required_bucket()?;
        let (db, needs_baseline) =
            self.open_or_resume_bucket_baseline(&name, None, Some(plan.table_id()))?;
        let table = open_materialized_catalog_table(db, plan, store_config)?;
        if needs_baseline {
            ensure_empty_baseline(&table)?;
        } else {
            ensure_empty_bucket(&table, bucket)?;
        }
        Ok(table)
    }

    /// Resume a writable shard from a selected snapshot boundary.
    pub fn resume_from_snapshot(self, snapshot_id: u64) -> Result<Table> {
        let name = self.required_table_name()?;
        let db = self.resume_bucket_snapshot(
            snapshot_id,
            &name,
            self.catalog_binding
                .as_ref()
                .map(|(plan, _)| plan.table_id()),
        )?;
        match self.catalog_binding {
            Some((plan, store_config)) => open_materialized_catalog_table(db, &plan, &store_config),
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

    fn required_bucket(&self) -> Result<u16> {
        self.bucket
            .ok_or_else(|| TableError::InvalidSchema("TableWriterBuilder requires bucket".into()))
    }

    fn bucket_config(&self) -> Result<Config> {
        let bucket = self.required_bucket()?;
        if !(1..=u16::MAX as u32 + 1).contains(&self.config.total_buckets)
            || u32::from(bucket) >= self.config.total_buckets
        {
            return Err(TableError::InvalidSchema(
                "single bucket must be in [0, total_buckets), with total_buckets in 1..=65536"
                    .into(),
            ));
        }
        let mut config = self.config.clone();
        config.wal_enabled = false;
        if config.snapshot_retention.is_some() && !config.snapshot_only_track {
            return Err(TableError::InvalidSchema(
                "single-bucket writers require snapshot retention to be disabled".into(),
            ));
        }
        Ok(config)
    }

    fn open_or_resume_bucket_baseline(
        &self,
        table_name: &str,
        standalone_schema: Option<&TableSchema>,
        catalog_table_id: Option<crate::catalog::TableId>,
    ) -> Result<(Arc<Db>, bool)> {
        let bucket = self.required_bucket()?;
        let governance = self.bucket_governance(bucket)?;
        if bucket_snapshot_exists(&self.config, bucket, 0)? {
            self.validate_bucket_snapshot(0, table_name, standalone_schema, catalog_table_id)?;
            let db = Arc::new(
                self.bucket_db_builder(governance)?
                    .resume_from_snapshot(0)?,
            );
            return Ok((db, false));
        }
        if bucket_has_persisted_state(&self.config, bucket)? {
            return Err(TableError::Storage(cobble::Error::InvalidState(format!(
                "{} has persisted state but no empty baseline snapshot 0",
                bucket_db_id(bucket)
            ))));
        }
        let db = Arc::new(self.bucket_db_builder(governance)?.open()?);
        Ok((db, true))
    }

    fn resume_bucket_snapshot(
        &self,
        snapshot_id: u64,
        table_name: &str,
        catalog_table_id: Option<crate::catalog::TableId>,
    ) -> Result<Arc<Db>> {
        let bucket = self.required_bucket()?;
        let governance = self.bucket_governance(bucket)?;
        self.validate_bucket_snapshot(snapshot_id, table_name, None, catalog_table_id)?;
        Ok(Arc::new(
            self.bucket_db_builder(governance)?
                .resume_from_snapshot(snapshot_id)?,
        ))
    }

    fn bucket_governance(&self, bucket: u16) -> Result<Arc<dyn DbGovernance>> {
        self.bucket_config()?;
        Ok(Arc::new(LockedDbGovernance::new(
            &self.config,
            bucket,
            default_governance(&self.config)?,
        )?))
    }

    fn bucket_db_builder(&self, governance: Arc<dyn DbGovernance>) -> Result<DbBuilder> {
        let bucket = self.required_bucket()?;
        let config = self.bucket_config()?;
        self.transforms.apply_to(
            DbBuilder::new(config)
                .bucket_ranges(vec![bucket..=bucket])
                .db_id(bucket_db_id(bucket))
                .governance(governance),
        )
    }

    fn validate_bucket_snapshot(
        &self,
        snapshot_id: u64,
        table_name: &str,
        standalone_schema: Option<&TableSchema>,
        catalog_table_id: Option<crate::catalog::TableId>,
    ) -> Result<()> {
        let bucket = self.required_bucket()?;
        let config = self.bucket_config()?;
        let db_id = bucket_db_id(bucket);
        let manifest_path = bucket_snapshot_manifest_path(&db_id, snapshot_id);
        let absolute_manifest = configured_local_meta_root(&config)?.join(manifest_path);
        if !absolute_manifest.exists() {
            return Err(TableError::InvalidSchema(format!(
                "single-bucket writer is missing snapshot {snapshot_id} for {}",
                bucket_db_id(bucket)
            )));
        }
        let snapshot = cobble::load_shard_snapshot_metadata(
            &config,
            &db_id,
            &absolute_manifest.to_string_lossy(),
        )?;
        if snapshot.snapshot_id != snapshot_id || snapshot.ranges.as_slice() != [bucket..=bucket] {
            return Err(TableError::InvalidSchema(format!(
                "single-bucket manifest must cover exactly bucket {bucket} at snapshot {snapshot_id}"
            )));
        }
        if snapshot_id == 0 && snapshot.data_size_bytes != 0 {
            return Err(TableError::InvalidSchema(
                "single-bucket baseline snapshot 0 must not contain data".into(),
            ));
        }
        let metadata = load_table_metadata_from_snapshot(&snapshot, table_name)?;
        if let Some(schema) = standalone_schema
            && (metadata.catalog_binding.is_some() || metadata.schema != *schema)
        {
            return Err(TableError::InvalidSchema(format!(
                "column family '{table_name}' is not this standalone table"
            )));
        }
        validate_catalog_binding(&metadata, catalog_table_id)
    }
}

fn bucket_db_id(bucket: u16) -> String {
    format!("bucket-{bucket}")
}

fn ensure_empty_baseline(table: &Table) -> Result<()> {
    let snapshot = table.snapshot_and_wait()?;
    if snapshot.snapshot_id != 0 {
        return Err(TableError::Storage(cobble::Error::InvalidState(format!(
            "new single-bucket writer must create empty baseline snapshot 0, got {}",
            snapshot.snapshot_id
        ))));
    }
    Ok(())
}

fn ensure_empty_bucket(table: &Table, bucket: u16) -> Result<()> {
    if table.scan(bucket)?.next().transpose()?.is_some() {
        return Err(TableError::InvalidSchema(
            "single-bucket baseline snapshot 0 must not contain table rows".into(),
        ));
    }
    Ok(())
}

fn default_governance(config: &Config) -> cobble::Result<Arc<dyn DbGovernance>> {
    match config.governance_mode {
        GovernanceMode::Filesystem => Ok(Arc::new(FileSystemDbGovernance::from_config(config)?)),
        GovernanceMode::Noop => Ok(Arc::new(NoopDbGovernance)),
    }
}

struct LockedDbGovernance {
    inner: Arc<dyn DbGovernance>,
    _lock: LocalBucketLock,
}

impl LockedDbGovernance {
    fn new(config: &Config, bucket: u16, inner: Arc<dyn DbGovernance>) -> cobble::Result<Self> {
        Ok(Self {
            inner,
            _lock: LocalBucketLock::acquire(config, bucket)?,
        })
    }
}

impl DbGovernance for LockedDbGovernance {
    fn register_db(
        &self,
        db_id: &str,
        ranges: &[RangeInclusive<u16>],
        total_buckets: u32,
    ) -> cobble::Result<()> {
        self.inner.register_db(db_id, ranges, total_buckets)
    }

    fn unregister_db(&self, db_id: &str) -> cobble::Result<()> {
        self.inner.unregister_db(db_id)
    }
}

struct LocalBucketLock {
    _file: File,
}

impl LocalBucketLock {
    fn acquire(config: &Config, bucket: u16) -> cobble::Result<Self> {
        let root = local_meta_root(config)?;
        let bucket_dir = root.join(bucket_db_id(bucket));
        std::fs::create_dir_all(&bucket_dir).map_err(lock_io_error)?;
        let bucket_dir = std::fs::canonicalize(&bucket_dir).map_err(lock_io_error)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(bucket_dir.join(".cobble-table-writer.lock"))
            .map_err(lock_io_error)?;
        file.try_lock().map_err(|error| {
            cobble::Error::InvalidState(format!("single-bucket writer is already active: {error}"))
        })?;
        Ok(Self { _file: file })
    }
}

fn local_meta_root(config: &Config) -> cobble::Result<PathBuf> {
    let root = configured_local_meta_root(config)?;
    std::fs::create_dir_all(&root).map_err(lock_io_error)?;
    std::fs::canonicalize(root).map_err(lock_io_error)
}

fn configured_local_meta_root(config: &Config) -> cobble::Result<PathBuf> {
    let volume = config
        .volumes
        .iter()
        .find(|volume| volume.supports(VolumeUsageKind::Meta))
        .ok_or_else(|| {
            cobble::Error::ConfigError("single-bucket writer requires a META volume".into())
        })?;
    let root = match Url::parse(&volume.base_dir) {
        Ok(url) if url.scheme() == "file" => url.to_file_path().map_err(|_| {
            cobble::Error::ConfigError("single-bucket META volume is not a local path".into())
        })?,
        Ok(_) => {
            return Err(cobble::Error::ConfigError(
                "single-bucket table writers require a local file:// META volume".into(),
            ));
        }
        Err(_) if volume.base_dir.contains("://") => {
            return Err(cobble::Error::ConfigError(format!(
                "invalid single-bucket META volume URL: {}",
                volume.base_dir
            )));
        }
        Err(_) => PathBuf::from(&volume.base_dir),
    };
    Ok(root)
}

fn bucket_has_persisted_state(config: &Config, bucket: u16) -> Result<bool> {
    let bucket_dir = local_meta_root(config)?.join(bucket_db_id(bucket));
    if !bucket_dir.exists() {
        return Ok(false);
    }
    for entry in std::fs::read_dir(bucket_dir).map_err(lock_io_error)? {
        let entry = entry.map_err(lock_io_error)?;
        if entry.file_name() != ".cobble-table-writer.lock" {
            return Ok(true);
        }
    }
    Ok(false)
}

fn bucket_snapshot_exists(config: &Config, bucket: u16, snapshot_id: u64) -> Result<bool> {
    let db_id = bucket_db_id(bucket);
    Ok(configured_local_meta_root(config)?
        .join(bucket_snapshot_manifest_path(&db_id, snapshot_id))
        .exists())
}

fn lock_io_error(error: io::Error) -> cobble::Error {
    cobble::Error::IoError(format!("single-bucket writer lock: {error}"))
}

fn open_materialized_catalog_table(
    db: Arc<Db>,
    plan: &TableWritePlan,
    store_config: &Config,
) -> Result<Table> {
    let (name, metadata) = crate::catalog::materialize_write_plan(store_config, db.as_ref(), plan)?;
    Table::from_metadata(db, name, metadata)
}

/// A typed global snapshot read proxy.
pub struct TableReader {
    typed: ArcSwap<TypedRead>,
    refresh: AutoRefreshController,
}

struct AutoRefreshController {
    interval: Option<Duration>,
    started_at: Instant,
    next_check_at: AtomicU64,
    refreshing: Mutex<()>,
}

impl AutoRefreshController {
    fn new(interval: Option<Duration>) -> Self {
        let started_at = Instant::now();
        let next_check_at = interval.map_or(0, duration_nanos);
        Self {
            interval,
            started_at,
            next_check_at: AtomicU64::new(next_check_at),
            refreshing: Mutex::new(()),
        }
    }

    fn due(&self) -> bool {
        self.interval.is_some()
            && self.elapsed_nanos() >= self.next_check_at.load(Ordering::Acquire)
    }

    fn schedule_next_check(&self) {
        let Some(interval) = self.interval else {
            return;
        };
        self.next_check_at.store(
            self.elapsed_nanos()
                .saturating_add(duration_nanos(interval)),
            Ordering::Release,
        );
    }

    fn elapsed_nanos(&self) -> u64 {
        self.started_at
            .elapsed()
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, ()>> {
        self.refreshing
            .lock()
            .map_err(|_| TableError::internal("table reader refresh lock poisoned"))
    }

    fn try_lock(&self) -> Result<Option<std::sync::MutexGuard<'_, ()>>> {
        match self.refreshing.try_lock() {
            Ok(guard) => Ok(Some(guard)),
            Err(std::sync::TryLockError::WouldBlock) => Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                Err(TableError::internal("table reader refresh lock poisoned"))
            }
        }
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
#[path = "../tests/unit/runtime.rs"]
mod tests;

impl TableReader {
    /// Open the snapshot carried by this core reader.
    ///
    /// Readers opened from a core current-pointer reader check for later committed snapshots on
    /// its configured interval. Core fixed-snapshot readers remain fixed.
    pub fn open(mut reader: Reader, name: impl Into<String>) -> Result<Self> {
        let refresh_interval = reader.auto_refresh_interval();
        reader.pin_current_snapshot();
        let name = validate_name(name.into())?;
        let metadata = global_metadata(&reader, &name)?;
        Self::from_metadata(reader, name, metadata, refresh_interval)
    }

    fn from_metadata(
        reader: Reader,
        name: String,
        metadata: TableMetadata,
        refresh_interval: Option<Duration>,
    ) -> Result<Self> {
        let state = Arc::new(GlobalReaderState::new(
            reader,
            name.clone(),
            metadata.clone(),
        ));
        Ok(Self {
            typed: ArcSwap::from_pointee(TypedRead::from_global_metadata(state, name, metadata)?),
            refresh: AutoRefreshController::new(refresh_interval),
        })
    }

    /// Return the schema of this reader's currently loaded global snapshot.
    ///
    /// This accessor never checks `CURRENT`. A later fallible read may refresh a current-pointer
    /// reader; call [`Self::refresh`] before schema inspection when the latest committed snapshot
    /// is required.
    pub fn schema(&self) -> Arc<TableSchema> {
        self.typed.load().schema_arc()
    }

    /// Start building one primary key in schema order.
    pub fn key_builder(&self) -> TableKeyBuilder {
        self.typed.load().key_builder()
    }

    /// Compile a reusable read projection from top-level field names.
    pub fn project_by_names<S: AsRef<str>>(&self, field_names: &[S]) -> Result<TableProjection> {
        self.view_for_access()?.project_by_names(field_names)
    }

    /// Read one row by primary key.
    pub fn get(&self, key: &TableKey) -> Result<Option<Vec<Value>>> {
        self.view_for_access()?.get(key)
    }

    /// Read many primary keys while preserving order and duplicates.
    pub fn multi_get(&self, keys: &[TableKey]) -> Result<Vec<Option<Vec<Value>>>> {
        self.view_for_access()?.multi_get(keys)
    }

    /// Scan all rows in one bucket.
    pub fn scan(&self, bucket: u16) -> Result<TableScan> {
        self.view_for_access()?.scan(bucket)
    }

    /// Scan one bucket from an inclusive primary-key bound to an exclusive bound.
    pub fn scan_bounds(
        &self,
        bucket: u16,
        start_key_inclusive: Option<&TableKey>,
        end_key_exclusive: Option<&TableKey>,
    ) -> Result<TableScan> {
        self.view_for_access()?
            .scan_bounds(bucket, start_key_inclusive, end_key_exclusive)
    }

    /// Build a portable full-scan plan pinned to this reader's current snapshot.
    pub fn scan_plan(&self) -> Result<crate::TableScanPlan> {
        let typed = self.view_for_access()?;
        let state = typed
            .global_state()
            .ok_or_else(|| TableError::internal("table reader is missing its global read state"))?;
        state.scan_plan()
    }

    /// Refresh this current-pointer reader to the latest committed global snapshot.
    ///
    /// Existing projections, scans, and scan plans retain their current fixed view. If the
    /// current pointer is unchanged, this returns `false`; an error leaves this reader unchanged.
    /// Fixed snapshot readers always return `false`.
    pub fn refresh(&self) -> Result<bool> {
        if self.refresh.interval.is_none() {
            return Ok(false);
        }
        let _guard = self.refresh.lock()?;
        let result = self.refresh_loaded_view();
        self.refresh.schedule_next_check();
        result
    }

    #[cfg(feature = "ffi")]
    #[doc(hidden)]
    pub fn auto_refresh_interval_nanos(&self) -> Option<u64> {
        self.refresh
            .interval
            .map(|interval| interval.as_nanos().min(u128::from(u64::MAX)) as u64)
    }

    fn view_for_access(&self) -> Result<Guard<Arc<TypedRead>>> {
        let view = self.typed.load();
        if !self.refresh.due() {
            return Ok(view);
        }
        let Some(_guard) = self.refresh.try_lock()? else {
            return Ok(view);
        };
        drop(view);
        if self.refresh.due() {
            let refreshed = self.refresh_loaded_view();
            self.refresh.schedule_next_check();
            refreshed?;
        }
        Ok(self.typed.load())
    }

    fn refresh_loaded_view(&self) -> Result<bool> {
        let typed = self.typed.load_full();
        let state = typed
            .global_state()
            .ok_or_else(|| TableError::internal("table reader is missing its global read state"))?;
        let Some((reader, name, previous_table_id)) = state.refreshed_snapshot()? else {
            return Ok(false);
        };
        let metadata = global_metadata(&reader, &name)?;
        if previous_table_id != metadata.catalog_binding.map(|binding| binding.table_id) {
            return Err(TableError::InvalidSchema(
                "refreshed global snapshot belongs to a different catalog table".into(),
            ));
        }
        let state = Arc::new(GlobalReaderState::new(
            reader,
            name.clone(),
            metadata.clone(),
        ));
        let typed = TypedRead::from_global_metadata(state, name, metadata)?;
        self.typed.store(Arc::new(typed));
        Ok(true)
    }

    #[cfg(feature = "ffi")]
    pub(crate) fn ffi_acquire_view(&self) -> crate::ffi::TableReaderView {
        crate::ffi::TableReaderView {
            typed: self.typed.load_full(),
        }
    }

    #[cfg(feature = "ffi")]
    pub(crate) fn ffi_acquire_view_if_changed(
        &self,
        current: &crate::ffi::TableReaderView,
    ) -> Option<crate::ffi::TableReaderView> {
        let typed = self.typed.load_full();
        if Arc::ptr_eq(&typed, &current.typed) {
            None
        } else {
            Some(crate::ffi::TableReaderView { typed })
        }
    }
}

/// Builder for a global typed reader from a fixed or current snapshot selection.
pub struct TableReaderBuilder {
    config: Config,
    table_name: Option<String>,
    selection: Option<TableReaderSelection>,
    catalog_table_id: Option<TableId>,
    transforms: TableSchemaTransformFactories,
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
            transforms: TableSchemaTransformFactories::default(),
        }
    }

    pub(crate) fn from_catalog(config: Config, table_name: String, table_id: TableId) -> Self {
        Self {
            config,
            table_name: Some(table_name),
            selection: None,
            catalog_table_id: Some(table_id),
            transforms: TableSchemaTransformFactories::default(),
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

    /// Open the current committed snapshot and check for later commits on access intervals.
    pub fn current_global_snapshot(mut self) -> Self {
        self.selection = Some(TableReaderSelection::CurrentGlobal);
        self
    }

    /// Register a factory for persisted schema transform specifications before opening.
    pub fn register_schema_transform<F, T>(
        mut self,
        transform_type: impl Into<String>,
        factory: F,
    ) -> Result<Self>
    where
        F: Fn(&[u8]) -> cobble::Result<T> + Send + Sync + 'static,
        T: Fn(Option<Bytes>) -> cobble::Result<Option<Bytes>> + Send + Sync + 'static,
    {
        self.transforms.register(transform_type, factory)?;
        Ok(self)
    }

    pub fn open(self) -> Result<TableReader> {
        let name = self.required_table_name()?;
        let builder = self
            .transforms
            .apply_to(ReaderBuilder::new(ReaderConfig::from_config(&self.config)))?;
        let mut reader = match self.selection.ok_or_else(|| {
            TableError::InvalidSchema("TableReaderBuilder requires a snapshot selection".into())
        })? {
            TableReaderSelection::Global { snapshot_id } => builder.open(snapshot_id)?,
            TableReaderSelection::CurrentGlobal => builder.open_current()?,
        };
        let refresh_interval = reader.auto_refresh_interval();
        reader.pin_current_snapshot();
        let metadata = global_metadata(&reader, &name)?;
        validate_catalog_binding(&metadata, self.catalog_table_id)?;
        TableReader::from_metadata(reader, name, metadata, refresh_interval)
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
    transforms: TableSchemaTransformFactories,
}

impl ReadOnlyTableBuilder {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            table_name: None,
            snapshot: None,
            catalog_table_id: None,
            transforms: TableSchemaTransformFactories::default(),
        }
    }

    pub(crate) fn from_catalog(config: Config, table_name: String, table_id: TableId) -> Self {
        Self {
            config,
            table_name: Some(table_name),
            snapshot: None,
            catalog_table_id: Some(table_id),
            transforms: TableSchemaTransformFactories::default(),
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

    /// Register a factory for persisted schema transform specifications before opening.
    pub fn register_schema_transform<F, T>(
        mut self,
        transform_type: impl Into<String>,
        factory: F,
    ) -> Result<Self>
    where
        F: Fn(&[u8]) -> cobble::Result<T> + Send + Sync + 'static,
        T: Fn(Option<Bytes>) -> cobble::Result<Option<Bytes>> + Send + Sync + 'static,
    {
        self.transforms.register(transform_type, factory)?;
        Ok(self)
    }

    pub fn open(self) -> Result<ReadOnlyTable> {
        let name = self.required_table_name()?;
        let (db_id, snapshot_id) = self.snapshot.ok_or_else(|| {
            TableError::InvalidSchema(
                "ReadOnlyTableBuilder requires a shard snapshot selection".into(),
            )
        })?;
        let db = Arc::new(
            self.transforms
                .apply_to(ReadOnlyDbBuilder::new(self.config).db_id(db_id))?
                .open(snapshot_id)?,
        );
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
    reader: Mutex<Reader>,
    name: String,
    metadata: TableMetadata,
}

impl GlobalReaderState {
    fn new(reader: Reader, name: String, metadata: TableMetadata) -> Self {
        let total_buckets = reader.current_global_snapshot().total_buckets;
        Self {
            total_buckets,
            reader: Mutex::new(reader),
            name,
            metadata,
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
        let mut reader = lock_physical_reader(&self.reader)?;
        Ok(reader.get_with_options(bucket, key, options)?)
    }

    pub(crate) fn multi_get(
        &self,
        keys: &[(u16, &[u8])],
        options: &ReadOptions,
    ) -> Result<Vec<Option<Vec<Option<bytes::Bytes>>>>> {
        let mut reader = lock_physical_reader(&self.reader)?;
        Ok(reader.multi_get_with_options(keys, options)?)
    }

    pub(crate) fn scan(
        &self,
        bucket: u16,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator> {
        let mut reader = lock_physical_reader(&self.reader)?;
        Ok(reader.scan_with_options_bounds(bucket, start, end, options)?)
    }

    pub(crate) fn scan_plan(&self) -> Result<crate::TableScanPlan> {
        let reader = lock_physical_reader(&self.reader)?;
        let snapshot = reader.current_global_snapshot();
        crate::TableScanPlan::from_global_reader(
            self.name.clone(),
            self.metadata.clone(),
            snapshot.id,
            snapshot.total_buckets,
            snapshot.shard_snapshots.clone(),
            reader.config().clone(),
        )
    }

    fn refreshed_snapshot(&self) -> Result<Option<(Reader, String, Option<TableId>)>> {
        Ok(lock_physical_reader(&self.reader)?
            .refreshed_snapshot()?
            .map(|reader| {
                (
                    reader,
                    self.name.clone(),
                    self.metadata
                        .catalog_binding
                        .map(|binding| binding.table_id),
                )
            }))
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

fn lock_physical_reader(reader: &Mutex<Reader>) -> Result<std::sync::MutexGuard<'_, Reader>> {
    reader
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
