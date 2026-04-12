use crate::block_cache::new_block_cache_with_config;
use crate::db_builder::DbBuilder;
use crate::db_iter::{DbIterator, DbIteratorOptions};
use crate::db_state::{DbStateHandle, LSMTreeScope, bucket_range_fits_total};
use crate::db_status::{CloseTransition, DbLifecycle};
use crate::error::{Error, Result};
use crate::file::FileManager;
use crate::lsm::{LSMTree, LSMTreeVersion};
use crate::memtable::{MemtableManager, MemtableManagerOptions};
use crate::merge_operator::MergeOperator;
use crate::metrics_manager::MetricsManager;
use crate::schema::{DEFAULT_COLUMN_FAMILY_ID, Schema, SchemaBuilder, SchemaManager};
use crate::snapshot::{
    ActiveMemtableSnapshotData, LoadedManifest, SnapshotCallback, SnapshotManager,
    SnapshotManifestInfo, snapshot_manifest_name,
};
use crate::sst::row_codec::{decode_value, decode_value_masked, encode_key_ref_into};
use crate::r#type::decode_merge_separated_array;
use crate::r#type::{Column, RefColumn, RefKey, RefValue, Value, ValueType};
use crate::vlog::{VlogPointer, VlogStore};
use crate::write_batch::{WriteBatch, WriteOp};
use crate::writer_options::WriterOptions;
use crate::{Config, ReadOptions, ScanOptions, TimeProvider, WriteOptions};
use bytes::{Bytes, BytesMut};
use log::info;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use uuid::Uuid;

use crate::governance::create_default_db_governance;
use crate::metrics_registry;
use crate::read_only_db::ReadOnlyDb;
use crate::ttl::{TTLProvider, TtlConfig};
use crate::util::{build_commit_short_id, build_version_string, init_logging};
#[path = "db_rescale.rs"]
mod rescale;
#[path = "db_restore.rs"]
mod restore;

/// Public database interface.
pub struct Db {
    id: String,
    db_lifecycle: Arc<DbLifecycle>,
    db_state: Arc<DbStateHandle>,
    config: Config,
    file_manager: Arc<FileManager>,
    lsm_tree: Arc<LSMTree>,
    memtable_manager: MemtableManager,
    vlog_store: Arc<VlogStore>,
    snapshot_manager: SnapshotManager,
    schema_manager: Arc<SchemaManager>,
    last_scope_synced_schema_version: AtomicU64,
    time_provider: Arc<dyn TimeProvider>,
    ttl_provider: Arc<TTLProvider>,
}

pub(crate) fn value_to_vec_of_columns(value: Value) -> Result<Option<Vec<Option<Bytes>>>> {
    let columns: Vec<Option<Bytes>> = value
        .columns
        .into_iter()
        .map(|col_opt| {
            col_opt.and_then(|col| match col.value_type() {
                ValueType::Put
                | ValueType::Merge
                // TODO: Read from value log for separated values
                | ValueType::PutSeparated
                | ValueType::MergeSeparated
                | ValueType::MergeSeparatedArray
                | ValueType::PutSeparatedArray => Some(Bytes::from(col)),
                ValueType::Delete => None,
            })
        })
        .collect();
    if columns.iter().all(Option::is_none) {
        return Ok(None);
    }
    Ok(Some(columns))
}

/// Resolve a single column value, handling any value log pointers using the provided callback.
fn resolve_column_with_vlog<F>(
    column: Column,
    resolve_pointer: &mut F,
    merge_operator: &dyn MergeOperator,
    time_provider: Option<&dyn TimeProvider>,
) -> Result<Option<Bytes>>
where
    F: FnMut(VlogPointer) -> Result<Bytes>,
{
    match column.value_type {
        ValueType::Delete => Ok(None),
        ValueType::Put => Ok(Some(Bytes::from(column))),
        ValueType::Merge => {
            // Read-path merge must reuse the same logical clock as write/compaction paths.
            let (merged, _) =
                merge_operator.merge(Bytes::new(), Bytes::from(column), time_provider)?;
            Ok(Some(merged))
        }
        ValueType::PutSeparated | ValueType::MergeSeparated => {
            let pointer = VlogPointer::from_bytes(column.data())?;
            let resolved = resolve_pointer(pointer)?;
            if column.value_type == ValueType::MergeSeparated {
                let (merged, _) = merge_operator.merge(Bytes::new(), resolved, time_provider)?;
                Ok(Some(merged))
            } else {
                Ok(Some(resolved))
            }
        }
        ValueType::MergeSeparatedArray | ValueType::PutSeparatedArray => {
            let items = decode_merge_separated_array(column.data())?;
            let mut merged = Bytes::new();
            for item in items {
                let item_value = match item.value_type {
                    ValueType::Put | ValueType::Merge => Bytes::copy_from_slice(item.data()),
                    ValueType::PutSeparated | ValueType::MergeSeparated => {
                        let pointer = VlogPointer::from_bytes(item.data())?;
                        resolve_pointer(pointer)?
                    }
                    ValueType::Delete
                    | ValueType::MergeSeparatedArray
                    | ValueType::PutSeparatedArray => {
                        return Err(Error::IoError(format!(
                            "Invalid value type in MergeSeparatedArray: {:?}",
                            item.value_type
                        )));
                    }
                };
                match item.value_type {
                    ValueType::Put | ValueType::PutSeparated => {
                        merged = item_value;
                    }
                    ValueType::Merge | ValueType::MergeSeparated => {
                        merged = merge_operator.merge(merged, item_value, time_provider)?.0;
                    }
                    ValueType::Delete
                    | ValueType::MergeSeparatedArray
                    | ValueType::PutSeparatedArray => unreachable!(),
                }
            }
            Ok(Some(merged))
        }
    }
}

/// Convert a Value into a Vec of optional column values, resolving any value log pointers using
/// the provided callback.
pub(crate) fn value_to_vec_of_columns_with_vlog<F>(
    value: Value,
    mut resolve_pointer: F,
    schema: &Schema,
    time_provider: Option<&dyn TimeProvider>,
) -> Result<Option<Vec<Option<Bytes>>>>
where
    F: FnMut(VlogPointer) -> Result<Bytes>,
{
    let resolve_pointer = &mut resolve_pointer;
    let mut columns = Vec::with_capacity(value.columns.len());
    for (column_idx, column) in value.columns.into_iter().enumerate() {
        let merge_operator = schema.operator(column_idx);
        let resolved = match column {
            Some(column) => {
                resolve_column_with_vlog(column, resolve_pointer, merge_operator, time_provider)?
            }
            None => None,
        };
        columns.push(resolved);
    }
    if columns.iter().all(Option::is_none) {
        return Ok(None);
    }
    Ok(Some(columns))
}

impl Db {
    #[inline]
    fn ensure_open(&self) -> Result<()> {
        self.db_lifecycle.ensure_open()
    }

    fn should_mark_error_on_read(err: &Error) -> bool {
        let message = match err {
            Error::IoError(msg) | Error::FileSystemError(msg) => msg,
            _ => return false,
        }
        .to_ascii_lowercase();
        message.contains("not found")
            || message.contains("no such file")
            || message.contains("does not exist")
    }

    fn maybe_mark_error_on_read(&self, err: &Error) {
        if Self::should_mark_error_on_read(err) {
            self.db_lifecycle.mark_error(err.clone());
        }
    }

    /// Open a database with the provided configuration.
    pub fn open(config: Config, bucket_ranges: Vec<RangeInclusive<u16>>) -> Result<Self> {
        DbBuilder::new(config).bucket_ranges(bucket_ranges).open()
    }

    pub(crate) fn open_with_builder(builder: DbBuilder) -> Result<Self> {
        let (config, bucket_ranges, db_id, governance) = builder.into_parts();
        if config.total_buckets == 0 || config.total_buckets > (u16::MAX as u32) + 1 {
            return Err(Error::ConfigError(
                "total_buckets must be in range 1..=65536".to_string(),
            ));
        }
        if bucket_ranges.is_empty() {
            return Err(Error::ConfigError(
                "bucket_ranges must not be empty".to_string(),
            ));
        }
        for range in &bucket_ranges {
            if !bucket_range_fits_total(range, config.total_buckets) {
                return Err(Error::ConfigError(format!(
                    "Invalid bucket range {}..={} for total_buckets {}",
                    range.start(),
                    range.end(),
                    config.total_buckets
                )));
            }
        }
        let config = config.normalize_volume_paths()?;
        init_logging(&config);
        info!(
            "Cobble db ({}, Rev:{}) start.",
            build_version_string(),
            build_commit_short_id()
        );
        metrics_registry::init_metrics();
        let id = db_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let metrics_manager = Arc::new(MetricsManager::new(&id));
        let hybrid_cache_plan =
            config.resolve_hybrid_cache_volume_plan(config.block_cache_size_bytes()?)?;
        let file_manager_config =
            config.apply_hybrid_cache_primary_partition_with_plan(hybrid_cache_plan.as_ref())?;

        if let Some(governance) = governance {
            governance.register_db(&id, &bucket_ranges, config.total_buckets)?;
        } else {
            let local_governance = create_default_db_governance(&config)?;
            local_governance.register_db(&id, &bucket_ranges, config.total_buckets)?;
        }

        let file_manager =
            FileManager::from_config(&file_manager_config, &id, Arc::clone(&metrics_manager))?;
        let file_manager = Arc::new(file_manager);
        let db_state = Arc::new(DbStateHandle::new());
        let schema_manager = Arc::new(SchemaManager::new(config.num_columns));
        let db_lifecycle = Arc::new(DbLifecycle::new_initializing());
        let db = Self::open_with_state(
            config,
            file_manager,
            db_state,
            Arc::clone(&db_lifecycle),
            id,
            bucket_ranges,
            0,
            hybrid_cache_plan,
            metrics_manager,
            schema_manager,
        )?;
        db.memtable_manager.open()?;
        db.db_lifecycle.mark_open()?;
        Ok(db)
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// Start a schema update transaction.
    pub fn update_schema(&self) -> SchemaBuilder {
        self.schema_manager.builder()
    }

    /// Return the current schema snapshot.
    pub fn current_schema(&self) -> Arc<Schema> {
        self.schema_manager.latest_schema()
    }

    /// Return the metrics samples for this database.
    pub fn metrics(&self) -> Vec<crate::MetricSample> {
        metrics_registry::snapshot_metrics(Some(&self.id))
    }

    fn scopes_for_bucket_ranges_and_column_families(
        bucket_ranges: &[RangeInclusive<u16>],
        column_family_ids: &[u8],
    ) -> Vec<LSMTreeScope> {
        let mut unique_ids = column_family_ids.to_vec();
        unique_ids.sort_unstable();
        unique_ids.dedup();
        if unique_ids.is_empty() {
            unique_ids.push(DEFAULT_COLUMN_FAMILY_ID);
        }
        let mut scopes = Vec::with_capacity(unique_ids.len() * bucket_ranges.len());
        for column_family_id in unique_ids {
            for bucket_range in bucket_ranges {
                scopes.push(LSMTreeScope::new(bucket_range.clone(), column_family_id));
            }
        }
        scopes
    }

    fn ensure_multi_lsm_scopes_for_schema(
        db_state: &Arc<DbStateHandle>,
        schema: &Schema,
    ) -> Result<()> {
        let _guard = db_state.lock();
        let snapshot = db_state.load();
        let mut desired_cf_ids = schema.column_family_ids();
        desired_cf_ids.sort_unstable();
        desired_cf_ids.dedup();
        if desired_cf_ids.is_empty() {
            desired_cf_ids.push(DEFAULT_COLUMN_FAMILY_ID);
        }

        let existing_scopes = snapshot.multi_lsm_version.tree_scopes();
        let existing_versions = snapshot.multi_lsm_version.tree_versions_cloned();
        if existing_scopes.len() != existing_versions.len() {
            return Err(Error::InvalidState(format!(
                "LSM tree scope count {} does not match version count {}",
                existing_scopes.len(),
                existing_versions.len()
            )));
        }
        let mut expanded_scopes = existing_scopes.clone();
        let mut expanded_versions = existing_versions;

        let base_ranges: Vec<RangeInclusive<u16>> = {
            let default_ranges: Vec<RangeInclusive<u16>> = existing_scopes
                .iter()
                .filter(|scope| scope.column_family_id == DEFAULT_COLUMN_FAMILY_ID)
                .map(|scope| scope.bucket_range.clone())
                .collect();
            if default_ranges.is_empty() {
                snapshot.bucket_ranges.clone()
            } else {
                default_ranges
            }
        };
        if expanded_scopes.is_empty() {
            expanded_scopes =
                Self::scopes_for_bucket_ranges_and_column_families(&base_ranges, &desired_cf_ids);
            if expanded_scopes.is_empty() {
                return Ok(());
            }
            expanded_versions =
                vec![Arc::new(LSMTreeVersion { levels: vec![] }); expanded_scopes.len()];
            let multi_lsm_version =
                crate::db_state::MultiLSMTreeVersion::from_scopes_with_tree_versions(
                    snapshot.multi_lsm_version.total_buckets(),
                    &expanded_scopes,
                    expanded_versions,
                )?;
            db_state.store(crate::db_state::DbState {
                seq_id: snapshot.seq_id,
                bucket_ranges: snapshot.bucket_ranges.clone(),
                multi_lsm_version,
                vlog_version: snapshot.vlog_version.clone(),
                active: snapshot.active.clone(),
                immutables: snapshot.immutables.clone(),
                suggested_base_snapshot_id: snapshot.suggested_base_snapshot_id,
            });
            return Ok(());
        }

        let mut added_any = false;
        for &column_family_id in &desired_cf_ids {
            for bucket_range in &base_ranges {
                let scope = LSMTreeScope::new(bucket_range.clone(), column_family_id);
                if snapshot
                    .multi_lsm_version
                    .tree_index_for_exact_scope(&scope)
                    .is_some()
                {
                    continue;
                }
                expanded_scopes.push(scope);
                expanded_versions.push(Arc::new(LSMTreeVersion { levels: vec![] }));
                added_any = true;
            }
        }
        if !added_any {
            return Ok(());
        }

        let multi_lsm_version =
            crate::db_state::MultiLSMTreeVersion::from_scopes_with_tree_versions(
                snapshot.multi_lsm_version.total_buckets(),
                &expanded_scopes,
                expanded_versions,
            )?;
        db_state.store(crate::db_state::DbState {
            seq_id: snapshot.seq_id,
            bucket_ranges: snapshot.bucket_ranges.clone(),
            multi_lsm_version,
            vlog_version: snapshot.vlog_version.clone(),
            active: snapshot.active.clone(),
            immutables: snapshot.immutables.clone(),
            suggested_base_snapshot_id: snapshot.suggested_base_snapshot_id,
        });
        Ok(())
    }

    fn ensure_multi_lsm_scopes_for_schema_if_dirty(&self, schema: &Schema) -> Result<()> {
        let schema_version = schema.version();
        if self
            .last_scope_synced_schema_version
            .load(Ordering::Acquire)
            == schema_version
        {
            return Ok(());
        }
        Self::ensure_multi_lsm_scopes_for_schema(&self.db_state, schema)?;
        self.last_scope_synced_schema_version
            .store(schema_version, Ordering::Release);
        Ok(())
    }

    /// Internal helper to write a single column value with the given ValueType.
    fn write_ref<K, V>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        value_type: ValueType,
        value: V,
        options: &WriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.ensure_open()?;
        let schema = self.schema_manager.latest_schema();
        self.ensure_multi_lsm_scopes_for_schema_if_dirty(schema.as_ref())?;
        let column_family_id = schema.resolve_column_family_id(options.column_family())?;
        let num_columns = schema.num_columns_in_family(column_family_id).unwrap_or(0);
        let column_idx = column as usize;
        if column_idx >= num_columns {
            return Err(Error::IoError(format!(
                "Column index {} exceeds num_columns {}",
                column_idx, num_columns
            )));
        }
        let column = RefColumn::new(value_type, value.as_ref());
        let expired_at = self
            .ttl_provider
            .get_expiration_timestamp(options.ttl_seconds);
        let mut columns: Vec<Option<RefColumn<'_>>> = vec![None; num_columns];
        columns[column_idx] = Some(column);
        let record = RefValue::new_with_expired_at(columns, expired_at);
        let key = RefKey::new_with_column_family(bucket, column_family_id, key.as_ref());
        self.memtable_manager.put(&key, &record)
    }

    /// Insert a single key/value pair into the given bucket and column.
    pub fn put<K, V>(&self, bucket: u16, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_with_options(bucket, key, column, value, &WriteOptions::default())
    }

    /// Insert a single key/value pair into the given bucket and column with write options.
    pub fn put_with_options<K, V>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        value: V,
        options: &WriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.write_ref(bucket, key, column, ValueType::Put, value, options)
    }

    /// Delete a single column value in the given bucket.
    pub fn delete<K>(&self, bucket: u16, key: K, column: u16) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.delete_with_options(bucket, key, column, &WriteOptions::default())
    }

    /// Delete a single column value in the given bucket with write options.
    pub fn delete_with_options<K>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        options: &WriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.write_ref(bucket, key, column, ValueType::Delete, [], options)
    }

    /// Merge a value into the given bucket and column.
    pub fn merge<K, V>(&self, bucket: u16, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.merge_with_options(bucket, key, column, value, &WriteOptions::default())
    }

    /// Merge a value into the given bucket and column with write options.
    pub fn merge_with_options<K, V>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        value: V,
        options: &WriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.write_ref(bucket, key, column, ValueType::Merge, value, options)
    }

    /// Write a batch of operations to the database.
    ///
    /// Write path: merges batch entries by (bucket, column_family, key) into consolidated
    /// Values, then writes each merged entry to the active memtable via
    /// put_ref. The memtable manager handles flush-to-L0 when the memtable
    /// is full, separated value extraction to VLOG, and schema validation.
    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.ensure_open()?;
        let mut pending: std::collections::BTreeMap<(u16, u8, Bytes), Value> =
            std::collections::BTreeMap::new();
        let schema = self.schema_manager.latest_schema();
        self.ensure_multi_lsm_scopes_for_schema_if_dirty(schema.as_ref())?;
        for (key_and_seq, op) in batch.ops {
            let bucket = key_and_seq.bucket;
            let column_family_id =
                schema.resolve_column_family_id(key_and_seq.column_family.as_deref())?;
            let num_columns = schema.num_columns_in_family(column_family_id).unwrap_or(0);
            let column_idx = key_and_seq.column as usize;
            if column_idx >= num_columns {
                return Err(Error::IoError(format!(
                    "Column index {} exceeds num_columns {}",
                    column_idx, num_columns
                )));
            }
            let (column, expired_at) = match op {
                WriteOp::Put(_, value, ttl_secs) => (
                    Column::new(ValueType::Put, value),
                    self.ttl_provider.get_expiration_timestamp(ttl_secs),
                ),
                WriteOp::Delete(_) => (
                    Column::new(ValueType::Delete, Bytes::new()),
                    self.ttl_provider.get_expiration_timestamp(None),
                ),
                WriteOp::Merge(_, value, ttl_secs) => (
                    Column::new(ValueType::Merge, value),
                    self.ttl_provider.get_expiration_timestamp(ttl_secs),
                ),
            };
            let mut columns = vec![None; num_columns];
            columns[column_idx] = Some(column);
            let next_value = Value::new_with_expired_at(columns, expired_at);
            match pending.entry((bucket, column_family_id, key_and_seq.key)) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(next_value);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let merged = std::mem::replace(entry.get_mut(), Value::new(Vec::new()))
                        .merge_in_column_family(
                            next_value,
                            &schema,
                            column_family_id,
                            Some(self.time_provider.as_ref()),
                        )?;
                    *entry.get_mut() = merged;
                }
            }
        }
        for ((bucket, column_family_id, raw_key), value) in pending {
            let key = RefKey::new_with_column_family(bucket, column_family_id, raw_key.as_ref());
            let columns: Vec<Option<RefColumn<'_>>> = value
                .columns()
                .iter()
                .map(|column| {
                    column
                        .as_ref()
                        .map(|column| RefColumn::new(column.value_type, column.data()))
                })
                .collect();
            let value_ref = RefValue::new_with_expired_at(columns, value.expired_at());
            self.memtable_manager.put(&key, &value_ref)?;
        }
        Ok(())
    }

    /// Close the database and flush pending state.
    pub fn close(&self) -> Result<()> {
        match self.db_lifecycle.begin_close()? {
            CloseTransition::AlreadyClosingOrClosed => return Ok(()),
            CloseTransition::Transitioned => {}
        }
        self.memtable_manager.close()?;
        let _ = self
            .snapshot_manager
            .wait_for_materialization(Duration::from_secs(30));
        self.lsm_tree.shutdown_compaction();
        self.snapshot_manager.close()?;
        self.db_lifecycle.mark_closed();
        Ok(())
    }

    /// Flush the active memtable and capture an LSM snapshot with a manifest.
    /// The manifest is materialized asynchronously after the flush completes.
    pub fn snapshot(&self) -> Result<u64> {
        self.ensure_open()?;
        let db_snapshot = self.snapshot_manager.create_snapshot(None);
        self.memtable_manager
            .flush_snapshot(db_snapshot.id, self.snapshot_manager.clone())?;
        Ok(db_snapshot.id)
    }

    pub fn snapshot_with_callback<F>(&self, callback: F) -> Result<u64>
    where
        F: Fn(Result<crate::coordinator::ShardSnapshotInput>) + Send + Sync + 'static,
    {
        self.ensure_open()?;
        let db_id = self.id.clone();
        let timestamp_seconds = self.now_seconds();
        let wrapper: SnapshotCallback = Arc::new(move |result: Result<SnapshotManifestInfo>| {
            callback(result.map(|info| crate::coordinator::ShardSnapshotInput {
                ranges: info.bucket_ranges,
                db_id: db_id.clone(),
                snapshot_id: info.id,
                manifest_path: info.manifest_path,
                timestamp_seconds,
            }));
        });
        let snapshot = self.snapshot_manager.create_snapshot(Some(wrapper));
        self.memtable_manager
            .flush_snapshot(snapshot.id, self.snapshot_manager.clone())?;
        Ok(snapshot.id)
    }

    /// Expire a snapshot and release its file references.
    pub fn expire_snapshot(&self, snapshot_id: u64) -> Result<bool> {
        self.ensure_open()?;
        self.snapshot_manager.expire_snapshot(snapshot_id)
    }

    /// Retain a snapshot to avoid auto-expiration.
    pub fn retain_snapshot(&self, snapshot_id: u64) -> bool {
        if self.ensure_open().is_err() {
            return false;
        }
        self.snapshot_manager.retain_snapshot(snapshot_id)
    }

    /// Build a ShardSnapshotInput for a given snapshot id.
    pub fn shard_snapshot_input(
        &self,
        snapshot_id: u64,
    ) -> Result<crate::coordinator::ShardSnapshotInput> {
        self.ensure_open()?;
        let manifest_name = snapshot_manifest_name(snapshot_id);
        let manifest_path = self
            .file_manager
            .get_metadata_file_full_path(&manifest_name)
            .ok_or_else(|| {
                Error::IoError(format!("Snapshot manifest not tracked: {}", manifest_name))
            })?;
        Ok(crate::coordinator::ShardSnapshotInput {
            ranges: self.db_state.load().bucket_ranges.clone(),
            db_id: self.id.clone(),
            snapshot_id,
            manifest_path,
            timestamp_seconds: 0,
        })
    }

    /// Open a read-only view from a snapshot manifest.
    pub fn open_read_only(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
    ) -> Result<ReadOnlyDb> {
        let config = config.normalize_volume_paths()?;
        init_logging(&config);
        ReadOnlyDb::open_with_db_id(config, snapshot_id, db_id)
    }

    /// Initialize the Db runtime from a pre-loaded DbState.
    ///
    /// Sets up all runtime components: TTL provider, LSM tree with block
    /// cache and multi-LSM bucket mapping, compaction worker (local or
    /// remote), VLOG store, snapshot manager, and memtable manager with
    /// flush/reclaim workers. Called by both fresh open and restore paths.
    #[allow(clippy::too_many_arguments)]
    fn open_with_state(
        config: Config,
        file_manager: Arc<FileManager>,
        db_state: Arc<DbStateHandle>,
        db_lifecycle: Arc<DbLifecycle>,
        id: String,
        bucket_ranges: Vec<RangeInclusive<u16>>,
        initial_vlog_file_seq: u32,
        hybrid_cache_plan: Option<crate::config::HybridCacheVolumePlan>,
        metrics_manager: Arc<MetricsManager>,
        schema_manager: Arc<SchemaManager>,
    ) -> Result<Self> {
        let time_provider = config.time_provider.create();
        let ttl_config = TtlConfig {
            enabled: config.ttl_enabled,
            default_ttl_seconds: config.default_ttl_seconds,
        };
        let ttl_provider = Arc::new(TTLProvider::new(&ttl_config, Arc::clone(&time_provider)));
        let block_cache_size = config.block_cache_size_bytes()?;
        let memtable_capacity = config.memtable_capacity_bytes()?;
        let value_separation_threshold = config.value_separation_threshold_bytes()?;
        let runtime_num_columns = schema_manager.current_num_columns();
        let mut lsm_tree = LSMTree::with_state_and_ttl(
            Arc::clone(&db_state),
            Arc::clone(&ttl_provider),
            Arc::clone(&db_lifecycle),
            Arc::clone(&metrics_manager),
        );
        if block_cache_size > 0 {
            lsm_tree.set_block_cache(Some(new_block_cache_with_config(
                &config,
                &id,
                block_cache_size,
                hybrid_cache_plan.as_ref(),
            )?));
        }
        let latest_schema = schema_manager.latest_schema();
        db_state.configure_multi_lsm(config.total_buckets, &bucket_ranges)?;
        Self::ensure_multi_lsm_scopes_for_schema(&db_state, latest_schema.as_ref())?;
        let last_scope_synced_schema_version = AtomicU64::new(latest_schema.version());
        let lsm_tree = Arc::new(lsm_tree);
        let mut memtable_writer_options =
            crate::compaction::build_writer_options(&config, 0, config.data_file_type)?;
        match &mut memtable_writer_options {
            WriterOptions::Sst(sst_options) => {
                sst_options.num_columns = runtime_num_columns;
                sst_options.metrics =
                    Some(metrics_manager.sst_writer_metrics(sst_options.compression));
            }
            WriterOptions::Parquet(parquet_options) => {
                parquet_options.num_columns = runtime_num_columns;
            }
        }
        let vlog_store = Arc::new(VlogStore::new(
            Arc::clone(&file_manager),
            memtable_writer_options.buffer_size(),
            value_separation_threshold,
        ));
        vlog_store.ensure_next_file_seq_at_least(initial_vlog_file_seq);
        // Compaction setup
        let mut compaction_options = crate::compaction::build_compaction_config(&config)?;
        compaction_options.num_columns = runtime_num_columns;
        let compaction_worker: Arc<dyn crate::compaction::CompactionWorker> =
            if let Some(addr) = config.compaction_remote_addr.clone() {
                Arc::new(crate::compaction::RemoteCompactionWorker::new(
                    addr,
                    Arc::clone(&file_manager),
                    Arc::downgrade(&lsm_tree),
                    config.clone(),
                    ttl_config.clone(),
                    Duration::from_millis(config.compaction_remote_timeout_ms),
                    Arc::clone(&metrics_manager),
                    Arc::clone(&schema_manager),
                )?)
            } else {
                Arc::new(crate::compaction::LocalCompactionWorker::new(
                    crate::compaction::CompactionExecutor::new(
                        compaction_options,
                        Arc::clone(&db_lifecycle),
                    )?,
                    Arc::clone(&file_manager),
                    Arc::downgrade(&lsm_tree),
                    config.clone(),
                    Arc::clone(&db_lifecycle),
                    Arc::clone(&metrics_manager),
                    Arc::clone(&schema_manager),
                ))
            };
        info!(
            "db compaction configured: l0_limit={} l1_base={} multiplier={} max_level={} target_file_size={}",
            compaction_options.l0_file_limit,
            compaction_options.l1_base_bytes,
            compaction_options.level_size_multiplier,
            compaction_options.max_level,
            compaction_options.target_file_size
        );
        lsm_tree.configure_compaction(compaction_options, Some(Arc::clone(&compaction_worker)));

        let snapshot_manager = SnapshotManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&schema_manager),
            config.snapshot_retention,
            bucket_ranges.clone(),
        );

        // Memtable manager setup
        let memtable_manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity,
                buffer_count: config.memtable_buffer_count,
                memtable_type: config.memtable_type,
                writer_options: memtable_writer_options,
                file_builder_factory: None,
                num_columns: runtime_num_columns,
                write_stall_limit: config.resolved_write_stall_limit(),
                schema_manager: Some(Arc::clone(&schema_manager)),
                auto_snapshot_manager: if config.snapshot_on_flush {
                    Some(snapshot_manager.clone())
                } else {
                    None
                },
                metrics_manager: Some(Arc::clone(&metrics_manager)),
                vlog_store: Some(Arc::clone(&vlog_store)),
                active_memtable_incremental_snapshot_ratio: config
                    .active_memtable_incremental_snapshot_ratio,
                db_lifecycle: Some(Arc::clone(&db_lifecycle)),
            },
        )?;

        Ok(Self {
            id,
            db_lifecycle,
            db_state,
            config,
            file_manager: Arc::clone(&file_manager),
            lsm_tree,
            memtable_manager,
            vlog_store,
            snapshot_manager,
            schema_manager,
            last_scope_synced_schema_version,
            time_provider,
            ttl_provider,
        })
    }

    fn take_over_snapshot_chain(&self, chain: &[LoadedManifest]) -> Result<()> {
        for entry in chain {
            self.snapshot_manager.import_snapshot_from_manifest(
                entry.snapshot_id,
                entry.base_snapshot_id,
                &entry.manifest,
            )?;
        }
        Ok(())
    }

    fn restore_active_memtable_snapshot_to_l0(
        &self,
        segments: &[ActiveMemtableSnapshotData],
    ) -> Result<()> {
        let restored = self
            .memtable_manager
            .restore_active_memtable_snapshot_to_l0(&self.file_manager, segments, 0)?;
        if !segments.is_empty() && !restored {
            return Err(Error::InvalidState(
                "active memtable snapshot restore did not flush".to_string(),
            ));
        }
        Ok(())
    }

    fn restore_active_memtable_snapshot_to_l0_with_source(
        &self,
        source_file_manager: &Arc<FileManager>,
        segments: &[ActiveMemtableSnapshotData],
        vlog_file_seq_offset: u32,
    ) -> Result<()> {
        let restored = self
            .memtable_manager
            .restore_active_memtable_snapshot_to_l0(
                source_file_manager,
                segments,
                vlog_file_seq_offset,
            )?;
        if !segments.is_empty() && !restored {
            return Err(Error::InvalidState(
                "active memtable snapshot restore did not flush".to_string(),
            ));
        }
        Ok(())
    }

    /// Lookup a key in a bucket across the memtable and LSM levels.
    ///
    /// Read path: snapshot DbState for consistent view → search active
    /// and immutable memtables → probe LSM levels L0..Ln via block cache →
    /// merge column values across levels using per-column MergeOperator →
    /// resolve VLOG pointers for separated values → apply TTL expiration
    /// and schema evolution when SST schema differs from current.
    pub fn get(&self, bucket: u16, key: &[u8]) -> Result<Option<Vec<Option<Bytes>>>> {
        self.get_with_options(bucket, key, &ReadOptions::default())
    }

    pub fn get_with_options(
        &self,
        bucket: u16,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<Bytes>>>> {
        self.ensure_open()?;
        let schema = self.schema_manager.latest_schema();
        let column_family_id = schema.resolve_column_family_id(options.column_family())?;
        if column_family_id != DEFAULT_COLUMN_FAMILY_ID {
            return Err(Error::IoError(format!(
                "ReadOptions.column_family {:?} is not supported before CF key-codec wiring",
                options.column_family()
            )));
        }
        let num_columns = schema.num_columns_in_family(column_family_id).unwrap_or(0);
        if let Some(max_index) = options.max_index()
            && max_index >= num_columns
        {
            return Err(Error::IoError(format!(
                "max_index {} in ReadOptions exceeds num_columns {}",
                max_index, num_columns
            )));
        }
        let mut encoded_key = BytesMut::with_capacity(3 + key.len());
        encode_key_ref_into(&RefKey::new(bucket, key), &mut encoded_key);
        let encoded_key = encoded_key.freeze();
        let selected_columns = options.columns();
        let masks = options.masks(num_columns);
        let selected_mask = masks.selected_mask.as_deref();
        let decode_mask = masks.base_mask.as_ref();
        let mask_size = decode_mask.len();

        let mut terminal_mask = if num_columns == 1 {
            None
        } else {
            Some(vec![0u8; mask_size])
        };
        let snapshot = self.db_state.load();
        let mut values: Vec<Value> = Vec::new();
        self.memtable_manager.get_all_with_snapshot(
            Arc::clone(&snapshot),
            encoded_key.as_ref(),
            |raw, source_schema| {
                let mut raw_value = Bytes::copy_from_slice(raw);
                let mut value = if source_schema.version() == schema.version() {
                    decode_value_masked(
                        &mut raw_value,
                        source_schema.num_columns(),
                        decode_mask,
                        None,
                    )?
                } else {
                    let decoded = decode_value(&mut raw_value, source_schema.num_columns())?;
                    self.schema_manager.evolve_value(
                        decoded,
                        source_schema.version(),
                        schema.version(),
                    )?
                };
                if let Some(mask) = terminal_mask.as_mut() {
                    for (idx, column) in value.columns().iter().enumerate().take(num_columns) {
                        if column
                            .as_ref()
                            .is_some_and(|column| column.value_type().is_terminal())
                        {
                            mask[idx / 8] |= 1 << (idx % 8);
                        }
                    }
                    if let Some(selected) = selected_mask {
                        for (idx, mask_byte) in mask.iter_mut().enumerate().take(mask_size) {
                            *mask_byte &= selected[idx];
                        }
                    }
                }
                if let Some(columns) = selected_columns {
                    value = value.select_columns(columns);
                }
                values.push(value);
                Ok(())
            },
        )?;
        let mut should_stop =
            num_columns > 1 && values.last().is_some_and(|value| value.is_terminal());
        let lsm_values = self.lsm_tree.get_with_snapshot(
            &self.file_manager,
            Arc::clone(&snapshot),
            bucket,
            encoded_key.as_ref(),
            schema.as_ref(),
            self.schema_manager.as_ref(),
            selected_columns,
            selected_mask,
            terminal_mask.as_deref_mut(),
        );
        let lsm_values = match lsm_values {
            Ok(values) => values,
            Err(err) => {
                self.maybe_mark_error_on_read(&err);
                return Err(err);
            }
        };
        for value in lsm_values {
            if should_stop {
                break;
            }
            if num_columns > 1 {
                should_stop = value.is_terminal();
            }
            values.push(value);
        }

        let values: Vec<Value> = values
            .into_iter()
            .filter(|v| !self.ttl_provider.expired(&v.expired_at))
            .rev()
            .collect();

        if values.is_empty() {
            return Ok(None);
        }
        let mut iter = values.into_iter();
        let mut merged = iter.next().expect("values not empty");
        for newer in iter {
            merged = merged.merge(newer, &schema, Some(self.time_provider.as_ref()))?;
        }
        let result = value_to_vec_of_columns_with_vlog(
            merged,
            |pointer| match self
                .vlog_store
                .read_pointer(&snapshot.vlog_version, pointer)
            {
                Ok(value) => Ok(value),
                Err(vlog_err) => self
                    .memtable_manager
                    .read_vlog_pointer_with_snapshot(Arc::clone(&snapshot), pointer)?
                    .ok_or(vlog_err),
            },
            &schema,
            Some(self.time_provider.as_ref()),
        );
        match result {
            Ok(value) => Ok(value),
            Err(err) => {
                self.maybe_mark_error_on_read(&err);
                Err(err)
            }
        }
    }

    pub fn scan<'a>(&'a self, bucket: u16, range: Range<&[u8]>) -> Result<DbIterator<'a>> {
        self.scan_with_options(bucket, range, &ScanOptions::default())
    }

    pub fn scan_with_options<'a>(
        &'a self,
        bucket: u16,
        range: Range<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator<'a>> {
        self.ensure_open()?;
        let snapshot = self.db_state.load();
        let schema = self.schema_manager.latest_schema();
        let num_columns = schema.num_columns();
        if let Some(max_index) = options.max_index()
            && max_index >= num_columns
        {
            return Err(Error::IoError(format!(
                "max_index {} in ScanOptions exceeds num_columns {}",
                max_index, num_columns
            )));
        }
        let memtable_iters = self
            .memtable_manager
            .scan_memtable_iterators_with_snapshot(
                Arc::clone(&snapshot),
                Arc::clone(&schema),
                options.columns(),
            )?;
        let lsm_iters = self.lsm_tree.scan_with_snapshot(
            &self.file_manager,
            Arc::clone(&snapshot),
            Arc::clone(&schema),
            Arc::clone(&self.schema_manager),
            options.read_ahead_bytes()?,
            options.columns(),
        );
        let (lsm_iters, effective_schema) = match lsm_iters {
            Ok(result) => result,
            Err(err) => {
                self.maybe_mark_error_on_read(&err);
                return Err(err);
            }
        };
        let encode_scan_key = |key: &[u8]| {
            let mut encoded = BytesMut::with_capacity(3 + key.len());
            encode_key_ref_into(&RefKey::new(bucket, key), &mut encoded);
            encoded.freeze()
        };
        let start_key = encode_scan_key(range.start);
        let end_bound = Some((encode_scan_key(range.end), false));
        let mut iter = DbIterator::new(
            memtable_iters,
            lsm_iters,
            DbIteratorOptions {
                end_bound,
                snapshot,
                memtable_manager: Some(&self.memtable_manager),
                vlog_store: Arc::clone(&self.vlog_store),
                ttl_provider: Arc::clone(&self.ttl_provider),
                schema: effective_schema,
            },
        );
        if let Err(err) = iter.seek(start_key.as_ref()) {
            self.maybe_mark_error_on_read(&err);
            return Err(err);
        }
        Ok(iter)
    }

    /// Set the current time for TTL evaluation (manual time provider only).
    pub fn set_time(&self, next: u32) {
        self.time_provider.set_time(next);
    }

    /// Returns the current logical time in seconds from the configured time provider.
    pub fn now_seconds(&self) -> u32 {
        self.time_provider.now_seconds()
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MergeOperator;
    use crate::db_state::full_bucket_range;
    use crate::{
        DbBuilder, DbGovernance, ReadOptions, ScanOptions, U32CounterMergeOperator,
        U64CounterMergeOperator, VolumeDescriptor, WriteOptions,
    };
    use serial_test::serial;
    use size::Size;
    use std::sync::{Arc, Mutex};

    fn cleanup_test_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn config_with_small_memtable(path: &str) -> Config {
        Config {
            memtable_capacity: Size::from_const(128),
            memtable_buffer_count: 2,
            num_columns: 1,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", path)),
            ..Config::default()
        }
    }

    fn open_db(config: Config) -> Db {
        let total_buckets = config.total_buckets;
        Db::open(
            config,
            std::iter::once(full_bucket_range(total_buckets)).collect(),
        )
        .unwrap()
    }

    #[derive(Default)]
    struct RecordingGovernance {
        calls: Mutex<Vec<(String, Vec<RangeInclusive<u16>>, u32)>>,
    }

    impl DbGovernance for RecordingGovernance {
        fn register_db(
            &self,
            db_id: &str,
            ranges: &[RangeInclusive<u16>],
            total_buckets: u32,
        ) -> Result<()> {
            self.calls.lock().expect("recording governance lock").push((
                db_id.to_string(),
                ranges.to_vec(),
                total_buckets,
            ));
            Ok(())
        }
    }

    #[test]
    #[serial(file)]
    fn test_db_rejects_mutation_and_read_after_close() {
        let root = "/tmp/db_state_after_close";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = open_db(config);
        db.put(0, b"k1", 0, b"v1").unwrap();
        db.close().unwrap();
        db.close().unwrap();

        let put_err = db.put(0, b"k2", 0, b"v2").unwrap_err();
        assert!(matches!(put_err, Error::InvalidState(_)));
        let get_err = db.get(0, b"k1").unwrap_err();
        assert!(matches!(get_err, Error::InvalidState(_)));
        let snapshot_err = db.snapshot().unwrap_err();
        assert!(matches!(snapshot_err, Error::InvalidState(_)));
        assert!(!db.retain_snapshot(0));

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_builder_uses_custom_governance() {
        let root = "/tmp/db_builder_custom_governance";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let total_buckets = config.total_buckets;
        let ranges = vec![full_bucket_range(total_buckets)];
        let governance = Arc::new(RecordingGovernance::default());
        let db = DbBuilder::new(config)
            .db_id("db-builder-governed")
            .bucket_ranges(ranges.clone())
            .governance(Arc::clone(&governance) as Arc<dyn DbGovernance>)
            .open()
            .unwrap();
        db.close().unwrap();

        let calls = governance.calls.lock().expect("recording governance lock");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "db-builder-governed");
        assert_eq!(calls[0].1, ranges);
        assert_eq!(calls[0].2, total_buckets);
        drop(calls);
        cleanup_test_root(root);
    }

    fn decode_u32_counter(bytes: &[u8]) -> u32 {
        u32::from_le_bytes(bytes.try_into().expect("u32 counter bytes"))
    }

    fn decode_u64_counter(bytes: &[u8]) -> u64 {
        u64::from_le_bytes(bytes.try_into().expect("u64 counter bytes"))
    }

    struct PipeMergeOperator;

    impl MergeOperator for PipeMergeOperator {
        fn merge(
            &self,
            existing_value: Bytes,
            value: Bytes,
            _time_provider: Option<&dyn TimeProvider>,
        ) -> Result<(Bytes, Option<ValueType>)> {
            if existing_value.is_empty() {
                Ok((value, None))
            } else {
                let mut merged = BytesMut::with_capacity(existing_value.len() + 1 + value.len());
                merged.extend_from_slice(existing_value.as_ref());
                merged.extend_from_slice(b"|");
                merged.extend_from_slice(value.as_ref());
                Ok((merged.freeze(), None))
            }
        }
    }

    #[test]
    #[serial(file)]
    fn test_db_write_batch_triggers_flush() {
        let root = "/tmp/db_write_batch_flush";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = open_db(config);
        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, vec![b'a'; 64]);
        batch.put(0, b"k2", 0, vec![b'b'; 64]);
        db.write_batch(batch).unwrap();

        let results = db.memtable_manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        assert_eq!(db.lsm_tree.level_files(0).len(), 1);

        db.memtable_manager.flush_active().unwrap();
        let results = db.memtable_manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        assert_eq!(db.lsm_tree.level_files(0).len(), 2);

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_write_batch_put_coalesces_with_flush() {
        let root = "/tmp/db_write_batch_put";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = open_db(config);
        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"old".to_vec());
        batch.put(0, b"k1", 0, b"new".to_vec());
        batch.put(0, b"k2", 0, vec![b'x'; 64]);
        db.write_batch(batch).unwrap();

        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), b"new");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_write_routes_non_default_column_family_to_separate_tree() {
        let root = "/tmp/db_write_cf_routing";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = open_db(config);
        let mut schema = db.update_schema();
        schema
            .add_column(0, None, None, Some("metrics".to_string()))
            .unwrap();
        let latest_schema = schema.commit();
        let metrics_cf = latest_schema
            .resolve_column_family_id(Some("metrics"))
            .unwrap();

        db.put_with_options(
            0,
            b"k_cf",
            0,
            b"v_cf",
            &WriteOptions::with_column_family("metrics"),
        )
        .unwrap();

        let mut batch = WriteBatch::new();
        batch.put(0, b"k_default", 0, b"v_default");
        batch.put_with_options(
            0,
            b"k_metrics",
            0,
            b"v_metrics",
            &WriteOptions::with_column_family("metrics"),
        );
        batch.delete_with_options(0, b"k_cf", 0, &WriteOptions::with_column_family("metrics"));
        db.write_batch(batch).unwrap();

        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let snapshot = db.db_state.load();
        let default_tree_idx = snapshot
            .multi_lsm_version
            .tree_index_for_bucket_and_column_family(0, DEFAULT_COLUMN_FAMILY_ID)
            .unwrap();
        let metrics_tree_idx = snapshot
            .multi_lsm_version
            .tree_index_for_bucket_and_column_family(0, metrics_cf)
            .unwrap();
        assert_ne!(default_tree_idx, metrics_tree_idx);
        assert!(
            !db.lsm_tree
                .level_files_in_tree(default_tree_idx, 0)
                .is_empty()
        );
        assert!(
            !db.lsm_tree
                .level_files_in_tree(metrics_tree_idx, 0)
                .is_empty()
        );

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_custom_merge_operator_per_column() {
        let root = "/tmp/db_custom_merge_operator";
        cleanup_test_root(root);
        let config = Config {
            memtable_capacity: Size::from_const(128),
            memtable_buffer_count: 2,
            num_columns: 2,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let db = open_db(config);
        let mut schema = db.update_schema();
        schema
            .set_column_operator(None, 0, Arc::new(PipeMergeOperator))
            .unwrap();
        let _ = schema.commit();

        db.put(0, b"k1", 0, b"base0").unwrap();
        db.merge(0, b"k1", 0, b"a").unwrap();
        db.merge(0, b"k1", 0, b"b").unwrap();
        db.put(0, b"k1", 1, b"base1").unwrap();
        db.merge(0, b"k1", 1, b"a").unwrap();
        db.merge(0, b"k1", 1, b"b").unwrap();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        assert_eq!(value[0].as_ref().unwrap().as_ref(), b"base0|a|b");
        assert_eq!(value[1].as_ref().unwrap().as_ref(), b"base1ab");

        let mut batch = WriteBatch::new();
        batch.merge(0, b"k2", 0, b"a");
        batch.merge(0, b"k2", 0, b"b");
        db.write_batch(batch).unwrap();

        let value = db.get(0, b"k2").unwrap().expect("value present");
        assert_eq!(value[0].as_ref().unwrap().as_ref(), b"a|b");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_counter_merge_operators_code_path() {
        let root = "/tmp/db_counter_merge_operator";
        cleanup_test_root(root);
        let config = Config {
            memtable_capacity: Size::from_const(128),
            memtable_buffer_count: 2,
            num_columns: 2,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let db = open_db(config);
        let mut schema = db.update_schema();
        schema
            .set_column_operator(None, 0, Arc::new(U32CounterMergeOperator))
            .unwrap();
        schema
            .set_column_operator(None, 1, Arc::new(U64CounterMergeOperator))
            .unwrap();
        let _ = schema.commit();

        db.put(0, b"k1", 0, 10u32.to_le_bytes()).unwrap();
        db.merge(0, b"k1", 0, 2u32.to_le_bytes()).unwrap();
        db.merge(0, b"k1", 0, 3u32.to_le_bytes()).unwrap();
        db.put(0, b"k1", 1, 100u64.to_le_bytes()).unwrap();
        db.merge(0, b"k1", 1, 11u64.to_le_bytes()).unwrap();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        assert_eq!(
            decode_u32_counter(value[0].as_ref().unwrap().as_ref()),
            15u32
        );
        assert_eq!(
            decode_u64_counter(value[1].as_ref().unwrap().as_ref()),
            111u64
        );

        let mut batch = WriteBatch::new();
        batch.merge(0, b"k2", 0, 4u32.to_le_bytes());
        batch.merge(0, b"k2", 0, 5u32.to_le_bytes());
        batch.merge(0, b"k2", 1, 7u64.to_le_bytes());
        batch.merge(0, b"k2", 1, 8u64.to_le_bytes());
        db.write_batch(batch).unwrap();

        let value = db.get(0, b"k2").unwrap().expect("value present");
        assert_eq!(
            decode_u32_counter(value[0].as_ref().unwrap().as_ref()),
            9u32
        );
        assert_eq!(
            decode_u64_counter(value[1].as_ref().unwrap().as_ref()),
            15u64
        );

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_evolves_older_schema_values() {
        let root = "/tmp/db_schema_evolution_get";
        cleanup_test_root(root);
        let config = Config {
            memtable_capacity: Size::from_const(128),
            memtable_buffer_count: 2,
            num_columns: 1,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let db = open_db(config);

        db.put(0, b"k1", 0, b"v1").unwrap();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut schema = db.update_schema();
        schema.add_column(1, None, None, None).unwrap();
        let _ = schema.commit();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        assert_eq!(value.len(), 2);
        assert_eq!(value[0].as_ref().unwrap().as_ref(), b"v1");
        assert!(value[1].is_none());

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_memtable_read_evolves_older_schema_values() {
        let root = "/tmp/db_schema_evolution_memtable_read";
        cleanup_test_root(root);
        let config = Config {
            memtable_capacity: Size::from_const(128),
            memtable_buffer_count: 2,
            num_columns: 1,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let db = open_db(config);

        db.put(0, b"k1", 0, b"v1").unwrap();

        let mut schema = db.update_schema();
        schema.add_column(1, None, None, None).unwrap();
        let _ = schema.commit();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        assert_eq!(value.len(), 2);
        assert_eq!(value[0].as_ref().unwrap().as_ref(), b"v1");
        assert!(value[1].is_none());

        let mut iter = db.scan(0, b"k1".as_slice()..b"k2".as_slice()).unwrap();
        let (scan_key, columns) = iter.next().unwrap().unwrap();
        assert_eq!(scan_key.as_ref(), b"k1");
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].as_ref().unwrap().as_ref(), b"v1");
        assert!(columns[1].is_none());
        assert!(iter.next().is_none());

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_rejects_separated_value_type_input() {
        let root = "/tmp/db_reject_separated_input";
        cleanup_test_root(root);
        let db = open_db(config_with_small_memtable(root));

        for value_type in [
            ValueType::PutSeparated,
            ValueType::MergeSeparated,
            ValueType::MergeSeparatedArray,
            ValueType::PutSeparatedArray,
        ] {
            let err = db
                .write_ref(0, b"k1", 0, value_type, b"value", &WriteOptions::default())
                .unwrap_err();
            assert!(matches!(err, Error::InputError(_)));
        }

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_value_separation_get_from_memtable_before_flush() {
        let root = "/tmp/db_value_separation_memtable";
        cleanup_test_root(root);
        let config = Config {
            value_separation_threshold: Some(Size::from_const(8)),
            ..config_with_small_memtable(root)
        };
        let db = open_db(config);
        let large = b"value-larger-than-threshold";
        db.put(0, b"k1", 0, large).unwrap();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        assert_eq!(value[0].as_ref().unwrap().as_ref(), large);

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_value_separation_flush_and_get() {
        use crate::sst::row_codec::decode_value;
        use crate::sst::{SSTIterator, SSTIteratorOptions};

        let root = "/tmp/db_value_separation";
        cleanup_test_root(root);
        let config = Config {
            value_separation_threshold: Some(Size::from_const(8)),
            ..config_with_small_memtable(root)
        };
        let db = open_db(config);
        let large = b"value-larger-than-threshold";
        db.put(0, b"k1", 0, large).unwrap();

        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let level0 = db.lsm_tree.level_files(0);
        assert_eq!(level0.len(), 1);
        let data_file = Arc::clone(&level0[0]);
        let reader = db
            .file_manager
            .open_data_file_reader(data_file.file_id)
            .unwrap();
        let mut iter = SSTIterator::with_cache_and_file(
            Box::new(reader),
            data_file.as_ref(),
            SSTIteratorOptions {
                bloom_filter_enabled: true,
                ..SSTIteratorOptions::default()
            },
            None,
        )
        .unwrap();
        iter.seek_to_first().unwrap();
        let (_, mut raw_value) = iter.current().unwrap().unwrap();
        let decoded = decode_value(&mut raw_value, 1).unwrap();
        let column = decoded
            .columns()
            .first()
            .and_then(|col| col.as_ref())
            .expect("column present");
        assert_eq!(column.value_type, ValueType::PutSeparated);
        assert_eq!(column.data().len(), 8);

        let value = db.get(0, b"k1").unwrap().expect("value present");
        assert_eq!(value[0].as_ref().unwrap().as_ref(), large);

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_resolves_merge_separated_array() {
        let root = "/tmp/db_get_merge_separated_array";
        cleanup_test_root(root);
        let config = Config {
            value_separation_threshold: Some(Size::from_const(4)),
            ..config_with_small_memtable(root)
        };
        let db = open_db(config);

        db.put(0, b"k1", 0, b"base-separated").unwrap();
        db.merge(0, b"k1", 0, b"-suffix-separated").unwrap();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        assert_eq!(
            value[0].as_ref().unwrap().as_ref(),
            b"base-separated-suffix-separated"
        );

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_prefers_newer_l0_file() {
        let root = "/tmp/db_get_newer_l0";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = open_db(config);

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"old".to_vec());
        batch.put(0, b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"new".to_vec());
        batch.put(0, b"k3", 0, vec![b'b'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), b"new");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_merges_across_l0_files() {
        let root = "/tmp/db_get_merge_l0";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = open_db(config);

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"base".to_vec());
        batch.put(0, b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.merge(0, b"k1", 0, b"_x".to_vec());
        batch.put(0, b"k3", 0, vec![b'b'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), b"base_x");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_memtable_overlaps_l0_value() {
        let root = "/tmp/db_get_memtable_overlaps_l0";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = open_db(config);

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"old".to_vec());
        batch.put(0, b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"new".to_vec());
        db.write_batch(batch).unwrap();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), b"new");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_memtable_merges_with_l0_value() {
        let root = "/tmp/db_get_memtable_merge_l0";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = open_db(config);

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"base".to_vec());
        batch.put(0, b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.merge(0, b"k1", 0, b"_x".to_vec());
        db.write_batch(batch).unwrap();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), b"base_x");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_multi_column_overrides_column_only() {
        let root = "/tmp/db_multi_column_override";
        cleanup_test_root(root);
        let config = Config {
            num_columns: 2,
            ..config_with_small_memtable(root)
        };
        let db = open_db(config);

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"c0-old".to_vec());
        batch.put(0, b"k1", 1, b"c1-old".to_vec());
        batch.put(0, b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 1, b"c1-new".to_vec());
        db.write_batch(batch).unwrap();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        let col0 = value[0].as_ref().unwrap();
        let col1 = value[1].as_ref().unwrap();
        assert_eq!(col0.as_ref(), b"c0-old");
        assert_eq!(col1.as_ref(), b"c1-new");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_multi_column_merge_across_l0() {
        let root = "/tmp/db_multi_column_merge_l0";
        cleanup_test_root(root);
        let config = Config {
            num_columns: 2,
            ..config_with_small_memtable(root)
        };
        let db = open_db(config);

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"c0".to_vec());
        batch.put(0, b"k1", 1, b"c1".to_vec());
        batch.put(0, b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.merge(0, b"k1", 1, b"_x".to_vec());
        batch.put(0, b"k3", 0, vec![b'b'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        let col0 = value[0].as_ref().unwrap();
        let col1 = value[1].as_ref().unwrap();
        assert_eq!(col0.as_ref(), b"c0");
        assert_eq!(col1.as_ref(), b"c1_x");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_with_column_index() {
        let root = "/tmp/db_get_column_index";
        cleanup_test_root(root);
        let config = Config {
            num_columns: 2,
            ..config_with_small_memtable(root)
        };
        let db = open_db(config);

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"c0".to_vec());
        batch.put(0, b"k1", 1, b"c1".to_vec());
        db.write_batch(batch).unwrap();

        let value = db
            .get_with_options(0, b"k1", &ReadOptions::for_columns(vec![1, 0]))
            .unwrap()
            .expect("value present");
        assert_eq!(value.len(), 2);
        assert_eq!(value[0].as_ref().unwrap().as_ref(), b"c1");
        assert_eq!(value[1].as_ref().unwrap().as_ref(), b"c0");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_scan_range_merges_memtable_and_l0() {
        let root = "/tmp/db_scan_range";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = open_db(config);

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"old".to_vec());
        batch.put(0, b"z1", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        db.put(0, b"k1", 0, b"new").unwrap();
        db.put(0, b"k2", 0, b"v2").unwrap();

        let mut iter = db.scan(0, b"k1".as_slice()..b"k3".as_slice()).unwrap();
        let mut rows = Vec::new();
        while let Some(row) = iter.next() {
            let (key, columns) = row.unwrap();
            rows.push((key, columns[0].clone()));
        }
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0.as_ref(), b"k1");
        assert_eq!(rows[0].1.as_ref().unwrap().as_ref(), b"new");
        assert_eq!(rows[1].0.as_ref(), b"k2");
        assert_eq!(rows[1].1.as_ref().unwrap().as_ref(), b"v2");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_scan_holds_snapshot_until_drop() {
        let root = "/tmp/db_scan_snapshot";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = open_db(config);

        db.put(0, b"k1", 0, b"old").unwrap();
        let mut iter = db.scan(0, b"".as_slice()..b"\xff".as_slice()).unwrap();
        db.put(0, b"k1", 0, b"new").unwrap();

        let (key, columns) = iter.next().unwrap().unwrap();
        assert_eq!(key.as_ref(), b"k1");
        assert_eq!(columns[0].as_ref().unwrap().as_ref(), b"old");
        assert!(iter.next().is_none());

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_scan_with_column_indices() {
        let root = "/tmp/db_scan_column_indices";
        cleanup_test_root(root);
        let config = Config {
            num_columns: 2,
            ..config_with_small_memtable(root)
        };
        let db = open_db(config);

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"c0-1".to_vec());
        batch.put(0, b"k1", 1, b"c1-1".to_vec());
        batch.put(0, b"k2", 0, b"c0-2".to_vec());
        batch.put(0, b"k2", 1, b"c1-2".to_vec());
        db.write_batch(batch).unwrap();

        let mut iter = db
            .scan_with_options(
                0,
                b"k1".as_slice()..b"k3".as_slice(),
                &ScanOptions::for_columns(vec![1, 0]),
            )
            .unwrap();

        let (k1, cols1) = iter.next().unwrap().unwrap();
        assert_eq!(k1.as_ref(), b"k1");
        assert_eq!(cols1.len(), 2);
        assert_eq!(cols1[0].as_ref().unwrap().as_ref(), b"c1-1");
        assert_eq!(cols1[1].as_ref().unwrap().as_ref(), b"c0-1");

        let (k2, cols2) = iter.next().unwrap().unwrap();
        assert_eq!(k2.as_ref(), b"k2");
        assert_eq!(cols2.len(), 2);
        assert_eq!(cols2[0].as_ref().unwrap().as_ref(), b"c1-2");
        assert_eq!(cols2[1].as_ref().unwrap().as_ref(), b"c0-2");

        assert!(iter.next().is_none());

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_scan_with_read_ahead_option() {
        let root = "/tmp/db_scan_read_ahead";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = open_db(config);

        db.put(0, b"k1", 0, b"v1").unwrap();
        db.put(0, b"k2", 0, b"v2").unwrap();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut options = ScanOptions::default();
            options.read_ahead_bytes = Size::from_const(128);
            let mut iter = db
                .scan_with_options(0, b"".as_slice()..b"\xff".as_slice(), &options)
                .unwrap();
            let mut keys = Vec::new();
            while let Some(row) = iter.next() {
                let (key, _) = row.unwrap();
                keys.push(key);
            }
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0].as_ref(), b"k1");
            assert_eq!(keys[1].as_ref(), b"k2");
        });

        cleanup_test_root(root);
    }
}
