use crate::cache::new_block_cache_with_config;
use crate::db_builder::DbBuilder;
use crate::db_iter::{DbIterator, DbIteratorOptions};
use crate::db_state::{DbStateHandle, LSMTreeScope, bucket_range_fits_total};
use crate::db_status::{CloseTransition, DbAccessGuard, DbLifecycle};
use crate::error::{Error, Result};
use crate::file::FileManager;
use crate::key_codec::{
    encode_key, encode_next_column_family_scan_key, encode_scan_key, encode_scan_key_after,
};
use crate::lsm::{BatchGetRequest, LSMTree, LSMTreeVersion};
use crate::memtable::{MemtableManager, MemtableManagerOptions};
use crate::merge_operator::MergeOperator;
use crate::metrics_manager::MetricsManager;
use crate::schema::{DEFAULT_COLUMN_FAMILY_ID, Schema, SchemaBuilder, SchemaManager};
use crate::snapshot::{
    ActiveMemtableSnapshotData, LoadedManifest, SnapshotCallback, SnapshotManager,
    SnapshotManifestInfo, load_manifest_for_snapshot, snapshot_manifest_name,
};
use crate::sst::row_codec::{decode_value, decode_value_masked};
use crate::r#type::{
    Column, RefColumn, RefKey, RefValue, Value, ValueType, decode_merge_separated_array,
};
use crate::vlog::{VlogPointer, VlogStore};
use crate::write_batch::{WriteBatch, WriteOp};
use crate::writer_options::WriterOptions;
use crate::{Config, ReadOptions, ScanOptions, TimeProvider, WriteOptions};
use bytes::Bytes;
use log::{error, info, warn};
use std::collections::HashMap;
use std::ops::{ControlFlow, Range, RangeInclusive};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
    db_governance: Option<Arc<dyn crate::governance::DbGovernance>>,
    db_lifecycle: Arc<DbLifecycle>,
    db_state: Arc<DbStateHandle>,
    config: Config,
    file_manager: Arc<FileManager>,
    lsm_tree: Arc<LSMTree>,
    memtable_manager: Arc<MemtableManager>,
    vlog_store: Arc<VlogStore>,
    snapshot_manager: SnapshotManager,
    schema_manager: Arc<SchemaManager>,
    /// Serializes LSM topology changes with dedicated-compaction result application.
    ///
    /// File-level flushes may continue concurrently, but tree scope/index changes must not occur
    /// between a dedicated result's scope validation and its durable snapshot proof.
    lsm_topology_lock: Arc<Mutex<()>>,
    last_scope_synced_schema_version: AtomicU64,
    default_write_options: WriteOptions,
    default_read_options: ReadOptions,
    default_scan_options: ScanOptions,
    time_provider: Arc<dyn TimeProvider>,
    ttl_provider: Arc<TTLProvider>,
    /// Dedicated compaction result poller. Only active in `CompactionMode::Dedicated`.
    dedicated_poller: Option<crate::compaction::dedicated_poller::DedicatedCompactionPollerHandle>,
    /// Periodically moves files between primary volume tiers in both directions.
    primary_tiering_worker: Option<crate::file::PrimaryTieringWorkerHandle>,
    adoption_coordinator: Arc<rescale::AdoptionCoordinator>,
    /// Durable runtime-manifest publisher for external observers.
    runtime_manifest_publisher:
        Option<Arc<crate::runtime_manifest::publisher::RuntimeManifestPublisherHandle>>,
}

/// Storage ownership policy for files imported by [`Db::expand_bucket_with_storage_mode`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpandStorageMode {
    /// Reference the source snapshot until its files are copied into this DB's owned storage.
    AdoptAsync,
    /// Keep a durable external reference to the source snapshot without copying it.
    ReferencePersistent,
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
            let mut operands = Vec::with_capacity(items.len());
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
                        // A Put replaces prior operands; batch the remaining payload once.
                        operands.clear();
                    }
                    ValueType::Merge | ValueType::MergeSeparated => {
                        operands.push(item_value);
                    }
                    ValueType::Delete
                    | ValueType::MergeSeparatedArray
                    | ValueType::PutSeparatedArray => unreachable!(),
                }
            }
            // One final batch merge avoids repeatedly concatenating the separated payload.
            if !operands.is_empty() {
                merged = merge_operator
                    .merge_batch(merged, operands, time_provider)?
                    .0;
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
    column_family_id: u8,
    time_provider: Option<&dyn TimeProvider>,
) -> Result<Option<Vec<Option<Bytes>>>>
where
    F: FnMut(VlogPointer) -> Result<Bytes>,
{
    let resolve_pointer = &mut resolve_pointer;
    let mut columns = Vec::with_capacity(value.columns.len());
    for (column_idx, column) in value.columns.into_iter().enumerate() {
        let merge_operator = schema.operator_in_family(column_family_id, column_idx);
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

pub(crate) fn select_projected_columns<T>(
    mut columns: Vec<Option<T>>,
    selected_columns: &[usize],
) -> Vec<Option<T>> {
    let mut projected = Vec::with_capacity(selected_columns.len());
    for &column_idx in selected_columns {
        projected.push(columns.get_mut(column_idx).and_then(|column| column.take()));
    }
    projected
}

impl Db {
    #[inline]
    fn begin_access(&self) -> Result<DbAccessGuard<'_>> {
        self.db_lifecycle.begin_access()
    }

    /// Applies an adaptive memtable switch decision if one was returned by the controller.
    /// Called after the operation completes and any active-memtable locks are released, so the
    /// switch (which may flush) cannot deadlock with the caller.
    ///
    /// The full [`SwitchDecision`] (epoch + generation) is passed to the manager, which validates
    /// it inside the transition lock **before** any side effect. If the decision is stale (mode
    /// was toggled, or a newer decision supersedes it), it is silently discarded. If the physical
    /// switch fails, the controller state is left unchanged so the next window can retry.
    #[inline]
    fn apply_adaptive_decision(&self, decision: Option<crate::memtable::SwitchDecision>) {
        if let Some(decision) = decision
            && let Err(err) = self.memtable_manager.apply_adaptive_switch(&decision)
        {
            log::warn!(
                "Adaptive memtable switch to {:?} failed: {}",
                decision.target,
                err
            );
        }
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
            || message.contains("filecorrupt")
            || message.contains("file corrupt")
            || message.contains("body write aborted")
            || message.contains("decode response body")
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

        let db_governance = match governance {
            Some(governance) => governance,
            None => create_default_db_governance(&config)?,
        };
        db_governance.register_db(&id, &bucket_ranges, config.total_buckets)?;

        let file_manager =
            FileManager::from_config(&file_manager_config, &id, Arc::clone(&metrics_manager))?;
        let file_manager = Arc::new(file_manager);
        let db_state = Arc::new(DbStateHandle::new());
        let schema_manager = Arc::new(SchemaManager::new(config.num_columns));
        let db_lifecycle = Arc::new(DbLifecycle::new_initializing());
        // Fresh open starts from an empty DbState, so bucket-range layout must be initialized here.
        db_state.configure_multi_lsm(config.total_buckets, &bucket_ranges)?;
        let db = match Self::open_with_state(
            config,
            file_manager,
            db_state,
            Arc::clone(&db_lifecycle),
            id.clone(),
            Some(Arc::clone(&db_governance)),
            bucket_ranges,
            0,
            hybrid_cache_plan,
            metrics_manager,
            schema_manager,
        ) {
            Ok(db) => db,
            Err(err) => {
                let _ = db_governance.unregister_db(&id);
                return Err(err);
            }
        };
        db.memtable_manager.open()?;
        db.db_lifecycle.mark_open()?;
        Ok(db)
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// Marks every currently referenced file on READONLY volumes for asynchronous loading into
    /// primary storage.
    ///
    /// LSM files are loaded from lower levels first and VLog files last. Each file is placed on
    /// the highest-priority primary volume with enough capacity. Files added after this call are
    /// not included unless this method is called again.
    ///
    /// Returns the number of current READONLY files marked for loading.
    pub fn load_readonly_files_to_primary(&self) -> Result<usize> {
        let _access = self.begin_access()?;
        let marked = self.file_manager.mark_readonly_files_for_primary_load(
            &self.db_state,
            self.config.sst_pinned_metadata_max_level,
            self.config.sst_pinned_metadata_partitions_enabled,
        );
        if marked == 0 {
            return Ok(0);
        }
        self.file_manager
            .trigger_primary_tiering_if_needed(&self.db_state)?;
        if let Some(worker) = &self.primary_tiering_worker {
            worker.wake();
        }
        Ok(marked)
    }

    pub fn jni_direct_buffer_pool_config(&self) -> Result<(usize, usize)> {
        Ok((
            self.config.jni_direct_buffer_size_bytes()?,
            self.config.jni_direct_buffer_pool_size,
        ))
    }

    /// Start a schema update transaction.
    pub fn update_schema(&self) -> SchemaBuilder {
        self.schema_manager
            .builder_with_access(self.db_lifecycle.begin_owned_access().ok())
    }

    /// Return the current schema snapshot.
    pub fn current_schema(&self) -> Arc<Schema> {
        let _access = self.begin_access().ok();
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
        let mut desired_cf_ids = schema.column_family_id_list();
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
            expanded_versions = (0..expanded_scopes.len())
                .map(|_| Arc::new(LSMTreeVersion { levels: vec![] }))
                .collect();
            let multi_lsm_version =
                crate::db_state::MultiLSMTreeVersion::from_scopes_with_tree_versions(
                    snapshot.multi_lsm_version.total_buckets(),
                    &expanded_scopes,
                    expanded_versions,
                )?;
            db_state.store(crate::db_state::DbState {
                seq_id: snapshot.seq_id,
                topology_epoch: snapshot.topology_epoch.saturating_add(1),
                bucket_ranges: snapshot.bucket_ranges.clone(),
                multi_lsm_version,
                vlog_version: snapshot.vlog_version.clone(),
                active: snapshot.active.clone(),
                immutables: snapshot.immutables.clone(),
                truncation_cursors: snapshot.truncation_cursors.clone(),
                suggested_base_snapshot_id: None,
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
            topology_epoch: snapshot.topology_epoch.saturating_add(1),
            bucket_ranges: snapshot.bucket_ranges.clone(),
            multi_lsm_version,
            vlog_version: snapshot.vlog_version.clone(),
            active: snapshot.active.clone(),
            immutables: snapshot.immutables.clone(),
            truncation_cursors: snapshot.truncation_cursors.clone(),
            suggested_base_snapshot_id: None,
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
        let _topology_guard = self.lsm_topology_lock.lock().unwrap();
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
        let _access = self.begin_access()?;
        let schema = self.schema_manager.latest_schema();
        self.ensure_multi_lsm_scopes_for_schema_if_dirty(schema.as_ref())?;
        let column_family_id = options.resolve_column_family_id_cached(schema.as_ref())?;
        let num_columns = schema.num_columns_in_family(column_family_id).unwrap_or(0);
        let column_idx = column as usize;
        if column_idx >= num_columns {
            return Err(Error::IoError(format!(
                "Column index {} exceeds num_columns {}",
                column_idx, num_columns
            )));
        }
        let column = RefColumn::new(value_type, value.as_ref());
        let value_has_ttl = schema.value_has_ttl_in_family(column_family_id);
        let expired_at = self
            .ttl_provider
            .get_expiration_timestamp(if value_has_ttl {
                options.ttl_seconds
            } else {
                None
            });
        let mut columns: Vec<Option<RefColumn<'_>>> = vec![None; num_columns];
        columns[column_idx] = Some(column);
        let record = RefValue::new_with_expired_at(columns, expired_at);
        let key = RefKey::new_with_column_family(bucket, column_family_id, key.as_ref());
        let result = self.memtable_manager.put(&key, &record);
        // Record after the write completes (active lock released) to avoid re-entering the
        // manager while holding it.
        let decision = self.memtable_manager.record_adaptive_write(1);
        self.apply_adaptive_decision(decision);
        result
    }

    /// Insert a single key/value pair into the given bucket and column.
    pub fn put<K, V>(&self, bucket: u16, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_with_options(bucket, key, column, value, &self.default_write_options)
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

    /// Inserts byte values into one bucket and column using one database access.
    pub fn put_column_batch_with_options<'a, I>(
        &self,
        bucket: u16,
        column: u16,
        entries: I,
        options: &WriteOptions,
    ) -> Result<()>
    where
        I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
    {
        let _access = self.begin_access()?;
        let schema = self.schema_manager.latest_schema();
        self.ensure_multi_lsm_scopes_for_schema_if_dirty(schema.as_ref())?;
        let column_family_id = options.resolve_column_family_id_cached(schema.as_ref())?;
        let num_columns = schema.num_columns_in_family(column_family_id).unwrap_or(0);
        let column_idx = column as usize;
        if column_idx >= num_columns {
            return Err(Error::IoError(format!(
                "Column index {} exceeds num_columns {}",
                column_idx, num_columns
            )));
        }
        let expired_at = self.ttl_provider.get_expiration_timestamp(
            if schema.value_has_ttl_in_family(column_family_id) {
                options.ttl_seconds
            } else {
                None
            },
        );

        // Count consumed entries with a Cell so we can record accurate adaptive stats even if
        // the batch is only partially consumed before an error. Avoids collecting the whole
        // batch into a Vec just to measure its length.
        let count = std::cell::Cell::new(0u64);
        let entries = entries
            .into_iter()
            .inspect(|_| count.set(count.get() + 1))
            .map(|(key, value)| {
                let mut columns = vec![None; num_columns];
                columns[column_idx] = Some(RefColumn::new(ValueType::Put, value));
                (
                    RefKey::new_with_column_family(bucket, column_family_id, key),
                    RefValue::new_with_expired_at(columns, expired_at),
                )
            });
        let result = self
            .memtable_manager
            .put_validated_batch(entries, num_columns);
        let decision = self.memtable_manager.record_adaptive_write(count.get());
        self.apply_adaptive_decision(decision);
        result
    }

    /// Delete a single column value in the given bucket.
    pub fn delete<K>(&self, bucket: u16, key: K, column: u16) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.delete_with_options(bucket, key, column, &self.default_write_options)
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
        self.merge_with_options(bucket, key, column, value, &self.default_write_options)
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
        let _access = self.begin_access()?;
        let batch_len = batch.ops.len() as u64;
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
                    self.ttl_provider.get_expiration_timestamp(
                        if schema.value_has_ttl_in_family(column_family_id) {
                            ttl_secs
                        } else {
                            None
                        },
                    ),
                ),
                WriteOp::Delete(_) => (
                    Column::new(ValueType::Delete, Bytes::new()),
                    self.ttl_provider.get_expiration_timestamp(None),
                ),
                WriteOp::Merge(_, value, ttl_secs) => (
                    Column::new(ValueType::Merge, value),
                    self.ttl_provider.get_expiration_timestamp(
                        if schema.value_has_ttl_in_family(column_family_id) {
                            ttl_secs
                        } else {
                            None
                        },
                    ),
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
        let decision = self.memtable_manager.record_adaptive_write(batch_len);
        self.apply_adaptive_decision(decision);
        Ok(())
    }

    pub(crate) fn lifecycle_error(&self) -> Option<Error> {
        self.db_lifecycle.error()
    }

    pub(crate) fn force_close(&self) {
        if let Some(worker) = &self.primary_tiering_worker {
            worker.stop();
            worker.join();
        }
        if let Some(publisher) = &self.runtime_manifest_publisher {
            publisher.stop();
            publisher.join();
        }
        if let Some(poller) = &self.dedicated_poller {
            poller.stop();
        }
        self.memtable_manager.force_close();
        self.lsm_tree.shutdown_compaction();
        self.snapshot_manager.force_close();
        if let Err(err) = self.unregister_governance() {
            warn!(
                "failed to unregister db {} during force close: {}",
                self.id, err
            );
        }
        self.db_lifecycle.mark_closed();
    }

    /// Close the database and flush pending state.
    pub fn close(&self) -> Result<()> {
        match self.db_lifecycle.begin_close() {
            Ok(CloseTransition::AlreadyClosingOrClosed) => return Ok(()),
            Ok(CloseTransition::Transitioned) => {}
            Err(err) => {
                self.force_close();
                return Err(err);
            }
        }
        self.db_lifecycle.wait_for_accesses_to_drain();
        if let Some(err) = self.lifecycle_error() {
            self.force_close();
            return Err(err);
        }
        if let Some(worker) = &self.primary_tiering_worker {
            worker.stop();
            worker.join();
        }
        if let Err(err) = self.memtable_manager.close() {
            self.force_close();
            return Err(err);
        }
        if let Some(err) = self.lifecycle_error() {
            self.force_close();
            return Err(err);
        }
        self.lsm_tree.shutdown_compaction();
        // Stop the dedicated compaction poller before closing the snapshot manager so the
        // poller does not race with snapshot shutdown (e.g. trying to materialize a snapshot
        // while the materializer worker is being torn down).
        if let Some(poller) = &self.dedicated_poller {
            poller.stop();
            poller.join();
        }
        if let Some(publisher) = &self.runtime_manifest_publisher
            && let Err(err) = publisher.publish_current()
        {
            self.db_lifecycle.mark_error(err.clone());
            self.force_close();
            return Err(err);
        }
        if let Some(publisher) = &self.runtime_manifest_publisher {
            publisher.stop();
            publisher.join();
        }
        if let Err(err) = self.snapshot_manager.close() {
            self.force_close();
            return Err(err);
        }
        if let Some(err) = self.lifecycle_error() {
            self.force_close();
            return Err(err);
        }
        if let Err(err) = self.unregister_governance() {
            self.force_close();
            return Err(err);
        }
        self.db_lifecycle.mark_closed();
        Ok(())
    }

    /// Flush the active memtable and capture an LSM snapshot with a manifest.
    /// The manifest is materialized asynchronously after the flush completes.
    pub fn snapshot(&self) -> Result<u64> {
        let _access = self.begin_access()?;
        self.memtable_manager
            .create_snapshot(self.snapshot_manager.clone(), None)
    }

    /// Change the memtable implementation used by future active memtables in this process.
    ///
    /// This is a runtime-only setting: it does not modify this database's [`Config`] or persisted
    /// properties. With `flush_current = false`, the active memtable is unchanged and the target
    /// applies at its next natural rotation. With `flush_current = true`, a non-empty active
    /// memtable rotates through the normal manual-flush and auto-snapshot path, while an empty
    /// active table is immediately replaced when its implementation differs.
    pub fn switch_memtable_type(
        &self,
        memtable_type: crate::MemtableType,
        flush_current: bool,
    ) -> Result<()> {
        let _access = self.begin_access()?;
        self.memtable_manager
            .switch_memtable_type(memtable_type, flush_current)
    }

    /// Flush the active memtable, schedule manifest materialization, and invoke the callback with
    /// a [`crate::coordinator::ShardSnapshotInput`] once publication completes.
    pub fn snapshot_with_callback<F>(&self, callback: F) -> Result<u64>
    where
        F: Fn(Result<crate::coordinator::ShardSnapshotInput>) + Send + Sync + 'static,
    {
        let _access = self.begin_access()?;
        self.create_snapshot_with_callback(callback)
    }

    /// Creates a snapshot while the caller already owns database access.
    ///
    /// Topology cutover uses this under exclusive access, where taking another
    /// ordinary access guard would be rejected by design.
    fn create_snapshot_with_callback<F>(&self, callback: F) -> Result<u64>
    where
        F: Fn(Result<crate::coordinator::ShardSnapshotInput>) + Send + Sync + 'static,
    {
        let db_id = self.id.clone();
        let timestamp_seconds = self.now_seconds();
        let schema_manager = Arc::clone(&self.schema_manager);
        let wrapper: SnapshotCallback = Arc::new(move |result: Result<SnapshotManifestInfo>| {
            callback(result.and_then(|info| {
                let column_family_ids = schema_manager
                    .schema(info.latest_schema_id)?
                    .column_family_ids();
                Ok(crate::coordinator::ShardSnapshotInput {
                    ranges: info.bucket_ranges,
                    column_family_ids,
                    db_id: db_id.clone(),
                    snapshot_id: info.id,
                    manifest_path: info.manifest_path,
                    timestamp_seconds,
                    data_size_bytes: info.data_size_bytes,
                    incremental_data_size_bytes: info.incremental_data_size_bytes,
                })
            }));
        });
        self.memtable_manager
            .create_snapshot(self.snapshot_manager.clone(), Some(wrapper))
    }

    fn create_snapshot_and_wait(&self, operation: &str) -> Result<u64> {
        let (tx, rx) = std::sync::mpsc::channel();
        let snapshot_id = self.create_snapshot_with_callback(move |result| {
            let _ = tx.send(result);
        })?;
        rx.recv().map_err(|_| {
            Error::IoError(format!(
                "Snapshot {snapshot_id} callback was dropped during {operation}"
            ))
        })??;
        Ok(snapshot_id)
    }

    fn create_snapshot_and_wait_with_before_flush<F>(
        &self,
        operation: &str,
        before_flush: F,
    ) -> Result<u64>
    where
        F: FnOnce(u64) -> Result<()>,
    {
        let (tx, rx) = std::sync::mpsc::channel();
        let snapshot_id = self.memtable_manager.create_snapshot_with_before_flush(
            self.snapshot_manager.clone(),
            Some(Arc::new(move |result| {
                let _ = tx.send(result);
            })),
            before_flush,
        )?;
        match rx.recv() {
            Ok(result) => {
                result?;
                Ok(snapshot_id)
            }
            Err(_) => Err(Error::IoError(format!(
                "Snapshot callback disconnected during {operation}"
            ))),
        }
    }

    /// Cancel an in-flight snapshot before manifest publication completes.
    pub fn cancel_snapshot(&self, snapshot_id: u64) -> Result<bool> {
        let _access = self.begin_access()?;
        let cancelled = self.snapshot_manager.cancel_snapshot(snapshot_id)?;
        if cancelled {
            let fallback = self.snapshot_manager.suggested_base_fallback(snapshot_id);
            self.db_state
                .rebase_suggested_snapshot(snapshot_id, fallback);
        }
        Ok(cancelled)
    }

    /// Expire a snapshot and release its file references.
    pub fn expire_snapshot(&self, snapshot_id: u64) -> Result<bool> {
        let _access = self.begin_access()?;
        let fallback = self.snapshot_manager.suggested_base_fallback(snapshot_id);
        let expired = self.snapshot_manager.expire_snapshot(snapshot_id)?;
        if expired {
            self.db_state
                .rebase_suggested_snapshot(snapshot_id, fallback);
        }
        Ok(expired)
    }

    /// Retain a completed snapshot to avoid auto-expiration by retention processing.
    pub fn retain_snapshot(&self, snapshot_id: u64) -> bool {
        let Ok(_access) = self.begin_access() else {
            return false;
        };
        self.snapshot_manager.retain_snapshot(snapshot_id)
    }

    /// Build a ShardSnapshotInput for a given snapshot id.
    pub fn shard_snapshot_input(
        &self,
        snapshot_id: u64,
    ) -> Result<crate::coordinator::ShardSnapshotInput> {
        let _access = self.begin_access()?;
        let manifest = load_manifest_for_snapshot(&self.file_manager, snapshot_id)?;
        let column_family_ids = self
            .schema_manager
            .schema(manifest.latest_schema_id)?
            .column_family_ids();
        let manifest_name = snapshot_manifest_name(snapshot_id);
        let manifest_path = self
            .file_manager
            .get_metadata_file_full_path(&manifest_name)
            .ok_or_else(|| {
                Error::IoError(format!("Snapshot manifest not tracked: {}", manifest_name))
            })?;
        Ok(crate::coordinator::ShardSnapshotInput {
            ranges: self.db_state.load().bucket_ranges.clone(),
            column_family_ids,
            db_id: self.id.clone(),
            snapshot_id,
            manifest_path,
            timestamp_seconds: 0,
            data_size_bytes: manifest.data_size_bytes,
            incremental_data_size_bytes: manifest.incremental_data_size_bytes,
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
        db_governance: Option<Arc<dyn crate::governance::DbGovernance>>,
        bucket_ranges: Vec<RangeInclusive<u16>>,
        initial_vlog_file_seq: u32,
        hybrid_cache_plan: Option<crate::config::HybridCacheVolumePlan>,
        metrics_manager: Arc<MetricsManager>,
        schema_manager: Arc<SchemaManager>,
    ) -> Result<Self> {
        crate::properties::refresh_db_properties(file_manager.as_ref(), &id, &config)?;
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
        lsm_tree.set_sst_read_metadata_cache_mode(config.sst_read_metadata_cache_mode);
        lsm_tree.set_sst_pinned_metadata_max_level(config.sst_pinned_metadata_max_level);
        lsm_tree.set_sst_pinned_metadata_partitions_enabled(
            config.sst_pinned_metadata_partitions_enabled,
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
        Self::ensure_multi_lsm_scopes_for_schema(&db_state, latest_schema.as_ref())?;
        let lsm_topology_lock = Arc::new(Mutex::new(()));
        let last_scope_synced_schema_version = AtomicU64::new(latest_schema.version());
        let lsm_tree = Arc::new(lsm_tree);
        let mut memtable_writer_options = crate::compaction::build_writer_options(
            &config,
            0,
            config.data_file_type,
            runtime_num_columns,
        )?;
        match &mut memtable_writer_options {
            WriterOptions::Sst(sst_options) => {
                sst_options.metrics =
                    Some(metrics_manager.sst_writer_metrics(sst_options.compression));
            }
            WriterOptions::Parquet(_) => {}
        }
        let vlog_store = Arc::new(VlogStore::new(
            Arc::clone(&file_manager),
            memtable_writer_options.buffer_size(),
            value_separation_threshold,
        ));
        vlog_store.ensure_next_file_seq_at_least(initial_vlog_file_seq);
        // Compaction setup
        let compaction_options =
            crate::compaction::build_compaction_config(&config, runtime_num_columns)?;
        let compaction_worker: Option<Arc<dyn crate::compaction::CompactionWorker>> =
            if config.compaction_mode == crate::config::CompactionMode::Dedicated {
                // Dedicated compaction mode: no in-process compaction worker. A separate
                // dedicated compactor process publishes results to the shared volume; the
                // writer's poller (started below) discovers, validates, and applies them.
                // Setting the worker to None disables compaction triggers and auto-split.
                info!("db compaction mode: dedicated (no in-process worker)");
                None
            } else if let Some(addr) = config.compaction_remote_addr.clone() {
                // Remote compaction with a local fallback. The remote worker is constructed
                // without connecting (capabilities are fetched lazily on the first compaction), so
                // `Db::open` succeeds even when the compactor is down. The local worker backs
                // every remote attempt so a transient remote failure falls back to local
                // compaction (or is skipped, per the configured failure mode) instead of failing
                // the DB.
                let remote = crate::compaction::RemoteCompactionWorker::new(
                    addr,
                    Arc::clone(&file_manager),
                    Arc::downgrade(&lsm_tree),
                    config.clone(),
                    ttl_config.clone(),
                    Duration::from_millis(config.compaction_remote_timeout_ms),
                    Arc::clone(&metrics_manager),
                    Arc::clone(&schema_manager),
                )?;
                let local = crate::compaction::LocalCompactionWorker::new(
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
                );
                Some(Arc::new(
                    crate::compaction::ResilientRemoteCompactionWorker::new(
                        remote,
                        local,
                        config.compaction_remote_failure_mode,
                        Arc::clone(&db_lifecycle),
                    ),
                ))
            } else {
                Some(Arc::new(crate::compaction::LocalCompactionWorker::new(
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
                )))
            };
        info!(
            "db compaction configured: l0_limit={} l1_base={} multiplier={} max_level={} target_file_size={}",
            compaction_options.l0_file_limit,
            compaction_options.l1_base_bytes,
            compaction_options.level_size_multiplier,
            compaction_options.max_level,
            compaction_options.target_file_size
        );
        lsm_tree.configure_compaction(compaction_options, compaction_worker.clone());

        let snapshot_manager = SnapshotManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&schema_manager),
            Arc::clone(&db_lifecycle),
            config.snapshot_retention,
            config.snapshot_only_track,
            config.snapshot_disable_incremental_base_link,
            bucket_ranges.clone(),
        );

        // Memtable manager setup
        let memtable_manager = Arc::new(MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity,
                buffer_count: config.memtable_buffer_count,
                memtable_type: config.memtable_type,
                writer_options: memtable_writer_options,
                num_columns: runtime_num_columns,
                write_stall_limit: config.resolved_write_stall_limit(),
                schema_manager: Some(Arc::clone(&schema_manager)),
                auto_snapshot_manager: if config.snapshot_on_flush
                    || (config.compaction_mode == crate::config::CompactionMode::Dedicated
                        && !config.runtime_manifests_enabled())
                {
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
        )?);

        if let Err(err) = file_manager.load_replica_catalog_as_writer_owner() {
            log::warn!("failed to load replica catalog: {}", err);
        }
        let runtime_manifest_publisher = if config.runtime_manifests_enabled() {
            Some(Arc::new(
                crate::runtime_manifest::publisher::RuntimeManifestPublisherHandle::open(
                    Arc::clone(&file_manager),
                    Arc::clone(&schema_manager),
                    Arc::clone(&db_state),
                    Arc::clone(&db_lifecycle),
                )?,
            ))
        } else {
            None
        };
        let adoption_coordinator = Arc::new(rescale::AdoptionCoordinator::new(
            (id.clone(), config.clone()),
            Arc::clone(&file_manager),
            Arc::clone(&db_state),
            Arc::clone(&db_lifecycle),
            Arc::clone(&memtable_manager),
            snapshot_manager.clone(),
            runtime_manifest_publisher.as_ref().map(Arc::clone),
        ));
        let adoption_tick = {
            let coordinator = Arc::clone(&adoption_coordinator);
            Arc::new(move || coordinator.tick())
        };
        let primary_tiering_worker =
            file_manager.start_primary_tiering_worker(&db_state, Some(adoption_tick))?;

        // Mark the DB as open before starting background observers so their
        // `ensure_open()` checks pass immediately.
        db_lifecycle.mark_open()?;

        if let Some(publisher) = &runtime_manifest_publisher {
            publisher.start();
        }

        // Start the dedicated compaction result poller if in dedicated mode.
        let dedicated_poller = if config.compaction_mode == crate::config::CompactionMode::Dedicated
        {
            let poller =
                crate::compaction::dedicated_poller::DedicatedCompactionPollerHandle::start(
                    Arc::clone(&file_manager),
                    Arc::clone(&lsm_tree),
                    snapshot_manager.clone(),
                    Arc::clone(&memtable_manager),
                    Arc::clone(&schema_manager),
                    Arc::clone(&db_lifecycle),
                    Arc::clone(&db_state),
                    runtime_manifest_publisher.as_ref().map(Arc::clone),
                    Arc::clone(&lsm_topology_lock),
                    Duration::from_millis(config.compaction_dedicated_poll_interval_ms),
                    config.clone(),
                );
            Some(poller)
        } else {
            None
        };

        Ok(Self {
            id,
            db_governance,
            db_lifecycle,
            db_state,
            config,
            file_manager: Arc::clone(&file_manager),
            lsm_tree,
            memtable_manager,
            vlog_store,
            snapshot_manager,
            schema_manager,
            lsm_topology_lock,
            last_scope_synced_schema_version,
            default_write_options: WriteOptions::default(),
            default_read_options: ReadOptions::default(),
            default_scan_options: ScanOptions::default(),
            time_provider,
            ttl_provider,
            dedicated_poller,
            primary_tiering_worker,
            adoption_coordinator,
            runtime_manifest_publisher,
        })
    }

    fn unregister_governance(&self) -> Result<()> {
        if let Some(governance) = &self.db_governance {
            governance.unregister_db(&self.id)?;
        }
        Ok(())
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
            .restore_active_memtable_snapshot_to_l0(&self.file_manager, segments)?;
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
    ) -> Result<()> {
        let restored = self
            .memtable_manager
            .restore_active_memtable_snapshot_to_l0(source_file_manager, segments)?;
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
        self.get_with_options(bucket, key, &self.default_read_options)
    }

    /// Read several keys from one consistent database-state snapshot.
    pub fn multi_get<K: AsRef<[u8]>>(
        &self,
        keys: &[(u16, K)],
    ) -> Result<Vec<Option<Vec<Option<Bytes>>>>> {
        self.multi_get_with_options(keys, &self.default_read_options)
    }

    pub fn multi_get_with_options<K: AsRef<[u8]>>(
        &self,
        keys: &[(u16, K)],
        options: &ReadOptions,
    ) -> Result<Vec<Option<Vec<Option<Bytes>>>>> {
        let _access = self.begin_access()?;
        let schema = self.schema_manager.latest_schema();
        let column_family_id = options.resolve_column_family_id_cached(schema.as_ref())?;
        let num_columns = schema.num_columns_in_family(column_family_id).unwrap_or(0);
        if let Some(max_index) = options.max_index()
            && max_index >= num_columns
        {
            return Err(Error::IoError(format!(
                "max_index {} in ReadOptions exceeds num_columns {}",
                max_index, num_columns
            )));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let key_count = keys.len() as u64;
        let mut unique = Vec::<(u16, Vec<u8>)>::new();
        let mut positions = Vec::with_capacity(keys.len());
        let mut seen = HashMap::<(u16, Vec<u8>), usize>::new();
        for (bucket, key) in keys {
            let entry = (*bucket, key.as_ref().to_vec());
            let next = unique.len();
            let index = *seen.entry(entry.clone()).or_insert_with(|| {
                unique.push(entry);
                next
            });
            positions.push(index);
        }

        let snapshot = self.db_state.load();
        let selected_columns = options.columns();
        let masks = options.masks(num_columns);
        let selected_mask = masks.selected_mask.as_deref();
        let base_decode_mask = masks.base_mask.as_ref();
        let mask_size = base_decode_mask.len();
        let mut requests = Vec::with_capacity(unique.len());
        for (bucket, key) in unique {
            let encoded_key = encode_key(bucket, column_family_id, key.as_slice());
            let mut terminal_mask = (num_columns > 1).then(|| vec![0u8; mask_size]);
            let mut values = Vec::new();
            let mut stopped = snapshot.key_is_truncated(bucket, column_family_id, key.as_slice());
            if !stopped {
                self.memtable_manager.get_all_with_snapshot_until(
                    Arc::clone(&snapshot),
                    encoded_key.as_ref(),
                    |raw, source_schema| {
                        let mut raw_value = Bytes::copy_from_slice(raw);
                        let value = if source_schema.version() == schema.version() {
                            decode_value_masked(
                                &mut raw_value,
                                source_schema
                                    .num_columns_in_family(column_family_id)
                                    .unwrap_or(0),
                                base_decode_mask,
                                None,
                            )?
                        } else {
                            let decoded = decode_value(
                                &mut raw_value,
                                source_schema
                                    .num_columns_in_family(column_family_id)
                                    .unwrap_or(0),
                            )?;
                            self.schema_manager.evolve_value(
                                decoded,
                                source_schema.version(),
                                schema.version(),
                            )?
                        };
                        if let Some(mask) = terminal_mask.as_mut() {
                            for (idx, column) in
                                value.columns().iter().enumerate().take(num_columns)
                            {
                                if column
                                    .as_ref()
                                    .is_some_and(|column| column.value_type().is_terminal())
                                {
                                    mask[idx / 8] |= 1 << (idx % 8);
                                }
                            }
                            if let Some(selected) = selected_mask {
                                for (idx, mask_byte) in mask.iter_mut().enumerate().take(mask_size)
                                {
                                    *mask_byte &= selected[idx];
                                }
                            }
                        }
                        let stop = num_columns == 1 && value.is_terminal();
                        values.push(value);
                        if stop {
                            stopped = true;
                            Ok(ControlFlow::Break(()))
                        } else {
                            Ok(ControlFlow::Continue(()))
                        }
                    },
                )?;
            }
            let mut decode_mask = base_decode_mask.to_vec();
            if let Some(mask) = terminal_mask.as_ref() {
                for (idx, mask_byte) in mask.iter().enumerate().take(mask_size) {
                    decode_mask[idx] &= !*mask_byte;
                }
            }
            requests.push(BatchGetRequest {
                bucket,
                encoded_key,
                values,
                terminal_mask,
                decode_mask,
                stopped,
            });
        }
        if let Err(err) = self.lsm_tree.get_many_with_snapshot(
            &self.file_manager,
            Arc::clone(&snapshot),
            &mut requests,
            schema.as_ref(),
            self.schema_manager.as_ref(),
            selected_columns,
            selected_mask,
            column_family_id,
        ) {
            self.maybe_mark_error_on_read(&err);
            return Err(err);
        }
        let unique_results = (|| {
            let mut unique_results = Vec::with_capacity(requests.len());
            for request in requests {
                let values = request
                    .values
                    .into_iter()
                    .filter(|value| !self.ttl_provider.expired(&value.expired_at))
                    .rev()
                    .collect::<Vec<_>>();
                if values.is_empty() {
                    unique_results.push(None);
                    continue;
                }
                let merged = Value::merge_all_in_column_family(
                    values,
                    &schema,
                    column_family_id,
                    Some(self.time_provider.as_ref()),
                )?;
                let result = value_to_vec_of_columns_with_vlog(
                    merged,
                    |pointer| {
                        self.vlog_store
                            .read_pointer(&snapshot.vlog_version, pointer)
                    },
                    &schema,
                    column_family_id,
                    Some(self.time_provider.as_ref()),
                )?;
                unique_results.push(result.map(|columns| match selected_columns {
                    Some(selected) => select_projected_columns(columns, selected),
                    None => columns,
                }));
            }
            Ok::<_, Error>(unique_results)
        })();
        let unique_results = match unique_results {
            Ok(results) => results,
            Err(err) => {
                self.maybe_mark_error_on_read(&err);
                return Err(err);
            }
        };
        let result: Result<Vec<_>> = Ok(positions
            .into_iter()
            .map(|index| unique_results[index].clone())
            .collect());
        let decision = self.memtable_manager.record_adaptive_point_read(key_count);
        self.apply_adaptive_decision(decision);
        result
    }

    pub fn get_with_options(
        &self,
        bucket: u16,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<Bytes>>>> {
        let _access = self.begin_access()?;
        // Wrap the lookup in a closure so we can uniformly record one point read on every return
        // path (including truncated miss and empty values) before applying any adaptive decision.
        let result = (|| {
            let schema = self.schema_manager.latest_schema();
            let column_family_id = options.resolve_column_family_id_cached(schema.as_ref())?;
            let num_columns = schema.num_columns_in_family(column_family_id).unwrap_or(0);
            if let Some(max_index) = options.max_index()
                && max_index >= num_columns
            {
                return Err(Error::IoError(format!(
                    "max_index {} in ReadOptions exceeds num_columns {}",
                    max_index, num_columns
                )));
            }
            let snapshot = self.db_state.load();
            if snapshot.key_is_truncated(bucket, column_family_id, key) {
                return Ok(None);
            }
            let encoded_key = encode_key(bucket, column_family_id, key);
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
            let mut values: Vec<Value> = Vec::new();
            let mut stopped_by_memtable_terminal = false;
            self.memtable_manager.get_all_with_snapshot_until(
                Arc::clone(&snapshot),
                encoded_key.as_ref(),
                |raw, source_schema| {
                    let mut raw_value = Bytes::copy_from_slice(raw);
                    let value = if source_schema.version() == schema.version() {
                        decode_value_masked(
                            &mut raw_value,
                            source_schema
                                .num_columns_in_family(column_family_id)
                                .unwrap_or(0),
                            decode_mask,
                            None,
                        )?
                    } else {
                        let decoded = decode_value(
                            &mut raw_value,
                            source_schema
                                .num_columns_in_family(column_family_id)
                                .unwrap_or(0),
                        )?;
                        self.schema_manager.evolve_value(
                            decoded,
                            source_schema.version(),
                            schema.version(),
                        )?
                    };
                    // Keep the established multi-column terminal-mask behavior unchanged.
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
                    // A terminal value in a physical single-column family, regardless of TTL, hides
                    // all older versions. Multi-column reads keep traversing for per-column masking.
                    let stop = num_columns == 1 && value.is_terminal();
                    values.push(value);
                    if stop {
                        stopped_by_memtable_terminal = true;
                        Ok(ControlFlow::Break(()))
                    } else {
                        Ok(ControlFlow::Continue(()))
                    }
                },
            )?;
            // A single-column terminal cuts off the rest of the lookup before older layers are read.
            // Multi-column reads continue through the LSM mask path.
            let mut should_stop = stopped_by_memtable_terminal;
            let lsm_values = if should_stop {
                Vec::new()
            } else {
                match self.lsm_tree.get_with_snapshot(
                    &self.file_manager,
                    Arc::clone(&snapshot),
                    bucket,
                    encoded_key.as_ref(),
                    schema.as_ref(),
                    self.schema_manager.as_ref(),
                    selected_columns,
                    selected_mask,
                    terminal_mask.as_deref_mut(),
                ) {
                    Ok(values) => values,
                    Err(err) => {
                        self.maybe_mark_error_on_read(&err);
                        return Err(err);
                    }
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
            let merged = Value::merge_all_in_column_family(
                values,
                &schema,
                column_family_id,
                Some(self.time_provider.as_ref()),
            )?;
            let result = value_to_vec_of_columns_with_vlog(
                merged,
                |pointer| {
                    self.vlog_store
                        .read_pointer(&snapshot.vlog_version, pointer)
                },
                &schema,
                column_family_id,
                Some(self.time_provider.as_ref()),
            );
            match result {
                Ok(value) => Ok(value.map(|columns| {
                    if let Some(selected_columns) = selected_columns {
                        select_projected_columns(columns, selected_columns)
                    } else {
                        columns
                    }
                })),
                Err(err) => {
                    self.maybe_mark_error_on_read(&err);
                    Err(err)
                }
            }
        })();

        // Record exactly one point read on every return path (truncated miss, empty-values miss,
        // error, or hit). Misses must count too: a Vec memtable still pays the point-lookup cost,
        // and uncounted misses would prevent rollback away from Vec.
        let decision = self.memtable_manager.record_adaptive_point_read(1);
        self.apply_adaptive_decision(decision);
        result
    }

    pub fn scan<'a>(&'a self, bucket: u16, range: Range<&[u8]>) -> Result<DbIterator<'a>> {
        self.scan_with_options(bucket, range, &self.default_scan_options)
    }

    pub fn scan_bounds<'a>(
        &'a self,
        bucket: u16,
        start_key_inclusive: Option<&[u8]>,
        end_key_exclusive: Option<&[u8]>,
    ) -> Result<DbIterator<'a>> {
        self.scan_with_options_bounds(
            bucket,
            start_key_inclusive,
            end_key_exclusive,
            &self.default_scan_options,
        )
    }

    pub fn scan_with_options<'a>(
        &'a self,
        bucket: u16,
        range: Range<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator<'a>> {
        self.scan_with_options_bounds(bucket, Some(range.start), Some(range.end), options)
    }

    pub fn scan_with_options_bounds<'a>(
        &'a self,
        bucket: u16,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator<'a>> {
        let access_guard = self.begin_access()?;
        let decision = self.memtable_manager.record_adaptive_range_scan();
        self.apply_adaptive_decision(decision);
        let snapshot = self.db_state.load();
        let schema = self.schema_manager.latest_schema();
        let resolved_scan_options = options.resolve_cached(&schema)?;
        let column_family_id = resolved_scan_options.column_family_id;
        let num_columns = schema.num_columns_in_family(column_family_id).unwrap_or(0);
        if let Some(max_index) = options.max_index()
            && max_index >= num_columns
        {
            return Err(Error::IoError(format!(
                "max_index {} in ScanOptions exceeds num_columns {}",
                max_index, num_columns
            )));
        }
        let truncation_cursor = snapshot.truncation_cursor(bucket, column_family_id);
        let start_key = match truncation_cursor {
            Some(ref cursor) if start.is_none_or(|candidate| candidate <= cursor.as_slice()) => {
                encode_scan_key_after(bucket, column_family_id, cursor.as_slice())
            }
            _ => encode_scan_key(bucket, column_family_id, start.unwrap_or(&[])),
        };
        let end_key = if let Some(end) = end {
            Some(encode_scan_key(bucket, column_family_id, end))
        } else {
            Some(encode_next_column_family_scan_key(
                bucket,
                column_family_id,
            )?)
        };
        let memtable_iters = self
            .memtable_manager
            .scan_memtable_iterators_with_snapshot(
                Arc::clone(&snapshot),
                Arc::clone(&schema),
                column_family_id,
                options.columns(),
                Some(start_key.clone()),
                end_key.clone(),
                options.max_rows(),
            )?;
        let end_bound = end_key.map(|end_key| (end_key, false));
        let lsm_iters = self.lsm_tree.scan_with_snapshot(
            &self.file_manager,
            Arc::clone(&snapshot),
            Arc::clone(&schema),
            Arc::clone(&self.schema_manager),
            options.read_ahead_bytes()?,
            options.columns(),
            bucket,
            column_family_id,
            start_key.as_ref(),
            end_bound.as_ref().map(|(end, _)| end.as_ref()),
            options.preload_scan_cursor_block(),
        );
        let lsm_iters = match lsm_iters {
            Ok(result) => result,
            Err(err) => {
                self.maybe_mark_error_on_read(&err);
                return Err(err);
            }
        };
        let mut iter = DbIterator::new(
            memtable_iters,
            lsm_iters,
            DbIteratorOptions {
                end_bound,
                lower_bound_exclusive: truncation_cursor
                    .as_deref()
                    .map(|cursor| encode_scan_key(bucket, column_family_id, cursor)),
                max_rows: options.max_rows(),
                snapshot,
                memtable_manager: Some(self.memtable_manager.as_ref()),
                access_guard: Some(access_guard),
                vlog_store: Arc::clone(&self.vlog_store),
                ttl_provider: Arc::clone(&self.ttl_provider),
                schema: resolved_scan_options.effective_schema,
                column_family_id,
                should_stop_at_block_boundary: options.should_stop_at_block_boundary(),
            },
        );
        if let Err(err) = iter.seek(start_key.as_ref()) {
            self.maybe_mark_error_on_read(&err);
            return Err(err);
        }
        Ok(iter)
    }

    pub fn advance_truncation_cursor(
        &self,
        bucket: u16,
        column_family: &str,
        key: &[u8],
    ) -> Result<()> {
        let _access = self.begin_access()?;
        let column_family_id = self
            .schema_manager
            .latest_schema()
            .resolve_column_family_id(Some(column_family))?;
        self.db_state
            .advance_truncation_cursor(bucket, column_family_id, key);
        Ok(())
    }

    pub fn advance_truncation_cursor_by_id(
        &self,
        bucket: u16,
        column_family_id: u8,
        key: &[u8],
    ) -> Result<()> {
        let _access = self.begin_access()?;
        self.db_state
            .advance_truncation_cursor(bucket, column_family_id, key);
        Ok(())
    }

    pub fn truncation_cursor(&self, bucket: u16, column_family: &str) -> Result<Option<Vec<u8>>> {
        let _access = self.begin_access()?;
        let column_family_id = self
            .schema_manager
            .latest_schema()
            .resolve_column_family_id(Some(column_family))?;
        let snapshot = self.db_state.load();
        Ok(snapshot.truncation_cursor(bucket, column_family_id))
    }

    pub fn truncation_cursor_by_id(
        &self,
        bucket: u16,
        column_family_id: u8,
    ) -> Result<Option<Vec<u8>>> {
        let _access = self.begin_access()?;
        let snapshot = self.db_state.load();
        Ok(snapshot.truncation_cursor(bucket, column_family_id))
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
        if let Err(err) = self.close() {
            error!("db drop forced close after error: {}", err);
            self.force_close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MergeOperator;
    use crate::db_state::full_bucket_range;
    use crate::file::{File, SequentialWriteFile};
    use crate::paths::{GOVERNANCE_MANIFEST_POINTER_NAME, snapshot_active_data_relative_path};
    use crate::snapshot::SnapshotLifecycleState;
    use crate::r#type::encode_merge_separated_array;
    use crate::{
        CompactionMode, DbBuilder, DbGovernance, GovernanceMode, MemtableType, ReadOptions,
        RuntimeManifestMode, ScanOptions, U32CounterMergeOperator, U64CounterMergeOperator,
        VolumeDescriptor, VolumeUsageKind, WriteOptions,
    };
    use bytes::BytesMut;
    use serial_test::serial;
    use size::Size;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Barrier, Mutex, mpsc};
    use std::time::Duration;

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

    fn runtime_manifest_store(db: &Db) -> crate::runtime_manifest::RuntimeManifestStore {
        crate::runtime_manifest::RuntimeManifestStore::new(Arc::clone(&db.file_manager))
    }

    #[test]
    #[serial(file)]
    fn writer_persists_plain_db_properties_without_volume_credentials() {
        let root = "/tmp/db_writer_properties";
        cleanup_test_root(root);
        let mut volume = VolumeDescriptor::single_volume(format!("file://{root}")).remove(0);
        volume.access_id = Some("writer-ak".to_string());
        volume.secret_key = Some("writer-sk".to_string());
        let config = Config {
            volumes: vec![volume],
            l0_file_limit: 9,
            ..Config::default()
        };
        let db = DbBuilder::new(config.clone())
            .bucket_ranges(vec![0..=0])
            .db_id("properties-shard")
            .open()
            .unwrap();
        let properties_path = format!("{root}/properties-shard/PROPERTIES");
        let contents = std::fs::read_to_string(&properties_path).unwrap();
        let parsed: toml::Value = toml::from_str(&contents).unwrap();

        assert_eq!(parsed["db_id"].as_str(), Some("properties-shard"));
        assert_eq!(parsed["config"]["l0_file_limit"].as_integer(), Some(9));
        assert!(parsed["config"]["volumes"][0].get("access_id").is_none());
        assert!(parsed["config"]["volumes"][0].get("secret_key").is_none());
        assert!(!contents.contains("writer-ak"));
        assert!(!contents.contains("writer-sk"));

        db.close().unwrap();

        let mut restarted_config = config;
        restarted_config.l0_file_limit = 15;
        restarted_config.volumes[0].access_id = Some("rotated-ak".to_string());
        restarted_config.volumes[0].secret_key = Some("rotated-sk".to_string());
        let restarted = DbBuilder::new(restarted_config)
            .bucket_ranges(vec![0..=0])
            .db_id("properties-shard")
            .open()
            .unwrap();
        let refreshed_contents = std::fs::read_to_string(&properties_path).unwrap();
        let refreshed: toml::Value = toml::from_str(&refreshed_contents).unwrap();

        assert_eq!(refreshed["config"]["l0_file_limit"].as_integer(), Some(15));
        assert!(!refreshed_contents.contains("rotated-ak"));
        assert!(!refreshed_contents.contains("rotated-sk"));

        restarted.close().unwrap();
        cleanup_test_root(root);
    }

    fn wait_for_runtime_generation_at_least(
        store: &crate::runtime_manifest::RuntimeManifestStore,
        generation: u64,
    ) -> crate::runtime_manifest::LoadedRuntimeManifest {
        for _ in 0..100 {
            if let Some(current) = store.load_current().unwrap()
                && current.generation >= generation
            {
                return current;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("runtime manifest did not reach generation {generation}");
    }

    type GovernanceCall = (String, Vec<RangeInclusive<u16>>, u32);

    #[derive(Default)]
    struct RecordingGovernance {
        register_calls: Mutex<Vec<GovernanceCall>>,
        unregister_calls: Mutex<Vec<String>>,
    }

    impl DbGovernance for RecordingGovernance {
        fn register_db(
            &self,
            db_id: &str,
            ranges: &[RangeInclusive<u16>],
            total_buckets: u32,
        ) -> Result<()> {
            self.register_calls
                .lock()
                .expect("recording governance register lock")
                .push((db_id.to_string(), ranges.to_vec(), total_buckets));
            Ok(())
        }

        fn unregister_db(&self, db_id: &str) -> Result<()> {
            self.unregister_calls
                .lock()
                .expect("recording governance unregister lock")
                .push(db_id.to_string());
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
        let cancel_err = db.cancel_snapshot(0).unwrap_err();
        assert!(matches!(cancel_err, Error::InvalidState(_)));
        assert!(!db.retain_snapshot(0));

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_cancel_snapshot_returns_cancelled_error_and_consumes_snapshot_id() {
        let root = "/tmp/db_cancel_snapshot";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = open_db(config);
        for i in 0..256 {
            let key = format!("k-{i}");
            let value = format!("value-{i}");
            db.put(0, key.as_bytes(), 0, value.as_bytes()).unwrap();
        }

        let (tx, rx) = mpsc::channel();
        let snapshot_id = db
            .snapshot_with_callback(move |result| {
                tx.send(result).expect("send cancelled snapshot result");
            })
            .unwrap();

        assert!(db.cancel_snapshot(snapshot_id).unwrap());
        let callback_result = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("receive cancelled snapshot result");
        assert!(matches!(callback_result, Err(Error::CancelledError(_))));
        let _ = db.memtable_manager.wait_for_flushes();
        assert!(
            db.snapshot_manager
                .wait_for_materialization(Duration::from_secs(10))
        );
        assert!(!db.expire_snapshot(snapshot_id).unwrap());
        let manifest_path = db
            .file_manager
            .metadata_path(&snapshot_manifest_name(snapshot_id));
        let active_data_path = db
            .file_manager
            .metadata_path(&snapshot_active_data_relative_path(snapshot_id));
        assert!(
            !db.file_manager
                .has_metadata_file(&snapshot_manifest_name(snapshot_id))
        );
        assert!(
            !db.file_manager
                .meta_volume
                .fs()
                .exists(&manifest_path)
                .expect("check cancelled snapshot manifest")
        );
        assert!(
            !db.file_manager
                .meta_volume
                .fs()
                .exists(&active_data_path)
                .expect("check cancelled active snapshot data")
        );

        let next_snapshot_id = db.snapshot().unwrap();
        assert_eq!(snapshot_id + 1, next_snapshot_id);

        db.close().unwrap();
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_cancel_snapshot_returns_false_once_publication_starts() {
        let root = "/tmp/db_cancel_snapshot_after_publication_start";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = open_db(config);

        let snapshot = db.snapshot_manager.create_snapshot(None);
        assert_eq!(snapshot.try_begin_publication(), Ok(()));

        assert!(!db.cancel_snapshot(snapshot.id).unwrap());
        assert!(db.expire_snapshot(snapshot.id).unwrap());
        assert_eq!(
            snapshot.lifecycle_state(),
            SnapshotLifecycleState::CommitStartedExpireRequested
        );

        db.close().unwrap();
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_failed_snapshot_completes_callback_and_releases_snapshot() {
        let root = "/tmp/db_failed_snapshot_callback";
        cleanup_test_root(root);
        let db = open_db(config_with_small_memtable(root));
        let (tx, rx) = mpsc::channel();
        let snapshot = db
            .snapshot_manager
            .create_snapshot(Some(Arc::new(move |result| {
                tx.send(result).expect("send failed snapshot result");
            })));

        db.snapshot_manager.fail_snapshot(
            snapshot.id,
            Error::IoError("flush worker failed before materialization".to_string()),
        );

        let callback_result = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("receive failed snapshot result");
        assert!(matches!(callback_result, Err(Error::IoError(_))));
        assert!(!db.expire_snapshot(snapshot.id).unwrap());

        db.close().unwrap();
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_snapshot_callback_completes_when_flush_worker_is_unavailable() {
        let root = "/tmp/db_snapshot_flush_worker_unavailable";
        cleanup_test_root(root);
        let db = open_db(config_with_small_memtable(root));
        db.memtable_manager.force_close();

        let (tx, rx) = mpsc::channel();
        let err = db
            .snapshot_with_callback(move |result| {
                tx.send(result).expect("send failed snapshot result");
            })
            .unwrap_err();
        assert!(matches!(err, Error::IoError(_)));
        let callback_result = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("receive failed snapshot result");
        assert!(matches!(callback_result, Err(Error::IoError(_))));

        db.force_close();
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_snapshot_completes_while_writer_rotates_multiple_memtables() {
        let root = "/tmp/db_snapshot_continuous_writes";
        cleanup_test_root(root);
        let primary = format!("file://{root}/primary");
        let snapshot = format!("file://{root}/snapshot");
        let db = Arc::new(open_db(Config {
            memtable_capacity: Size::from_kib(8),
            memtable_buffer_count: 2,
            l0_file_limit: 64,
            file_transfer_concurrency: 2,
            num_columns: 1,
            volumes: vec![
                VolumeDescriptor::new(primary, vec![VolumeUsageKind::PrimaryDataPriorityHigh]),
                VolumeDescriptor::new(
                    snapshot,
                    vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
                ),
            ],
            ..Config::default()
        }));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer_db = Arc::clone(&db);
        let writer_stop = Arc::clone(&stop);
        let writer = std::thread::spawn(move || {
            let value = vec![b'v'; 512];
            let mut index = 0_u64;
            while !writer_stop.load(AtomicOrdering::Relaxed) {
                let key = format!("key-{}", index % 256);
                writer_db.put(0, key.as_bytes(), 0, &value)?;
                index += 1;
                if index.is_multiple_of(64) {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            Ok::<(), Error>(())
        });

        std::thread::sleep(Duration::from_millis(50));
        let (tx, rx) = mpsc::channel();
        let mut snapshot_id = None;
        let snapshot_result =
            (|| -> Result<(u64, Result<crate::coordinator::ShardSnapshotInput>)> {
                let id = db.snapshot_with_callback(move |result| {
                    let _ = tx.send(result);
                })?;
                snapshot_id = Some(id);
                let result = rx.recv_timeout(Duration::from_secs(5)).map_err(|err| {
                    Error::IoError(format!(
                        "snapshot did not complete while writes continued: {err}"
                    ))
                })?;
                Ok((id, result))
            })();
        stop.store(true, AtomicOrdering::Relaxed);
        let writer_result = writer.join().expect("continuous writer did not panic");

        if let Err(err) = &snapshot_result {
            if let Some(snapshot_id) = snapshot_id {
                let _ = db.cancel_snapshot(snapshot_id);
            }
            db.force_close();
            cleanup_test_root(root);
            panic!("snapshot result: {err}");
        }
        let (snapshot_id, result) = snapshot_result.expect("snapshot result");
        writer_result.expect("continuous writer result");
        assert!(result.is_ok(), "snapshot result: {result:?}");
        assert!(db.expire_snapshot(snapshot_id).unwrap());
        db.close().unwrap();
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_concurrent_snapshots_follow_id_order_during_writes() {
        const WRITER_COUNT: usize = 3;
        const SNAPSHOT_COUNT: usize = 8;
        const WRITES_PER_WRITER: usize = 300;

        let root = "/tmp/db_concurrent_snapshots";
        cleanup_test_root(root);
        let db = Arc::new(open_db(Config {
            memtable_capacity: Size::from_kib(8),
            memtable_buffer_count: 4,
            l0_file_limit: 128,
            write_stall_limit: Some(128),
            snapshot_retention: None,
            num_columns: 1,
            volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
            ..Config::default()
        }));
        let start = Arc::new(Barrier::new(WRITER_COUNT + SNAPSHOT_COUNT + 1));

        let writers = (0..WRITER_COUNT)
            .map(|writer_id| {
                let writer_db = Arc::clone(&db);
                let writer_start = Arc::clone(&start);
                std::thread::spawn(move || {
                    writer_start.wait();
                    for sequence in 0..WRITES_PER_WRITER {
                        let key = format!("snapshot-writer-{writer_id}-{sequence:04}");
                        let value = vec![b'a' + writer_id as u8; 128];
                        writer_db
                            .put(0, key.as_bytes(), 0, &value)
                            .expect("write during concurrent snapshots");
                        if sequence.is_multiple_of(32) {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        let snapshots = (0..SNAPSHOT_COUNT)
            .map(|snapshot_thread| {
                let snapshot_db = Arc::clone(&db);
                let snapshot_start = Arc::clone(&start);
                std::thread::spawn(move || {
                    snapshot_start.wait();
                    std::thread::sleep(Duration::from_millis((snapshot_thread % 4) as u64));
                    let (tx, rx) = mpsc::channel();
                    let snapshot_id = snapshot_db
                        .snapshot_with_callback(move |result| {
                            let _ = tx.send(result);
                        })
                        .expect("create concurrent snapshot");
                    let input = rx
                        .recv_timeout(Duration::from_secs(10))
                        .expect("concurrent snapshot callback")
                        .expect("materialize concurrent snapshot");
                    assert_eq!(input.snapshot_id, snapshot_id);
                    assert!(snapshot_db.retain_snapshot(snapshot_id));
                    snapshot_id
                })
            })
            .collect::<Vec<_>>();

        start.wait();
        for writer in writers {
            writer.join().expect("snapshot writer did not panic");
        }
        let mut snapshot_ids = snapshots
            .into_iter()
            .map(|snapshot| snapshot.join().expect("snapshot thread did not panic"))
            .collect::<Vec<_>>();
        snapshot_ids.sort_unstable();
        snapshot_ids.dedup();
        assert_eq!(snapshot_ids.len(), SNAPSHOT_COUNT);

        let mut previous_seq_id = None;
        for snapshot_id in &snapshot_ids {
            let manifest = load_manifest_for_snapshot(&db.file_manager, *snapshot_id)
                .expect("load concurrent snapshot manifest");
            if let Some(previous) = previous_seq_id {
                assert!(
                    manifest.seq_id >= previous,
                    "snapshot id order regressed from seq {previous} to {} at id {snapshot_id}",
                    manifest.seq_id
                );
            }
            previous_seq_id = Some(manifest.seq_id);
        }

        for snapshot_id in snapshot_ids {
            assert!(db.expire_snapshot(snapshot_id).unwrap());
        }
        for writer_id in 0..WRITER_COUNT {
            for sequence in 0..WRITES_PER_WRITER {
                let key = format!("snapshot-writer-{writer_id}-{sequence:04}");
                assert!(db.get(0, key.as_bytes()).unwrap().is_some());
            }
        }
        db.close().unwrap();
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_single_writer_with_concurrent_gets_and_scans_across_memtable_rotation() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Db>();

        for memtable_type in [
            MemtableType::Hash,
            MemtableType::Skiplist,
            MemtableType::Vec,
        ] {
            let root = format!(
                "/tmp/db_concurrent_reads_{}",
                format!("{memtable_type:?}").to_lowercase()
            );
            cleanup_test_root(&root);
            let db = Arc::new(open_db(Config {
                memtable_capacity: Size::from_kib(16),
                memtable_buffer_count: 3,
                memtable_type,
                l0_file_limit: 64,
                num_columns: 1,
                volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
                ..Config::default()
            }));
            let published = Arc::new(AtomicUsize::new(0));
            let done = Arc::new(AtomicBool::new(false));
            let start = Arc::new(Barrier::new(6));

            let writer_db = Arc::clone(&db);
            let writer_published = Arc::clone(&published);
            let writer_done = Arc::clone(&done);
            let writer_start = Arc::clone(&start);
            let writer = std::thread::spawn(move || {
                writer_start.wait();
                for index in 1..=1_000usize {
                    let key = format!("key-{index:05}");
                    let value = format!("value-{index:05}-{}", "x".repeat(128));
                    writer_db
                        .put(0, key.as_bytes(), 0, value.as_bytes())
                        .expect("single writer put");
                    writer_published.store(index, AtomicOrdering::Release);
                }
                writer_done.store(true, AtomicOrdering::Release);
            });

            let readers = (0..4usize)
                .map(|reader_id| {
                    let reader_db = Arc::clone(&db);
                    let reader_published = Arc::clone(&published);
                    let reader_done = Arc::clone(&done);
                    let reader_start = Arc::clone(&start);
                    std::thread::spawn(move || {
                        reader_start.wait();
                        let mut round = 0usize;
                        while !reader_done.load(AtomicOrdering::Acquire) {
                            let high = reader_published.load(AtomicOrdering::Acquire);
                            if high > 0 {
                                let index =
                                    1 + (round.wrapping_mul(97).wrapping_add(reader_id)) % high;
                                let key = format!("key-{index:05}");
                                let expected_prefix = format!("value-{index:05}-");
                                let row = reader_db
                                    .get(0, key.as_bytes())
                                    .expect("concurrent get")
                                    .expect("published key is visible");
                                assert!(
                                    row[0]
                                        .as_ref()
                                        .expect("column exists")
                                        .starts_with(expected_prefix.as_bytes())
                                );
                            }
                            if round.is_multiple_of(32) {
                                let mut previous = None;
                                let iter = reader_db
                                    .scan(0, b"".as_slice()..b"\xff".as_slice())
                                    .expect("concurrent scan");
                                for row in iter {
                                    let (key, columns) = row.expect("scan row");
                                    if let Some(previous) = previous.as_ref() {
                                        assert!(previous < &key);
                                    }
                                    assert!(columns[0].is_some());
                                    previous = Some(key);
                                }
                            }
                            round += 1;
                        }
                    })
                })
                .collect::<Vec<_>>();

            start.wait();
            writer.join().expect("writer did not panic");
            for reader in readers {
                reader.join().expect("reader did not panic");
            }
            for index in 1..=1_000usize {
                let key = format!("key-{index:05}");
                assert!(db.get(0, key.as_bytes()).unwrap().is_some());
            }
            db.close().unwrap();
            cleanup_test_root(&root);
        }
    }

    #[test]
    #[serial(file)]
    fn test_concurrent_writers_and_readers_across_memtable_rotation() {
        const WRITER_COUNT: usize = 6;
        const READER_COUNT: usize = 2;
        const WRITES_PER_WRITER: usize = 200;

        for memtable_type in [
            MemtableType::Hash,
            MemtableType::Skiplist,
            MemtableType::Vec,
        ] {
            let root = format!(
                "/tmp/db_concurrent_writes_{}",
                format!("{memtable_type:?}").to_lowercase()
            );
            cleanup_test_root(&root);
            let db = Arc::new(open_db(Config {
                memtable_capacity: Size::from_kib(16),
                memtable_buffer_count: 4,
                memtable_type,
                l0_file_limit: 128,
                write_stall_limit: Some(128),
                num_columns: 1,
                volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
                ..Config::default()
            }));
            let done = Arc::new(AtomicBool::new(false));
            let start = Arc::new(Barrier::new(WRITER_COUNT + READER_COUNT + 1));

            let writers = (0..WRITER_COUNT)
                .map(|writer_id| {
                    let writer_db = Arc::clone(&db);
                    let writer_start = Arc::clone(&start);
                    std::thread::spawn(move || {
                        writer_start.wait();
                        for sequence in 0..WRITES_PER_WRITER {
                            let key = format!("writer-{writer_id}-key-{sequence:04}");
                            let value = format!(
                                "writer-{writer_id}-value-{sequence:04}-{}",
                                "x".repeat(128)
                            );
                            let latest_key = format!("writer-{writer_id}-latest");
                            let latest = format!("{sequence:04}");
                            match writer_id % 3 {
                                0 => {
                                    writer_db
                                        .put(0, key.as_bytes(), 0, value.as_bytes())
                                        .expect("concurrent writer put");
                                    writer_db
                                        .put(0, latest_key.as_bytes(), 0, latest.as_bytes())
                                        .expect("concurrent writer ordered put");
                                }
                                1 => {
                                    let entries = [
                                        (key.as_bytes(), value.as_bytes()),
                                        (latest_key.as_bytes(), latest.as_bytes()),
                                    ];
                                    writer_db
                                        .put_column_batch_with_options(
                                            0,
                                            0,
                                            entries,
                                            &WriteOptions::default(),
                                        )
                                        .expect("concurrent column batch");
                                }
                                _ => {
                                    let mut batch = WriteBatch::new();
                                    batch.put(0, key.as_bytes(), 0, value.as_bytes());
                                    batch.put(0, latest_key.as_bytes(), 0, latest.as_bytes());
                                    writer_db
                                        .write_batch(batch)
                                        .expect("concurrent write batch");
                                }
                            }

                            if sequence.is_multiple_of(50) {
                                let deleted_key =
                                    format!("writer-{writer_id}-deleted-{sequence:04}");
                                writer_db
                                    .put(0, deleted_key.as_bytes(), 0, b"temporary")
                                    .expect("put before concurrent delete");
                                writer_db
                                    .delete(0, deleted_key.as_bytes(), 0)
                                    .expect("concurrent delete");

                                let merged_key = format!("writer-{writer_id}-merged-{sequence:04}");
                                writer_db
                                    .put(0, merged_key.as_bytes(), 0, b"base")
                                    .expect("put before concurrent merge");
                                writer_db
                                    .merge(0, merged_key.as_bytes(), 0, b"-tail")
                                    .expect("concurrent merge");
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();

            let readers = (0..READER_COUNT)
                .map(|reader_id| {
                    let reader_db = Arc::clone(&db);
                    let reader_done = Arc::clone(&done);
                    let reader_start = Arc::clone(&start);
                    std::thread::spawn(move || {
                        reader_start.wait();
                        let mut round = 0usize;
                        while !reader_done.load(AtomicOrdering::Acquire) {
                            let writer_id = (round + reader_id) % WRITER_COUNT;
                            let key = format!("writer-{writer_id}-latest");
                            let _ = reader_db.get(0, key.as_bytes()).expect("concurrent get");
                            if round.is_multiple_of(64) {
                                let iter = reader_db
                                    .scan(0, b"writer-".as_slice()..b"writer.".as_slice())
                                    .expect("concurrent scan");
                                for row in iter {
                                    row.expect("concurrent scan row");
                                }
                            }
                            round += 1;
                        }
                    })
                })
                .collect::<Vec<_>>();

            start.wait();
            for writer in writers {
                writer.join().expect("writer did not panic");
            }
            done.store(true, AtomicOrdering::Release);
            for reader in readers {
                reader.join().expect("reader did not panic");
            }

            for writer_id in 0..WRITER_COUNT {
                for sequence in 0..WRITES_PER_WRITER {
                    let key = format!("writer-{writer_id}-key-{sequence:04}");
                    assert!(
                        db.get(0, key.as_bytes()).unwrap().is_some(),
                        "missing {key} for {memtable_type:?}"
                    );
                }
                let key = format!("writer-{writer_id}-latest");
                let value = db.get(0, key.as_bytes()).unwrap().unwrap();
                assert_eq!(
                    value[0].as_deref(),
                    Some(format!("{:04}", WRITES_PER_WRITER - 1).as_bytes())
                );
                for sequence in (0..WRITES_PER_WRITER).step_by(50) {
                    let deleted_key = format!("writer-{writer_id}-deleted-{sequence:04}");
                    assert!(db.get(0, deleted_key.as_bytes()).unwrap().is_none());
                    let merged_key = format!("writer-{writer_id}-merged-{sequence:04}");
                    let value = db.get(0, merged_key.as_bytes()).unwrap().unwrap();
                    assert_eq!(value[0].as_deref(), Some(b"base-tail".as_slice()));
                }
            }
            db.close().unwrap();
            cleanup_test_root(&root);
        }
    }

    #[test]
    #[serial(file)]
    fn test_concurrent_oversized_writes_replace_empty_active_once() {
        const WRITER_COUNT: usize = 4;
        const WRITES_PER_WRITER: usize = 12;

        let root = "/tmp/db_concurrent_oversized_writes";
        cleanup_test_root(root);
        let db = Arc::new(open_db(Config {
            memtable_capacity: Size::from_const(128),
            memtable_buffer_count: 3,
            memtable_type: MemtableType::Hash,
            l0_file_limit: 128,
            write_stall_limit: Some(128),
            num_columns: 1,
            volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
            ..Config::default()
        }));
        let start = Arc::new(Barrier::new(WRITER_COUNT + 1));
        let writers = (0..WRITER_COUNT)
            .map(|writer_id| {
                let writer_db = Arc::clone(&db);
                let writer_start = Arc::clone(&start);
                std::thread::spawn(move || {
                    writer_start.wait();
                    for sequence in 0..WRITES_PER_WRITER {
                        let key = format!("oversized-{writer_id}-{sequence:03}");
                        let value = vec![b'a' + writer_id as u8; 1_024 + sequence];
                        writer_db
                            .put(0, key.as_bytes(), 0, &value)
                            .expect("concurrent oversized put");
                    }
                })
            })
            .collect::<Vec<_>>();

        start.wait();
        for writer in writers {
            writer.join().expect("oversized writer did not panic");
        }
        for writer_id in 0..WRITER_COUNT {
            for sequence in 0..WRITES_PER_WRITER {
                let key = format!("oversized-{writer_id}-{sequence:03}");
                let expected = vec![b'a' + writer_id as u8; 1_024 + sequence];
                let value = db.get(0, key.as_bytes()).unwrap().unwrap();
                assert_eq!(value[0].as_deref(), Some(expected.as_slice()));
            }
        }
        db.close().unwrap();
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_switch_memtable_type_replaces_empty_and_flushes_nonempty_active() {
        let root = "/tmp/db_switch_memtable_type";
        cleanup_test_root(root);
        let db = open_db(Config {
            memtable_capacity: Size::from_const(128),
            memtable_buffer_count: 1,
            memtable_type: MemtableType::Hash,
            volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
            ..Config::default()
        });
        let active_type = || db.memtable_manager.active_memtable_type();

        assert_eq!(active_type(), Some(MemtableType::Hash));
        db.switch_memtable_type(MemtableType::Vec, true).unwrap();
        assert_eq!(active_type(), Some(MemtableType::Vec));
        assert!(db.db_state.load().immutables.is_empty());

        db.switch_memtable_type(MemtableType::Hash, true).unwrap();
        assert_eq!(active_type(), Some(MemtableType::Hash));

        let oversized = vec![b'x'; 1_024];
        db.put(0, b"oversized", 0, &oversized).unwrap();
        assert_eq!(active_type(), Some(MemtableType::Vec));
        assert_eq!(
            db.memtable_manager.target_memtable_type(),
            MemtableType::Hash
        );
        db.switch_memtable_type(MemtableType::Vec, true).unwrap();
        let flush_results = db.memtable_manager.wait_for_flushes();
        assert_eq!(flush_results.len(), 1);
        assert!(flush_results[0].is_ok());
        assert_eq!(
            db.memtable_manager.wait_for_active_memtable_type().unwrap(),
            MemtableType::Vec
        );
        assert_eq!(
            db.get(0, b"oversized").unwrap().unwrap()[0].as_deref(),
            Some(oversized.as_slice())
        );

        db.switch_memtable_type(MemtableType::Hash, true).unwrap();
        assert_eq!(active_type(), Some(MemtableType::Hash));
        db.put(0, b"flushed", 0, b"value").unwrap();
        db.switch_memtable_type(MemtableType::Skiplist, true)
            .unwrap();
        let flush_results = db.memtable_manager.wait_for_flushes();
        assert_eq!(flush_results.len(), 1);
        assert!(flush_results[0].is_ok());
        assert_eq!(
            db.memtable_manager.wait_for_active_memtable_type().unwrap(),
            MemtableType::Skiplist
        );
        assert_eq!(
            db.get(0, b"flushed").unwrap().unwrap()[0].as_deref(),
            Some(b"value".as_slice())
        );

        assert!(db.memtable_manager.wait_for_flushes().is_empty());
        db.put(0, b"same-target", 0, b"value").unwrap();
        db.switch_memtable_type(MemtableType::Skiplist, true)
            .unwrap();
        let flush_results = db.memtable_manager.wait_for_flushes();
        assert_eq!(flush_results.len(), 1);
        assert!(flush_results[0].is_ok());
        assert_eq!(
            db.memtable_manager.wait_for_active_memtable_type().unwrap(),
            MemtableType::Skiplist
        );
        assert_eq!(
            db.get(0, b"same-target").unwrap().unwrap()[0].as_deref(),
            Some(b"value".as_slice())
        );

        db.switch_memtable_type(MemtableType::Vec, true).unwrap();
        assert_eq!(active_type(), Some(MemtableType::Vec));
        db.switch_memtable_type(MemtableType::Hash, true).unwrap();
        assert_eq!(active_type(), Some(MemtableType::Hash));

        db.close().unwrap();
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_adaptive_memtable_switches_to_vec_on_pure_writes() {
        let root = "/tmp/db_adaptive_memtable";
        cleanup_test_root(root);
        let db = open_db(Config {
            memtable_capacity: Size::from_kib(64),
            memtable_buffer_count: 2,
            memtable_type: MemtableType::Adaptive,
            l0_file_limit: 64,
            num_columns: 1,
            volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
            ..Config::default()
        });

        // Initial concrete type is Skiplist (Adaptive resolves to Skiplist).
        assert_eq!(
            db.memtable_manager.target_memtable_type(),
            MemtableType::Skiplist
        );

        // 4096 writes should trigger the adaptive controller to switch to Vec
        // (pure writes >= 99.9% with zero reads).
        for i in 0..4097u32 {
            let key = format!("key{i}");
            db.put(0, key.as_bytes(), 0, b"value").unwrap();
        }

        // The controller should have switched the target to Vec (non-disruptive, flush_current=false).
        assert_eq!(
            db.memtable_manager.target_memtable_type(),
            MemtableType::Vec
        );

        // Switching to a concrete type pins it and disables adaptive statistics.
        db.switch_memtable_type(MemtableType::Skiplist, false)
            .unwrap();
        assert_eq!(
            db.memtable_manager.target_memtable_type(),
            MemtableType::Skiplist
        );
        assert!(!db.memtable_manager.adaptive_enabled());
        // Writes no longer trigger adaptive switches (stats disabled).
        for i in 0..8192u32 {
            let key = format!("pure{i}");
            db.put(0, key.as_bytes(), 0, b"value").unwrap();
        }
        assert_eq!(
            db.memtable_manager.target_memtable_type(),
            MemtableType::Skiplist
        );

        // Switching back to Adaptive re-enables statistics, resuming from Skiplist.
        db.switch_memtable_type(MemtableType::Adaptive, false)
            .unwrap();
        assert!(db.memtable_manager.adaptive_enabled());
        assert_eq!(
            db.memtable_manager.target_memtable_type(),
            MemtableType::Skiplist
        );
        // Now pure writes should trigger a switch to Vec again.
        for i in 0..4097u32 {
            let key = format!("resume{i}");
            db.put(0, key.as_bytes(), 0, b"value").unwrap();
        }
        assert_eq!(
            db.memtable_manager.target_memtable_type(),
            MemtableType::Vec
        );

        db.close().unwrap();
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_adaptive_memtable_no_deadlock_on_vec_rollback_during_writes() {
        // Regression test for P1 deadlock: recording a write while holding the active-memtable
        // write lock must not re-enter the manager to perform a flush. The adaptive decision is
        // applied only after the write completes and the lock is released.
        let root = "/tmp/db_adaptive_no_deadlock";
        cleanup_test_root(root);
        let db = open_db(Config {
            memtable_capacity: Size::from_kib(64),
            memtable_buffer_count: 2,
            memtable_type: MemtableType::Adaptive,
            l0_file_limit: 64,
            num_columns: 1,
            volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
            ..Config::default()
        });

        // Phase 1: enter Vec via pure writes.
        for i in 0..4097u32 {
            db.put(0, format!("w{i}").as_bytes(), 0, b"value").unwrap();
        }
        assert_eq!(
            db.memtable_manager.target_memtable_type(),
            MemtableType::Vec
        );

        // Phase 2: issue reads to trigger VEC rollback (flush_current=true).
        // This must not deadlock - the decision is applied after the read returns.
        for i in 0..20u32 {
            let _ = db.get(0, format!("w{i}").as_bytes());
        }
        // After reads on VEC, the controller should have rolled back to Skiplist.
        assert_eq!(
            db.memtable_manager.target_memtable_type(),
            MemtableType::Skiplist
        );

        // Phase 3: verify the DB is still usable after the rollback.
        db.put(0, b"after_rollback", 0, b"value").unwrap();
        assert_eq!(
            db.get(0, b"after_rollback").unwrap().unwrap()[0].as_deref(),
            Some(b"value".as_slice())
        );

        db.close().unwrap();
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_switch_memtable_type_without_flush_defers_until_natural_rotation() {
        let root = "/tmp/db_switch_memtable_type_deferred";
        cleanup_test_root(root);
        let db = open_db(Config {
            memtable_capacity: Size::from_const(128),
            memtable_buffer_count: 1,
            memtable_type: MemtableType::Hash,
            volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
            ..Config::default()
        });

        db.put(0, b"deferred", 0, b"value").unwrap();
        assert_eq!(
            db.memtable_manager.active_memtable_type(),
            Some(MemtableType::Hash)
        );
        db.switch_memtable_type(MemtableType::Vec, false).unwrap();
        assert_eq!(
            db.memtable_manager.target_memtable_type(),
            MemtableType::Vec
        );
        assert_eq!(
            db.memtable_manager.active_memtable_type(),
            Some(MemtableType::Hash)
        );
        assert!(db.db_state.load().immutables.is_empty());
        assert!(db.memtable_manager.wait_for_flushes().is_empty());
        assert_eq!(
            db.get(0, b"deferred").unwrap().unwrap()[0].as_deref(),
            Some(b"value".as_slice())
        );

        db.memtable_manager.flush_active().unwrap();
        let flush_results = db.memtable_manager.wait_for_flushes();
        assert_eq!(flush_results.len(), 1);
        assert!(flush_results[0].is_ok());
        assert_eq!(
            db.memtable_manager.wait_for_active_memtable_type().unwrap(),
            MemtableType::Vec
        );
        assert_eq!(
            db.get(0, b"deferred").unwrap().unwrap()[0].as_deref(),
            Some(b"value".as_slice())
        );

        db.close().unwrap();
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

        let register_calls = governance
            .register_calls
            .lock()
            .expect("recording governance register lock");
        assert_eq!(register_calls.len(), 1);
        assert_eq!(register_calls[0].0, "db-builder-governed");
        assert_eq!(register_calls[0].1, ranges);
        assert_eq!(register_calls[0].2, total_buckets);
        drop(register_calls);

        let unregister_calls = governance
            .unregister_calls
            .lock()
            .expect("recording governance unregister lock");
        assert_eq!(
            unregister_calls.as_slice(),
            &["db-builder-governed".to_string()]
        );
        drop(unregister_calls);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_uses_noop_governance_when_explicitly_configured() {
        let root = "/tmp/db_noop_governance";
        cleanup_test_root(root);
        let mut config = config_with_small_memtable(root);
        config.governance_mode = GovernanceMode::Noop;

        let db = open_db(config);
        db.close().unwrap();

        assert!(
            !std::path::Path::new(root)
                .join(GOVERNANCE_MANIFEST_POINTER_NAME)
                .exists()
        );
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

    #[derive(Default)]
    struct BatchCountingMergeOperator {
        merge_calls: AtomicUsize,
        merge_batch_calls: AtomicUsize,
    }

    impl MergeOperator for BatchCountingMergeOperator {
        fn merge(
            &self,
            existing_value: Bytes,
            value: Bytes,
            _time_provider: Option<&dyn TimeProvider>,
        ) -> Result<(Bytes, Option<ValueType>)> {
            self.merge_calls.fetch_add(1, AtomicOrdering::Relaxed);
            let mut merged = BytesMut::with_capacity(existing_value.len() + value.len());
            merged.extend_from_slice(existing_value.as_ref());
            merged.extend_from_slice(value.as_ref());
            Ok((merged.freeze(), None))
        }

        fn merge_batch(
            &self,
            existing_value: Bytes,
            operands: Vec<Bytes>,
            _time_provider: Option<&dyn TimeProvider>,
        ) -> Result<(Bytes, Option<ValueType>)> {
            self.merge_batch_calls.fetch_add(1, AtomicOrdering::Relaxed);
            let mut merged = BytesMut::with_capacity(
                existing_value.len() + operands.iter().map(Bytes::len).sum::<usize>(),
            );
            merged.extend_from_slice(existing_value.as_ref());
            for operand in operands {
                merged.extend_from_slice(operand.as_ref());
            }
            Ok((merged.freeze(), None))
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
    fn runtime_manifest_embedded_mode_publishes_initial_flush_and_final_state() {
        let root = "/tmp/db_runtime_manifest_embedded";
        cleanup_test_root(root);
        let mut config = config_with_small_memtable(root);
        config.runtime_manifest_mode = RuntimeManifestMode::Enabled;
        config.governance_mode = GovernanceMode::Noop;
        let db = open_db(config);
        let store = runtime_manifest_store(&db);

        let initial = store
            .load_current()
            .unwrap()
            .expect("initial runtime manifest");
        assert!(
            initial
                .manifest
                .tree_levels
                .iter()
                .all(|levels| { levels.iter().all(|level| level.files.is_empty()) })
        );
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(
            store
                .load_current()
                .unwrap()
                .expect("unchanged manifest")
                .generation,
            initial.generation
        );

        let current_seq_id = db.db_state.load().seq_id;
        db.runtime_manifest_publisher
            .as_ref()
            .expect("enabled runtime manifest publisher")
            .publish_at_least(current_seq_id)
            .unwrap();
        let barrier = wait_for_runtime_generation_at_least(&store, initial.generation + 1);
        assert_eq!(
            barrier.generation,
            initial.generation + 1,
            "coalesced no-op observations must not consume generations"
        );
        assert!(barrier.manifest.seq_id >= current_seq_id);

        db.put(0, b"runtime-key", 0, vec![b'x'; 96]).unwrap();
        db.memtable_manager.flush_active().unwrap();
        db.memtable_manager.wait_for_flushes();
        let flushed = wait_for_runtime_generation_at_least(&store, barrier.generation + 1);
        assert!(
            flushed
                .manifest
                .tree_levels
                .iter()
                .flat_map(|levels| levels.iter())
                .any(|level| !level.files.is_empty())
        );

        db.advance_truncation_cursor_by_id(0, DEFAULT_COLUMN_FAMILY_ID, b"runtime-key")
            .unwrap();
        let with_cursor = wait_for_runtime_generation_at_least(&store, flushed.generation + 1);
        assert_eq!(with_cursor.manifest.truncation_cursors.len(), 1);

        db.close().unwrap();
        let final_manifest = store
            .load_current()
            .unwrap()
            .expect("final runtime manifest");
        assert!(final_manifest.generation >= with_cursor.generation);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn runtime_manifest_dedicated_suspension_survives_failure_and_resumes_after_publish() {
        let root = "/tmp/db_runtime_manifest_dedicated_suspension";
        cleanup_test_root(root);
        let mut config = config_with_small_memtable(root);
        config.runtime_manifest_mode = RuntimeManifestMode::Enabled;
        config.governance_mode = GovernanceMode::Noop;
        let db = open_db(config);
        let store = runtime_manifest_store(&db);
        let initial = store.load_current().unwrap().unwrap();
        let publisher = db
            .runtime_manifest_publisher
            .as_ref()
            .expect("enabled runtime manifest publisher");

        assert!(publisher.suspend_for_owner("test-job").unwrap());
        assert!(
            !publisher.suspend_for_owner("test-job").unwrap(),
            "same-job retry must reuse the existing suspension"
        );
        assert!(
            publisher.suspend_for_owner("other-job").is_err(),
            "another job cannot take over an unproven edit"
        );
        db.put(0, b"suspended-key", 0, vec![b'x'; 96]).unwrap();
        db.memtable_manager.flush_active().unwrap();
        db.memtable_manager.wait_for_flushes();
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(
            store.load_current().unwrap().unwrap().generation,
            initial.generation,
            "background publication must stay suspended after the persisted state changes"
        );

        assert!(
            publisher
                .publish_at_least_and_resume("test-job", u64::MAX)
                .is_err()
        );
        assert!(
            publisher.publish_current().is_err(),
            "a failed barrier publish must leave suspension active"
        );
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(
            store.load_current().unwrap().unwrap().generation,
            initial.generation
        );

        let current_seq_id = db.db_state.load().seq_id;
        publisher
            .publish_at_least_and_resume("test-job", current_seq_id)
            .unwrap();
        let published =
            wait_for_runtime_generation_at_least(&store, initial.generation.saturating_add(1));
        assert!(published.manifest.seq_id >= current_seq_id);
        publisher
            .publish_current()
            .expect("successful barrier publication must clear suspension");

        db.close().unwrap();
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn runtime_manifest_close_rejects_suspended_dedicated_apply() {
        let root = "/tmp/db_runtime_manifest_suspended_close";
        cleanup_test_root(root);
        let mut config = config_with_small_memtable(root);
        config.runtime_manifest_mode = RuntimeManifestMode::Enabled;
        config.governance_mode = GovernanceMode::Noop;
        let db = open_db(config);
        db.runtime_manifest_publisher
            .as_ref()
            .unwrap()
            .suspend_for_owner("close-test-job")
            .unwrap();

        let err = db.close().expect_err("close must reject suspended publish");
        assert!(err.to_string().contains("suspended for close-test-job"));
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn runtime_manifest_auto_mode_only_enables_dedicated_compaction() {
        let embedded_root = "/tmp/db_runtime_manifest_auto_embedded";
        cleanup_test_root(embedded_root);
        let mut embedded = config_with_small_memtable(embedded_root);
        embedded.governance_mode = GovernanceMode::Noop;
        let db = open_db(embedded);
        assert!(
            runtime_manifest_store(&db)
                .load_current()
                .unwrap()
                .is_none()
        );
        db.close().unwrap();
        cleanup_test_root(embedded_root);

        let dedicated_root = "/tmp/db_runtime_manifest_auto_dedicated";
        cleanup_test_root(dedicated_root);
        let mut dedicated = config_with_small_memtable(dedicated_root);
        dedicated.governance_mode = GovernanceMode::Noop;
        dedicated.compaction_mode = CompactionMode::Dedicated;
        let db = open_db(dedicated);
        assert!(
            runtime_manifest_store(&db)
                .load_current()
                .unwrap()
                .is_some()
        );
        db.close().unwrap();
        cleanup_test_root(dedicated_root);

        let disabled_root = "/tmp/db_runtime_manifest_dedicated_disabled";
        cleanup_test_root(disabled_root);
        let mut disabled = config_with_small_memtable(disabled_root);
        disabled.governance_mode = GovernanceMode::Noop;
        disabled.compaction_mode = CompactionMode::Dedicated;
        disabled.runtime_manifest_mode = RuntimeManifestMode::Disabled;
        let db = open_db(disabled);
        assert!(
            runtime_manifest_store(&db)
                .load_current()
                .unwrap()
                .is_none()
        );
        db.close().unwrap();
        cleanup_test_root(disabled_root);
    }

    #[test]
    #[serial(file)]
    fn dedicated_runtime_mode_publishes_flush_without_auto_snapshot() {
        let root = "/tmp/db_runtime_manifest_dedicated_flush";
        cleanup_test_root(root);
        let mut config = config_with_small_memtable(root);
        config.governance_mode = GovernanceMode::Noop;
        config.compaction_mode = CompactionMode::Dedicated;
        let db = open_db(config);

        db.put(0, b"runtime-flush", 0, vec![b'x'; 96]).unwrap();
        db.memtable_manager.flush_active().unwrap();
        db.memtable_manager.wait_for_flushes();

        let store = runtime_manifest_store(&db);
        let runtime = wait_for_runtime_generation_at_least(&store, 2);
        assert!(
            runtime
                .manifest
                .tree_levels
                .iter()
                .flat_map(|levels| levels.iter())
                .any(|level| !level.files.is_empty())
        );
        assert!(
            crate::snapshot::manifest::list_snapshot_manifest_ids(&db.file_manager)
                .unwrap()
                .is_empty()
        );
        db.close().unwrap();
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn dedicated_snapshot_mode_publishes_on_flush() {
        let root = "/tmp/db_runtime_manifest_dedicated_snapshot";
        cleanup_test_root(root);
        let mut config = config_with_small_memtable(root);
        config.governance_mode = GovernanceMode::Noop;
        config.compaction_mode = CompactionMode::Dedicated;
        config.runtime_manifest_mode = RuntimeManifestMode::Disabled;
        let db = open_db(config);

        db.put(0, b"snapshot-flush", 0, vec![b'x'; 96]).unwrap();
        db.memtable_manager.flush_active().unwrap();
        db.memtable_manager.wait_for_flushes();
        for _ in 0..100 {
            if !crate::snapshot::manifest::list_snapshot_manifest_ids(&db.file_manager)
                .unwrap()
                .is_empty()
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !crate::snapshot::manifest::list_snapshot_manifest_ids(&db.file_manager)
                .unwrap()
                .is_empty()
        );
        assert!(
            runtime_manifest_store(&db)
                .load_current()
                .unwrap()
                .is_none()
        );
        db.close().unwrap();
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn runtime_manifest_generation_continues_on_reopen() {
        let root = "/tmp/db_runtime_manifest_reopen";
        cleanup_test_root(root);
        let mut config = config_with_small_memtable(root);
        config.runtime_manifest_mode = RuntimeManifestMode::Enabled;
        config.governance_mode = GovernanceMode::Noop;
        let ranges = vec![full_bucket_range(config.total_buckets)];

        let db = DbBuilder::new(config.clone())
            .bucket_ranges(ranges.clone())
            .db_id("runtime-manifest-reopen")
            .open()
            .unwrap();
        let first_generation = runtime_manifest_store(&db)
            .load_current()
            .unwrap()
            .expect("initial manifest")
            .generation;
        db.close().unwrap();

        let reopened = DbBuilder::new(config)
            .bucket_ranges(ranges)
            .db_id("runtime-manifest-reopen")
            .open()
            .unwrap();
        let second_generation = runtime_manifest_store(&reopened)
            .load_current()
            .unwrap()
            .expect("reopened manifest")
            .generation;
        assert!(second_generation > first_generation);
        reopened.close().unwrap();
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn runtime_manifest_enabled_open_rejects_corrupt_current_without_overwriting_it() {
        let root = "/tmp/db_runtime_manifest_corrupt_current";
        cleanup_test_root(root);
        let mut config = config_with_small_memtable(root);
        config.runtime_manifest_mode = RuntimeManifestMode::Enabled;
        config.governance_mode = GovernanceMode::Noop;
        let ranges = vec![full_bucket_range(config.total_buckets)];
        let db_id = "runtime-manifest-corrupt-current";

        let db = DbBuilder::new(config.clone())
            .bucket_ranges(ranges.clone())
            .db_id(db_id)
            .open()
            .unwrap();
        let file_manager = Arc::clone(&db.file_manager);
        db.close().unwrap();
        let mut writer = file_manager
            .create_metadata_file("runtime/CURRENT")
            .unwrap();
        writer.write(b"not-a-generation\n").unwrap();
        writer.close().unwrap();

        let error = match DbBuilder::new(config)
            .bucket_ranges(ranges)
            .db_id(db_id)
            .open()
        {
            Ok(_) => panic!("corrupt runtime CURRENT unexpectedly opened the DB"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("Runtime CURRENT"));
        assert!(
            crate::runtime_manifest::RuntimeManifestStore::new(file_manager)
                .load_current()
                .is_err()
        );
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
        batch.put(0, b"k1", 0, b"old");
        batch.put(0, b"k1", 0, b"new");
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
    fn test_db_column_batch_rotates_memtables_without_losing_entries() {
        let root = "/tmp/db_column_batch_rotation";
        cleanup_test_root(root);
        let db = open_db(config_with_small_memtable(root));
        let entries = (0..20)
            .map(|index| {
                (
                    format!("key-{index}").into_bytes(),
                    vec![b'a' + (index % 20) as u8; 48],
                )
            })
            .collect::<Vec<_>>();

        db.put_column_batch_with_options(
            0,
            0,
            entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
            &WriteOptions::default(),
        )
        .unwrap();

        for (key, expected) in &entries {
            let value = db.get(0, key).unwrap().expect("batch value present");
            assert_eq!(value[0].as_ref().unwrap().as_ref(), expected.as_slice());
        }
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
    fn test_db_non_default_single_column_family_round_trip_and_scan() {
        let root = "/tmp/db_cf_single_column_roundtrip";
        cleanup_test_root(root);
        let config = Config {
            num_columns: 2,
            ..config_with_small_memtable(root)
        };
        let db = open_db(config);
        let mut schema = db.update_schema();
        schema
            .add_column(0, None, None, Some("metrics".to_string()))
            .unwrap();
        schema.commit();

        db.put_with_options(
            0,
            b"k1",
            0,
            b"v1",
            &WriteOptions::with_column_family("metrics"),
        )
        .unwrap();
        db.put_with_options(
            0,
            b"k2",
            0,
            b"v2",
            &WriteOptions::with_column_family("metrics"),
        )
        .unwrap();

        let value = db
            .get_with_options(0, b"k1", &ReadOptions::for_column_in_family("metrics", 0))
            .unwrap()
            .expect("value present");
        assert_eq!(value.len(), 1);
        assert_eq!(value[0].as_ref().unwrap().as_ref(), b"v1");

        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut iter = db
            .scan_with_options(
                0,
                b"k1".as_slice()..b"k3".as_slice(),
                &ScanOptions::for_column(0).with_column_family("metrics"),
            )
            .unwrap();

        let (k1, cols1) = iter.next().unwrap().unwrap();
        assert_eq!(k1.as_ref(), b"k1");
        assert_eq!(cols1.len(), 1);
        assert_eq!(cols1[0].as_ref().unwrap().as_ref(), b"v1");

        let (k2, cols2) = iter.next().unwrap().unwrap();
        assert_eq!(k2.as_ref(), b"k2");
        assert_eq!(cols2.len(), 1);
        assert_eq!(cols2[0].as_ref().unwrap().as_ref(), b"v2");

        assert!(iter.next().is_none());

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
    fn test_db_get_batches_contiguous_merge_operands() {
        let root = "/tmp/db_get_batches_merges";
        cleanup_test_root(root);
        let mut config = config_with_small_memtable(root);
        config.memtable_capacity = Size::from_const(1024 * 1024);
        let db = open_db(config);
        let operator = Arc::new(BatchCountingMergeOperator::default());
        let mut schema = db.update_schema();
        schema
            .set_column_operator(None, 0, Arc::clone(&operator) as Arc<dyn MergeOperator>)
            .unwrap();
        schema.commit();

        db.put(0, b"k1", 0, b"base").unwrap();
        for operand in [b"-a".as_slice(), b"-b".as_slice(), b"-c".as_slice()] {
            db.merge(0, b"k1", 0, operand).unwrap();
        }

        let value = db.get(0, b"k1").unwrap().expect("value present");
        assert_eq!(value[0].as_deref(), Some(b"base-a-b-c".as_slice()));
        assert_eq!(operator.merge_batch_calls.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(operator.merge_calls.load(AtomicOrdering::Relaxed), 0);
        cleanup_test_root(root);
    }

    #[test]
    fn test_resolve_separated_array_batches_merges_and_resets_on_put() {
        let operator = Arc::new(BatchCountingMergeOperator::default());
        let first_pointer = VlogPointer::new(1, 10).to_bytes();
        let second_pointer = VlogPointer::new(1, 20).to_bytes();
        let items = [
            Column::new(ValueType::Merge, b"-discarded".to_vec()),
            Column::new(ValueType::MergeSeparated, first_pointer.to_vec()),
            Column::new(ValueType::Put, b"reset".to_vec()),
            Column::new(ValueType::MergeSeparated, second_pointer.to_vec()),
            Column::new(ValueType::Merge, b"-last".to_vec()),
        ];
        let refs: Vec<_> = items
            .iter()
            .map(|item| RefColumn::new(item.value_type, item.data()))
            .collect();
        let encoded = encode_merge_separated_array(&refs).unwrap();
        let column = Column::new(ValueType::PutSeparatedArray, encoded);

        let resolved = resolve_column_with_vlog(
            column,
            &mut |pointer| {
                if pointer.offset() == 10 {
                    Ok(Bytes::from_static(b"-first"))
                } else {
                    assert_eq!(pointer.offset(), 20);
                    Ok(Bytes::from_static(b"-second"))
                }
            },
            operator.as_ref(),
            None,
        )
        .unwrap()
        .unwrap();

        assert_eq!(resolved.as_ref(), b"reset-second-last");
        assert_eq!(operator.merge_batch_calls.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(operator.merge_calls.load(AtomicOrdering::Relaxed), 0);
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
        batch.put(0, b"k1", 0, b"old");
        batch.put(0, b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"new");
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
    fn test_db_multi_get_preserves_order_duplicates_and_memtable_l0_merges() {
        let root = "/tmp/db_multi_get";
        cleanup_test_root(root);
        let db = open_db(config_with_small_memtable(root));

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"base");
        batch.put(0, b"k2", 0, b"old");
        batch.put(0, b"pad", 0, vec![b'x'; 64]);
        db.write_batch(batch).unwrap();
        db.memtable_manager.flush_active().unwrap();
        db.memtable_manager.wait_for_flushes();

        db.merge(0, b"k1", 0, b"-memtable").unwrap();
        db.put(0, b"k3", 0, b"fresh").unwrap();
        let values = db
            .multi_get(&[
                (0, b"k2".as_slice()),
                (0, b"k1"),
                (0, b"k2"),
                (0, b"missing"),
                (0, b"k3"),
            ])
            .unwrap();

        assert_eq!(values.len(), 5);
        assert_eq!(
            values[0].as_ref().unwrap()[0].as_deref(),
            Some(b"old".as_slice())
        );
        assert_eq!(
            values[1].as_ref().unwrap()[0].as_deref(),
            Some(b"base-memtable".as_slice())
        );
        assert_eq!(values[2], values[0]);
        assert!(values[3].is_none());
        assert_eq!(
            values[4].as_ref().unwrap()[0].as_deref(),
            Some(b"fresh".as_slice())
        );

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_multi_get_matches_get_for_generic_keys_buckets_and_projection() {
        let root = "/tmp/db_multi_get_options";
        cleanup_test_root(root);
        let db = DbBuilder::new(Config {
            total_buckets: 2,
            num_columns: 2,
            ..config_with_small_memtable(root)
        })
        .bucket_ranges(vec![0..=0, 1..=1])
        .open()
        .unwrap();
        db.put(0, b"left", 0, b"left-0").unwrap();
        db.put(0, b"left", 1, b"left-1").unwrap();
        db.put(1, b"right", 0, b"right-0").unwrap();
        db.put(1, b"right", 1, b"right-1").unwrap();
        let options = ReadOptions::for_columns(vec![1]);
        let keys = vec![
            (1, b"right".to_vec()),
            (0, b"left".to_vec()),
            (1, b"right".to_vec()),
            (0, b"missing".to_vec()),
        ];

        let expected = keys
            .iter()
            .map(|(bucket, key)| db.get_with_options(*bucket, key, &options))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            db.multi_get_with_options(&keys, &options).unwrap(),
            expected
        );
        let empty: Vec<(u16, Vec<u8>)> = Vec::new();
        assert!(db.multi_get(&empty).unwrap().is_empty());

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_read_only_db_multi_get_matches_snapshot_get() {
        let root = "/tmp/read_only_db_multi_get";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = open_db(config.clone());
        db.put(0, b"k1", 0, b"snapshot-1").unwrap();
        db.put(0, b"k2", 0, b"snapshot-2").unwrap();
        let (tx, rx) = mpsc::channel();
        let snapshot_id = db
            .snapshot_with_callback(move |result| {
                tx.send(result).expect("send snapshot result");
            })
            .unwrap();
        rx.recv_timeout(Duration::from_secs(10))
            .expect("receive snapshot result")
            .unwrap();
        let read_only = Db::open_read_only(config, snapshot_id, db.id().to_string()).unwrap();
        let keys = vec![
            (0, b"k2".to_vec()),
            (0, b"k1".to_vec()),
            (0, b"k2".to_vec()),
            (0, b"missing".to_vec()),
        ];

        let expected = keys
            .iter()
            .map(|(bucket, key)| read_only.get(*bucket, key))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(read_only.multi_get(&keys).unwrap(), expected);

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
        batch.put(0, b"k1", 0, b"base");
        batch.put(0, b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.merge(0, b"k1", 0, b"_x");
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
        batch.put(0, b"k1", 0, b"old");
        batch.put(0, b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 0, b"new");
        db.write_batch(batch).unwrap();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), b"new");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_active_merge_collects_terminal_base() {
        let root = "/tmp/db_get_active_merge_terminal";
        cleanup_test_root(root);
        let db = open_db(config_with_small_memtable(root));

        db.put(0, b"k1", 0, b"base").unwrap();
        db.merge(0, b"k1", 0, b"_merge").unwrap();

        let value = db.get(0, b"k1").unwrap().expect("value present");
        assert_eq!(value[0].as_deref(), Some(b"base_merge".as_slice()));
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_active_delete_hides_l0_value() {
        let root = "/tmp/db_get_active_delete_terminal";
        cleanup_test_root(root);
        let db = open_db(config_with_small_memtable(root));

        db.put(0, b"k1", 0, b"old").unwrap();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.delete(0, b"k1", 0).unwrap();

        assert_eq!(db.get(0, b"k1").unwrap(), None);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_multi_column_expired_terminal_keeps_l0_column_masked() {
        let root = "/tmp/db_multi_column_expired_terminal_mask";
        cleanup_test_root(root);
        let mut config = Config {
            num_columns: 2,
            ..config_with_small_memtable(root)
        };
        config.ttl_enabled = true;
        let db = open_db(config);

        db.put(0, b"k1", 0, b"c0-old").unwrap();
        db.put(0, b"k1", 1, b"c1-old").unwrap();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        db.put_with_options(0, b"k1", 0, b"c0-expired", &WriteOptions::with_ttl(1))
            .unwrap();
        std::thread::sleep(Duration::from_millis(1_100));

        let value = db.get(0, b"k1").unwrap().expect("remaining column present");
        assert_eq!(value[0], None);
        assert_eq!(value[1].as_deref(), Some(b"c1-old".as_slice()));
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
        batch.put(0, b"k1", 0, b"base");
        batch.put(0, b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.merge(0, b"k1", 0, b"_x");
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
        batch.put(0, b"k1", 0, b"c0-old");
        batch.put(0, b"k1", 1, b"c1-old");
        batch.put(0, b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.put(0, b"k1", 1, b"c1-new");
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
        batch.put(0, b"k1", 0, b"c0");
        batch.put(0, b"k1", 1, b"c1");
        batch.put(0, b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.merge(0, b"k1", 1, b"_x");
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
        batch.put(0, b"k1", 0, b"c0");
        batch.put(0, b"k1", 1, b"c1");
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
        batch.put(0, b"k1", 0, b"old");
        batch.put(0, b"z1", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        db.put(0, b"k1", 0, b"new").unwrap();
        db.put(0, b"k2", 0, b"v2").unwrap();

        let iter = db.scan(0, b"k1".as_slice()..b"k3".as_slice()).unwrap();
        let mut rows = Vec::new();
        for row in iter {
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
    fn test_db_get_with_projected_merge_operator_column() {
        let root = "/tmp/db_get_projected_merge_operator_column";
        cleanup_test_root(root);
        let config = Config {
            num_columns: 2,
            ..config_with_small_memtable(root)
        };
        let db = open_db(config);

        let mut schema = db.update_schema();
        schema
            .set_column_operator(None, 1, Arc::new(U64CounterMergeOperator))
            .unwrap();
        let _ = schema.commit();

        db.put(0, b"k1", 0, b"base").unwrap();
        db.put(0, b"k1", 1, 1u64.to_le_bytes()).unwrap();
        db.merge(0, b"k1", 1, 10u64.to_le_bytes()).unwrap();

        let value = db
            .get_with_options(0, b"k1", &ReadOptions::for_column(1))
            .unwrap()
            .expect("value present");
        assert_eq!(value.len(), 1);
        assert_eq!(
            decode_u64_counter(value[0].as_ref().unwrap().as_ref()),
            11u64
        );

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
    fn test_db_close_waits_for_scan_iterator_drop() {
        let root = "/tmp/db_close_waits_for_scan_iterator";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = Arc::new(open_db(config));

        db.put(0, b"k1", 0, b"v1").unwrap();
        let iter = db.scan(0, b"".as_slice()..b"\xff".as_slice()).unwrap();

        let close_db = Arc::clone(&db);
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            close_db.close().unwrap();
            done_tx.send(()).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());

        drop(iter);

        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        handle.join().unwrap();

        let err = db.get(0, b"k1").unwrap_err();
        assert!(matches!(err, Error::InvalidState(_)));

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_close_waits_for_schema_builder_drop() {
        let root = "/tmp/db_close_waits_for_schema_builder";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = Arc::new(open_db(config));

        let schema_builder = db.update_schema();

        let close_db = Arc::clone(&db);
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            close_db.close().unwrap();
            done_tx.send(()).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());

        drop(schema_builder);

        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        handle.join().unwrap();
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
        batch.put(0, b"k1", 0, b"c0-1");
        batch.put(0, b"k1", 1, b"c1-1");
        batch.put(0, b"k2", 0, b"c0-2");
        batch.put(0, b"k2", 1, b"c1-2");
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

        let mut options = ScanOptions::default();
        options.read_ahead_bytes = Size::from_const(128);
        let iter = db
            .scan_with_options_bounds(0, None, None, &options)
            .unwrap();
        let mut keys = Vec::new();
        for row in iter {
            let (key, _) = row.unwrap();
            keys.push(key);
        }
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].as_ref(), b"k1");
        assert_eq!(keys[1].as_ref(), b"k2");

        cleanup_test_root(root);
    }
}
