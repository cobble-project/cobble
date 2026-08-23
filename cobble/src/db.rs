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
use crate::sst::row_codec::{decode_key, decode_value, decode_value_masked};
use crate::r#type::{
    Column, RefColumn, RefKey, RefValue, Value, ValueType, decode_merge_separated_array,
};
use crate::vlog::{VlogPointer, VlogStore};
use crate::wal::{WalCompletion, WalId, WalSegment, WalStore, WalWriter};
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
pub use restore::RecoveryMode;

#[derive(Clone)]
struct RecoveredWalCheckpoint {
    store: Arc<WalStore>,
    checkpoint_id: WalId,
}

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
    /// Present only when durable WAL is explicitly enabled.
    wal_writer: Option<Arc<WalWriter>>,
    /// Durable WAL replayed while the current writer has WAL disabled. This is runtime-only: the
    /// next successfully published snapshot records and truncates this boundary.
    recovered_wal_checkpoint: Arc<Mutex<Option<RecoveredWalCheckpoint>>>,
}

/// Storage ownership policy for files imported by [`Db::expand_bucket_with_storage_mode`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpandStorageMode {
    /// Reference the source snapshot until its files are copied into this DB's owned storage.
    AdoptAsync,
    /// Keep a durable external reference to the source snapshot without copying it.
    ReferencePersistent,
    /// Keep the external reference durable while asynchronously caching preferred local reads.
    ReferencePersistentWithCache,
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
        let mut db = match Self::open_with_state(
            config,
            file_manager,
            db_state,
            Arc::clone(&db_lifecycle),
            id.clone(),
            Some(Arc::clone(&db_governance)),
            bucket_ranges,
            0,
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
        if db.config.wal_enabled
            && let Err(err) = db.create_snapshot_and_wait("initial WAL snapshot")
        {
            db.force_close();
            return Err(err);
        }
        db.start_dedicated_poller();
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

    #[cfg(feature = "ffi")]
    pub(crate) fn jni_direct_buffer_pool_config(&self) -> Result<(usize, usize)> {
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

    /// Return the configured number of logical buckets.
    pub fn total_buckets(&self) -> u32 {
        self.config.total_buckets
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
        let result = if let Some(wal_writer) = &self.wal_writer {
            self.put_ref_with_wal(
                wal_writer,
                schema.version(),
                num_columns,
                &key,
                &record,
                options.await_durable,
            )
        } else {
            self.memtable_manager.put(&key, &record)
        };
        // Record after the write completes (active lock released) to avoid re-entering the
        // manager while holding it.
        let decision = self.memtable_manager.record_adaptive_write(1);
        self.apply_adaptive_decision(decision);
        result
    }

    fn put_ref_with_wal(
        &self,
        wal_writer: &WalWriter,
        schema_id: u64,
        num_columns: usize,
        key: &RefKey<'_>,
        value: &RefValue<'_>,
        await_durable: bool,
    ) -> Result<()> {
        let mut guard = wal_writer.lock_for_schema(schema_id)?;
        let completion = {
            let mut batch = guard.begin_batch();
            batch.append_ref(schema_id, key, value, num_columns);
            match self.memtable_manager.put(key, value) {
                Ok(()) => batch.commit().expect("single WAL append has a completion"),
                Err(err) => return Err(err),
            }
        };
        drop(guard);
        if await_durable {
            completion.wait_result()?;
        }
        Ok(())
    }

    fn finish_partially_applied_wal_batch(
        &self,
        completion: Arc<WalCompletion>,
        await_durable: bool,
        applied: Result<()>,
    ) -> Result<()> {
        if let Err(err) = applied {
            // A batch may have applied an earlier prefix after its WAL entries joined the ordered
            // batch. We cannot safely roll it back, so stop serving rather than leave this
            // process with an ambiguous failed write.
            self.db_lifecycle.mark_error(err.clone());
            return Err(err);
        }
        if await_durable {
            completion.wait_result()?;
        }
        Ok(())
    }

    /// Replays the durable WAL tail that is newer than a restored snapshot checkpoint.
    ///
    /// This calls the memtable manager directly, so recovery never appends a second WAL record.
    pub(crate) fn replay_wal_after_checkpoint(
        &self,
        checkpoint_id: u64,
        recovery_volume: &crate::VolumeDescriptor,
    ) -> Result<()> {
        let store = WalStore::open_existing(
            recovery_volume,
            &self.id,
            &crate::file::FileSystemRegistry::new(),
        )?;
        let ids = store
            .list()?
            .into_iter()
            .filter(|wal_id| *wal_id > checkpoint_id)
            .collect::<Vec<_>>();
        let mut expected = checkpoint_id.saturating_add(1);
        let mut replayed_through = None;
        for wal_id in ids {
            if wal_id != expected {
                return Err(Error::InvalidState(format!(
                    "WAL replay has a gap after checkpoint {checkpoint_id}: expected {expected}, found {wal_id}"
                )));
            }
            match store.read(wal_id)? {
                WalSegment::Data {
                    schema_id,
                    entry_bytes,
                    ..
                } => {
                    if self.schema_manager.schema(schema_id).is_err() {
                        self.schema_manager
                            .register_schema_from_file(&self.file_manager, schema_id)?;
                    }
                    let schema = self.schema_manager.schema(schema_id)?;
                    self.ensure_multi_lsm_scopes_for_schema_if_dirty(schema.as_ref())?;
                    for (mut encoded_key, mut encoded_value) in
                        crate::memtable::decode_vec_entry_stream(entry_bytes.as_ref())?
                    {
                        let key = decode_key(&mut encoded_key)?;
                        let num_columns = schema
                            .num_columns_in_family(key.column_family())
                            .ok_or_else(|| {
                                Error::InvalidState(format!(
                                    "WAL {} references unknown column family {} in schema {}",
                                    wal_id,
                                    key.column_family(),
                                    schema_id
                                ))
                            })?;
                        let value = decode_value(&mut encoded_value, num_columns)?;
                        let columns = value
                            .columns()
                            .iter()
                            .map(|column| {
                                column
                                    .as_ref()
                                    .map(|column| RefColumn::new(column.value_type, column.data()))
                            })
                            .collect();
                        let key = RefKey::new_with_column_family(
                            key.bucket(),
                            key.column_family(),
                            key.data(),
                        );
                        let value = RefValue::new_with_expired_at(columns, value.expired_at());
                        self.memtable_manager.put(&key, &value)?;
                    }
                }
                WalSegment::TruncationCursor { edits, .. } => {
                    for edit in edits {
                        self.db_state.advance_truncation_cursor(
                            edit.bucket,
                            edit.column_family_id,
                            edit.key.as_ref(),
                        );
                    }
                }
            }
            replayed_through = Some(wal_id);
            expected = expected.saturating_add(1);
        }
        if let Some(checkpoint_id) = replayed_through {
            *self.recovered_wal_checkpoint.lock().unwrap() = Some(RecoveredWalCheckpoint {
                store: Arc::new(store),
                checkpoint_id,
            });
        }
        Ok(())
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

    #[doc(hidden)]
    pub fn put_columns_with_options<K, V>(
        &self,
        bucket: u16,
        key: K,
        columns: &[V],
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
        if columns.len() != num_columns {
            return Err(Error::IoError(format!(
                "row column count {} does not match column family {} count {}",
                columns.len(),
                column_family_id,
                num_columns
            )));
        }
        let columns = columns
            .iter()
            .map(|value| Some(RefColumn::new(ValueType::Put, value.as_ref())))
            .collect();
        let expired_at = self.ttl_provider.get_expiration_timestamp(
            if schema.value_has_ttl_in_family(column_family_id) {
                options.ttl_seconds
            } else {
                None
            },
        );
        let record = RefValue::new_with_expired_at(columns, expired_at);
        let key = RefKey::new_with_column_family(bucket, column_family_id, key.as_ref());
        let result = if let Some(wal_writer) = &self.wal_writer {
            self.put_ref_with_wal(
                wal_writer,
                schema.version(),
                num_columns,
                &key,
                &record,
                options.await_durable,
            )
        } else {
            self.memtable_manager.put(&key, &record)
        };
        let decision = self.memtable_manager.record_adaptive_write(1);
        self.apply_adaptive_decision(decision);
        result
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
        let result = if let Some(wal_writer) = &self.wal_writer {
            let mut guard = wal_writer.lock_for_schema(schema.version())?;
            let (result, completion) = {
                let mut batch = guard.begin_batch();
                let result = self.memtable_manager.put_validated_batch_with_callback(
                    entries,
                    num_columns,
                    |key, value| batch.append_ref(schema.version(), key, value, num_columns),
                );
                let completion = batch.commit();
                (result, completion)
            };
            drop(guard);
            if let Some(completion) = completion {
                self.finish_partially_applied_wal_batch(completion, options.await_durable, result)
            } else {
                result
            }
        } else {
            self.memtable_manager
                .put_validated_batch(entries, num_columns)
        };
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

    #[doc(hidden)]
    pub fn delete_row_with_options<K>(
        &self,
        bucket: u16,
        key: K,
        options: &WriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.delete_rows_with_options(&[(bucket, key.as_ref())], options)
    }

    #[doc(hidden)]
    pub fn delete_rows_with_options(
        &self,
        keys: &[(u16, &[u8])],
        options: &WriteOptions,
    ) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let _access = self.begin_access()?;
        let schema = self.schema_manager.latest_schema();
        self.ensure_multi_lsm_scopes_for_schema_if_dirty(schema.as_ref())?;
        let column_family_id = options.resolve_column_family_id_cached(schema.as_ref())?;
        let num_columns = schema.num_columns_in_family(column_family_id).unwrap_or(0);
        let expired_at = self.ttl_provider.get_expiration_timestamp(None);
        let count = std::cell::Cell::new(0u64);
        let entries = keys
            .iter()
            .inspect(|_| count.set(count.get() + 1))
            .map(|(bucket, key)| {
                let columns = (0..num_columns)
                    .map(|_| Some(RefColumn::new(ValueType::Delete, &[])))
                    .collect();
                (
                    RefKey::new_with_column_family(*bucket, column_family_id, key),
                    RefValue::new_with_expired_at(columns, expired_at),
                )
            });
        let result = if let Some(wal_writer) = &self.wal_writer {
            let mut guard = wal_writer.lock_for_schema(schema.version())?;
            let (result, completion) = {
                let mut batch = guard.begin_batch();
                let result = self.memtable_manager.put_validated_batch_with_callback(
                    entries,
                    num_columns,
                    |key, value| batch.append_ref(schema.version(), key, value, num_columns),
                );
                let completion = batch.commit();
                (result, completion)
            };
            drop(guard);
            if let Some(completion) = completion {
                self.finish_partially_applied_wal_batch(completion, options.await_durable, result)
            } else {
                result
            }
        } else {
            self.memtable_manager
                .put_validated_batch(entries, num_columns)
        };
        let decision = self.memtable_manager.record_adaptive_write(count.get());
        self.apply_adaptive_decision(decision);
        result
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
        self.write_batch_with_options(batch, &self.default_write_options)
    }

    /// Writes a batch of operations with the requested WAL durability behavior.
    pub fn write_batch_with_options(
        &self,
        batch: WriteBatch,
        options: &WriteOptions,
    ) -> Result<()> {
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
        if let Some(wal_writer) = &self.wal_writer {
            let mut guard = wal_writer.lock_for_schema(schema.version())?;
            let (result, completion) = {
                let mut batch = guard.begin_batch();
                let result = (|| {
                    for ((bucket, column_family_id, raw_key), value) in &pending {
                        let key = RefKey::new_with_column_family(
                            *bucket,
                            *column_family_id,
                            raw_key.as_ref(),
                        );
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
                        batch.append_ref(schema.version(), &key, &value_ref, value.columns().len());
                        self.memtable_manager.put(&key, &value_ref)?;
                    }
                    Ok(())
                })();
                let completion = batch.commit();
                (result, completion)
            };
            drop(guard);
            let result = if let Some(completion) = completion {
                self.finish_partially_applied_wal_batch(completion, options.await_durable, result)
            } else {
                result
            };
            let decision = self.memtable_manager.record_adaptive_write(batch_len);
            self.apply_adaptive_decision(decision);
            return result;
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
        if let Some(wal_writer) = &self.wal_writer {
            wal_writer.force_close();
        }
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
        if let Some(wal_writer) = &self.wal_writer
            && let Err(err) = wal_writer.close()
        {
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
        self.create_snapshot_with_wal_checkpoint(None)
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
                    timestamp_seconds: info.timestamp_seconds,
                    data_size_bytes: info.data_size_bytes,
                    incremental_data_size_bytes: info.incremental_data_size_bytes,
                })
            }));
        });
        self.create_snapshot_with_wal_checkpoint(Some(wrapper))
    }

    fn create_snapshot_with_wal_checkpoint(
        &self,
        callback: Option<SnapshotCallback>,
    ) -> Result<u64> {
        let Some(wal_writer) = &self.wal_writer else {
            let recovered_checkpoint = self.recovered_wal_checkpoint.lock().unwrap().clone();
            let Some(recovered_checkpoint) = recovered_checkpoint else {
                return self
                    .memtable_manager
                    .create_snapshot(self.snapshot_manager.clone(), callback);
            };
            let recovered_wal_checkpoint = Arc::clone(&self.recovered_wal_checkpoint);
            let checkpoint_id = recovered_checkpoint.checkpoint_id;
            let user_callback = callback;
            let callback: SnapshotCallback = Arc::new(move |result| {
                if result.is_ok() {
                    match recovered_checkpoint.store.truncate_through(checkpoint_id) {
                        Ok(()) => {
                            let mut current = recovered_wal_checkpoint.lock().unwrap();
                            if current.as_ref().is_some_and(|checkpoint| {
                                checkpoint.checkpoint_id == checkpoint_id
                                    && Arc::ptr_eq(&checkpoint.store, &recovered_checkpoint.store)
                            }) {
                                *current = None;
                            }
                        }
                        Err(err) => warn!(
                            "snapshot WAL recovery checkpoint {} published but truncation failed: {}",
                            checkpoint_id, err
                        ),
                    }
                }
                if let Some(callback) = &user_callback {
                    callback(result);
                }
            });
            return self.memtable_manager.create_snapshot_with_wal_checkpoint(
                self.snapshot_manager.clone(),
                Some(callback),
                checkpoint_id,
                None,
            );
        };
        let barrier = wal_writer.begin_snapshot_barrier()?;
        let checkpoint_id = barrier.checkpoint_id();
        let wal_writer = Arc::clone(wal_writer);
        let recovered_checkpoint = self.recovered_wal_checkpoint.lock().unwrap().clone();
        let recovered_wal_checkpoint = Arc::clone(&self.recovered_wal_checkpoint);
        let user_callback = callback;
        let callback: SnapshotCallback = Arc::new(move |result| {
            if result.is_ok() {
                if let Err(err) = wal_writer.truncate_through(checkpoint_id) {
                    warn!(
                        "snapshot WAL checkpoint {} published but WAL truncation failed: {}",
                        checkpoint_id, err
                    );
                }
                if let Some(recovered_checkpoint) = &recovered_checkpoint {
                    match recovered_checkpoint
                        .store
                        .truncate_through(recovered_checkpoint.checkpoint_id)
                    {
                        Ok(()) => {
                            let mut current = recovered_wal_checkpoint.lock().unwrap();
                            if current.as_ref().is_some_and(|checkpoint| {
                                checkpoint.checkpoint_id == recovered_checkpoint.checkpoint_id
                                    && Arc::ptr_eq(&checkpoint.store, &recovered_checkpoint.store)
                            }) {
                                *current = None;
                            }
                        }
                        Err(err) => warn!(
                            "snapshot recovered WAL checkpoint {} published but truncation failed: {}",
                            recovered_checkpoint.checkpoint_id, err
                        ),
                    }
                }
            }
            if let Some(callback) = &user_callback {
                callback(result);
            }
        });
        self.memtable_manager.create_snapshot_with_wal_checkpoint(
            self.snapshot_manager.clone(),
            Some(callback),
            checkpoint_id,
            self.config
                .volumes
                .iter()
                .find(|volume| volume.supports(crate::VolumeUsageKind::Wal))
                .map(crate::config::sanitize_volume_descriptor),
        )
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
            timestamp_seconds: manifest.timestamp_seconds,
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

    /// Starts result consumption only after the caller has finished constructing the complete
    /// writable state. Restore paths deliberately call this after snapshot takeover, active
    /// memtable restoration, and WAL replay so a persisted result cannot race startup.
    fn start_dedicated_poller(&mut self) {
        if self.config.compaction_mode != crate::config::CompactionMode::Dedicated
            || self.dedicated_poller.is_some()
        {
            return;
        }
        self.dedicated_poller = Some(
            crate::compaction::dedicated_poller::DedicatedCompactionPollerHandle::start(
                Arc::clone(&self.file_manager),
                Arc::clone(&self.lsm_tree),
                self.snapshot_manager.clone(),
                Arc::clone(&self.memtable_manager),
                Arc::clone(&self.schema_manager),
                Arc::clone(&self.db_lifecycle),
                Arc::clone(&self.db_state),
                self.runtime_manifest_publisher.as_ref().map(Arc::clone),
                Arc::clone(&self.lsm_topology_lock),
                Duration::from_millis(self.config.compaction_dedicated_poll_interval_ms),
                self.config.clone(),
            ),
        );
    }

    /// Initialize the Db runtime from a pre-loaded DbState.
    ///
    /// Sets up all runtime components: TTL provider, LSM tree with block
    /// cache and multi-LSM bucket mapping, compaction worker (local or
    /// remote), VLOG store, snapshot manager, and memtable manager with
    /// flush/reclaim workers. The dedicated result poller remains stopped until the caller has
    /// finished fresh-open or restore initialization. Called by both fresh open and restore paths.
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
        restored_wal_checkpoint_id: u64,
        hybrid_cache_plan: Option<crate::config::HybridCacheVolumePlan>,
        metrics_manager: Arc<MetricsManager>,
        schema_manager: Arc<SchemaManager>,
    ) -> Result<Self> {
        if config.wal_enabled
            && (config.snapshot_on_flush
                || (config.compaction_mode == crate::config::CompactionMode::Dedicated
                    && !config.runtime_manifests_enabled()))
        {
            return Err(Error::ConfigError(
                "WAL requires manually triggered snapshots; automatic snapshots are not supported"
                    .to_string(),
            ));
        }
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
            Arc::clone(&time_provider),
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

        let runtime_manifest_publisher = if config.runtime_manifests_enabled() {
            Some(Arc::new(
                crate::runtime_manifest::publisher::RuntimeManifestPublisherHandle::open(
                    Arc::clone(&file_manager),
                    Arc::clone(&schema_manager),
                    Arc::clone(&db_state),
                    Arc::clone(&db_lifecycle),
                    config.compaction_mode,
                    Arc::clone(&time_provider),
                )?,
            ))
        } else {
            None
        };
        if let Some(publisher) = &runtime_manifest_publisher {
            let publisher = Arc::downgrade(publisher);
            file_manager.install_durable_replica_route_publisher(Arc::new(move || {
                publisher
                    .upgrade()
                    .ok_or_else(|| {
                        Error::InvalidState(
                            "runtime manifest publisher stopped during replica transfer"
                                .to_string(),
                        )
                    })?
                    .publish_current()
            }));
        }
        let adoption_coordinator = Arc::new(rescale::AdoptionCoordinator::new(
            (id.clone(), config.clone()),
            (Arc::clone(&file_manager), Arc::clone(&lsm_tree)),
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
        let wal_writer = if config.wal_enabled {
            Some(WalWriter::open(
                &config,
                &id,
                Arc::clone(&file_manager),
                Arc::clone(&schema_manager),
                Arc::clone(&db_lifecycle),
                restored_wal_checkpoint_id,
            )?)
        } else {
            None
        };
        let primary_tiering_worker =
            match file_manager.start_primary_tiering_worker(&db_state, Some(adoption_tick)) {
                Ok(worker) => worker,
                Err(err) => {
                    if let Some(wal_writer) = &wal_writer {
                        wal_writer.force_close();
                    }
                    return Err(err);
                }
            };

        // Mark the DB as open before starting background observers so their
        // `ensure_open()` checks pass immediately.
        if let Err(err) = db_lifecycle.mark_open() {
            if let Some(wal_writer) = &wal_writer {
                wal_writer.force_close();
            }
            if let Some(worker) = &primary_tiering_worker {
                worker.stop();
                worker.join();
            }
            return Err(err);
        }

        if let Some(publisher) = &runtime_manifest_publisher {
            publisher.start();
        }

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
            dedicated_poller: None,
            primary_tiering_worker,
            adoption_coordinator,
            runtime_manifest_publisher,
            wal_writer,
            recovered_wal_checkpoint: Arc::new(Mutex::new(None)),
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
        let mut unique = Vec::<(u16, &[u8])>::with_capacity(keys.len());
        let mut positions: Option<Vec<usize>> = None;
        let mut seen = HashMap::<(u16, &[u8]), usize>::with_capacity(keys.len());
        for (input_index, (bucket, key)) in keys.iter().enumerate() {
            let entry = (*bucket, key.as_ref());
            let next = unique.len();
            let index = *seen.entry(entry).or_insert_with(|| {
                unique.push(entry);
                next
            });
            if let Some(positions) = positions.as_mut() {
                positions.push(index);
            } else if index != input_index {
                let mut duplicate_positions = Vec::with_capacity(keys.len());
                duplicate_positions.extend(0..input_index);
                duplicate_positions.push(index);
                positions = Some(duplicate_positions);
            }
        }

        let snapshot = self.db_state.load();
        let selected_columns = options.columns();
        let masks = options.masks(num_columns);
        let selected_mask = masks.selected_mask.as_deref();
        let base_decode_mask = masks.base_mask.as_ref();
        let mask_size = base_decode_mask.len();
        let mut requests = Vec::with_capacity(unique.len());
        for (bucket, key) in unique {
            let encoded_key = encode_key(bucket, column_family_id, key);
            let mut terminal_mask = (num_columns > 1).then(|| vec![0u8; mask_size]);
            let mut values = Vec::new();
            let mut stopped = snapshot.key_is_truncated(bucket, column_family_id, key);
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
        let result: Result<Vec<_>> = match positions {
            None => Ok(unique_results),
            Some(positions) => Ok(positions
                .into_iter()
                .map(|index| unique_results[index].clone())
                .collect()),
        };
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

    pub fn scan(&self, bucket: u16, range: Range<&[u8]>) -> Result<DbIterator> {
        self.scan_with_options(bucket, range, &self.default_scan_options)
    }

    pub fn scan_bounds(
        &self,
        bucket: u16,
        start_key_inclusive: Option<&[u8]>,
        end_key_exclusive: Option<&[u8]>,
    ) -> Result<DbIterator> {
        self.scan_with_options_bounds(
            bucket,
            start_key_inclusive,
            end_key_exclusive,
            &self.default_scan_options,
        )
    }

    pub fn scan_with_options(
        &self,
        bucket: u16,
        range: Range<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator> {
        self.scan_with_options_bounds(bucket, Some(range.start), Some(range.end), options)
    }

    pub fn scan_with_options_bounds(
        &self,
        bucket: u16,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator> {
        let access_guard = self.db_lifecycle.begin_owned_access()?;
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
                memtable_manager: Some(Arc::clone(&self.memtable_manager)),
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
        self.advance_truncation_cursor_with_id(bucket, column_family_id, key)
    }

    pub fn advance_truncation_cursor_by_id(
        &self,
        bucket: u16,
        column_family_id: u8,
        key: &[u8],
    ) -> Result<()> {
        let _access = self.begin_access()?;
        self.advance_truncation_cursor_with_id(bucket, column_family_id, key)
    }

    fn advance_truncation_cursor_with_id(
        &self,
        bucket: u16,
        column_family_id: u8,
        key: &[u8],
    ) -> Result<()> {
        if let Some(wal_writer) = &self.wal_writer {
            let mut guard = wal_writer.lock()?;
            let completion = guard.append_truncation(bucket, column_family_id, key);
            self.db_state
                .advance_truncation_cursor(bucket, column_family_id, key);
            drop(guard);
            return completion.wait_result();
        }
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
#[path = "../tests/unit/db.rs"]
mod tests;
