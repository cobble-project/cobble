use crate::db_iter::{DbIterator, DbIteratorOptions};
use crate::error::{Error, Result};
use crate::file::{FileManager, FileSystemRegistry};
use crate::lsm::LSMTree;
use crate::memtable::{MemtableManager, MemtableManagerOptions};
use crate::merge_operator::MergeOperator;
use crate::metrics_manager::MetricsManager;
use crate::schema::{Schema, SchemaBuilder, SchemaManager};
use crate::snapshot::{
    ActiveMemtableSnapshotData, LoadedManifest, ManifestPayload, SnapshotCallback, SnapshotManager,
    apply_manifest_tree_level_edits, build_tree_versions_from_manifest,
    build_vlog_version_from_manifest, decode_manifest, load_manifest_for_snapshot,
    snapshot_manifest_name,
};
use crate::sst::block_cache::new_block_cache;
use crate::sst::row_codec::{decode_value, decode_value_masked, encode_key_ref_into};
use crate::r#type::decode_merge_separated_array;
use crate::r#type::{Column, RefColumn, RefKey, RefValue, Value, ValueType};
use crate::vlog::{VlogPointer, VlogStore};
use crate::write_batch::{WriteBatch, WriteOp};
use crate::{Config, ReadOptions, ScanOptions, TimeProvider};
use bytes::{Bytes, BytesMut};
use log::info;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::db_state::{DbStateHandle, MultiLSMTreeVersion};
use crate::governance::{GovernanceManager, create_manifest_lock_provider};
use crate::metrics_registry;
use crate::read_only_db::ReadOnlyDb;
use crate::ttl::{TTLProvider, TtlConfig};
use crate::util::init_logging;

/// Public database interface.
pub struct Db {
    id: String,
    bucket_ranges: Vec<Range<u16>>,
    file_manager: Arc<FileManager>,
    lsm_tree: Arc<LSMTree>,
    memtable_manager: MemtableManager,
    vlog_store: Arc<VlogStore>,
    snapshot_manager: SnapshotManager,
    schema_manager: Arc<SchemaManager>,
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

fn parse_snapshot_manifest_id(name: &str) -> Option<u64> {
    let name = name.rsplit('/').next().unwrap_or(name);
    name.strip_prefix("SNAPSHOT-")?.parse::<u64>().ok()
}

fn load_manifest_entry(
    file_manager: &Arc<FileManager>,
    snapshot_id: u64,
    loaded_by_id: &HashMap<u64, LoadedManifest>,
) -> Result<LoadedManifest> {
    let manifest_name = snapshot_manifest_name(snapshot_id);
    let reader = file_manager.open_metadata_file_reader_untracked(&manifest_name)?;
    let bytes = reader.read_at(0, reader.size())?;
    let (base_snapshot_id, manifest) = match decode_manifest(bytes.as_ref())? {
        ManifestPayload::Snapshot(manifest) => (None, manifest),
        ManifestPayload::IncrementalSnapshot(incremental) => {
            let base_snapshot_id = Some(incremental.base_snapshot_id);
            let manifest = if let Some(base) = loaded_by_id.get(&incremental.base_snapshot_id) {
                let mut resolved = base.manifest.clone();
                apply_manifest_tree_level_edits(
                    &mut resolved.tree_levels,
                    &incremental.tree_level_edits,
                )?;
                resolved.vlog_files = incremental.vlog_files;
                resolved.id = incremental.id;
                resolved.seq_id = incremental.seq_id;
                resolved.latest_schema_id = incremental.latest_schema_id;
                resolved.active_memtable_data = incremental.active_memtable_data;
                resolved.bucket_ranges = incremental.bucket_ranges;
                resolved
            } else {
                load_manifest_for_snapshot(file_manager, snapshot_id)?
            };
            (base_snapshot_id, manifest)
        }
    };
    Ok(LoadedManifest {
        snapshot_id,
        base_snapshot_id,
        manifest,
    })
}

/// Resolve a single column value, handling any value log pointers using the provided callback.
fn resolve_column_with_vlog<F>(
    column: Column,
    resolve_pointer: &mut F,
    merge_operator: &dyn MergeOperator,
) -> Result<Option<Bytes>>
where
    F: FnMut(VlogPointer) -> Result<Bytes>,
{
    match column.value_type {
        ValueType::Delete => Ok(None),
        ValueType::Put => Ok(Some(Bytes::from(column))),
        ValueType::Merge => {
            let merged = merge_operator.merge(Bytes::new(), Bytes::from(column))?;
            Ok(Some(merged))
        }
        ValueType::PutSeparated | ValueType::MergeSeparated => {
            let pointer = VlogPointer::from_bytes(column.data())?;
            let resolved = resolve_pointer(pointer)?;
            if column.value_type == ValueType::MergeSeparated {
                let merged = merge_operator.merge(Bytes::new(), resolved)?;
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
                        merged = merge_operator.merge(merged, item_value)?;
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
) -> Result<Option<Vec<Option<Bytes>>>>
where
    F: FnMut(VlogPointer) -> Result<Bytes>,
{
    let resolve_pointer = &mut resolve_pointer;
    let mut columns = Vec::with_capacity(value.columns.len());
    for (column_idx, column) in value.columns.into_iter().enumerate() {
        let merge_operator = schema.operator(column_idx);
        let resolved = match column {
            Some(column) => resolve_column_with_vlog(column, resolve_pointer, merge_operator)?,
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
    /// Open a database with the provided configuration.
    pub fn open(config: Config, bucket_ranges: Vec<Range<u16>>) -> Result<Self> {
        if config.total_buckets == 0 {
            return Err(Error::ConfigError(
                "total_buckets must be greater than 0".to_string(),
            ));
        }
        if bucket_ranges.is_empty() {
            return Err(Error::ConfigError(
                "bucket_ranges must not be empty".to_string(),
            ));
        }
        for range in &bucket_ranges {
            if range.start >= range.end || range.end > config.total_buckets {
                return Err(Error::ConfigError(format!(
                    "Invalid bucket range {}..{} for total_buckets {}",
                    range.start, range.end, config.total_buckets
                )));
            }
        }
        init_logging(&config);
        metrics_registry::init_metrics();
        let id = Uuid::new_v4().to_string();
        let metrics_manager = Arc::new(MetricsManager::new(id.clone()));

        // register the governance db id
        let registry = FileSystemRegistry::new();
        let volumes = if config.volumes.is_empty() {
            return Err(Error::ConfigError("No volumes configured".to_string()));
        } else {
            config.volumes.clone()
        };
        let meta_volume = volumes
            .iter()
            .find(|volume| volume.supports(crate::config::VolumeUsageKind::Meta))
            .unwrap_or_else(|| volumes.first().expect("No meta volume exists."));
        let governance_fs = registry.get_or_register_volume(meta_volume)?;
        let governance = GovernanceManager::new(
            Arc::clone(&governance_fs),
            create_manifest_lock_provider(Arc::clone(&governance_fs), &config)?,
        );
        governance.insert_and_publish(&id, bucket_ranges.clone(), config.total_buckets)?;

        let file_manager = FileManager::from_config(&config, &id, Arc::clone(&metrics_manager))?;
        let file_manager = Arc::new(file_manager);
        let db_state = Arc::new(DbStateHandle::new());
        let schema_manager = Arc::new(SchemaManager::new(config.num_columns));
        let db = Self::open_with_state(
            config,
            file_manager,
            db_state,
            id,
            bucket_ranges,
            0,
            metrics_manager,
            schema_manager,
        )?;
        db.memtable_manager.open()?;
        Ok(db)
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// Start a schema update transaction.
    pub fn update_schema(&self) -> SchemaBuilder {
        self.schema_manager.builder()
    }

    /// Return the metrics samples for this database.
    pub fn metrics(&self) -> Vec<crate::MetricSample> {
        metrics_registry::snapshot_metrics(Some(&self.id))
    }

    /// Internal helper to write a single column value with the given ValueType.
    fn write_ref<K, V>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        value_type: ValueType,
        value: V,
        ttl_seconds: Option<u32>,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let num_columns = self.schema_manager.current_num_columns();
        let column_idx = column as usize;
        if column_idx >= num_columns {
            return Err(Error::IoError(format!(
                "Column index {} exceeds num_columns {}",
                column_idx, num_columns
            )));
        }
        let column = RefColumn::new(value_type, value.as_ref());
        let expired_at = self.ttl_provider.get_expiration_timestamp(ttl_seconds);
        let mut columns: Vec<Option<RefColumn<'_>>> = vec![None; num_columns];
        columns[column_idx] = Some(column);
        let record = RefValue::new_with_expired_at(columns, expired_at);
        let key = RefKey::new(bucket, key.as_ref());
        self.memtable_manager.put(&key, &record)
    }

    /// Insert a single key/value pair into the given bucket and column.
    pub fn put<K, V>(&self, bucket: u16, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_with_ttl(bucket, key, column, value, None)
    }

    /// Insert a single key/value pair into the given bucket and column with a TTL.
    pub fn put_with_ttl<K, V>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        value: V,
        ttl_seconds: Option<u32>,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.write_ref(bucket, key, column, ValueType::Put, value, ttl_seconds)
    }

    /// Delete a single column value in the given bucket.
    pub fn delete<K>(&self, bucket: u16, key: K, column: u16) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.write_ref(bucket, key, column, ValueType::Delete, [], None)
    }

    /// Merge a value into the given bucket and column.
    pub fn merge<K, V>(&self, bucket: u16, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.merge_with_ttl(bucket, key, column, value, None)
    }

    /// Merge a value into the given bucket and column with a TTL.
    pub fn merge_with_ttl<K, V>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        value: V,
        ttl_seconds: Option<u32>,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.write_ref(bucket, key, column, ValueType::Merge, value, ttl_seconds)
    }

    /// Write a batch of operations to the database.
    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let mut pending: std::collections::BTreeMap<(u16, Bytes), Value> =
            std::collections::BTreeMap::new();
        let schema = self.schema_manager.latest_schema();
        let num_columns = schema.num_columns();
        for (key_and_seq, op) in batch.ops {
            let bucket = key_and_seq.bucket;
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
            match pending.entry((bucket, key_and_seq.key)) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(next_value);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let merged = std::mem::replace(entry.get_mut(), Value::new(Vec::new()))
                        .merge(next_value, &schema)?;
                    *entry.get_mut() = merged;
                }
            }
        }
        for ((bucket, raw_key), value) in pending {
            let key = RefKey::new(bucket, raw_key.as_ref());
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
        self.memtable_manager.close()?;
        let _ = self
            .snapshot_manager
            .wait_for_materialization(Duration::from_secs(30));
        self.lsm_tree.shutdown_compaction();
        self.snapshot_manager.close()?;
        Ok(())
    }

    /// Flush the active memtable and capture an LSM snapshot with a manifest.
    /// The manifest is materialized asynchronously after the flush completes.
    pub fn snapshot(&self) -> Result<u64> {
        let db_snapshot = self.snapshot_manager.create_snapshot(None);
        self.memtable_manager
            .flush_snapshot(db_snapshot.id, self.snapshot_manager.clone())?;
        Ok(db_snapshot.id)
    }

    pub fn snapshot_with_callback<F>(&self, callback: F) -> Result<u64>
    where
        F: Fn(Result<u64>) + Send + Sync + 'static,
    {
        let callback: SnapshotCallback = Arc::new(callback);
        let snapshot = self.snapshot_manager.create_snapshot(Some(callback));
        self.memtable_manager
            .flush_snapshot(snapshot.id, self.snapshot_manager.clone())?;
        Ok(snapshot.id)
    }

    /// Expire a snapshot and release its file references.
    pub fn expire_snapshot(&self, snapshot_id: u64) -> Result<bool> {
        self.snapshot_manager.expire_snapshot(snapshot_id)
    }

    /// Retain a snapshot to avoid auto-expiration.
    pub fn retain_snapshot(&self, snapshot_id: u64) -> bool {
        self.snapshot_manager.retain_snapshot(snapshot_id)
    }

    /// Build a BucketSnapshotInput for a given snapshot id.
    pub fn bucket_snapshot_input(
        &self,
        snapshot_id: u64,
    ) -> Result<crate::coordinator::BucketSnapshotInput> {
        let manifest_name = snapshot_manifest_name(snapshot_id);
        let manifest_path = self
            .file_manager
            .get_metadata_file_full_path(&manifest_name)
            .ok_or_else(|| {
                Error::IoError(format!("Snapshot manifest not tracked: {}", manifest_name))
            })?;
        Ok(crate::coordinator::BucketSnapshotInput {
            ranges: self.bucket_ranges.clone(),
            db_id: self.id.clone(),
            snapshot_id,
            manifest_path,
        })
    }

    /// Open a read-only view from a snapshot manifest.
    pub fn open_read_only(config: Config, snapshot_id: u64, db_id: String) -> Result<ReadOnlyDb> {
        init_logging(&config);
        ReadOnlyDb::open_with_db_id(config, snapshot_id, db_id)
    }

    /// Open a writable database initialized from a snapshot manifest.
    pub fn open_from_snapshot(config: Config, snapshot_id: u64, db_id: String) -> Result<Self> {
        init_logging(&config);
        metrics_registry::init_metrics();
        let metrics_manager = Arc::new(MetricsManager::new(db_id.clone()));
        let file_manager = FileManager::from_config(&config, &db_id, Arc::clone(&metrics_manager))?;
        let file_manager = Arc::new(file_manager);
        let manifest = load_manifest_for_snapshot(&file_manager, snapshot_id)?;
        let schema_manager = Arc::new(SchemaManager::from_manifest(
            &file_manager,
            &manifest,
            config.num_columns,
        )?);
        let vlog_version = build_vlog_version_from_manifest(&file_manager, &manifest, true)?;
        let max_file_seq = manifest
            .tree_levels
            .iter()
            .flat_map(|levels| levels.iter())
            .flat_map(|level| level.files.iter().map(|file| file.seq))
            .chain(manifest.vlog_files.iter().map(|file| file.file_seq as u64))
            .max()
            .unwrap_or(0);
        let max_seq = manifest.seq_id;
        if manifest.bucket_ranges.is_empty() {
            return Err(Error::InvalidState(format!(
                "Snapshot {} manifest missing bucket_ranges",
                snapshot_id
            )));
        }
        let bucket_ranges = manifest.bucket_ranges.clone();
        let active_memtable_data = manifest.active_memtable_data.clone();
        let tree_versions = build_tree_versions_from_manifest(&file_manager, manifest, true)?;
        let multi_lsm_version = MultiLSMTreeVersion::from_bucket_ranges_with_tree_versions(
            config.total_buckets,
            &bucket_ranges,
            tree_versions.into_iter().map(Arc::new).collect(),
        )?;

        let db_state = Arc::new(DbStateHandle::new());
        db_state.store(crate::db_state::DbState {
            seq_id: max_seq,
            multi_lsm_version,
            vlog_version,
            active: None,
            immutables: Vec::new().into(),
            suggested_base_snapshot_id: Some(snapshot_id),
        });
        let initial_file_seq = max_file_seq.saturating_add(1);
        let db = Self::open_with_state(
            config,
            file_manager,
            db_state,
            db_id,
            bucket_ranges,
            initial_file_seq,
            metrics_manager,
            schema_manager,
        )?;
        db.restore_active_memtable_snapshot_to_l0(&active_memtable_data)?;
        db.memtable_manager.open()?;
        Ok(db)
    }

    /// Resume a writable database from an existing folder by loading all snapshot manifests.
    pub fn resume(config: Config, db_id: String) -> Result<Self> {
        init_logging(&config);
        metrics_registry::init_metrics();
        let metrics_manager = Arc::new(MetricsManager::new(db_id.clone()));
        let file_manager = FileManager::from_config(&config, &db_id, Arc::clone(&metrics_manager))?;
        let file_manager = Arc::new(file_manager);
        let mut snapshot_ids: Vec<u64> = file_manager
            .list_snapshot_metadata_names()?
            .into_iter()
            .filter_map(|name| parse_snapshot_manifest_id(&name))
            .collect();
        snapshot_ids.sort_unstable();
        snapshot_ids.dedup();
        if snapshot_ids.is_empty() {
            return Err(Error::IoError(format!(
                "No snapshot manifests found for db {}",
                db_id
            )));
        }

        // loaded all manifests into memory
        let mut loaded = Vec::with_capacity(snapshot_ids.len());
        let mut loaded_by_id = HashMap::new();
        for snapshot_id in snapshot_ids {
            let entry = load_manifest_entry(&file_manager, snapshot_id, &loaded_by_id)?;
            loaded_by_id.insert(snapshot_id, entry.clone());
            loaded.push(entry);
        }
        // initialize the db using latest
        let latest = loaded.last().ok_or_else(|| {
            Error::IoError(format!("No snapshot manifests found for db {}", db_id))
        })?;
        let manifest = latest.manifest.clone();
        if manifest.bucket_ranges.is_empty() {
            return Err(Error::InvalidState(format!(
                "Snapshot {} manifest missing bucket_ranges",
                latest.snapshot_id
            )));
        }
        let bucket_ranges = manifest.bucket_ranges.clone();
        let active_memtable_data = manifest.active_memtable_data.clone();
        let schema_manager = Arc::new(SchemaManager::from_manifests(
            &file_manager,
            loaded.iter().map(|entry| &entry.manifest),
            config.num_columns,
        )?);
        let vlog_version = build_vlog_version_from_manifest(&file_manager, &manifest, false)?;
        let max_file_seq = manifest
            .tree_levels
            .iter()
            .flat_map(|levels| levels.iter())
            .flat_map(|level| level.files.iter().map(|file| file.seq))
            .chain(manifest.vlog_files.iter().map(|file| file.file_seq as u64))
            .max()
            .unwrap_or(0);
        let tree_versions = build_tree_versions_from_manifest(&file_manager, manifest, false)?;
        let multi_lsm_version = MultiLSMTreeVersion::from_bucket_ranges_with_tree_versions(
            config.total_buckets,
            &bucket_ranges,
            tree_versions.into_iter().map(Arc::new).collect(),
        )?;
        let db_state = Arc::new(DbStateHandle::new());
        db_state.store(crate::db_state::DbState {
            seq_id: latest.manifest.seq_id,
            multi_lsm_version,
            vlog_version,
            active: None,
            immutables: Vec::new().into(),
            suggested_base_snapshot_id: Some(latest.snapshot_id),
        });
        let db = Self::open_with_state(
            config,
            file_manager,
            db_state,
            db_id,
            bucket_ranges,
            max_file_seq.saturating_add(1),
            metrics_manager,
            schema_manager,
        )?;
        // take over all snapshots so they can be expired properly
        db.take_over_snapshot_chain(&loaded)?;
        db.restore_active_memtable_snapshot_to_l0(&active_memtable_data)?;
        db.memtable_manager.open()?;
        Ok(db)
    }

    #[allow(clippy::too_many_arguments)]
    fn open_with_state(
        config: Config,
        file_manager: Arc<FileManager>,
        db_state: Arc<DbStateHandle>,
        id: String,
        bucket_ranges: Vec<Range<u16>>,
        initial_file_seq: u64,
        metrics_manager: Arc<MetricsManager>,
        schema_manager: Arc<SchemaManager>,
    ) -> Result<Self> {
        let time_provider = config.time_provider.create();
        let ttl_config = TtlConfig {
            enabled: config.ttl_enabled,
            default_ttl_seconds: config.default_ttl_seconds,
        };
        let ttl_provider = Arc::new(TTLProvider::new(&ttl_config, Arc::clone(&time_provider)));
        let runtime_num_columns = schema_manager.current_num_columns();
        let mut lsm_tree = LSMTree::with_state_and_ttl(
            Arc::clone(&db_state),
            Arc::clone(&ttl_provider),
            Arc::clone(&metrics_manager),
        );
        if config.block_cache_size > 0 {
            lsm_tree.set_block_cache(Some(new_block_cache(config.block_cache_size)));
        }
        db_state.configure_multi_lsm(config.total_buckets, &bucket_ranges)?;
        let lsm_tree = Arc::new(lsm_tree);
        let mut sst_options = crate::compaction::build_sst_writer_options(&config, 0);
        sst_options.num_columns = runtime_num_columns;
        sst_options.metrics = Some(metrics_manager.sst_writer_metrics(sst_options.compression));
        let vlog_store = Arc::new(VlogStore::new(
            Arc::clone(&file_manager),
            sst_options.buffer_size,
            config.value_separation_threshold,
        ));
        // Compaction setup
        let mut compaction_options = crate::compaction::build_compaction_config(&config);
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
                    crate::compaction::CompactionExecutor::new(compaction_options)?,
                    Arc::clone(&file_manager),
                    Arc::downgrade(&lsm_tree),
                    config.clone(),
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
                memtable_capacity: config.memtable_capacity,
                buffer_count: config.memtable_buffer_count,
                memtable_type: config.memtable_type,
                sst_options,
                file_builder_factory: None,
                num_columns: runtime_num_columns,
                write_stall_limit: config.resolved_write_stall_limit(),
                initial_seq: initial_file_seq,
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
            },
        )?;

        Ok(Self {
            id,
            bucket_ranges,
            file_manager: Arc::clone(&file_manager),
            lsm_tree,
            memtable_manager,
            vlog_store,
            snapshot_manager,
            schema_manager,
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
            .restore_active_memtable_snapshot_to_l0(segments)?;
        if !segments.is_empty() && !restored {
            return Err(Error::InvalidState(
                "active memtable snapshot restore did not flush".to_string(),
            ));
        }
        Ok(())
    }

    /// Lookup a key in a bucket across the memtable and LSM levels.
    pub fn get(
        &self,
        bucket: u16,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<Bytes>>>> {
        let schema = self.schema_manager.latest_schema();
        let num_columns = schema.num_columns();
        if let Some(max_index) = options.max_index()
            && max_index >= num_columns
        {
            return Err(Error::IoError(format!(
                "max_index {} in ReadOptions exceeds num_columns {}",
                max_index, num_columns
            )));
        }
        let mut encoded_key = BytesMut::with_capacity(2 + key.len());
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
        let snapshot = self.memtable_manager.db_state().load();
        let mut values: Vec<Value> = Vec::new();
        let memtable_min_seq = self.memtable_manager.get_all_with_snapshot(
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
            memtable_min_seq,
        )?;
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
            merged = merged.merge(newer, &schema)?;
        }
        value_to_vec_of_columns_with_vlog(
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
        )
    }

    pub fn scan<'a>(
        &'a self,
        bucket: u16,
        range: Range<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator<'a>> {
        let snapshot = self.memtable_manager.db_state().load();
        let schema = self.schema_manager.latest_schema();
        let memtable_iters = self
            .memtable_manager
            .scan_memtable_iterators_with_snapshot(Arc::clone(&snapshot), Arc::clone(&schema))?;
        let lsm_iters = self.lsm_tree.scan_with_snapshot(
            &self.file_manager,
            Arc::clone(&snapshot),
            Arc::clone(&schema),
            Arc::clone(&self.schema_manager),
            options.read_ahead_bytes,
        )?;
        let encode_scan_key = |key: &[u8]| {
            let mut encoded = BytesMut::with_capacity(2 + key.len());
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
                schema_manager: Arc::clone(&self.schema_manager),
            },
        );
        iter.seek(start_key.as_ref())?;
        Ok(iter)
    }

    /// Set the current time for TTL evaluation (manual time provider only).
    pub fn set_time(&self, next: u32) {
        self.time_provider.set_time(next);
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
    use crate::{
        ReadOptions, ScanOptions, U32CounterMergeOperator, U64CounterMergeOperator,
        VolumeDescriptor,
    };
    use serial_test::serial;
    use std::sync::Arc;

    fn cleanup_test_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn config_with_small_memtable(path: &str) -> Config {
        Config {
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 1,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", path)),
            ..Config::default()
        }
    }

    fn open_db(config: Config) -> Db {
        let total_buckets = config.total_buckets;
        Db::open(config, std::iter::once(0u16..total_buckets).collect()).unwrap()
    }

    fn decode_u32_counter(bytes: &[u8]) -> u32 {
        u32::from_le_bytes(bytes.try_into().expect("u32 counter bytes"))
    }

    fn decode_u64_counter(bytes: &[u8]) -> u64 {
        u64::from_le_bytes(bytes.try_into().expect("u64 counter bytes"))
    }

    struct PipeMergeOperator;

    impl MergeOperator for PipeMergeOperator {
        fn merge(&self, existing_value: Bytes, value: Bytes) -> Result<Bytes> {
            if existing_value.is_empty() {
                Ok(value)
            } else {
                let mut merged = BytesMut::with_capacity(existing_value.len() + 1 + value.len());
                merged.extend_from_slice(existing_value.as_ref());
                merged.extend_from_slice(b"|");
                merged.extend_from_slice(value.as_ref());
                Ok(merged.freeze())
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

        let value = db
            .get(0, b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), b"new");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_custom_merge_operator_per_column() {
        let root = "/tmp/db_custom_merge_operator";
        cleanup_test_root(root);
        let config = Config {
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 2,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let db = open_db(config);
        let mut schema = db.update_schema();
        schema
            .set_column_operator(0, Arc::new(PipeMergeOperator))
            .unwrap();
        let _ = schema.commit();

        db.put(0, b"k1", 0, b"base0").unwrap();
        db.merge(0, b"k1", 0, b"a").unwrap();
        db.merge(0, b"k1", 0, b"b").unwrap();
        db.put(0, b"k1", 1, b"base1").unwrap();
        db.merge(0, b"k1", 1, b"a").unwrap();
        db.merge(0, b"k1", 1, b"b").unwrap();

        let value = db
            .get(0, b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
        assert_eq!(value[0].as_ref().unwrap().as_ref(), b"base0|a|b");
        assert_eq!(value[1].as_ref().unwrap().as_ref(), b"base1ab");

        let mut batch = WriteBatch::new();
        batch.merge(0, b"k2", 0, b"a");
        batch.merge(0, b"k2", 0, b"b");
        db.write_batch(batch).unwrap();

        let value = db
            .get(0, b"k2", &ReadOptions::default())
            .unwrap()
            .expect("value present");
        assert_eq!(value[0].as_ref().unwrap().as_ref(), b"a|b");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_counter_merge_operators_code_path() {
        let root = "/tmp/db_counter_merge_operator";
        cleanup_test_root(root);
        let config = Config {
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 2,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let db = open_db(config);
        let mut schema = db.update_schema();
        schema
            .set_column_operator(0, Arc::new(U32CounterMergeOperator))
            .unwrap();
        schema
            .set_column_operator(1, Arc::new(U64CounterMergeOperator))
            .unwrap();
        let _ = schema.commit();

        db.put(0, b"k1", 0, 10u32.to_le_bytes()).unwrap();
        db.merge(0, b"k1", 0, 2u32.to_le_bytes()).unwrap();
        db.merge(0, b"k1", 0, 3u32.to_le_bytes()).unwrap();
        db.put(0, b"k1", 1, 100u64.to_le_bytes()).unwrap();
        db.merge(0, b"k1", 1, 11u64.to_le_bytes()).unwrap();

        let value = db
            .get(0, b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
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

        let value = db
            .get(0, b"k2", &ReadOptions::default())
            .unwrap()
            .expect("value present");
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
            memtable_capacity: 128,
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
        schema.add_column(1, None, None).unwrap();
        let _ = schema.commit();

        let value = db
            .get(0, b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
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
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 1,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let db = open_db(config);

        db.put(0, b"k1", 0, b"v1").unwrap();

        let mut schema = db.update_schema();
        schema.add_column(1, None, None).unwrap();
        let _ = schema.commit();

        let value = db
            .get(0, b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
        assert_eq!(value.len(), 2);
        assert_eq!(value[0].as_ref().unwrap().as_ref(), b"v1");
        assert!(value[1].is_none());

        let mut iter = db
            .scan(
                0,
                b"k1".as_slice()..b"k2".as_slice(),
                &ScanOptions::default(),
            )
            .unwrap();
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
                .write_ref(0, b"k1", 0, value_type, b"value", None)
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
            value_separation_threshold: 8,
            ..config_with_small_memtable(root)
        };
        let db = open_db(config);
        let large = b"value-larger-than-threshold";
        db.put(0, b"k1", 0, large).unwrap();

        let value = db
            .get(0, b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
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
            value_separation_threshold: 8,
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

        let value = db
            .get(0, b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
        assert_eq!(value[0].as_ref().unwrap().as_ref(), large);

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_resolves_merge_separated_array() {
        let root = "/tmp/db_get_merge_separated_array";
        cleanup_test_root(root);
        let config = Config {
            value_separation_threshold: 4,
            ..config_with_small_memtable(root)
        };
        let db = open_db(config);

        db.put(0, b"k1", 0, b"base-separated").unwrap();
        db.merge(0, b"k1", 0, b"-suffix-separated").unwrap();

        let value = db
            .get(0, b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
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

        let value = db
            .get(0, b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
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

        let value = db
            .get(0, b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
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

        let value = db
            .get(0, b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
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

        let value = db
            .get(0, b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
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

        let value = db
            .get(0, b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
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

        let value = db
            .get(0, b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
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
            .get(0, b"k1", &ReadOptions::for_columns(vec![1, 0]))
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

        let mut iter = db
            .scan(
                0,
                b"k1".as_slice()..b"k3".as_slice(),
                &ScanOptions::default(),
            )
            .unwrap();
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
        let mut iter = db
            .scan(
                0,
                b"".as_slice()..b"\xff".as_slice(),
                &ScanOptions::default(),
            )
            .unwrap();
        db.put(0, b"k1", 0, b"new").unwrap();

        let (key, columns) = iter.next().unwrap().unwrap();
        assert_eq!(key.as_ref(), b"k1");
        assert_eq!(columns[0].as_ref().unwrap().as_ref(), b"old");
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
            let mut iter = db
                .scan(
                    0,
                    b"".as_slice()..b"\xff".as_slice(),
                    &ScanOptions {
                        read_ahead_bytes: 128,
                    },
                )
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
