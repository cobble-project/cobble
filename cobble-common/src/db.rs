use crate::error::{Error, Result};
use crate::file::{FileManager, FileSystemRegistry};
use crate::lsm::LSMTree;
use crate::memtable::{MemtableManager, MemtableManagerOptions};
use crate::snapshot::{
    SnapshotManager, build_levels_from_manifest, decode_manifest, snapshot_manifest_name,
};
use crate::sst::block_cache::new_block_cache;
use crate::sst::row_codec::{decode_value_masked, encode_key, encode_value};
use crate::r#type::{Column, Key, RefColumn, RefKey, RefValue, Value, ValueType};
use crate::write_batch::{WriteBatch, WriteOp};
use crate::{Config, TimeProvider};
use bytes::Bytes;
use log::info;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

use crate::db_state::DbStateHandle;
use crate::governance::{GovernanceManager, create_manifest_lock_provider};
use crate::metrics_registry;
use crate::read_only_db::ReadOnlyDb;
use crate::ttl::{TTLProvider, TtlConfig};
use crate::util::init_logging;

/// Public database interface.
pub struct Db {
    id: String,
    file_manager: Arc<FileManager>,
    lsm_tree: Arc<LSMTree>,
    memtable_manager: MemtableManager,
    snapshot_manager: SnapshotManager,
    num_columns: usize,
    time_provider: Arc<dyn TimeProvider>,
    ttl_provider: Arc<TTLProvider>,
}

#[derive(Clone, Debug)]
pub struct ReadOptions {
    pub column_indices: Option<Vec<usize>>,
    max_index: Option<usize>,
    cached_masks: Arc<Mutex<Option<ReadOptionsMasks>>>,
}

#[derive(Clone, Debug)]
pub(crate) struct ReadOptionsMasks {
    pub(crate) num_columns: usize,
    pub(crate) selected_mask: Option<Arc<[u8]>>,
    pub(crate) base_mask: Arc<[u8]>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            column_indices: None,
            max_index: None,
            cached_masks: Arc::new(Mutex::new(None)),
        }
    }
}

impl ReadOptions {
    pub fn for_column(column_index: usize) -> Self {
        Self::new_with_indices(Some(vec![column_index]))
    }

    pub fn for_columns(column_indices: Vec<usize>) -> Self {
        Self::new_with_indices(Some(column_indices))
    }

    fn new_with_indices(column_indices: Option<Vec<usize>>) -> Self {
        let max_index = column_indices
            .as_ref()
            .and_then(|indices| indices.iter().max().cloned());
        Self {
            column_indices,
            max_index,
            cached_masks: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn columns(&self) -> Option<&[usize]> {
        self.column_indices.as_deref()
    }

    pub(crate) fn max_index(&self) -> Option<usize> {
        self.max_index
    }

    pub(crate) fn masks(&self, num_columns: usize) -> ReadOptionsMasks {
        let mut guard = self.cached_masks.lock().unwrap();
        if guard
            .as_ref()
            .map(|mask| mask.num_columns != num_columns)
            .unwrap_or(true)
        {
            *guard = Some(self.build_masks(num_columns));
        }
        guard.as_ref().expect("cached mask initialized").clone()
    }

    fn build_masks(&self, num_columns: usize) -> ReadOptionsMasks {
        let mask_size = num_columns.div_ceil(8).max(1);
        let last_bits = (num_columns - 1) % 8 + 1;
        let last_mask = (1u8 << last_bits) - 1;
        let selected_mask = self.column_indices.as_ref().map(|columns| {
            let mut mask = vec![0u8; mask_size];
            for &column_idx in columns {
                if column_idx < num_columns {
                    mask[column_idx / 8] |= 1 << (column_idx % 8);
                }
            }
            mask[mask_size - 1] &= last_mask;
            Arc::from(mask.into_boxed_slice())
        });
        let base_mask = if let Some(mask) = selected_mask.as_ref() {
            Arc::clone(mask)
        } else {
            let mut mask = vec![0xFF; mask_size];
            mask[mask_size - 1] &= last_mask;
            Arc::from(mask.into_boxed_slice())
        };
        ReadOptionsMasks {
            num_columns,
            selected_mask,
            base_mask,
        }
    }
}

impl Db {
    /// Open a database with the provided configuration.
    #[allow(clippy::single_range_in_vec_init)]
    pub fn open(config: Config) -> Result<Self> {
        init_logging(&config);
        metrics_registry::init_metrics();
        let id = Uuid::new_v4().to_string();

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
        governance.insert_and_publish(&id, vec![0u16..1u16], 1)?;

        let mut file_manager = FileManager::from_config(&config, &id)?;
        file_manager.set_db_id(id.clone());
        let file_manager = Arc::new(file_manager);
        let db_state = Arc::new(DbStateHandle::new());
        Self::open_with_state(config, file_manager, db_state, id, 0)
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// Return the metrics samples for this database.
    pub fn metrics(&self) -> Vec<crate::MetricSample> {
        metrics_registry::snapshot_metrics(Some(&self.id))
    }

    /// Insert a single key/value pair into the given column.
    pub fn put<K, V>(&self, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let column_idx = column as usize;
        if column_idx >= self.num_columns {
            return Err(Error::IoError(format!(
                "Column index {} exceeds num_columns {}",
                column_idx, self.num_columns
            )));
        }
        let column = RefColumn::new(ValueType::Put, value.as_ref());
        let expired_at = self.ttl_provider.get_expiration_timestamp(None);
        let mut columns: Vec<Option<RefColumn<'_>>> = vec![None; self.num_columns];
        columns[column_idx] = Some(column);
        let record = RefValue::new_with_expired_at(columns, expired_at);
        let key = RefKey::new(0, key.as_ref());
        self.memtable_manager.put_ref(&key, &record)
    }

    /// Write a batch of operations to the database.
    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let mut pending: std::collections::BTreeMap<Vec<u8>, Value> =
            std::collections::BTreeMap::new();
        for (key_and_seq, op) in batch.ops {
            let column_idx = key_and_seq.column as usize;
            if column_idx >= self.num_columns {
                return Err(Error::IoError(format!(
                    "Column index {} exceeds num_columns {}",
                    column_idx, self.num_columns
                )));
            }
            let (column, expired_at) = match op {
                WriteOp::Put(_, value, ttl_secs) => (
                    Column::new(ValueType::Put, value.to_vec()),
                    self.ttl_provider.get_expiration_timestamp(ttl_secs),
                ),
                WriteOp::Delete(_) => (
                    Column::new(ValueType::Delete, Vec::new()),
                    self.ttl_provider.get_expiration_timestamp(None),
                ),
                WriteOp::Merge(_, value, ttl_secs) => (
                    Column::new(ValueType::Merge, value.to_vec()),
                    self.ttl_provider.get_expiration_timestamp(ttl_secs),
                ),
            };
            let mut columns = vec![None; self.num_columns];
            columns[column_idx] = Some(column);
            let next_value = Value::new_with_expired_at(columns, expired_at);
            match pending.entry(key_and_seq.key.to_vec()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(next_value);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let merged = std::mem::replace(entry.get_mut(), Value::new(Vec::new()))
                        .merge(next_value);
                    *entry.get_mut() = merged;
                }
            }
        }
        for (raw_key, value) in pending {
            let key = Key::new(0, raw_key);
            let encoded_key = encode_key(&key);
            let encoded_value = encode_value(&value, self.num_columns);
            self.memtable_manager
                .put(encoded_key.as_ref(), encoded_value.as_ref())?;
        }
        Ok(())
    }

    /// Close the database and flush pending state.
    pub fn close(&self) -> Result<()> {
        self.memtable_manager.close()?;
        self.lsm_tree.shutdown_compaction();
        self.snapshot_manager.close()?;
        Ok(())
    }

    /// Flush the active memtable and capture an LSM snapshot with a manifest.
    /// The manifest is materialized asynchronously after the flush completes.
    pub fn snapshot(&self) -> Result<u64> {
        let db_snapshot = self.snapshot_manager.create_snapshot();
        self.memtable_manager
            .flush_snapshot(db_snapshot.id, self.snapshot_manager.clone())?;
        Ok(db_snapshot.id)
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
        ranges: Vec<std::ops::Range<u16>>,
    ) -> Result<crate::maintainer::BucketSnapshotInput> {
        let manifest_name = snapshot_manifest_name(snapshot_id);
        let manifest_path = self
            .file_manager
            .get_metadata_file_full_path(&manifest_name)
            .ok_or_else(|| {
                Error::IoError(format!("Snapshot manifest not tracked: {}", manifest_name))
            })?;
        Ok(crate::maintainer::BucketSnapshotInput {
            ranges,
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
        let mut file_manager = FileManager::from_config(&config, &db_id)?;
        file_manager.set_db_id(db_id.clone());
        let file_manager = Arc::new(file_manager);

        let manifest_name = snapshot_manifest_name(snapshot_id);
        let reader = file_manager.open_metadata_file_reader_untracked(&manifest_name)?;
        let bytes = reader.read_at(0, reader.size())?;
        let manifest = decode_manifest(bytes.as_ref())?;
        let max_file_seq = manifest
            .levels
            .iter()
            .flat_map(|level| level.files.iter().map(|file| file.seq))
            .max()
            .unwrap_or(0);
        let max_seq = manifest.seq_id;
        let levels = build_levels_from_manifest(&file_manager, manifest, false)?;

        let db_state = Arc::new(DbStateHandle::new());
        db_state.store(crate::db_state::DbState {
            seq_id: max_seq,
            lsm_version: crate::lsm::LSMTreeVersion { levels },
            active: None,
            immutables: Vec::new().into(),
        });
        let initial_file_seq = max_file_seq.saturating_add(1);
        Self::open_with_state(config, file_manager, db_state, db_id, initial_file_seq)
    }

    fn open_with_state(
        config: Config,
        file_manager: Arc<FileManager>,
        db_state: Arc<DbStateHandle>,
        id: String,
        initial_file_seq: u64,
    ) -> Result<Self> {
        let time_provider = config.time_provider.create();
        let ttl_config = TtlConfig {
            enabled: config.ttl_enabled,
            default_ttl_seconds: config.default_ttl_seconds,
        };
        let ttl_provider = Arc::new(TTLProvider::new(&ttl_config, Arc::clone(&time_provider)));

        let mut lsm_tree =
            LSMTree::with_state_and_ttl(Arc::clone(&db_state), Arc::clone(&ttl_provider));
        if config.block_cache_size > 0 {
            lsm_tree.set_block_cache(Some(new_block_cache(config.block_cache_size)));
        }
        lsm_tree.set_db_id(id.clone());
        let lsm_tree = Arc::new(lsm_tree);
        let mut sst_options = crate::compaction::build_sst_writer_options(&config, 0);
        sst_options.metrics_db_id = Some(id.clone());
        // Compaction setup
        let compaction_options = crate::compaction::build_compaction_config(&config);
        let compaction_worker: Arc<dyn crate::compaction::CompactionWorker> =
            if let Some(address) = config.compaction_remote_addr.as_ref() {
                Arc::new(crate::compaction::RemoteCompactionWorker::new(
                    address.clone(),
                    Arc::clone(&file_manager),
                    Arc::downgrade(&lsm_tree),
                    config.clone(),
                    ttl_config.clone(),
                    Duration::from_millis(config.compaction_remote_timeout_ms),
                )?)
            } else {
                Arc::new(crate::compaction::LocalCompactionWorker::new(
                    crate::compaction::CompactionExecutor::new(compaction_options)?,
                    Arc::clone(&file_manager),
                    Arc::downgrade(&lsm_tree),
                    config.clone(),
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

        // Memtable manager setup
        let snapshot_manager =
            SnapshotManager::new(Arc::clone(&file_manager), config.snapshot_retention);
        let memtable_manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: config.memtable_capacity,
                buffer_count: config.memtable_buffer_count,
                sst_options,
                file_builder_factory: None,
                num_columns: config.num_columns,
                write_stall_limit: config.resolved_write_stall_limit(),
                initial_seq: initial_file_seq,
                auto_snapshot_manager: if config.snapshot_on_flush {
                    Some(snapshot_manager.clone())
                } else {
                    None
                },
                db_id: id.clone(),
            },
        )?;

        Ok(Self {
            id,
            file_manager: Arc::clone(&file_manager),
            lsm_tree,
            memtable_manager,
            snapshot_manager,
            num_columns: config.num_columns,
            time_provider,
            ttl_provider,
        })
    }

    /// Lookup a key across the memtable and LSM levels.
    pub fn get(&self, key: &[u8], options: &ReadOptions) -> Result<Option<Vec<Option<Bytes>>>> {
        if let Some(max_index) = options.max_index()
            && max_index >= self.num_columns
        {
            return Err(Error::IoError(format!(
                "max_index {} in ReadOptions exceeds num_columns {}",
                max_index, self.num_columns
            )));
        }
        let lookup_key = Key::new(0, key.to_vec());
        let encoded_key = encode_key(&lookup_key);
        let selected_columns = options.columns();
        let masks = options.masks(self.num_columns);
        let selected_mask = masks.selected_mask.as_deref();
        let decode_mask = masks.base_mask.as_ref();
        let mask_size = decode_mask.len();

        let mut terminal_mask = if self.num_columns == 1 {
            None
        } else {
            Some(vec![0u8; mask_size])
        };
        let snapshot = self.memtable_manager.db_state().load();
        let mut values: Vec<Value> = Vec::new();
        let memtable_min_seq = self.memtable_manager.get_all_with_snapshot(
            Arc::clone(&snapshot),
            encoded_key.as_ref(),
            |raw| {
                let mut value = decode_value_masked(
                    raw,
                    self.num_columns,
                    decode_mask,
                    terminal_mask.as_deref_mut(),
                )?;
                if let (Some(mask), Some(selected)) = (terminal_mask.as_mut(), selected_mask) {
                    for (idx, mask_byte) in mask.iter_mut().enumerate().take(mask_size) {
                        *mask_byte &= selected[idx];
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
            self.num_columns > 1 && values.last().is_some_and(|value| value.is_terminal());
        let lsm_values = self.lsm_tree.get_with_snapshot(
            &self.file_manager,
            Arc::clone(&snapshot),
            encoded_key.as_ref(),
            self.num_columns,
            selected_columns,
            selected_mask,
            terminal_mask.as_deref_mut(),
            memtable_min_seq,
        )?;
        for value in lsm_values {
            if should_stop {
                break;
            }
            if self.num_columns > 1 {
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
            merged = merged.merge(newer);
        }
        Ok(Some(
            merged
                .columns
                .into_iter()
                .map(|col_opt| {
                    col_opt.and_then(|col| match col.value_type() {
                        ValueType::Put | ValueType::Merge => Some(Bytes::from(col)),
                        ValueType::Delete => None,
                    })
                })
                .collect(),
        ))
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
    use crate::VolumeDescriptor;
    use serial_test::serial;

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

    #[test]
    #[serial(file)]
    fn test_db_write_batch_triggers_flush() {
        let root = "/tmp/db_write_batch_flush";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = Db::open(config).unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"k1", 0, vec![b'a'; 64]);
        batch.put(b"k2", 0, vec![b'b'; 64]);
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
        let db = Db::open(config).unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"k1", 0, b"old".to_vec());
        batch.put(b"k1", 0, b"new".to_vec());
        batch.put(b"k2", 0, vec![b'x'; 64]);
        db.write_batch(batch).unwrap();

        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let value = db
            .get(b"k1", &ReadOptions::default())
            .unwrap()
            .expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), b"new");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_prefers_newer_l0_file() {
        let root = "/tmp/db_get_newer_l0";
        cleanup_test_root(root);
        let config = config_with_small_memtable(root);
        let db = Db::open(config).unwrap();

        let mut batch = WriteBatch::new();
        batch.put(b"k1", 0, b"old".to_vec());
        batch.put(b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.put(b"k1", 0, b"new".to_vec());
        batch.put(b"k3", 0, vec![b'b'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let value = db
            .get(b"k1", &ReadOptions::default())
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
        let db = Db::open(config).unwrap();

        let mut batch = WriteBatch::new();
        batch.put(b"k1", 0, b"base".to_vec());
        batch.put(b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.merge(b"k1", 0, b"_x".to_vec());
        batch.put(b"k3", 0, vec![b'b'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let value = db
            .get(b"k1", &ReadOptions::default())
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
        let db = Db::open(config).unwrap();

        let mut batch = WriteBatch::new();
        batch.put(b"k1", 0, b"old".to_vec());
        batch.put(b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.put(b"k1", 0, b"new".to_vec());
        db.write_batch(batch).unwrap();

        let value = db
            .get(b"k1", &ReadOptions::default())
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
        let db = Db::open(config).unwrap();

        let mut batch = WriteBatch::new();
        batch.put(b"k1", 0, b"base".to_vec());
        batch.put(b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.merge(b"k1", 0, b"_x".to_vec());
        db.write_batch(batch).unwrap();

        let value = db
            .get(b"k1", &ReadOptions::default())
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
        let db = Db::open(config).unwrap();

        let mut batch = WriteBatch::new();
        batch.put(b"k1", 0, b"c0-old".to_vec());
        batch.put(b"k1", 1, b"c1-old".to_vec());
        batch.put(b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.put(b"k1", 1, b"c1-new".to_vec());
        db.write_batch(batch).unwrap();

        let value = db
            .get(b"k1", &ReadOptions::default())
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
        let db = Db::open(config).unwrap();

        let mut batch = WriteBatch::new();
        batch.put(b"k1", 0, b"c0".to_vec());
        batch.put(b"k1", 1, b"c1".to_vec());
        batch.put(b"k2", 0, vec![b'a'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let mut batch = WriteBatch::new();
        batch.merge(b"k1", 1, b"_x".to_vec());
        batch.put(b"k3", 0, vec![b'b'; 64]);
        db.write_batch(batch).unwrap();
        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let value = db
            .get(b"k1", &ReadOptions::default())
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
        let db = Db::open(config).unwrap();

        let mut batch = WriteBatch::new();
        batch.put(b"k1", 0, b"c0".to_vec());
        batch.put(b"k1", 1, b"c1".to_vec());
        db.write_batch(batch).unwrap();

        let value = db
            .get(b"k1", &ReadOptions::for_columns(vec![1, 0]))
            .unwrap()
            .expect("value present");
        assert_eq!(value.len(), 2);
        assert_eq!(value[0].as_ref().unwrap().as_ref(), b"c1");
        assert_eq!(value[1].as_ref().unwrap().as_ref(), b"c0");

        cleanup_test_root(root);
    }
}
