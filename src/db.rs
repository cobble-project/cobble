use crate::error::{Error, Result};
use crate::file::{FileManager, FileSystemRegistry};
use crate::lsm::LSMTree;
use crate::memtable::{MemtableManager, MemtableManagerOptions};
use crate::snapshot::SnapshotManager;
use crate::sst::SSTWriterOptions;
use crate::sst::block_cache::new_block_cache;
use crate::sst::row_codec::{decode_value, encode_key, encode_value};
use crate::r#type::{Column, Key, Value, ValueType};
use crate::write_batch::{WriteBatch, WriteOp};
use crate::{Config, TimeProvider};
use bytes::Bytes;
use log::info;
use std::sync::Arc;

use crate::db_state::DbStateHandle;
use crate::ttl::{TTLProvider, TtlConfig};

/// Public database interface.
pub struct Db {
    file_manager: Arc<FileManager>,
    lsm_tree: Arc<LSMTree>,
    memtable_manager: MemtableManager,
    snapshot_manager: SnapshotManager,
    num_columns: usize,
    time_provider: Arc<dyn TimeProvider>,
    ttl_provider: Arc<TTLProvider>,
}

impl Db {
    /// Open a database with the provided configuration.
    pub fn open(config: Config) -> Result<Self> {
        Self::init_logging(&config);
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(config.path.clone())?;
        let file_manager = Arc::new(FileManager::with_defaults(fs)?);
        let db_state = Arc::new(DbStateHandle::new());
        let time_provider = config.time_provider.create();
        let ttl_provider = Arc::new(TTLProvider::new(
            &TtlConfig {
                enabled: config.ttl_enabled,
                default_ttl_seconds: config.default_ttl_seconds,
            },
            Arc::clone(&time_provider),
        ));

        let mut lsm_tree =
            LSMTree::with_state_and_ttl(Arc::clone(&db_state), Arc::clone(&ttl_provider));
        if config.block_cache_size > 0 {
            lsm_tree.set_block_cache(Some(new_block_cache(config.block_cache_size)));
        }
        let lsm_tree = Arc::new(lsm_tree);
        let sst_options = SSTWriterOptions {
            num_columns: config.num_columns,
            ..SSTWriterOptions::default()
        };

        // Compaction setup
        let compaction_options = crate::compaction::CompactionConfig {
            policy: config.compaction_policy,
            l0_file_limit: config.l0_file_limit,
            l1_base_bytes: config.l1_base_bytes,
            level_size_multiplier: config.level_size_multiplier,
            max_level: config.max_level,
            num_columns: config.num_columns,
            target_file_size: config.base_file_size,
            ..crate::compaction::CompactionConfig::default()
        };
        let compaction_factory = crate::compaction::make_sst_builder_factory(sst_options.clone());
        let compaction_worker = Arc::new(crate::compaction::CompactionWorker::new(
            crate::compaction::CompactionExecutor::new(compaction_options)?,
            Arc::clone(&compaction_factory),
            Arc::clone(&file_manager),
            Arc::downgrade(&lsm_tree),
        ));
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
                auto_snapshot_manager: if config.snapshot_on_flush {
                    Some(snapshot_manager.clone())
                } else {
                    None
                },
            },
        )?;

        Ok(Self {
            file_manager: Arc::clone(&file_manager),
            lsm_tree,
            memtable_manager,
            snapshot_manager,
            num_columns: config.num_columns,
            time_provider,
            ttl_provider,
        })
    }

    fn init_logging(config: &Config) {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let mut builder = log4rs::Config::builder();
            let mut root = log4rs::config::Root::builder();
            if config.log_console {
                let stdout = log4rs::append::console::ConsoleAppender::builder().build();
                builder = builder.appender(
                    log4rs::config::Appender::builder().build("stdout", Box::new(stdout)),
                );
                root = root.appender("stdout");
            }
            if let Some(ref path) = config.log_path {
                let file = log4rs::append::file::FileAppender::builder()
                    .build(path)
                    .unwrap();
                builder = builder
                    .appender(log4rs::config::Appender::builder().build("file", Box::new(file)));
                root = root.appender("file");
            }
            let root = root.build(config.log_level);
            let config = builder.build(root).unwrap();
            let _ = log4rs::init_config(config);
        });
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

    /// Lookup a key across the memtable and LSM levels.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<Option<Bytes>>>> {
        let lookup_key = Key::new(0, key.to_vec());
        let encoded_key = encode_key(&lookup_key);

        let snapshot = self.memtable_manager.db_state().load();
        let mut values: Vec<Value> = Vec::new();
        let memtable_min_seq = self.memtable_manager.get_all_with_snapshot(
            Arc::clone(&snapshot),
            encoded_key.as_ref(),
            |raw| {
                values.push(decode_value(raw, self.num_columns)?);
                Ok(())
            },
        )?;
        let mut terminal_mask = if self.num_columns == 1 {
            None
        } else {
            let mask_size = self.num_columns.div_ceil(8).max(1);
            let mut mask = vec![0u8; mask_size];
            for value in &values {
                let value_mask = value.terminal_mask();
                for (idx, byte) in value_mask.iter().enumerate().take(mask_size) {
                    mask[idx] |= *byte;
                }
            }
            Some(mask)
        };
        let mut should_stop =
            self.num_columns > 1 && values.last().is_some_and(|value| value.is_terminal());
        let lsm_values = self.lsm_tree.get_with_snapshot(
            &self.file_manager,
            Arc::clone(&snapshot),
            encoded_key.as_ref(),
            self.num_columns,
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
    use serial_test::serial;

    fn cleanup_test_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    #[serial(file)]
    fn test_db_write_batch_triggers_flush() {
        let root = "/tmp/db_write_batch_flush";
        cleanup_test_root(root);
        let config = Config {
            path: format!("file://{}", root),
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 1,
            ..Config::default()
        };
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
        let config = Config {
            path: format!("file://{}", root),
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 1,
            ..Config::default()
        };
        let db = Db::open(config).unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"k1", 0, b"old".to_vec());
        batch.put(b"k1", 0, b"new".to_vec());
        batch.put(b"k2", 0, vec![b'x'; 64]);
        db.write_batch(batch).unwrap();

        let _ = db.memtable_manager.wait_for_flushes();
        db.memtable_manager.flush_active().unwrap();
        let _ = db.memtable_manager.wait_for_flushes();

        let value = db.get(b"k1").unwrap().expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), b"new");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_prefers_newer_l0_file() {
        let root = "/tmp/db_get_newer_l0";
        cleanup_test_root(root);
        let config = Config {
            path: format!("file://{}", root),
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 1,
            ..Config::default()
        };
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

        let value = db.get(b"k1").unwrap().expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), b"new");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_merges_across_l0_files() {
        let root = "/tmp/db_get_merge_l0";
        cleanup_test_root(root);
        let config = Config {
            path: format!("file://{}", root),
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 1,
            ..Config::default()
        };
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

        let value = db.get(b"k1").unwrap().expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), b"base_x");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_memtable_overlaps_l0_value() {
        let root = "/tmp/db_get_memtable_overlaps_l0";
        cleanup_test_root(root);
        let config = Config {
            path: format!("file://{}", root),
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 1,
            ..Config::default()
        };
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

        let value = db.get(b"k1").unwrap().expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), b"new");

        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_db_get_memtable_merges_with_l0_value() {
        let root = "/tmp/db_get_memtable_merge_l0";
        cleanup_test_root(root);
        let config = Config {
            path: format!("file://{}", root),
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 1,
            ..Config::default()
        };
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

        let value = db.get(b"k1").unwrap().expect("value present");
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
            path: format!("file://{}", root),
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 2,
            ..Config::default()
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

        let value = db.get(b"k1").unwrap().expect("value present");
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
            path: format!("file://{}", root),
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 2,
            ..Config::default()
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

        let value = db.get(b"k1").unwrap().expect("value present");
        let col0 = value[0].as_ref().unwrap();
        let col1 = value[1].as_ref().unwrap();
        assert_eq!(col0.as_ref(), b"c0");
        assert_eq!(col1.as_ref(), b"c1_x");

        cleanup_test_root(root);
    }
}
