use std::sync::{Arc, Mutex};

use crate::Config;
use crate::error::{Error, Result};
use crate::file::{FileManager, FileSystemRegistry};
use crate::lsm::LSMTree;
use crate::memtable::{MemtableManager, MemtableManagerOptions};
use crate::sst::SSTWriterOptions;
use crate::sst::row_codec::{encode_key, encode_value};
use crate::r#type::{Column, Key, Value, ValueType};
use crate::write_batch::{WriteBatch, WriteOp};

/// Public database interface.
pub struct Db {
    file_manager: Arc<FileManager>,
    lsm_tree: Arc<Mutex<LSMTree>>,
    memtable_manager: MemtableManager,
    num_columns: usize,
}

impl Db {
    /// Open a database with the provided configuration.
    pub fn open(config: Config) -> Result<Self> {
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(config.path)?;
        let file_manager = Arc::new(FileManager::with_defaults(fs)?);
        let lsm_tree = Arc::new(Mutex::new(LSMTree::default()));
        let sst_options = SSTWriterOptions {
            num_columns: config.num_columns,
            ..SSTWriterOptions::default()
        };
        let memtable_manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: config.memtable_capacity,
                buffer_count: config.memtable_buffer_count,
                sst_options,
                file_builder_factory: None,
                num_columns: config.num_columns,
            },
        )?;
        Ok(Self {
            file_manager,
            lsm_tree,
            memtable_manager,
            num_columns: config.num_columns,
        })
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
            let column = match op {
                WriteOp::Put(_, value) => Column::new(ValueType::Put, value.to_vec()),
                WriteOp::Delete(_) => Column::new(ValueType::Delete, Vec::new()),
                WriteOp::Merge(_, value) => Column::new(ValueType::Merge, value.to_vec()),
            };
            let mut columns = vec![None; self.num_columns];
            columns[column_idx] = Some(column);
            let next_value = Value::new(columns);
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
        self.memtable_manager.close()
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
