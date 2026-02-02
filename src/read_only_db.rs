use crate::Config;
use crate::db_state::DbStateHandle;
use crate::error::Result;
use crate::file::{File, FileManager, FileSystemRegistry};
use crate::lsm::{LSMTree, LSMTreeVersion};
use crate::metrics_registry;
use crate::snapshot::{build_levels_from_manifest, decode_manifest, snapshot_manifest_name};
use crate::sst::block_cache::new_block_cache;
use crate::sst::row_codec::encode_key;
use crate::ttl::{TTLProvider, TtlConfig};
use crate::r#type::{Key, Value, ValueType};
use bytes::Bytes;
use std::sync::Arc;
use uuid::Uuid;

/// Read-only database that serves data from a snapshot manifest.
pub struct ReadOnlyDb {
    id: String,
    file_manager: Arc<FileManager>,
    lsm_tree: Arc<LSMTree>,
    num_columns: usize,
    ttl_provider: Arc<TTLProvider>,
}

impl ReadOnlyDb {
    /// Open a read-only view from a snapshot manifest scoped to a database id.
    pub fn open_with_db_id(
        config: Config,
        snapshot_id: u64,
        snapshot_db_id: String,
    ) -> Result<Self> {
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(config.path.clone())?;
        metrics_registry::init_metrics();
        let db_id = Uuid::new_v4().to_string();
        let mut file_manager = FileManager::with_db_id(Arc::clone(&fs), &snapshot_db_id)?;
        file_manager.set_db_id(db_id.clone());
        let file_manager = Arc::new(file_manager);
        let time_provider = config.time_provider.create();
        let ttl_provider = Arc::new(TTLProvider::new(
            &TtlConfig {
                enabled: config.ttl_enabled,
                default_ttl_seconds: config.default_ttl_seconds,
            },
            Arc::clone(&time_provider),
        ));
        let manifest_name = snapshot_manifest_name(snapshot_id);
        let reader = file_manager.open_metadata_file_reader_untracked(&manifest_name)?;
        let bytes = reader.read_at(0, reader.size())?;
        let manifest = decode_manifest(bytes.as_ref())?;
        let levels = build_levels_from_manifest(&file_manager, manifest, true)?;

        let db_state = Arc::new(DbStateHandle::new());
        db_state.store(crate::db_state::DbState {
            seq_id: 0,
            lsm_version: LSMTreeVersion { levels },
            active: None,
            immutables: Vec::new().into(),
        });
        let mut lsm_tree =
            LSMTree::with_state_and_ttl(Arc::clone(&db_state), Arc::clone(&ttl_provider));
        if config.block_cache_size > 0 {
            lsm_tree.set_block_cache(Some(new_block_cache(config.block_cache_size)));
        }
        lsm_tree.set_db_id(db_id.clone());
        let lsm_tree = Arc::new(lsm_tree);
        Ok(Self {
            id: db_id,
            file_manager,
            lsm_tree,
            num_columns: config.num_columns,
            ttl_provider,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// Return the metrics samples for this database.
    pub fn metrics(&self) -> Vec<crate::MetricSample> {
        metrics_registry::snapshot_metrics(Some(&self.id))
    }

    /// Lookup a key across the snapshot LSM levels.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<Option<Bytes>>>> {
        let lookup_key = Key::new(0, key.to_vec());
        let encoded_key = encode_key(&lookup_key);
        let lsm_values = self.lsm_tree.get(
            &self.file_manager,
            encoded_key.as_ref(),
            self.num_columns,
            None,
            None,
        )?;

        let values: Vec<Value> = lsm_values
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
}
