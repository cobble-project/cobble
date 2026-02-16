use crate::db::value_to_vec_of_columns;
use crate::db_state::DbStateHandle;
use crate::error::{Error, Result};
use crate::file::{File, FileManager};
use crate::lsm::{LSMTree, LSMTreeVersion};
use crate::metrics_manager::MetricsManager;
use crate::metrics_registry;
use crate::snapshot::{build_levels_from_manifest, decode_manifest, snapshot_manifest_name};
use crate::sst::block_cache::{BlockCache, new_block_cache};
use crate::sst::row_codec::encode_key;
use crate::ttl::{TTLProvider, TtlConfig};
use crate::r#type::{Key, Value};
use crate::{Config, ReadOptions};
use bytes::Bytes;
use std::sync::Arc;

/// Read-only database that serves data from a snapshot manifest.
pub struct ReadOnlyDb {
    file_manager: Arc<FileManager>,
    lsm_tree: Arc<LSMTree>,
    num_columns: usize,
    ttl_provider: Arc<TTLProvider>,
    metrics_manager: Arc<MetricsManager>,
}

impl ReadOnlyDb {
    /// Open a read-only view from a snapshot manifest scoped to a database id.
    pub fn open_with_db_id(
        config: Config,
        snapshot_id: u64,
        snapshot_db_id: String,
    ) -> Result<Self> {
        Self::open_with_db_id_and_cache(config, snapshot_id, snapshot_db_id, None)
    }

    pub fn open_with_db_id_and_cache(
        config: Config,
        snapshot_id: u64,
        snapshot_db_id: String,
        block_cache: Option<BlockCache>,
    ) -> Result<Self> {
        let metrics_manager = Arc::new(MetricsManager::new(snapshot_db_id.clone()));
        Self::open_with_db_id_and_cache_with_metrics(
            config,
            snapshot_id,
            snapshot_db_id,
            block_cache,
            metrics_manager,
        )
    }

    pub fn open_with_db_id_and_cache_with_metrics(
        config: Config,
        snapshot_id: u64,
        snapshot_db_id: String,
        block_cache: Option<BlockCache>,
        metrics_manager: Arc<MetricsManager>,
    ) -> Result<Self> {
        metrics_registry::init_metrics();
        let file_manager =
            FileManager::from_config(&config, &snapshot_db_id, Arc::clone(&metrics_manager))?;
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
        let mut lsm_tree = LSMTree::with_state_and_ttl(
            Arc::clone(&db_state),
            Arc::clone(&ttl_provider),
            Arc::clone(&metrics_manager),
        );
        if let Some(block_cache) = block_cache {
            lsm_tree.set_block_cache(Some(block_cache));
        } else if config.block_cache_size > 0 {
            lsm_tree.set_block_cache(Some(new_block_cache(config.block_cache_size)));
        }
        let lsm_tree = Arc::new(lsm_tree);
        Ok(Self {
            file_manager,
            lsm_tree,
            num_columns: config.num_columns,
            ttl_provider,
            metrics_manager,
        })
    }

    pub fn id(&self) -> &str {
        self.metrics_manager.db_id()
    }

    /// Return the metrics samples for this database.
    pub fn metrics(&self) -> Vec<crate::MetricSample> {
        metrics_registry::snapshot_metrics(Some(self.metrics_manager.db_id()))
    }

    /// Lookup a key across the snapshot LSM levels.
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
        let masks = options.masks(self.num_columns);
        let selected_mask = masks.selected_mask.as_deref();
        let lsm_values = self.lsm_tree.get(
            &self.file_manager,
            encoded_key.as_ref(),
            self.num_columns,
            options.columns(),
            selected_mask,
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
        value_to_vec_of_columns(merged)
    }
}
