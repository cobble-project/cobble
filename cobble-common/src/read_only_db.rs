use crate::db::value_to_vec_of_columns_with_vlog;
use crate::db_iter::{DbIterator, DbIteratorOptions};
use crate::db_state::DbStateHandle;
use crate::error::{Error, Result};
use crate::file::FileManager;
use crate::lsm::{LSMTree, LSMTreeVersion};
use crate::metrics_manager::MetricsManager;
use crate::metrics_registry;
use crate::schema::SchemaManager;
use crate::snapshot::{
    build_levels_from_manifest, build_vlog_version_from_manifest, load_manifest_for_snapshot,
};
use crate::sst::block_cache::{BlockCache, new_block_cache};
use crate::sst::row_codec::encode_key_ref_into;
use crate::ttl::{TTLProvider, TtlConfig};
use crate::r#type::{RefKey, Value};
use crate::vlog::VlogStore;
use crate::{Config, ReadOptions, ScanOptions};
use bytes::Bytes;
use std::ops::Range;
use std::sync::Arc;

/// Read-only database that serves data from a snapshot manifest.
pub struct ReadOnlyDb {
    file_manager: Arc<FileManager>,
    lsm_tree: Arc<LSMTree>,
    vlog_store: Arc<VlogStore>,
    schema_manager: Arc<SchemaManager>,
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
        let manifest = load_manifest_for_snapshot(&file_manager, snapshot_id)?;
        let vlog_version = build_vlog_version_from_manifest(&file_manager, &manifest, true)?;
        let levels = build_levels_from_manifest(&file_manager, manifest, true)?;
        let sst_options = crate::compaction::build_sst_writer_options(&config, 0);
        let vlog_store = Arc::new(VlogStore::new(
            Arc::clone(&file_manager),
            sst_options.buffer_size,
            config.value_separation_threshold,
        ));

        let db_state = Arc::new(DbStateHandle::new());
        db_state.store(crate::db_state::DbState {
            seq_id: 0,
            lsm_version: LSMTreeVersion { levels },
            vlog_version,
            active: None,
            immutables: Vec::new().into(),
            suggested_base_snapshot_id: Some(snapshot_id),
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
        let schema_manager = Arc::new(SchemaManager::new(config.num_columns));
        Ok(Self {
            file_manager,
            lsm_tree,
            vlog_store,
            schema_manager,
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
        let mut encoded_key = bytes::BytesMut::with_capacity(2 + key.len());
        encode_key_ref_into(&RefKey::new(0, key), &mut encoded_key);
        let encoded_key = encoded_key.freeze();
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
        let schema = self.schema_manager.latest_schema();
        for newer in iter {
            merged = merged.merge(newer, &schema)?;
        }
        let snapshot = self.lsm_tree.db_state().load();
        value_to_vec_of_columns_with_vlog(
            merged,
            |pointer| {
                self.vlog_store
                    .read_pointer(&snapshot.vlog_version, pointer)
            },
            &schema,
        )
    }

    pub fn scan(
        &self,
        bucket: u16,
        range: Range<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator<'static>> {
        let snapshot = self.lsm_tree.db_state().load();
        let lsm_iters = self.lsm_tree.scan_with_snapshot(
            &self.file_manager,
            Arc::clone(&snapshot),
            self.num_columns,
            options.read_ahead_bytes,
        )?;
        let encode_scan_key = |key: &[u8]| {
            let mut encoded = bytes::BytesMut::with_capacity(2 + key.len());
            encode_key_ref_into(&RefKey::new(bucket, key), &mut encoded);
            encoded.freeze()
        };
        let start_key = encode_scan_key(range.start);
        let end_bound = Some((encode_scan_key(range.end), false));
        let mut iter: DbIterator<'static> = DbIterator::new(
            Vec::new(),
            lsm_iters,
            DbIteratorOptions {
                end_bound,
                snapshot,
                memtable_manager: None,
                vlog_store: Arc::clone(&self.vlog_store),
                ttl_provider: Arc::clone(&self.ttl_provider),
                num_columns: self.num_columns,
                schema_manager: Arc::clone(&self.schema_manager),
            },
        );
        iter.seek(start_key.as_ref())?;
        Ok(iter)
    }
}
