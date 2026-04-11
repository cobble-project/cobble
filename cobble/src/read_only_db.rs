use crate::block_cache::{BlockCache, new_block_cache_with_config};
use crate::db::value_to_vec_of_columns_with_vlog;
use crate::db_iter::{DbIterator, DbIteratorOptions};
use crate::db_state::{DbStateHandle, MultiLSMTreeVersion};
use crate::db_status::DbLifecycle;
use crate::error::{Error, Result};
use crate::file::FileManager;
use crate::lsm::LSMTree;
use crate::metrics_manager::MetricsManager;
use crate::metrics_registry;
use crate::schema::{DEFAULT_COLUMN_FAMILY_ID, Schema, SchemaManager};
use crate::snapshot::{
    build_tree_versions_from_manifest, build_vlog_version_from_manifest, load_manifest_for_snapshot,
};
use crate::sst::row_codec::encode_key_ref_into;
use crate::ttl::{TTLProvider, TtlConfig};
use crate::r#type::{RefKey, Value};
use crate::util::{build_commit_short_id, build_version_string};
use crate::vlog::VlogStore;
use crate::{Config, MergeOperatorResolver, ReadOptions, ScanOptions};
use bytes::Bytes;
use log::info;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

/// Read-only database that serves data from a snapshot manifest.
pub struct ReadOnlyDb {
    file_manager: Arc<FileManager>,
    lsm_tree: Arc<LSMTree>,
    vlog_store: Arc<VlogStore>,
    schema_manager: Arc<SchemaManager>,
    ttl_provider: Arc<TTLProvider>,
    metrics_manager: Arc<MetricsManager>,
}

impl ReadOnlyDb {
    /// Open a read-only view from a snapshot manifest scoped to a database id.
    pub fn open_with_db_id(
        config: Config,
        snapshot_id: u64,
        snapshot_db_id: impl Into<String>,
    ) -> Result<Self> {
        Self::open_with_db_id_and_cache(config, snapshot_id, snapshot_db_id, None)
    }

    /// Open with a merge operator resolver for custom operator restoration.
    pub fn open_with_db_id_and_resolver(
        config: Config,
        snapshot_id: u64,
        snapshot_db_id: impl Into<String>,
        resolver: Arc<dyn MergeOperatorResolver>,
    ) -> Result<Self> {
        let snapshot_db_id = snapshot_db_id.into();
        let metrics_manager = Arc::new(MetricsManager::new(&snapshot_db_id));
        Self::open_internal(
            config,
            snapshot_id,
            snapshot_db_id,
            None,
            metrics_manager,
            Some(resolver),
        )
    }

    pub fn open_with_db_id_and_cache(
        config: Config,
        snapshot_id: u64,
        snapshot_db_id: impl Into<String>,
        block_cache: Option<BlockCache>,
    ) -> Result<Self> {
        let snapshot_db_id = snapshot_db_id.into();
        let metrics_manager = Arc::new(MetricsManager::new(&snapshot_db_id));
        Self::open_internal(
            config,
            snapshot_id,
            snapshot_db_id,
            block_cache,
            metrics_manager,
            None,
        )
    }

    pub fn open_with_db_id_and_cache_with_metrics(
        config: Config,
        snapshot_id: u64,
        snapshot_db_id: impl Into<String>,
        block_cache: Option<BlockCache>,
        metrics_manager: Arc<MetricsManager>,
    ) -> Result<Self> {
        let snapshot_db_id = snapshot_db_id.into();
        Self::open_internal(
            config,
            snapshot_id,
            snapshot_db_id,
            block_cache,
            metrics_manager,
            None,
        )
    }

    pub fn open_with_db_id_and_cache_with_metrics_and_resolver(
        config: Config,
        snapshot_id: u64,
        snapshot_db_id: impl Into<String>,
        block_cache: Option<BlockCache>,
        metrics_manager: Arc<MetricsManager>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        let snapshot_db_id = snapshot_db_id.into();
        Self::open_internal(
            config,
            snapshot_id,
            snapshot_db_id,
            block_cache,
            metrics_manager,
            resolver,
        )
    }

    fn open_internal(
        config: Config,
        snapshot_id: u64,
        snapshot_db_id: String,
        block_cache: Option<BlockCache>,
        metrics_manager: Arc<MetricsManager>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        let config = config.normalize_volume_paths()?;
        info!(
            "Cobble read-only db ({}, Rev:{}) start.",
            build_version_string(),
            build_commit_short_id()
        );
        metrics_registry::init_metrics();
        let file_manager =
            FileManager::from_config(&config, &snapshot_db_id, Arc::clone(&metrics_manager))?;
        let file_manager = Arc::new(file_manager);
        let block_cache_size = config.block_cache_size_bytes()?;
        let value_separation_threshold = config.value_separation_threshold_bytes()?;
        let time_provider = config.time_provider.create();
        let ttl_provider = Arc::new(TTLProvider::new(
            &TtlConfig {
                enabled: config.ttl_enabled,
                default_ttl_seconds: config.default_ttl_seconds,
            },
            Arc::clone(&time_provider),
        ));
        let manifest = load_manifest_for_snapshot(&file_manager, snapshot_id)?;
        if manifest.bucket_ranges.is_empty() {
            return Err(Error::InvalidState(format!(
                "Snapshot {} manifest missing bucket_ranges",
                snapshot_id
            )));
        }
        let bucket_ranges = manifest.bucket_ranges.clone();
        let lsm_tree_bucket_ranges = if manifest.lsm_tree_bucket_ranges.is_empty() {
            manifest.bucket_ranges.clone()
        } else {
            manifest.lsm_tree_bucket_ranges.clone()
        };
        let schema_manager = Arc::new(SchemaManager::from_manifest(
            &file_manager,
            &manifest,
            config.num_columns,
            resolver,
        )?);
        let vlog_version = build_vlog_version_from_manifest(&file_manager, &manifest, true)?;
        let tree_versions = build_tree_versions_from_manifest(&file_manager, &manifest, true)?;
        let multi_lsm_version = MultiLSMTreeVersion::from_bucket_ranges_with_tree_versions(
            config.total_buckets,
            &lsm_tree_bucket_ranges,
            tree_versions.into_iter().map(Arc::new).collect(),
        )?;
        let writer_options =
            crate::compaction::build_writer_options(&config, 0, config.data_file_type)?;
        let vlog_store = Arc::new(VlogStore::new(
            Arc::clone(&file_manager),
            writer_options.buffer_size(),
            value_separation_threshold,
        ));

        let db_state = Arc::new(DbStateHandle::new());
        db_state.store(crate::db_state::DbState {
            seq_id: 0,
            bucket_ranges: bucket_ranges.clone(),
            multi_lsm_version,
            vlog_version,
            active: None,
            immutables: VecDeque::new(),
            suggested_base_snapshot_id: Some(snapshot_id),
        });
        let mut lsm_tree = LSMTree::with_state_and_ttl(
            Arc::clone(&db_state),
            Arc::clone(&ttl_provider),
            Arc::new(DbLifecycle::new_open()),
            Arc::clone(&metrics_manager),
        );
        if let Some(block_cache) = block_cache {
            lsm_tree.set_block_cache(Some(block_cache));
        } else if block_cache_size > 0 {
            lsm_tree.set_block_cache(Some(new_block_cache_with_config(
                &config,
                &snapshot_db_id,
                block_cache_size,
                None,
            )?));
        }
        let lsm_tree = Arc::new(lsm_tree);
        Ok(Self {
            file_manager,
            lsm_tree,
            vlog_store,
            schema_manager,
            ttl_provider,
            metrics_manager,
        })
    }

    pub fn id(&self) -> &str {
        self.metrics_manager.db_id()
    }

    /// Return the current schema for this read-only database.
    pub fn current_schema(&self) -> Arc<Schema> {
        self.schema_manager.latest_schema()
    }

    /// Return the metrics samples for this database.
    pub fn metrics(&self) -> Vec<crate::MetricSample> {
        metrics_registry::snapshot_metrics(Some(self.metrics_manager.db_id()))
    }

    /// Lookup a key in a bucket across the snapshot LSM levels.
    pub fn get(&self, bucket: u16, key: &[u8]) -> Result<Option<Vec<Option<Bytes>>>> {
        self.get_with_options(bucket, key, &ReadOptions::default())
    }

    pub fn get_with_options(
        &self,
        bucket: u16,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<Bytes>>>> {
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
        let mut encoded_key = bytes::BytesMut::with_capacity(2 + key.len());
        encode_key_ref_into(&RefKey::new(bucket, key), &mut encoded_key);
        let encoded_key = encoded_key.freeze();
        let masks = options.masks(num_columns);
        let selected_mask = masks.selected_mask.as_deref();
        let lsm_values = self.lsm_tree.get(
            &self.file_manager,
            bucket,
            encoded_key.as_ref(),
            schema.as_ref(),
            self.schema_manager.as_ref(),
            options.columns(),
            selected_mask,
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
            merged = merged.merge(newer, &schema, Some(self.ttl_provider.time_provider()))?;
        }
        let snapshot = self.lsm_tree.db_state().load();
        value_to_vec_of_columns_with_vlog(
            merged,
            |pointer| {
                self.vlog_store
                    .read_pointer(&snapshot.vlog_version, pointer)
            },
            &schema,
            Some(self.ttl_provider.time_provider()),
        )
    }

    pub fn scan(&self, bucket: u16, range: Range<&[u8]>) -> Result<DbIterator<'static>> {
        self.scan_with_options(bucket, range, &ScanOptions::default())
    }

    pub fn scan_with_options(
        &self,
        bucket: u16,
        range: Range<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator<'static>> {
        self.scan_with_options_bounds(bucket, Some(range.start), Some(range.end), options)
    }

    pub(crate) fn scan_with_options_bounds(
        &self,
        bucket: u16,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator<'static>> {
        let snapshot = self.lsm_tree.db_state().load();
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
        let (lsm_iters, effective_schema) = self.lsm_tree.scan_with_snapshot(
            &self.file_manager,
            Arc::clone(&snapshot),
            Arc::clone(&schema),
            Arc::clone(&self.schema_manager),
            options.read_ahead_bytes()?,
            options.columns(),
        )?;
        let encode_scan_key = |key: &[u8]| {
            let mut encoded = bytes::BytesMut::with_capacity(2 + key.len());
            encode_key_ref_into(&RefKey::new(bucket, key), &mut encoded);
            encoded.freeze()
        };
        let start_key = encode_scan_key(start.unwrap_or(&[]));
        let end_bound = end.map(|key| (encode_scan_key(key), false));
        let mut iter: DbIterator<'static> = DbIterator::new(
            Vec::new(),
            lsm_iters,
            DbIteratorOptions {
                end_bound,
                snapshot,
                memtable_manager: None,
                vlog_store: Arc::clone(&self.vlog_store),
                ttl_provider: Arc::clone(&self.ttl_provider),
                schema: effective_schema,
            },
        );
        iter.seek(start_key.as_ref())?;
        Ok(iter)
    }
}
