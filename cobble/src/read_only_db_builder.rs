use crate::cache::BlockCache;
use crate::coordinator::ShardSnapshotRef;
use crate::error::Result;
use crate::merge_operator::MergeOperatorResolver;
use crate::metrics_manager::MetricsManager;
use crate::schema::SchemaTransformRegistry;
use crate::{Config, ReadOnlyDb};
use bytes::Bytes;
use std::sync::Arc;

/// Builder for a snapshot-backed [`ReadOnlyDb`] with custom runtime schema wiring.
pub struct ReadOnlyDbBuilder {
    config: Config,
    db_id: Option<String>,
    block_cache: Option<BlockCache>,
    metrics_manager: Option<Arc<MetricsManager>>,
    resolver: Option<Arc<dyn MergeOperatorResolver>>,
    transforms: Arc<SchemaTransformRegistry>,
}

impl ReadOnlyDbBuilder {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            db_id: None,
            block_cache: None,
            metrics_manager: None,
            resolver: None,
            transforms: Arc::new(SchemaTransformRegistry::default()),
        }
    }

    /// Selects the source database id recorded by the snapshot manifest.
    pub fn db_id(mut self, db_id: impl Into<String>) -> Self {
        self.db_id = Some(db_id.into());
        self
    }

    pub fn block_cache(mut self, block_cache: BlockCache) -> Self {
        self.block_cache = Some(block_cache);
        self
    }

    pub fn metrics_manager(mut self, metrics_manager: Arc<MetricsManager>) -> Self {
        self.metrics_manager = Some(metrics_manager);
        self
    }

    pub fn merge_operator_resolver(mut self, resolver: Arc<dyn MergeOperatorResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// Register a factory for persisted schema transform specifications.
    pub fn register_schema_transform<F, T>(
        self,
        transform_type: impl Into<String>,
        factory: F,
    ) -> Result<Self>
    where
        F: Fn(&[u8]) -> Result<T> + Send + Sync + 'static,
        T: Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync + 'static,
    {
        self.transforms.register(transform_type, factory)?;
        Ok(self)
    }

    /// Opens the selected snapshot. A source [`Self::db_id`] is required.
    pub fn open(self, snapshot_id: u64) -> Result<ReadOnlyDb> {
        let db_id = self.db_id.ok_or_else(|| {
            crate::Error::ConfigError(
                "ReadOnlyDbBuilder requires db_id for the snapshot source".to_string(),
            )
        })?;
        let metrics_manager = self
            .metrics_manager
            .unwrap_or_else(|| Arc::new(MetricsManager::new(&db_id)));
        ReadOnlyDb::open_internal(
            self.config,
            snapshot_id,
            db_id,
            self.block_cache,
            metrics_manager,
            self.resolver,
            self.transforms,
        )
    }

    /// Opens the exact shard snapshot recorded by a global snapshot manifest.
    ///
    /// The manifest path is authoritative and may be under any subdirectory
    /// of the configured source volumes. Callers must still provide storage
    /// routes and credentials covering that shard's metadata and data files.
    pub fn open_shard_snapshot(self, shard: &ShardSnapshotRef) -> Result<ReadOnlyDb> {
        if shard.manifest_path.trim().is_empty() {
            return Err(crate::Error::ConfigError(format!(
                "Shard snapshot {}:{} is missing its manifest path",
                shard.db_id, shard.snapshot_id
            )));
        }
        if let Some(db_id) = self.db_id.as_ref()
            && db_id != &shard.db_id
        {
            return Err(crate::Error::ConfigError(format!(
                "ReadOnlyDbBuilder db_id {} does not match shard snapshot db_id {}",
                db_id, shard.db_id
            )));
        }
        let metrics_manager = self
            .metrics_manager
            .unwrap_or_else(|| Arc::new(MetricsManager::new(&shard.db_id)));
        ReadOnlyDb::open_from_manifest_path_internal(
            self.config,
            shard.snapshot_id,
            shard.db_id.clone(),
            &shard.manifest_path,
            self.block_cache,
            metrics_manager,
            self.resolver,
            self.transforms,
        )
    }
}

impl crate::SchemaTransformRegistrar for ReadOnlyDbBuilder {
    fn register_schema_transform<F, T>(
        &self,
        transform_type: impl Into<String>,
        factory: F,
    ) -> Result<()>
    where
        F: Fn(&[u8]) -> Result<T> + Send + Sync + 'static,
        T: Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync + 'static,
    {
        self.transforms.register(transform_type, factory)
    }
}
