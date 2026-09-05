use crate::cache::BlockCache;
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

    /// Registers a stable persisted transform ID for one optional column's bytes.
    /// Missing IDs make this direct snapshot open fail before returning.
    pub fn register_schema_transform<F>(
        self,
        transform_id: impl Into<String>,
        transform: F,
    ) -> Result<Self>
    where
        F: Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync + 'static,
    {
        self.transforms.register(transform_id, transform)?;
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
}
