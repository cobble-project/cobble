use crate::error::Result;
use crate::merge_operator::MergeOperatorResolver;
use crate::schema::SchemaTransformRegistry;
use crate::{Reader, ReaderConfig};
use bytes::Bytes;
use std::sync::Arc;

/// Builder for a lazy [`Reader`] with custom runtime schema wiring.
pub struct ReaderBuilder {
    config: ReaderConfig,
    resolver: Option<Arc<dyn MergeOperatorResolver>>,
    transforms: Arc<SchemaTransformRegistry>,
}

impl ReaderBuilder {
    pub fn new(config: ReaderConfig) -> Self {
        Self {
            config,
            resolver: None,
            transforms: Arc::new(SchemaTransformRegistry::default()),
        }
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

    /// Opens a fixed global snapshot. Shard schema transforms are resolved lazily
    /// when a routed bucket first opens its read-only shard.
    pub fn open(self, global_snapshot_id: u64) -> Result<Reader> {
        Reader::open_with_resolver_and_transforms(
            self.config,
            global_snapshot_id,
            self.resolver,
            self.transforms,
        )
    }

    /// Opens the current global snapshot pointer. Refreshes remain lazy: a
    /// missing shard transform fails only when that shard is opened, and the
    /// failed open is not inserted into the cache.
    pub fn open_current(self) -> Result<Reader> {
        Reader::open_current_with_resolver_and_transforms(
            self.config,
            self.resolver,
            self.transforms,
        )
    }
}
