use crate::Config;
use crate::db::{Db, RecoveryMode};
use crate::error::Result;
use crate::governance::DbGovernance;
use crate::merge_operator::MergeOperatorResolver;
use crate::schema::SchemaTransformRegistry;
use bytes::Bytes;
use std::ops::RangeInclusive;
use std::sync::Arc;

pub(crate) type DbBuilderParts = (
    Config,
    Vec<RangeInclusive<u16>>,
    Option<String>,
    Option<Arc<dyn DbGovernance>>,
    Option<Arc<dyn MergeOperatorResolver>>,
    Arc<SchemaTransformRegistry>,
);

/// Builder for opening a writable [`Db`] with optional custom runtime wiring.
pub struct DbBuilder {
    config: Config,
    bucket_ranges: Vec<RangeInclusive<u16>>,
    db_id: Option<String>,
    governance: Option<Arc<dyn DbGovernance>>,
    resolver: Option<Arc<dyn MergeOperatorResolver>>,
    transforms: Arc<SchemaTransformRegistry>,
}

impl DbBuilder {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            bucket_ranges: Vec::new(),
            db_id: None,
            governance: None,
            resolver: None,
            transforms: Arc::new(SchemaTransformRegistry::default()),
        }
    }

    pub fn bucket_ranges(mut self, bucket_ranges: Vec<RangeInclusive<u16>>) -> Self {
        self.bucket_ranges = bucket_ranges;
        self
    }

    pub fn db_id<S>(mut self, db_id: S) -> Self
    where
        S: Into<String>,
    {
        self.db_id = Some(db_id.into());
        self
    }

    pub fn governance(mut self, governance: Arc<dyn DbGovernance>) -> Self {
        self.governance = Some(governance);
        self
    }

    /// Use custom merge operators when loading persisted schemas.
    pub fn merge_operator_resolver(mut self, resolver: Arc<dyn MergeOperatorResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// Register a single-column schema transform under a stable persisted ID.
    ///
    /// Register transforms before opening an existing database so persisted schema
    /// transitions can be validated before recovery starts background work.
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

    pub fn open(self) -> Result<Db> {
        Db::open_with_builder(self)
    }

    /// Open this database at an exact selected snapshot boundary.
    pub fn open_from_snapshot(self, snapshot_id: u64) -> Result<Db> {
        self.open_from_snapshot_with_recovery_mode(snapshot_id, RecoveryMode::SnapshotOnly)
    }

    /// Open this database at a selected snapshot using the requested recovery mode.
    pub fn open_from_snapshot_with_recovery_mode(
        self,
        snapshot_id: u64,
        recovery_mode: RecoveryMode,
    ) -> Result<Db> {
        Db::open_from_snapshot_with_builder(self, snapshot_id, recovery_mode)
    }

    /// Resume from the latest snapshot and replay the durable WAL tail when available.
    pub fn resume(self) -> Result<Db> {
        self.resume_with_recovery_mode(RecoveryMode::LatestWithWal)
    }

    /// Resume from the latest snapshot using the requested recovery mode.
    pub fn resume_with_recovery_mode(self, recovery_mode: RecoveryMode) -> Result<Db> {
        Db::resume_with_builder(self, None, recovery_mode)
    }

    /// Resume from a selected snapshot while retaining its existing snapshot chain.
    pub fn resume_from_snapshot(self, snapshot_id: u64) -> Result<Db> {
        self.resume_from_snapshot_with_recovery_mode(snapshot_id, RecoveryMode::SnapshotOnly)
    }

    /// Resume from a selected snapshot using the requested recovery mode.
    pub fn resume_from_snapshot_with_recovery_mode(
        self,
        snapshot_id: u64,
        recovery_mode: RecoveryMode,
    ) -> Result<Db> {
        Db::resume_with_builder(self, Some(snapshot_id), recovery_mode)
    }

    pub(crate) fn into_parts(self) -> DbBuilderParts {
        (
            self.config,
            self.bucket_ranges,
            self.db_id,
            self.governance,
            self.resolver,
            self.transforms,
        )
    }
}
