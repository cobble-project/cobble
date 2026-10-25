use crate::catalog::{CatalogTable, physical_table_name};
use crate::runtime::validate_catalog_binding;
use crate::table::load_table_metadata;
use crate::transform::TableSchemaTransformFactories;
use crate::{ReadOnlyTable, Result};
use bytes::Bytes;
use cobble::{
    Config, MergeOperatorResolver, MetricSample, ReadOnlyDb, ReadOnlyDbBuilder, ShardSnapshotRef,
};
use std::sync::Arc;

/// Fixed snapshot access to multiple typed tables sharing one read-only database shard.
///
/// Tables, projections and scanners retain their shared database independently of this store.
/// The source snapshot and its files must remain available; opening does not establish
/// cross-process snapshot retention.
pub struct ReadOnlyTableStore {
    db: Arc<ReadOnlyDb>,
}

/// Snapshot database builder that installs table merge operators and schema transforms.
pub struct ReadOnlyTableStoreBuilder {
    builder: ReadOnlyDbBuilder,
    transforms: TableSchemaTransformFactories,
}

impl ReadOnlyTableStoreBuilder {
    pub fn new(config: Config) -> Self {
        Self {
            builder: ReadOnlyDbBuilder::new(config)
                .merge_operator_resolver(crate::table_merge_operator_resolver(None)),
            transforms: TableSchemaTransformFactories::default(),
        }
    }

    pub fn db_id(mut self, db_id: impl Into<String>) -> Self {
        self.builder = self.builder.db_id(db_id);
        self
    }

    pub fn merge_operator_resolver(mut self, resolver: Arc<dyn MergeOperatorResolver>) -> Self {
        self.builder = self
            .builder
            .merge_operator_resolver(crate::table_merge_operator_resolver(Some(resolver)));
        self
    }

    /// Register a custom persisted transform in addition to built-in table transforms.
    pub fn register_schema_transform<F, T>(
        mut self,
        transform_type: impl Into<String>,
        factory: F,
    ) -> Result<Self>
    where
        F: Fn(&[u8]) -> cobble::Result<T> + Send + Sync + 'static,
        T: Fn(Option<Bytes>) -> cobble::Result<Option<Bytes>> + Send + Sync + 'static,
    {
        self.transforms.register(transform_type, factory)?;
        Ok(self)
    }

    /// Open a fixed snapshot of the selected source database. A DB id is required.
    pub fn open(self, snapshot_id: u64) -> Result<ReadOnlyTableStore> {
        Ok(ReadOnlyTableStore {
            db: Arc::new(self.transforms.apply_to(self.builder)?.open(snapshot_id)?),
        })
    }

    /// Open directly from a manifest, using its snapshot id and the source database identity.
    ///
    /// The source DB id is inferred from the conventional manifest path unless set explicitly.
    pub fn open_with_manifest_path(
        self,
        manifest_path: impl Into<String>,
    ) -> Result<ReadOnlyTableStore> {
        Ok(ReadOnlyTableStore {
            db: Arc::new(
                self.transforms
                    .apply_to(self.builder)?
                    .open_with_manifest_path(manifest_path)?,
            ),
        })
    }

    /// Open an explicit shard manifest using the configured source routes and credentials.
    pub fn open_shard_snapshot(self, shard: &ShardSnapshotRef) -> Result<ReadOnlyTableStore> {
        Ok(ReadOnlyTableStore {
            db: Arc::new(
                self.transforms
                    .apply_to(self.builder)?
                    .open_shard_snapshot(shard)?,
            ),
        })
    }
}

impl ReadOnlyTableStore {
    pub fn builder(config: Config) -> ReadOnlyTableStoreBuilder {
        ReadOnlyTableStoreBuilder::new(config)
    }

    /// Open a physical table using the schema persisted in this snapshot.
    pub fn open_table(&self, name: impl Into<String>) -> Result<ReadOnlyTable> {
        ReadOnlyTable::open(Arc::clone(&self.db), name)
    }

    /// Open by catalog identity without applying the catalog's schema or materializing storage.
    pub fn open_materialized_table(&self, table: &CatalogTable) -> Result<ReadOnlyTable> {
        let name = physical_table_name(table.table_id());
        let metadata = load_table_metadata(&self.db.current_schema(), &name)?;
        validate_catalog_binding(&metadata, Some(table.table_id()))?;
        ReadOnlyTable::from_shard_metadata(Arc::clone(&self.db), name, metadata)
    }

    pub fn id(&self) -> &str {
        self.db.id()
    }

    /// Metrics include all tables and column families in this fixed store snapshot.
    pub fn metrics(&self) -> Vec<MetricSample> {
        self.db.metrics()
    }
}
