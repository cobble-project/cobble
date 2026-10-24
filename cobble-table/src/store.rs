use crate::catalog::{CatalogResult, CatalogTable};
use crate::transform::TableSchemaTransformFactories;
use crate::{Result, Table, TableSchema};
use bytes::Bytes;
use cobble::{
    Config, Db, DbBuilder, ExpandStorageMode, MergeOperatorResolver, MetricSample,
    ShardSnapshotMetadata,
};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

/// Lifecycle owner for multiple typed tables sharing one writable database shard.
///
/// Tables use the existing direct storage paths. Snapshots capture the entire store, not an
/// individual table. Snapshot retention and WAL behavior remain controlled by [`Config`].
pub struct TableStore {
    db: Arc<Db>,
}

/// Thin database builder that installs built-in table transforms before recovery.
pub struct TableStoreBuilder {
    builder: DbBuilder,
    transforms: TableSchemaTransformFactories,
}

impl TableStoreBuilder {
    pub fn new(config: Config) -> Self {
        Self {
            builder: DbBuilder::new(config)
                .merge_operator_resolver(crate::table_merge_operator_resolver(None)),
            transforms: TableSchemaTransformFactories::default(),
        }
    }

    pub fn db_id(mut self, db_id: impl Into<String>) -> Self {
        self.builder = self.builder.db_id(db_id);
        self
    }

    pub fn bucket_ranges(mut self, ranges: Vec<RangeInclusive<u16>>) -> Self {
        self.builder = self.builder.bucket_ranges(ranges);
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

    pub fn open(self) -> Result<TableStore> {
        Ok(TableStore {
            db: Arc::new(self.transforms.apply_to(self.builder)?.open()?),
        })
    }

    /// Restore a new store from a source snapshot, retaining registered schema transforms.
    ///
    /// A fresh DB identity and snapshot chain are created; the builder's DB ID is ignored.
    /// Bucket ranges come from the source manifest, not the builder. The source WAL is not replayed.
    pub fn open_new_with_manifest_path(
        self,
        manifest_path: impl Into<String>,
    ) -> Result<TableStore> {
        Ok(TableStore {
            db: Arc::new(
                self.transforms
                    .apply_to(self.builder)?
                    .open_new_with_manifest_path(manifest_path)?,
            ),
        })
    }

    /// Resume the existing store's snapshot chain at exactly this snapshot.
    ///
    /// This is snapshot-only recovery: later durable WAL records are not replayed.
    pub fn resume_from_snapshot(self, snapshot_id: u64) -> Result<TableStore> {
        Ok(TableStore {
            db: Arc::new(
                self.transforms
                    .apply_to(self.builder)?
                    .resume_from_snapshot(snapshot_id)?,
            ),
        })
    }
}

impl TableStore {
    pub fn builder(config: Config) -> TableStoreBuilder {
        TableStoreBuilder::new(config)
    }

    /// Create a table or reopen it when its persisted schema is identical.
    pub fn create_table(&self, name: impl Into<String>, schema: TableSchema) -> Result<Table> {
        Table::create(Arc::clone(&self.db), name, schema)
    }

    pub fn open_table(&self, name: impl Into<String>) -> Result<Table> {
        Table::open(Arc::clone(&self.db), name)
    }

    /// Materialize a catalog definition using this store's shared storage, without opening a
    /// separate database. Calls must be externally serialized with other schema updates.
    pub fn materialize_table(&self, table: &CatalogTable) -> CatalogResult<Table> {
        table.materialize_table(Arc::clone(&self.db))
    }

    /// Open by catalog table identity using the store's persisted schema, without applying the
    /// captured catalog version. After recovery this preserves the selected snapshot's schema.
    pub fn open_materialized_table(&self, table: &CatalogTable) -> Result<Table> {
        Table::open(
            Arc::clone(&self.db),
            crate::catalog::physical_table_name(table.table_id()),
        )
    }

    pub fn id(&self) -> &str {
        self.db.id()
    }

    /// Metrics include all tables and column families in this store.
    pub fn metrics(&self) -> Vec<MetricSample> {
        self.db.metrics()
    }

    /// Capture all tables at one shard boundary without waiting for manifest publication.
    pub fn snapshot(&self) -> Result<u64> {
        Ok(self.db.snapshot()?)
    }

    /// Capture the whole store; the callback reports completed manifest publication or failure.
    pub fn snapshot_with_callback<F>(&self, callback: F) -> Result<u64>
    where
        F: Fn(cobble::Result<ShardSnapshotMetadata>) + Send + Sync + 'static,
    {
        Ok(self.db.snapshot_with_callback(callback)?)
    }

    /// Capture all tables and wait for snapshot publication without polling.
    pub fn snapshot_and_wait(&self) -> Result<ShardSnapshotMetadata> {
        crate::snapshot::snapshot_and_wait(&self.db)
    }

    pub fn shard_snapshot_metadata(&self, snapshot_id: u64) -> Result<ShardSnapshotMetadata> {
        Ok(self.db.shard_snapshot_metadata(snapshot_id)?)
    }

    pub fn cancel_snapshot(&self, snapshot_id: u64) -> Result<bool> {
        Ok(self.db.cancel_snapshot(snapshot_id)?)
    }

    pub fn retain_snapshot(&self, snapshot_id: u64) -> bool {
        self.db.retain_snapshot(snapshot_id)
    }

    pub fn expire_snapshot(&self, snapshot_id: u64) -> Result<bool> {
        Ok(self.db.expire_snapshot(snapshot_id)?)
    }

    /// Restart the whole store at an exact snapshot after releasing every table, projection,
    /// and scanner. Reopen tables afterwards so their schemas match the restored state.
    ///
    /// This inherits [`Db::switch_to_snapshot`]'s WAL caveat: switching does not isolate the
    /// old WAL sequence. Before publishing a new snapshot, restarting at this historical state
    /// requires [`TableStoreBuilder::resume_from_snapshot`]; ordinary latest recovery can replay
    /// the newer WAL tail. Cross-process snapshot mutations must be externally serialized.
    pub fn switch_to_snapshot(&mut self, snapshot_id: u64) -> Result<()> {
        self.exclusive_db()?.switch_to_snapshot(snapshot_id)?;
        Ok(())
    }

    /// Import buckets from a source manifest after releasing every table-derived handle.
    ///
    /// Expansion may change the shared schema; reopen tables after this operation. The returned
    /// snapshot follows the core import semantics; leased file adoption may still be running.
    pub fn expand_bucket_from_manifest(
        &mut self,
        source_db_id: impl Into<String>,
        source_manifest_path: impl Into<String>,
        ranges: Option<Vec<RangeInclusive<u16>>>,
        storage_mode: ExpandStorageMode,
    ) -> Result<u64> {
        Ok(self.exclusive_db()?.expand_bucket_from_manifest(
            source_db_id,
            source_manifest_path,
            ranges,
            storage_mode,
        )?)
    }

    /// Remove owned buckets after releasing every table-derived handle.
    /// Returns the core snapshot of the state immediately before removal.
    pub fn shrink_bucket(&mut self, ranges: Vec<RangeInclusive<u16>>) -> Result<u64> {
        Ok(self.exclusive_db()?.shrink_bucket(ranges)?)
    }

    pub fn wait_for_expand_adoption(&self, timeout: Duration) -> Result<()> {
        Ok(self.db.wait_for_expand_adoption(timeout)?)
    }

    /// Close the shared database. Release scanners first: close waits for their active reads
    /// to finish. Table and projection handles may remain, but future operations fail afterwards.
    pub fn close(&self) -> Result<()> {
        Ok(self.db.close()?)
    }

    fn exclusive_db(&mut self) -> Result<&mut Db> {
        Arc::get_mut(&mut self.db).ok_or_else(|| {
            cobble::Error::InvalidState(
                "release all table, projection, and scanner handles before changing the store state"
                    .to_string(),
            )
            .into()
        })
    }
}
