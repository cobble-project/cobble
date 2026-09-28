use super::materialize::materialize_table_definition;
use super::model::{CatalogSchemaId, CatalogTable, ShardSchemaMapping, TableId};
use super::{CatalogError, CatalogResult, CatalogSchemaStore};
use crate::snapshot::TableSnapshotCommitter;
use crate::write::{TABLE_WRITE_PLAN_FORMAT, TABLE_WRITE_PLAN_VERSION};
use crate::{
    ReadOnlyTableBuilder, Table, TableError, TableReaderBuilder, TableWriteBuilder, TableWritePlan,
    TableWriterBuilder,
};
use cobble::{Config, CoordinatorConfig, Db, DbCoordinator, VolumeDescriptor, VolumeUsageKind};
use std::path::Path;
use std::sync::Arc;
use url::Url;

#[cfg(test)]
#[path = "../../tests/unit/catalog_storage.rs"]
mod storage_tests;

/// Process-local data storage routing and schema-store association for a catalog table.
///
/// `config` supplies shared Meta, Snapshot, and WAL locations. Writer and reader builders receive
/// their own primary-data and cache locations at runtime; the schema store manages its metadata
/// location independently. Neither credentials nor the store implementation are serialized into
/// writer plans.
pub struct CatalogRuntimeContext {
    pub(crate) config: Config,
    pub(crate) storage_id: String,
    pub(crate) schema_store: Arc<dyn CatalogSchemaStore>,
}

impl CatalogRuntimeContext {
    pub fn new(
        config: Config,
        storage_id: impl Into<String>,
        schema_store: Arc<dyn CatalogSchemaStore>,
    ) -> CatalogResult<Self> {
        let storage_id = storage_id.into();
        if !valid_storage_id(&storage_id) {
            return Err(CatalogError::InvalidIdentifier(format!(
                "invalid catalog storage id: {storage_id}"
            )));
        }
        if !config
            .volumes
            .iter()
            .any(|volume| volume.supports(VolumeUsageKind::Meta))
        {
            return Err(CatalogError::InvalidMetadata(
                "catalog runtime requires a shared metadata volume".to_string(),
            ));
        }
        Ok(Self {
            config,
            storage_id,
            schema_store,
        })
    }

    fn scoped_config(&self, runtime: Config, table_id: TableId) -> Config {
        scoped_table_config(&self.config.volumes, &self.storage_id, table_id, runtime)
    }
}

pub(crate) fn valid_storage_id(value: &str) -> bool {
    !value.is_empty() && value != "." && value != ".." && !value.contains(['/', '\\'])
}

fn scoped_table_config(
    shared_volumes: &[VolumeDescriptor],
    storage_id: &str,
    table_id: TableId,
    mut runtime: Config,
) -> Config {
    let relative_root = format!("{storage_id}/tables/TABLE-{table_id}");
    let mut volumes = shared_volumes
        .iter()
        .filter_map(|volume| shared_volume(volume, &relative_root))
        .collect::<Vec<_>>();
    volumes.extend(
        runtime
            .volumes
            .iter()
            .flat_map(|volume| runtime_volumes(volume, &relative_root)),
    );
    runtime.volumes = volumes;
    runtime
}

fn shared_volume(source: &VolumeDescriptor, relative_root: &str) -> Option<VolumeDescriptor> {
    plan_shared_volume(source).map(|mut volume| {
        volume.base_dir = append_relative_path(&volume.base_dir, relative_root);
        volume
    })
}

fn plan_shared_volume(source: &VolumeDescriptor) -> Option<VolumeDescriptor> {
    let mut volume = source.clone();
    volume.kinds = 0;
    for kind in [
        VolumeUsageKind::Meta,
        VolumeUsageKind::Snapshot,
        VolumeUsageKind::Wal,
    ] {
        if source.supports(kind) {
            volume.set_usage(kind);
        }
    }
    (volume.kinds != 0).then_some(volume)
}

fn runtime_volumes(source: &VolumeDescriptor, relative_root: &str) -> Vec<VolumeDescriptor> {
    let mut owned = source.clone();
    owned.kinds = 0;
    for kind in [
        VolumeUsageKind::PrimaryDataPriorityHigh,
        VolumeUsageKind::PrimaryDataPriorityMedium,
        VolumeUsageKind::PrimaryDataPriorityLow,
        VolumeUsageKind::Cache,
    ] {
        if source.supports(kind) {
            owned.set_usage(kind);
        }
    }
    let mut volumes = Vec::new();
    if owned.kinds != 0 {
        owned.base_dir = append_relative_path(&owned.base_dir, relative_root);
        volumes.push(owned);
    }
    if source.supports(VolumeUsageKind::Readonly) {
        let mut readonly = source.clone();
        readonly.kinds = 0;
        readonly.set_usage(VolumeUsageKind::Readonly);
        volumes.push(readonly);
    }
    volumes
}

fn append_relative_path(base: &str, relative: &str) -> String {
    if let Ok(mut url) = Url::parse(base) {
        let parent = url.path().trim_end_matches('/');
        let child = relative.trim_matches('/');
        let path = match (parent, child) {
            ("", child) => format!("/{child}"),
            (parent, "") => parent.to_string(),
            (parent, child) => format!("{parent}/{child}"),
        };
        url.set_path(&path);
        return url.to_string();
    }
    Path::new(base)
        .join(relative)
        .to_string_lossy()
        .into_owned()
}

pub(crate) fn physical_table_name(table_id: TableId) -> String {
    format!("t{table_id}")
}

impl CatalogTable {
    /// Return the stable physical column-family name for this catalog table.
    #[cfg(feature = "ffi")]
    #[doc(hidden)]
    pub fn physical_name(&self) -> String {
        physical_table_name(self.table_id())
    }

    /// Materialize this captured catalog schema into one writable shard.
    ///
    /// Calls for a shard must not run concurrently with other core schema updates.
    pub fn materialize_table(&self, db: Arc<Db>) -> CatalogResult<Table> {
        let (physical_name, metadata) = materialize_connected(self, db.as_ref())?;
        Table::from_metadata(db, physical_name, metadata).map_err(Into::into)
    }

    /// Start building a portable writer initialization plan for this table.
    pub fn new_write_builder(&self) -> TableWriteBuilder {
        TableWriteBuilder::new(self.clone(), self.runtime_context.config.total_buckets)
    }

    /// Build an owned writer for this table using its catalog-managed shared storage.
    pub fn writer_builder(&self, runtime: Config) -> CatalogResult<TableWriterBuilder> {
        self.new_write_builder()
            .total_buckets(runtime.total_buckets)
            .build()?
            .writer_builder(runtime)
    }

    /// Materialize this loaded catalog version into a writable table and refresh its local layout.
    ///
    /// The caller controls which catalog version is loaded; this method never follows catalog
    /// CURRENT implicitly.
    pub fn refresh_writer(&self, table: &mut Table) -> CatalogResult<bool> {
        if table.name() != physical_table_name(self.table_id()) {
            return Err(TableError::InvalidSchema(
                "Table does not belong to this catalog table".to_string(),
            )
            .into());
        }
        materialize_connected(self, table.db())?;
        table.refresh_schema().map_err(Into::into)
    }

    /// Build an owned snapshot reader for this table using its catalog-managed shared storage.
    pub fn reader_builder(&self, runtime: Config) -> CatalogResult<TableReaderBuilder> {
        let context = &self.runtime_context;
        let config = context.scoped_config(runtime, self.table_id());
        Ok(TableReaderBuilder::from_catalog(
            config,
            physical_table_name(self.table_id()),
            self.table_id(),
        ))
    }

    /// Build an owned shard snapshot table for this catalog table.
    pub fn readonly_table_builder(&self, runtime: Config) -> CatalogResult<ReadOnlyTableBuilder> {
        let context = &self.runtime_context;
        let config = context.scoped_config(runtime, self.table_id());
        Ok(ReadOnlyTableBuilder::from_catalog(
            config,
            physical_table_name(self.table_id()),
            self.table_id(),
        ))
    }

    /// Build an in-process committer in this table's global snapshot namespace.
    pub fn snapshot_committer(
        &self,
        runtime: Config,
        max_pending_commits: usize,
    ) -> CatalogResult<TableSnapshotCommitter> {
        let total_buckets = runtime.total_buckets;
        let coordinator = Arc::new(self.coordinator(runtime)?);
        Ok(TableSnapshotCommitter::new(
            coordinator,
            total_buckets,
            max_pending_commits,
        )?)
    }

    /// Open the core coordinator in this table's global snapshot namespace.
    pub fn coordinator(&self, runtime: Config) -> CatalogResult<DbCoordinator> {
        let context = &self.runtime_context;
        let config = context.scoped_config(runtime, self.table_id());
        Ok(DbCoordinator::open(CoordinatorConfig::from_config(
            &config,
        ))?)
    }
}

fn materialize_connected(
    table: &CatalogTable,
    db: &Db,
) -> CatalogResult<(String, crate::metadata::TableMetadata)> {
    let store = &table.runtime_context.schema_store;
    materialize_table_definition(
        db,
        table.table_id(),
        table.catalog_schema_id(),
        table.schema(),
        |schema_id| store.load_schema_version(table.table_id(), schema_id),
        |schema_id, core_schema_id| {
            store.record_shard_schema_mapping(ShardSchemaMapping::new(
                table.table_id(),
                db.id(),
                schema_id,
                core_schema_id,
            ))
        },
    )
}

pub(crate) fn build_write_plan(
    table: CatalogTable,
    total_buckets: u32,
) -> CatalogResult<TableWritePlan> {
    let context = &table.runtime_context;
    let schema_history = (0..=table.catalog_schema_id().as_u32())
        .map(|schema_id| {
            context
                .schema_store
                .load_schema_version(table.table_id(), CatalogSchemaId::from(schema_id))
        })
        .collect::<CatalogResult<Vec<_>>>()?;
    if schema_history.last() != Some(&table.schema_version) {
        return Err(CatalogError::InvalidMetadata(
            "loaded schema history does not match the catalog table".to_string(),
        ));
    }
    let table_id = table.table_id();
    let shared_volumes = context
        .config
        .volumes
        .iter()
        .filter_map(plan_shared_volume)
        .map(|volume| volume.without_credentials())
        .collect();
    let plan = TableWritePlan {
        format: TABLE_WRITE_PLAN_FORMAT.to_string(),
        version: TABLE_WRITE_PLAN_VERSION,
        identifier: table.identifier,
        table_id,
        schema_history,
        storage_id: context.storage_id.clone(),
        shared_volumes,
        total_buckets,
        auth_source: Some(context.config.clone()),
    };
    plan.validate()?;
    Ok(plan)
}

pub(crate) fn writer_builder_from_write_plan(
    plan: &TableWritePlan,
    runtime: Config,
) -> CatalogResult<TableWriterBuilder> {
    plan.validate()?;
    let credential_source = plan.auth_source.as_ref().unwrap_or(&runtime);
    let shared_volumes = plan
        .shared_volumes
        .iter()
        .map(|volume| volume.with_credentials_from(credential_source))
        .collect::<Vec<_>>();
    let mut config = scoped_table_config(&shared_volumes, &plan.storage_id, plan.table_id, runtime);
    config.total_buckets = plan.total_buckets;
    Ok(TableWriterBuilder::from_write_plan(
        config,
        physical_table_name(plan.table_id),
        plan.clone(),
    ))
}

pub(crate) fn materialize_write_plan(
    db: &Db,
    plan: &TableWritePlan,
) -> crate::Result<(String, crate::metadata::TableMetadata)> {
    plan.validate()?;
    let target = plan.target_schema();
    materialize_table_definition(
        db,
        plan.table_id,
        target.catalog_schema_id(),
        target.schema(),
        |schema_id| {
            plan.schema_history
                .get(schema_id.as_u32() as usize)
                .cloned()
                .ok_or_else(|| {
                    CatalogError::InvalidMetadata(
                        "table write plan is missing a schema version".to_string(),
                    )
                })
        },
        |_, _| Ok(()),
    )
    .map_err(|error| TableError::internal(error.to_string()))
}
