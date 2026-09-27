use super::CatalogResult;
use super::model::{CatalogTable, TableId};
use crate::snapshot::TableSnapshotCommitter;
use crate::write::{TABLE_WRITE_PLAN_FORMAT, TABLE_WRITE_PLAN_VERSION};
use crate::{
    ReadOnlyTableBuilder, Table, TableReaderBuilder, TableWriteBuilder, TableWritePlan,
    TableWriterBuilder,
};
use cobble::{Config, CoordinatorConfig, Db, DbCoordinator, VolumeDescriptor, VolumeUsageKind};
use std::path::Path;
use std::sync::Arc;
use url::Url;

#[cfg(test)]
#[path = "../../tests/unit/catalog_storage.rs"]
mod storage_tests;

/// Process-local storage association for a catalog table.
///
/// It is not part of catalog metadata. In this stage, FileCatalog supplies this association.
pub(crate) struct CatalogRuntimeContext {
    pub(crate) config: Config,
    pub(crate) storage_id: String,
}

impl CatalogRuntimeContext {
    fn scoped_config(&self, runtime: Config, table_id: TableId) -> Config {
        scoped_table_config(&self.config.volumes, &self.storage_id, table_id, runtime)
    }
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
        physical_table_name(self.table_id)
    }

    /// Materialize this captured catalog schema into one writable shard.
    ///
    /// Calls for a shard must not run concurrently with other core schema updates.
    pub fn materialize_table(&self, db: Arc<Db>) -> CatalogResult<Table> {
        super::file_catalog::materialize_catalog_table(self, db)
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
        super::file_catalog::refresh_catalog_table_writer(self, table)
    }

    /// Build an owned snapshot reader for this table using its catalog-managed shared storage.
    pub fn reader_builder(&self, runtime: Config) -> CatalogResult<TableReaderBuilder> {
        let context = &self.runtime_context;
        let config = context.scoped_config(runtime, self.table_id);
        Ok(TableReaderBuilder::from_catalog(
            config,
            physical_table_name(self.table_id),
            self.table_id,
        ))
    }

    /// Build an owned shard snapshot table for this catalog table.
    pub fn readonly_table_builder(&self, runtime: Config) -> CatalogResult<ReadOnlyTableBuilder> {
        let context = &self.runtime_context;
        let config = context.scoped_config(runtime, self.table_id);
        Ok(ReadOnlyTableBuilder::from_catalog(
            config,
            physical_table_name(self.table_id),
            self.table_id,
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
        let config = context.scoped_config(runtime, self.table_id);
        Ok(DbCoordinator::open(CoordinatorConfig::from_config(
            &config,
        ))?)
    }
}

pub(crate) fn build_write_plan(
    table: CatalogTable,
    total_buckets: u32,
) -> CatalogResult<TableWritePlan> {
    let context = &table.runtime_context;
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
        table_id: table.table_id,
        catalog_schema_id: table.catalog_schema_id,
        schema: table.schema,
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
    let store_config = Config {
        volumes: shared_volumes.clone(),
        ..Config::default()
    };
    let mut config = scoped_table_config(&shared_volumes, &plan.storage_id, plan.table_id, runtime);
    config.total_buckets = plan.total_buckets;
    Ok(TableWriterBuilder::from_write_plan(
        config,
        physical_table_name(plan.table_id),
        plan.clone(),
        store_config,
    ))
}
