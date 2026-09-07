use crate::catalog::{CatalogResult, CatalogSchemaId, CatalogTable, TableId, TableIdentifier};
use crate::{Result, TableError, TableSchema, TableWriterBuilder};
use cobble::{Config, VolumeDescriptor, VolumeUsageKind};
use serde::{Deserialize, Serialize};

pub(crate) const TABLE_WRITE_PLAN_FORMAT: &str = "cobble-table-write-plan";
pub(crate) const TABLE_WRITE_PLAN_VERSION: u32 = 1;

/// Builds a portable writer initialization plan from one catalog table definition.
pub struct TableWriteBuilder {
    table: CatalogTable,
    total_buckets: u32,
}

impl TableWriteBuilder {
    pub(crate) fn new(table: CatalogTable, total_buckets: u32) -> Self {
        Self {
            table,
            total_buckets,
        }
    }

    /// Set the table-wide bucket count captured in the plan.
    pub fn total_buckets(mut self, total_buckets: u32) -> Self {
        self.total_buckets = total_buckets;
        self
    }

    /// Freeze this table definition and shared-storage locations for worker use.
    pub fn build(self) -> CatalogResult<TableWritePlan> {
        crate::catalog::build_write_plan(self.table, self.total_buckets)
    }
}

/// Serializable, catalog-independent initialization for one table writer shard.
///
/// The plan carries only shared metadata, snapshot, and WAL locations. Workers supply their own
/// primary-data, cache, and read-only runtime volumes when creating a writer.
#[derive(Clone, Serialize, Deserialize)]
pub struct TableWritePlan {
    pub(crate) format: String,
    pub(crate) version: u32,
    pub(crate) identifier: TableIdentifier,
    pub(crate) table_id: TableId,
    pub(crate) catalog_schema_id: CatalogSchemaId,
    pub(crate) schema: TableSchema,
    pub(crate) storage_id: String,
    pub(crate) shared_volumes: Vec<VolumeDescriptor>,
    pub(crate) total_buckets: u32,
    #[serde(skip)]
    pub(crate) auth_source: Option<Config>,
}

impl TableWritePlan {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.format != TABLE_WRITE_PLAN_FORMAT {
            return Err(TableError::InvalidSchema(format!(
                "unsupported table write plan format: {}",
                self.format
            )));
        }
        if self.version != TABLE_WRITE_PLAN_VERSION {
            return Err(TableError::InvalidSchema(format!(
                "unsupported table write plan version: {}",
                self.version
            )));
        }
        if self.identifier.namespace().is_empty()
            || self.identifier.name().is_empty()
            || self.identifier.name() != self.identifier.name().trim()
        {
            return Err(TableError::InvalidSchema(
                "table write plan has an invalid identifier".to_string(),
            ));
        }
        self.schema.validate()?;
        if self.storage_id.is_empty()
            || self.storage_id == "."
            || self.storage_id == ".."
            || self.storage_id.contains(['/', '\\'])
        {
            return Err(TableError::InvalidSchema(
                "table write plan has an invalid storage id".to_string(),
            ));
        }
        if !(1..=u16::MAX as u32 + 1).contains(&self.total_buckets) {
            return Err(TableError::InvalidSchema(
                "table write plan total_buckets must be in range 1..=65536".to_string(),
            ));
        }
        if self.shared_volumes.is_empty()
            || !self
                .shared_volumes
                .iter()
                .any(|volume| volume.supports(VolumeUsageKind::Meta))
            || self.shared_volumes.iter().any(|volume| {
                volume.supports(VolumeUsageKind::PrimaryDataPriorityHigh)
                    || volume.supports(VolumeUsageKind::PrimaryDataPriorityMedium)
                    || volume.supports(VolumeUsageKind::PrimaryDataPriorityLow)
                    || volume.supports(VolumeUsageKind::Cache)
                    || volume.supports(VolumeUsageKind::Readonly)
            })
        {
            return Err(TableError::InvalidSchema(
                "table write plan has invalid shared volumes".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Initialize a shard writer builder using this fixed table definition.
    pub fn writer_builder(&self, runtime: Config) -> CatalogResult<TableWriterBuilder> {
        crate::catalog::writer_builder_from_write_plan(self, runtime)
    }
}
