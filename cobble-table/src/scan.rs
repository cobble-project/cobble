use crate::metadata::TableMetadata;
use crate::runtime::TableSchemaTransformFactories;
use crate::table::{CompiledTable, compile_table, decode_table_scan_row, validate_name};
use crate::{Result, TableError, TableSchema, Value};
use bytes::Bytes;
use cobble::{
    Config, ReadOnlyDbBuilder, ScanOptions, ScanSplit, ScanSplitScanner, ShardSnapshotRef,
    VolumeDescriptor, VolumeUsageKind,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const TABLE_SCAN_PLAN_FORMAT: &str = "cobble-table-scan-plan";
const TABLE_SCAN_PLAN_VERSION: u32 = 1;

/// A serializable, fixed global-snapshot scan plan for one typed table.
///
/// A plan does not retain the snapshot against expiration; applications keep
/// the referenced global and shard snapshots available for their workers.
#[derive(Clone, Serialize, Deserialize)]
pub struct TableScanPlan {
    format: String,
    version: u32,
    name: String,
    global_snapshot_id: u64,
    total_buckets: u32,
    metadata: TableMetadata,
    shards: Vec<ShardSnapshotRef>,
    source_volumes: Vec<VolumeDescriptor>,
    #[serde(skip)]
    auth_source: Option<Config>,
}

impl TableScanPlan {
    pub(crate) fn from_global_reader(
        name: String,
        metadata: TableMetadata,
        global_snapshot_id: u64,
        total_buckets: u32,
        shards: Vec<ShardSnapshotRef>,
        config: Config,
    ) -> Result<Self> {
        let plan = Self {
            format: TABLE_SCAN_PLAN_FORMAT.to_string(),
            version: TABLE_SCAN_PLAN_VERSION,
            name,
            global_snapshot_id,
            total_buckets,
            metadata,
            shards,
            source_volumes: source_volumes(&config),
            auth_source: Some(config),
        };
        plan.validate()?;
        Ok(plan)
    }

    /// Return the fixed global snapshot selected for this scan.
    pub fn snapshot_id(&self) -> u64 {
        self.global_snapshot_id
    }

    /// Return the schema fixed into this plan.
    pub fn schema(&self) -> &TableSchema {
        &self.metadata.schema
    }

    /// Produce one independently serializable split per shard snapshot.
    pub fn splits(&self) -> Result<Vec<TableScanSplit>> {
        self.validate()?;
        Ok(self
            .shards
            .iter()
            .cloned()
            .map(|shard| TableScanSplit {
                format: self.format.clone(),
                version: self.version,
                name: self.name.clone(),
                global_snapshot_id: self.global_snapshot_id,
                total_buckets: self.total_buckets,
                metadata: self.metadata.clone(),
                split: full_scan_split(shard),
                source_volumes: self.source_volumes.clone(),
                auth_source: self.auth_source.clone(),
            })
            .collect())
    }

    fn validate(&self) -> Result<()> {
        validate_plan(
            &self.format,
            self.version,
            &self.name,
            self.total_buckets,
            &self.metadata,
            &self.source_volumes,
        )?;
        if self.shards.is_empty() {
            return Err(TableError::InvalidSchema(
                "table scan plan has no shard snapshots".to_string(),
            ));
        }
        Ok(())
    }
}

/// A serializable full-table scan assignment for one shard snapshot.
#[derive(Clone, Serialize, Deserialize)]
pub struct TableScanSplit {
    format: String,
    version: u32,
    name: String,
    global_snapshot_id: u64,
    total_buckets: u32,
    metadata: TableMetadata,
    split: ScanSplit,
    source_volumes: Vec<VolumeDescriptor>,
    #[serde(skip)]
    auth_source: Option<Config>,
}

impl TableScanSplit {
    /// Return the global snapshot that assigned this shard.
    pub fn snapshot_id(&self) -> u64 {
        self.global_snapshot_id
    }

    /// Return this split's shard snapshot identity.
    pub fn shard_snapshot_id(&self) -> u64 {
        self.split.shard.snapshot_id
    }

    /// Open this shard and return a typed full-scan iterator.
    pub fn create_scanner(&self, runtime: Config) -> Result<TableScanSplitScanner> {
        self.create_scanner_with_transforms(runtime, &TableSchemaTransformFactories::default())
    }

    /// Start configuring a worker-local scanner for this split.
    pub fn scanner_builder(&self, runtime: Config) -> TableScanSplitScannerBuilder {
        TableScanSplitScannerBuilder {
            split: self.clone(),
            runtime,
            transforms: TableSchemaTransformFactories::default(),
        }
    }

    fn create_scanner_with_transforms(
        &self,
        runtime: Config,
        transforms: &TableSchemaTransformFactories,
    ) -> Result<TableScanSplitScanner> {
        self.validate()?;
        let credential_source = self.auth_source.as_ref().unwrap_or(&runtime);
        let source_volumes = self
            .source_volumes
            .iter()
            .map(|volume| volume.with_credentials_from(credential_source))
            .collect::<Vec<_>>();
        let mut config = runtime_read_config(runtime);
        config.volumes.extend(source_volumes);
        config.total_buckets = self.total_buckets;

        let compiled = compile_table(self.metadata.clone(), self.total_buckets)?;
        let builder = transforms.apply_to_read_only_builder(ReadOnlyDbBuilder::new(config))?;
        let scanner = self.split.create_scanner_with_builder(
            builder,
            &ScanOptions::default().with_column_family(self.name.clone()),
        )?;
        Ok(TableScanSplitScanner {
            inner: scanner,
            compiled,
        })
    }

    fn validate(&self) -> Result<()> {
        validate_plan(
            &self.format,
            self.version,
            &self.name,
            self.total_buckets,
            &self.metadata,
            &self.source_volumes,
        )?;
        if self.split.shard.ranges.is_empty() {
            return Err(TableError::InvalidSchema(
                "table scan split has no bucket ranges".to_string(),
            ));
        }
        if self.split.start.is_some()
            || self.split.end.is_some()
            || self.split.start_bucket.is_some()
            || self.split.start_key_exclusive.is_some()
            || self.split.end_bucket.is_some()
            || self.split.end_key_inclusive.is_some()
        {
            return Err(TableError::InvalidSchema(
                "table scan split must cover its complete shard".to_string(),
            ));
        }
        Ok(())
    }
}

/// Builder for one worker-local typed scan split scanner.
pub struct TableScanSplitScannerBuilder {
    split: TableScanSplit,
    runtime: Config,
    transforms: TableSchemaTransformFactories,
}

impl TableScanSplitScannerBuilder {
    /// Register a factory for persisted schema transform specifications before opening.
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

    /// Open this fixed split using the supplied worker runtime configuration.
    pub fn open(self) -> Result<TableScanSplitScanner> {
        self.split
            .create_scanner_with_transforms(self.runtime, &self.transforms)
    }
}

/// Typed iterator over every row assigned to one table scan split.
pub struct TableScanSplitScanner {
    inner: ScanSplitScanner,
    compiled: Arc<CompiledTable>,
}

impl Iterator for TableScanSplitScanner {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|row| {
            let (_, key, columns) = row?;
            decode_table_scan_row(&self.compiled, &key, &columns)
        })
    }
}

fn full_scan_split(shard: ShardSnapshotRef) -> ScanSplit {
    ScanSplit {
        shard,
        start: None,
        end: None,
        start_bucket: None,
        start_key_exclusive: None,
        end_bucket: None,
        end_key_inclusive: None,
    }
}

fn validate_plan(
    format: &str,
    version: u32,
    name: &str,
    total_buckets: u32,
    metadata: &TableMetadata,
    source_volumes: &[VolumeDescriptor],
) -> Result<()> {
    if format != TABLE_SCAN_PLAN_FORMAT {
        return Err(TableError::InvalidSchema(format!(
            "unsupported table scan plan format: {format}"
        )));
    }
    if version != TABLE_SCAN_PLAN_VERSION {
        return Err(TableError::InvalidSchema(format!(
            "unsupported table scan plan version: {version}"
        )));
    }
    validate_name(name.to_string())?;
    metadata.validate()?;
    if !(1..=u16::MAX as u32 + 1).contains(&total_buckets) {
        return Err(TableError::InvalidSchema(
            "table scan plan total_buckets must be in range 1..=65536".to_string(),
        ));
    }
    if !source_volumes
        .iter()
        .any(|volume| volume.supports(VolumeUsageKind::Meta))
        || source_volumes.iter().any(|volume| {
            volume.supports(VolumeUsageKind::PrimaryDataPriorityHigh)
                || volume.supports(VolumeUsageKind::PrimaryDataPriorityMedium)
                || volume.supports(VolumeUsageKind::PrimaryDataPriorityLow)
                || volume.supports(VolumeUsageKind::Cache)
                || volume.supports(VolumeUsageKind::Readonly)
        })
    {
        return Err(TableError::InvalidSchema(
            "table scan plan has invalid source volumes".to_string(),
        ));
    }
    Ok(())
}

fn source_volumes(config: &Config) -> Vec<VolumeDescriptor> {
    config
        .volumes
        .iter()
        .filter_map(|source| {
            let mut volume = source.clone();
            volume.kinds = 0;
            for kind in [VolumeUsageKind::Meta, VolumeUsageKind::Snapshot] {
                if source.supports(kind) {
                    volume.set_usage(kind);
                }
            }
            (volume.kinds != 0).then_some(volume.without_credentials())
        })
        .collect()
}

fn runtime_read_config(mut runtime: Config) -> Config {
    runtime.volumes = runtime
        .volumes
        .into_iter()
        .filter_map(|source| {
            let mut volume = source.clone();
            volume.kinds = 0;
            for kind in [
                VolumeUsageKind::PrimaryDataPriorityHigh,
                VolumeUsageKind::PrimaryDataPriorityMedium,
                VolumeUsageKind::PrimaryDataPriorityLow,
                VolumeUsageKind::Cache,
                VolumeUsageKind::Readonly,
            ] {
                if source.supports(kind) {
                    volume.set_usage(kind);
                }
            }
            (volume.kinds != 0).then_some(volume)
        })
        .collect();
    runtime
}
