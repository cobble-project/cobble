use crate::metadata::TableMetadata;
#[cfg(feature = "ffi")]
use crate::table::build_scan_options_for_fields;
use crate::table::{
    CompiledTable, ProjectionPlan, build_schema_projection_parts, compile_table,
    decode_projected_scan_row, decode_table_scan_row, validate_name,
};
use crate::transform::TableSchemaTransformFactories;
use crate::{Result, TableError, TableReadSchema, TableSchema, Value};
use bytes::Bytes;
use cobble::{
    Config, ReadOnlyDbBuilder, ScanOptions, ScanSplit, ScanSplitScanner, ShardSnapshotRef,
    VolumeDescriptor, VolumeUsageKind,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const TABLE_SCAN_PLAN_FORMAT: &str = "cobble-table-scan-plan";
const TABLE_SCAN_PLAN_VERSION: u32 = 1;

type ScanProjection = Arc<CompiledScanProjection>;

struct CompiledScanProjection {
    compiled: Arc<CompiledTable>,
    plan: Arc<ProjectionPlan>,
    options: ScanOptions,
}

fn compile_projection(
    name: &str,
    metadata: &TableMetadata,
    total_buckets: u32,
    selection: Option<&TableReadSchema>,
) -> Result<Option<ScanProjection>> {
    selection
        .map(|schema| {
            let compiled = compile_table(metadata.clone(), total_buckets)?;
            let (plan, _, options) =
                build_schema_projection_parts(name, Arc::clone(&compiled), schema)?;
            Ok(Arc::new(CompiledScanProjection {
                compiled,
                plan,
                options,
            }))
        })
        .transpose()
}

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    read_schema: Option<TableReadSchema>,
    // Only the selection is transported. In-process splits share this compiled plan;
    // transported workers compile at scanner setup, then reuse it for every row.
    #[serde(skip)]
    projection: Option<ScanProjection>,
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
            read_schema: None,
            projection: None,
        };
        plan.validate()?;
        Ok(plan)
    }

    /// Return the fixed global snapshot selected for this scan.
    pub fn snapshot_id(&self) -> u64 {
        self.global_snapshot_id
    }

    /// Return the bucket count fixed by the global snapshot.
    pub fn total_buckets(&self) -> u32 {
        self.total_buckets
    }

    /// Return the schema fixed into this plan.
    pub fn schema(&self) -> &TableSchema {
        &self.metadata.schema
    }

    /// Select fields by stable identity relative to this plan's captured source schema.
    /// The selection applies to all workers; it does not evolve or replace the source schema.
    pub fn project(&self, read_schema: &TableReadSchema) -> Result<Self> {
        let projection = compile_projection(
            &self.name,
            &self.metadata,
            self.total_buckets,
            Some(read_schema),
        )?;
        let mut plan = self.clone();
        plan.read_schema = Some(read_schema.clone());
        plan.projection = projection;
        Ok(plan)
    }

    /// Return the selected fields in output order, or all source fields when unprojected.
    pub fn read_schema(&self) -> TableReadSchema {
        self.read_schema.clone().unwrap_or_else(|| TableReadSchema {
            fields: self.metadata.schema.fields.clone(),
        })
    }

    /// Return the total encoded data size reported by all shard snapshots.
    pub fn data_size_bytes(&self) -> u64 {
        self.shards.iter().fold(0_u64, |total, shard| {
            total.saturating_add(shard.data_size_bytes)
        })
    }

    /// Produce one independently serializable split per shard snapshot.
    pub fn splits(&self) -> Result<Vec<TableScanSplit>> {
        self.validate()?;
        let projection = match &self.projection {
            Some(projection) => Some(Arc::clone(projection)),
            None => compile_projection(
                &self.name,
                &self.metadata,
                self.total_buckets,
                self.read_schema.as_ref(),
            )?,
        };
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
                read_schema: self.read_schema.clone(),
                projection: projection.clone(),
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

/// A serializable table scan assignment for one shard snapshot, optionally key-bounded.
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    read_schema: Option<TableReadSchema>,
    // Never deserialize compiled indexes: worker-local scanner setup validates and
    // compiles the selection once, and row decoding reuses the resulting plan.
    #[serde(skip)]
    projection: Option<ScanProjection>,
}

impl TableScanSplit {
    /// Select fields relative to the captured source schema for this worker assignment.
    pub fn project(&self, read_schema: &TableReadSchema) -> Result<Self> {
        let projection = compile_projection(
            &self.name,
            &self.metadata,
            self.total_buckets,
            Some(read_schema),
        )?;
        let mut split = self.clone();
        split.read_schema = Some(read_schema.clone());
        split.projection = projection;
        Ok(split)
    }

    /// Return selected fields in output order, or all source fields when unprojected.
    pub fn read_schema(&self) -> TableReadSchema {
        self.read_schema.clone().unwrap_or_else(|| TableReadSchema {
            fields: self.metadata.schema.fields.clone(),
        })
    }

    /// Return the global snapshot that assigned this shard.
    pub fn snapshot_id(&self) -> u64 {
        self.global_snapshot_id
    }

    /// Return this split's shard snapshot identity.
    pub fn shard_snapshot_id(&self) -> u64 {
        self.split.shard.snapshot_id
    }

    /// Open this shard and decode the selected fields for every assigned row.
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
        let projection = match &self.projection {
            Some(projection) => Some(Arc::clone(projection)),
            None => compile_projection(
                &self.name,
                &self.metadata,
                self.total_buckets,
                self.read_schema.as_ref(),
            )?,
        };
        let compiled = match &projection {
            Some(projection) => Arc::clone(&projection.compiled),
            None => compile_table(self.metadata.clone(), self.total_buckets)?,
        };
        let scanner = self.create_raw_scanner_with_transforms(
            runtime,
            transforms,
            projection
                .as_ref()
                .map(|projection| projection.options.clone())
                .unwrap_or_else(|| ScanOptions::default().with_column_family(self.name.clone())),
        )?;
        Ok(TableScanSplitScanner {
            inner: scanner,
            compiled,
            projection: projection.map(|projection| Arc::clone(&projection.plan)),
            read_schema: self.read_schema(),
        })
    }

    fn create_raw_scanner_with_transforms(
        &self,
        runtime: Config,
        transforms: &TableSchemaTransformFactories,
        scan_options: ScanOptions,
    ) -> Result<ScanSplitScanner> {
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

        let builder = transforms.apply_to(
            ReadOnlyDbBuilder::new(config)
                .merge_operator_resolver(crate::table_merge_operator_resolver(None)),
        )?;
        let scanner = self
            .split
            .create_scanner_with_builder(builder, &scan_options)?;
        Ok(scanner)
    }

    #[cfg(feature = "ffi")]
    pub(crate) fn create_projected_raw_scanner(
        &self,
        runtime: Config,
        field_names: &[String],
        read_ahead_bytes: i64,
    ) -> Result<ScanSplitScanner> {
        let compiled = compile_table(self.metadata.clone(), self.total_buckets)?;
        let mut options = build_scan_options_for_fields(&self.name, compiled, field_names)?;
        options.read_ahead_bytes = size::Size::from_const(read_ahead_bytes);
        self.create_raw_scanner_with_transforms(
            runtime,
            &TableSchemaTransformFactories::default(),
            options,
        )
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
        for (bucket, key, label) in [
            (
                self.split.start_bucket,
                self.split.start_key_exclusive.as_ref(),
                "start",
            ),
            (
                self.split.end_bucket,
                self.split.end_key_inclusive.as_ref(),
                "end",
            ),
        ] {
            if bucket.is_some() != key.is_some() {
                return Err(TableError::InvalidSchema(format!(
                    "table scan split {label} bucket and key must be set together"
                )));
            }
            if let Some(bucket) = bucket
                && (u32::from(bucket) >= self.total_buckets
                    || !self
                        .split
                        .shard
                        .ranges
                        .iter()
                        .any(|range| bucket >= *range.start() && bucket <= *range.end()))
            {
                return Err(TableError::InvalidSchema(format!(
                    "table scan split {label} bucket is outside its shard"
                )));
            }
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
    /// Select fields before opening this worker-local scanner.
    pub fn project(mut self, read_schema: &TableReadSchema) -> Result<Self> {
        self.split = self.split.project(read_schema)?;
        Ok(self)
    }
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
    projection: Option<Arc<ProjectionPlan>>,
    read_schema: TableReadSchema,
}

impl TableScanSplitScanner {
    /// Return the fields decoded by this scanner, in output order.
    pub fn read_schema(&self) -> &TableReadSchema {
        &self.read_schema
    }
}

impl Iterator for TableScanSplitScanner {
    type Item = Result<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|row| {
            let (_, key, columns) = row?;
            match &self.projection {
                Some(projection) => {
                    decode_projected_scan_row(&self.compiled, projection, &key, &columns)
                }
                None => decode_table_scan_row(&self.compiled, &key, &columns),
            }
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
