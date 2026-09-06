use crate::structured_db::{
    StructuredColumnValue, StructuredDbIterator, StructuredReadOptions, StructuredScanOptions,
    StructuredSchema, combined_resolver, decode_row, load_structured_schema_from_cobble_schema,
};
use bytes::Bytes;
use cobble::{
    Config, GlobalSnapshotManifest, GlobalSnapshotSummary, MergeOperatorResolver,
    ReadOnlyDbBuilder, Reader, ReaderBuilder, ReaderConfig, Result, VolumeDescriptor,
};
use std::ops::Range;
use std::sync::Arc;

type SchemaTransformCallback = Arc<dyn Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync>;

/// Builder for a structured snapshot reader with runtime schema wiring.
pub struct StructuredReaderBuilder {
    inner: ReaderBuilder,
    volumes: Vec<VolumeDescriptor>,
    resolver: Option<Arc<dyn MergeOperatorResolver>>,
    callbacks: Vec<(String, SchemaTransformCallback)>,
}

impl StructuredReaderBuilder {
    pub fn new(config: ReaderConfig) -> Self {
        Self {
            volumes: config.volumes.clone(),
            inner: ReaderBuilder::new(config).merge_operator_resolver(combined_resolver(None)),
            resolver: None,
            callbacks: Vec::new(),
        }
    }

    pub fn merge_operator_resolver(mut self, resolver: Arc<dyn MergeOperatorResolver>) -> Self {
        self.resolver = Some(resolver);
        self.inner = self
            .inner
            .merge_operator_resolver(combined_resolver(self.resolver.clone()));
        self
    }

    /// Register a raw single-column transform before opening a snapshot reader.
    pub fn register_schema_transform<F>(
        mut self,
        transform_id: impl Into<String>,
        transform: F,
    ) -> Result<Self>
    where
        F: Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync + 'static,
    {
        let transform_id = transform_id.into();
        let callback: SchemaTransformCallback = Arc::new(transform);
        self.inner = self
            .inner
            .register_schema_transform(transform_id.clone(), {
                let callback = Arc::clone(&callback);
                move |value| callback(value)
            })?;
        self.callbacks.push((transform_id, callback));
        Ok(self)
    }

    pub fn open(self, global_snapshot_id: u64) -> Result<StructuredReader> {
        self.open_inner(Some(global_snapshot_id))
    }

    pub fn open_current(self) -> Result<StructuredReader> {
        self.open_inner(None)
    }

    fn open_inner(self, global_snapshot_id: Option<u64>) -> Result<StructuredReader> {
        let reader = match global_snapshot_id {
            Some(snapshot_id) => self.inner.open(snapshot_id)?,
            None => self.inner.open_current()?,
        };
        StructuredReader::from_reader(reader, self.volumes, self.resolver, self.callbacks)
    }
}

pub struct StructuredReader {
    reader: Reader,
    structured_schema: Arc<StructuredSchema>,
    schema_snapshot_id: u64,
    volumes: Vec<VolumeDescriptor>,
    resolver: Option<Arc<dyn MergeOperatorResolver>>,
    callbacks: Vec<(String, SchemaTransformCallback)>,
    default_read_options: StructuredReadOptions,
    default_scan_options: StructuredScanOptions,
}

impl StructuredReader {
    pub fn open(read_config: ReaderConfig, global_snapshot_id: u64) -> Result<Self> {
        StructuredReaderBuilder::new(read_config).open(global_snapshot_id)
    }

    pub fn open_current(read_config: ReaderConfig) -> Result<Self> {
        StructuredReaderBuilder::new(read_config).open_current()
    }

    fn from_reader(
        reader: Reader,
        volumes: Vec<VolumeDescriptor>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
        callbacks: Vec<(String, SchemaTransformCallback)>,
    ) -> Result<Self> {
        let structured_schema =
            load_schema_from_reader(&reader, &volumes, resolver.as_ref(), &callbacks)?;
        let schema_snapshot_id = reader.current_global_snapshot().id;
        Ok(Self {
            reader,
            structured_schema: Arc::new(structured_schema),
            schema_snapshot_id,
            volumes,
            resolver,
            callbacks,
            default_read_options: StructuredReadOptions::default(),
            default_scan_options: StructuredScanOptions::default(),
        })
    }

    pub fn current_schema(&self) -> StructuredSchema {
        self.structured_schema.as_ref().clone()
    }

    // ── Read operations ─────────────────────────────────────────────────

    pub fn get(
        &mut self,
        bucket_id: u16,
        key: &[u8],
    ) -> Result<Option<Vec<Option<StructuredColumnValue>>>> {
        let default_options = self.default_read_options.clone();
        self.get_with_options(bucket_id, key, &default_options)
    }

    pub fn multi_get<K: AsRef<[u8]>>(
        &mut self,
        keys: &[(u16, K)],
    ) -> Result<Vec<Option<Vec<Option<StructuredColumnValue>>>>> {
        let default_options = self.default_read_options.clone();
        self.multi_get_with_options(keys, &default_options)
    }

    pub fn multi_get_with_options<K: AsRef<[u8]>>(
        &mut self,
        keys: &[(u16, K)],
        options: &StructuredReadOptions,
    ) -> Result<Vec<Option<Vec<Option<StructuredColumnValue>>>>> {
        let raw_keys = keys
            .iter()
            .map(|(bucket, key)| (*bucket, key.as_ref()))
            .collect::<Vec<_>>();
        let raw = self
            .reader
            .multi_get_with_options(&raw_keys, options.as_cobble())?;
        self.refresh_structured_schema_if_changed()?;
        let projected_schema = options.resolve_projected_schema_cached(&self.structured_schema)?;
        raw.into_iter()
            .map(|raw| {
                raw.map(|columns| decode_row(&projected_schema, 0, columns))
                    .transpose()
            })
            .collect()
    }

    pub fn get_with_options(
        &mut self,
        bucket_id: u16,
        key: &[u8],
        options: &StructuredReadOptions,
    ) -> Result<Option<Vec<Option<StructuredColumnValue>>>> {
        let raw = self
            .reader
            .get_with_options(bucket_id, key, options.as_cobble())?;
        self.refresh_structured_schema_if_changed()?;
        let projected_schema = options.resolve_projected_schema_cached(&self.structured_schema)?;
        raw.map(|columns| decode_row(&projected_schema, 0, columns))
            .transpose()
    }

    pub fn scan(&mut self, bucket_id: u16, range: Range<&[u8]>) -> Result<StructuredDbIterator> {
        let default_options = self.default_scan_options.clone();
        self.scan_with_options(bucket_id, range, &default_options)
    }

    pub fn scan_with_options(
        &mut self,
        bucket_id: u16,
        range: Range<&[u8]>,
        options: &StructuredScanOptions,
    ) -> Result<StructuredDbIterator> {
        let inner = self
            .reader
            .scan_with_options(bucket_id, range, options.as_cobble())?;
        self.refresh_structured_schema_if_changed()?;
        let projected_schema = options.resolve_projected_schema_cached(&self.structured_schema)?;
        Ok(StructuredDbIterator::new(inner, projected_schema, 0))
    }

    // ── Snapshot management ─────────────────────────────────────────────

    pub fn refresh(&mut self) -> Result<()> {
        self.reader.refresh()?;
        self.refresh_structured_schema_if_changed()
    }

    /// Register a raw single-column transform for future lazy shard opens.
    pub fn register_schema_transform<F>(
        &mut self,
        transform_id: impl Into<String>,
        transform: F,
    ) -> Result<()>
    where
        F: Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync + 'static,
    {
        let transform_id = transform_id.into();
        let callback: SchemaTransformCallback = Arc::new(transform);
        self.reader
            .register_schema_transform(transform_id.clone(), {
                let callback = Arc::clone(&callback);
                move |value| callback(value)
            })?;
        self.callbacks.push((transform_id, callback));
        Ok(())
    }

    pub fn read_mode(&self) -> &'static str {
        self.reader.read_mode()
    }

    pub fn configured_snapshot_id(&self) -> Option<u64> {
        self.reader.configured_snapshot_id()
    }

    pub fn current_global_snapshot(&self) -> &GlobalSnapshotManifest {
        self.reader.current_global_snapshot()
    }

    pub fn list_global_snapshots(&self) -> Result<Vec<GlobalSnapshotSummary>> {
        self.reader.list_global_snapshots()
    }

    pub fn list_global_snapshot_manifests(&self) -> Result<Vec<GlobalSnapshotManifest>> {
        self.reader.list_global_snapshot_manifests()
    }

    fn refresh_structured_schema_if_changed(&mut self) -> Result<()> {
        let snapshot_id = self.reader.current_global_snapshot().id;
        if snapshot_id == self.schema_snapshot_id {
            return Ok(());
        }
        let schema = load_schema_from_reader(
            &self.reader,
            &self.volumes,
            self.resolver.as_ref(),
            &self.callbacks,
        )?;
        self.structured_schema = Arc::new(schema);
        self.schema_snapshot_id = snapshot_id;
        Ok(())
    }
}

/// Load the structured schema from the first shard of the reader's current global snapshot.
fn load_schema_from_reader(
    reader: &Reader,
    volumes: &[VolumeDescriptor],
    resolver: Option<&Arc<dyn cobble::MergeOperatorResolver>>,
    callbacks: &[(String, SchemaTransformCallback)],
) -> Result<StructuredSchema> {
    let manifest = reader.current_global_snapshot();
    let shard = manifest.shard_snapshots.first().ok_or_else(|| {
        cobble::Error::ConfigError("global snapshot has no shard snapshots".to_string())
    })?;
    let config = Config {
        volumes: volumes.to_vec(),
        total_buckets: manifest.total_buckets,
        ..cobble::Config::default()
    };
    let mut builder = ReadOnlyDbBuilder::new(config)
        .db_id(shard.db_id.clone())
        .merge_operator_resolver(combined_resolver(resolver.cloned()));
    for (transform_id, callback) in callbacks {
        let transform_id = transform_id.clone();
        let callback = Arc::clone(callback);
        builder = builder.register_schema_transform(transform_id, move |value| callback(value))?;
    }
    let read_only = builder.open(shard.snapshot_id)?;
    load_structured_schema_from_cobble_schema(&read_only.current_schema())
}

#[cfg(test)]
#[path = "../tests/unit/structured_reader.rs"]
mod tests;
