use crate::structured_db::{
    StructuredColumnValue, StructuredDbIterator, StructuredReadOptions, StructuredScanOptions,
    StructuredSchema, combined_resolver, decode_row, load_structured_schema_from_cobble_schema,
};
use cobble::{
    GlobalSnapshotManifest, GlobalSnapshotSummary, ReadOnlyDb, Reader, ReaderConfig, Result,
    VolumeDescriptor,
};
use std::ops::Range;
use std::sync::Arc;

pub struct StructuredReader {
    reader: Reader,
    structured_schema: Arc<StructuredSchema>,
    default_read_options: StructuredReadOptions,
    default_scan_options: StructuredScanOptions,
}

impl StructuredReader {
    pub fn open(read_config: ReaderConfig, global_snapshot_id: u64) -> Result<Self> {
        let volumes = read_config.volumes.clone();
        let resolver = combined_resolver(None);
        let reader = Reader::open_with_resolver(read_config, global_snapshot_id, Some(resolver))?;
        let structured_schema = load_schema_from_reader(&reader, &volumes)?;
        Ok(Self {
            reader,
            structured_schema: Arc::new(structured_schema),
            default_read_options: StructuredReadOptions::default(),
            default_scan_options: StructuredScanOptions::default(),
        })
    }

    pub fn open_current(read_config: ReaderConfig) -> Result<Self> {
        let volumes = read_config.volumes.clone();
        let resolver = combined_resolver(None);
        let reader = Reader::open_current_with_resolver(read_config, Some(resolver))?;
        let structured_schema = load_schema_from_reader(&reader, &volumes)?;
        Ok(Self {
            reader,
            structured_schema: Arc::new(structured_schema),
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
        let projected_schema = options.resolve_projected_schema_cached(&self.structured_schema)?;
        self.reader
            .multi_get_with_options(&raw_keys, options.as_cobble())?
            .into_iter()
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
        let projected_schema = options.resolve_projected_schema_cached(&self.structured_schema)?;
        raw.map(|columns| decode_row(&projected_schema, 0, columns))
            .transpose()
    }

    pub fn scan(
        &mut self,
        bucket_id: u16,
        range: Range<&[u8]>,
    ) -> Result<StructuredDbIterator<'static>> {
        let default_options = self.default_scan_options.clone();
        self.scan_with_options(bucket_id, range, &default_options)
    }

    pub fn scan_with_options(
        &mut self,
        bucket_id: u16,
        range: Range<&[u8]>,
        options: &StructuredScanOptions,
    ) -> Result<StructuredDbIterator<'static>> {
        let inner = self
            .reader
            .scan_with_options(bucket_id, range, options.as_cobble())?;
        let projected_schema = options.resolve_projected_schema_cached(&self.structured_schema)?;
        Ok(StructuredDbIterator::new(inner, projected_schema, 0))
    }

    // ── Snapshot management ─────────────────────────────────────────────

    pub fn refresh(&mut self) -> Result<()> {
        self.reader.refresh()
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
}

/// Load the structured schema from the first shard of the reader's current global snapshot.
fn load_schema_from_reader(
    reader: &Reader,
    volumes: &[VolumeDescriptor],
) -> Result<StructuredSchema> {
    let manifest = reader.current_global_snapshot();
    let shard = manifest.shard_snapshots.first().ok_or_else(|| {
        cobble::Error::ConfigError("global snapshot has no shard snapshots".to_string())
    })?;
    let config = cobble::Config {
        volumes: volumes.to_vec(),
        total_buckets: manifest.total_buckets,
        ..cobble::Config::default()
    };
    let read_only = ReadOnlyDb::open_with_db_id_and_resolver(
        config,
        shard.snapshot_id,
        shard.db_id.clone(),
        combined_resolver(None),
    )?;
    load_structured_schema_from_cobble_schema(&read_only.current_schema())
}

#[cfg(test)]
#[path = "../tests/unit/structured_reader.rs"]
mod tests;
