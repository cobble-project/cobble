use crate::structured_db::{
    StructuredColumnFamilySchema, StructuredColumnValue, StructuredScanOptions, combined_resolver,
    decode_row, load_structured_schema_from_cobble_schema,
};
use bytes::Bytes;
use cobble::{
    Config, GlobalSnapshotManifest, MergeOperatorResolver, ReadOnlyDb, Result, ScanPlan, ScanSplit,
    ScanSplitScanner, ShardSnapshotRef,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Structured distributed scan plan.
///
/// Wraps `cobble::ScanPlan` and produces structured scan splits/scanners.
pub struct StructuredScanPlan {
    inner: ScanPlan,
}

impl StructuredScanPlan {
    pub fn new(manifest: GlobalSnapshotManifest) -> Self {
        Self {
            inner: ScanPlan::new(manifest),
        }
    }

    pub fn with_start(mut self, start: Vec<u8>) -> Self {
        self.inner = self.inner.with_start(start);
        self
    }

    pub fn with_end(mut self, end: Vec<u8>) -> Self {
        self.inner = self.inner.with_end(end);
        self
    }

    pub fn splits(&self) -> Vec<StructuredScanSplit> {
        self.inner.splits().into_iter().map(Into::into).collect()
    }
}

/// Structured version of a distributed scan split.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StructuredScanSplit {
    pub shard: ShardSnapshotRef,
    pub start: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
    pub start_bucket: Option<u16>,
    pub start_key_exclusive: Option<Vec<u8>>,
    pub end_bucket: Option<u16>,
    pub end_key_inclusive: Option<Vec<u8>>,
}

pub struct StructuredScanSplitPartition {
    pub before: StructuredScanSplit,
    pub after: StructuredScanSplit,
}

impl From<ScanSplit> for StructuredScanSplit {
    fn from(value: ScanSplit) -> Self {
        Self {
            shard: value.shard,
            start: value.start,
            end: value.end,
            start_bucket: value.start_bucket,
            start_key_exclusive: value.start_key_exclusive,
            end_bucket: value.end_bucket,
            end_key_inclusive: value.end_key_inclusive,
        }
    }
}

impl From<StructuredScanSplit> for ScanSplit {
    fn from(value: StructuredScanSplit) -> Self {
        Self {
            shard: value.shard,
            start: value.start,
            end: value.end,
            start_bucket: value.start_bucket,
            start_key_exclusive: value.start_key_exclusive,
            end_bucket: value.end_bucket,
            end_key_inclusive: value.end_key_inclusive,
        }
    }
}

impl StructuredScanSplit {
    pub fn split_after(
        &self,
        bucket: u16,
        key_inclusive: Vec<u8>,
    ) -> Result<StructuredScanSplitPartition> {
        let partition = ScanSplit::from(self.clone()).split_after(bucket, key_inclusive)?;
        Ok(StructuredScanSplitPartition {
            before: partition.before.into(),
            after: partition.after.into(),
        })
    }

    pub fn create_scanner_without_options(
        &self,
        config: Config,
    ) -> Result<StructuredScanSplitScanner> {
        self.create_scanner_without_options_internal(config, None)
    }

    pub fn create_scanner(
        &self,
        config: Config,
        options: &StructuredScanOptions,
    ) -> Result<StructuredScanSplitScanner> {
        self.create_scanner_internal(config, None, options)
    }

    pub fn create_scanner_with_resolver_without_options(
        &self,
        config: Config,
        resolver: Arc<dyn MergeOperatorResolver>,
    ) -> Result<StructuredScanSplitScanner> {
        self.create_scanner_without_options_internal(config, Some(resolver))
    }

    pub fn create_scanner_with_resolver(
        &self,
        config: Config,
        resolver: Arc<dyn MergeOperatorResolver>,
        options: &StructuredScanOptions,
    ) -> Result<StructuredScanSplitScanner> {
        self.create_scanner_internal(config, Some(resolver), options)
    }

    fn create_scanner_internal(
        &self,
        config: Config,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
        options: &StructuredScanOptions,
    ) -> Result<StructuredScanSplitScanner> {
        let resolver = combined_resolver(resolver);
        let read_only = ReadOnlyDb::open_with_db_id_and_resolver(
            config.clone(),
            self.shard.snapshot_id,
            self.shard.db_id.clone(),
            Arc::clone(&resolver),
        )?;
        let structured_schema = Arc::new(load_structured_schema_from_cobble_schema(
            &read_only.current_schema(),
        )?);
        let projected_schema = options.resolve_projected_schema_cached(&structured_schema)?;
        let scanner = ScanSplit::from(self.clone()).create_scanner_with_resolver(
            config,
            resolver,
            options.as_cobble(),
        )?;
        Ok(StructuredScanSplitScanner {
            inner: scanner,
            structured_schema: projected_schema,
        })
    }

    fn create_scanner_without_options_internal(
        &self,
        config: Config,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<StructuredScanSplitScanner> {
        let resolver = combined_resolver(resolver);
        let read_only = ReadOnlyDb::open_with_db_id_and_resolver(
            config.clone(),
            self.shard.snapshot_id,
            self.shard.db_id.clone(),
            Arc::clone(&resolver),
        )?;
        let structured_schema = Arc::new(load_structured_schema_from_cobble_schema(
            &read_only.current_schema(),
        )?);
        let projected_schema = Arc::new(structured_schema.projected(0, None));
        let scanner = ScanSplit::from(self.clone())
            .create_scanner_with_resolver_without_options(config, resolver)?;
        Ok(StructuredScanSplitScanner {
            inner: scanner,
            structured_schema: projected_schema,
        })
    }
}

pub struct StructuredScanSplitScanner {
    inner: ScanSplitScanner,
    structured_schema: Arc<StructuredColumnFamilySchema>,
}

impl StructuredScanSplitScanner {
    pub fn consume_next_row<T, F>(&mut self, mut consumer: F) -> Result<Option<T>>
    where
        F: FnMut(&Bytes, &[Option<StructuredColumnValue>]) -> Result<T>,
    {
        let structured_schema = Arc::clone(&self.structured_schema);
        self.inner.consume_next_row(|key, columns| {
            let decoded = decode_row(&structured_schema, 0, columns.to_vec())?;
            consumer(key, &decoded)
        })
    }

    pub fn consume_next_row_with_bucket<T, F>(&mut self, mut consumer: F) -> Result<Option<T>>
    where
        F: FnMut(u16, &Bytes, &[Option<StructuredColumnValue>]) -> Result<T>,
    {
        let structured_schema = Arc::clone(&self.structured_schema);
        self.inner
            .consume_next_row_with_bucket(|bucket, key, columns| {
                let decoded = decode_row(&structured_schema, 0, columns.to_vec())?;
                consumer(bucket, key, &decoded)
            })
    }
}

impl Iterator for StructuredScanSplitScanner {
    type Item = Result<(u16, Bytes, Vec<Option<StructuredColumnValue>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| {
            let (bucket, key, columns) = item?;
            let decoded = decode_row(&self.structured_schema, 0, columns)?;
            Ok((bucket, key, decoded))
        })
    }
}

#[cfg(test)]
#[path = "../tests/unit/structured_scan.rs"]
mod tests;
