//! Distributed scan support.
//!
//! Provides a three-step flow:
//! 1. Create a [`ScanPlan`] from a [`GlobalSnapshotManifest`].
//! 2. Generate [`ScanSplit`]s from the plan — each split targets one shard.
//! 3. On each node, call [`ScanSplit::create_scanner`] to produce a
//!    [`ScanSplitScanner`] that iterates over key-value pairs.

use crate::config::{Config, ScanOptions};
use crate::coordinator::{GlobalSnapshotManifest, ShardSnapshotRef};
use crate::db_iter::{BucketedRow, DbIterator};
use crate::error::Result;
use crate::merge_operator::MergeOperatorResolver;
use crate::read_only_db::ReadOnlyDb;
use crate::sst::row_codec::encode_key;
use crate::r#type::Key;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Scan plan generated from a global snapshot.
///
/// A plan holds the snapshot manifest and scan parameters.
/// Call [`splits`](ScanPlan::splits) to generate distributable splits.
pub struct ScanPlan {
    manifest: GlobalSnapshotManifest,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
}

impl ScanPlan {
    /// Create a plan from a global snapshot manifest.
    pub fn new(manifest: GlobalSnapshotManifest) -> Self {
        Self {
            manifest,
            start: None,
            end: None,
        }
    }

    /// Restrict the scan start key (inclusive).
    pub fn with_start(mut self, start: Vec<u8>) -> Self {
        self.start = Some(start);
        self
    }

    /// Restrict the scan end key (exclusive).
    pub fn with_end(mut self, end: Vec<u8>) -> Self {
        self.end = Some(end);
        self
    }

    /// Generate one [`ScanSplit`] per shard snapshot.
    pub fn splits(&self) -> Vec<ScanSplit> {
        self.manifest
            .shard_snapshots
            .iter()
            .map(|shard| ScanSplit {
                shard: shard.clone(),
                start: self.start.clone(),
                end: self.end.clone(),
                start_bucket: None,
                start_key_exclusive: None,
                end_bucket: None,
                end_key_inclusive: None,
            })
            .collect()
    }
}

/// A serializable scan split that can be distributed to remote nodes.
///
/// Each split covers exactly one shard and its bucket ranges.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScanSplit {
    /// The shard snapshot to scan.
    pub shard: ShardSnapshotRef,
    /// Optional scan start key (inclusive).
    pub start: Option<Vec<u8>>,
    /// Optional scan end key (exclusive).
    pub end: Option<Vec<u8>>,
    /// Optional split-local start bucket.
    pub start_bucket: Option<u16>,
    /// Optional split-local start key skipped once in `start_bucket`.
    pub start_key_exclusive: Option<Vec<u8>>,
    /// Optional split-local end bucket.
    pub end_bucket: Option<u16>,
    /// Optional split-local end key included once in `end_bucket`.
    pub end_key_inclusive: Option<Vec<u8>>,
}

/// A pair of scan splits partitioned around one bucket/key boundary.
pub struct ScanSplitPartition {
    pub before: ScanSplit,
    pub after: ScanSplit,
}

impl ScanSplit {
    /// Splits this scan into the rows up to `key_inclusive` and the rows after it.
    pub fn split_after(&self, bucket: u16, key_inclusive: Vec<u8>) -> Result<ScanSplitPartition> {
        let (first_bucket, last_bucket) = self.single_range_bucket_bounds()?;
        if bucket < first_bucket || bucket > last_bucket {
            return Err(crate::Error::InputError(format!(
                "split boundary bucket {bucket} is outside [{first_bucket}, {last_bucket}]"
            )));
        }
        let mut before = self.clone();
        before.end_bucket = Some(bucket);
        before.end_key_inclusive = Some(key_inclusive.clone());
        let mut after = self.clone();
        after.start_bucket = Some(bucket);
        after.start_key_exclusive = Some(key_inclusive);
        Ok(ScanSplitPartition { before, after })
    }

    fn single_range_bucket_bounds(&self) -> Result<(u16, u16)> {
        if self.shard.ranges.len() != 1 {
            return Err(crate::Error::InputError(
                "scan split bucket partitioning requires exactly one shard range".to_string(),
            ));
        }
        let range = &self.shard.ranges[0];
        Ok((*range.start(), *range.end()))
    }

    /// Create a scanner with the given config and scan options.
    pub fn create_scanner(
        &self,
        config: Config,
        options: &ScanOptions,
    ) -> Result<ScanSplitScanner> {
        self.create_scanner_internal(config, None, Some(options))
    }

    /// Create a scanner with default scan behavior (no explicit scan options).
    pub fn create_scanner_without_options(&self, config: Config) -> Result<ScanSplitScanner> {
        self.create_scanner_internal(config, None, None)
    }

    /// Create a scanner with a merge operator resolver.
    pub fn create_scanner_with_resolver(
        &self,
        config: Config,
        resolver: Arc<dyn MergeOperatorResolver>,
        options: &ScanOptions,
    ) -> Result<ScanSplitScanner> {
        self.create_scanner_internal(config, Some(resolver), Some(options))
    }

    /// Create a scanner with a merge operator resolver and default scan behavior.
    pub fn create_scanner_with_resolver_without_options(
        &self,
        config: Config,
        resolver: Arc<dyn MergeOperatorResolver>,
    ) -> Result<ScanSplitScanner> {
        self.create_scanner_internal(config, Some(resolver), None)
    }

    fn create_scanner_internal(
        &self,
        config: Config,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
        options: Option<&ScanOptions>,
    ) -> Result<ScanSplitScanner> {
        let db = match resolver {
            Some(resolver) => ReadOnlyDb::open_with_db_id_and_resolver(
                config,
                self.shard.snapshot_id,
                self.shard.db_id.clone(),
                resolver,
            )?,
            _ => ReadOnlyDb::open_with_db_id(
                config,
                self.shard.snapshot_id,
                self.shard.db_id.clone(),
            )?,
        };
        let buckets: Vec<u16> = self
            .shard
            .ranges
            .iter()
            .flat_map(|range| *range.start()..=*range.end())
            .collect();
        ScanSplitScanner::new(
            db,
            buckets,
            ScanSplitScannerBounds {
                start: self.start.clone(),
                end: self.end.clone(),
                start_bucket: self.start_bucket,
                start_key_exclusive: self.start_key_exclusive.clone(),
                end_bucket: self.end_bucket,
                end_key_inclusive: self.end_key_inclusive.clone(),
            },
            options.cloned(),
        )
    }
}

/// Scanner that iterates over all key-value pairs in a scan split.
///
/// Iterates bucket by bucket through the shard's bucket ranges,
/// producing `(bucket, key, columns)` rows in key order within each bucket.
pub struct ScanSplitScanner {
    db: ReadOnlyDb,
    buckets: Vec<u16>,
    current_bucket_index: usize,
    current_iter: Option<DbIterator<'static>>,
    bounds: ScanSplitScannerBounds,
    scan_options: Option<ScanOptions>,
    skip_start_key_once: bool,
}

struct ScanSplitScannerBounds {
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    start_bucket: Option<u16>,
    start_key_exclusive: Option<Vec<u8>>,
    end_bucket: Option<u16>,
    end_key_inclusive: Option<Vec<u8>>,
}

impl ScanSplitScanner {
    fn new(
        db: ReadOnlyDb,
        buckets: Vec<u16>,
        bounds: ScanSplitScannerBounds,
        scan_options: Option<ScanOptions>,
    ) -> Result<Self> {
        let current_bucket_index = match bounds.start_bucket {
            Some(bucket) => buckets
                .iter()
                .position(|candidate| *candidate >= bucket)
                .unwrap_or(buckets.len()),
            None => 0,
        };
        let mut scanner = Self {
            db,
            buckets,
            current_bucket_index,
            current_iter: None,
            bounds,
            scan_options,
            skip_start_key_once: false,
        };
        scanner.advance_to_next_bucket()?;
        Ok(scanner)
    }

    fn advance_to_next_bucket(&mut self) -> Result<()> {
        if self.current_bucket_index < self.buckets.len() {
            let bucket = self.buckets[self.current_bucket_index];
            if self
                .bounds
                .end_bucket
                .is_some_and(|end_bucket| bucket > end_bucket)
            {
                self.current_iter = None;
                return Ok(());
            }
            let start = self.bounds.start.as_deref();
            let end = self.bounds.end.as_deref();
            let mut seek_target = None;
            self.skip_start_key_once = false;
            if self.should_apply_start_to_bucket(bucket)
                && let Some(start_key_exclusive) = self.bounds.start_key_exclusive.as_deref()
                && start.is_none_or(|current_start| current_start <= start_key_exclusive)
            {
                seek_target = Some(encode_key(&Key::new(bucket, start_key_exclusive.to_vec())));
                self.skip_start_key_once = true;
            }
            let mut iter = match self.scan_options.as_ref() {
                Some(scan_options) => {
                    self.db
                        .scan_with_options_bounds(bucket, start, end, scan_options)?
                }
                None => self.db.scan_bounds(bucket, start, end)?,
            };
            if let Some(target) = seek_target.as_deref() {
                iter.seek(target)?;
            }
            self.current_iter = Some(iter);
        } else {
            self.current_iter = None;
        }
        Ok(())
    }

    fn should_apply_start_to_bucket(&self, bucket: u16) -> bool {
        matches!(self.bounds.start_bucket, Some(start_bucket) if start_bucket == bucket)
            && self.bounds.start_key_exclusive.is_some()
    }

    fn should_skip_start_key(&mut self, bucket: u16, key: &[u8]) -> bool {
        if !self.skip_start_key_once {
            return false;
        }
        if self.bounds.start_bucket != Some(bucket) {
            self.skip_start_key_once = false;
            return false;
        }
        match self.bounds.start_key_exclusive.as_deref() {
            Some(start_key) if key <= start_key => true,
            _ => {
                self.skip_start_key_once = false;
                false
            }
        }
    }

    fn advance_bucket_index_to(&mut self, bucket: u16) -> bool {
        while self.current_bucket_index < self.buckets.len()
            && self.buckets[self.current_bucket_index] < bucket
        {
            self.current_bucket_index += 1;
        }
        self.current_bucket_index < self.buckets.len()
            && self.buckets[self.current_bucket_index] == bucket
    }

    fn is_past_split_end(&self, bucket: u16, key: &[u8]) -> bool {
        matches!(
            (self.bounds.end_bucket, self.bounds.end_key_inclusive.as_deref()),
            (Some(end_bucket), Some(end_key))
                if bucket > end_bucket || (bucket == end_bucket && key > end_key)
        )
    }

    fn next_row(&mut self) -> Result<Option<BucketedRow>> {
        loop {
            if let Some(iter) = &mut self.current_iter {
                if let Some((bucket, key, columns)) = iter.next_row_with_bucket()? {
                    if !self.advance_bucket_index_to(bucket) {
                        self.current_iter = None;
                        return Ok(None);
                    }
                    if self.should_skip_start_key(bucket, key.as_ref()) {
                        continue;
                    }
                    if self.is_past_split_end(bucket, key.as_ref()) {
                        self.current_iter = None;
                        self.current_bucket_index = self.buckets.len();
                        return Ok(None);
                    }
                    return Ok(Some((bucket, key, columns)));
                }
                // Current bucket exhausted, move to next.
                self.current_bucket_index += 1;
                self.current_iter = None;
                self.advance_to_next_bucket()?;
            } else {
                return Ok(None);
            }
        }
    }

    pub fn consume_next_row<T, F>(&mut self, mut consumer: F) -> Result<Option<T>>
    where
        F: FnMut(&Bytes, &[Option<Bytes>]) -> Result<T>,
    {
        let Some((_bucket, key, columns)) = self.next_row()? else {
            return Ok(None);
        };
        consumer(&key, &columns).map(Some)
    }

    pub fn consume_next_row_with_bucket<T, F>(&mut self, mut consumer: F) -> Result<Option<T>>
    where
        F: FnMut(u16, &Bytes, &[Option<Bytes>]) -> Result<T>,
    {
        let Some((bucket, key, columns)) = self.next_row()? else {
            return Ok(None);
        };
        consumer(bucket, &key, &columns).map(Some)
    }
}

impl Iterator for ScanSplitScanner {
    type Item = Result<(u16, Bytes, Vec<Option<Bytes>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_row().transpose()
    }
}

#[cfg(test)]
#[path = "../tests/unit/scan.rs"]
mod tests;
