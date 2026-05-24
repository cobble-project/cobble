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
mod tests {
    use super::*;
    use crate::config::VolumeDescriptor;
    use crate::coordinator::{CoordinatorConfig, DbCoordinator};
    use crate::{Db, ScanOptions, WriteBatch, WriteOptions};
    use std::collections::BTreeMap;

    fn cleanup_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn full_bucket_range() -> Vec<std::ops::RangeInclusive<u16>> {
        vec![0u16..=3u16]
    }

    /// Write data, snapshot with callback, retain snapshot, return (Db, shard_input).
    /// Caller must close the Db when done.
    fn write_and_snapshot(
        config: &Config,
        writes: impl FnOnce(&Db),
    ) -> (Db, crate::coordinator::ShardSnapshotInput) {
        let db = Db::open(config.clone(), full_bucket_range()).unwrap();
        writes(&db);
        let (tx, rx) = std::sync::mpsc::channel();
        db.snapshot_with_callback(move |result| {
            let _ = tx.send(result);
        })
        .unwrap();
        let shard_input = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("snapshot callback timed out")
            .unwrap();
        db.retain_snapshot(shard_input.snapshot_id);
        (db, shard_input)
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_scan_plan_basic() {
        let root = "/tmp/cobble_scan_plan_basic";
        cleanup_root(root);

        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
            num_columns: 1,
            total_buckets: 4,
            ..Config::default()
        };
        let (_db, shard_input) = write_and_snapshot(&config, |db| {
            let mut batch = WriteBatch::new();
            batch.put(0, b"key1", 0, b"val1");
            batch.put(0, b"key2", 0, b"val2");
            batch.put(0, b"key3", 0, b"val3");
            db.write_batch(batch).unwrap();
        });

        let coordinator = DbCoordinator::open(CoordinatorConfig {
            volumes: vec![crate::config::VolumeDescriptor::new(
                format!("file://{}/coordinator", root),
                vec![
                    crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                    crate::config::VolumeUsageKind::Meta,
                ],
            )],
            snapshot_retention: None,
        })
        .unwrap();
        let global = coordinator
            .take_global_snapshot(4, vec![shard_input])
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();

        // Create scan plan and splits.
        let plan = ScanPlan::new(global);
        let splits = plan.splits();
        assert_eq!(splits.len(), 1);

        // Create scanner from split.
        let scanner = splits[0]
            .create_scanner(config, &ScanOptions::default())
            .unwrap();
        let results: Vec<_> = scanner.map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1.as_ref(), b"key1");
        assert_eq!(results[1].1.as_ref(), b"key2");
        assert_eq!(results[2].1.as_ref(), b"key3");

        cleanup_root(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_scan_plan_with_range() {
        let root = "/tmp/cobble_scan_plan_range";
        cleanup_root(root);

        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
            num_columns: 1,
            total_buckets: 4,
            ..Config::default()
        };
        let (_db, shard_input) = write_and_snapshot(&config, |db| {
            let mut batch = WriteBatch::new();
            batch.put(0, b"aaa", 0, b"v1");
            batch.put(0, b"bbb", 0, b"v2");
            batch.put(0, b"ccc", 0, b"v3");
            batch.put(0, b"ddd", 0, b"v4");
            db.write_batch(batch).unwrap();
        });
        let coordinator = DbCoordinator::open(CoordinatorConfig {
            volumes: vec![crate::config::VolumeDescriptor::new(
                format!("file://{}/coordinator", root),
                vec![
                    crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                    crate::config::VolumeUsageKind::Meta,
                ],
            )],
            snapshot_retention: None,
        })
        .unwrap();
        let global = coordinator
            .take_global_snapshot(4, vec![shard_input])
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();

        // Scan with key range [bbb, ddd).
        let plan = ScanPlan::new(global)
            .with_start(b"bbb".to_vec())
            .with_end(b"ddd".to_vec());
        let splits = plan.splits();
        let scanner = splits[0]
            .create_scanner(config, &ScanOptions::default())
            .unwrap();
        let results: Vec<_> = scanner.map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1.as_ref(), b"bbb");
        assert_eq!(results[1].1.as_ref(), b"ccc");

        cleanup_root(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_scan_split_scanner_consume_next_row() {
        let root = "/tmp/cobble_scan_plan_consume_next_row";
        cleanup_root(root);

        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
            num_columns: 2,
            total_buckets: 4,
            ..Config::default()
        };
        let (_db, shard_input) = write_and_snapshot(&config, |db| {
            let mut batch = WriteBatch::new();
            batch.put(0, b"key1", 0, b"v10");
            batch.put(0, b"key1", 1, b"v11");
            batch.put(0, b"key2", 0, b"v20");
            db.write_batch(batch).unwrap();
        });

        let coordinator = DbCoordinator::open(CoordinatorConfig {
            volumes: vec![crate::config::VolumeDescriptor::new(
                format!("file://{}/coordinator", root),
                vec![
                    crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                    crate::config::VolumeUsageKind::Meta,
                ],
            )],
            snapshot_retention: None,
        })
        .unwrap();
        let global = coordinator
            .take_global_snapshot(4, vec![shard_input])
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();

        let mut scanner = ScanPlan::new(global)
            .splits()
            .remove(0)
            .create_scanner(config, &ScanOptions::default())
            .unwrap();
        let mut rows = Vec::new();
        while let Some(row) = scanner
            .consume_next_row(|key, columns| Ok((key.clone(), columns.to_vec())))
            .unwrap()
        {
            rows.push(row);
        }

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0.as_ref(), b"key1");
        assert_eq!(rows[0].1[0].as_deref(), Some(b"v10".as_slice()));
        assert_eq!(rows[0].1[1].as_deref(), Some(b"v11".as_slice()));
        assert_eq!(rows[1].0.as_ref(), b"key2");
        assert_eq!(rows[1].1[0].as_deref(), Some(b"v20".as_slice()));
        assert_eq!(rows[1].1[1].as_deref(), None);

        cleanup_root(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_scan_split_scanner_resume_after_bucket_key() {
        let root = "/tmp/cobble_scan_plan_resume_after_bucket_key";
        cleanup_root(root);

        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
            num_columns: 1,
            total_buckets: 4,
            ..Config::default()
        };
        let (_db, shard_input) = write_and_snapshot(&config, |db| {
            let mut batch = WriteBatch::new();
            batch.put(0, b"z0", 0, b"v0");
            batch.put(1, b"z1", 0, b"v1");
            batch.put(2, b"m2a", 0, b"v2a");
            batch.put(2, b"m2b", 0, b"v2b");
            batch.put(3, b"a3", 0, b"v3");
            db.write_batch(batch).unwrap();
        });

        let coordinator = DbCoordinator::open(CoordinatorConfig {
            volumes: vec![crate::config::VolumeDescriptor::new(
                format!("file://{}/coordinator", root),
                vec![
                    crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                    crate::config::VolumeUsageKind::Meta,
                ],
            )],
            snapshot_retention: None,
        })
        .unwrap();
        let global = coordinator
            .take_global_snapshot(4, vec![shard_input])
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();

        let scanner = ScanPlan::new(global)
            .splits()
            .remove(0)
            .split_after(2, b"m2a".to_vec())
            .unwrap()
            .after
            .create_scanner(config, &ScanOptions::default())
            .unwrap();
        let results: Vec<_> = scanner.map(|r| r.unwrap()).collect();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1.as_ref(), b"m2b");
        assert_eq!(results[1].1.as_ref(), b"a3");

        cleanup_root(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_scan_plan_column_projection() {
        let root = "/tmp/cobble_scan_plan_col_proj";
        cleanup_root(root);

        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
            num_columns: 3,
            total_buckets: 4,
            ..Config::default()
        };
        let (_db, shard_input) = write_and_snapshot(&config, |db| {
            let mut batch = WriteBatch::new();
            batch.put(0, b"key1", 0, b"a0");
            batch.put(0, b"key1", 1, b"a1");
            batch.put(0, b"key1", 2, b"a2");
            db.write_batch(batch).unwrap();
        });
        let coordinator = DbCoordinator::open(CoordinatorConfig {
            volumes: vec![crate::config::VolumeDescriptor::new(
                format!("file://{}/coordinator", root),
                vec![
                    crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                    crate::config::VolumeUsageKind::Meta,
                ],
            )],
            snapshot_retention: None,
        })
        .unwrap();
        let global = coordinator
            .take_global_snapshot(4, vec![shard_input])
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();

        // Scan with column projection: only column 1.
        let opts = ScanOptions::for_column(1);
        let plan = ScanPlan::new(global);
        let splits = plan.splits();
        let scanner = splits[0].create_scanner(config, &opts).unwrap();
        let results: Vec<_> = scanner.map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 1);
        // Column projection returns only the selected columns (compact array).
        assert_eq!(results[0].2.len(), 1);
        assert_eq!(results[0].2[0].as_deref(), Some(b"a1".as_slice()));

        cleanup_root(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_scan_plan_column_family_projection() {
        let root = "/tmp/cobble_scan_plan_cf_proj";
        cleanup_root(root);

        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
            num_columns: 1,
            total_buckets: 4,
            ..Config::default()
        };
        let (_db, shard_input) = write_and_snapshot(&config, |db| {
            let mut schema = db.update_schema();
            schema
                .add_column(0, None, None, Some("metrics".to_string()))
                .unwrap();
            let latest_schema = schema.commit();
            assert_eq!(latest_schema.column_family_ids().get("metrics"), Some(&1));

            db.put(0, b"key1", 0, b"default").unwrap();
            db.put_with_options(
                0,
                b"key1",
                0,
                b"metrics-1",
                &WriteOptions::with_column_family("metrics"),
            )
            .unwrap();
            db.put_with_options(
                0,
                b"key2",
                0,
                b"metrics-2",
                &WriteOptions::with_column_family("metrics"),
            )
            .unwrap();
        });
        assert_eq!(shard_input.column_family_ids.get("metrics"), Some(&1));

        let coordinator = DbCoordinator::open(CoordinatorConfig {
            volumes: vec![crate::config::VolumeDescriptor::new(
                format!("file://{}/coordinator", root),
                vec![
                    crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                    crate::config::VolumeUsageKind::Meta,
                ],
            )],
            snapshot_retention: None,
        })
        .unwrap();
        let global = coordinator
            .take_global_snapshot(4, vec![shard_input])
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();

        let plan = ScanPlan::new(global);
        let splits = plan.splits();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].shard.column_family_ids.get("metrics"), Some(&1));

        let opts = ScanOptions::for_column(0).with_column_family("metrics");
        let scanner = splits[0].create_scanner(config, &opts).unwrap();
        let results: Vec<_> = scanner.map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1.as_ref(), b"key1");
        assert_eq!(results[0].2[0].as_deref(), Some(b"metrics-1".as_slice()));
        assert_eq!(results[1].1.as_ref(), b"key2");
        assert_eq!(results[1].2[0].as_deref(), Some(b"metrics-2".as_slice()));

        cleanup_root(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_scan_plan_without_end_includes_ff_prefixed_keys() {
        let root = "/tmp/cobble_scan_plan_no_end";
        cleanup_root(root);

        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
            num_columns: 1,
            total_buckets: 4,
            ..Config::default()
        };
        let (_db, shard_input) = write_and_snapshot(&config, |db| {
            let mut batch = WriteBatch::new();
            batch.put(0, b"\xff", 0, b"v1");
            batch.put(0, b"\xff\x01", 0, b"v2");
            batch.put(0, b"\xff\xff", 0, b"v3");
            db.write_batch(batch).unwrap();
        });

        let coordinator = DbCoordinator::open(CoordinatorConfig {
            volumes: vec![crate::config::VolumeDescriptor::new(
                format!("file://{}/coordinator", root),
                vec![
                    crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                    crate::config::VolumeUsageKind::Meta,
                ],
            )],
            snapshot_retention: None,
        })
        .unwrap();
        let global = coordinator
            .take_global_snapshot(4, vec![shard_input])
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();

        let plan = ScanPlan::new(global);
        let splits = plan.splits();
        let scanner = splits[0]
            .create_scanner(config, &ScanOptions::default())
            .unwrap();
        let results: Vec<_> = scanner.map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1.as_ref(), b"\xff");
        assert_eq!(results[1].1.as_ref(), b"\xff\x01");
        assert_eq!(results[2].1.as_ref(), b"\xff\xff");

        cleanup_root(root);
    }

    #[test]
    fn test_scan_split_serialization() {
        let split = ScanSplit {
            shard: ShardSnapshotRef {
                ranges: vec![0u16..=3u16],
                column_family_ids: BTreeMap::from([("default".to_string(), 0)]),
                db_id: "test-db".to_string(),
                snapshot_id: 42,
                manifest_path: "file:///tmp/manifest".to_string(),
                timestamp_seconds: 100,
                data_size_bytes: 1234,
                incremental_data_size_bytes: 567,
            },
            start: Some(b"start".to_vec()),
            end: Some(b"end".to_vec()),
            start_bucket: Some(2),
            start_key_exclusive: Some(b"resume".to_vec()),
            end_bucket: Some(3),
            end_key_inclusive: Some(b"tail".to_vec()),
        };

        let json = serde_json::to_string(&split).unwrap();
        let deserialized: ScanSplit = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.shard.db_id, "test-db");
        assert_eq!(deserialized.shard.snapshot_id, 42);
        assert_eq!(deserialized.shard.timestamp_seconds, 100);
        assert_eq!(deserialized.start, Some(b"start".to_vec()));
        assert_eq!(deserialized.end, Some(b"end".to_vec()));
        assert_eq!(deserialized.start_bucket, Some(2));
        assert_eq!(deserialized.start_key_exclusive, Some(b"resume".to_vec()));
        assert_eq!(deserialized.end_bucket, Some(3));
        assert_eq!(deserialized.end_key_inclusive, Some(b"tail".to_vec()));
    }
}
