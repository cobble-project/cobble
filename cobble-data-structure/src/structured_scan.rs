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
mod tests {
    use super::*;
    use crate::list::{ListConfig, ListRetainMode};
    use crate::{
        StructuredColumnType, StructuredDb, StructuredScanOptions, StructuredSchema,
        StructuredWriteOptions,
    };
    use cobble::{
        CoordinatorConfig, DbCoordinator, ShardSnapshotInput, VolumeDescriptor, VolumeUsageKind,
    };
    use std::collections::BTreeMap;

    fn cleanup_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn test_schema() -> StructuredSchema {
        StructuredSchema {
            column_families: BTreeMap::from([(
                0,
                crate::StructuredColumnFamilySchema {
                    columns: BTreeMap::from([(
                        1,
                        StructuredColumnType::List(ListConfig {
                            max_elements: Some(8),
                            retain_mode: ListRetainMode::Last,
                            preserve_element_ttl: false,
                        }),
                    )]),
                },
            )]),
            ..Default::default()
        }
    }

    fn test_config(root: &str) -> Config {
        Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
            num_columns: 1,
            total_buckets: 4,
            ..Config::default()
        }
    }

    fn write_and_snapshot(
        config: &Config,
        structured_schema: StructuredSchema,
        writes: impl FnOnce(&StructuredDb),
    ) -> (StructuredDb, ShardSnapshotInput) {
        let mut db = StructuredDb::open(config.clone(), vec![0u16..=3u16]).unwrap();
        db.update_schema()
            .add_list_column(
                None,
                1,
                match structured_schema
                    .column_families
                    .get(&0)
                    .and_then(|family| family.columns.get(&1))
                {
                    Some(StructuredColumnType::List(cfg)) => cfg.clone(),
                    _ => panic!("test schema missing list column 1"),
                },
            )
            .commit()
            .unwrap();
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
    fn test_structured_scan_split_scanner() {
        let root = "/tmp/structured_scan_split_scanner";
        cleanup_root(root);

        let config = test_config(root);
        let (_db, shard_input) = write_and_snapshot(&config, test_schema(), |db| {
            db.put(0, b"k1", 0, b"v1".to_vec()).unwrap();
            db.merge(0, b"k1", 1, vec![b"a".to_vec()]).unwrap();
            db.merge(0, b"k1", 1, vec![b"b".to_vec()]).unwrap();
            db.put(0, b"k2", 0, b"v2".to_vec()).unwrap();
        });

        let coordinator = DbCoordinator::open(CoordinatorConfig {
            volumes: vec![VolumeDescriptor::new(
                format!("file://{}/coordinator", root),
                vec![
                    VolumeUsageKind::PrimaryDataPriorityHigh,
                    VolumeUsageKind::Meta,
                ],
            )],
            snapshot_retention: None,
        })
        .unwrap();
        let global = coordinator
            .take_global_snapshot(4, vec![shard_input])
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();

        let plan = StructuredScanPlan::new(global);
        let splits = plan.splits();
        assert_eq!(splits.len(), 1);

        let scanner = splits[0]
            .create_scanner(config, &StructuredScanOptions::default())
            .unwrap();
        let results: Vec<_> = scanner.map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 0);
        assert_eq!(results[0].1.as_ref(), b"k1");
        assert_eq!(
            results[0].2[1],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]))
        );
        assert_eq!(results[1].0, 0);
        assert_eq!(results[1].1.as_ref(), b"k2");

        cleanup_root(root);
    }

    #[test]
    fn test_structured_scan_split_scanner_consume_next_row() {
        let root = "/tmp/structured_scan_split_scanner_consume";
        cleanup_root(root);

        let config = test_config(root);
        let (_db, shard_input) = write_and_snapshot(&config, test_schema(), |db| {
            db.put(0, b"k1", 0, b"v1".to_vec()).unwrap();
            db.merge(0, b"k1", 1, vec![b"a".to_vec()]).unwrap();
            db.merge(0, b"k1", 1, vec![b"b".to_vec()]).unwrap();
            db.put(0, b"k2", 0, b"v2".to_vec()).unwrap();
        });

        let coordinator = DbCoordinator::open(CoordinatorConfig {
            volumes: vec![VolumeDescriptor::new(
                format!("file://{}/coordinator", root),
                vec![
                    VolumeUsageKind::PrimaryDataPriorityHigh,
                    VolumeUsageKind::Meta,
                ],
            )],
            snapshot_retention: None,
        })
        .unwrap();
        let global = coordinator
            .take_global_snapshot(4, vec![shard_input])
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();

        let mut scanner = StructuredScanPlan::new(global)
            .splits()
            .remove(0)
            .create_scanner(config, &StructuredScanOptions::default())
            .unwrap();
        let mut rows = Vec::new();
        while let Some(row) = scanner
            .consume_next_row_with_bucket(|bucket, key, columns| {
                Ok((bucket, key.clone(), columns.to_vec()))
            })
            .unwrap()
        {
            rows.push(row);
        }

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 0);
        assert_eq!(rows[0].1.as_ref(), b"k1");
        assert_eq!(
            rows[0].2[1],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]))
        );
        assert_eq!(rows[1].0, 0);
        assert_eq!(rows[1].1.as_ref(), b"k2");
        assert_eq!(
            rows[1].2[0],
            Some(StructuredColumnValue::Bytes(Bytes::from_static(b"v2")))
        );

        cleanup_root(root);
    }

    #[test]
    fn test_structured_scan_split_scanner_column_family() {
        let root = "/tmp/structured_scan_split_scanner_cf";
        cleanup_root(root);

        let config = test_config(root);
        let mut db = StructuredDb::open(config.clone(), vec![0u16..=3u16]).unwrap();
        db.update_schema()
            .add_list_column(Some("metrics".to_string()), 0, ListConfig::default())
            .commit()
            .unwrap();
        let metrics_write = StructuredWriteOptions::with_column_family("metrics");
        db.put_with_options(0, b"k1", 0, vec![Bytes::from_static(b"a")], &metrics_write)
            .unwrap();
        db.merge_with_options(0, b"k1", 0, vec![Bytes::from_static(b"b")], &metrics_write)
            .unwrap();
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

        let coordinator = DbCoordinator::open(CoordinatorConfig {
            volumes: vec![VolumeDescriptor::new(
                format!("file://{}/coordinator", root),
                vec![
                    VolumeUsageKind::PrimaryDataPriorityHigh,
                    VolumeUsageKind::Meta,
                ],
            )],
            snapshot_retention: None,
        })
        .unwrap();
        let global = coordinator
            .take_global_snapshot(4, vec![shard_input])
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();

        let split = StructuredScanPlan::new(global).splits().remove(0);
        let scanner = split
            .create_scanner(
                config,
                &StructuredScanOptions::for_column(0).with_column_family("metrics"),
            )
            .unwrap();
        let rows: Vec<_> = scanner.map(|r| r.unwrap()).collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 0);
        assert_eq!(rows[0].1.as_ref(), b"k1");
        assert_eq!(
            rows[0].2[0],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]))
        );

        cleanup_root(root);
    }

    #[test]
    fn test_structured_scan_split_serialization() {
        let split = StructuredScanSplit {
            shard: ShardSnapshotRef {
                ranges: vec![0u16..=3u16],
                column_family_ids: BTreeMap::from([("default".to_string(), 0)]),
                db_id: "test-db".to_string(),
                snapshot_id: 7,
                manifest_path: "test-db/snapshot/SNAPSHOT-7".to_string(),
                timestamp_seconds: 42,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            },
            start: Some(b"a".to_vec()),
            end: Some(b"z".to_vec()),
            start_bucket: Some(1),
            start_key_exclusive: Some(b"m".to_vec()),
            end_bucket: Some(3),
            end_key_inclusive: Some(b"t".to_vec()),
        };
        let json = serde_json::to_string(&split).unwrap();
        let decoded: StructuredScanSplit = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.shard.db_id, "test-db");
        assert_eq!(decoded.shard.snapshot_id, 7);
        assert_eq!(decoded.start, Some(b"a".to_vec()));
        assert_eq!(decoded.end, Some(b"z".to_vec()));
        assert_eq!(decoded.start_bucket, Some(1));
        assert_eq!(decoded.start_key_exclusive, Some(b"m".to_vec()));
        assert_eq!(decoded.end_bucket, Some(3));
        assert_eq!(decoded.end_key_inclusive, Some(b"t".to_vec()));
    }

    #[test]
    fn test_structured_scan_split_partition_preserves_bucket_boundaries() {
        let split = StructuredScanSplit {
            shard: ShardSnapshotRef {
                ranges: vec![2u16..=4u16],
                column_family_ids: BTreeMap::from([("default".to_string(), 0)]),
                db_id: "test-db".to_string(),
                snapshot_id: 11,
                manifest_path: "test-db/snapshot/SNAPSHOT-11".to_string(),
                timestamp_seconds: 0,
                data_size_bytes: 0,
                incremental_data_size_bytes: 0,
            },
            start: Some(b"a".to_vec()),
            end: Some(b"z".to_vec()),
            start_bucket: None,
            start_key_exclusive: None,
            end_bucket: None,
            end_key_inclusive: None,
        };

        let partition = split.split_after(3, b"resume".to_vec()).unwrap();
        assert_eq!(partition.before.end_bucket, Some(3));
        assert_eq!(partition.before.end_key_inclusive, Some(b"resume".to_vec()));
        assert_eq!(partition.after.start_bucket, Some(3));
        assert_eq!(
            partition.after.start_key_exclusive,
            Some(b"resume".to_vec())
        );
    }

    #[test]
    fn test_structured_scan_projection_reindexes_schema() {
        let root = "/tmp/structured_scan_projection_reindex";
        cleanup_root(root);

        let config = test_config(root);
        let (_db, shard_input) = write_and_snapshot(&config, test_schema(), |db| {
            db.put(0, b"k1", 0, b"v1".to_vec()).unwrap();
            db.merge(0, b"k1", 1, vec![b"a".to_vec()]).unwrap();
            db.merge(0, b"k1", 1, vec![b"b".to_vec()]).unwrap();
        });

        let coordinator = DbCoordinator::open(CoordinatorConfig {
            volumes: vec![VolumeDescriptor::new(
                format!("file://{}/coordinator", root),
                vec![
                    VolumeUsageKind::PrimaryDataPriorityHigh,
                    VolumeUsageKind::Meta,
                ],
            )],
            snapshot_retention: None,
        })
        .unwrap();
        let global = coordinator
            .take_global_snapshot(4, vec![shard_input])
            .unwrap();
        coordinator.materialize_global_snapshot(&global).unwrap();

        let split = StructuredScanPlan::new(global).splits().remove(0);
        let scanner = split
            .create_scanner(config, &StructuredScanOptions::for_column(1))
            .unwrap();
        let rows: Vec<_> = scanner.map(|r| r.unwrap()).collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 0);
        assert_eq!(rows[0].1.as_ref(), b"k1");
        assert_eq!(rows[0].2.len(), 1);
        assert_eq!(
            rows[0].2[0],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]))
        );

        cleanup_root(root);
    }
}
