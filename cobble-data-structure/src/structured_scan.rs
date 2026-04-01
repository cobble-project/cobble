use crate::structured_db::{
    StructuredColumnValue, StructuredSchema, combined_resolver, decode_row,
    load_structured_schema_from_cobble_schema, project_structured_schema_for_scan,
};
use bytes::Bytes;
use cobble::{
    Config, GlobalSnapshotManifest, MergeOperatorResolver, ReadOnlyDb, Result, ScanOptions,
    ScanPlan, ScanSplit, ScanSplitScanner, ShardSnapshotRef,
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
}

impl From<ScanSplit> for StructuredScanSplit {
    fn from(value: ScanSplit) -> Self {
        Self {
            shard: value.shard,
            start: value.start,
            end: value.end,
        }
    }
}

impl From<StructuredScanSplit> for ScanSplit {
    fn from(value: StructuredScanSplit) -> Self {
        Self {
            shard: value.shard,
            start: value.start,
            end: value.end,
        }
    }
}

impl StructuredScanSplit {
    pub fn create_scanner(
        &self,
        config: Config,
        options: &ScanOptions,
    ) -> Result<StructuredScanSplitScanner> {
        self.create_scanner_internal(config, None, options)
    }

    pub fn create_scanner_with_resolver(
        &self,
        config: Config,
        resolver: Arc<dyn MergeOperatorResolver>,
        options: &ScanOptions,
    ) -> Result<StructuredScanSplitScanner> {
        self.create_scanner_internal(config, Some(resolver), options)
    }

    fn create_scanner_internal(
        &self,
        config: Config,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
        options: &ScanOptions,
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
        let projected_schema = project_structured_schema_for_scan(&structured_schema, options);
        let scanner = ScanSplit::from(self.clone())
            .create_scanner_with_resolver(config, resolver, options)?;
        Ok(StructuredScanSplitScanner {
            inner: scanner,
            structured_schema: projected_schema,
        })
    }
}

pub struct StructuredScanSplitScanner {
    inner: ScanSplitScanner,
    structured_schema: Arc<StructuredSchema>,
}

impl Iterator for StructuredScanSplitScanner {
    type Item = Result<(Bytes, Vec<Option<StructuredColumnValue>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| {
            let (key, columns) = item?;
            let decoded = decode_row(&self.structured_schema, 0, columns)?;
            Ok((key, decoded))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::list::{ListConfig, ListRetainMode};
    use crate::{StructuredColumnType, StructuredDb, StructuredSchema};
    use cobble::{
        CoordinatorConfig, DbCoordinator, ScanOptions, ShardSnapshotInput, VolumeDescriptor,
        VolumeUsageKind,
    };
    use std::collections::BTreeMap;

    fn cleanup_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn test_schema() -> StructuredSchema {
        StructuredSchema {
            columns: BTreeMap::from([(
                1,
                StructuredColumnType::List(ListConfig {
                    max_elements: Some(8),
                    retain_mode: ListRetainMode::Last,
                    preserve_element_ttl: false,
                }),
            )]),
        }
    }

    fn test_config(root: &str) -> Config {
        Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
            num_columns: 2,
            total_buckets: 4,
            ..Config::default()
        }
    }

    fn write_and_snapshot(
        config: &Config,
        structured_schema: StructuredSchema,
        writes: impl FnOnce(&StructuredDb),
    ) -> (StructuredDb, ShardSnapshotInput) {
        let db = StructuredDb::open(config.clone(), vec![0u16..=3u16], structured_schema).unwrap();
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
            .create_scanner(config, &ScanOptions::default())
            .unwrap();
        let results: Vec<_> = scanner.map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0.as_ref(), b"k1");
        assert_eq!(
            results[0].1[1],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]))
        );
        assert_eq!(results[1].0.as_ref(), b"k2");

        cleanup_root(root);
    }

    #[test]
    fn test_structured_scan_split_serialization() {
        let split = StructuredScanSplit {
            shard: ShardSnapshotRef {
                ranges: vec![0u16..=3u16],
                db_id: "test-db".to_string(),
                snapshot_id: 7,
                manifest_path: "test-db/snapshot/SNAPSHOT-7".to_string(),
                timestamp_seconds: 42,
            },
            start: Some(b"a".to_vec()),
            end: Some(b"z".to_vec()),
        };
        let json = serde_json::to_string(&split).unwrap();
        let decoded: StructuredScanSplit = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.shard.db_id, "test-db");
        assert_eq!(decoded.shard.snapshot_id, 7);
        assert_eq!(decoded.start, Some(b"a".to_vec()));
        assert_eq!(decoded.end, Some(b"z".to_vec()));
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
            .create_scanner(config, &ScanOptions::for_column(1))
            .unwrap();
        let rows: Vec<_> = scanner.map(|r| r.unwrap()).collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.as_ref(), b"k1");
        assert_eq!(rows[0].1.len(), 1);
        assert_eq!(
            rows[0].1[0],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]))
        );

        cleanup_root(root);
    }
}
