//! Functionality for expanding owned buckets by importing snapshot data from another db.
use super::Db;
use crate::data_file::intersect_bucket_ranges;
use crate::db_state::{DbState, MultiLSMTreeVersion, bucket_range_fits_total};
use crate::error::{Error, Result};
use crate::file::{File, FileManager, SequentialWriteFile};
use crate::lsm::LSMTree;
use crate::metrics_manager::MetricsManager;
use crate::paths::schema_file_relative_path;
use crate::snapshot::{
    build_tree_versions_from_manifest, build_vlog_version_from_manifest,
    list_snapshot_manifest_ids, load_manifest_entry,
};
use crate::util::{
    normalize_bucket_ranges, range_is_covered_by_ranges, ranges_overlap, subtract_range_by_cuts,
    subtract_ranges,
};
use std::collections::{BTreeSet, HashMap};
use std::ops::RangeInclusive;
use std::sync::Arc;

impl Db {
    /// Expands owned bucket ranges by importing LSM tree and VLOG state from a snapshot of another db, while
    /// keeping source files read-only and remapping file IDs to avoid collisions in local tracking. The source
    /// snapshot must have bucket ranges that are fully covered by the requested expand ranges, and the target db
    /// must not have any existing owned ranges that overlap with the requested expand ranges. The source snapshot's
    /// active memtable segments will be replayed into L0 of the target after metadata-level merging,
    /// with a file-level VLOG seq offset to avoid conflicts with existing VLOG files.
    /// This operation is irreversible and may take time to complete depending on the size of the
    /// imported snapshot, but the target db will be available for reads and writes on non-overlapping
    /// buckets during the process. Returns the snapshot ID of the source snapshot used for expansion.
    pub fn expand_bucket(
        &self,
        source_db_id: String,
        snapshot_id: Option<u64>,
        ranges: Option<Vec<RangeInclusive<u16>>>,
    ) -> Result<u64> {
        if source_db_id == self.id {
            return Err(Error::ConfigError(
                "cannot expand bucket from the same db".to_string(),
            ));
        }
        // Step 1: Build a read-only file manager for the source db and resolve snapshot.
        let source_metrics = Arc::new(MetricsManager::new(format!(
            "{}-expand-source",
            source_db_id
        )));
        let source_file_manager = Arc::new(FileManager::from_config(
            &self.config,
            &source_db_id,
            source_metrics,
        )?);
        let source_snapshot_id = match snapshot_id {
            Some(snapshot_id) => snapshot_id,
            None => {
                let snapshot_ids = list_snapshot_manifest_ids(&source_file_manager)?;
                snapshot_ids.last().copied().ok_or_else(|| {
                    Error::IoError(format!(
                        "No snapshot manifests found for db {}",
                        source_db_id
                    ))
                })?
            }
        };
        let source_entry =
            load_manifest_entry(&source_file_manager, source_snapshot_id, &HashMap::new())?;
        let mut source_manifest = source_entry.manifest;
        if source_manifest.bucket_ranges.is_empty() {
            return Err(Error::InvalidState(format!(
                "Snapshot {} manifest missing bucket_ranges",
                source_snapshot_id
            )));
        }
        let expand_ranges = ranges.unwrap_or_else(|| source_manifest.bucket_ranges.clone());
        if expand_ranges.is_empty() {
            return Err(Error::ConfigError(
                "expand ranges must not be empty".to_string(),
            ));
        }
        for range in &expand_ranges {
            if !bucket_range_fits_total(range, self.config.total_buckets) {
                return Err(Error::ConfigError(format!(
                    "Invalid expand range {}..={} for total_buckets {}",
                    range.start(),
                    range.end(),
                    self.config.total_buckets
                )));
            }
            if !range_is_covered_by_ranges(range, &source_manifest.bucket_ranges) {
                return Err(Error::ConfigError(format!(
                    "Expand range {}..={} is outside source snapshot bucket ranges",
                    range.start(),
                    range.end()
                )));
            }
        }
        let source_tree_ranges = if source_manifest.lsm_tree_bucket_ranges.is_empty() {
            source_manifest.bucket_ranges.clone()
        } else {
            source_manifest.lsm_tree_bucket_ranges.clone()
        };
        for range in &expand_ranges {
            if !range_is_covered_by_ranges(range, &source_tree_ranges) {
                return Err(Error::InvalidState(format!(
                    "Expand range {}..={} is not fully covered by source LSM trees",
                    range.start(),
                    range.end()
                )));
            }
        }

        // Step 2: Remap file IDs to avoid collisions in local tracking, while keeping source files read-only.
        let source_file_ids: BTreeSet<u64> = source_manifest
            .tree_levels
            .iter()
            .flat_map(|levels| levels.iter())
            .flat_map(|level| level.files.iter().map(|file| file.file_id))
            .chain(source_manifest.vlog_files.iter().map(|file| file.file_id))
            .collect();
        let remapped_ids = self
            .file_manager
            .reserve_data_file_ids(source_file_ids.len());
        let file_id_map: HashMap<u64, u64> =
            source_file_ids.iter().copied().zip(remapped_ids).collect();
        for levels in &mut source_manifest.tree_levels {
            for level in levels {
                for file in &mut level.files {
                    if let Some(mapped) = file_id_map.get(&file.file_id) {
                        file.file_id = *mapped;
                    }
                }
            }
        }
        for file in &mut source_manifest.vlog_files {
            if let Some(mapped) = file_id_map.get(&file.file_id) {
                file.file_id = *mapped;
            }
        }

        // Step 3: Ensure target has all schema files required by source snapshot.
        let current_schema = self.schema_manager.latest_schema();
        if source_manifest.latest_schema_id > current_schema.version() {
            return Err(Error::InvalidState(format!(
                "Source snapshot schema {} is newer than current schema {}",
                source_manifest.latest_schema_id,
                current_schema.version()
            )));
        }
        let mut required_schema_ids = BTreeSet::from([source_manifest.latest_schema_id]);
        for levels in &source_manifest.tree_levels {
            for level in levels {
                for file in &level.files {
                    if file.schema_id <= source_manifest.latest_schema_id {
                        for schema_id in file.schema_id..=source_manifest.latest_schema_id {
                            required_schema_ids.insert(schema_id);
                        }
                    } else {
                        required_schema_ids.insert(file.schema_id);
                    }
                }
            }
        }
        for schema_id in required_schema_ids {
            if self.schema_manager.schema(schema_id).is_ok() {
                continue;
            }
            let schema_path = schema_file_relative_path(schema_id);
            let reader = source_file_manager.open_metadata_file_reader_untracked(&schema_path)?;
            let bytes = reader.read_at(0, reader.size())?;
            let mut writer = self.file_manager.create_metadata_file(&schema_path)?;
            writer.write(bytes.as_ref())?;
            writer.close()?;
            self.schema_manager.register_schema_from_file(
                &self.file_manager,
                schema_id,
                self.config.num_columns,
            )?;
        }

        // Step 4: Validate ownership ranges and reserve a conflict-free VLOG seq window.
        let guard = self.db_state.lock();
        let current = self.db_state.load();
        for current_range in &current.bucket_ranges {
            if expand_ranges
                .iter()
                .any(|incoming| ranges_overlap(current_range, incoming))
            {
                return Err(Error::ConfigError(format!(
                    "Expand range overlaps existing owned range {}..={}",
                    current_range.start(),
                    current_range.end()
                )));
            }
        }
        let source_vlog_max_seq = source_manifest
            .vlog_files
            .iter()
            .map(|file| file.file_seq)
            .chain(
                source_manifest
                    .active_memtable_data
                    .iter()
                    .filter_map(|segment| segment.vlog_file_seq),
            )
            .max();
        let vlog_file_seq_offset = if let Some(max_seq) = source_vlog_max_seq {
            let span = max_seq
                .checked_add(1)
                .ok_or_else(|| Error::IoError("source vlog seq span overflow".to_string()))?;
            self.vlog_store.reserve_file_seq_span(span)
        } else {
            0
        };

        // Step 5: Shift source VLOG seqs at metadata level and apply per-file seq offset on imported SSTs.
        for file in &mut source_manifest.vlog_files {
            file.file_seq = file
                .file_seq
                .checked_add(vlog_file_seq_offset)
                .ok_or_else(|| {
                    Error::IoError(format!(
                        "VLOG file seq overflow: {} + {}",
                        file.file_seq, vlog_file_seq_offset
                    ))
                })?;
        }
        for levels in &mut source_manifest.tree_levels {
            for level in levels {
                for file in &mut level.files {
                    file.vlog_file_seq_offset = file
                        .vlog_file_seq_offset
                        .checked_add(vlog_file_seq_offset)
                        .ok_or_else(|| {
                            Error::IoError(format!(
                                "VLOG file seq offset overflow: {} + {}",
                                file.vlog_file_seq_offset, vlog_file_seq_offset
                            ))
                        })?;
                }
            }
        }

        // Step 6: Build imported tree/vlog versions as read-only source files.
        let source_tree_versions =
            build_tree_versions_from_manifest(&self.file_manager, source_manifest.clone(), true)?;
        if source_tree_versions.len() != source_tree_ranges.len() {
            return Err(Error::InvalidState(format!(
                "Source tree version count {} does not match range count {}",
                source_tree_versions.len(),
                source_tree_ranges.len()
            )));
        }
        let mut imported_ranges = Vec::new();
        let mut imported_versions = Vec::new();
        for expand_range in &expand_ranges {
            for (source_version, source_range) in
                source_tree_versions.iter().zip(source_tree_ranges.iter())
            {
                let Some(intersection) = intersect_bucket_ranges(expand_range, source_range) else {
                    continue;
                };
                imported_ranges.push(intersection.clone());
                imported_versions.push(LSMTree::clone_version_for_range(
                    source_version,
                    &intersection,
                ));
            }
        }
        if imported_ranges.is_empty() {
            return Err(Error::InvalidState(
                "No source LSM trees matched requested expand ranges".to_string(),
            ));
        }
        let source_vlog =
            build_vlog_version_from_manifest(&self.file_manager, &source_manifest, true)?;

        // Step 7: Merge source tree/vlog versions into target state.
        let mut merged_ranges = current.multi_lsm_version.bucket_ranges();
        let mut merged_versions = current.multi_lsm_version.tree_versions_cloned();
        merged_ranges.extend(imported_ranges);
        merged_versions.extend(imported_versions);
        let merged_multi_lsm = MultiLSMTreeVersion::from_bucket_ranges_with_tree_versions(
            current.multi_lsm_version.total_buckets(),
            &merged_ranges,
            merged_versions,
        )?;
        let mut merged_vlog_entries = current.vlog_version.files_with_entries();
        let mut existing_vlog_seqs: HashMap<u32, u64> = merged_vlog_entries
            .iter()
            .map(|(seq, tracked_id, _)| (*seq, tracked_id.file_id()))
            .collect();
        for (seq, tracked_id, valid_entries) in source_vlog.files_with_entries() {
            if let Some(existing_file_id) = existing_vlog_seqs.get(&seq) {
                if *existing_file_id != tracked_id.file_id() {
                    return Err(Error::InvalidState(format!(
                        "VLOG file seq {} conflict while taking over shard",
                        seq
                    )));
                }
                continue;
            }
            existing_vlog_seqs.insert(seq, tracked_id.file_id());
            merged_vlog_entries.push((seq, tracked_id, valid_entries));
        }
        let merged_vlog = crate::vlog::VlogVersion::from_files_with_entries(merged_vlog_entries);
        let mut updated_ranges = current.bucket_ranges.clone();
        updated_ranges.extend(expand_ranges);
        let updated_ranges = normalize_bucket_ranges(updated_ranges);
        self.db_state.store(DbState {
            seq_id: current.seq_id,
            bucket_ranges: updated_ranges.clone(),
            multi_lsm_version: merged_multi_lsm,
            vlog_version: merged_vlog,
            active: current.active.clone(),
            immutables: current.immutables.clone(),
            suggested_base_snapshot_id: None,
        });
        drop(guard);

        // Step 8: Replay source active memtable snapshot segments into L0 with a file-level VLOG seq offset.
        self.snapshot_manager.set_bucket_ranges(updated_ranges);
        self.restore_active_memtable_snapshot_to_l0_with_source(
            &source_file_manager,
            &source_manifest.active_memtable_data,
            vlog_file_seq_offset,
        )?;
        Ok(source_snapshot_id)
    }

    /// Shrinks owned bucket ranges by kicking out specified ranges and removing all data in those ranges,
    /// while keeping the db available for reads and writes on remaining owned buckets during the process.
    /// The requested shrink ranges must be fully covered by current owned ranges, and the resulting owned ranges after
    /// shrink must not be empty. This operation creates a snapshot to capture the state before shrink,
    /// which will be used as the base for future expand operations on the kicked-out ranges,
    /// and returns the snapshot ID of that snapshot.
    pub fn shrink_bucket(&self, ranges: Vec<RangeInclusive<u16>>) -> Result<u64> {
        if ranges.is_empty() {
            return Err(Error::ConfigError(
                "shrink ranges must not be empty".to_string(),
            ));
        }
        let shrink_ranges = normalize_bucket_ranges(ranges);
        for range in &shrink_ranges {
            if !bucket_range_fits_total(range, self.config.total_buckets) {
                return Err(Error::ConfigError(format!(
                    "Invalid shrink range {}..={} for total_buckets {}",
                    range.start(),
                    range.end(),
                    self.config.total_buckets
                )));
            }
        }

        let precheck = self.db_state.load();
        for range in &shrink_ranges {
            if !range_is_covered_by_ranges(range, &precheck.bucket_ranges) {
                return Err(Error::ConfigError(format!(
                    "Shrink range {}..={} is outside current owned ranges",
                    range.start(),
                    range.end()
                )));
            }
        }
        if subtract_ranges(&precheck.bucket_ranges, &shrink_ranges).is_empty() {
            return Err(Error::ConfigError(
                "cannot shrink all owned bucket ranges".to_string(),
            ));
        }

        let (tx, rx) = std::sync::mpsc::channel();
        let snapshot_id = self.snapshot_with_callback(move |result| {
            let _ = tx.send(result);
        })?;
        match rx.recv_timeout(std::time::Duration::from_secs(900)) {
            Ok(result) => result?,
            Err(_) => {
                return Err(Error::IoError(format!(
                    "Timed out waiting for snapshot {} before shrink",
                    snapshot_id
                )));
            }
        };

        let guard = self.db_state.lock();
        let current = self.db_state.load();
        for range in &shrink_ranges {
            if !range_is_covered_by_ranges(range, &current.bucket_ranges) {
                return Err(Error::ConfigError(format!(
                    "Shrink range {}..={} is outside current owned ranges",
                    range.start(),
                    range.end()
                )));
            }
        }
        let updated_ranges = subtract_ranges(&current.bucket_ranges, &shrink_ranges);
        if updated_ranges.is_empty() {
            return Err(Error::ConfigError(
                "cannot shrink all owned bucket ranges".to_string(),
            ));
        }
        let tree_ranges = current.multi_lsm_version.bucket_ranges();
        let tree_versions = current.multi_lsm_version.tree_versions_cloned();
        if tree_ranges.len() != tree_versions.len() {
            return Err(Error::InvalidState(format!(
                "LSM tree version count {} does not match range count {}",
                tree_versions.len(),
                tree_ranges.len()
            )));
        }
        let mut updated_tree_ranges = Vec::new();
        let mut updated_tree_versions = Vec::new();
        for (tree_range, tree_version) in tree_ranges.into_iter().zip(tree_versions.into_iter()) {
            for kept_range in subtract_range_by_cuts(&tree_range, &shrink_ranges) {
                updated_tree_ranges.push(kept_range.clone());
                if kept_range == tree_range {
                    updated_tree_versions.push(tree_version.clone());
                } else {
                    updated_tree_versions.push(LSMTree::clone_version_for_range(
                        tree_version.as_ref(),
                        &kept_range,
                    ));
                }
            }
        }
        if updated_tree_ranges.is_empty() {
            return Err(Error::ConfigError(
                "cannot shrink all LSM tree ranges".to_string(),
            ));
        }
        let updated_multi_lsm = MultiLSMTreeVersion::from_bucket_ranges_with_tree_versions(
            current.multi_lsm_version.total_buckets(),
            &updated_tree_ranges,
            updated_tree_versions,
        )?;
        self.db_state.store(DbState {
            seq_id: current.seq_id,
            bucket_ranges: updated_ranges.clone(),
            multi_lsm_version: updated_multi_lsm,
            vlog_version: current.vlog_version.clone(),
            active: current.active.clone(),
            immutables: current.immutables.clone(),
            suggested_base_snapshot_id: None,
        });
        drop(guard);
        self.snapshot_manager.set_bucket_ranges(updated_ranges);
        Ok(snapshot_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_state::full_bucket_range;
    use crate::file::FileManager;
    use crate::metrics_manager::MetricsManager;
    use crate::{Config, ReadOptions, VolumeDescriptor};
    use serial_test::serial;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;

    fn cleanup_test_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    #[serial(file)]
    fn test_expand_bucket_from_latest_snapshot() {
        let root = "/tmp/db_expand_bucket";
        cleanup_test_root(root);
        let mut config = Config {
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 1,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        config.total_buckets = 8;
        let source = Db::open(config.clone(), vec![2u16..=3u16]).unwrap();
        source.put(2, b"k1", 0, b"v1").unwrap();
        let (tx, rx) = mpsc::channel();
        let source_snapshot = source
            .snapshot_with_callback(move |result| {
                let _ = tx.send(result);
            })
            .unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap(),
            source_snapshot
        );

        let target = Db::open(config, vec![0u16..=1u16]).unwrap();
        let imported_snapshot = target
            .expand_bucket(source.id().to_string(), Some(source_snapshot), None)
            .unwrap();
        assert_eq!(imported_snapshot, source_snapshot);

        let value = target
            .get(2, b"k1", &ReadOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(value[0].as_deref(), Some(&b"v1"[..]));

        target.put(3, b"k2", 0, b"v2").unwrap();
        let value = target
            .get(3, b"k2", &ReadOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(value[0].as_deref(), Some(&b"v2"[..]));

        drop(target);
        drop(source);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_expand_bucket_outside_source_rejected() {
        let root = "/tmp/db_expand_bucket_outside_source";
        cleanup_test_root(root);
        let mut config = Config {
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 1,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        config.total_buckets = 8;
        let source = Db::open(config.clone(), vec![1u16..=2u16]).unwrap();
        source.put(1, b"k1", 0, b"v1").unwrap();
        let target = Db::open(config, vec![3u16..=4u16]).unwrap();
        let (tx, rx) = mpsc::channel();
        let snapshot_id = source
            .snapshot_with_callback(move |result| {
                let _ = tx.send(result);
            })
            .unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap(),
            snapshot_id
        );
        let err = target
            .expand_bucket(
                source.id().to_string(),
                Some(snapshot_id),
                Some(vec![0u16..=1u16]),
            )
            .unwrap_err();
        assert!(matches!(err, Error::ConfigError(_)));

        drop(target);
        drop(source);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_expand_bucket_accepts_full_range_with_empty_source() {
        let root = "/tmp/db_expand_bucket_empty_source";
        cleanup_test_root(root);
        let config = Config {
            total_buckets: 4,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let source = Db::open(config.clone(), vec![2u16..=3u16]).unwrap();
        let (tx, rx) = mpsc::channel();
        let snapshot_id = source
            .snapshot_with_callback(move |result| {
                let _ = tx.send(result);
            })
            .unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap(),
            snapshot_id
        );
        let target = Db::open(config, std::iter::once(full_bucket_range(2)).collect()).unwrap();
        target
            .expand_bucket(
                source.id().to_string(),
                Some(snapshot_id),
                Some(vec![2u16..=3u16]),
            )
            .unwrap();
        target.put(2, b"k", 0, b"v").unwrap();
        let got = target
            .get(2, b"k", &ReadOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(got[0].as_deref(), Some(&b"v"[..]));
        drop(target);
        drop(source);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_expand_bucket_restores_active_memtable_segments() {
        let root = "/tmp/db_expand_bucket_active_segments";
        cleanup_test_root(root);
        let config = Config {
            total_buckets: 8,
            memtable_capacity: 8 * 1024,
            memtable_buffer_count: 2,
            num_columns: 1,
            value_separation_threshold: 1,
            active_memtable_incremental_snapshot_ratio: 1.0,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let source = Db::open(config.clone(), vec![4u16..=5u16]).unwrap();
        source.put(4, b"k-sep", 0, b"payload-separated").unwrap();
        let (tx, rx) = mpsc::channel();
        let snapshot_id = source
            .snapshot_with_callback(move |result| {
                let _ = tx.send(result);
            })
            .unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap(),
            snapshot_id
        );
        let source_metrics = Arc::new(MetricsManager::new("rescale-source-manifest"));
        let source_file_manager =
            Arc::new(FileManager::from_config(&config, source.id(), source_metrics).unwrap());
        let source_manifest =
            crate::snapshot::load_manifest_for_snapshot(&source_file_manager, snapshot_id).unwrap();
        assert!(!source_manifest.active_memtable_data.is_empty());

        let target = Db::open(config, vec![0u16..=1u16]).unwrap();
        target
            .expand_bucket(source.id().to_string(), Some(snapshot_id), None)
            .unwrap();
        let got = target
            .get(4, b"k-sep", &ReadOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(got[0].as_deref(), Some(&b"payload-separated"[..]));

        drop(target);
        drop(source);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_shrink_bucket_removes_data_from_kicked_range() {
        let root = "/tmp/db_shrink_bucket";
        cleanup_test_root(root);
        let config = Config {
            total_buckets: 8,
            memtable_capacity: 128,
            memtable_buffer_count: 2,
            num_columns: 1,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let db = Db::open(config.clone(), vec![0u16..=3u16]).unwrap();
        db.put(1, b"k1", 0, b"v1").unwrap();
        db.put(2, b"k2", 0, b"v2").unwrap();

        let shrink_snapshot = db.shrink_bucket(vec![2u16..=3u16]).unwrap();
        let bucket_input = db.bucket_snapshot_input(shrink_snapshot).unwrap();
        assert_eq!(bucket_input.ranges, vec![0u16..=1u16]);

        let kept = db.get(1, b"k1", &ReadOptions::default()).unwrap().unwrap();
        assert_eq!(kept[0].as_deref(), Some(&b"v1"[..]));
        let removed = db.get(2, b"k2", &ReadOptions::default()).unwrap();
        assert!(removed.is_none());

        let metrics = Arc::new(MetricsManager::new("shrink-manifest"));
        let file_manager = Arc::new(FileManager::from_config(&config, db.id(), metrics).unwrap());
        let manifest =
            crate::snapshot::load_manifest_for_snapshot(&file_manager, shrink_snapshot).unwrap();
        assert_eq!(manifest.bucket_ranges, vec![0u16..=3u16]);

        drop(db);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_shrink_bucket_rejects_outside_range() {
        let root = "/tmp/db_shrink_bucket_outside";
        cleanup_test_root(root);
        let config = Config {
            total_buckets: 8,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let db = Db::open(config, vec![0u16..=1u16]).unwrap();
        let err = db.shrink_bucket(vec![2u16..=2u16]).unwrap_err();
        assert!(matches!(err, Error::ConfigError(_)));
        drop(db);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_shrink_bucket_rejects_removing_all_ranges() {
        let root = "/tmp/db_shrink_bucket_all";
        cleanup_test_root(root);
        let config = Config {
            total_buckets: 8,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let db = Db::open(config, vec![0u16..=1u16]).unwrap();
        let err = db.shrink_bucket(vec![0u16..=1u16]).unwrap_err();
        assert!(matches!(err, Error::ConfigError(_)));
        drop(db);
        cleanup_test_root(root);
    }
}
