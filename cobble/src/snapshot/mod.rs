//! Snapshot manager and manifest encoding for LSM state.
mod manager;
mod manifest;
mod memtable;

use crate::db_state::LSMTreeScope;
use crate::error::Result;
use crate::file::TrackedFile;
use crate::lsm::LSMTreeVersion;
use crate::vlog::VlogVersion;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;
use std::sync::Arc;

pub(crate) use manager::SnapshotManager;
pub(crate) use manifest::{
    LoadedManifest, ManifestSnapshot, build_tree_scopes_from_manifest,
    build_tree_versions_from_manifest, build_vlog_version_from_manifest,
    list_snapshot_manifest_ids, load_manifest_entry, load_manifest_for_snapshot,
    snapshot_manifest_name,
};
pub(crate) use memtable::ActiveMemtableSnapshotData;

/// Internal snapshot record tracked by the manager.
#[derive(Clone)]
pub(crate) struct DbSnapshot {
    pub id: u64,
    pub manifest_path: String,
    pub base_snapshot_id: Option<u64>,
    // Per-tree LSM versions without tracked file references.
    pub lsm_versions: Vec<LSMTreeVersion>,
    // Source data file id -> tracked snapshot file.
    pub tracked_data_files: BTreeMap<u64, Arc<TrackedFile>>,
    // Vlog version at the time of snapshot creation, without tracked references.
    pub vlog_version: VlogVersion,
    pub seq_id: u64,
    pub latest_schema_id: u64,
    pub referenced_schema_ids: BTreeSet<u64>,
    pub active_memtable_data: Vec<ActiveMemtableSnapshotData>,
    pub lsm_tree_bucket_ranges: Vec<RangeInclusive<u16>>,
    pub tree_scopes: Vec<LSMTreeScope>,
    pub bucket_ranges: Vec<RangeInclusive<u16>>,
    pub finished: bool,
    pub callback: Option<SnapshotCallback>,
}

/// Information about a materialized snapshot, passed to snapshot callbacks.
#[derive(Clone)]
pub(crate) struct SnapshotManifestInfo {
    pub id: u64,
    pub manifest_path: String,
    pub bucket_ranges: Vec<RangeInclusive<u16>>,
    pub latest_schema_id: u64,
}

pub(crate) type SnapshotCallback =
    Arc<dyn Fn(Result<SnapshotManifestInfo>) + Send + Sync + 'static>;

impl DbSnapshot {
    pub(crate) fn new(id: u64, manifest_path: &str, callback: Option<SnapshotCallback>) -> Self {
        Self {
            id,
            manifest_path: manifest_path.to_string(),
            base_snapshot_id: None,
            lsm_versions: vec![],
            tracked_data_files: BTreeMap::new(),
            vlog_version: VlogVersion::new(),
            seq_id: 0,
            latest_schema_id: 0,
            referenced_schema_ids: BTreeSet::new(),
            active_memtable_data: Vec::new(),
            lsm_tree_bucket_ranges: Vec::new(),
            tree_scopes: Vec::new(),
            bucket_ranges: Vec::new(),
            finished: false,
            callback,
        }
    }
}
