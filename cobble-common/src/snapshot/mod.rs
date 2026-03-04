//! Snapshot manager and manifest encoding for LSM state.
mod manager;
mod manifest;
mod memtable;

use crate::error::Result;
use crate::file::TrackedFile;
use crate::lsm::LSMTreeVersion;
use crate::vlog::VlogVersion;
use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::Arc;

pub(crate) use manager::SnapshotManager;
pub(crate) use manifest::{
    LoadedManifest, ManifestPayload, ManifestSnapshot, apply_manifest_tree_level_edits,
    build_tree_versions_from_manifest, build_vlog_version_from_manifest, decode_manifest,
    load_manifest_for_snapshot, snapshot_manifest_name,
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
    // Tracked references to all files included in the snapshot, used for reference counting and
    // cleanup when expiring snapshots. This includes both LSM files and vlog files.
    pub tracked_files: Vec<Arc<TrackedFile>>,
    // Vlog version at the time of snapshot creation, without tracked references.
    pub vlog_version: VlogVersion,
    pub seq_id: u64,
    pub latest_schema_id: u64,
    pub referenced_schema_ids: BTreeSet<u64>,
    pub active_memtable_data: Vec<ActiveMemtableSnapshotData>,
    pub bucket_ranges: Vec<Range<u16>>,
    pub finished: bool,
    pub callback: Option<SnapshotCallback>,
}

pub(crate) type SnapshotCallback = Arc<dyn Fn(Result<u64>) + Send + Sync + 'static>;

impl DbSnapshot {
    pub(crate) fn new(id: u64, manifest_path: String, callback: Option<SnapshotCallback>) -> Self {
        Self {
            id,
            manifest_path,
            base_snapshot_id: None,
            lsm_versions: vec![],
            tracked_files: Vec::new(),
            vlog_version: VlogVersion::new(),
            seq_id: 0,
            latest_schema_id: 0,
            referenced_schema_ids: BTreeSet::new(),
            active_memtable_data: Vec::new(),
            bucket_ranges: Vec::new(),
            finished: false,
            callback,
        }
    }
}
