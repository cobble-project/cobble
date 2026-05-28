//! Snapshot manager and manifest encoding for LSM state.
mod manager;
mod manifest;
mod memtable;

use crate::db_state::LSMTreeScope;
use crate::db_state::TruncationCursorMap;
use crate::error::Result;
use crate::file::TrackedFile;
use crate::lsm::LSMTreeVersion;
use crate::vlog::VlogVersion;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

pub(crate) use manager::SnapshotManager;
pub(crate) use manifest::{
    LoadedManifest, ManifestSnapshot, build_tree_scopes_from_manifest,
    build_tree_versions_from_manifest, build_truncation_cursors_from_manifest,
    build_vlog_version_from_manifest, list_snapshot_manifest_ids, load_manifest_chain_from_path,
    load_manifest_entry, load_manifest_for_snapshot, snapshot_manifest_name,
};
pub(crate) use memtable::ActiveMemtableSnapshotData;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum SnapshotLifecycleState {
    Pending = 0,
    Cancelled = 1,
    CommitStarted = 2,
    CommitStartedExpireRequested = 3,
    Published = 4,
}

impl SnapshotLifecycleState {
    pub(crate) fn from_raw(value: u8) -> Self {
        match value {
            0 => Self::Pending,
            1 => Self::Cancelled,
            2 => Self::CommitStarted,
            3 => Self::CommitStartedExpireRequested,
            4 => Self::Published,
            _ => panic!("invalid snapshot publication state: {}", value),
        }
    }

    pub(crate) fn is_cancelled_raw(state: &AtomicU8) -> bool {
        Self::from_raw(state.load(Ordering::SeqCst)) == Self::Cancelled
    }
}

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
    pub truncation_cursors: TruncationCursorMap,
    pub data_size_bytes: u64,
    pub incremental_data_size_bytes: u64,
    pub active_memtable_total_size_bytes: u64,
    pub callback: Option<SnapshotCallback>,
    pub lifecycle_state: Arc<AtomicU8>,
}

/// Information about a materialized snapshot, passed to snapshot callbacks.
#[derive(Clone)]
pub(crate) struct SnapshotManifestInfo {
    pub id: u64,
    pub manifest_path: String,
    pub bucket_ranges: Vec<RangeInclusive<u16>>,
    pub latest_schema_id: u64,
    pub data_size_bytes: u64,
    pub incremental_data_size_bytes: u64,
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
            truncation_cursors: TruncationCursorMap::new(),
            data_size_bytes: 0,
            incremental_data_size_bytes: 0,
            active_memtable_total_size_bytes: 0,
            callback,
            lifecycle_state: Arc::new(AtomicU8::new(SnapshotLifecycleState::Pending as u8)),
        }
    }

    pub(crate) fn lifecycle_state(&self) -> SnapshotLifecycleState {
        SnapshotLifecycleState::from_raw(self.lifecycle_state.load(Ordering::SeqCst))
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        SnapshotLifecycleState::is_cancelled_raw(self.lifecycle_state.as_ref())
    }

    pub(crate) fn is_published(&self) -> bool {
        self.lifecycle_state() == SnapshotLifecycleState::Published
    }

    pub(crate) fn try_cancel(&self) -> bool {
        self.lifecycle_state
            .compare_exchange(
                SnapshotLifecycleState::Pending as u8,
                SnapshotLifecycleState::Cancelled as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }

    pub(crate) fn try_begin_publication(&self) -> Result<(), SnapshotLifecycleState> {
        self.lifecycle_state
            .compare_exchange(
                SnapshotLifecycleState::Pending as u8,
                SnapshotLifecycleState::CommitStarted as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| ())
            .map_err(SnapshotLifecycleState::from_raw)
    }

    pub(crate) fn request_expire_after_publication_start(&self) -> bool {
        loop {
            match self.lifecycle_state() {
                SnapshotLifecycleState::CommitStarted => {
                    if self
                        .lifecycle_state
                        .compare_exchange(
                            SnapshotLifecycleState::CommitStarted as u8,
                            SnapshotLifecycleState::CommitStartedExpireRequested as u8,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        return true;
                    }
                }
                SnapshotLifecycleState::CommitStartedExpireRequested => return true,
                _ => return false,
            }
        }
    }

    pub(crate) fn mark_published(&self) -> bool {
        loop {
            match self.lifecycle_state() {
                SnapshotLifecycleState::CommitStarted => {
                    if self
                        .lifecycle_state
                        .compare_exchange(
                            SnapshotLifecycleState::CommitStarted as u8,
                            SnapshotLifecycleState::Published as u8,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        return false;
                    }
                }
                SnapshotLifecycleState::CommitStartedExpireRequested => {
                    if self
                        .lifecycle_state
                        .compare_exchange(
                            SnapshotLifecycleState::CommitStartedExpireRequested as u8,
                            SnapshotLifecycleState::Published as u8,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        return true;
                    }
                }
                SnapshotLifecycleState::Published => return false,
                state => panic!("cannot publish snapshot from lifecycle state {:?}", state),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DbSnapshot, SnapshotLifecycleState};

    #[test]
    fn cancel_wins_only_before_publication_starts() {
        let snapshot = DbSnapshot::new(7, "manifest", None);

        assert!(snapshot.try_cancel());
        assert_eq!(
            snapshot.lifecycle_state(),
            SnapshotLifecycleState::Cancelled
        );
        assert!(!snapshot.try_cancel());
        assert_eq!(
            snapshot.try_begin_publication(),
            Err(SnapshotLifecycleState::Cancelled)
        );
    }

    #[test]
    fn publication_start_blocks_later_cancellation() {
        let snapshot = DbSnapshot::new(8, "manifest", None);

        assert_eq!(snapshot.try_begin_publication(), Ok(()));
        assert_eq!(
            snapshot.lifecycle_state(),
            SnapshotLifecycleState::CommitStarted
        );
        assert!(!snapshot.try_cancel());

        snapshot.mark_published();
        assert_eq!(
            snapshot.lifecycle_state(),
            SnapshotLifecycleState::Published
        );
        assert!(!snapshot.try_cancel());
    }

    #[test]
    fn expire_request_is_folded_into_lifecycle_state() {
        let snapshot = DbSnapshot::new(9, "manifest", None);

        assert_eq!(snapshot.try_begin_publication(), Ok(()));
        assert!(snapshot.request_expire_after_publication_start());
        assert_eq!(
            snapshot.lifecycle_state(),
            SnapshotLifecycleState::CommitStartedExpireRequested
        );
        assert!(snapshot.mark_published());
        assert_eq!(
            snapshot.lifecycle_state(),
            SnapshotLifecycleState::Published
        );
    }
}
