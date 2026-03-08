use super::DbSnapshot;
use crate::config::MemtableType;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct ActiveMemtableSnapshotData {
    /// Snapshot-owned metadata file path that stores the incremental payload segment.
    pub(crate) path: String,
    /// Active memtable implementation type this segment belongs to.
    pub(crate) memtable_type: MemtableType,
    /// Active memtable identity this segment belongs to.
    pub(crate) memtable_id: String,
    /// Inclusive start offset of KV bytes in memtable data stream.
    pub(crate) start_offset: u64,
    /// Exclusive end offset of KV bytes in memtable data stream.
    pub(crate) end_offset: u64,
    /// VLOG file sequence used by this memtable segment, if separated values exist.
    pub(crate) vlog_file_seq: Option<u32>,
    /// Inclusive start offset of VLOG bytes in recorder stream.
    pub(crate) vlog_start_offset: u32,
    /// Exclusive end offset of VLOG bytes in recorder stream.
    pub(crate) vlog_end_offset: u32,
    /// Byte offset inside `path` where this segment's VLOG payload starts.
    pub(crate) vlog_data_file_offset: u64,
}

pub(super) fn collect_active_memtable_snapshot_segments(
    snapshots: &BTreeMap<u64, Arc<DbSnapshot>>,
    base_snapshot_id: Option<u64>,
    memtable_type: MemtableType,
    memtable_id: &str,
) -> Vec<ActiveMemtableSnapshotData> {
    let mut current = base_snapshot_id;
    let mut visited = HashSet::new();
    let mut expected_end: Option<u64> = None;
    let mut collected_rev = Vec::new();
    let mut discontinuous = false;
    while let Some(id) = current {
        if !visited.insert(id) {
            break;
        }
        let Some(snapshot) = snapshots.get(&id) else {
            break;
        };
        let mut matched_in_snapshot = false;
        for segment in snapshot.active_memtable_data.iter().rev() {
            if segment.memtable_type != memtable_type || segment.memtable_id != memtable_id {
                continue;
            }
            if segment.end_offset < segment.start_offset {
                discontinuous = true;
                break;
            }
            if let Some(end) = expected_end
                && segment.end_offset != end
            {
                discontinuous = true;
                break;
            }
            matched_in_snapshot = true;
            expected_end = Some(segment.start_offset);
            collected_rev.push(segment.clone());
            if expected_end == Some(0) {
                break;
            }
        }
        if discontinuous || expected_end == Some(0) {
            break;
        }
        if !matched_in_snapshot {
            break;
        }
        current = snapshot.base_snapshot_id;
    }
    if discontinuous || expected_end != Some(0) {
        return Vec::new();
    }
    collected_rev.reverse();
    collected_rev
}
