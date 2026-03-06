use crate::error::{Error, Result};
use crate::lsm::LSMTreeVersion;
use crate::memtable::{ActiveMemtable, ImmutableMemtable};
use crate::vlog::VlogVersion;
use arc_swap::ArcSwap;
use std::collections::VecDeque;
use std::ops::RangeInclusive;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Condvar, Mutex};

#[derive(Clone)]
struct TreeVersionEntry {
    bucket_range: RangeInclusive<u16>,
    lsm_version: Arc<LSMTreeVersion>,
}

#[derive(Clone)]
pub(crate) struct MultiLSMTreeVersion {
    total_buckets: u32,
    bucket_to_tree_idx: Vec<u32>,
    tree_versions: Vec<TreeVersionEntry>,
}

impl MultiLSMTreeVersion {
    pub(crate) fn new(lsm_version: LSMTreeVersion) -> Self {
        Self {
            total_buckets: 0,
            bucket_to_tree_idx: Vec::new(),
            tree_versions: vec![TreeVersionEntry {
                bucket_range: 0u16..=0u16,
                lsm_version: Arc::new(lsm_version),
            }],
        }
    }

    pub(crate) fn from_bucket_ranges(
        total_buckets: u32,
        bucket_ranges: &[RangeInclusive<u16>],
        lsm_version: Arc<LSMTreeVersion>,
    ) -> Result<Self> {
        let tree_versions = (0..bucket_ranges.len())
            .map(|_| Arc::clone(&lsm_version))
            .collect();
        Self::from_bucket_ranges_with_tree_versions(total_buckets, bucket_ranges, tree_versions)
    }

    pub(crate) fn from_bucket_ranges_with_tree_versions(
        total_buckets: u32,
        bucket_ranges: &[RangeInclusive<u16>],
        lsm_versions: Vec<Arc<LSMTreeVersion>>,
    ) -> Result<Self> {
        if total_buckets == 0 || total_buckets > (u16::MAX as u32) + 1 {
            return Err(Error::ConfigError(
                "total_buckets must be in range 1..=65536".to_string(),
            ));
        }
        if bucket_ranges.is_empty() {
            return Err(Error::ConfigError(
                "bucket_ranges must not be empty".to_string(),
            ));
        }
        if lsm_versions.len() != bucket_ranges.len() {
            return Err(Error::InvalidState(format!(
                "LSM tree version count {} does not match bucket range count {}",
                lsm_versions.len(),
                bucket_ranges.len()
            )));
        }
        let mut bucket_to_tree_idx = vec![u32::MAX; bucket_slots_for_total(total_buckets)];
        let mut tree_versions = Vec::with_capacity(bucket_ranges.len());
        for (tree_idx, range) in bucket_ranges.iter().enumerate() {
            if !bucket_range_fits_total(range, total_buckets) {
                return Err(Error::ConfigError(format!(
                    "Invalid bucket range {}..={} for total_buckets {}",
                    range.start(),
                    range.end(),
                    total_buckets
                )));
            }
            let Some(last_bucket) = bucket_range_last(range) else {
                return Err(Error::ConfigError(format!(
                    "Invalid bucket range {}..={} for total_buckets {}",
                    range.start(),
                    range.end(),
                    total_buckets
                )));
            };
            let mut bucket = *range.start();
            loop {
                let slot = &mut bucket_to_tree_idx[bucket as usize];
                if *slot != u32::MAX {
                    return Err(Error::ConfigError(format!(
                        "Overlapping bucket range detected at bucket {}",
                        bucket
                    )));
                }
                *slot = tree_idx as u32;
                if bucket == last_bucket {
                    break;
                }
                bucket = bucket.saturating_add(1);
            }
            tree_versions.push(TreeVersionEntry {
                bucket_range: range.clone(),
                lsm_version: Arc::clone(&lsm_versions[tree_idx]),
            });
        }
        Ok(Self {
            total_buckets,
            bucket_to_tree_idx,
            tree_versions,
        })
    }

    #[cfg(test)]
    pub(crate) fn from_parts(
        total_buckets: u32,
        bucket_to_tree_idx: Vec<u32>,
        tree_versions: Vec<Arc<LSMTreeVersion>>,
    ) -> Self {
        let mut entries = Vec::with_capacity(tree_versions.len());
        for (tree_idx, lsm_version) in tree_versions.into_iter().enumerate() {
            let mut start: Option<u16> = None;
            let mut end: Option<u16> = None;
            for (bucket, mapped) in bucket_to_tree_idx.iter().enumerate() {
                if *mapped != tree_idx as u32 {
                    continue;
                }
                let bucket = bucket as u16;
                if start.is_none() {
                    start = Some(bucket);
                }
                end = Some(bucket);
            }
            entries.push(TreeVersionEntry {
                bucket_range: match (start, end) {
                    (Some(start), Some(end)) => start..=end,
                    _ => 0u16..=0u16,
                },
                lsm_version,
            });
        }
        Self {
            total_buckets,
            bucket_to_tree_idx,
            tree_versions: entries,
        }
    }

    pub(crate) fn version_of_index(&self, i: usize) -> Arc<LSMTreeVersion> {
        self.tree_versions
            .get(i)
            .expect("Invalid tree index")
            .lsm_version
            .clone()
    }

    pub(crate) fn tree_count(&self) -> usize {
        self.tree_versions.len().max(1)
    }

    pub(crate) fn tree_versions_cloned(&self) -> Vec<Arc<LSMTreeVersion>> {
        self.tree_versions
            .iter()
            .map(|entry| Arc::clone(&entry.lsm_version))
            .collect()
    }

    pub(crate) fn total_buckets(&self) -> u32 {
        self.total_buckets
    }

    pub(crate) fn tree_index_for_bucket(&self, bucket: u16) -> Option<usize> {
        if self.bucket_to_tree_idx.is_empty() {
            return Some(0);
        }
        let tree_idx = *self.bucket_to_tree_idx.get(bucket as usize)?;
        if tree_idx == u32::MAX {
            return None;
        }
        Some(tree_idx as usize)
    }

    pub(crate) fn version_for_bucket(&self, bucket: u16) -> Option<Arc<LSMTreeVersion>> {
        self.tree_index_for_bucket(bucket)
            .map(|tree_idx| self.version_of_index(tree_idx))
    }

    pub(crate) fn bucket_range_of_tree(&self, tree_idx: usize) -> Option<RangeInclusive<u16>> {
        self.tree_versions
            .get(tree_idx)
            .map(|entry| entry.bucket_range.clone())
    }

    pub(crate) fn bucket_ranges(&self) -> Vec<RangeInclusive<u16>> {
        self.tree_versions
            .iter()
            .map(|entry| entry.bucket_range.clone())
            .collect()
    }

    pub(crate) fn tree_index_for_exact_range(&self, range: &RangeInclusive<u16>) -> Option<usize> {
        self.tree_versions
            .iter()
            .position(|entry| &entry.bucket_range == range)
    }

    pub(crate) fn with_lsm_version_at(
        &self,
        tree_idx: usize,
        lsm_version: Arc<LSMTreeVersion>,
    ) -> Self {
        let mut tree_versions = self.tree_versions.clone();
        if tree_versions.is_empty() {
            tree_versions.push(TreeVersionEntry {
                bucket_range: 0u16..=0u16,
                lsm_version,
            });
        } else {
            let slot = tree_versions.get_mut(tree_idx).expect("Invalid tree index");
            slot.lsm_version = lsm_version;
        }
        Self {
            total_buckets: self.total_buckets,
            bucket_to_tree_idx: self.bucket_to_tree_idx.clone(),
            tree_versions,
        }
    }
}

pub(crate) struct DbState {
    pub(crate) seq_id: u64,
    pub(crate) multi_lsm_version: MultiLSMTreeVersion,
    pub(crate) vlog_version: VlogVersion,
    pub(crate) active: Option<Arc<Mutex<ActiveMemtable>>>,
    pub(crate) immutables: VecDeque<ImmutableMemtable>,
    // This is used to suggest a base snapshot ID for new snapshots
    pub(crate) suggested_base_snapshot_id: Option<u64>,
}

pub(crate) struct DbStateHandle {
    current: ArcSwap<DbState>,
    lock: Mutex<()>,
    next_seq_id: AtomicU64,
    changed: Condvar,
}

impl DbStateHandle {
    pub(crate) fn new() -> Self {
        Self {
            current: ArcSwap::from_pointee(DbState {
                seq_id: 0,
                multi_lsm_version: MultiLSMTreeVersion::new(LSMTreeVersion { levels: vec![] }),
                vlog_version: VlogVersion::new(),
                active: None,
                immutables: VecDeque::new(),
                suggested_base_snapshot_id: None,
            }),
            lock: Mutex::new(()),
            next_seq_id: AtomicU64::new(1),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn allocate_seq_id(&self) -> u64 {
        self.next_seq_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn load(&self) -> Arc<DbState> {
        self.current.load_full()
    }

    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.lock.lock().unwrap()
    }

    pub(crate) fn wait_for_change<'a>(
        &self,
        guard: std::sync::MutexGuard<'a, ()>,
    ) -> std::sync::MutexGuard<'a, ()> {
        self.changed.wait(guard).unwrap()
    }

    pub(crate) fn cas_mutate<F>(&self, expected: u64, f: F) -> bool
    where
        F: FnOnce(&DbStateHandle, &DbState) -> Option<DbState>,
    {
        let current = self.load();
        if current.seq_id != expected {
            return false;
        }
        let Some(new_version) = f(self, &current) else {
            return true;
        };
        self.store(new_version);
        true
    }

    pub(crate) fn update_suggested_snapshot(&self, seq_id: u64, snapshot_id: u64) -> bool {
        let _guard = self.lock();
        self.cas_mutate(seq_id, |_db_state, snapshot| {
            if snapshot.suggested_base_snapshot_id == Some(snapshot_id) {
                return None;
            }
            Some(DbState {
                seq_id,
                multi_lsm_version: snapshot.multi_lsm_version.clone(),
                vlog_version: snapshot.vlog_version.clone(),
                active: snapshot.active.clone(),
                immutables: snapshot.immutables.clone(),
                suggested_base_snapshot_id: Some(snapshot_id),
            })
        })
    }

    pub(crate) fn configure_multi_lsm(
        &self,
        total_buckets: u32,
        bucket_ranges: &[RangeInclusive<u16>],
    ) -> Result<()> {
        let _guard = self.lock();
        let snapshot = self.load();
        let existing_tree_count = snapshot.multi_lsm_version.tree_count();
        let tree_versions = if existing_tree_count == bucket_ranges.len() {
            snapshot.multi_lsm_version.tree_versions_cloned()
        } else if existing_tree_count == 1 {
            let primary = snapshot.multi_lsm_version.version_of_index(0);
            (0..bucket_ranges.len())
                .map(|_| Arc::clone(&primary))
                .collect()
        } else {
            return Err(Error::InvalidState(format!(
                "Cannot configure {} bucket ranges with {} existing LSM trees",
                bucket_ranges.len(),
                existing_tree_count
            )));
        };
        let multi_lsm_version = MultiLSMTreeVersion::from_bucket_ranges_with_tree_versions(
            total_buckets,
            bucket_ranges,
            tree_versions,
        )?;
        self.store(DbState {
            seq_id: snapshot.seq_id,
            multi_lsm_version,
            vlog_version: snapshot.vlog_version.clone(),
            active: snapshot.active.clone(),
            immutables: snapshot.immutables.clone(),
            suggested_base_snapshot_id: snapshot.suggested_base_snapshot_id,
        });
        Ok(())
    }

    pub(crate) fn store(&self, new_version: DbState) {
        self.current.store(Arc::new(new_version));
        self.changed.notify_all();
    }
}

//----------------------------
// bucket utilities
//----------------------------
pub(crate) fn bucket_slots_for_total(total_buckets: u32) -> usize {
    total_buckets as usize
}

pub(crate) fn bucket_max_for_total(total_buckets: u32) -> u16 {
    total_buckets.saturating_sub(1) as u16
}

pub(crate) fn full_bucket_range(total_buckets: u32) -> RangeInclusive<u16> {
    0u16..=bucket_max_for_total(total_buckets)
}

pub(crate) fn bucket_range_is_empty(range: &RangeInclusive<u16>) -> bool {
    range.start() > range.end()
}

pub(crate) fn bucket_range_last(range: &RangeInclusive<u16>) -> Option<u16> {
    if bucket_range_is_empty(range) {
        return None;
    }
    Some(*range.end())
}

pub(crate) fn bucket_range_len(range: &RangeInclusive<u16>) -> usize {
    if bucket_range_is_empty(range) {
        return 0;
    }
    ((*range.end() as usize).saturating_sub(*range.start() as usize)) + 1
}

pub(crate) fn bucket_range_fits_total(range: &RangeInclusive<u16>, total_buckets: u32) -> bool {
    let slots = bucket_slots_for_total(total_buckets);
    if slots == 0 || bucket_range_is_empty(range) {
        return false;
    }
    let Some(last) = bucket_range_last(range) else {
        return false;
    };
    let max_bucket = slots.saturating_sub(1);
    (*range.start() as usize) <= max_bucket && (last as usize) <= max_bucket
}

//----------------------------
// end of bucket utilities
//----------------------------
