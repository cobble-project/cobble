use crate::error::{Error, Result};
use crate::lsm::LSMTreeVersion;
use crate::memtable::{ActiveMemtable, ImmutableMemtable};
use crate::vlog::VlogVersion;
use arc_swap::ArcSwap;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Condvar, Mutex};

#[derive(Clone)]
pub(crate) struct MultiLSMTreeVersion {
    bucket_to_tree_idx: Vec<u16>,
    tree_versions: Vec<Arc<LSMTreeVersion>>,
}

impl MultiLSMTreeVersion {
    pub(crate) fn new(lsm_version: LSMTreeVersion) -> Self {
        Self {
            bucket_to_tree_idx: Vec::new(),
            tree_versions: vec![Arc::new(lsm_version)],
        }
    }

    pub(crate) fn from_bucket_ranges(
        total_buckets: u16,
        bucket_ranges: &[Range<u16>],
        lsm_version: Arc<LSMTreeVersion>,
    ) -> Result<Self> {
        if total_buckets == 0 {
            return Err(Error::ConfigError(
                "total_buckets must be greater than 0".to_string(),
            ));
        }
        if bucket_ranges.is_empty() {
            return Err(Error::ConfigError(
                "bucket_ranges must not be empty".to_string(),
            ));
        }
        let mut bucket_to_tree_idx = vec![u16::MAX; total_buckets as usize];
        for (tree_idx, range) in bucket_ranges.iter().enumerate() {
            if range.start >= range.end || range.end > total_buckets {
                return Err(Error::ConfigError(format!(
                    "Invalid bucket range {}..{} for total_buckets {}",
                    range.start, range.end, total_buckets
                )));
            }
            for bucket in range.start..range.end {
                let slot = &mut bucket_to_tree_idx[bucket as usize];
                if *slot != u16::MAX {
                    return Err(Error::ConfigError(format!(
                        "Overlapping bucket range detected at bucket {}",
                        bucket
                    )));
                }
                *slot = tree_idx as u16;
            }
        }
        Ok(Self {
            bucket_to_tree_idx,
            tree_versions: (0..bucket_ranges.len())
                .map(|_| Arc::clone(&lsm_version))
                .collect(),
        })
    }

    #[cfg(test)]
    pub(crate) fn from_parts(
        bucket_to_tree_idx: Vec<u16>,
        tree_versions: Vec<Arc<LSMTreeVersion>>,
    ) -> Self {
        Self {
            bucket_to_tree_idx,
            tree_versions,
        }
    }

    pub(crate) fn version_of_index(&self, i: usize) -> Arc<LSMTreeVersion> {
        self.tree_versions
            .get(i)
            .expect("Invalid tree index")
            .clone()
    }

    pub(crate) fn version_for_bucket(&self, bucket: u16) -> Option<Arc<LSMTreeVersion>> {
        if self.bucket_to_tree_idx.is_empty() {
            return Some(self.version_of_index(0));
        }
        let tree_idx = *self.bucket_to_tree_idx.get(bucket as usize)?;
        if tree_idx == u16::MAX {
            return None;
        }
        self.tree_versions.get(tree_idx as usize).cloned()
    }

    pub(crate) fn with_lsm_version(&self, lsm_version: Arc<LSMTreeVersion>) -> Self {
        let tree_count = self.tree_versions.len().max(1);
        Self {
            bucket_to_tree_idx: self.bucket_to_tree_idx.clone(),
            tree_versions: (0..tree_count).map(|_| Arc::clone(&lsm_version)).collect(),
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
        total_buckets: u16,
        bucket_ranges: &[Range<u16>],
    ) -> Result<()> {
        let _guard = self.lock();
        let snapshot = self.load();
        let multi_lsm_version = MultiLSMTreeVersion::from_bucket_ranges(
            total_buckets,
            bucket_ranges,
            snapshot.multi_lsm_version.version_of_index(0),
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
