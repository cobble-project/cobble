//! In-process coordination for table shard snapshots.
//!
//! [`TableSnapshotCommitter`] keeps partial commits only in memory, with a bounded number of
//! commit IDs rather than a wall-clock TTL. The runtime must replay incomplete commits after
//! failure. Commit IDs order batches only within one committer process and are independent from
//! global snapshot IDs assigned by [`DbCoordinator`]. This first version assumes one active
//! committer across processes and does not persist commit identity, so a replacement committer may
//! publish a new equivalent global snapshot when work is replayed.

use crate::{Result, TableError};
use cobble::{DbCoordinator, GlobalSnapshotManifest, ShardSnapshotInput};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Collects shard snapshots and materializes each complete table checkpoint.
pub struct TableSnapshotCommitter {
    coordinator: Arc<DbCoordinator>,
    total_buckets: u32,
    max_pending_commits: usize,
    state: Mutex<CommitterState>,
}

struct CommitterState {
    latest_completed_commit_id: Option<u64>,
    pending: BTreeMap<u64, PendingCommit>,
}

#[derive(Default)]
struct PendingCommit {
    shards: BTreeMap<String, ShardSnapshotInput>,
    prepared_snapshot: Option<GlobalSnapshotManifest>,
}

impl TableSnapshotCommitter {
    pub fn new(
        coordinator: Arc<DbCoordinator>,
        total_buckets: u32,
        max_pending_commits: usize,
    ) -> Result<Self> {
        if total_buckets == 0 || total_buckets > u16::MAX as u32 + 1 {
            return Err(cobble::Error::ConfigError(
                "total_buckets must be in range 1..=65536".to_string(),
            )
            .into());
        }
        if max_pending_commits == 0 {
            return Err(cobble::Error::ConfigError(
                "max_pending_commits must be positive".to_string(),
            )
            .into());
        }
        Ok(Self {
            coordinator,
            total_buckets,
            max_pending_commits,
            state: Mutex::new(CommitterState {
                latest_completed_commit_id: None,
                pending: BTreeMap::new(),
            }),
        })
    }

    /// Submit one shard snapshot for a process-local commit ID.
    ///
    /// Returns `Some` only when this shard completes and materializes the commit. Pending,
    /// completed, and superseded submissions return `None`.
    pub fn submit(
        &self,
        commit_id: u64,
        mut snapshot: ShardSnapshotInput,
    ) -> Result<Option<GlobalSnapshotManifest>> {
        let mut state = self.lock_state()?;
        if is_completed_or_superseded(&state, commit_id) {
            return Ok(None);
        }

        normalize_ranges(self.total_buckets, &mut snapshot)?;
        if let std::collections::btree_map::Entry::Vacant(entry) = state.pending.entry(commit_id) {
            entry.insert(PendingCommit::default());
            if !retain_new_pending_commit(&mut state, commit_id, self.max_pending_commits) {
                return Ok(None);
            }
        }
        {
            let pending = state
                .pending
                .get_mut(&commit_id)
                .expect("commit remains in the pending window");
            insert_shard(pending, snapshot, commit_id)?;
            if !has_exact_bucket_coverage(self.total_buckets, pending) {
                return Ok(None);
            }
        }
        self.commit_complete(&mut state, commit_id)
    }

    /// Commit an already collected, complete checkpoint batch.
    ///
    /// Inputs may be unordered. Equivalent duplicate shard inputs are ignored, while gaps,
    /// overlaps, and conflicting duplicates are rejected. Returns `None` when this commit or a
    /// higher commit has already completed.
    pub fn commit_batch(
        &self,
        commit_id: u64,
        shard_snapshots: Vec<ShardSnapshotInput>,
    ) -> Result<Option<GlobalSnapshotManifest>> {
        let mut state = self.lock_state()?;
        if is_completed_or_superseded(&state, commit_id) {
            return Ok(None);
        }

        let mut batch = PendingCommit::default();
        for mut snapshot in shard_snapshots {
            normalize_ranges(self.total_buckets, &mut snapshot)?;
            insert_shard(&mut batch, snapshot, commit_id)?;
        }
        if !has_exact_bucket_coverage(self.total_buckets, &batch) {
            return Err(coordination_error(format!(
                "table snapshot commit {commit_id} does not cover all buckets"
            )));
        }
        match state.pending.entry(commit_id) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let pending = entry.get();
                if pending.prepared_snapshot.is_some() && pending.shards != batch.shards {
                    return Err(coordination_error(format!(
                        "commit {commit_id} conflicts with its prepared snapshot"
                    )));
                }
                if pending.prepared_snapshot.is_none() {
                    entry.insert(batch);
                }
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(batch);
            }
        }
        self.commit_complete(&mut state, commit_id)
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, CommitterState>> {
        self.state
            .lock()
            .map_err(|_| coordination_error("table snapshot commit lock poisoned"))
    }

    fn commit_complete(
        &self,
        state: &mut CommitterState,
        commit_id: u64,
    ) -> Result<Option<GlobalSnapshotManifest>> {
        let snapshot = {
            let pending = state
                .pending
                .get_mut(&commit_id)
                .expect("complete commit remains pending until publication succeeds");
            if pending.prepared_snapshot.is_none() {
                let inputs = canonical_inputs(pending);
                pending.prepared_snapshot = Some(
                    self.coordinator
                        .take_global_snapshot(self.total_buckets, inputs)?,
                );
            }
            pending
                .prepared_snapshot
                .as_ref()
                .expect("snapshot was prepared")
                .clone()
        };
        let materialized = self.materialize_prepared_snapshot(&snapshot)?;
        state.latest_completed_commit_id = Some(commit_id);
        state.pending.retain(|id, _| *id > commit_id);
        Ok(materialized.then_some(snapshot))
    }

    fn materialize_prepared_snapshot(&self, snapshot: &GlobalSnapshotManifest) -> Result<bool> {
        if let Some(current) = self.coordinator.load_current_global_snapshot()? {
            if current.id > snapshot.id {
                return Ok(false);
            }
            if current.id == snapshot.id {
                if current != *snapshot {
                    return Err(coordination_error(format!(
                        "global snapshot {} conflicts with its prepared manifest",
                        snapshot.id
                    )));
                }
                return Ok(true);
            }
        }
        self.coordinator.materialize_global_snapshot(snapshot)?;
        Ok(true)
    }
}

fn is_completed_or_superseded(state: &CommitterState, commit_id: u64) -> bool {
    state
        .latest_completed_commit_id
        .is_some_and(|latest| commit_id <= latest)
}

fn retain_new_pending_commit(
    state: &mut CommitterState,
    commit_id: u64,
    max_pending_commits: usize,
) -> bool {
    if state.pending.len() <= max_pending_commits {
        return true;
    }
    let oldest = *state
        .pending
        .first_key_value()
        .expect("pending contains the inserted commit")
        .0;
    state.pending.remove(&oldest);
    oldest != commit_id
}

fn insert_shard(
    pending: &mut PendingCommit,
    snapshot: ShardSnapshotInput,
    commit_id: u64,
) -> Result<()> {
    match pending.shards.get(&snapshot.db_id) {
        Some(existing) if existing != &snapshot => Err(coordination_error(format!(
            "shard {} submitted conflicting snapshots for commit {commit_id}",
            snapshot.db_id
        ))),
        Some(_) => Ok(()),
        None => {
            reject_overlapping_ranges(pending, &snapshot)?;
            pending.shards.insert(snapshot.db_id.clone(), snapshot);
            Ok(())
        }
    }
}

fn normalize_ranges(total_buckets: u32, input: &mut ShardSnapshotInput) -> Result<()> {
    if input.ranges.is_empty() {
        return Err(coordination_error(format!(
            "shard snapshot ranges must not be empty for {}",
            input.db_id
        )));
    }
    input
        .ranges
        .sort_by_key(|range| (*range.start(), *range.end()));
    let mut previous_end = None;
    for range in &input.ranges {
        let start = u32::from(*range.start());
        let end = u32::from(*range.end());
        if start > end || end >= total_buckets {
            return Err(coordination_error(format!(
                "invalid shard snapshot range {start}..={end} for {total_buckets} buckets"
            )));
        }
        if previous_end.is_some_and(|previous| start <= previous) {
            return Err(coordination_error(format!(
                "shard snapshot ranges overlap at bucket {start}"
            )));
        }
        previous_end = Some(end);
    }
    Ok(())
}

fn reject_overlapping_ranges(pending: &PendingCommit, input: &ShardSnapshotInput) -> Result<()> {
    for (existing_db_id, existing) in &pending.shards {
        let mut left = existing.ranges.iter().peekable();
        let mut right = input.ranges.iter().peekable();
        while let (Some(existing), Some(candidate)) = (left.peek(), right.peek()) {
            if existing.end() < candidate.start() {
                left.next();
            } else if candidate.end() < existing.start() {
                right.next();
            } else {
                return Err(coordination_error(format!(
                    "shard snapshot ranges overlap between {existing_db_id} and {}",
                    input.db_id
                )));
            }
        }
    }
    Ok(())
}

fn has_exact_bucket_coverage(total_buckets: u32, pending: &PendingCommit) -> bool {
    let mut ranges = pending
        .shards
        .values()
        .flat_map(|input| input.ranges.iter())
        .collect::<Vec<_>>();
    ranges.sort_by_key(|range| (*range.start(), *range.end()));
    let mut expected = 0;
    for range in ranges {
        if u32::from(*range.start()) != expected {
            return false;
        }
        expected = u32::from(*range.end()) + 1;
    }
    expected == total_buckets
}

fn canonical_inputs(pending: &PendingCommit) -> Vec<ShardSnapshotInput> {
    let mut inputs = pending.shards.values().cloned().collect::<Vec<_>>();
    inputs.sort_by(|left, right| {
        left.ranges
            .iter()
            .map(|range| (*range.start(), *range.end()))
            .cmp(
                right
                    .ranges
                    .iter()
                    .map(|range| (*range.start(), *range.end())),
            )
            .then_with(|| left.db_id.cmp(&right.db_id))
    });
    inputs
}

fn coordination_error(message: impl Into<String>) -> TableError {
    cobble::Error::CoordinationError(message.into()).into()
}
