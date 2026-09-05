use crate::error::{Error, Result};
use crate::lsm::LSMTreeVersion;
use crate::memtable::{ActiveMemtable, ImmutableMemtable};
use crate::schema::Schema;
use crate::schema::{DEFAULT_COLUMN_FAMILY_ID, MAX_COLUMN_FAMILY_COUNT};
use crate::vlog::VlogVersion;
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use uuid::Uuid;

#[derive(Clone)]
struct TreeVersionEntry {
    scope: LSMTreeScope,
    lsm_version: Arc<LSMTreeVersion>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LSMTreeScope {
    pub(crate) bucket_range: RangeInclusive<u16>,
    pub(crate) column_family_id: u8,
}

impl LSMTreeScope {
    pub(crate) fn new(bucket_range: RangeInclusive<u16>, column_family_id: u8) -> Self {
        Self {
            bucket_range,
            column_family_id,
        }
    }
}

#[derive(Clone, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct TruncationCursorId {
    pub(crate) bucket: u16,
    pub(crate) column_family_id: u8,
}

impl TruncationCursorId {
    pub(crate) fn new(bucket: u16, column_family_id: u8) -> Self {
        Self {
            bucket,
            column_family_id,
        }
    }
}

pub(crate) type TruncationCursorMap = HashMap<TruncationCursorId, Vec<u8>>;

#[derive(Clone)]
pub(crate) struct TruncationCursorStore {
    inner: Arc<TruncationCursorStoreInner>,
}

struct TruncationCursorStoreInner {
    current: RwLock<Arc<TruncationCursorMap>>,
    has_any: AtomicBool,
}

#[derive(Clone)]
pub(crate) struct TruncationCursorSnapshot {
    cursors: Arc<TruncationCursorMap>,
    has_any: bool,
}

impl TruncationCursorStore {
    fn new(cursors: TruncationCursorMap) -> Self {
        let has_any = !cursors.is_empty();
        Self {
            inner: Arc::new(TruncationCursorStoreInner {
                current: RwLock::new(Arc::new(cursors)),
                has_any: AtomicBool::new(has_any),
            }),
        }
    }

    pub(crate) fn get(&self, bucket: u16, column_family_id: u8) -> Option<Vec<u8>> {
        if !self.inner.has_any.load(Ordering::Acquire) {
            return None;
        }
        let id = TruncationCursorId::new(bucket, column_family_id);
        self.inner.current.read().unwrap().get(&id).cloned()
    }

    pub(crate) fn key_is_truncated(&self, bucket: u16, column_family_id: u8, key: &[u8]) -> bool {
        if !self.inner.has_any.load(Ordering::Acquire) {
            return false;
        }
        let id = TruncationCursorId::new(bucket, column_family_id);
        self.inner
            .current
            .read()
            .unwrap()
            .get(&id)
            .is_some_and(|cursor| key <= cursor.as_slice())
    }

    /// Captures a stable cursor view for snapshot materialization.
    ///
    /// Short readers only hold the read lock long enough to copy one cursor. Snapshot capture is
    /// the intentionally long-lived reader: it clones the current `Arc<HashMap<...>>` and then can
    /// outlive the lock. Writers use `Arc::get_mut` while holding the write lock. If no captured
    /// snapshot owns the map, the update is in-place; if a snapshot still references it, the writer
    /// clones the map and swaps in the updated copy. The `RwLock` is therefore just pointer
    /// protection, while the map `Arc` is what keeps stale snapshot views immutable without forcing
    /// every poll to clone the cursor map.
    pub(crate) fn capture(&self) -> TruncationCursorSnapshot {
        if !self.inner.has_any.load(Ordering::Acquire) {
            return TruncationCursorSnapshot {
                cursors: Arc::new(TruncationCursorMap::new()),
                has_any: false,
            };
        }
        TruncationCursorSnapshot {
            cursors: Arc::clone(&self.inner.current.read().unwrap()),
            has_any: true,
        }
    }

    pub(crate) fn to_map(&self) -> TruncationCursorMap {
        self.capture().to_map()
    }

    pub(crate) fn advance(&self, bucket: u16, column_family_id: u8, key: &[u8]) -> bool {
        let id = TruncationCursorId::new(bucket, column_family_id);
        let mut current = self.inner.current.write().unwrap();
        if current
            .get(&id)
            .is_some_and(|cursor| key <= cursor.as_slice())
        {
            return true;
        }

        if let Some(cursors) = Arc::get_mut(&mut *current) {
            cursors.insert(id, key.to_vec());
        } else {
            let mut next = (**current).clone();
            next.insert(id, key.to_vec());
            *current = Arc::new(next);
        }
        self.inner.has_any.store(true, Ordering::Release);
        true
    }
}

impl TruncationCursorSnapshot {
    pub(crate) fn as_map(&self) -> &TruncationCursorMap {
        self.cursors.as_ref()
    }

    pub(crate) fn to_map(&self) -> TruncationCursorMap {
        if !self.has_any {
            return TruncationCursorMap::new();
        }
        (*self.cursors).clone()
    }
}

pub(crate) fn new_truncation_cursors() -> TruncationCursorStore {
    new_truncation_cursors_with(HashMap::new())
}

pub(crate) fn new_truncation_cursors_with(cursors: TruncationCursorMap) -> TruncationCursorStore {
    TruncationCursorStore::new(cursors)
}

pub(crate) fn default_column_family_scopes(
    bucket_ranges: &[RangeInclusive<u16>],
) -> Vec<LSMTreeScope> {
    bucket_ranges
        .iter()
        .cloned()
        .map(|bucket_range| LSMTreeScope::new(bucket_range, DEFAULT_COLUMN_FAMILY_ID))
        .collect()
}

#[derive(Clone)]
pub(crate) struct MultiLSMTreeVersion {
    total_buckets: u32,
    bucket_to_tree_idx_by_cf: Vec<Vec<u32>>,
    tree_versions: Vec<TreeVersionEntry>,
}

impl MultiLSMTreeVersion {
    pub(crate) fn new(lsm_version: LSMTreeVersion) -> Self {
        Self {
            total_buckets: 0,
            bucket_to_tree_idx_by_cf: Vec::new(),
            tree_versions: vec![TreeVersionEntry {
                scope: LSMTreeScope::new(0u16..=0u16, DEFAULT_COLUMN_FAMILY_ID),
                lsm_version: Arc::new(lsm_version),
            }],
        }
    }

    pub(crate) fn from_scopes_with_tree_versions(
        total_buckets: u32,
        scopes: &[LSMTreeScope],
        lsm_versions: Vec<Arc<LSMTreeVersion>>,
    ) -> Result<Self> {
        if total_buckets == 0 || total_buckets > (u16::MAX as u32) + 1 {
            return Err(Error::ConfigError(
                "total_buckets must be in range 1..=65536".to_string(),
            ));
        }
        if scopes.is_empty() {
            return Err(Error::ConfigError(
                "tree scopes must not be empty".to_string(),
            ));
        }
        if lsm_versions.len() != scopes.len() {
            return Err(Error::InvalidState(format!(
                "LSM tree version count {} does not match tree scope count {}",
                lsm_versions.len(),
                scopes.len()
            )));
        }
        let bucket_slots = bucket_slots_for_total(total_buckets);
        let max_cf_id = scopes
            .iter()
            .map(|scope| usize::from(scope.column_family_id))
            .max()
            .unwrap_or(usize::from(DEFAULT_COLUMN_FAMILY_ID));
        let mut bucket_to_tree_idx_by_cf = vec![Vec::<u32>::new(); max_cf_id + 1];
        let mut tree_versions = Vec::with_capacity(scopes.len());
        for (tree_idx, scope) in scopes.iter().enumerate() {
            if usize::from(scope.column_family_id) >= MAX_COLUMN_FAMILY_COUNT {
                return Err(Error::ConfigError(format!(
                    "Invalid column family id {} in tree scope",
                    scope.column_family_id
                )));
            }
            if !bucket_range_fits_total(&scope.bucket_range, total_buckets) {
                return Err(Error::ConfigError(format!(
                    "Invalid bucket range {}..={} for total_buckets {} in column_family {}",
                    scope.bucket_range.start(),
                    scope.bucket_range.end(),
                    total_buckets,
                    scope.column_family_id
                )));
            }
            let Some(last_bucket) = bucket_range_last(&scope.bucket_range) else {
                return Err(Error::ConfigError(format!(
                    "Invalid bucket range {}..={} for total_buckets {} in column_family {}",
                    scope.bucket_range.start(),
                    scope.bucket_range.end(),
                    total_buckets,
                    scope.column_family_id
                )));
            };
            let cf_slot = usize::from(scope.column_family_id);
            let cf_bucket_to_tree_idx = &mut bucket_to_tree_idx_by_cf[cf_slot];
            if cf_bucket_to_tree_idx.is_empty() {
                *cf_bucket_to_tree_idx = vec![u32::MAX; bucket_slots];
            }
            let mut bucket = *scope.bucket_range.start();
            loop {
                let slot = &mut cf_bucket_to_tree_idx[bucket as usize];
                if *slot != u32::MAX {
                    return Err(Error::ConfigError(format!(
                        "Overlapping bucket range detected at bucket {} in column_family {}",
                        bucket, scope.column_family_id
                    )));
                }
                *slot = tree_idx as u32;
                if bucket == last_bucket {
                    break;
                }
                bucket = bucket.saturating_add(1);
            }
            tree_versions.push(TreeVersionEntry {
                scope: scope.clone(),
                lsm_version: Arc::clone(&lsm_versions[tree_idx]),
            });
        }
        Ok(Self {
            total_buckets,
            bucket_to_tree_idx_by_cf,
            tree_versions,
        })
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

    pub(crate) fn tree_index_for_bucket_and_column_family(
        &self,
        bucket: u16,
        column_family_id: u8,
    ) -> Option<usize> {
        if self.bucket_to_tree_idx_by_cf.is_empty() {
            return Some(0);
        }
        let bucket_to_tree_idx = self
            .bucket_to_tree_idx_by_cf
            .get(column_family_id as usize)
            .filter(|mapping| !mapping.is_empty())?;
        let tree_idx = *bucket_to_tree_idx.get(bucket as usize)?;
        if tree_idx == u32::MAX {
            return None;
        }
        Some(tree_idx as usize)
    }

    pub(crate) fn version_for_bucket_and_column_family(
        &self,
        bucket: u16,
        column_family_id: u8,
    ) -> Option<Arc<LSMTreeVersion>> {
        self.tree_index_for_bucket_and_column_family(bucket, column_family_id)
            .map(|tree_idx| self.version_of_index(tree_idx))
    }

    pub(crate) fn bucket_range_of_tree(&self, tree_idx: usize) -> Option<RangeInclusive<u16>> {
        self.tree_versions
            .get(tree_idx)
            .map(|entry| entry.scope.bucket_range.clone())
    }

    pub(crate) fn tree_scope_of_tree(&self, tree_idx: usize) -> Option<LSMTreeScope> {
        self.tree_versions
            .get(tree_idx)
            .map(|entry| entry.scope.clone())
    }

    pub(crate) fn bucket_ranges(&self) -> Vec<RangeInclusive<u16>> {
        self.tree_versions
            .iter()
            .map(|entry| entry.scope.bucket_range.clone())
            .collect()
    }

    pub(crate) fn tree_scopes(&self) -> Vec<LSMTreeScope> {
        self.tree_versions
            .iter()
            .map(|entry| entry.scope.clone())
            .collect()
    }

    pub(crate) fn tree_index_for_exact_range(&self, range: &RangeInclusive<u16>) -> Option<usize> {
        self.tree_index_for_exact_scope(&LSMTreeScope::new(range.clone(), DEFAULT_COLUMN_FAMILY_ID))
    }

    pub(crate) fn tree_index_for_exact_scope(&self, scope: &LSMTreeScope) -> Option<usize> {
        self.tree_versions
            .iter()
            .position(|entry| &entry.scope == scope)
    }

    pub(crate) fn with_lsm_version_at(
        &self,
        tree_idx: usize,
        lsm_version: Arc<LSMTreeVersion>,
    ) -> Self {
        let mut tree_versions = self.tree_versions.clone();
        if tree_versions.is_empty() {
            tree_versions.push(TreeVersionEntry {
                scope: LSMTreeScope::new(0u16..=0u16, DEFAULT_COLUMN_FAMILY_ID),
                lsm_version,
            });
        } else {
            let slot = tree_versions.get_mut(tree_idx).expect("Invalid tree index");
            slot.lsm_version = lsm_version;
        }
        Self {
            total_buckets: self.total_buckets,
            bucket_to_tree_idx_by_cf: self.bucket_to_tree_idx_by_cf.clone(),
            tree_versions,
        }
    }
}

/// Immutable snapshot of the entire database state at a point in time.
/// Swapped atomically via ArcSwap in DbStateHandle so readers see a
/// consistent view without locking. Each mutation (flush, compaction,
/// split) produces a new DbState and publishes it via CAS.
pub(crate) struct DbState {
    pub(crate) seq_id: u64,
    pub(crate) topology_epoch: u64,
    pub(crate) bucket_ranges: Vec<RangeInclusive<u16>>,
    pub(crate) multi_lsm_version: MultiLSMTreeVersion,
    pub(crate) vlog_version: VlogVersion,
    pub(crate) active: Option<Arc<RwLock<ActiveMemtable>>>,
    pub(crate) immutables: VecDeque<ImmutableMemtable>,
    pub(crate) truncation_cursors: TruncationCursorStore,
    // This is used to suggest a base snapshot ID for new snapshots
    pub(crate) suggested_base_snapshot_id: Option<u64>,
    /// The active table's schema is published alongside its pointer so source
    /// metadata never needs to lock the active table.
    pub(crate) active_schema: Option<Arc<Schema>>,
    /// Minimum physical source schema per column family across this snapshot.
    /// It is in-memory snapshot metadata, reconstructed from current refs.
    pub(crate) min_source_schema_by_cf: Vec<Option<u64>>,
}

impl DbState {
    pub(crate) fn truncation_cursor(&self, bucket: u16, column_family_id: u8) -> Option<Vec<u8>> {
        self.truncation_cursors.get(bucket, column_family_id)
    }

    pub(crate) fn key_is_truncated(&self, bucket: u16, column_family_id: u8, key: &[u8]) -> bool {
        self.truncation_cursors
            .key_is_truncated(bucket, column_family_id, key)
    }

    pub(crate) fn truncation_cursors_snapshot(&self) -> TruncationCursorMap {
        self.truncation_cursors.to_map()
    }

    pub(crate) fn min_source_schema_id(&self, column_family_id: u8) -> Option<u64> {
        self.min_source_schema_by_cf
            .get(column_family_id as usize)
            .copied()
            .flatten()
    }

    pub(crate) fn scan_requires_schema_aware(
        &self,
        target_schema: &Schema,
        column_family_id: u8,
    ) -> bool {
        self.min_source_schema_id(column_family_id)
            .is_some_and(|source_min| {
                target_schema
                    .last_incompatible_schema_id_in_family(column_family_id)
                    .is_some_and(|barrier| source_min < barrier)
            })
    }

    fn refresh_source_schema_metadata(&mut self) {
        fn record(minima: &mut Vec<Option<u64>>, column_family_id: u8, schema_id: u64) {
            let index = column_family_id as usize;
            if minima.len() <= index {
                minima.resize(index + 1, None);
            }
            minima[index] = Some(minima[index].map_or(schema_id, |current| current.min(schema_id)));
        }
        fn record_schema(minima: &mut Vec<Option<u64>>, schema: &Schema) {
            for column_family_id in schema.column_family_id_list() {
                record(minima, column_family_id, schema.version());
            }
        }
        let mut minima = Vec::<Option<u64>>::new();
        if let Some(schema) = &self.active_schema {
            record_schema(&mut minima, schema);
        }
        for immutable in &self.immutables {
            record_schema(&mut minima, immutable.schema());
        }
        for entry in &self.multi_lsm_version.tree_versions {
            for level in &entry.lsm_version.levels {
                for file in &level.files {
                    record(&mut minima, entry.scope.column_family_id, file.schema_id);
                }
            }
        }
        self.min_source_schema_by_cf = minima;
    }
}

/// Thread-safe handle for reading and mutating DbState.
/// Uses ArcSwap for lock-free reads (`load()`) and a Mutex for
/// serializing state mutations (flush apply, compaction apply, split).
pub(crate) struct DbStateHandle {
    current: ArcSwap<DbState>,
    lock: Mutex<()>,
    next_seq_id: AtomicU64,
    changed: Arc<Condvar>,
}

impl DbStateHandle {
    pub(crate) fn new() -> Self {
        Self {
            current: ArcSwap::from_pointee(DbState {
                seq_id: 0,
                topology_epoch: 0,
                bucket_ranges: Vec::new(),
                multi_lsm_version: MultiLSMTreeVersion::new(LSMTreeVersion { levels: vec![] }),
                vlog_version: VlogVersion::new(),
                active: None,
                active_schema: None,
                immutables: VecDeque::new(),
                min_source_schema_by_cf: Vec::new(),
                truncation_cursors: new_truncation_cursors(),
                suggested_base_snapshot_id: None,
            }),
            lock: Mutex::new(()),
            next_seq_id: AtomicU64::new(1),
            changed: Arc::new(Condvar::new()),
        }
    }

    pub(crate) fn allocate_seq_id(&self) -> u64 {
        self.next_seq_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn advance_next_seq_id(&self, min_next_seq_id: u64) {
        self.next_seq_id
            .fetch_max(min_next_seq_id, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn next_seq_id(&self) -> u64 {
        self.next_seq_id.load(std::sync::atomic::Ordering::SeqCst)
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

    /// Notifies all waiters on the changed condvar without actually changing state.
    /// Used to wake up write-stall waiters when the DB enters an error state.
    pub(crate) fn notify_changed(&self) {
        self.changed.notify_all();
    }

    pub(crate) fn changed_condvar(&self) -> &Arc<Condvar> {
        &self.changed
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
                topology_epoch: snapshot.topology_epoch,
                bucket_ranges: snapshot.bucket_ranges.clone(),
                multi_lsm_version: snapshot.multi_lsm_version.clone(),
                vlog_version: snapshot.vlog_version.clone(),
                active: snapshot.active.clone(),
                active_schema: snapshot.active_schema.clone(),
                immutables: snapshot.immutables.clone(),
                min_source_schema_by_cf: Vec::new(),
                truncation_cursors: snapshot.truncation_cursors.clone(),
                suggested_base_snapshot_id: Some(snapshot_id),
            })
        })
    }

    pub(crate) fn rebase_suggested_snapshot(
        &self,
        snapshot_id: u64,
        replacement: Option<u64>,
    ) -> bool {
        let _guard = self.lock();
        let current = self.load();
        if current.suggested_base_snapshot_id != Some(snapshot_id) {
            return true;
        }
        self.store(DbState {
            seq_id: current.seq_id,
            topology_epoch: current.topology_epoch,
            bucket_ranges: current.bucket_ranges.clone(),
            multi_lsm_version: current.multi_lsm_version.clone(),
            vlog_version: current.vlog_version.clone(),
            active: current.active.clone(),
            active_schema: current.active_schema.clone(),
            immutables: current.immutables.clone(),
            min_source_schema_by_cf: Vec::new(),
            truncation_cursors: current.truncation_cursors.clone(),
            suggested_base_snapshot_id: replacement,
        });
        true
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
        let scopes = default_column_family_scopes(bucket_ranges);
        let multi_lsm_version = MultiLSMTreeVersion::from_scopes_with_tree_versions(
            total_buckets,
            &scopes,
            tree_versions,
        )?;
        self.store(DbState {
            seq_id: snapshot.seq_id,
            topology_epoch: snapshot.topology_epoch.saturating_add(1),
            bucket_ranges: bucket_ranges.to_vec(),
            multi_lsm_version,
            vlog_version: snapshot.vlog_version.clone(),
            active: snapshot.active.clone(),
            active_schema: snapshot.active_schema.clone(),
            immutables: snapshot.immutables.clone(),
            min_source_schema_by_cf: Vec::new(),
            truncation_cursors: snapshot.truncation_cursors.clone(),
            suggested_base_snapshot_id: None,
        });
        Ok(())
    }

    pub(crate) fn advance_truncation_cursor(
        &self,
        bucket: u16,
        column_family_id: u8,
        key: &[u8],
    ) -> bool {
        let _guard = self.lock();
        let snapshot = self.load();
        let id = TruncationCursorId::new(bucket, column_family_id);
        let mut cursors = snapshot.truncation_cursors_snapshot();
        if cursors
            .get(&id)
            .is_some_and(|current| key <= current.as_slice())
        {
            return true;
        }
        cursors.insert(id, key.to_vec());
        self.store(DbState {
            seq_id: self.allocate_seq_id(),
            topology_epoch: snapshot.topology_epoch,
            bucket_ranges: snapshot.bucket_ranges.clone(),
            multi_lsm_version: snapshot.multi_lsm_version.clone(),
            vlog_version: snapshot.vlog_version.clone(),
            active: snapshot.active.clone(),
            active_schema: snapshot.active_schema.clone(),
            immutables: snapshot.immutables.clone(),
            min_source_schema_by_cf: Vec::new(),
            truncation_cursors: new_truncation_cursors_with(cursors),
            suggested_base_snapshot_id: snapshot.suggested_base_snapshot_id,
        });
        true
    }

    pub(crate) fn store(&self, mut new_version: DbState) {
        new_version.refresh_source_schema_metadata();
        // Publish the allocator advance before the state itself. A thread that observes the new
        // state must never allocate its sequence id (or an older one), otherwise stale flush or
        // compaction work could pass a CAS after restore. Advancing early may leave harmless gaps
        // when stores race, while publishing the state first would allow ABA.
        let min_next_seq_id = new_version
            .seq_id
            .checked_add(1)
            .expect("DbState sequence id exhausted");
        self.advance_next_seq_id(min_next_seq_id);
        self.current.store(Arc::new(new_version));
        self.changed.notify_all();
    }

    /// Removes an immutable memtable by id from the current state.
    /// Used in flush error paths so the memtable can be fully dropped,
    /// triggering the reclaimer which notifies `buffer_ready`.
    pub(crate) fn remove_immutable(&self, memtable_id: Uuid) {
        let _guard = self.lock();
        let snapshot = self.load();
        let mut immutables = snapshot.immutables.clone();
        let before = immutables.len();
        immutables.retain(|imm| imm.id != memtable_id);
        if immutables.len() == before {
            return;
        }
        self.store(DbState {
            seq_id: self.allocate_seq_id(),
            topology_epoch: snapshot.topology_epoch,
            bucket_ranges: snapshot.bucket_ranges.clone(),
            multi_lsm_version: snapshot.multi_lsm_version.clone(),
            vlog_version: snapshot.vlog_version.clone(),
            active: snapshot.active.clone(),
            active_schema: snapshot.active_schema.clone(),
            immutables,
            min_source_schema_by_cf: Vec::new(),
            truncation_cursors: snapshot.truncation_cursors.clone(),
            suggested_base_snapshot_id: snapshot.suggested_base_snapshot_id,
        });
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

#[cfg(test)]
#[path = "../tests/unit/db_state.rs"]
mod tests;
