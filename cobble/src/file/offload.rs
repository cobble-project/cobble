use super::file_manager::{FileId, PhysicalDeletePolicy};
use crate::Error;
use crate::config::PrimaryVolumeOffloadPolicyKind;
use crate::data_file::{DataFile, DataFileType};
use crate::db_state::DbStateHandle;
use crate::file::logical_file::{ReplicaLifecycle, ReplicaOrigin};
use crate::file::{DataVolume, FileManager, PrimaryDataPlacement, TrackedFile, TrackedWriter};
use crate::sst::PinnedSstReadMetadata;
use dashmap::{DashMap, Entry};
use log::warn;
use rand::random;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{Notify, Semaphore, TryAcquireError};

const PRIMARY_TIERING_SCAN_INTERVAL: Duration = Duration::from_secs(1);
const MAX_BACKFILL_TRIGGER_WATERMARK: f64 = 0.80;
const MIN_OFFLOAD_BACKFILL_GAP: f64 = 0.01;

fn effective_backfill_trigger_watermark(requested: f64, offload_trigger: f64) -> f64 {
    requested
        .min(MAX_BACKFILL_TRIGGER_WATERMARK)
        .min((offload_trigger - MIN_OFFLOAD_BACKFILL_GAP).max(0.0))
}

fn projected_source_release_bytes(source: &TrackedFile) -> u64 {
    if source.physical_delete_policy() == PhysicalDeletePolicy::ManagedDelete
        && source.explicit_refs.load(AtomicOrdering::SeqCst) == 0
    {
        source.size_bytes()
    } else {
        0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VolumePressure {
    pub(crate) priority_rank: u8,
    pub(crate) used_bytes: u64,
    pub(crate) size_limit: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PrimaryOffloadFileRef {
    pub(crate) file_id: FileId,
    pub(crate) size_bytes: u64,
    pub(crate) priority: u8,
}

pub(crate) trait PrimaryOffloadPolicy: Send + Sync {
    fn compare_file_refs(
        &self,
        left: &PrimaryOffloadFileRef,
        right: &PrimaryOffloadFileRef,
    ) -> Ordering;

    fn sort_file_refs(&self, refs: &mut [PrimaryOffloadFileRef]) {
        refs.sort_by(|left, right| self.compare_file_refs(left, right));
    }

    fn select_candidate(
        &self,
        candidates: &[(FileId, Arc<TrackedFile>)],
        _source: &VolumePressure,
        _target: &VolumePressure,
    ) -> Option<FileId> {
        let mut refs: Vec<PrimaryOffloadFileRef> = candidates
            .iter()
            .map(|candidate| PrimaryOffloadFileRef {
                file_id: candidate.0,
                size_bytes: candidate
                    .1
                    .size_bytes
                    .load(std::sync::atomic::Ordering::SeqCst),
                priority: candidate.1.priority(),
            })
            .collect();
        self.sort_file_refs(&mut refs);
        refs.first().map(|item| item.file_id)
    }
}

type OffloadJobFn = dyn Fn(FileId) + Send + Sync + 'static;

fn primary_tiering_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        let worker_threads = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(1)
            .min(4);
        Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("cobble-tiering")
            .enable_all()
            .build()
            .expect("failed to build primary tiering runtime")
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrimaryTieringDirection {
    Offload,
    Backfill,
    ReadonlyLoad,
    Adoption,
}

enum PrimaryMoveGuard {
    Backfill {
        db_state: std::sync::Weak<DbStateHandle>,
        max_target_used_bytes: u64,
    },
    ReadonlyLoad {
        db_state: std::sync::Weak<DbStateHandle>,
        max_target_used_bytes: u64,
        pin_metadata: bool,
        pin_partitions: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrimaryMoveGuardStatus {
    Valid,
    Retry,
    Stale,
}

#[derive(Clone)]
struct OffloadJobPlan {
    source_volume: Arc<DataVolume>,
    target_volume: Arc<DataVolume>,
    reserved_incoming_bytes: u64,
    projected_source_release_bytes: u64,
    copied_bytes: Arc<AtomicU64>,
    direction: PrimaryTieringDirection,
}

/// Immutable scheduling data for one READONLY file promotion.
///
/// The pin decision is made while traversing LSM levels, where level ordinal is available. The
/// worker must not infer it from file priority because one physical file may be referenced by
/// more than one logical level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReadonlyLoadRequest {
    priority: u8,
    pin_metadata: bool,
    pin_partitions: bool,
}

impl ReadonlyLoadRequest {
    fn merge(&mut self, other: Self) {
        self.priority = self.priority.max(other.priority);
        self.pin_metadata |= other.pin_metadata;
        self.pin_partitions = (self.pin_partitions || other.pin_partitions) && self.pin_metadata;
    }
}

pub(crate) struct OffloadRuntime {
    accepting: AtomicBool,
    semaphore: Arc<Semaphore>,
    planned_jobs: Arc<DashMap<FileId, OffloadJobPlan>>,
    pending_readonly_loads: DashMap<FileId, ReadonlyLoadRequest>,
    lifecycle: Mutex<()>,
    planning: Mutex<()>,
    idle: Arc<Mutex<()>>,
    done: Arc<Condvar>,
    primary_volume_by_rank: HashMap<u8, Vec<Arc<DataVolume>>>,
    policy: Arc<dyn PrimaryOffloadPolicy>,
}

impl Default for OffloadRuntime {
    fn default() -> Self {
        Self::new(&[])
    }
}

impl OffloadRuntime {
    pub(crate) fn new(data_volumes: &[Arc<DataVolume>]) -> Self {
        Self::new_with_policy_kind(data_volumes, PrimaryVolumeOffloadPolicyKind::Priority, 4)
    }

    pub(crate) fn new_with_policy_kind(
        data_volumes: &[Arc<DataVolume>],
        policy_kind: PrimaryVolumeOffloadPolicyKind,
        concurrency: usize,
    ) -> Self {
        let policy = policy_from_kind_arc(policy_kind);
        Self::new_with_policy(data_volumes, policy, concurrency)
    }

    fn new_with_policy(
        data_volumes: &[Arc<DataVolume>],
        policy: Arc<dyn PrimaryOffloadPolicy>,
        concurrency: usize,
    ) -> Self {
        let mut primary_volume_by_rank = HashMap::new();
        for volume in data_volumes {
            if !volume.supports_primary_data {
                continue;
            }
            let rank = volume.priority.rank();
            primary_volume_by_rank
                .entry(rank)
                .or_insert_with(Vec::new)
                .push(Arc::clone(volume));
        }
        Self {
            accepting: AtomicBool::new(true),
            semaphore: Arc::new(Semaphore::new(concurrency.max(1))),
            planned_jobs: Arc::new(DashMap::new()),
            pending_readonly_loads: DashMap::new(),
            lifecycle: Mutex::new(()),
            planning: Mutex::new(()),
            idle: Arc::new(Mutex::new(())),
            done: Arc::new(Condvar::new()),
            primary_volume_by_rank,
            policy,
        }
    }

    pub(crate) fn primary_volume_by_rank(&self, rank: u8) -> Option<Arc<DataVolume>> {
        self.primary_volume_by_rank
            .get(&rank)
            .and_then(|volumes| volumes.first())
            .map(Arc::clone)
    }

    /// Shares the database-wide background transfer budget with snapshot uploads.
    pub(crate) fn transfer_semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.semaphore)
    }

    pub(crate) fn select_lower_priority_primary_volume(
        &self,
        source_priority_rank: u8,
        write_stop_watermark: f64,
    ) -> Option<Arc<DataVolume>> {
        if source_priority_rank <= 1 {
            return None;
        }
        for rank in (1..source_priority_rank).rev() {
            if let Some(volumes) = self.primary_volume_by_rank.get(&rank) {
                for volume in volumes {
                    let projected_used = self.projected_target_physical_bytes(volume);
                    let write_stopped = volume.size_limit.is_some_and(|limit| {
                        limit == 0 || projected_used as f64 / limit as f64 >= write_stop_watermark
                    });
                    if !write_stopped {
                        return Some(Arc::clone(volume));
                    }
                }
            }
        }
        None
    }

    pub(crate) fn select_candidate(
        &self,
        candidates: &[(FileId, Arc<TrackedFile>)],
        source: &VolumePressure,
        target: &VolumePressure,
    ) -> Option<FileId> {
        self.policy.select_candidate(candidates, source, target)
    }

    fn complete_job(&self, file_id: FileId) {
        if let Some((_, plan)) = self.planned_jobs.remove(&file_id) {
            plan.source_volume
                .subtract_projected_offload_bytes(plan.projected_source_release_bytes);
        }
        if self.planned_jobs.is_empty() {
            self.done.notify_all();
        }
    }

    fn is_queued_or_running(&self, file_id: FileId) -> bool {
        self.planned_jobs.contains_key(&file_id)
    }

    fn is_adoption_job(&self, file_id: FileId) -> bool {
        self.planned_jobs
            .get(&file_id)
            .is_some_and(|plan| plan.direction == PrimaryTieringDirection::Adoption)
    }

    fn projected_source_offload_bytes(&self, source_volume: &Arc<DataVolume>) -> u64 {
        source_volume.projected_offload_bytes()
    }

    fn projected_target_incoming_bytes(&self, target_volume: &Arc<DataVolume>) -> u64 {
        self.planned_jobs
            .iter()
            .filter(|entry| Arc::ptr_eq(&entry.value().target_volume, target_volume))
            .map(|entry| {
                entry
                    .value()
                    .reserved_incoming_bytes
                    .saturating_sub(entry.value().copied_bytes.load(AtomicOrdering::SeqCst))
            })
            .sum()
    }

    fn projected_target_physical_bytes(&self, target_volume: &Arc<DataVolume>) -> u64 {
        target_volume
            .used_bytes
            .load(AtomicOrdering::SeqCst)
            .saturating_add(self.projected_target_incoming_bytes(target_volume))
    }

    fn record_copy_progress(&self, file_id: FileId, copied_bytes: u64) {
        let Some(plan) = self.planned_jobs.get(&file_id) else {
            return;
        };
        let mut current = plan.copied_bytes.load(AtomicOrdering::SeqCst);
        loop {
            let next = current
                .saturating_add(copied_bytes)
                .min(plan.reserved_incoming_bytes);
            match plan.copied_bytes.compare_exchange(
                current,
                next,
                AtomicOrdering::SeqCst,
                AtomicOrdering::SeqCst,
            ) {
                Ok(_) => return,
                Err(updated) => current = updated,
            }
        }
    }

    fn reset_copy_progress(&self, file_id: FileId) {
        if let Some(plan) = self.planned_jobs.get(&file_id) {
            plan.copied_bytes.store(0, AtomicOrdering::SeqCst);
        }
    }

    fn has_offload_jobs(&self) -> bool {
        self.planned_jobs
            .iter()
            .any(|entry| entry.value().direction == PrimaryTieringDirection::Offload)
    }

    fn mark_readonly_load(&self, file_id: FileId, request: ReadonlyLoadRequest) {
        self.pending_readonly_loads
            .entry(file_id)
            .and_modify(|current| current.merge(request))
            .or_insert(request);
    }

    fn complete_readonly_load(&self, file_id: FileId) {
        self.pending_readonly_loads.remove(&file_id);
    }

    fn has_available_worker_slot(&self) -> bool {
        self.accepting.load(AtomicOrdering::Acquire) && self.semaphore.available_permits() != 0
    }

    fn schedule(
        self: &Arc<Self>,
        file_id: FileId,
        plan: OffloadJobPlan,
        handler: Arc<OffloadJobFn>,
        file_manager_keepalive: Option<Arc<FileManager>>,
    ) -> Result<bool, String> {
        let _lifecycle = self.lifecycle.lock().unwrap();
        if !self.accepting.load(AtomicOrdering::Acquire) {
            return Err("Primary tiering runtime is stopped".to_string());
        }
        let permit = match Arc::clone(&self.semaphore).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => return Ok(false),
            Err(TryAcquireError::Closed) => {
                return Err("Primary tiering runtime is stopped".to_string());
            }
        };
        match self.planned_jobs.entry(file_id) {
            Entry::Occupied(_) => {
                return Ok(false);
            }
            Entry::Vacant(vacant) => {
                plan.source_volume
                    .add_projected_offload_bytes(plan.projected_source_release_bytes);
                vacant.insert(plan);
            }
        }
        let runtime = Arc::clone(self);
        primary_tiering_runtime().spawn(async move {
            let result = tokio::task::spawn_blocking(move || handler(file_id)).await;
            if let Err(err) = result {
                warn!("primary tiering task failed for file_id={file_id}: {err}");
            }
            runtime.complete_job(file_id);
            drop(permit);
            drop(file_manager_keepalive);
        });
        Ok(true)
    }

    pub(crate) fn wait_idle(&self, timeout: Duration) -> bool {
        if self.planned_jobs.is_empty() {
            return true;
        }
        let guard = self.idle.lock().unwrap();
        let (guard, _) = self
            .done
            .wait_timeout_while(guard, timeout, |_| !self.planned_jobs.is_empty())
            .unwrap();
        drop(guard);
        self.planned_jobs.is_empty()
    }

    pub(crate) fn stop(&self) {
        {
            let _lifecycle = self.lifecycle.lock().unwrap();
            self.accepting.store(false, AtomicOrdering::Release);
        }
        let guard = self.idle.lock().unwrap();
        let guard = self
            .done
            .wait_while(guard, |_| !self.planned_jobs.is_empty())
            .unwrap();
        drop(guard);
    }
}

impl Drop for OffloadRuntime {
    fn drop(&mut self) {
        self.accepting.store(false, AtomicOrdering::Release);
        if !self.planned_jobs.is_empty() {
            warn!(
                "primary tiering runtime dropped with {} unfinished jobs",
                self.planned_jobs.len()
            );
        }
    }
}

pub(crate) struct PrimaryTieringWorkerHandle {
    stop: Arc<AtomicBool>,
    wake: Arc<Notify>,
    finished: Arc<(Mutex<bool>, Condvar)>,
}

impl PrimaryTieringWorkerHandle {
    fn start(
        file_manager: &Arc<FileManager>,
        db_state: &Arc<DbStateHandle>,
        adoption_tick: Option<Arc<dyn Fn() -> crate::Result<()> + Send + Sync>>,
    ) -> crate::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(Notify::new());
        let finished = Arc::new((Mutex::new(false), Condvar::new()));
        let weak_file_manager = Arc::downgrade(file_manager);
        let weak_db_state = Arc::downgrade(db_state);
        let stop_for_worker = Arc::clone(&stop);
        let wake_for_worker = Arc::clone(&wake);
        let worker = primary_tiering_runtime().spawn(async move {
            while !stop_for_worker.load(AtomicOrdering::Acquire) {
                let Some(file_manager) = weak_file_manager.upgrade() else {
                    break;
                };
                let Some(db_state) = weak_db_state.upgrade() else {
                    break;
                };
                if let Err(err) = tokio::task::block_in_place(|| {
                    file_manager.trigger_primary_tiering_if_needed(&db_state)
                }) {
                    warn!("primary volume tiering scan failed: {}", err);
                }
                if let Some(tick) = &adoption_tick
                    && let Err(err) = tokio::task::block_in_place(|| tick())
                {
                    warn!("external adoption scan failed: {err}");
                }
                drop(db_state);
                drop(file_manager);

                tokio::select! {
                    _ = tokio::time::sleep(PRIMARY_TIERING_SCAN_INTERVAL) => {}
                    _ = wake_for_worker.notified() => {}
                }
            }
        });
        let finished_for_watcher = Arc::clone(&finished);
        primary_tiering_runtime().spawn(async move {
            if let Err(err) = worker.await {
                warn!("primary volume tiering scan task failed: {err}");
            }
            let (lock, condvar) = finished_for_watcher.as_ref();
            *lock.lock().unwrap() = true;
            condvar.notify_all();
        });
        Ok(Self {
            stop,
            wake,
            finished,
        })
    }

    pub(crate) fn stop(&self) {
        self.stop.store(true, AtomicOrdering::Release);
        self.wake.notify_one();
    }

    pub(crate) fn wake(&self) {
        self.wake.notify_one();
    }

    pub(crate) fn join(&self) {
        let (lock, condvar) = self.finished.as_ref();
        let guard = lock.lock().unwrap();
        let guard = condvar.wait_while(guard, |finished| !*finished).unwrap();
        drop(guard);
    }
}

impl Drop for PrimaryTieringWorkerHandle {
    fn drop(&mut self) {
        self.stop();
        self.join();
    }
}

// ----------------------------
// Offload policies
// ----------------------------
static LARGEST_FILE_OFFLOAD_POLICY: LargestFileOffloadPolicy = LargestFileOffloadPolicy;
static PRIORITY_OFFLOAD_POLICY: PriorityOffloadPolicy = PriorityOffloadPolicy;

fn policy_from_kind_arc(
    policy_kind: PrimaryVolumeOffloadPolicyKind,
) -> Arc<dyn PrimaryOffloadPolicy> {
    match policy_kind {
        PrimaryVolumeOffloadPolicyKind::LargestFile => Arc::new(LargestFileOffloadPolicy),
        PrimaryVolumeOffloadPolicyKind::Priority => Arc::new(PriorityOffloadPolicy),
    }
}

fn policy_from_kind_ref(
    policy_kind: PrimaryVolumeOffloadPolicyKind,
) -> &'static dyn PrimaryOffloadPolicy {
    match policy_kind {
        PrimaryVolumeOffloadPolicyKind::LargestFile => &LARGEST_FILE_OFFLOAD_POLICY,
        PrimaryVolumeOffloadPolicyKind::Priority => &PRIORITY_OFFLOAD_POLICY,
    }
}

pub(crate) fn compare_primary_offload_file_refs(
    policy_kind: PrimaryVolumeOffloadPolicyKind,
    left: &PrimaryOffloadFileRef,
    right: &PrimaryOffloadFileRef,
) -> Ordering {
    policy_from_kind_ref(policy_kind).compare_file_refs(left, right)
}

#[derive(Default)]
pub(crate) struct LargestFileOffloadPolicy;

impl PrimaryOffloadPolicy for LargestFileOffloadPolicy {
    fn compare_file_refs(
        &self,
        left: &PrimaryOffloadFileRef,
        right: &PrimaryOffloadFileRef,
    ) -> Ordering {
        right
            .size_bytes
            .cmp(&left.size_bytes)
            .then_with(|| left.file_id.cmp(&right.file_id))
    }
}

#[derive(Default)]
pub(crate) struct PriorityOffloadPolicy;

impl PrimaryOffloadPolicy for PriorityOffloadPolicy {
    fn compare_file_refs(
        &self,
        left: &PrimaryOffloadFileRef,
        right: &PrimaryOffloadFileRef,
    ) -> Ordering {
        left.priority
            .cmp(&right.priority)
            .then_with(|| right.size_bytes.cmp(&left.size_bytes))
            .then_with(|| left.file_id.cmp(&right.file_id))
    }
}

// ----------------------------
// File Manager offload logic
// ----------------------------
impl FileManager {
    pub(crate) fn create_untracked_data_file_writer_on_volume(
        &self,
        volume: &Arc<DataVolume>,
    ) -> crate::Result<(TrackedWriter, Arc<TrackedFile>)> {
        let tracked = Arc::new(TrackedFile::managed(
            self.data_file_path(0),
            Arc::clone(volume.fs()),
            Some(Arc::clone(volume)),
        ));
        let writer = volume.fs().open_write(tracked.path())?;
        Ok((TrackedWriter::new(writer, Arc::clone(&tracked)), tracked))
    }

    pub(crate) fn stop_offload_worker(&self) {
        self.offload_runtime.stop();
    }

    pub(crate) fn start_primary_tiering_worker(
        self: &Arc<Self>,
        db_state: &Arc<DbStateHandle>,
        adoption_tick: Option<Arc<dyn Fn() -> crate::Result<()> + Send + Sync>>,
    ) -> crate::Result<Option<PrimaryTieringWorkerHandle>> {
        PrimaryTieringWorkerHandle::start(self, db_state, adoption_tick).map(Some)
    }

    pub(crate) fn trigger_offload_if_needed(self: &Arc<Self>) -> crate::Result<usize> {
        let _planning = self.offload_runtime.planning.lock().unwrap();
        self.trigger_offload_if_needed_locked()
    }

    fn trigger_offload_if_needed_locked(self: &Arc<Self>) -> crate::Result<usize> {
        if !self.offload_runtime.has_available_worker_slot() {
            return Ok(0);
        }
        let mut scheduled = 0usize;
        for source_volume in &self.data_volumes {
            if !source_volume.supports_primary_data {
                continue;
            }
            let Some(size_limit) = source_volume.size_limit else {
                continue;
            };
            let source_rank = source_volume.priority.rank();
            let trigger_used_bytes = (size_limit as f64
                * self.options.primary_volume_offload_trigger_watermark)
                .ceil() as u64;
            let already_planned = self
                .offload_runtime
                .projected_source_offload_bytes(source_volume);
            let mut projected_used = self
                .offload_runtime
                .projected_target_physical_bytes(source_volume)
                .saturating_sub(already_planned);
            if projected_used < trigger_used_bytes {
                continue;
            }
            let mut attempted = HashSet::new();
            while projected_used > trigger_used_bytes
                && self.offload_runtime.has_available_worker_slot()
            {
                let Some(target_volume) =
                    self.offload_runtime.select_lower_priority_primary_volume(
                        source_rank,
                        self.options.primary_volume_write_stop_watermark,
                    )
                else {
                    break;
                };
                if Arc::ptr_eq(source_volume, &target_volume) {
                    break;
                }
                let Some(file_id) = self.select_offload_candidate_with_exclusions(
                    source_volume,
                    &target_volume,
                    &attempted,
                ) else {
                    break;
                };
                attempted.insert(file_id);
                let estimated_bytes = self
                    .preferred_tracked_file(file_id)
                    .map(|tracked| tracked.size_bytes())
                    .unwrap_or(0);
                if estimated_bytes == 0 {
                    break;
                }
                if self.schedule_offload_move(file_id, &target_volume)? {
                    scheduled += 1;
                    projected_used = self
                        .offload_runtime
                        .projected_target_physical_bytes(source_volume)
                        .saturating_sub(
                            self.offload_runtime
                                .projected_source_offload_bytes(source_volume),
                        );
                    continue;
                }
                continue;
            }
        }
        Ok(scheduled)
    }

    pub(crate) fn trigger_primary_tiering_if_needed(
        self: &Arc<Self>,
        db_state: &Arc<DbStateHandle>,
    ) -> crate::Result<usize> {
        let _planning = self.offload_runtime.planning.lock().unwrap();
        if !self.offload_runtime.has_available_worker_slot() {
            return Ok(0);
        }
        let offloaded = self.trigger_offload_if_needed_locked()?;
        if offloaded != 0 || self.offload_runtime.has_offload_jobs() {
            return Ok(offloaded);
        }
        let cache_moves = self.move_persistent_caches_if_needed_locked(db_state)?;
        if cache_moves != 0 || self.offload_runtime.has_offload_jobs() {
            return Ok(cache_moves);
        }
        let evicted = self.evict_persistent_caches_if_needed_locked(db_state)?;
        if evicted != 0 {
            return Ok(evicted);
        }
        let cached = self.schedule_referenced_persistent_caches(db_state)?;
        if cached != 0 || self.offload_runtime.has_offload_jobs() {
            return Ok(cached);
        }
        let loaded = self.trigger_readonly_loads_locked(db_state)?;
        if loaded != 0 {
            return Ok(loaded);
        }
        self.trigger_backfill_if_needed_locked(db_state)
    }

    pub(crate) fn mark_readonly_files_for_primary_load(
        &self,
        db_state: &DbStateHandle,
        pinned_metadata_max_level: Option<u8>,
        pin_metadata_partitions: bool,
    ) -> usize {
        let referenced_files = referenced_readonly_load_requests(
            db_state,
            pinned_metadata_max_level,
            pin_metadata_partitions,
        );
        let mut marked = 0usize;
        for (file_id, request) in referenced_files {
            let is_readonly = self.preferred_tracked_file(file_id).is_some_and(|tracked| {
                tracked
                    .volume
                    .as_ref()
                    .is_some_and(|volume| volume.readonly_source)
            });
            if is_readonly {
                self.offload_runtime.mark_readonly_load(file_id, request);
                marked += 1;
            }
        }
        marked
    }

    fn trigger_readonly_loads_locked(
        self: &Arc<Self>,
        db_state: &Arc<DbStateHandle>,
    ) -> crate::Result<usize> {
        if !self.offload_runtime.has_available_worker_slot() {
            return Ok(0);
        }
        let referenced_priorities = referenced_primary_file_priorities(db_state);
        let vlog_file_ids = referenced_vlog_file_ids(db_state);
        let mut stale = Vec::new();
        let mut candidates = Vec::new();
        for entry in &self.offload_runtime.pending_readonly_loads {
            let file_id = *entry.key();
            if !referenced_priorities.contains_key(&file_id) {
                stale.push(file_id);
                continue;
            }
            let request = *entry.value();
            if self.offload_runtime.is_queued_or_running(file_id) {
                continue;
            }
            let Some((size_bytes, true)) =
                self.preferred_tracked_file(file_id).and_then(|tracked| {
                    tracked
                        .volume
                        .as_ref()
                        .map(|volume| (tracked.size_bytes(), volume.readonly_source))
                })
            else {
                stale.push(file_id);
                continue;
            };
            if size_bytes == 0 {
                continue;
            }
            let placement = if vlog_file_ids.contains(&file_id) {
                PrimaryDataPlacement::Vlog
            } else {
                PrimaryDataPlacement::Standard
            };
            candidates.push((file_id, request, size_bytes, placement));
        }
        for file_id in stale {
            self.offload_runtime.complete_readonly_load(file_id);
        }
        candidates.sort_by(|left, right| {
            right
                .1
                .priority
                .cmp(&left.1.priority)
                .then_with(|| left.0.cmp(&right.0))
        });

        let mut scheduled = 0usize;
        for (file_id, request, size_bytes, placement) in candidates {
            if !self.offload_runtime.has_available_worker_slot() {
                break;
            }
            let Some(target_volume) =
                self.select_readonly_load_target(file_id, size_bytes, placement)
            else {
                continue;
            };
            if self.schedule_readonly_load_move(file_id, &target_volume, db_state, request)? {
                scheduled += 1;
            }
        }
        Ok(scheduled)
    }

    fn select_readonly_load_target(
        &self,
        file_id: FileId,
        size_bytes: u64,
        placement: PrimaryDataPlacement,
    ) -> Option<Arc<DataVolume>> {
        self.select_primary_tiering_target(placement, |volume| {
            let reserved_incoming_bytes =
                if self.has_ready_replica_on_primary_volume(file_id, volume) {
                    0
                } else {
                    size_bytes
                };
            let max_target_used_bytes = self.max_readonly_load_target_used_bytes(volume);
            self.offload_runtime
                .projected_target_physical_bytes(volume)
                .saturating_add(reserved_incoming_bytes)
                <= max_target_used_bytes
        })
    }

    fn select_adoption_target(
        &self,
        size_bytes: u64,
        placement: PrimaryDataPlacement,
    ) -> Option<Arc<DataVolume>> {
        self.select_primary_tiering_target(placement, |volume| {
            self.offload_runtime
                .projected_target_physical_bytes(volume)
                .saturating_add(size_bytes)
                <= self.max_readonly_load_target_used_bytes(volume)
        })
    }

    /// Chooses a background copy target under the same policy as synchronous primary placement.
    ///
    /// Standard files retain the historical highest-priority eligible target. When direct VLOG
    /// placement is enabled, VLOG copies are limited to the lowest writable primary tier; no
    /// higher tier is considered if that tier cannot currently accept the file.
    fn select_primary_tiering_target(
        &self,
        placement: PrimaryDataPlacement,
        is_eligible: impl Fn(&Arc<DataVolume>) -> bool,
    ) -> Option<Arc<DataVolume>> {
        let writable_primary = self
            .data_volumes
            .iter()
            .filter(|volume| volume.supports_primary_data && !volume.readonly_source);
        if self.uses_lowest_primary_tier(placement) {
            let lowest_rank = self.lowest_writable_primary_rank()?;
            let candidates = writable_primary
                .filter(|volume| volume.priority.rank() == lowest_rank && is_eligible(volume))
                .collect::<Vec<_>>();
            return match candidates.len() {
                0 => None,
                1 => Some(Arc::clone(candidates[0])),
                _ => Some(Arc::clone(candidates[random::<usize>() % candidates.len()])),
            };
        }

        writable_primary
            .filter(|volume| is_eligible(volume))
            .max_by_key(|volume| volume.priority.rank())
            .map(Arc::clone)
    }

    fn max_readonly_load_target_used_bytes(&self, target_volume: &Arc<DataVolume>) -> u64 {
        let has_lower_primary_tier = self.data_volumes.iter().any(|volume| {
            volume.supports_primary_data && volume.priority.rank() < target_volume.priority.rank()
        });
        let watermark = if has_lower_primary_tier {
            // Do not explicitly load above the offload trigger, otherwise the next scan can
            // immediately send the same bytes back down to a lower tier.
            self.options.primary_volume_offload_trigger_watermark
        } else {
            self.options.primary_volume_write_stop_watermark
        };
        target_volume
            .size_limit
            .map(|size_limit| (size_limit as f64 * watermark).floor() as u64)
            .unwrap_or(u64::MAX)
    }

    fn trigger_backfill_if_needed_locked(
        self: &Arc<Self>,
        db_state: &Arc<DbStateHandle>,
    ) -> crate::Result<usize> {
        if !self.offload_runtime.has_available_worker_slot() {
            return Ok(0);
        }
        let referenced_priorities = referenced_primary_file_priorities(db_state);
        if referenced_priorities.is_empty() {
            return Ok(0);
        }

        let backfill_trigger_watermark = effective_backfill_trigger_watermark(
            self.options.primary_volume_backfill_trigger_watermark,
            self.options.primary_volume_offload_trigger_watermark,
        );
        if backfill_trigger_watermark <= 0.0 {
            return Ok(0);
        }

        let mut scheduled = 0usize;
        for target_volume in &self.data_volumes {
            if !target_volume.supports_primary_data || target_volume.readonly_source {
                continue;
            }
            let target_rank = target_volume.priority.rank();
            if !self
                .data_volumes
                .iter()
                .any(|volume| volume.supports_primary_data && volume.priority.rank() < target_rank)
            {
                continue;
            }
            let mut projected_used = self
                .offload_runtime
                .projected_target_physical_bytes(target_volume);
            let (refill_target_bytes, offload_trigger_bytes) = if let Some(size_limit) =
                target_volume.size_limit
            {
                let trigger_bytes = (size_limit as f64 * backfill_trigger_watermark).floor() as u64;
                if projected_used >= trigger_bytes {
                    continue;
                }
                // Fill only to the midpoint between the backfill and offload thresholds.
                // This hysteresis keeps a completed batch from immediately moving down again.
                let refill_target_ratio = (backfill_trigger_watermark
                    + self.options.primary_volume_offload_trigger_watermark)
                    / 2.0;
                (
                    (size_limit as f64 * refill_target_ratio).floor() as u64,
                    (size_limit as f64 * self.options.primary_volume_offload_trigger_watermark)
                        .floor() as u64,
                )
            } else {
                // An unlimited higher-priority volume can absorb every referenced file from
                // lower tiers, so it does not need a usage-ratio trigger or refill target.
                (u64::MAX, u64::MAX)
            };
            // Direct-low VLOG placement is durable policy: do not immediately undo it by
            // promoting the same VLOG files during primary-tier backfill. Derive the protected
            // ids from the live state rather than from file priority, because priority only
            // influences ordering and is not a file-type contract.
            let mut attempted = if self.options.vlog_low_priority_primary_enabled {
                referenced_vlog_file_ids(db_state)
            } else {
                HashSet::new()
            };
            while projected_used < refill_target_bytes
                && self.offload_runtime.has_available_worker_slot()
            {
                let remaining_before_offload = offload_trigger_bytes.saturating_sub(projected_used);
                let Some(file_id) = self.select_backfill_candidate_with_exclusions(
                    target_volume,
                    &referenced_priorities,
                    &attempted,
                    remaining_before_offload,
                ) else {
                    break;
                };
                attempted.insert(file_id);
                let estimated_bytes = self
                    .preferred_tracked_file(file_id)
                    .map(|tracked| tracked.size_bytes())
                    .unwrap_or(0);
                if estimated_bytes == 0 {
                    continue;
                }
                if self.schedule_backfill_move(
                    file_id,
                    target_volume,
                    db_state,
                    offload_trigger_bytes,
                )? {
                    scheduled += 1;
                    projected_used = self
                        .offload_runtime
                        .projected_target_physical_bytes(target_volume);
                }
            }
        }
        Ok(scheduled)
    }

    pub(crate) fn create_data_file_with_offload(
        self: &Arc<Self>,
    ) -> crate::Result<(FileId, TrackedWriter)> {
        let _ = self.trigger_offload_if_needed()?;
        self.create_data_file()
    }

    pub(crate) fn schedule_offload_move(
        self: &Arc<Self>,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
    ) -> crate::Result<bool> {
        self.schedule_primary_move(file_id, target_volume, None)
    }

    fn schedule_backfill_move(
        self: &Arc<Self>,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
        db_state: &Arc<DbStateHandle>,
        max_target_used_bytes: u64,
    ) -> crate::Result<bool> {
        self.schedule_primary_move(
            file_id,
            target_volume,
            Some(PrimaryMoveGuard::Backfill {
                db_state: Arc::downgrade(db_state),
                max_target_used_bytes,
            }),
        )
    }

    fn schedule_readonly_load_move(
        self: &Arc<Self>,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
        db_state: &Arc<DbStateHandle>,
        request: ReadonlyLoadRequest,
    ) -> crate::Result<bool> {
        let max_target_used_bytes = self.max_readonly_load_target_used_bytes(target_volume);
        self.schedule_primary_move(
            file_id,
            target_volume,
            Some(PrimaryMoveGuard::ReadonlyLoad {
                db_state: Arc::downgrade(db_state),
                max_target_used_bytes,
                pin_metadata: request.pin_metadata,
                pin_partitions: request.pin_partitions,
            }),
        )
    }

    fn schedule_primary_move(
        self: &Arc<Self>,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
        move_guard: Option<PrimaryMoveGuard>,
    ) -> crate::Result<bool> {
        let target_volume = Arc::clone(target_volume);
        let target_volume_for_job = Arc::clone(&target_volume);
        let source_tracked = self
            .preferred_tracked_file(file_id)
            .ok_or_else(|| Error::IoError(format!("Data file {} is not tracked", file_id)))?;
        let source_volume = source_tracked
            .volume
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| Error::IoError(format!("Data file {} has no volume", file_id)))?;
        let estimated_bytes = source_tracked.size_bytes();
        if estimated_bytes == 0 {
            return Ok(false);
        }
        let direction = match &move_guard {
            Some(PrimaryMoveGuard::ReadonlyLoad { .. }) => PrimaryTieringDirection::ReadonlyLoad,
            _ if source_volume.priority.rank() > target_volume.priority.rank() => {
                PrimaryTieringDirection::Offload
            }
            _ => PrimaryTieringDirection::Backfill,
        };
        let projected_source_release_bytes =
            projected_source_release_bytes(source_tracked.as_ref());
        let reserved_incoming_bytes =
            if self.has_ready_replica_on_primary_volume(file_id, &target_volume) {
                0
            } else {
                estimated_bytes
            };
        let max_target_used_bytes = move_guard
            .as_ref()
            .map(|guard| match guard {
                PrimaryMoveGuard::Backfill {
                    max_target_used_bytes,
                    ..
                } => *max_target_used_bytes,
                PrimaryMoveGuard::ReadonlyLoad {
                    max_target_used_bytes,
                    ..
                } => *max_target_used_bytes,
            })
            .or_else(|| {
                target_volume.size_limit.map(|size_limit| {
                    (size_limit as f64 * self.options.primary_volume_write_stop_watermark).floor()
                        as u64
                })
            })
            .unwrap_or(u64::MAX);
        if self
            .offload_runtime
            .projected_target_physical_bytes(&target_volume)
            .saturating_add(reserved_incoming_bytes)
            > max_target_used_bytes
        {
            return Ok(false);
        }
        let manager = Arc::downgrade(self);
        let readonly_pin = match &move_guard {
            Some(PrimaryMoveGuard::ReadonlyLoad {
                db_state,
                pin_metadata,
                pin_partitions,
                ..
            }) if *pin_metadata => Some((std::sync::Weak::clone(db_state), *pin_partitions)),
            _ => None,
        };
        let handler = Arc::new(move |scheduled_file_id| {
            if let Some(manager) = manager.upgrade() {
                let guard_status = manager.primary_move_guard_status(
                    scheduled_file_id,
                    &target_volume_for_job,
                    move_guard.as_ref(),
                );
                if guard_status != PrimaryMoveGuardStatus::Valid {
                    if guard_status == PrimaryMoveGuardStatus::Stale
                        && matches!(&move_guard, Some(PrimaryMoveGuard::ReadonlyLoad { .. }))
                    {
                        manager
                            .offload_runtime
                            .complete_readonly_load(scheduled_file_id);
                    }
                    manager.record_offload_noop();
                    return;
                }
                let runtime = Arc::clone(&manager.offload_runtime);
                let mut progress = |bytes| runtime.record_copy_progress(scheduled_file_id, bytes);
                let mut rollback = || runtime.reset_copy_progress(scheduled_file_id);
                match manager.move_file_to_primary_volume_with_progress(
                    scheduled_file_id,
                    &target_volume_for_job,
                    &mut progress,
                    &mut rollback,
                ) {
                    Ok(true) => {
                        if let Some((db_state, pin_partitions)) = &readonly_pin
                            && let Some(db_state) = db_state.upgrade()
                        {
                            manager.pin_promoted_readonly_sst_metadata(
                                scheduled_file_id,
                                &db_state,
                                *pin_partitions,
                            );
                        }
                        if matches!(&move_guard, Some(PrimaryMoveGuard::ReadonlyLoad { .. })) {
                            runtime.complete_readonly_load(scheduled_file_id);
                        }
                    }
                    Ok(false) => {
                        runtime.reset_copy_progress(scheduled_file_id);
                        manager.record_offload_noop();
                    }
                    Err(err) => {
                        runtime.reset_copy_progress(scheduled_file_id);
                        manager.record_offload_failed();
                        warn!(
                            "offload move failed for file_id={} target_rank={}: {}",
                            scheduled_file_id,
                            target_volume_for_job.priority.rank(),
                            err
                        );
                    }
                }
            }
        });
        let scheduled = self
            .offload_runtime
            .schedule(
                file_id,
                OffloadJobPlan {
                    source_volume,
                    target_volume: Arc::clone(&target_volume),
                    reserved_incoming_bytes,
                    projected_source_release_bytes,
                    copied_bytes: Arc::new(AtomicU64::new(0)),
                    direction,
                },
                handler,
                Some(Arc::clone(self)),
            )
            .map_err(Error::IoError)?;
        if scheduled {
            self.record_offload_scheduled();
        }
        Ok(scheduled)
    }

    fn primary_move_guard_status(
        &self,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
        move_guard: Option<&PrimaryMoveGuard>,
    ) -> PrimaryMoveGuardStatus {
        let Some(move_guard) = move_guard else {
            return PrimaryMoveGuardStatus::Valid;
        };
        match move_guard {
            PrimaryMoveGuard::Backfill {
                db_state,
                max_target_used_bytes,
            } => {
                let Some(db_state) = db_state.upgrade() else {
                    return PrimaryMoveGuardStatus::Stale;
                };
                if !referenced_primary_file_priorities(&db_state).contains_key(&file_id) {
                    return PrimaryMoveGuardStatus::Stale;
                }
                if self
                    .offload_runtime
                    .projected_target_physical_bytes(target_volume)
                    > *max_target_used_bytes
                {
                    return PrimaryMoveGuardStatus::Retry;
                }
                PrimaryMoveGuardStatus::Valid
            }
            PrimaryMoveGuard::ReadonlyLoad {
                db_state,
                max_target_used_bytes,
                ..
            } => {
                let Some(db_state) = db_state.upgrade() else {
                    return PrimaryMoveGuardStatus::Stale;
                };
                if !referenced_primary_file_priorities(&db_state).contains_key(&file_id)
                    || !self.preferred_tracked_file(file_id).is_some_and(|tracked| {
                        tracked
                            .volume
                            .as_ref()
                            .is_some_and(|volume| volume.readonly_source)
                    })
                {
                    return PrimaryMoveGuardStatus::Stale;
                }
                if self
                    .offload_runtime
                    .projected_target_physical_bytes(target_volume)
                    > *max_target_used_bytes
                {
                    return PrimaryMoveGuardStatus::Retry;
                }
                PrimaryMoveGuardStatus::Valid
            }
        }
    }

    /// Builds the existing immutable SST pin after a READONLY file has become readable from a
    /// primary volume. Pinning is deliberately best-effort: the promotion is already durable and
    /// a later foreground read can retry the same `get_or_load` path.
    fn pin_promoted_readonly_sst_metadata(
        &self,
        file_id: FileId,
        db_state: &DbStateHandle,
        pin_partitions: bool,
    ) {
        let Some(data_file) = find_sst_data_file(db_state, file_id) else {
            return;
        };
        if data_file.pinned_sst_read_metadata().is_some() {
            return;
        }
        let result = self.open_data_file_reader(file_id).and_then(|reader| {
            PinnedSstReadMetadata::get_or_load(&reader, data_file.as_ref(), true, pin_partitions)
                .map(|_| ())
        });
        if let Err(err) = result {
            warn!(
                "readonly promotion completed but pinned SST metadata could not be loaded for \
                 file_id={file_id}: {err}"
            );
        }
    }

    pub(crate) fn select_offload_candidate(
        &self,
        source_volume: &Arc<DataVolume>,
        target_volume: &Arc<DataVolume>,
    ) -> Option<FileId> {
        let excluded = HashSet::new();
        self.select_offload_candidate_with_exclusions(source_volume, target_volume, &excluded)
    }

    fn select_offload_candidate_with_exclusions(
        &self,
        source_volume: &Arc<DataVolume>,
        target_volume: &Arc<DataVolume>,
        excluded_file_ids: &HashSet<FileId>,
    ) -> Option<FileId> {
        if !source_volume.supports_primary_data || !target_volume.supports_primary_data {
            return None;
        }
        let candidates: Vec<(FileId, Arc<TrackedFile>)> = self
            .logical_files
            .iter()
            .filter_map(|entry| {
                if excluded_file_ids.contains(entry.key()) {
                    return None;
                }
                let logical = entry.value();
                if logical.persistent_cache_requested()
                    && matches!(
                        logical
                            .preferred_replica_any()
                            .map(|replica| replica.origin()),
                        Some(ReplicaOrigin::Owned)
                    )
                    && matches!(
                        logical.durable_replica().map(|replica| replica.origin()),
                        Some(ReplicaOrigin::ExternalPersistent { .. })
                    )
                {
                    return None;
                }
                let tracked = logical.preferred_replica_any()?;
                let tracked = &tracked.tracked;
                let volume = tracked.volume.as_ref()?;
                if !volume.supports_primary_data || !Arc::ptr_eq(volume, source_volume) {
                    return None;
                }
                let explicit_refs = tracked
                    .explicit_refs
                    .load(std::sync::atomic::Ordering::SeqCst);
                if explicit_refs != 0
                    && tracked.physical_delete_policy() != PhysicalDeletePolicy::ManagedDelete
                {
                    return None;
                }
                if self.offload_runtime.is_queued_or_running(*entry.key()) {
                    return None;
                }
                let size_bytes = tracked.size_bytes.load(std::sync::atomic::Ordering::SeqCst);
                if size_bytes == 0 {
                    return None;
                }
                Some((*entry.key(), Arc::clone(tracked)))
            })
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let source_pressure = VolumePressure {
            priority_rank: source_volume.priority.rank(),
            used_bytes: source_volume
                .used_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            size_limit: source_volume.size_limit,
        };
        let target_pressure = VolumePressure {
            priority_rank: target_volume.priority.rank(),
            used_bytes: target_volume
                .used_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            size_limit: target_volume.size_limit,
        };
        self.offload_runtime
            .select_candidate(&candidates, &source_pressure, &target_pressure)
    }

    fn select_backfill_candidate_with_exclusions(
        &self,
        target_volume: &Arc<DataVolume>,
        referenced_priorities: &HashMap<FileId, u8>,
        excluded_file_ids: &HashSet<FileId>,
        max_size_bytes: u64,
    ) -> Option<FileId> {
        if max_size_bytes == 0 || target_volume.readonly_source {
            return None;
        }
        self.logical_files
            .iter()
            .filter_map(|entry| {
                let file_id = *entry.key();
                if excluded_file_ids.contains(&file_id)
                    || self.offload_runtime.is_queued_or_running(file_id)
                {
                    return None;
                }
                let priority = *referenced_priorities.get(&file_id)?;
                let tracked = entry.value().preferred_replica_any()?;
                let tracked = &tracked.tracked;
                let source_volume = tracked.volume.as_ref()?;
                if !source_volume.supports_primary_data
                    || source_volume.priority.rank() >= target_volume.priority.rank()
                    || Arc::ptr_eq(source_volume, target_volume)
                {
                    return None;
                }
                let size_bytes = tracked.size_bytes();
                if size_bytes == 0 || size_bytes > max_size_bytes {
                    return None;
                }
                Some((file_id, priority, size_bytes))
            })
            .max_by(|left, right| {
                left.1
                    .cmp(&right.1)
                    .then_with(|| left.2.cmp(&right.2))
                    .then_with(|| right.0.cmp(&left.0))
            })
            .map(|candidate| candidate.0)
    }

    fn has_ready_replica_on_primary_volume(
        &self,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
    ) -> bool {
        self.get_logical_file(file_id)
            .and_then(|logical| logical.replica_on_volume(target_volume))
            .is_some_and(|replica| target_volume.supports_primary_data && replica.is_readable())
    }

    pub(crate) fn move_file_to_primary_volume(
        &self,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
    ) -> crate::Result<bool> {
        self.move_file_to_primary_volume_with_progress(
            file_id,
            target_volume,
            &mut |_| {},
            &mut || {},
        )
    }

    fn adopt_external_leased_file(
        &self,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
        progress: &mut dyn FnMut(u64),
    ) -> crate::Result<bool> {
        let source = self
            .preferred_tracked_file(file_id)
            .ok_or_else(|| Error::IoError(format!("Data file {file_id} is not tracked")))?;
        let Some(logical) = self.get_logical_file(file_id) else {
            return Ok(false);
        };
        if !matches!(
            logical
                .preferred_replica_any()
                .map(|replica| replica.origin()),
            Some(ReplicaOrigin::ExternalLeased { .. })
        ) {
            return Ok(false);
        }
        let source_reader = source.fs().open_read(source.path())?;
        let (mut writer, owned) =
            self.create_untracked_data_file_writer_on_volume(target_volume)?;
        self.copy_reader_to_tracked_writer_with_progress(
            source_reader.as_ref(),
            &mut writer,
            progress,
        )?;
        owned.set_priority(source.priority());
        let rollback_state = logical.replica_state_snapshot();
        let Some(source_replica_id) =
            self.select_new_replica_retaining_source_if(file_id, &source, Arc::clone(&owned))
        else {
            return Ok(false);
        };
        if let Err(err) = self.publish_durable_replica_route() {
            logical.restore_replica_state_snapshot(rollback_state);
            return Err(err);
        }
        self.retire_replica(file_id, source_replica_id);
        Ok(true)
    }

    pub(crate) fn schedule_adopt_external_leased_file(
        self: &Arc<Self>,
        file_id: FileId,
        target_volume: Arc<DataVolume>,
    ) -> crate::Result<bool> {
        let source = self
            .preferred_tracked_file(file_id)
            .ok_or_else(|| Error::IoError(format!("Data file {file_id} is not tracked")))?;
        let Some(source_volume) = source.volume.as_ref().map(Arc::clone) else {
            return Ok(false);
        };
        if self
            .offload_runtime
            .projected_target_physical_bytes(&target_volume)
            .saturating_add(source.size_bytes())
            > self.max_readonly_load_target_used_bytes(&target_volume)
        {
            return Ok(false);
        }
        let manager = Arc::downgrade(self);
        let target_for_job = Arc::clone(&target_volume);
        let copied_bytes = source.size_bytes();
        let handler = Arc::new(move |scheduled_file_id| {
            if let Some(manager) = manager.upgrade() {
                let runtime = Arc::clone(&manager.offload_runtime);
                let mut progress = |bytes| runtime.record_copy_progress(scheduled_file_id, bytes);
                match manager.adopt_external_leased_file(
                    scheduled_file_id,
                    &target_for_job,
                    &mut progress,
                ) {
                    Ok(true) => manager.record_offload_completed_copy(copied_bytes),
                    Ok(false) => manager.record_offload_noop(),
                    Err(err) => {
                        manager.record_offload_failed();
                        warn!("external adoption failed for file_id={scheduled_file_id}: {err}");
                    }
                }
            }
        });
        let scheduled = self
            .offload_runtime
            .schedule(
                file_id,
                OffloadJobPlan {
                    source_volume,
                    target_volume,
                    reserved_incoming_bytes: source.size_bytes(),
                    projected_source_release_bytes: 0,
                    copied_bytes: Arc::new(AtomicU64::new(0)),
                    direction: PrimaryTieringDirection::Adoption,
                },
                handler,
                Some(Arc::clone(self)),
            )
            .map_err(Error::IoError)?;
        if scheduled {
            self.record_offload_scheduled();
        }
        Ok(scheduled)
    }

    pub(crate) fn schedule_referenced_adoptions(
        self: &Arc<Self>,
        db_state: &DbStateHandle,
    ) -> crate::Result<usize> {
        let priorities = referenced_primary_file_priorities(db_state);
        let vlog_file_ids = referenced_vlog_file_ids(db_state);
        let mut candidates = priorities
            .into_iter()
            .filter(|(file_id, _)| {
                self.preferred_replica_origin(*file_id)
                    .is_some_and(|origin| matches!(origin, ReplicaOrigin::ExternalLeased { .. }))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        let mut scheduled = 0;
        for (file_id, _) in candidates {
            if !self.offload_runtime.has_available_worker_slot() {
                break;
            }
            let size = self
                .preferred_tracked_file(file_id)
                .map(|file| file.size_bytes())
                .unwrap_or(0);
            let placement = if vlog_file_ids.contains(&file_id) {
                PrimaryDataPlacement::Vlog
            } else {
                PrimaryDataPlacement::Standard
            };
            let Some(target) = self.select_adoption_target(size, placement) else {
                continue;
            };
            if self.schedule_adopt_external_leased_file(file_id, target)? {
                scheduled += 1;
            }
        }
        Ok(scheduled)
    }

    fn cache_external_persistent_file(
        &self,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
        progress: &mut dyn FnMut(u64),
    ) -> crate::Result<bool> {
        let source = self
            .preferred_tracked_file(file_id)
            .ok_or_else(|| Error::IoError(format!("Data file {file_id} is not tracked")))?;
        let Some(logical) = self.get_logical_file(file_id) else {
            return Ok(false);
        };
        if !matches!(
            logical
                .preferred_replica_any()
                .map(|replica| replica.origin()),
            Some(ReplicaOrigin::ExternalPersistent { .. })
        ) {
            return Ok(false);
        }
        let reader = source.fs().open_read(source.path())?;
        let (mut writer, owned) =
            self.create_untracked_data_file_writer_on_volume(target_volume)?;
        self.copy_reader_to_tracked_writer_with_progress(reader.as_ref(), &mut writer, progress)?;
        owned.set_priority(source.priority());
        if logical
            .add_and_select_replica_if(&source, Arc::clone(&owned), ReplicaLifecycle::OwnedReady)
            .is_none()
        {
            return Ok(false);
        }
        Ok(true)
    }

    fn move_persistent_cache_to_volume_with_progress(
        &self,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
        progress: &mut dyn FnMut(u64),
        rollback: &mut dyn FnMut(),
    ) -> crate::Result<bool> {
        let source = self
            .preferred_tracked_file(file_id)
            .ok_or_else(|| Error::IoError(format!("Data file {file_id} is not tracked")))?;
        let Some(logical) = self.get_logical_file(file_id) else {
            return Ok(false);
        };
        if !logical.persistent_cache_requested()
            || !matches!(
                logical
                    .preferred_replica_any()
                    .map(|replica| replica.origin()),
                Some(ReplicaOrigin::Owned)
            )
            || !matches!(
                logical.durable_replica().map(|replica| replica.origin()),
                Some(ReplicaOrigin::ExternalPersistent { .. })
            )
        {
            return Ok(false);
        }
        let source_replica_id = logical
            .preferred_replica_any()
            .expect("preferred replica")
            .replica_id;
        let replacement_replica_id = if let Some(replica) = logical.replica_on_volume(target_volume)
        {
            replica.tracked.set_priority(source.priority());
            if !logical.retain_and_select_replica_if(&source, replica.replica_id) {
                return Ok(false);
            }
            replica.replica_id
        } else {
            let reader = source.fs().open_read(source.path())?;
            let (mut writer, owned) =
                self.create_untracked_data_file_writer_on_volume(target_volume)?;
            if let Err(err) = self.copy_reader_to_tracked_writer_with_progress(
                reader.as_ref(),
                &mut writer,
                progress,
            ) {
                rollback();
                return Err(err);
            }
            owned.set_priority(source.priority());
            if logical
                .add_and_select_replica_if(&source, owned, ReplicaLifecycle::OwnedReady)
                .is_none()
            {
                rollback();
                return Ok(false);
            }
            logical
                .preferred_replica_any()
                .expect("new preferred replica")
                .replica_id
        };
        self.retire_replica(file_id, source_replica_id);
        debug_assert_eq!(
            logical
                .preferred_replica_any()
                .map(|replica| replica.replica_id),
            Some(replacement_replica_id)
        );
        self.record_offload_completed_copy(source.size_bytes());
        Ok(true)
    }

    fn schedule_persistent_cache_move(
        self: &Arc<Self>,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
    ) -> crate::Result<bool> {
        let source = self
            .preferred_tracked_file(file_id)
            .ok_or_else(|| Error::IoError(format!("Data file {file_id} is not tracked")))?;
        let Some(source_volume) = source.volume.as_ref().map(Arc::clone) else {
            return Ok(false);
        };
        let copied_bytes = source.size_bytes();
        if copied_bytes == 0
            || self
                .offload_runtime
                .projected_target_physical_bytes(target_volume)
                .saturating_add(copied_bytes)
                > self.max_readonly_load_target_used_bytes(target_volume)
        {
            return Ok(false);
        }
        let manager = Arc::downgrade(self);
        let target_for_job = Arc::clone(target_volume);
        let handler = Arc::new(move |scheduled_file_id| {
            if let Some(manager) = manager.upgrade() {
                let runtime = Arc::clone(&manager.offload_runtime);
                let mut progress = |bytes| runtime.record_copy_progress(scheduled_file_id, bytes);
                let mut rollback = || runtime.reset_copy_progress(scheduled_file_id);
                match manager.move_persistent_cache_to_volume_with_progress(
                    scheduled_file_id,
                    &target_for_job,
                    &mut progress,
                    &mut rollback,
                ) {
                    Ok(true) => {}
                    Ok(false) => {
                        runtime.reset_copy_progress(scheduled_file_id);
                        manager.record_offload_noop();
                    }
                    Err(err) => {
                        runtime.reset_copy_progress(scheduled_file_id);
                        manager.record_offload_failed();
                        warn!(
                            "persistent cache move failed for file_id={scheduled_file_id}: {err}"
                        );
                    }
                }
            }
        });
        let scheduled = self
            .offload_runtime
            .schedule(
                file_id,
                OffloadJobPlan {
                    source_volume,
                    target_volume: Arc::clone(target_volume),
                    reserved_incoming_bytes: copied_bytes,
                    projected_source_release_bytes: projected_source_release_bytes(&source),
                    copied_bytes: Arc::new(AtomicU64::new(0)),
                    direction: PrimaryTieringDirection::Offload,
                },
                handler,
                Some(Arc::clone(self)),
            )
            .map_err(Error::IoError)?;
        if scheduled {
            self.record_offload_scheduled();
        }
        Ok(scheduled)
    }

    fn move_persistent_caches_if_needed_locked(
        self: &Arc<Self>,
        db_state: &DbStateHandle,
    ) -> crate::Result<usize> {
        let priorities = referenced_primary_file_priorities(db_state);
        let mut scheduled = 0;
        for source_volume in &self.data_volumes {
            let Some(size_limit) = source_volume.size_limit else {
                continue;
            };
            let trigger_used_bytes = (size_limit as f64
                * self.options.primary_volume_offload_trigger_watermark)
                .ceil() as u64;
            if !source_volume.supports_primary_data
                || self
                    .offload_runtime
                    .projected_target_physical_bytes(source_volume)
                    < trigger_used_bytes
            {
                continue;
            }
            let mut candidates = priorities
                .iter()
                .filter_map(|(file_id, priority)| {
                    let logical = self.get_logical_file(*file_id)?;
                    let preferred = logical.preferred_replica_any()?;
                    let source_matches = preferred
                        .tracked
                        .volume
                        .as_ref()
                        .is_some_and(|volume| Arc::ptr_eq(volume, source_volume));
                    (source_matches
                        && !self.offload_runtime.is_queued_or_running(*file_id)
                        && logical.persistent_cache_requested()
                        && matches!(preferred.origin(), ReplicaOrigin::Owned)
                        && matches!(
                            logical.durable_replica().map(|replica| replica.origin()),
                            Some(ReplicaOrigin::ExternalPersistent { .. })
                        ))
                    .then_some((
                        *file_id,
                        *priority,
                        Arc::clone(&preferred.tracked),
                    ))
                })
                .collect::<Vec<_>>();
            candidates
                .sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
            for (file_id, _, source) in candidates {
                if !self.offload_runtime.has_available_worker_slot() {
                    return Ok(scheduled);
                }
                let Some(target_volume) = self
                    .data_volumes
                    .iter()
                    .filter(|target| {
                        target.supports_primary_data
                            && !target.readonly_source
                            && target.priority.rank() < source_volume.priority.rank()
                            && self
                                .offload_runtime
                                .projected_target_physical_bytes(target)
                                .saturating_add(source.size_bytes())
                                <= self.max_readonly_load_target_used_bytes(target)
                    })
                    .max_by_key(|target| target.priority.rank())
                    .map(Arc::clone)
                else {
                    continue;
                };
                if self.schedule_persistent_cache_move(file_id, &target_volume)? {
                    scheduled += 1;
                }
            }
        }
        Ok(scheduled)
    }

    pub(crate) fn schedule_referenced_persistent_caches(
        self: &Arc<Self>,
        db_state: &DbStateHandle,
    ) -> crate::Result<usize> {
        let vlog_file_ids = referenced_vlog_file_ids(db_state);
        let mut candidates = referenced_primary_file_priorities(db_state)
            .into_iter()
            .filter(|(file_id, _)| {
                self.get_logical_file(*file_id).is_some_and(|logical| {
                    logical.persistent_cache_requested()
                        && matches!(
                            logical
                                .preferred_replica_any()
                                .map(|replica| replica.origin()),
                            Some(ReplicaOrigin::ExternalPersistent { .. })
                        )
                        && matches!(
                            logical.durable_replica().map(|replica| replica.origin()),
                            Some(ReplicaOrigin::ExternalPersistent { .. })
                        )
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        let mut scheduled = 0;
        for (file_id, _) in candidates {
            if !self.offload_runtime.has_available_worker_slot() {
                break;
            }
            let Some(source) = self.preferred_tracked_file(file_id) else {
                continue;
            };
            let Some(source_volume) = source.volume.as_ref().map(Arc::clone) else {
                continue;
            };
            let placement = if vlog_file_ids.contains(&file_id) {
                PrimaryDataPlacement::Vlog
            } else {
                PrimaryDataPlacement::Standard
            };
            let Some(target_volume) = self.select_adoption_target(source.size_bytes(), placement)
            else {
                continue;
            };
            let manager = Arc::downgrade(self);
            let target_for_job = Arc::clone(&target_volume);
            let copied_bytes = source.size_bytes();
            let handler = Arc::new(move |scheduled_file_id| {
                if let Some(manager) = manager.upgrade() {
                    let runtime = Arc::clone(&manager.offload_runtime);
                    let mut progress =
                        |bytes| runtime.record_copy_progress(scheduled_file_id, bytes);
                    match manager.cache_external_persistent_file(
                        scheduled_file_id,
                        &target_for_job,
                        &mut progress,
                    ) {
                        Ok(true) => manager.record_offload_completed_copy(copied_bytes),
                        Ok(false) => manager.record_offload_noop(),
                        Err(err) => {
                            manager.record_offload_failed();
                            warn!(
                                "persistent cache copy failed for file_id={scheduled_file_id}: {err}"
                            );
                        }
                    }
                }
            });
            if self
                .offload_runtime
                .schedule(
                    file_id,
                    OffloadJobPlan {
                        source_volume,
                        target_volume,
                        reserved_incoming_bytes: copied_bytes,
                        projected_source_release_bytes: 0,
                        copied_bytes: Arc::new(AtomicU64::new(0)),
                        direction: PrimaryTieringDirection::Adoption,
                    },
                    handler,
                    Some(Arc::clone(self)),
                )
                .map_err(Error::IoError)?
            {
                self.record_offload_scheduled();
                scheduled += 1;
            }
        }
        Ok(scheduled)
    }

    fn evict_persistent_caches_if_needed_locked(
        &self,
        db_state: &DbStateHandle,
    ) -> crate::Result<usize> {
        let priorities = referenced_primary_file_priorities(db_state);
        let mut evicted = 0;
        for volume in &self.data_volumes {
            if !volume.supports_primary_data {
                continue;
            }
            let Some(size_limit) = volume.size_limit else {
                continue;
            };
            let trigger_used_bytes = (size_limit as f64
                * self.options.primary_volume_offload_trigger_watermark)
                .ceil() as u64;
            if volume.used_bytes.load(AtomicOrdering::SeqCst) < trigger_used_bytes {
                continue;
            }
            let mut candidates = priorities
                .iter()
                .filter_map(|(file_id, priority)| {
                    let logical = self.get_logical_file(*file_id)?;
                    let preferred = logical.preferred_replica_any()?;
                    let on_volume = preferred
                        .tracked
                        .volume
                        .as_ref()
                        .is_some_and(|candidate| Arc::ptr_eq(candidate, volume));
                    let has_lower_target = self.data_volumes.iter().any(|target| {
                        target.supports_primary_data
                            && !target.readonly_source
                            && target.priority.rank() < volume.priority.rank()
                            && self
                                .offload_runtime
                                .projected_target_physical_bytes(target)
                                .saturating_add(preferred.tracked.size_bytes())
                                <= self.max_readonly_load_target_used_bytes(target)
                    });
                    (on_volume
                        && logical.persistent_cache_requested()
                        && !has_lower_target
                        && matches!(preferred.origin(), ReplicaOrigin::Owned)
                        && matches!(
                            logical.durable_replica().map(|replica| replica.origin()),
                            Some(ReplicaOrigin::ExternalPersistent { .. })
                        ))
                    .then_some((*file_id, *priority))
                })
                .collect::<Vec<_>>();
            candidates
                .sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
            for (file_id, _) in candidates {
                if self.evict_preferred_persistent_cache(file_id)? {
                    evicted += 1;
                    break;
                }
            }
        }
        Ok(evicted)
    }

    pub(crate) fn has_referenced_adoption_jobs(&self, db_state: &DbStateHandle) -> bool {
        referenced_primary_file_priorities(db_state)
            .into_keys()
            .any(|file_id| self.offload_runtime.is_adoption_job(file_id))
    }

    fn move_file_to_primary_volume_with_progress(
        &self,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
        progress: &mut dyn FnMut(u64),
        rollback: &mut dyn FnMut(),
    ) -> crate::Result<bool> {
        let source_tracked = self
            .preferred_tracked_file(file_id)
            .ok_or_else(|| Error::IoError(format!("Data file {} is not tracked", file_id)))?;
        let Some(source_volume) = &source_tracked.volume else {
            return Ok(false);
        };
        if (!source_volume.supports_primary_data && !source_volume.readonly_source)
            || !target_volume.supports_primary_data
        {
            return Ok(false);
        }
        if Arc::ptr_eq(source_volume, target_volume) {
            return Ok(false);
        }
        if let Some(logical) = self.get_logical_file(file_id)
            && let Some(target_replica) = logical.replica_on_volume(target_volume)
        {
            target_replica
                .tracked
                .set_priority(source_tracked.priority());
            let rollback_state = logical.replica_state_snapshot();
            let Some(source_replica_id) = self.select_existing_replica_retaining_source_if(
                file_id,
                &source_tracked,
                target_replica.replica_id,
            ) else {
                return Ok(false);
            };
            if let Err(err) = self.publish_durable_replica_route() {
                logical.restore_replica_state_snapshot(rollback_state);
                return Err(err);
            }
            self.retire_replica(file_id, source_replica_id);
            self.record_offload_completed_promotion();
            return Ok(true);
        }
        let copied_bytes = source_tracked
            .size_bytes
            .load(std::sync::atomic::Ordering::SeqCst);
        let source_reader = source_tracked.fs().open_read(source_tracked.path())?;
        let (mut writer, new_tracked) =
            self.create_untracked_data_file_writer_on_volume(target_volume)?;
        if let Err(err) = self.copy_reader_to_tracked_writer_with_progress(
            source_reader.as_ref(),
            &mut writer,
            progress,
        ) {
            rollback();
            return Err(err);
        }
        new_tracked.set_priority(source_tracked.priority());
        let Some(logical) = self.get_logical_file(file_id) else {
            rollback();
            return Ok(false);
        };
        let rollback_state = logical.replica_state_snapshot();
        let Some(source_replica_id) = self.select_new_replica_retaining_source_if(
            file_id,
            &source_tracked,
            Arc::clone(&new_tracked),
        ) else {
            rollback();
            return Ok(false);
        };
        if let Err(err) = self.publish_durable_replica_route() {
            logical.restore_replica_state_snapshot(rollback_state);
            rollback();
            return Err(err);
        }
        self.retire_replica(file_id, source_replica_id);
        self.record_offload_completed_copy(copied_bytes);
        Ok(true)
    }
}

fn referenced_readonly_load_requests(
    db_state: &DbStateHandle,
    pinned_metadata_max_level: Option<u8>,
    pin_metadata_partitions: bool,
) -> HashMap<FileId, ReadonlyLoadRequest> {
    let state = db_state.load();
    let mut requests = HashMap::<FileId, ReadonlyLoadRequest>::new();
    for tree_version in state.multi_lsm_version.tree_versions_cloned() {
        for level in &tree_version.levels {
            let priority = crate::file::lsm_file_priority_for_level(level.ordinal);
            for file in &level.files {
                let pin_metadata = file.file_type == DataFileType::SSTable
                    && pinned_metadata_max_level
                        .is_some_and(|max_level| level.ordinal <= max_level);
                let request = ReadonlyLoadRequest {
                    priority,
                    pin_metadata,
                    pin_partitions: pin_metadata && pin_metadata_partitions,
                };
                requests
                    .entry(file.file_id)
                    .and_modify(|current| current.merge(request))
                    .or_insert(request);
            }
        }
    }
    for (_, tracked_id, _) in state.vlog_version.files_with_entries() {
        let request = ReadonlyLoadRequest {
            priority: crate::file::VLOG_FILE_PRIORITY,
            pin_metadata: false,
            pin_partitions: false,
        };
        requests
            .entry(tracked_id.file_id())
            .and_modify(|current| current.merge(request))
            .or_insert(request);
    }
    requests
}

fn find_sst_data_file(db_state: &DbStateHandle, file_id: FileId) -> Option<Arc<DataFile>> {
    for tree_version in db_state.load().multi_lsm_version.tree_versions_cloned() {
        for level in &tree_version.levels {
            if let Some(file) = level
                .files
                .iter()
                .find(|file| file.file_id == file_id && file.file_type == DataFileType::SSTable)
            {
                return Some(Arc::clone(file));
            }
        }
    }
    None
}

fn referenced_primary_file_priorities(db_state: &DbStateHandle) -> HashMap<FileId, u8> {
    let state = db_state.load();
    let mut priorities = HashMap::<FileId, u8>::new();
    for tree_version in state.multi_lsm_version.tree_versions_cloned() {
        for level in &tree_version.levels {
            let priority = crate::file::lsm_file_priority_for_level(level.ordinal);
            for file in &level.files {
                priorities
                    .entry(file.file_id)
                    .and_modify(|current| *current = (*current).max(priority))
                    .or_insert(priority);
            }
        }
    }
    for (_, tracked_id, _) in state.vlog_version.files_with_entries() {
        priorities
            .entry(tracked_id.file_id())
            .or_insert(crate::file::VLOG_FILE_PRIORITY);
    }
    priorities
}

fn referenced_vlog_file_ids(db_state: &DbStateHandle) -> HashSet<FileId> {
    db_state
        .load()
        .vlog_version
        .files_with_entries()
        .into_iter()
        .map(|(_, tracked_id, _)| tracked_id.file_id())
        .collect()
}

#[cfg(test)]
#[path = "../../tests/unit/file/offload.rs"]
mod tests;
