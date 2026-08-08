use super::file_manager::FileId;
use crate::Error;
use crate::config::PrimaryVolumeOffloadPolicyKind;
use crate::data_file::{DataFile, DataFileType};
use crate::db_state::DbStateHandle;
use crate::file::{DataVolume, FileManager, TrackedFile, TrackedWriter};
use crate::sst::PinnedSstReadMetadata;
use dashmap::{DashMap, Entry};
use log::warn;
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
    if source.is_marked_for_deletion() && source.explicit_refs.load(AtomicOrdering::SeqCst) == 0 {
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
                if let Err(err) = file_manager.trigger_primary_tiering_if_needed(&db_state) {
                    warn!("primary volume tiering scan failed: {}", err);
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
    fn create_untracked_data_file_writer_on_volume(
        &self,
        volume: &Arc<DataVolume>,
    ) -> crate::Result<(TrackedWriter, Arc<TrackedFile>)> {
        let tracked = Arc::new(TrackedFile::new(
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
    ) -> crate::Result<Option<PrimaryTieringWorkerHandle>> {
        let distinct_primary_ranks = self
            .data_volumes
            .iter()
            .filter(|volume| volume.supports_primary_data)
            .map(|volume| volume.priority.rank())
            .collect::<HashSet<_>>();
        let has_readonly_source = self
            .data_volumes
            .iter()
            .any(|volume| volume.readonly_source);
        if distinct_primary_ranks.len() < 2 && !has_readonly_source {
            return Ok(None);
        }
        PrimaryTieringWorkerHandle::start(self, db_state).map(Some)
    }

    #[cfg(test)]
    fn wait_for_offload_idle(&self, timeout: Duration) -> bool {
        self.offload_runtime.wait_idle(timeout)
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
            candidates.push((file_id, request, size_bytes));
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
        for (file_id, request, size_bytes) in candidates {
            if !self.offload_runtime.has_available_worker_slot() {
                break;
            }
            let Some(target_volume) = self.select_readonly_load_target(file_id, size_bytes) else {
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
    ) -> Option<Arc<DataVolume>> {
        self.data_volumes
            .iter()
            .filter(|volume| volume.supports_primary_data && !volume.readonly_source)
            .filter(|volume| {
                let reserved_incoming_bytes =
                    if self.has_snapshot_replica_on_primary_volume(file_id, volume) {
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
            let mut attempted = HashSet::new();
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
            if self.has_snapshot_replica_on_primary_volume(file_id, &target_volume) {
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
                let tracked = entry.value().preferred_replica_any()?;
                let tracked = &tracked.tracked;
                let volume = tracked.volume.as_ref()?;
                if !volume.supports_primary_data || !Arc::ptr_eq(volume, source_volume) {
                    return None;
                }
                let explicit_refs = tracked
                    .explicit_refs
                    .load(std::sync::atomic::Ordering::SeqCst);
                if explicit_refs != 0 && !tracked.is_marked_for_deletion() {
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

    #[cfg(test)]
    fn primary_volume_by_rank(&self, rank: u8) -> Option<Arc<DataVolume>> {
        self.offload_runtime.primary_volume_by_rank(rank)
    }

    fn has_snapshot_replica_on_primary_volume(
        &self,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
    ) -> bool {
        let Some(snapshot_replica_file_id) = self.snapshot_replica_hint_file_id(file_id) else {
            return false;
        };
        self.preferred_tracked_file(snapshot_replica_file_id)
            .is_some_and(|snapshot_tracked| {
                snapshot_tracked.volume.as_ref().is_some_and(|volume| {
                    volume.supports_primary_data && Arc::ptr_eq(volume, target_volume)
                })
            })
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
        if let Some(snapshot_replica_file_id) = self.snapshot_replica_hint_file_id(file_id)
            && let Some(snapshot_tracked) = self.preferred_tracked_file(snapshot_replica_file_id)
            && let Some(snapshot_volume) = &snapshot_tracked.volume
            && snapshot_volume.supports_primary_data
            && Arc::ptr_eq(snapshot_volume, target_volume)
        {
            snapshot_tracked.set_priority(source_tracked.priority());
            if !self.replace_data_file_replica(file_id, &source_tracked, snapshot_tracked) {
                return Ok(false);
            }
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
        if !self.replace_data_file_replica(file_id, &source_tracked, new_tracked) {
            rollback();
            return Ok(false);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::{
        File, FileSystemRegistry, RandomAccessFile, SequentialWriteFile, test_utils,
    };
    use crate::sst::{
        PinnedSstReadMetadata, SSTIteratorOptions, SSTPointReader, SSTWriter, SSTWriterOptions,
    };
    use crate::{Config, MetricsManager, VolumeUsageKind};
    use bytes::Bytes;
    use size::Size;

    fn pressure(rank: u8) -> VolumePressure {
        VolumePressure {
            priority_rank: rank,
            used_bytes: 1,
            size_limit: Some(2),
        }
    }

    fn register_readonly_sst(
        file_manager: &Arc<FileManager>,
        readonly_root: &std::path::Path,
        file_id: FileId,
        partitioned_index: bool,
    ) -> Arc<DataFile> {
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", readonly_root.display()))
            .unwrap();
        fs.create_dir("db").unwrap();
        fs.create_dir("db/data").unwrap();
        let relative_path = format!("db/data/{file_id}.sst");
        let mut writer = SSTWriter::new(
            fs.open_write(&relative_path).unwrap(),
            SSTWriterOptions {
                block_size: 32,
                bloom_filter_enabled: true,
                partitioned_index,
                block_checksum_enabled: false,
                ..SSTWriterOptions::default()
            },
        );
        for key in [b"key000".as_slice(), b"key001", b"key002", b"key003"] {
            writer.add(key, b"value").unwrap();
        }
        let result = writer.finish_with_range().unwrap();
        let path = readonly_root.join(&relative_path);
        file_manager
            .register_data_file_readonly(file_id, &format!("file://{}", path.display()))
            .unwrap();
        let data_file = DataFile::new(
            DataFileType::SSTable,
            result.first_key,
            result.last_key,
            file_id,
            crate::file::TrackedFileId::new(file_manager, file_id),
            0,
            result.file_size,
            0u16..=0u16,
            0u16..=0u16,
        );
        data_file.set_meta_bytes(result.meta_bytes);
        if let Some(metadata) = result.sst_read_metadata {
            data_file.set_sst_read_metadata(metadata);
        }
        Arc::new(data_file)
    }

    fn store_readonly_sst_state(
        db_state: &Arc<DbStateHandle>,
        level_ordinal: u8,
        data_file: Arc<DataFile>,
    ) {
        let current = db_state.load();
        db_state.store(crate::db_state::DbState {
            seq_id: current.seq_id,
            bucket_ranges: vec![0u16..=0u16],
            multi_lsm_version: crate::db_state::MultiLSMTreeVersion::new(
                crate::lsm::LSMTreeVersion {
                    levels: vec![crate::lsm::Level {
                        ordinal: level_ordinal,
                        tiered: level_ordinal == 0,
                        files: vec![data_file],
                    }],
                },
            ),
            vlog_version: crate::vlog::VlogVersion::new(),
            active: current.active.clone(),
            immutables: current.immutables.clone(),
            truncation_cursors: current.truncation_cursors.clone(),
            suggested_base_snapshot_id: None,
        });
    }

    struct FailingReader {
        size: usize,
    }

    impl File for FailingReader {
        fn close(&mut self) -> crate::Result<()> {
            Ok(())
        }

        fn size(&self) -> usize {
            self.size
        }
    }

    impl RandomAccessFile for FailingReader {
        fn read_at(&self, _offset: usize, _size: usize) -> crate::Result<Bytes> {
            Err(Error::IoError(
                "injected pinned metadata read failure".to_string(),
            ))
        }
    }

    #[test]
    fn backfill_watermark_can_exceed_half_offload_but_never_eighty_percent() {
        assert_eq!(effective_backfill_trigger_watermark(0.70, 0.85), 0.70);
        assert_eq!(effective_backfill_trigger_watermark(0.90, 0.95), 0.80);
        assert!((effective_backfill_trigger_watermark(0.70, 0.60) - 0.59).abs() < f64::EPSILON);
    }

    #[test]
    fn tiering_runtime_executes_multiple_jobs_concurrently() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}/high", dir.path().display()),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}/low", dir.path().display()),
                    vec![VolumeUsageKind::PrimaryDataPriorityLow],
                ),
            ],
            file_transfer_concurrency: 2,
            ..Config::default()
        };
        let metrics = Arc::new(MetricsManager::new("tiering-runtime-concurrency"));
        let fm = FileManager::from_config(&config, "db", metrics).unwrap();
        let source = fm.primary_volume_by_rank(3).unwrap();
        let target = fm.primary_volume_by_rank(1).unwrap();
        let gate = Arc::new((Mutex::new((0usize, false)), Condvar::new()));
        let handler_gate = Arc::clone(&gate);
        let handler = Arc::new(move |_| {
            let (lock, condvar) = handler_gate.as_ref();
            let mut state = lock.lock().unwrap();
            state.0 += 1;
            condvar.notify_all();
            while !state.1 {
                state = condvar.wait(state).unwrap();
            }
        });

        for file_id in [101, 102] {
            fm.offload_runtime
                .schedule(
                    file_id,
                    OffloadJobPlan {
                        source_volume: Arc::clone(&source),
                        target_volume: Arc::clone(&target),
                        reserved_incoming_bytes: 10,
                        projected_source_release_bytes: 10,
                        copied_bytes: Arc::new(AtomicU64::new(0)),
                        direction: PrimaryTieringDirection::Offload,
                    },
                    handler.clone(),
                    None,
                )
                .unwrap();
        }

        let (lock, condvar) = gate.as_ref();
        let state = lock.lock().unwrap();
        let (mut state, _) = condvar
            .wait_timeout_while(state, Duration::from_secs(2), |state| state.0 < 2)
            .unwrap();
        let ran_concurrently = state.0 == 2;
        state.1 = true;
        condvar.notify_all();
        drop(state);

        assert!(fm.wait_for_offload_idle(Duration::from_secs(5)));
        assert!(
            ran_concurrently,
            "both workers should start before either job is released"
        );
    }

    #[test]
    fn target_accounting_replaces_written_reservation_with_actual_usage() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}/high", dir.path().display()),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}/low", dir.path().display()),
                    vec![VolumeUsageKind::PrimaryDataPriorityLow],
                ),
            ],
            ..Config::default()
        };
        let metrics = Arc::new(MetricsManager::new("tiering-runtime-accounting"));
        let fm = FileManager::from_config(&config, "db", metrics).unwrap();
        let source = fm.primary_volume_by_rank(3).unwrap();
        let target = fm.primary_volume_by_rank(1).unwrap();
        target.add_usage(20);
        fm.offload_runtime.planned_jobs.insert(
            103,
            OffloadJobPlan {
                source_volume: source,
                target_volume: Arc::clone(&target),
                reserved_incoming_bytes: 100,
                projected_source_release_bytes: 0,
                copied_bytes: Arc::new(AtomicU64::new(0)),
                direction: PrimaryTieringDirection::Offload,
            },
        );

        assert_eq!(
            fm.offload_runtime.projected_target_physical_bytes(&target),
            120
        );
        target.add_usage(40);
        fm.offload_runtime.record_copy_progress(103, 40);
        assert_eq!(
            fm.offload_runtime.projected_target_physical_bytes(&target),
            120,
            "written bytes must replace, not duplicate, the incoming reservation"
        );

        target.subtract_usage(40);
        fm.offload_runtime.reset_copy_progress(103);
        assert_eq!(
            fm.offload_runtime.projected_target_physical_bytes(&target),
            120,
            "a failed temporary copy restores the full incoming reservation"
        );
        fm.offload_runtime.complete_job(103);
    }

    #[test]
    fn source_accounting_does_not_claim_snapshot_retained_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            volumes: crate::VolumeDescriptor::single_volume(format!(
                "file://{}",
                dir.path().display()
            )),
            ..Config::default()
        };
        let metrics = Arc::new(MetricsManager::new("tiering-source-accounting"));
        let fm = FileManager::from_config(&config, "db", metrics).unwrap();
        let (file_id, mut writer) = fm.create_data_file().unwrap();
        writer.write(&[b'x'; 128]).unwrap();
        writer.close().unwrap();

        let tracked = fm.preferred_tracked_file(file_id).unwrap();
        assert_eq!(projected_source_release_bytes(&tracked), 128);

        let snapshot_ref = fm.data_file_ref(file_id).unwrap();
        assert_eq!(
            projected_source_release_bytes(snapshot_ref.as_ref()),
            0,
            "snapshot-retained bytes remain part of physical source usage"
        );
        snapshot_ref.dereference();
    }

    #[test]
    fn largest_file_policy_picks_largest() {
        let policy = LargestFileOffloadPolicy;
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/offload-policy-largest")
            .unwrap();
        let candidates = vec![
            (
                7,
                Arc::new(TrackedFile::new("a".to_string(), Arc::clone(&fs), None)),
            ),
            (
                3,
                Arc::new(TrackedFile::new("b".to_string(), Arc::clone(&fs), None)),
            ),
        ];
        candidates[0].1.update_size_bytes(128);
        candidates[1].1.update_size_bytes(256);
        assert_eq!(
            policy.select_candidate(&candidates, &pressure(3), &pressure(2)),
            Some(3)
        );
    }

    #[test]
    fn largest_file_policy_handles_empty_candidates() {
        let policy = LargestFileOffloadPolicy;
        assert!(
            policy
                .select_candidate(&[], &pressure(3), &pressure(2))
                .is_none()
        );
    }

    #[test]
    fn largest_file_policy_tie_breaks_by_file_id() {
        let policy = LargestFileOffloadPolicy;
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/offload-policy-tie")
            .unwrap();
        let candidates = vec![
            (
                12,
                Arc::new(TrackedFile::new("b".to_string(), Arc::clone(&fs), None)),
            ),
            (
                6,
                Arc::new(TrackedFile::new("a".to_string(), Arc::clone(&fs), None)),
            ),
        ];
        candidates[0].1.update_size_bytes(64);
        candidates[1].1.update_size_bytes(64);
        assert_eq!(
            policy.select_candidate(&candidates, &pressure(3), &pressure(2)),
            Some(6)
        );
    }

    #[test]
    fn priority_policy_prefers_lower_priority_over_larger_size() {
        let policy = PriorityOffloadPolicy;
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/offload-policy-priority")
            .unwrap();
        let candidates = vec![
            (
                11,
                Arc::new(TrackedFile::new("a".to_string(), Arc::clone(&fs), None)),
            ),
            (
                22,
                Arc::new(TrackedFile::new("b".to_string(), Arc::clone(&fs), None)),
            ),
        ];
        candidates[0].1.update_size_bytes(1024);
        candidates[1].1.update_size_bytes(32);
        candidates[0].1.set_priority(200);
        candidates[1].1.set_priority(3);
        assert_eq!(
            policy.select_candidate(&candidates, &pressure(3), &pressure(2)),
            Some(22)
        );
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_select_offload_candidate_uses_policy() {
        let root = "/tmp/file_manager_offload_policy";
        let _ = std::fs::remove_dir_all(root);
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}/high", root),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}/low", root),
                    vec![VolumeUsageKind::PrimaryDataPriorityLow],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-policy"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let (small_id, mut small_writer) = fm.create_data_file().unwrap();
        small_writer.write(&[b'a'; 32]).unwrap();
        small_writer.close().unwrap();
        let (large_id, mut large_writer) = fm.create_data_file().unwrap();
        large_writer.write(&[b'b'; 128]).unwrap();
        large_writer.close().unwrap();
        let source_volume = fm.primary_volume_by_rank(3).unwrap();
        let target_volume = fm.primary_volume_by_rank(1).unwrap();
        let selected = fm.select_offload_candidate(&source_volume, &target_volume);
        assert_eq!(selected, Some(large_id));
        assert_ne!(small_id, large_id);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_select_offload_candidate_prefers_lower_priority_file() {
        let root = "/tmp/file_manager_offload_priority_policy";
        let _ = std::fs::remove_dir_all(root);
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}/high", root),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}/low", root),
                    vec![VolumeUsageKind::PrimaryDataPriorityLow],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-priority"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let (large_high_pri_id, mut large_writer) = fm.create_data_file().unwrap();
        large_writer.write(&vec![b'a'; 512]).unwrap();
        large_writer.close().unwrap();
        let (small_low_pri_id, mut small_writer) = fm.create_data_file().unwrap();
        small_writer.write(&[b'b'; 32]).unwrap();
        small_writer.close().unwrap();
        fm.set_data_file_priority(large_high_pri_id, 200).unwrap();
        fm.set_data_file_priority(small_low_pri_id, 3).unwrap();
        let source_volume = fm.primary_volume_by_rank(3).unwrap();
        let target_volume = fm.primary_volume_by_rank(1).unwrap();
        let selected = fm.select_offload_candidate(&source_volume, &target_volume);
        assert_eq!(selected, Some(small_low_pri_id));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_select_offload_candidate_uses_configured_largest_file_policy() {
        let root = "/tmp/file_manager_offload_policy_option_largest";
        let _ = std::fs::remove_dir_all(root);
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}/high", root),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}/low", root),
                    vec![VolumeUsageKind::PrimaryDataPriorityLow],
                ),
            ],
            primary_volume_offload_policy: crate::PrimaryVolumeOffloadPolicyKind::LargestFile,
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-policy-option"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let (large_high_pri_id, mut large_writer) = fm.create_data_file().unwrap();
        large_writer.write(&vec![b'a'; 512]).unwrap();
        large_writer.close().unwrap();
        let (small_low_pri_id, mut small_writer) = fm.create_data_file().unwrap();
        small_writer.write(&[b'b'; 32]).unwrap();
        small_writer.close().unwrap();
        fm.set_data_file_priority(large_high_pri_id, 200).unwrap();
        fm.set_data_file_priority(small_low_pri_id, 3).unwrap();
        let source_volume = fm.primary_volume_by_rank(3).unwrap();
        let target_volume = fm.primary_volume_by_rank(1).unwrap();
        let selected = fm.select_offload_candidate(&source_volume, &target_volume);
        assert_eq!(selected, Some(large_high_pri_id));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_select_offload_candidate_skips_snapshot_replica_files() {
        let root = "/tmp/file_manager_offload_skip_snapshot_replica";
        let _ = std::fs::remove_dir_all(root);
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}/high", root),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}/low", root),
                    vec![VolumeUsageKind::PrimaryDataPriorityLow],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new(
            "file-manager-offload-skip-snapshot-replica",
        ));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let (source_file_id, mut source_writer) = fm.create_data_file().unwrap();
        source_writer.write(&[b's'; 64]).unwrap();
        source_writer.close().unwrap();
        let (snapshot_replica_id, mut snapshot_writer) = fm.create_data_file().unwrap();
        snapshot_writer.write(&vec![b'r'; 1024]).unwrap();
        snapshot_writer.close().unwrap();
        fm.make_data_file_readonly(snapshot_replica_id).unwrap();
        let snapshot_ref = fm.data_file_ref(snapshot_replica_id).unwrap();
        fm.register_snapshot_replica_hint(source_file_id, snapshot_replica_id);
        let source_volume = fm.primary_volume_by_rank(3).unwrap();
        let target_volume = fm.primary_volume_by_rank(1).unwrap();
        let selected = fm.select_offload_candidate(&source_volume, &target_volume);
        assert_eq!(selected, Some(source_file_id));
        drop(snapshot_ref);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_offload_move_is_async_and_keeps_reads_available() {
        let root = "/tmp/file_manager_offload_async";
        let _ = std::fs::remove_dir_all(root);
        let high_url = format!("file://{}/high", root);
        let low_url = format!("file://{}/low", root);
        let registry = FileSystemRegistry::new();
        let high_fs = registry.get_or_register(high_url.clone()).unwrap();
        let low_fs = registry.get_or_register(low_url.clone()).unwrap();
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    high_url,
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    low_url,
                    vec![VolumeUsageKind::PrimaryDataPriorityLow],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-async"));
        let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
        let payload = vec![b'x'; 8 * 1024 * 1024];
        let (file_id, mut writer) = fm.create_data_file().unwrap();
        writer.write(&payload).unwrap();
        writer.close().unwrap();

        let old_path = fm.get_data_file_path(file_id).unwrap();
        assert!(high_fs.exists(&old_path).unwrap());
        let old_reader = fm.open_data_file_reader(file_id).unwrap();
        let target_volume = fm.primary_volume_by_rank(1).unwrap();

        assert!(fm.schedule_offload_move(file_id, &target_volume).unwrap());
        assert!(!fm.schedule_offload_move(file_id, &target_volume).unwrap());
        assert!(fm.wait_for_offload_idle(Duration::from_secs(20)));

        assert_eq!(
            old_reader.read_at(payload.len() - 16, 16).unwrap().as_ref(),
            &payload[payload.len() - 16..]
        );
        let new_path = fm.get_data_file_path(file_id).unwrap();
        assert_ne!(old_path, new_path);
        assert!(low_fs.exists(&new_path).unwrap());
        let new_reader = fm.open_data_file_reader(file_id).unwrap();
        assert_eq!(
            new_reader.read_at(payload.len() - 16, 16).unwrap().as_ref(),
            &payload[payload.len() - 16..]
        );
        drop(old_reader);
        test_utils::wait_for_file_deletion(&high_fs, &old_path);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_offload_promotes_existing_snapshot_replica_on_target_volume() {
        let root = "/tmp/file_manager_offload_promote_snapshot_replica";
        let _ = std::fs::remove_dir_all(root);
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}/high", root),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}/low", root),
                    vec![
                        VolumeUsageKind::PrimaryDataPriorityLow,
                        VolumeUsageKind::Snapshot,
                    ],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new(
            "file-manager-offload-promote-snapshot-replica",
        ));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();

        let (source_file_id, mut source_writer) = fm.create_data_file().unwrap();
        source_writer.write(&vec![b'x'; 512]).unwrap();
        source_writer.close().unwrap();
        let source_path = fm.get_data_file_path(source_file_id).unwrap();

        let (snapshot_replica_file_id, copied) = fm
            .copy_data_file_to_snapshot_volume_with_result(source_file_id, None)
            .unwrap();
        assert!(copied);
        fm.register_snapshot_replica_hint(source_file_id, snapshot_replica_file_id);
        let snapshot_replica_path = fm.get_data_file_path(snapshot_replica_file_id).unwrap();

        let target_volume = fm.primary_volume_by_rank(1).unwrap();
        let promoted = fm
            .move_file_to_primary_volume(source_file_id, &target_volume)
            .unwrap();
        assert!(promoted);
        assert_eq!(
            fm.get_data_file_path(source_file_id).unwrap(),
            snapshot_replica_path
        );
        assert_ne!(source_path, snapshot_replica_path);
        assert_eq!(
            fm.snapshot_replica_hint_file_id(source_file_id),
            Some(snapshot_replica_file_id)
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_backfill_preserves_source_until_snapshot_reference_is_released() {
        let root = "/tmp/file_manager_backfill_snapshot_lifecycle";
        let _ = std::fs::remove_dir_all(root);
        let high_url = format!("file://{root}/high");
        let low_url = format!("file://{root}/low");
        let registry = FileSystemRegistry::new();
        let low_fs = registry.get_or_register(low_url.clone()).unwrap();
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    high_url,
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    low_url,
                    vec![
                        VolumeUsageKind::PrimaryDataPriorityLow,
                        VolumeUsageKind::Snapshot,
                    ],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new(
            "file-manager-backfill-snapshot-lifecycle",
        ));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let low_volume = fm.primary_volume_by_rank(1).unwrap();
        let high_volume = fm.primary_volume_by_rank(3).unwrap();

        let (file_id, mut writer) = fm.create_data_file().unwrap();
        writer.write(b"snapshot-retained").unwrap();
        writer.close().unwrap();
        assert!(
            fm.move_file_to_primary_volume(file_id, &low_volume)
                .unwrap()
        );
        let low_path = fm.get_data_file_path(file_id).unwrap();
        let snapshot_ref = fm.data_file_ref(file_id).unwrap();

        assert!(
            fm.move_file_to_primary_volume(file_id, &high_volume)
                .unwrap()
        );
        assert!(
            low_fs.exists(&low_path).unwrap(),
            "backfill must retain the low-tier source while a snapshot references it"
        );

        snapshot_ref.dereference();
        assert!(low_fs.exists(&low_path).unwrap());
        drop(snapshot_ref);
        test_utils::wait_for_file_deletion(&low_fs, &low_path);
        assert!(
            !low_fs.exists(&low_path).unwrap(),
            "the source may be deleted only after the snapshot reference is released"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_backfill_prefers_low_lsm_levels_and_skips_unreferenced_files() {
        let root = "/tmp/file_manager_backfill_priority";
        let _ = std::fs::remove_dir_all(root);
        let mut high = crate::VolumeDescriptor::new(
            format!("file://{root}/high"),
            vec![VolumeUsageKind::PrimaryDataPriorityHigh],
        );
        high.size_limit = Some(Size::from_kib(8));
        let low = crate::VolumeDescriptor::new(
            format!("file://{root}/low"),
            vec![VolumeUsageKind::PrimaryDataPriorityLow],
        );
        let config = Config {
            volumes: vec![high, low],
            base_file_size: Size::from_const(64),
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-backfill-priority"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let low_volume = fm.primary_volume_by_rank(1).unwrap();
        let high_volume = fm.primary_volume_by_rank(3).unwrap();

        let create_on_low = |size: usize| {
            let (file_id, mut writer) = fm.create_data_file().unwrap();
            writer.write(&vec![b'x'; size]).unwrap();
            writer.close().unwrap();
            assert!(
                fm.move_file_to_primary_volume(file_id, &low_volume)
                    .unwrap()
            );
            file_id
        };
        let l0_file = create_on_low(128);
        let l3_file = create_on_low(256);
        let vlog_file = create_on_low(384);
        let unreferenced_file = create_on_low(512);

        let referenced_priorities = HashMap::from([
            (l0_file, crate::file::lsm_file_priority_for_level(0)),
            (l3_file, crate::file::lsm_file_priority_for_level(3)),
            (vlog_file, crate::file::VLOG_FILE_PRIORITY),
        ]);
        let mut excluded = HashSet::new();
        assert_eq!(
            fm.select_backfill_candidate_with_exclusions(
                &high_volume,
                &referenced_priorities,
                &excluded,
                u64::MAX,
            ),
            Some(l0_file)
        );
        excluded.insert(l0_file);
        assert_eq!(
            fm.select_backfill_candidate_with_exclusions(
                &high_volume,
                &referenced_priorities,
                &excluded,
                u64::MAX,
            ),
            Some(l3_file)
        );
        excluded.insert(l3_file);
        assert_eq!(
            fm.select_backfill_candidate_with_exclusions(
                &high_volume,
                &referenced_priorities,
                &excluded,
                u64::MAX,
            ),
            Some(vlog_file)
        );
        excluded.insert(vlog_file);
        assert_eq!(
            fm.select_backfill_candidate_with_exclusions(
                &high_volume,
                &referenced_priorities,
                &excluded,
                u64::MAX,
            ),
            None
        );
        assert!(!referenced_priorities.contains_key(&unreferenced_file));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_readonly_load_prefers_high_priority_target_when_low_precedes_high() {
        let dir = tempfile::tempdir().unwrap();
        let high_root = dir.path().join("high");
        let low_root = dir.path().join("low");
        let readonly_root = dir.path().join("readonly");
        std::fs::create_dir_all(readonly_root.join("db/data")).unwrap();

        let mut high = crate::VolumeDescriptor::new(
            format!("file://{}", high_root.display()),
            vec![VolumeUsageKind::PrimaryDataPriorityHigh],
        );
        high.size_limit = Some(Size::from_const(200));
        let low = crate::VolumeDescriptor::new(
            format!("file://{}", low_root.display()),
            vec![VolumeUsageKind::PrimaryDataPriorityLow],
        );
        let readonly = crate::VolumeDescriptor::new(
            format!("file://{}", readonly_root.display()),
            vec![VolumeUsageKind::Readonly],
        );
        let config = Config {
            // Flink puts the checkpoint-backed low tier before the local high tier. Target
            // selection must follow priority, not this descriptor order.
            volumes: vec![low, high, readonly],
            file_transfer_concurrency: 1,
            base_file_size: Size::from_const(64),
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-readonly-load"));
        let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());

        let register_readonly = |file_id: FileId, size: usize| {
            let path = readonly_root.join(format!("db/data/{file_id}.sst"));
            std::fs::write(&path, vec![b'x'; size]).unwrap();
            fm.register_data_file_readonly(file_id, &format!("file://{}", path.display()))
                .unwrap();
            Arc::new(crate::data_file::DataFile::new(
                crate::data_file::DataFileType::SSTable,
                vec![file_id as u8],
                vec![file_id as u8 + 1],
                file_id,
                crate::file::TrackedFileId::new(&fm, file_id),
                0,
                size,
                0u16..=0u16,
                0u16..=0u16,
            ))
        };
        let l0_file = register_readonly(101, 256);
        let l3_file = register_readonly(102, 64);
        let added_after_mark = register_readonly(103, 32);
        let vlog_file = register_readonly(104, 32);
        let old_l0_readonly_tracking = fm.preferred_tracked_file(101).unwrap();
        let source_paths = [101, 102, 103, 104].map(|file_id| {
            (
                file_id,
                readonly_root.join(format!("db/data/{file_id}.sst")),
            )
        });

        let db_state = Arc::new(crate::db_state::DbStateHandle::new());
        let initial = db_state.load();
        db_state.store(crate::db_state::DbState {
            seq_id: initial.seq_id,
            bucket_ranges: vec![0u16..=0u16],
            multi_lsm_version: crate::db_state::MultiLSMTreeVersion::new(
                crate::lsm::LSMTreeVersion {
                    levels: vec![
                        crate::lsm::Level {
                            ordinal: 0,
                            tiered: true,
                            files: vec![Arc::clone(&l0_file)],
                        },
                        crate::lsm::Level {
                            ordinal: 3,
                            tiered: false,
                            files: vec![Arc::clone(&l3_file)],
                        },
                    ],
                },
            ),
            vlog_version: crate::vlog::VlogVersion::from_files_with_entries(vec![(
                0,
                crate::file::TrackedFileId::new(&fm, vlog_file.file_id),
                1,
            )]),
            active: initial.active.clone(),
            immutables: initial.immutables.clone(),
            truncation_cursors: initial.truncation_cursors.clone(),
            suggested_base_snapshot_id: None,
        });
        assert_eq!(
            fm.mark_readonly_files_for_primary_load(&db_state, None, false),
            3
        );

        let marked = db_state.load();
        db_state.store(crate::db_state::DbState {
            seq_id: marked.seq_id,
            bucket_ranges: marked.bucket_ranges.clone(),
            multi_lsm_version: crate::db_state::MultiLSMTreeVersion::new(
                crate::lsm::LSMTreeVersion {
                    levels: vec![
                        crate::lsm::Level {
                            ordinal: 0,
                            tiered: true,
                            files: vec![l0_file, added_after_mark],
                        },
                        crate::lsm::Level {
                            ordinal: 3,
                            tiered: false,
                            files: vec![l3_file],
                        },
                    ],
                },
            ),
            vlog_version: marked.vlog_version.clone(),
            active: marked.active.clone(),
            immutables: marked.immutables.clone(),
            truncation_cursors: marked.truncation_cursors.clone(),
            suggested_base_snapshot_id: None,
        });

        assert_eq!(fm.trigger_primary_tiering_if_needed(&db_state).unwrap(), 1);
        assert!(fm.wait_for_offload_idle(Duration::from_secs(5)));
        assert_eq!(
            fm.preferred_tracked_file(101).and_then(|tracked| {
                tracked.volume.as_ref().map(|volume| volume.priority.rank())
            }),
            Some(1),
            "the highest-priority L0 file should fall back to low when high cannot fit it"
        );
        assert!(
            !Arc::ptr_eq(
                &fm.preferred_tracked_file(101).unwrap(),
                &old_l0_readonly_tracking
            ),
            "the old READONLY TrackedFile must no longer be preferred"
        );

        assert_eq!(fm.trigger_primary_tiering_if_needed(&db_state).unwrap(), 1);
        assert!(fm.wait_for_offload_idle(Duration::from_secs(5)));
        assert_eq!(
            fm.preferred_tracked_file(102).and_then(|tracked| {
                tracked.volume.as_ref().map(|volume| volume.priority.rank())
            }),
            Some(3),
            "the next marked file should use high when it fits"
        );
        assert_eq!(fm.trigger_primary_tiering_if_needed(&db_state).unwrap(), 1);
        assert!(fm.wait_for_offload_idle(Duration::from_secs(5)));
        assert_eq!(
            fm.preferred_tracked_file(104).and_then(|tracked| {
                tracked.volume.as_ref().map(|volume| volume.priority.rank())
            }),
            Some(3),
            "the VLog file should load only after all marked LSM files"
        );
        assert!(
            fm.preferred_tracked_file(103).is_some_and(|tracked| {
                tracked
                    .volume
                    .as_ref()
                    .is_some_and(|volume| volume.readonly_source)
            }),
            "a file that became current after marking must remain unmarked"
        );
        for (_, path) in source_paths {
            assert!(
                path.exists(),
                "loading must never delete the original READONLY file"
            );
        }
    }

    #[test]
    fn readonly_load_pins_eligible_sst_after_promotion_and_reuses_it() {
        let dir = tempfile::tempdir().unwrap();
        let primary_root = dir.path().join("primary");
        let readonly_root = dir.path().join("readonly");
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}", primary_root.display()),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}", readonly_root.display()),
                    vec![VolumeUsageKind::Readonly],
                ),
            ],
            sst_pinned_metadata_max_level: Some(0),
            sst_pinned_metadata_partitions_enabled: true,
            ..Config::default()
        };
        let fm = Arc::new(
            FileManager::from_config(
                &config,
                "readonly-load-pin",
                Arc::new(MetricsManager::new("readonly-load-pin")),
            )
            .unwrap(),
        );
        let data_file = register_readonly_sst(&fm, &readonly_root, 301, true);
        let db_state = Arc::new(DbStateHandle::new());
        store_readonly_sst_state(&db_state, 0, Arc::clone(&data_file));

        assert_eq!(
            fm.mark_readonly_files_for_primary_load(
                &db_state,
                config.sst_pinned_metadata_max_level,
                config.sst_pinned_metadata_partitions_enabled,
            ),
            1
        );
        assert_eq!(fm.trigger_primary_tiering_if_needed(&db_state).unwrap(), 1);
        assert!(fm.wait_for_offload_idle(Duration::from_secs(5)));
        assert!(fm.is_data_file_on_primary_volume(data_file.file_id));

        let pin = data_file
            .pinned_sst_read_metadata()
            .expect("eligible SST should be pinned after promotion");
        assert!(pin.index_partition(0).unwrap().is_some());
        assert!(pin.filter_partition(0).unwrap().is_some());

        let reader = fm.open_data_file_reader(data_file.file_id).unwrap();
        let reused = PinnedSstReadMetadata::get_or_load(&reader, data_file.as_ref(), false, false)
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&pin, &reused));
        assert_eq!(
            SSTPointReader::get_exact(
                Box::new(fm.open_data_file_reader(data_file.file_id).unwrap()),
                data_file.as_ref(),
                SSTIteratorOptions {
                    bloom_filter_enabled: true,
                    ..SSTIteratorOptions::default()
                },
                None,
                b"key002",
            )
            .unwrap()
            .as_deref(),
            Some(b"value".as_slice())
        );
    }

    #[test]
    fn readonly_load_keeps_ineligible_and_existing_pins_as_noops() {
        let dir = tempfile::tempdir().unwrap();
        let primary_root = dir.path().join("primary");
        let readonly_root = dir.path().join("readonly");
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}", primary_root.display()),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}", readonly_root.display()),
                    vec![VolumeUsageKind::Readonly],
                ),
            ],
            ..Config::default()
        };
        let fm = Arc::new(
            FileManager::from_config(
                &config,
                "readonly-load-noop",
                Arc::new(MetricsManager::new("readonly-load-noop")),
            )
            .unwrap(),
        );
        let data_file = register_readonly_sst(&fm, &readonly_root, 302, false);
        let db_state = Arc::new(DbStateHandle::new());
        store_readonly_sst_state(&db_state, 1, Arc::clone(&data_file));

        assert_eq!(
            fm.mark_readonly_files_for_primary_load(&db_state, Some(0), true),
            1
        );
        assert_eq!(fm.trigger_primary_tiering_if_needed(&db_state).unwrap(), 1);
        assert!(fm.wait_for_offload_idle(Duration::from_secs(5)));
        assert!(fm.is_data_file_on_primary_volume(data_file.file_id));
        assert!(
            data_file.pinned_sst_read_metadata().is_none(),
            "an ineligible level must not be pinned by promotion"
        );

        let reader = fm.open_data_file_reader(data_file.file_id).unwrap();
        let pin = PinnedSstReadMetadata::get_or_load(&reader, data_file.as_ref(), true, false)
            .unwrap()
            .unwrap();
        fm.pin_promoted_readonly_sst_metadata(data_file.file_id, &db_state, true);
        let reused = data_file.pinned_sst_read_metadata().unwrap();
        assert!(Arc::ptr_eq(&pin, &reused));

        let ineligible = referenced_readonly_load_requests(&db_state, Some(0), true);
        assert!(!ineligible.get(&data_file.file_id).unwrap().pin_metadata);
        assert!(!ineligible.get(&data_file.file_id).unwrap().pin_partitions);

        let disabled = referenced_readonly_load_requests(&db_state, None, true);
        assert!(!disabled.get(&data_file.file_id).unwrap().pin_metadata);
        assert!(!disabled.get(&data_file.file_id).unwrap().pin_partitions);
    }

    #[test]
    fn readonly_load_request_uses_lsm_level_and_sst_type_for_pinning() {
        let sst = Arc::new(DataFile::new_detached(
            DataFileType::SSTable,
            vec![0],
            vec![1],
            304,
            0,
            1,
            0u16..=0u16,
            0u16..=0u16,
        ));
        let parquet = Arc::new(DataFile::new_detached(
            DataFileType::Parquet,
            vec![1],
            vec![2],
            305,
            0,
            1,
            0u16..=0u16,
            0u16..=0u16,
        ));
        let db_state = Arc::new(DbStateHandle::new());
        let current = db_state.load();
        db_state.store(crate::db_state::DbState {
            seq_id: current.seq_id,
            bucket_ranges: vec![0u16..=0u16],
            multi_lsm_version: crate::db_state::MultiLSMTreeVersion::new(
                crate::lsm::LSMTreeVersion {
                    levels: vec![
                        crate::lsm::Level {
                            ordinal: 0,
                            tiered: true,
                            files: vec![Arc::clone(&sst), parquet],
                        },
                        crate::lsm::Level {
                            ordinal: 3,
                            tiered: false,
                            files: vec![sst],
                        },
                    ],
                },
            ),
            vlog_version: crate::vlog::VlogVersion::new(),
            active: current.active.clone(),
            immutables: current.immutables.clone(),
            truncation_cursors: current.truncation_cursors.clone(),
            suggested_base_snapshot_id: None,
        });

        let requests = referenced_readonly_load_requests(&db_state, Some(0), true);
        assert!(requests.get(&304).unwrap().pin_metadata);
        assert!(requests.get(&304).unwrap().pin_partitions);
        assert!(!requests.get(&305).unwrap().pin_metadata);
        assert!(!requests.get(&305).unwrap().pin_partitions);

        let disabled = referenced_readonly_load_requests(&db_state, None, true);
        assert!(!disabled.get(&304).unwrap().pin_metadata);
        assert!(!disabled.get(&304).unwrap().pin_partitions);
        assert!(!disabled.get(&305).unwrap().pin_metadata);
        assert!(!disabled.get(&305).unwrap().pin_partitions);

        let mut invalid = ReadonlyLoadRequest {
            priority: 0,
            pin_metadata: false,
            pin_partitions: false,
        };
        invalid.merge(ReadonlyLoadRequest {
            priority: 1,
            pin_metadata: false,
            pin_partitions: true,
        });
        assert!(!invalid.pin_metadata);
        assert!(!invalid.pin_partitions);
    }

    #[test]
    fn readonly_load_pin_failure_keeps_promotion_and_allows_foreground_retry() {
        let dir = tempfile::tempdir().unwrap();
        let primary_root = dir.path().join("primary");
        let readonly_root = dir.path().join("readonly");
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}", primary_root.display()),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}", readonly_root.display()),
                    vec![VolumeUsageKind::Readonly],
                ),
            ],
            ..Config::default()
        };
        let fm = Arc::new(
            FileManager::from_config(
                &config,
                "readonly-load-pin-failure",
                Arc::new(MetricsManager::new("readonly-load-pin-failure")),
            )
            .unwrap(),
        );
        let data_file = register_readonly_sst(&fm, &readonly_root, 303, false);
        let db_state = Arc::new(DbStateHandle::new());
        store_readonly_sst_state(&db_state, 0, Arc::clone(&data_file));

        let target = fm.primary_volume_by_rank(3).unwrap();
        let mut progress = |_| {};
        let mut rollback = || {};
        assert!(
            fm.move_file_to_primary_volume_with_progress(
                data_file.file_id,
                &target,
                &mut progress,
                &mut rollback,
            )
            .unwrap()
        );
        assert!(fm.is_data_file_on_primary_volume(data_file.file_id));

        let cache_key = fm.preferred_replica_key(data_file.file_id).unwrap();
        fm.reader_cache.lock().unwrap().insert(
            cache_key.clone(),
            Arc::new(FailingReader {
                size: data_file.size,
            }),
        );
        fm.pin_promoted_readonly_sst_metadata(data_file.file_id, &db_state, false);
        assert!(data_file.pinned_sst_read_metadata().is_none());
        assert!(fm.is_data_file_on_primary_volume(data_file.file_id));

        fm.reader_cache.lock().unwrap().remove(&cache_key);
        assert_eq!(
            SSTPointReader::get_exact(
                Box::new(fm.open_data_file_reader(data_file.file_id).unwrap()),
                data_file.as_ref(),
                SSTIteratorOptions {
                    bloom_filter_enabled: true,
                    pin_metadata: true,
                    ..SSTIteratorOptions::default()
                },
                None,
                b"key001",
            )
            .unwrap()
            .as_deref(),
            Some(b"value".as_slice())
        );
        assert!(data_file.pinned_sst_read_metadata().is_some());
    }

    #[test]
    fn test_readonly_load_keeps_mark_when_no_primary_volume_can_fit() {
        let dir = tempfile::tempdir().unwrap();
        let primary_root = dir.path().join("primary");
        let readonly_root = dir.path().join("readonly");
        std::fs::create_dir_all(readonly_root.join("db/data")).unwrap();
        let source_path = readonly_root.join("db/data/201.sst");
        std::fs::write(&source_path, vec![b'x'; 128]).unwrap();

        let mut primary = crate::VolumeDescriptor::new(
            format!("file://{}", primary_root.display()),
            vec![VolumeUsageKind::PrimaryDataPriorityHigh],
        );
        primary.size_limit = Some(Size::from_const(100));
        let readonly = crate::VolumeDescriptor::new(
            format!("file://{}", readonly_root.display()),
            vec![VolumeUsageKind::Readonly],
        );
        let config = Config {
            volumes: vec![primary, readonly],
            base_file_size: Size::from_const(32),
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("readonly-load-no-space"));
        let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
        fm.register_data_file_readonly(201, &format!("file://{}", source_path.display()))
            .unwrap();
        let data_file = Arc::new(crate::data_file::DataFile::new(
            crate::data_file::DataFileType::SSTable,
            vec![0],
            vec![1],
            201,
            crate::file::TrackedFileId::new(&fm, 201),
            0,
            128,
            0u16..=0u16,
            0u16..=0u16,
        ));
        let db_state = Arc::new(crate::db_state::DbStateHandle::new());
        let current = db_state.load();
        db_state.store(crate::db_state::DbState {
            seq_id: current.seq_id,
            bucket_ranges: vec![0u16..=0u16],
            multi_lsm_version: crate::db_state::MultiLSMTreeVersion::new(
                crate::lsm::LSMTreeVersion {
                    levels: vec![crate::lsm::Level {
                        ordinal: 0,
                        tiered: true,
                        files: vec![data_file],
                    }],
                },
            ),
            vlog_version: crate::vlog::VlogVersion::new(),
            active: current.active.clone(),
            immutables: current.immutables.clone(),
            truncation_cursors: current.truncation_cursors.clone(),
            suggested_base_snapshot_id: None,
        });

        assert_eq!(
            fm.mark_readonly_files_for_primary_load(&db_state, None, false),
            1
        );
        assert_eq!(fm.trigger_primary_tiering_if_needed(&db_state).unwrap(), 0);
        assert!(
            fm.offload_runtime.pending_readonly_loads.contains_key(&201),
            "a capacity-blocked load must remain marked for a later retry"
        );
        assert!(fm.preferred_tracked_file(201).is_some_and(|tracked| {
            tracked
                .volume
                .as_ref()
                .is_some_and(|volume| volume.readonly_source)
        }));
        assert!(source_path.exists());
        let worker = fm.start_primary_tiering_worker(&db_state).unwrap();
        assert!(
            worker.is_some(),
            "READONLY loading needs a scanner even with one primary tier"
        );
        let worker = worker.unwrap();
        worker.stop();
        worker.join();
    }

    #[test]
    fn test_move_deletes_uncommitted_target_when_file_is_removed_during_copy() {
        let dir = tempfile::tempdir().unwrap();
        let source_root = dir.path().join("source");
        let target_root = dir.path().join("target");
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}", source_root.display()),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}", target_root.display()),
                    vec![VolumeUsageKind::PrimaryDataPriorityLow],
                ),
            ],
            base_file_size: Size::from_const(64),
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("move-delete-during-copy"));
        let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
        let (file_id, mut writer) = fm.create_data_file().unwrap();
        writer.write(&vec![b'x'; 1024]).unwrap();
        writer.close().unwrap();
        drop(writer);
        let source_path = fm.get_data_file_path(file_id).unwrap();
        let source_fs = fm.primary_volume_by_rank(3).unwrap().fs().clone();
        let target_volume = fm.primary_volume_by_rank(1).unwrap();
        let target_fs = target_volume.fs().clone();
        let mut copied_target_path = None;
        let mut removed = false;
        let moved = {
            let mut progress = |_: u64| {
                if removed {
                    return;
                }
                let target_name = target_fs
                    .list("db/data")
                    .unwrap()
                    .into_iter()
                    .next()
                    .expect("copy target should exist before commit");
                copied_target_path = Some(format!("db/data/{target_name}"));
                fm.remove_data_file(file_id).unwrap();
                removed = true;
            };
            fm.move_file_to_primary_volume_with_progress(
                file_id,
                &target_volume,
                &mut progress,
                &mut || {},
            )
            .unwrap()
        };

        assert!(!moved, "a logically removed file must not commit its copy");
        assert!(!fm.has_data_file(file_id));
        let copied_target_path = copied_target_path.expect("copy target path");
        test_utils::wait_for_file_deletion(&target_fs, &copied_target_path);
        assert!(
            !target_fs.exists(&copied_target_path).unwrap(),
            "the uncommitted target copy must be deleted"
        );
        test_utils::wait_for_file_deletion(&source_fs, &source_path);
        assert!(
            !source_fs.exists(&source_path).unwrap(),
            "the owned source must also follow normal lifecycle deletion"
        );
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_primary_tiering_worker_backfills_only_current_lsm_files() {
        let root = "/tmp/file_manager_backfill_worker";
        let _ = std::fs::remove_dir_all(root);
        let high = crate::VolumeDescriptor::new(
            format!("file://{root}/high"),
            vec![VolumeUsageKind::PrimaryDataPriorityHigh],
        );
        let low = crate::VolumeDescriptor::new(
            format!("file://{root}/low"),
            vec![VolumeUsageKind::PrimaryDataPriorityLow],
        );
        let config = Config {
            volumes: vec![high, low],
            base_file_size: Size::from_const(64),
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-backfill-worker"));
        let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
        let low_volume = fm.primary_volume_by_rank(1).unwrap();

        let create_on_low = |size: usize| {
            let (file_id, mut writer) = fm.create_data_file().unwrap();
            writer.write(&vec![b'x'; size]).unwrap();
            writer.close().unwrap();
            assert!(
                fm.move_file_to_primary_volume(file_id, &low_volume)
                    .unwrap()
            );
            file_id
        };
        let l0_file_id = create_on_low(256);
        let vlog_file_id = create_on_low(256);
        let unreferenced_file_id = create_on_low(256);

        let data_file = Arc::new(crate::data_file::DataFile::new(
            crate::data_file::DataFileType::SSTable,
            vec![0],
            vec![1],
            l0_file_id,
            crate::file::TrackedFileId::new(&fm, l0_file_id),
            0,
            256,
            0u16..=0u16,
            0u16..=0u16,
        ));
        let db_state = Arc::new(crate::db_state::DbStateHandle::new());
        let current = db_state.load();
        db_state.store(crate::db_state::DbState {
            seq_id: current.seq_id,
            bucket_ranges: vec![0u16..=0u16],
            multi_lsm_version: crate::db_state::MultiLSMTreeVersion::new(
                crate::lsm::LSMTreeVersion {
                    levels: vec![crate::lsm::Level {
                        ordinal: 0,
                        tiered: true,
                        files: vec![data_file],
                    }],
                },
            ),
            vlog_version: crate::vlog::VlogVersion::from_files_with_entries(vec![(
                0,
                crate::file::TrackedFileId::new(&fm, vlog_file_id),
                1,
            )]),
            active: current.active.clone(),
            immutables: current.immutables.clone(),
            truncation_cursors: current.truncation_cursors.clone(),
            suggested_base_snapshot_id: None,
        });
        let referenced_priorities = referenced_primary_file_priorities(&db_state);
        assert_eq!(
            referenced_priorities.get(&l0_file_id),
            Some(&crate::file::lsm_file_priority_for_level(0))
        );
        assert_eq!(
            referenced_priorities.get(&vlog_file_id),
            Some(&crate::file::VLOG_FILE_PRIORITY)
        );
        assert!(!referenced_priorities.contains_key(&unreferenced_file_id));

        let worker = fm.start_primary_tiering_worker(&db_state).unwrap().unwrap();
        let moved = (0..100).any(|_| {
            let on_high = fm
                .preferred_tracked_file(l0_file_id)
                .and_then(|tracked| {
                    tracked
                        .volume
                        .as_ref()
                        .map(|volume| volume.priority.rank() == 3)
                })
                .unwrap_or(false);
            if !on_high {
                std::thread::sleep(Duration::from_millis(20));
            }
            on_high
        });
        worker.stop();
        worker.join();
        assert!(fm.wait_for_offload_idle(Duration::from_secs(20)));

        assert!(moved, "the current L0 file should be backfilled");
        assert_eq!(
            fm.preferred_tracked_file(vlog_file_id).and_then(|tracked| {
                tracked.volume.as_ref().map(|volume| volume.priority.rank())
            }),
            Some(3),
            "a referenced VLOG file should be eligible after LSM files"
        );
        assert_eq!(
            fm.preferred_tracked_file(unreferenced_file_id)
                .and_then(|tracked| {
                    tracked.volume.as_ref().map(|volume| volume.priority.rank())
                }),
            Some(1),
            "an unreferenced tracked file must remain on the low-priority volume"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_create_data_file_with_offload_triggers_background_offload() {
        let root = "/tmp/file_manager_offload_trigger_watermark";
        let _ = std::fs::remove_dir_all(root);
        let high_url = format!("file://{}/high", root);
        let low_url = format!("file://{}/low", root);
        let registry = FileSystemRegistry::new();
        let high_fs = registry.get_or_register(high_url.clone()).unwrap();
        let low_fs = registry.get_or_register(low_url.clone()).unwrap();
        let mut high =
            crate::VolumeDescriptor::new(high_url, vec![VolumeUsageKind::PrimaryDataPriorityHigh]);
        high.size_limit = Some(Size::from_kib(1));
        let low =
            crate::VolumeDescriptor::new(low_url, vec![VolumeUsageKind::PrimaryDataPriorityLow]);
        let config = Config {
            volumes: vec![high, low],
            base_file_size: Size::from_const(64),
            primary_volume_write_stop_watermark: 0.95,
            primary_volume_offload_trigger_watermark: 0.5,
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-watermark"));
        let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());

        let (file_id, mut writer) = fm.create_data_file().unwrap();
        writer.write(&vec![b'x'; 600]).unwrap();
        writer.close().unwrap();
        let old_path = fm.get_data_file_path(file_id).unwrap();
        assert!(high_fs.exists(&old_path).unwrap());

        let (_new_id, mut new_writer) = fm.create_data_file_with_offload().unwrap();
        new_writer.write(b"small").unwrap();
        new_writer.close().unwrap();

        assert!(fm.wait_for_offload_idle(Duration::from_secs(20)));
        let new_path = fm.get_data_file_path(file_id).unwrap();
        assert_ne!(new_path, old_path);
        assert!(low_fs.exists(&new_path).unwrap());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_trigger_offload_loops_until_projected_watermark_recovers() {
        let root = "/tmp/file_manager_offload_loop_trigger";
        let _ = std::fs::remove_dir_all(root);
        let high_url = format!("file://{}/high", root);
        let low_url = format!("file://{}/low", root);
        let registry = FileSystemRegistry::new();
        let low_fs = registry.get_or_register(low_url.clone()).unwrap();
        let mut high =
            crate::VolumeDescriptor::new(high_url, vec![VolumeUsageKind::PrimaryDataPriorityHigh]);
        high.size_limit = Some(Size::from_const(1200));
        let low =
            crate::VolumeDescriptor::new(low_url, vec![VolumeUsageKind::PrimaryDataPriorityLow]);
        let config = Config {
            volumes: vec![high, low],
            base_file_size: Size::from_const(64),
            primary_volume_write_stop_watermark: 0.95,
            primary_volume_offload_trigger_watermark: 0.4,
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-loop-trigger"));
        let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
        let mut file_ids = Vec::new();
        for _ in 0..3 {
            let (file_id, mut writer) = fm.create_data_file().unwrap();
            writer.write(&vec![b'x'; 300]).unwrap();
            writer.close().unwrap();
            file_ids.push(file_id);
        }

        let scheduled = fm.trigger_offload_if_needed().unwrap();
        assert_eq!(scheduled, 2);
        assert!(fm.wait_for_offload_idle(Duration::from_secs(20)));

        let moved_to_low = file_ids
            .iter()
            .filter(|file_id| {
                let path = fm.get_data_file_path(**file_id).unwrap();
                low_fs.exists(&path).unwrap_or(false)
            })
            .count();
        assert!(moved_to_low >= 2);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_trigger_offload_uses_planned_bytes_to_avoid_overscheduling() {
        let root = "/tmp/file_manager_offload_planned_backpressure";
        let _ = std::fs::remove_dir_all(root);
        let mut high = crate::VolumeDescriptor::new(
            format!("file://{}/high", root),
            vec![VolumeUsageKind::PrimaryDataPriorityHigh],
        );
        high.size_limit = Some(Size::from_kib(1));
        let low = crate::VolumeDescriptor::new(
            format!("file://{}/low", root),
            vec![VolumeUsageKind::PrimaryDataPriorityLow],
        );
        let config = Config {
            volumes: vec![high, low],
            base_file_size: Size::from_const(64),
            primary_volume_write_stop_watermark: 0.95,
            primary_volume_offload_trigger_watermark: 0.8,
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-backpressure"));
        let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
        for _ in 0..3 {
            let (_id, mut writer) = fm.create_data_file().unwrap();
            writer.write(&vec![b'x'; 300]).unwrap();
            writer.close().unwrap();
        }
        let first = fm.trigger_offload_if_needed().unwrap();
        let second = fm.trigger_offload_if_needed().unwrap();
        assert_eq!(first, 1);
        assert_eq!(second, 0);
        assert!(fm.wait_for_offload_idle(Duration::from_secs(20)));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_write_stop_watermark_blocks_new_writes() {
        let root = "/tmp/file_manager_write_stop_watermark";
        let _ = std::fs::remove_dir_all(root);
        let high_url = format!("file://{}/high", root);
        let mut high =
            crate::VolumeDescriptor::new(high_url, vec![VolumeUsageKind::PrimaryDataPriorityHigh]);
        high.size_limit = Some(Size::from_kib(1));
        let config = Config {
            volumes: vec![high],
            base_file_size: Size::from_const(64),
            primary_volume_write_stop_watermark: 0.5,
            primary_volume_offload_trigger_watermark: 0.4,
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-write-stop-watermark"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();

        let (_id, mut writer) = fm.create_data_file().unwrap();
        writer.write(&vec![b'x'; 600]).unwrap();
        writer.close().unwrap();

        let err = match fm.create_data_file() {
            Ok(_) => panic!("writes should stop after crossing write-stop watermark"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("All primary data volumes are full")
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
