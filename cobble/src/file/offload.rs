//! Schedules primary file transfers and accounts for work in flight.

use super::tracked_file::{FileId, PhysicalDeletePolicy};
use crate::Error;
use crate::config::PrimaryVolumeOffloadPolicyKind;
use crate::data_file::{DataFile, DataFileType};
use crate::db_state::DbStateHandle;
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

mod imports;
mod tiering;

use tiering::{policy_from_kind_arc, policy_from_kind_ref};

const PRIMARY_TIERING_SCAN_INTERVAL: Duration = Duration::from_secs(1);

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

    fn complete_job(&self, file_id: FileId) {
        // Pair completion with wait_idle/stop so a waiter cannot miss the final notification.
        let _idle = self.idle.lock().unwrap();
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
            // Idle means the completed job's worker slot is available to the next caller.
            drop(permit);
            runtime.complete_job(file_id);
            drop(file_manager_keepalive);
        });
        Ok(true)
    }

    pub(crate) fn wait_idle(&self, timeout: Duration) -> bool {
        let guard = self.idle.lock().unwrap();
        let (guard, _) = self
            .done
            .wait_timeout_while(guard, timeout, |_| !self.planned_jobs.is_empty())
            .unwrap();
        let idle = self.planned_jobs.is_empty();
        drop(guard);
        idle
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

    pub(crate) fn wake_signal(&self) -> Arc<Notify> {
        Arc::clone(&self.wake)
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

pub(crate) fn compare_primary_offload_file_refs(
    policy_kind: PrimaryVolumeOffloadPolicyKind,
    left: &PrimaryOffloadFileRef,
    right: &PrimaryOffloadFileRef,
) -> Ordering {
    policy_from_kind_ref(policy_kind).compare_file_refs(left, right)
}

// ----------------------------
// File Manager offload logic
// ----------------------------
impl FileManager {
    /// Creates an unowned transfer target that preserves the source basename when it is unused
    /// on the target volume. Ownership starts only after fast copy or writer creation succeeds.
    pub(crate) fn create_transfer_data_file_target_on_volume(
        &self,
        volume: &Arc<DataVolume>,
        source_path: &str,
    ) -> crate::Result<Arc<TrackedFile>> {
        let file_name = source_path
            .rsplit('/')
            .next()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                Error::InvalidState(format!(
                    "Transfer source has no data-file basename: {source_path}"
                ))
            })?;
        let preserved_path = self.data_file_path_with_name(file_name);
        if volume.fs().exists(&preserved_path).unwrap_or(true) {
            return Ok(self.create_fresh_transfer_target_on_volume(volume));
        }
        Ok(Arc::new(TrackedFile::retained(
            preserved_path,
            Arc::clone(volume.fs()),
            Some(Arc::clone(volume)),
        )))
    }

    pub(crate) fn create_fresh_transfer_target_on_volume(
        &self,
        volume: &Arc<DataVolume>,
    ) -> Arc<TrackedFile> {
        Arc::new(TrackedFile::retained(
            self.data_file_path(0),
            Arc::clone(volume.fs()),
            Some(Arc::clone(volume)),
        ))
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
        let new_tracked = match self.transfer_data_file_to_volume(
            source_tracked.fs(),
            source_tracked.path(),
            target_volume,
            None,
            progress,
        ) {
            Ok(target) => target,
            Err(err) => {
                rollback();
                return Err(err);
            }
        };
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
