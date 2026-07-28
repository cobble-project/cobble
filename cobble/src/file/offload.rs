use super::file_manager::FileId;
use crate::Error;
use crate::config::PrimaryVolumeOffloadPolicyKind;
use crate::db_state::DbStateHandle;
use crate::file::{DataVolume, FileManager, TrackedFile, TrackedWriter};
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

pub(crate) struct OffloadRuntime {
    accepting: AtomicBool,
    semaphore: Arc<Semaphore>,
    planned_jobs: Arc<DashMap<FileId, OffloadJobPlan>>,
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
        if distinct_primary_ranks.len() < 2 {
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
                    .data_files
                    .get(&file_id)
                    .map(|entry| {
                        entry
                            .value()
                            .size_bytes
                            .load(std::sync::atomic::Ordering::SeqCst)
                    })
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
        self.trigger_backfill_if_needed_locked(db_state)
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
                    .data_files
                    .get(&file_id)
                    .map(|entry| entry.value().size_bytes())
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
            Some((Arc::downgrade(db_state), max_target_used_bytes)),
        )
    }

    fn schedule_primary_move(
        self: &Arc<Self>,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
        backfill_guard: Option<(std::sync::Weak<DbStateHandle>, u64)>,
    ) -> crate::Result<bool> {
        let target_volume = Arc::clone(target_volume);
        let target_volume_for_job = Arc::clone(&target_volume);
        let source_tracked = self
            .data_files
            .get(&file_id)
            .map(|entry| Arc::clone(entry.value()))
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
        let direction = if source_volume.priority.rank() > target_volume.priority.rank() {
            PrimaryTieringDirection::Offload
        } else {
            PrimaryTieringDirection::Backfill
        };
        let projected_source_release_bytes =
            projected_source_release_bytes(source_tracked.as_ref());
        let reserved_incoming_bytes =
            if self.has_snapshot_replica_on_primary_volume(file_id, &target_volume) {
                0
            } else {
                estimated_bytes
            };
        let max_target_used_bytes = backfill_guard
            .as_ref()
            .map(|(_, max_target_used_bytes)| *max_target_used_bytes)
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
        let handler = Arc::new(move |scheduled_file_id| {
            if let Some(manager) = manager.upgrade() {
                if let Some((referenced_state, max_target_used_bytes)) = &backfill_guard {
                    let Some(db_state) = referenced_state.upgrade() else {
                        manager.record_offload_noop();
                        return;
                    };
                    if !referenced_primary_file_priorities(&db_state)
                        .contains_key(&scheduled_file_id)
                    {
                        manager.record_offload_noop();
                        return;
                    }
                    if manager
                        .offload_runtime
                        .projected_target_physical_bytes(&target_volume_for_job)
                        > *max_target_used_bytes
                    {
                        manager.record_offload_noop();
                        return;
                    }
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
                    Ok(true) => {}
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
            .data_files
            .iter()
            .filter_map(|entry| {
                if excluded_file_ids.contains(entry.key()) {
                    return None;
                }
                let tracked = entry.value();
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
        self.data_files
            .iter()
            .filter_map(|entry| {
                let file_id = *entry.key();
                if excluded_file_ids.contains(&file_id)
                    || self.offload_runtime.is_queued_or_running(file_id)
                {
                    return None;
                }
                let priority = *referenced_priorities.get(&file_id)?;
                let tracked = entry.value();
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
        self.data_files
            .get(&snapshot_replica_file_id)
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
            .data_files
            .get(&file_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| Error::IoError(format!("Data file {} is not tracked", file_id)))?;
        let Some(source_volume) = &source_tracked.volume else {
            return Ok(false);
        };
        if !source_volume.supports_primary_data || !target_volume.supports_primary_data {
            return Ok(false);
        }
        if Arc::ptr_eq(source_volume, target_volume) {
            return Ok(false);
        }
        if let Some(snapshot_replica_file_id) = self.snapshot_replica_hint_file_id(file_id)
            && let Some(snapshot_tracked) = self
                .data_files
                .get(&snapshot_replica_file_id)
                .map(|entry| Arc::clone(entry.value()))
            && let Some(snapshot_volume) = &snapshot_tracked.volume
            && snapshot_volume.supports_primary_data
            && Arc::ptr_eq(snapshot_volume, target_volume)
        {
            snapshot_tracked.set_priority(source_tracked.priority());
            match self.data_files.entry(file_id) {
                Entry::Occupied(mut occupied) => {
                    if !Arc::ptr_eq(occupied.get(), &source_tracked) {
                        return Ok(false);
                    }
                    occupied.insert(snapshot_tracked);
                }
                Entry::Vacant(_) => return Ok(false),
            }
            if let Ok(mut cache) = self.reader_cache.lock() {
                cache.remove(&file_id);
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
        match self.data_files.entry(file_id) {
            Entry::Occupied(mut occupied) => {
                if !Arc::ptr_eq(occupied.get(), &source_tracked) {
                    rollback();
                    return Ok(false);
                }
                occupied.insert(new_tracked);
            }
            Entry::Vacant(_) => {
                rollback();
                return Ok(false);
            }
        }
        if let Ok(mut cache) = self.reader_cache.lock() {
            cache.remove(&file_id);
        }
        self.record_offload_completed_copy(copied_bytes);
        Ok(true)
    }
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
    use crate::{Config, MetricsManager, VolumeUsageKind};
    use size::Size;

    fn pressure(rank: u8) -> VolumePressure {
        VolumePressure {
            priority_rank: rank,
            used_bytes: 1,
            size_limit: Some(2),
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
        writer.write(&vec![b'x'; 128]).unwrap();
        writer.close().unwrap();

        let tracked = fm.data_files.get(&file_id).unwrap();
        assert_eq!(projected_source_release_bytes(tracked.value()), 128);
        drop(tracked);

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
                .data_files
                .get(&l0_file_id)
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
            fm.data_files.get(&vlog_file_id).and_then(|tracked| {
                tracked.volume.as_ref().map(|volume| volume.priority.rank())
            }),
            Some(3),
            "a referenced VLOG file should be eligible after LSM files"
        );
        assert_eq!(
            fm.data_files
                .get(&unreferenced_file_id)
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
