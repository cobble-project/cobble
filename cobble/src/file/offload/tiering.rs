//! Candidate selection and planning for primary tiering and READONLY promotions.

use super::{
    FileId, OffloadRuntime, PrimaryOffloadFileRef, PrimaryOffloadPolicy, ReadonlyLoadRequest,
    VolumePressure, referenced_primary_file_priorities, referenced_vlog_file_ids,
};
use crate::config::PrimaryVolumeOffloadPolicyKind;
use crate::data_file::DataFileType;
use crate::db_state::DbStateHandle;
use crate::file::logical_file::ReplicaOrigin;
use crate::file::tracked_file::PhysicalDeletePolicy;
use crate::file::{DataVolume, FileManager, PrimaryDataPlacement, TrackedFile};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const MAX_BACKFILL_TRIGGER_WATERMARK: f64 = 0.80;
const MIN_OFFLOAD_BACKFILL_GAP: f64 = 0.01;

fn effective_backfill_trigger_watermark(requested: f64, offload_trigger: f64) -> f64 {
    requested
        .min(MAX_BACKFILL_TRIGGER_WATERMARK)
        .min((offload_trigger - MIN_OFFLOAD_BACKFILL_GAP).max(0.0))
}

static LARGEST_FILE_OFFLOAD_POLICY: LargestFileOffloadPolicy = LargestFileOffloadPolicy;
static PRIORITY_OFFLOAD_POLICY: PriorityOffloadPolicy = PriorityOffloadPolicy;

pub(super) fn policy_from_kind_arc(
    policy_kind: PrimaryVolumeOffloadPolicyKind,
) -> Arc<dyn PrimaryOffloadPolicy> {
    match policy_kind {
        PrimaryVolumeOffloadPolicyKind::LargestFile => Arc::new(LargestFileOffloadPolicy),
        PrimaryVolumeOffloadPolicyKind::Priority => Arc::new(PriorityOffloadPolicy),
    }
}

pub(super) fn policy_from_kind_ref(
    policy_kind: PrimaryVolumeOffloadPolicyKind,
) -> &'static dyn PrimaryOffloadPolicy {
    match policy_kind {
        PrimaryVolumeOffloadPolicyKind::LargestFile => &LARGEST_FILE_OFFLOAD_POLICY,
        PrimaryVolumeOffloadPolicyKind::Priority => &PRIORITY_OFFLOAD_POLICY,
    }
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

impl OffloadRuntime {
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
}

impl FileManager {
    pub(crate) fn trigger_offload_if_needed(self: &Arc<Self>) -> crate::Result<usize> {
        let _planning = self.offload_runtime.planning.lock().unwrap();
        self.trigger_offload_if_needed_locked()
    }

    pub(super) fn trigger_offload_if_needed_locked(self: &Arc<Self>) -> crate::Result<usize> {
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

    pub(super) fn trigger_readonly_loads_locked(
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

    pub(super) fn trigger_backfill_if_needed_locked(
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

#[cfg(test)]
#[path = "../../../tests/unit/file/offload_tiering.rs"]
mod tests;
