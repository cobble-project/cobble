//! Background transfers for imported replicas and persistent import caches.

use super::{
    FileId, OffloadJobPlan, PrimaryTieringDirection, projected_source_release_bytes,
    referenced_primary_file_priorities, referenced_vlog_file_ids,
};
use crate::Error;
use crate::db_state::DbStateHandle;
use crate::file::logical_file::{ReplicaLifecycle, ReplicaOrigin};
use crate::file::{DataVolume, FileManager, PrimaryDataPlacement};
use log::warn;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

impl FileManager {
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
        let owned = self.transfer_data_file_to_volume(
            source.fs(),
            source.path(),
            target_volume,
            None,
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
        let owned = self.transfer_data_file_to_volume(
            source.fs(),
            source.path(),
            target_volume,
            None,
            progress,
        )?;
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
            let owned = match self.transfer_data_file_to_volume(
                source.fs(),
                source.path(),
                target_volume,
                None,
                progress,
            ) {
                Ok(owned) => owned,
                Err(err) => {
                    rollback();
                    return Err(err);
                }
            };
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

    pub(super) fn move_persistent_caches_if_needed_locked(
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

    pub(super) fn evict_persistent_caches_if_needed_locked(
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

    /// Current file-route dependencies, shared by adoption progress and snapshot
    /// retention. Historical snapshot dependencies are tracked separately.
    pub(crate) fn referenced_import_leases(
        &self,
        db_state: &DbStateHandle,
    ) -> std::collections::BTreeSet<String> {
        referenced_primary_file_priorities(db_state)
            .into_keys()
            .filter_map(|file_id| match self.preferred_replica_origin(file_id) {
                Some(ReplicaOrigin::ExternalLeased { export_id }) => Some(export_id),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/file/offload_imports.rs"]
mod tests;
