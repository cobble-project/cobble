//! Logical file and physical replica model.

use crate::data_file::DataFileType;
use crate::file::{FileId, TrackedFile};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

pub(crate) type ReplicaId = u64;

/// Properties of immutable file bytes, independent of where those bytes are stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImmutableFileMetadata {
    pub(crate) file_type: DataFileType,
    pub(crate) start_key: Vec<u8>,
    pub(crate) end_key: Vec<u8>,
    pub(crate) schema_id: u64,
    pub(crate) size: usize,
    pub(crate) bucket_range: std::ops::RangeInclusive<u16>,
    pub(crate) has_separated_values: bool,
    pub(crate) max_expired_at: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FileCommitState {
    Uncommitted,
    Committed,
    Retired,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ReplicaOrigin {
    #[default]
    Owned,
    ExternalPersistent {
        source_id: String,
    },
    ExternalLeased {
        export_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReplicaLifecycle {
    Staging,
    OwnedReady,
    PublishedTransfer,
    PendingAdoption,
    ReadonlySource,
    ReadonlyView,
    ExternalReference,
    Retiring,
}

impl ReplicaLifecycle {
    pub(crate) fn is_readable(&self) -> bool {
        matches!(
            self,
            Self::OwnedReady
                | Self::PublishedTransfer
                | Self::PendingAdoption
                | Self::ReadonlySource
                | Self::ReadonlyView
                | Self::ExternalReference
        )
    }
}

/// One concrete byte copy. `TrackedFile` carries this replica's priority, references, and
/// physical deletion behavior.
pub(crate) struct PhysicalReplica {
    pub(crate) replica_id: ReplicaId,
    pub(crate) tracked: Arc<TrackedFile>,
    lifecycle: Mutex<ReplicaLifecycle>,
    origin: ReplicaOrigin,
}

impl PhysicalReplica {
    pub(crate) fn owned(
        replica_id: ReplicaId,
        tracked: Arc<TrackedFile>,
        lifecycle: ReplicaLifecycle,
    ) -> Self {
        Self::new(replica_id, tracked, lifecycle, ReplicaOrigin::Owned)
    }

    pub(crate) fn new(
        replica_id: ReplicaId,
        tracked: Arc<TrackedFile>,
        lifecycle: ReplicaLifecycle,
        origin: ReplicaOrigin,
    ) -> Self {
        Self {
            replica_id,
            tracked,
            lifecycle: Mutex::new(lifecycle),
            origin,
        }
    }

    pub(crate) fn lifecycle(&self) -> ReplicaLifecycle {
        self.lifecycle.lock().unwrap().clone()
    }

    pub(crate) fn origin(&self) -> ReplicaOrigin {
        self.origin.clone()
    }

    fn set_lifecycle(&self, lifecycle: ReplicaLifecycle) {
        *self.lifecycle.lock().unwrap() = lifecycle;
    }

    pub(crate) fn is_readable(&self) -> bool {
        self.lifecycle().is_readable()
    }
}

/// Immutable LSM identity with its selected physical replica.
pub(crate) struct LogicalFile {
    pub(crate) file_id: FileId,
    metadata: OnceLock<ImmutableFileMetadata>,
    commit_state: Mutex<FileCommitState>,
    replica_state: RwLock<ReplicaState>,
    next_replica_id: AtomicU64,
}

struct ReplicaState {
    replicas: HashMap<ReplicaId, Arc<PhysicalReplica>>,
    preferred_read_replica: Option<ReplicaId>,
}

pub(crate) struct ReplicaStateSnapshot {
    pub(crate) preferred_replica_id: Option<ReplicaId>,
    pub(crate) replicas: Vec<Arc<PhysicalReplica>>,
}

impl LogicalFile {
    pub(crate) fn new(
        file_id: FileId,
        tracked: Arc<TrackedFile>,
        lifecycle: ReplicaLifecycle,
        commit_state: FileCommitState,
        origin: ReplicaOrigin,
    ) -> Self {
        let replica = Arc::new(PhysicalReplica::new(0, tracked, lifecycle, origin));
        let mut replicas = HashMap::new();
        replicas.insert(0, replica);
        Self {
            file_id,
            metadata: OnceLock::new(),
            commit_state: Mutex::new(commit_state),
            replica_state: RwLock::new(ReplicaState {
                replicas,
                preferred_read_replica: Some(0),
            }),
            next_replica_id: AtomicU64::new(1),
        }
    }

    pub(crate) fn initialize_metadata(&self, metadata: ImmutableFileMetadata) -> Result<()> {
        if let Some(current) = self.metadata.get() {
            if current == &metadata {
                return Ok(());
            }
            return Err(Error::InvalidState(format!(
                "logical file {} was attached with different immutable metadata",
                self.file_id
            )));
        }
        if self.metadata.set(metadata.clone()).is_ok() {
            return Ok(());
        }
        if self.metadata.get() == Some(&metadata) {
            Ok(())
        } else {
            Err(Error::InvalidState(format!(
                "logical file {} was attached with different immutable metadata",
                self.file_id
            )))
        }
    }

    pub(crate) fn metadata(&self) -> Option<ImmutableFileMetadata> {
        self.metadata.get().cloned()
    }

    pub(crate) fn preferred_replica(&self) -> Option<Arc<PhysicalReplica>> {
        self.preferred_replica_any()
            .filter(|replica| replica.is_readable())
    }

    pub(crate) fn preferred_replica_any(&self) -> Option<Arc<PhysicalReplica>> {
        let state = self.replica_state.read().unwrap();
        state
            .preferred_read_replica
            .and_then(|id| state.replicas.get(&id).cloned())
    }

    pub(crate) fn preferred_replica_lifecycle(&self) -> Option<ReplicaLifecycle> {
        self.preferred_replica_any()
            .map(|replica| replica.lifecycle())
    }

    pub(crate) fn add_replica(
        &self,
        tracked: Arc<TrackedFile>,
        lifecycle: ReplicaLifecycle,
    ) -> ReplicaId {
        self.add_replica_with_origin(tracked, lifecycle, ReplicaOrigin::Owned)
    }

    pub(crate) fn add_replica_with_origin(
        &self,
        tracked: Arc<TrackedFile>,
        lifecycle: ReplicaLifecycle,
        origin: ReplicaOrigin,
    ) -> ReplicaId {
        let replica_id = self.next_replica_id.fetch_add(1, Ordering::Relaxed);
        self.replica_state.write().unwrap().replicas.insert(
            replica_id,
            Arc::new(PhysicalReplica::new(replica_id, tracked, lifecycle, origin)),
        );
        replica_id
    }

    pub(crate) fn remove_replica(&self, replica_id: ReplicaId) -> Option<Arc<PhysicalReplica>> {
        let mut state = self.replica_state.write().unwrap();
        if state.preferred_read_replica == Some(replica_id) {
            return None;
        }
        let replica = state.replicas.remove(&replica_id)?;
        replica.set_lifecycle(ReplicaLifecycle::Retiring);
        Some(replica)
    }

    pub(crate) fn set_replica_lifecycle(
        &self,
        replica_id: ReplicaId,
        lifecycle: ReplicaLifecycle,
    ) -> bool {
        let state = self.replica_state.read().unwrap();
        let Some(replica) = state.replicas.get(&replica_id) else {
            return false;
        };
        replica.set_lifecycle(lifecycle);
        true
    }

    pub(crate) fn replica_on_volume(
        &self,
        volume: &Arc<crate::file::DataVolume>,
    ) -> Option<Arc<PhysicalReplica>> {
        self.replica_state
            .read()
            .unwrap()
            .replicas
            .values()
            .filter_map(|replica| {
                (replica.is_readable()
                    && replica
                        .tracked
                        .volume
                        .as_ref()
                        .is_some_and(|replica_volume| Arc::ptr_eq(replica_volume, volume)))
                .then_some(Arc::clone(replica))
            })
            .min_by_key(|replica| replica.replica_id)
    }

    pub(crate) fn retain_and_select_replica_if(
        &self,
        expected: &Arc<TrackedFile>,
        replica_id: ReplicaId,
    ) -> bool {
        let mut state = self.replica_state.write().unwrap();
        let Some(current) = state
            .preferred_read_replica
            .and_then(|id| state.replicas.get(&id))
        else {
            return false;
        };
        if !Arc::ptr_eq(&current.tracked, expected)
            || !state
                .replicas
                .get(&replica_id)
                .is_some_and(|replica| replica.is_readable())
        {
            return false;
        }
        state.preferred_read_replica = Some(replica_id);
        true
    }

    pub(crate) fn add_and_select_replica_if(
        &self,
        expected: &Arc<TrackedFile>,
        tracked: Arc<TrackedFile>,
        lifecycle: ReplicaLifecycle,
    ) -> Option<ReplicaId> {
        let mut state = self.replica_state.write().unwrap();
        let current_id = state.preferred_read_replica?;
        let current = state.replicas.get(&current_id)?;
        if !Arc::ptr_eq(&current.tracked, expected) {
            return None;
        }
        let replica_id = self.next_replica_id.fetch_add(1, Ordering::Relaxed);
        state.replicas.insert(
            replica_id,
            Arc::new(PhysicalReplica::owned(replica_id, tracked, lifecycle)),
        );
        state.preferred_read_replica = Some(replica_id);
        Some(current_id)
    }

    pub(crate) fn set_preferred_lifecycle(&self, lifecycle: ReplicaLifecycle) {
        if let Some(replica) = self.preferred_replica_any() {
            replica.set_lifecycle(lifecycle);
        }
    }

    pub(crate) fn replica_ids(&self) -> Vec<ReplicaId> {
        self.replica_state
            .read()
            .unwrap()
            .replicas
            .keys()
            .copied()
            .collect()
    }

    pub(crate) fn replica_state_snapshot(&self) -> ReplicaStateSnapshot {
        let state = self.replica_state.read().unwrap();
        ReplicaStateSnapshot {
            preferred_replica_id: state.preferred_read_replica,
            replicas: state.replicas.values().cloned().collect(),
        }
    }

    pub(crate) fn replica_at_absolute_path(&self, path: &str) -> Option<Arc<PhysicalReplica>> {
        self.replica_state
            .read()
            .unwrap()
            .replicas
            .values()
            .find(|replica| replica.tracked.absolute_path() == path)
            .cloned()
    }

    pub(crate) fn restore_replica_state(
        &self,
        replicas: Vec<(ReplicaId, Arc<TrackedFile>, ReplicaLifecycle)>,
        preferred_replica_id: Option<ReplicaId>,
    ) {
        self.restore_replica_state_with_origins(
            replicas
                .into_iter()
                .map(|(id, tracked, lifecycle)| (id, tracked, lifecycle, ReplicaOrigin::Owned))
                .collect(),
            preferred_replica_id,
        );
    }

    pub(crate) fn restore_replica_state_with_origins(
        &self,
        replicas: Vec<(ReplicaId, Arc<TrackedFile>, ReplicaLifecycle, ReplicaOrigin)>,
        preferred_replica_id: Option<ReplicaId>,
    ) {
        let mut state = self.replica_state.write().unwrap();
        let next_id = replicas.iter().map(|(id, _, _, _)| *id).max().unwrap_or(0);
        state.replicas = replicas
            .into_iter()
            .map(|(id, tracked, lifecycle, origin)| {
                (
                    id,
                    Arc::new(PhysicalReplica::new(id, tracked, lifecycle, origin)),
                )
            })
            .collect();
        state.preferred_read_replica =
            preferred_replica_id.filter(|id| state.replicas.contains_key(id));
        self.next_replica_id
            .fetch_max(next_id.saturating_add(1), Ordering::Relaxed);
    }

    pub(crate) fn finish_staging_replica(&self) {
        if self.preferred_replica_lifecycle() == Some(ReplicaLifecycle::Staging) {
            self.set_preferred_lifecycle(ReplicaLifecycle::OwnedReady);
        }
    }

    pub(crate) fn commit_state(&self) -> FileCommitState {
        *self.commit_state.lock().unwrap()
    }

    pub(crate) fn set_commit_state(&self, state: FileCommitState) {
        *self.commit_state.lock().unwrap() = state;
    }
}
