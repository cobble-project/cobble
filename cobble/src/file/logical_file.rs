//! Logical file and physical replica model.

use crate::data_file::DataFileType;
use crate::file::{FileId, TrackedFile};
use crate::{Error, Result};
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReplicaLifecycle {
    Staging,
    OwnedReady,
    PublishedTransfer,
    PendingAdoption,
    ReadonlySource,
    ReadonlyView,
    ExternalReference { durability: ExternalDurability },
    Retiring,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExternalDurability {
    Leased,
    Persistent,
}

impl ReplicaLifecycle {
    fn is_readable(&self) -> bool {
        matches!(
            self,
            Self::OwnedReady
                | Self::PublishedTransfer
                | Self::PendingAdoption
                | Self::ReadonlySource
                | Self::ReadonlyView
                | Self::ExternalReference { .. }
        )
    }
}

/// One concrete byte copy. `TrackedFile` carries this replica's priority, references, and
/// physical deletion behavior.
pub(crate) struct PhysicalReplica {
    pub(crate) replica_id: ReplicaId,
    pub(crate) tracked: Arc<TrackedFile>,
    lifecycle: Mutex<ReplicaLifecycle>,
}

impl PhysicalReplica {
    pub(crate) fn new(
        replica_id: ReplicaId,
        tracked: Arc<TrackedFile>,
        lifecycle: ReplicaLifecycle,
    ) -> Self {
        Self {
            replica_id,
            tracked,
            lifecycle: Mutex::new(lifecycle),
        }
    }

    pub(crate) fn lifecycle(&self) -> ReplicaLifecycle {
        self.lifecycle.lock().unwrap().clone()
    }

    fn set_lifecycle(&self, lifecycle: ReplicaLifecycle) {
        *self.lifecycle.lock().unwrap() = lifecycle;
    }

    fn is_readable(&self) -> bool {
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

impl LogicalFile {
    pub(crate) fn new(
        file_id: FileId,
        tracked: Arc<TrackedFile>,
        lifecycle: ReplicaLifecycle,
        commit_state: FileCommitState,
    ) -> Self {
        let replica = Arc::new(PhysicalReplica::new(0, tracked, lifecycle));
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

    pub(crate) fn replace_preferred_replica_if(
        &self,
        expected: &Arc<TrackedFile>,
        tracked: Arc<TrackedFile>,
        lifecycle: ReplicaLifecycle,
    ) -> bool {
        let mut state = self.replica_state.write().unwrap();
        let Some(current) = state
            .preferred_read_replica
            .and_then(|id| state.replicas.get(&id))
        else {
            return false;
        };
        if !Arc::ptr_eq(&current.tracked, expected) {
            return false;
        }
        let replica_id = self.next_replica_id.fetch_add(1, Ordering::Relaxed);
        let replica = Arc::new(PhysicalReplica::new(replica_id, tracked, lifecycle));
        state.replicas.clear();
        state.replicas.insert(replica_id, replica);
        state.preferred_read_replica = Some(replica_id);
        true
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
