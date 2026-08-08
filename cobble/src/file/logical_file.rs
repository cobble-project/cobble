//! Logical file and physical replica compatibility model.
//!
//! The existing `TrackedFile` remains responsible for physical lifetime in this phase. This
//! module records the logical identity and the replica currently used by the legacy read path.

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

/// One concrete byte copy. Priority and explicit references intentionally remain on
/// `TrackedFile` until the legacy read and ownership paths move to replicas.
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

/// Immutable LSM identity with the physical replica selected by the current legacy path.
pub(crate) struct LogicalFile {
    pub(crate) file_id: FileId,
    metadata: OnceLock<ImmutableFileMetadata>,
    commit_state: Mutex<FileCommitState>,
    replicas: RwLock<HashMap<ReplicaId, Arc<PhysicalReplica>>>,
    preferred_read_replica: RwLock<Option<ReplicaId>>,
    next_replica_id: AtomicU64,
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
            replicas: RwLock::new(replicas),
            preferred_read_replica: RwLock::new(Some(0)),
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
        let preferred = *self.preferred_read_replica.read().unwrap();
        let replicas = self.replicas.read().unwrap();
        preferred
            .and_then(|id| replicas.get(&id).cloned())
            .filter(|replica| replica.is_readable())
    }

    pub(crate) fn preferred_replica_lifecycle(&self) -> Option<ReplicaLifecycle> {
        let preferred = *self.preferred_read_replica.read().unwrap();
        preferred.and_then(|id| {
            self.replicas
                .read()
                .unwrap()
                .get(&id)
                .map(|replica| replica.lifecycle())
        })
    }

    pub(crate) fn replace_preferred_replica(
        &self,
        tracked: Arc<TrackedFile>,
        lifecycle: ReplicaLifecycle,
    ) {
        let replica_id = self.next_replica_id.fetch_add(1, Ordering::Relaxed);
        let replica = Arc::new(PhysicalReplica::new(replica_id, tracked, lifecycle));
        let mut replicas = self.replicas.write().unwrap();
        replicas.clear();
        replicas.insert(replica_id, replica);
        *self.preferred_read_replica.write().unwrap() = Some(replica_id);
    }

    pub(crate) fn set_preferred_lifecycle(&self, lifecycle: ReplicaLifecycle) {
        let preferred = *self.preferred_read_replica.read().unwrap();
        if let Some(replica) =
            preferred.and_then(|id| self.replicas.read().unwrap().get(&id).cloned())
        {
            replica.set_lifecycle(lifecycle);
        }
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
