//! One dependency policy for offline shard and global cleanup. No DB or user
//! codec is opened here; all protection is derived from durable metadata.

use super::{CleanupScope, FileKey, SnapshotKey, Storage};
use crate::file::ReadAllFile;
use crate::rescale_protocol::{EXPORT_LEASE_DIR, ExportLease};
use crate::{Error, Result};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotPreservationReason {
    /// Required by an unselected shard snapshot or global reference.
    RetainedSnapshot,
    /// A retained snapshot needs this manifest as an incremental base.
    IncrementalDependency,
    /// A durable export lease still protects the source snapshot chain.
    ExportLease,
    /// Keep a blocked global and its complete shard inventory for retry.
    PendingShardCleanup,
}

impl SnapshotPreservationReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RetainedSnapshot => "retained_snapshot",
            Self::IncrementalDependency => "incremental_dependency",
            Self::ExportLease => "export_lease",
            Self::PendingShardCleanup => "pending_shard_cleanup",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct PreservedSnapshot {
    /// Some for a shard snapshot; None for a global snapshot.
    pub db_id: Option<String>,
    pub snapshot_id: u64,
    pub manifest_path: String,
    pub reasons: Vec<SnapshotPreservationReason>,
}

#[derive(Default)]
pub(super) struct Protection {
    pub(super) shards: BTreeMap<SnapshotKey, BTreeSet<SnapshotPreservationReason>>,
    pub(super) globals: BTreeSet<u64>,
}

impl Protection {
    fn retain(
        &mut self,
        scope: &CleanupScope,
        root: SnapshotKey,
        reason: SnapshotPreservationReason,
    ) -> Result<()> {
        for id in scope.shards[root.0].chain(root.1, false)? {
            let reasons = self.shards.entry((root.0, id)).or_default();
            reasons.insert(reason);
            if id != root.1 {
                reasons.insert(SnapshotPreservationReason::IncrementalDependency);
            }
        }
        Ok(())
    }
}

impl CleanupScope {
    pub(super) fn protection(&self, storage: &Storage) -> Result<Protection> {
        let mut protection = Protection::default();
        for (index, shard) in self.shards.iter().enumerate() {
            for &id in shard.snapshots.keys() {
                if !self.selected.contains(&(index, id)) {
                    protection.retain(
                        self,
                        (index, id),
                        SnapshotPreservationReason::RetainedSnapshot,
                    )?;
                }
            }
            let root = shard
                .directory
                .path
                .strip_suffix("/snapshot")
                .expect("shard directory");
            let directory =
                FileKey::new(shard.directory.volume, format!("{root}/{EXPORT_LEASE_DIR}"));
            for name in storage.fs(&directory).list(&directory.path)? {
                if !name.starts_with("LEASE-") || !name.ends_with(".json") {
                    continue;
                }
                let key = FileKey::new(directory.volume, format!("{}/{name}", directory.path));
                let lease = ExportLease::decode(
                    storage.fs(&key).open_read(&key.path)?.read_all()?.as_ref(),
                )?;
                if lease.source_db_id != shard.db_id
                    || name != format!("LEASE-{}.json", lease.export_id)
                {
                    return Err(Error::InvalidState(format!(
                        "export lease identity mismatch: {}",
                        key.path
                    )));
                }
                protection.retain(
                    self,
                    (index, lease.snapshot_id),
                    SnapshotPreservationReason::ExportLease,
                )?;
            }
        }
        let Some(globals) = &self.globals else {
            return Ok(protection);
        };
        // Different volume routes can name the same physical manifest.
        let locations = globals
            .values()
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|key| Ok((key, storage.location(&self.shards[key.0].key(key.1))?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mut shared = BTreeSet::new();
        for (_, refs) in globals
            .iter()
            .filter(|(id, _)| !self.selected_globals.contains(id))
        {
            for &root in refs {
                shared.insert(locations[&root].clone());
                protection.retain(self, root, SnapshotPreservationReason::RetainedSnapshot)?;
            }
        }
        // A direct reference from a surviving global carries the shard's future
        // cleanup responsibility. A lease or incremental descendant does not.
        // Keep blocked globals intact, including their other shards, so retries
        // retain a complete inventory rather than orphaning leftover snapshots.
        loop {
            let protected_locations = protection
                .shards
                .keys()
                .map(|key| storage.location(&self.shards[key.0].key(key.1)))
                .collect::<Result<BTreeSet<_>>>()?;
            let blocked = self
                .selected_globals
                .iter()
                .filter(|id| !protection.globals.contains(id))
                .filter(|id| {
                    globals[id].iter().any(|key| {
                        protected_locations.contains(&locations[key])
                            && !shared.contains(&locations[key])
                    })
                })
                .copied()
                .collect::<Vec<_>>();
            if blocked.is_empty() {
                break;
            }
            for id in blocked {
                protection.globals.insert(id);
                for &root in &globals[&id] {
                    protection.retain(
                        self,
                        root,
                        SnapshotPreservationReason::PendingShardCleanup,
                    )?;
                }
            }
        }
        Ok(protection)
    }
}
