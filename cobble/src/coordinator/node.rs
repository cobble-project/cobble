//! Coordinator node for global snapshot manifests.
use crate::coordinator::CoordinatorConfig;
use crate::coordinator::file::MetadataWriter;
use crate::error::Error::IoError;
use crate::error::{Error, Result};
use crate::file::{
    BufferedWriter, File, FileSystem, FileSystemRegistry, MetadataReader, SequentialWriteFile,
};
use crate::paths::{
    SNAPSHOT_DIR, global_snapshot_current_path, global_snapshot_manifest_path,
    snapshot_manifest_name,
};
use crate::util::{build_commit_short_id, build_version_string};
use dashmap::DashSet;
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Global manifests version 2 reference shard snapshots with big-endian bucket prefixes.
pub(crate) const GLOBAL_SNAPSHOT_MANIFEST_VERSION_CURRENT: u32 = 2;

/// Bucket snapshot reference input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardSnapshotInput {
    pub ranges: Vec<RangeInclusive<u16>>,
    pub column_family_ids: BTreeMap<String, u8>,
    pub db_id: String,
    pub snapshot_id: u64,
    pub manifest_path: String,
    /// Timestamp (seconds) when the shard snapshot was initiated.
    pub timestamp_seconds: u32,
    pub data_size_bytes: u64,
    pub incremental_data_size_bytes: u64,
}

/// Bucket snapshot reference stored in a global manifest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ShardSnapshotRef {
    pub ranges: Vec<RangeInclusive<u16>>,
    pub column_family_ids: BTreeMap<String, u8>,
    pub db_id: String,
    pub snapshot_id: u64,
    pub manifest_path: String,
    /// Timestamp (seconds) when the shard snapshot was initiated.
    pub timestamp_seconds: u32,
    pub data_size_bytes: u64,
    pub incremental_data_size_bytes: u64,
}

/// Global snapshot manifest referencing bucket-level snapshots.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GlobalSnapshotManifest {
    pub version: u32,
    pub id: u64,
    pub total_buckets: u32,
    pub column_family_ids: BTreeMap<String, u8>,
    pub shard_snapshots: Vec<ShardSnapshotRef>,
    /// Watermark: the minimum timestamp (seconds) across all shard snapshots.
    pub watermark_seconds: u32,
}

impl GlobalSnapshotManifest {
    pub(crate) fn validate_version(&self) -> Result<()> {
        if self.version != GLOBAL_SNAPSHOT_MANIFEST_VERSION_CURRENT {
            return Err(Error::IoError(format!(
                "Unsupported global snapshot manifest version: {} (expected {})",
                self.version, GLOBAL_SNAPSHOT_MANIFEST_VERSION_CURRENT
            )));
        }
        Ok(())
    }
}

/// Coordinator node that materializes global snapshots on shared storage.
pub struct DbCoordinator {
    config: CoordinatorConfig,
    fs: Arc<dyn FileSystem>,
    next_id: AtomicU64,
    retained: DashSet<u64>,
}

impl DbCoordinator {
    pub fn open(config: CoordinatorConfig) -> Result<Self> {
        info!(
            "Cobble db coordinator ({}, Rev:{}) start.",
            build_version_string(),
            build_commit_short_id()
        );
        let registry = FileSystemRegistry::new();
        let volumes = if config.volumes.is_empty() {
            return Err(IoError(
                "No volumes configured for coordinator node".to_string(),
            ));
        } else {
            config.volumes.clone()
        };
        let meta_volume = volumes
            .iter()
            .find(|volume| volume.supports(crate::config::VolumeUsageKind::Meta))
            .unwrap_or_else(|| volumes.first().expect("No meta volume exists"));
        let fs = registry.get_or_register_volume(meta_volume)?;
        // ensure snapshot directory exists
        if !fs.exists(SNAPSHOT_DIR)? {
            fs.create_dir(SNAPSHOT_DIR)?;
        }
        let config = CoordinatorConfig {
            volumes: config.volumes,
            snapshot_retention: config.snapshot_retention,
        };
        // determine next snapshot id, load from current pointer
        let next_id = load_latest_snapshot_id(&fs)?.map_or(0, |id| id + 1);
        Ok(Self {
            config,
            fs,
            next_id: AtomicU64::new(next_id),
            retained: DashSet::new(),
        })
    }

    /// Create a new global snapshot description from bucket-level snapshots.
    pub fn take_global_snapshot(
        &self,
        total_buckets: u32,
        shard_snapshots: Vec<ShardSnapshotInput>,
    ) -> Result<GlobalSnapshotManifest> {
        let id = self.allocate_snapshot_id();
        Self::build_global_snapshot(total_buckets, shard_snapshots, id)
    }

    pub fn take_global_snapshot_with_id(
        &self,
        total_buckets: u32,
        shard_snapshots: Vec<ShardSnapshotInput>,
        id: u64,
    ) -> Result<GlobalSnapshotManifest> {
        Self::build_global_snapshot(total_buckets, shard_snapshots, id)
    }

    pub(crate) fn allocate_snapshot_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    fn build_global_snapshot(
        total_buckets: u32,
        shard_snapshots: Vec<ShardSnapshotInput>,
        id: u64,
    ) -> Result<GlobalSnapshotManifest> {
        if shard_snapshots.is_empty() {
            return Err(Error::IoError(
                "bucket snapshots required to build global snapshot".to_string(),
            ));
        }
        let column_family_ids = merge_column_family_ids(&shard_snapshots)?;
        let mut watermark_seconds = u32::MAX;
        let mut bucket_refs = Vec::with_capacity(shard_snapshots.len());
        for bucket in shard_snapshots {
            if bucket.manifest_path.is_empty() {
                return Err(Error::ConfigError(format!(
                    "Bucket snapshot manifest path missing for {}:{}",
                    bucket.db_id, bucket.snapshot_id
                )));
            }
            watermark_seconds = watermark_seconds.min(bucket.timestamp_seconds);
            bucket_refs.push(ShardSnapshotRef {
                ranges: bucket.ranges,
                column_family_ids: bucket.column_family_ids,
                db_id: bucket.db_id,
                snapshot_id: bucket.snapshot_id,
                manifest_path: bucket.manifest_path,
                timestamp_seconds: bucket.timestamp_seconds,
                data_size_bytes: bucket.data_size_bytes,
                incremental_data_size_bytes: bucket.incremental_data_size_bytes,
            });
        }
        Ok(GlobalSnapshotManifest {
            version: GLOBAL_SNAPSHOT_MANIFEST_VERSION_CURRENT,
            id,
            total_buckets,
            column_family_ids,
            shard_snapshots: bucket_refs,
            watermark_seconds,
        })
    }

    /// Materialize a global snapshot manifest and update the pointer.
    pub fn materialize_global_snapshot(&self, snapshot: &GlobalSnapshotManifest) -> Result<()> {
        let manifest_path = global_snapshot_manifest_path(snapshot.id);
        let writer = MetadataWriter::new(&manifest_path, &self.fs)?;
        let mut buffered = BufferedWriter::new(writer, 8192);
        encode_global_manifest(&mut buffered, snapshot)?;
        buffered.close()?;
        self.publish_manifest_pointer(&snapshot_manifest_name(snapshot.id))?;
        self.process_retention()?;
        Ok(())
    }

    /// Load a global snapshot manifest by id.
    pub fn load_global_snapshot(&self, snapshot_id: u64) -> Result<GlobalSnapshotManifest> {
        let manifest_path = global_snapshot_manifest_path(snapshot_id);
        let reader = self.fs.open_read(&manifest_path)?;
        let payload = MetadataReader::new(reader).read_all()?;
        decode_global_manifest(payload.as_ref())
    }

    /// Load the latest global snapshot manifest referenced by the pointer.
    pub fn load_current_global_snapshot(&self) -> Result<Option<GlobalSnapshotManifest>> {
        let snapshot_id = load_latest_snapshot_id(&self.fs)?;
        let Some(snapshot_id) = snapshot_id else {
            return Ok(None);
        };
        self.load_global_snapshot(snapshot_id).map(Some)
    }

    /// List all materialized global snapshots under the snapshot directory.
    pub fn list_global_snapshots(&self) -> Result<Vec<GlobalSnapshotManifest>> {
        let mut snapshots = Vec::new();
        for entry in self.fs.list(SNAPSHOT_DIR)? {
            let manifest_name = entry.rsplit('/').next().unwrap_or(entry.as_str()).trim();
            let Ok(snapshot_id) = parse_snapshot_id(manifest_name) else {
                continue;
            };
            match self.load_global_snapshot(snapshot_id) {
                Ok(snapshot) => snapshots.push(snapshot),
                Err(Error::ChecksumMismatch(_)) => {}
                Err(err) => return Err(err),
            }
        }
        snapshots.sort_by_key(|snapshot| snapshot.id);
        Ok(snapshots)
    }

    /// Retain a global snapshot id so auto-retention and expire won't delete it.
    pub fn retain_snapshot(&self, snapshot_id: u64) -> bool {
        let manifest_path = global_snapshot_manifest_path(snapshot_id);
        match self.fs.exists(&manifest_path) {
            Ok(false) | Err(_) => false,
            Ok(true) => {
                self.retained.insert(snapshot_id);
                true
            }
        }
    }

    /// Expire one global snapshot manifest if it exists.
    /// This call first removes retain protection on the snapshot, then attempts deletion.
    /// CURRENT pointer snapshot is still protected.
    pub fn expire_snapshot(&self, snapshot_id: u64) -> Result<bool> {
        self.retained.remove(&snapshot_id);
        self.expire_snapshot_if_allowed(snapshot_id)
    }

    fn expire_snapshot_if_allowed(&self, snapshot_id: u64) -> Result<bool> {
        let pointer_id = load_latest_snapshot_id(&self.fs)?;
        if pointer_id == Some(snapshot_id) {
            return Ok(false);
        }
        if self.retained.contains(&snapshot_id) {
            return Ok(false);
        }
        let manifest_path = global_snapshot_manifest_path(snapshot_id);
        if !self.fs.exists(&manifest_path)? {
            return Ok(false);
        }
        self.fs.delete(&manifest_path)?;
        Ok(true)
    }

    fn publish_manifest_pointer(&self, manifest_name: &str) -> Result<()> {
        let pointer_path = global_snapshot_current_path();
        let mut writer = MetadataWriter::new(&pointer_path, &self.fs)?;
        writer.write(manifest_name.as_bytes())?;
        writer.close()?;
        Ok(())
    }

    fn process_retention(&self) -> Result<()> {
        let Some(retention) = self.config.snapshot_retention else {
            return Ok(());
        };
        let mut snapshots = Vec::new();
        for entry in self.fs.list(SNAPSHOT_DIR)? {
            let manifest_name = entry.rsplit('/').next().unwrap_or(entry.as_str()).trim();
            let Ok(snapshot_id) = parse_snapshot_id(manifest_name) else {
                continue;
            };
            snapshots.push(snapshot_id);
        }
        snapshots.sort();
        if snapshots.len() <= retention {
            return Ok(());
        }
        let current_id = load_latest_snapshot_id(&self.fs)?;
        let keep_from = snapshots.len().saturating_sub(retention);
        for snapshot in snapshots.into_iter().take(keep_from) {
            if current_id == Some(snapshot) || self.retained.contains(&snapshot) {
                continue;
            }
            let _ = self.expire_snapshot_if_allowed(snapshot)?;
        }
        Ok(())
    }
}

fn merge_column_family_ids(shard_snapshots: &[ShardSnapshotInput]) -> Result<BTreeMap<String, u8>> {
    let mut by_name = BTreeMap::new();
    let mut by_id = BTreeMap::new();
    for shard in shard_snapshots {
        if shard.column_family_ids.is_empty() {
            return Err(Error::CoordinationError(format!(
                "Column family ids missing for {}:{}",
                shard.db_id, shard.snapshot_id
            )));
        }
        for (name, id) in &shard.column_family_ids {
            if let Some(existing_id) = by_name.get(name) {
                if *existing_id != *id {
                    return Err(Error::CoordinationError(format!(
                        "column family '{}' has conflicting ids {} and {} across shards",
                        name, existing_id, id
                    )));
                }
            } else {
                by_name.insert(name.clone(), *id);
            }
            if let Some(existing_name) = by_id.get(id) {
                if existing_name != name {
                    return Err(Error::CoordinationError(format!(
                        "column family id {} is assigned to both '{}' and '{}' across shards",
                        id, existing_name, name
                    )));
                }
            } else {
                by_id.insert(*id, name.clone());
            }
        }
    }
    Ok(by_name)
}

fn parse_snapshot_id(name: &str) -> Result<u64> {
    let Some(id) = name.trim().strip_prefix("SNAPSHOT-") else {
        return Err(Error::IoError(format!(
            "Invalid snapshot manifest name: {}",
            name
        )));
    };
    id.parse::<u64>()
        .map_err(|err| Error::IoError(format!("Invalid snapshot id {}: {}", name, err)))
}

fn encode_global_manifest<W: SequentialWriteFile>(
    writer: &mut BufferedWriter<W>,
    snapshot: &GlobalSnapshotManifest,
) -> Result<()> {
    let json = serde_json::to_vec(snapshot)
        .map_err(|err| Error::IoError(format!("Failed to encode global manifest: {}", err)))?;
    writer.write(&json)?;
    Ok(())
}

fn decode_global_manifest(bytes: &[u8]) -> Result<GlobalSnapshotManifest> {
    let manifest: GlobalSnapshotManifest = serde_json::from_slice(bytes)
        .map_err(|err| Error::IoError(format!("Failed to decode global manifest: {}", err)))?;
    manifest.validate_version()?;
    Ok(manifest)
}

fn load_latest_snapshot_id(fs: &Arc<dyn FileSystem>) -> Result<Option<u64>> {
    let pointer_path = global_snapshot_current_path();
    if !fs.exists(&pointer_path)? {
        return Ok(None);
    }
    let reader = fs.open_read(&pointer_path)?;
    let payload = match MetadataReader::new(reader).read_all() {
        Ok(payload) => payload,
        Err(Error::ChecksumMismatch(_)) => return Ok(None),
        Err(err) => return Err(err),
    };
    let manifest_name = String::from_utf8(payload.to_vec())
        .map(|s| s.trim().to_string())
        .map_err(|err| Error::IoError(format!("Invalid manifest pointer: {}", err)))?;
    if manifest_name.is_empty() {
        return Ok(None);
    }
    parse_snapshot_id(&manifest_name).map(Some)
}

#[cfg(test)]
#[path = "../../tests/unit/coordinator/node.rs"]
mod tests;
