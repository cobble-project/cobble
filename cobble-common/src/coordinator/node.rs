//! Coordinator node for global snapshot manifests.
use crate::coordinator::CoordinatorConfig;
use crate::coordinator::file::MetadataWriter;
use crate::error::Error::IoError;
use crate::error::{Error, Result};
use crate::file::{BufferedWriter, File, FileSystem, FileSystemRegistry, SequentialWriteFile};
use crate::paths::{
    SNAPSHOT_DIR, global_snapshot_current_path, global_snapshot_manifest_path,
    snapshot_manifest_name,
};
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Bucket snapshot reference input.
#[derive(Clone, Debug)]
pub struct BucketSnapshotInput {
    pub ranges: Vec<Range<u16>>,
    pub db_id: String,
    pub snapshot_id: u64,
    pub manifest_path: String,
}

/// Bucket snapshot reference stored in a global manifest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BucketSnapshotRef {
    pub ranges: Vec<Range<u16>>,
    pub db_id: String,
    pub snapshot_id: u64,
    pub manifest_path: String,
}

/// Global snapshot manifest referencing bucket-level snapshots.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GlobalSnapshotManifest {
    pub id: u64,
    pub total_buckets: u16,
    pub bucket_snapshots: Vec<BucketSnapshotRef>,
}

/// Coordinator node that materializes global snapshots on shared storage.
pub struct DbCoordinator {
    config: CoordinatorConfig,
    fs: Arc<dyn FileSystem>,
    next_id: AtomicU64,
}

impl DbCoordinator {
    pub fn open(config: CoordinatorConfig) -> Result<Self> {
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
        };
        // determine next snapshot id, load from current pointer
        let next_id = load_latest_snapshot_id(&fs)?.map_or(0, |id| id + 1);
        Ok(Self {
            config,
            fs,
            next_id: AtomicU64::new(next_id),
        })
    }

    /// Create a new global snapshot description from bucket-level snapshots.
    pub fn take_global_snapshot(
        &self,
        total_buckets: u16,
        bucket_snapshots: Vec<BucketSnapshotInput>,
    ) -> Result<GlobalSnapshotManifest> {
        let id = self.allocate_snapshot_id();
        Self::build_global_snapshot(total_buckets, bucket_snapshots, id)
    }

    pub fn take_global_snapshot_with_id(
        &self,
        total_buckets: u16,
        bucket_snapshots: Vec<BucketSnapshotInput>,
        id: u64,
    ) -> Result<GlobalSnapshotManifest> {
        Self::build_global_snapshot(total_buckets, bucket_snapshots, id)
    }

    pub fn allocate_snapshot_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    fn build_global_snapshot(
        total_buckets: u16,
        bucket_snapshots: Vec<BucketSnapshotInput>,
        id: u64,
    ) -> Result<GlobalSnapshotManifest> {
        if bucket_snapshots.is_empty() {
            return Err(Error::IoError(
                "bucket snapshots required to build global snapshot".to_string(),
            ));
        }
        let mut bucket_refs = Vec::with_capacity(bucket_snapshots.len());
        for bucket in bucket_snapshots {
            if bucket.manifest_path.is_empty() {
                return Err(Error::ConfigError(format!(
                    "Bucket snapshot manifest path missing for {}:{}",
                    bucket.db_id, bucket.snapshot_id
                )));
            }
            bucket_refs.push(BucketSnapshotRef {
                ranges: bucket.ranges,
                db_id: bucket.db_id,
                snapshot_id: bucket.snapshot_id,
                manifest_path: bucket.manifest_path,
            });
        }
        Ok(GlobalSnapshotManifest {
            id,
            total_buckets,
            bucket_snapshots: bucket_refs,
        })
    }

    /// Materialize a global snapshot manifest and update the pointer.
    pub fn materialize_global_snapshot(&self, snapshot: &GlobalSnapshotManifest) -> Result<()> {
        let manifest_path = global_snapshot_manifest_path(snapshot.id);
        let writer = self.fs.open_write(&manifest_path)?;
        let mut buffered = BufferedWriter::new(writer, 8192);
        encode_global_manifest(&mut buffered, snapshot)?;
        buffered.close()?;
        self.publish_manifest_pointer(&snapshot_manifest_name(snapshot.id))?;
        Ok(())
    }

    /// Load a global snapshot manifest by id.
    pub fn load_global_snapshot(&self, snapshot_id: u64) -> Result<GlobalSnapshotManifest> {
        let manifest_path = global_snapshot_manifest_path(snapshot_id);
        let reader = self.fs.open_read(&manifest_path)?;
        let bytes = reader.read_at(0, reader.size())?;
        decode_global_manifest(bytes.as_ref())
    }

    /// Load the latest global snapshot manifest referenced by the pointer.
    pub fn load_current_global_snapshot(&self) -> Result<Option<GlobalSnapshotManifest>> {
        let snapshot_id = load_latest_snapshot_id(&self.fs)?;
        let Some(snapshot_id) = snapshot_id else {
            return Ok(None);
        };
        self.load_global_snapshot(snapshot_id).map(Some)
    }

    fn publish_manifest_pointer(&self, manifest_name: &str) -> Result<()> {
        let pointer_path = global_snapshot_current_path();
        let mut writer = MetadataWriter::new(pointer_path, &self.fs)?;
        writer.write(manifest_name.as_bytes())?;
        writer.close()?;
        Ok(())
    }
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
    serde_json::from_slice(bytes)
        .map_err(|err| Error::IoError(format!("Failed to decode global manifest: {}", err)))
}

fn load_latest_snapshot_id(fs: &Arc<dyn FileSystem>) -> Result<Option<u64>> {
    let pointer_path = global_snapshot_current_path();
    if !fs.exists(&pointer_path)? {
        return Ok(None);
    }
    let reader = fs.open_read(&pointer_path)?;
    let bytes = reader.read_at(0, reader.size())?;
    let manifest_name = String::from_utf8(bytes.to_vec())
        .map(|s| s.trim().to_string())
        .map_err(|err| Error::IoError(format!("Invalid manifest pointer: {}", err)))?;
    if manifest_name.is_empty() {
        return Ok(None);
    }
    parse_snapshot_id(&manifest_name).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FileSystemRegistry;
    use crate::paths::{bucket_snapshot_dir, bucket_snapshot_manifest_path};

    fn cleanup_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn write_bucket_snapshot(
        fs: Arc<dyn FileSystem>,
        root: &str,
        db_id: &str,
        snapshot_id: u64,
    ) -> String {
        fs.create_dir(db_id).unwrap();
        let snapshot_dir = bucket_snapshot_dir(db_id);
        fs.create_dir(&snapshot_dir).unwrap();
        let path = bucket_snapshot_manifest_path(db_id, snapshot_id);
        let mut writer = fs.open_write(&path).unwrap();
        writer.write(b"{}").unwrap();
        writer.close().unwrap();
        format!("file://{}/{}", root, path)
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_global_snapshot_round_trip() {
        let root = "/tmp/coordinator_global_snapshot";
        cleanup_root(root);
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", root))
            .unwrap();
        let path_a = write_bucket_snapshot(Arc::clone(&fs), root, "db-a", 1);
        let path_b = write_bucket_snapshot(Arc::clone(&fs), root, "db-b", 2);

        let node = DbCoordinator::open(CoordinatorConfig {
            volumes: vec![crate::config::VolumeDescriptor::new(
                format!("file://{}", root),
                vec![
                    crate::config::VolumeUsageKind::PrimaryDataPriorityHigh,
                    crate::config::VolumeUsageKind::Meta,
                ],
            )],
        })
        .unwrap();

        let snapshot = node
            .take_global_snapshot(
                4,
                vec![
                    BucketSnapshotInput {
                        ranges: vec![0u16..2u16],
                        db_id: "db-a".to_string(),
                        snapshot_id: 1,
                        manifest_path: path_a.clone(),
                    },
                    BucketSnapshotInput {
                        ranges: vec![2u16..4u16],
                        db_id: "db-b".to_string(),
                        snapshot_id: 2,
                        manifest_path: path_b.clone(),
                    },
                ],
            )
            .unwrap();
        node.materialize_global_snapshot(&snapshot).unwrap();

        let loaded = node.load_current_global_snapshot().unwrap().unwrap();
        assert_eq!(loaded.id, snapshot.id);
        assert_eq!(loaded.bucket_snapshots, snapshot.bucket_snapshots);
        assert_eq!(loaded.bucket_snapshots[0].manifest_path, path_a);
        assert_eq!(loaded.bucket_snapshots[1].manifest_path, path_b);

        cleanup_root(root);
    }
}
