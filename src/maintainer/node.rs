//! Maintainer node for global snapshot manifests.
use crate::error::{Error, Result};
use crate::file::{BufferedWriter, File, FileSystem, FileSystemRegistry, SequentialWriteFile};
use crate::maintainer::MaintainerConfig;
use crate::maintainer::file::MetadataWriter;
use crate::paths::{
    SNAPSHOT_DIR, bucket_snapshot_manifest_path, global_snapshot_current_path,
    global_snapshot_manifest_path, snapshot_manifest_name,
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

/// Maintainer node that materializes global snapshots on shared storage.
pub struct MaintainerNode {
    config: MaintainerConfig,
    fs: Arc<dyn FileSystem>,
    next_id: AtomicU64,
}

impl MaintainerNode {
    pub fn open(config: MaintainerConfig) -> Result<Self> {
        let registry = FileSystemRegistry::new();
        let volumes = if config.volumes.is_empty() {
            vec![crate::config::VolumeDescriptor::new(
                "file:///tmp/".into(),
                vec![
                    crate::config::VolumeUsageKind::PrimaryData,
                    crate::config::VolumeUsageKind::Meta,
                ],
            )]
        } else {
            config.volumes.clone()
        };
        let meta_volume = volumes
            .iter()
            .find(|volume| volume.supports(crate::config::VolumeUsageKind::Meta))
            .unwrap_or_else(|| volumes.first().expect("default volume exists"));
        let fs = registry.get_or_register_volume(meta_volume)?;
        // ensure snapshot directory exists
        if !fs.exists(SNAPSHOT_DIR)? {
            fs.create_dir(SNAPSHOT_DIR)?;
        }
        let config = MaintainerConfig {
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
        if bucket_snapshots.is_empty() {
            return Err(Error::IoError(
                "bucket snapshots required to build global snapshot".to_string(),
            ));
        }
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let mut bucket_refs = Vec::with_capacity(bucket_snapshots.len());
        for bucket in bucket_snapshots {
            let manifest_path = bucket_snapshot_manifest_path(&bucket.db_id, bucket.snapshot_id);
            if !self.fs.exists(&manifest_path)? {
                return Err(Error::IoError(format!(
                    "Bucket snapshot manifest not found: {}",
                    manifest_path
                )));
            }
            bucket_refs.push(BucketSnapshotRef {
                ranges: bucket.ranges,
                db_id: bucket.db_id,
                snapshot_id: bucket.snapshot_id,
                manifest_path,
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
    use crate::paths::bucket_snapshot_dir;

    fn cleanup_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn write_bucket_snapshot(fs: Arc<dyn FileSystem>, db_id: &str, snapshot_id: u64) {
        fs.create_dir(db_id).unwrap();
        let snapshot_dir = bucket_snapshot_dir(db_id);
        fs.create_dir(&snapshot_dir).unwrap();
        let path = bucket_snapshot_manifest_path(db_id, snapshot_id);
        let mut writer = fs.open_write(&path).unwrap();
        writer.write(b"{}").unwrap();
        writer.close().unwrap();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_global_snapshot_round_trip() {
        let root = "/tmp/maintainer_global_snapshot";
        cleanup_root(root);
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", root))
            .unwrap();
        write_bucket_snapshot(Arc::clone(&fs), "db-a", 1);
        write_bucket_snapshot(Arc::clone(&fs), "db-b", 2);

        let node = MaintainerNode::open(MaintainerConfig {
            volumes: vec![crate::config::VolumeDescriptor::new(
                format!("file://{}", root),
                vec![
                    crate::config::VolumeUsageKind::PrimaryData,
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
                    },
                    BucketSnapshotInput {
                        ranges: vec![2u16..4u16],
                        db_id: "db-b".to_string(),
                        snapshot_id: 2,
                    },
                ],
            )
            .unwrap();
        node.materialize_global_snapshot(&snapshot).unwrap();

        let loaded = node.load_current_global_snapshot().unwrap().unwrap();
        assert_eq!(loaded.id, snapshot.id);
        assert_eq!(loaded.bucket_snapshots, snapshot.bucket_snapshots);

        cleanup_root(root);
    }
}
