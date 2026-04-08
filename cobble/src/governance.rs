//! Governance manifest management for distributed bucket ownership.
//! This module provides structures and functions to manage governance manifests.
use crate::Config;
use crate::config::VolumeDescriptor;
use crate::db_state::{bucket_range_fits_total, bucket_range_last};
use crate::error::{Error, Result};
use crate::file::{
    File, FileSystem, FileSystemRegistry, MetadataReader, MetadataWriter, SequentialWriteFile,
};
use serde::{Deserialize, Serialize};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::paths::{
    GOVERNANCE_MANIFEST_LOCK_NAME, GOVERNANCE_MANIFEST_POINTER_NAME, governance_manifest_lock_path,
};

/// Pluggable governance coordinator used during writable DB open.
pub trait DbGovernance: Send + Sync {
    fn register_db(
        &self,
        db_id: &str,
        ranges: &[RangeInclusive<u16>],
        total_buckets: u32,
    ) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct GovernanceEntry {
    pub(crate) db_id: String,
    pub(crate) ranges: Vec<RangeInclusive<u16>>,
}

/// Snapshot of bucket ownership for distributed governance.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct GovernanceManifest {
    pub(crate) total_buckets: u32,
    pub(crate) assignments: Vec<GovernanceEntry>,
}

/// Guard for a held manifest lock. Dropping the guard releases the lock.
pub(crate) trait ManifestLockGuard: Send + Sync {}

/// Provider for obtaining a manifest lock.
pub(crate) trait ManifestLockProvider: Send + Sync {
    fn lock(&self) -> Result<Box<dyn ManifestLockGuard>>;
}

/// File-system-backed lock provider for local deployments.
/// TODO: Replace with distributed lock for multi-node deployments.
pub(crate) struct FileManifestLockProvider {
    fs: Arc<dyn FileSystem>,
    lock_path: String,
    retries: usize,
    retry_delay: Duration,
}

impl FileManifestLockProvider {
    pub(crate) fn new(fs: Arc<dyn FileSystem>, lock_path: &str) -> Self {
        Self {
            fs,
            lock_path: lock_path.to_string(),
            retries: 50,
            retry_delay: Duration::from_millis(50),
        }
    }

    pub(crate) fn with_retry(
        fs: Arc<dyn FileSystem>,
        lock_path: &str,
        retries: usize,
        retry_delay: Duration,
    ) -> Self {
        Self {
            fs,
            lock_path: lock_path.to_string(),
            retries,
            retry_delay,
        }
    }
}

struct FileManifestLockGuard {
    fs: Arc<dyn FileSystem>,
    lock_path: String,
}

impl ManifestLockGuard for FileManifestLockGuard {}

impl Drop for FileManifestLockGuard {
    fn drop(&mut self) {
        let _ = self.fs.delete(&self.lock_path);
    }
}

impl ManifestLockProvider for FileManifestLockProvider {
    fn lock(&self) -> Result<Box<dyn ManifestLockGuard>> {
        for attempt in 0..=self.retries {
            match self.fs.create_dir(&self.lock_path) {
                Ok(()) => {
                    return Ok(Box::new(FileManifestLockGuard {
                        fs: Arc::clone(&self.fs),
                        lock_path: self.lock_path.clone(),
                    }));
                }
                Err(err) => {
                    if attempt >= self.retries {
                        return Err(err);
                    }
                    std::thread::sleep(self.retry_delay);
                }
            }
        }
        Err(Error::IoError(
            "failed to acquire manifest lock".to_string(),
        ))
    }
}

pub(crate) fn create_manifest_lock_provider(
    fs: Arc<dyn FileSystem>,
    _config: &Config,
) -> Result<Arc<dyn ManifestLockProvider>> {
    Ok(Arc::new(FileManifestLockProvider::new(
        fs,
        GOVERNANCE_MANIFEST_LOCK_NAME,
    )))
}

pub struct FileSystemDbGovernance {
    manager: GovernanceManager,
}

impl FileSystemDbGovernance {
    pub fn from_config(config: &Config) -> Result<Self> {
        Self::from_volumes(&config.volumes)
    }

    pub fn from_volumes(volumes: &[VolumeDescriptor]) -> Result<Self> {
        let registry = FileSystemRegistry::new();
        if volumes.is_empty() {
            return Err(Error::ConfigError("No volumes configured".to_string()));
        }
        let meta_volume = volumes
            .iter()
            .find(|volume| volume.supports(crate::config::VolumeUsageKind::Meta))
            .unwrap_or_else(|| volumes.first().expect("No meta volume exists"));
        let fs = registry.get_or_register_volume(meta_volume)?;
        let lock_provider = Arc::new(FileManifestLockProvider::new(
            Arc::clone(&fs),
            GOVERNANCE_MANIFEST_LOCK_NAME,
        ));
        let manager = GovernanceManager::new(fs, lock_provider);
        Ok(Self { manager })
    }
}

impl DbGovernance for FileSystemDbGovernance {
    fn register_db(
        &self,
        db_id: &str,
        ranges: &[RangeInclusive<u16>],
        total_buckets: u32,
    ) -> Result<()> {
        self.manager
            .insert_and_publish(db_id, ranges.to_vec(), total_buckets)?;
        Ok(())
    }
}

pub(crate) fn create_default_db_governance(config: &Config) -> Result<Arc<dyn DbGovernance>> {
    Ok(Arc::new(FileSystemDbGovernance::from_config(config)?))
}

/// Manager for governance manifests with lock coordination.
pub(crate) struct GovernanceManager {
    fs: Arc<dyn FileSystem>,
    lock_provider: Arc<dyn ManifestLockProvider>,
}

impl GovernanceManager {
    pub(crate) fn new(
        fs: Arc<dyn FileSystem>,
        lock_provider: Arc<dyn ManifestLockProvider>,
    ) -> Self {
        Self { fs, lock_provider }
    }

    pub(crate) fn with_file_lock(fs: Arc<dyn FileSystem>, root_dir: &str) -> Result<Self> {
        if !fs.exists(root_dir)? {
            fs.create_dir(root_dir)?;
        }
        let lock_path = governance_manifest_lock_path(root_dir);
        let lock_provider = Arc::new(FileManifestLockProvider::new(Arc::clone(&fs), &lock_path));
        Ok(Self::new(fs, lock_provider))
    }

    pub(crate) fn insert_and_publish(
        &self,
        id: &str,
        ranges: Vec<RangeInclusive<u16>>,
        total_buckets: u32,
    ) -> Result<String> {
        let _guard = self.lock_provider.lock()?;
        let mut current_manifest = self.load_current()?.unwrap_or_else(|| GovernanceManifest {
            total_buckets,
            assignments: Vec::new(),
        });
        if current_manifest.total_buckets != total_buckets {
            return Err(Error::IoError(format!(
                "Total buckets mismatch: existing {}, new {}",
                current_manifest.total_buckets, total_buckets
            )));
        }
        // Validate ranges
        for range in &ranges {
            if !bucket_range_fits_total(range, total_buckets) {
                return Err(Error::IoError(format!(
                    "Invalid range {:?} for total buckets {}",
                    range, total_buckets
                )));
            }
        }
        // Validate the range overlaps
        for entry in &current_manifest.assignments {
            if entry.db_id == *id {
                continue;
            }
            for existing_range in &entry.ranges {
                for new_range in &ranges {
                    let existing_last = bucket_range_last(existing_range).ok_or_else(|| {
                        Error::IoError(format!(
                            "Invalid range {:?} for total buckets {}",
                            existing_range, total_buckets
                        ))
                    })?;
                    let new_last = bucket_range_last(new_range).ok_or_else(|| {
                        Error::IoError(format!(
                            "Invalid range {:?} for total buckets {}",
                            new_range, total_buckets
                        ))
                    })?;
                    if !(new_last < *existing_range.start() || existing_last < *new_range.start()) {
                        return Err(Error::IoError(format!(
                            "Range {:?} overlaps with existing range {:?} for db_id {}",
                            new_range, existing_range, entry.db_id
                        )));
                    }
                }
            }
        }
        // Remove any existing entry for the same db_id
        current_manifest
            .assignments
            .retain(|entry| entry.db_id != id);
        current_manifest.assignments.push(GovernanceEntry {
            db_id: id.to_string(),
            ranges,
        });
        self.publish(&current_manifest)
    }

    fn publish(&self, manifest: &GovernanceManifest) -> Result<String> {
        let manifest_name = format!("MANIFEST.{}", Uuid::new_v4());
        let mut manifest_writer = MetadataWriter::new(self.fs.open_write(&manifest_name)?);
        let manifest_bytes = serde_json::to_vec(manifest).map_err(|err| {
            Error::IoError(format!("Failed to encode governance manifest: {}", err))
        })?;
        manifest_writer.write(&manifest_bytes)?;
        manifest_writer.close()?;

        let mut pointer_writer =
            MetadataWriter::new(self.fs.open_write(GOVERNANCE_MANIFEST_POINTER_NAME)?);
        pointer_writer.write(manifest_name.as_bytes())?;
        pointer_writer.close()?;
        Ok(manifest_name)
    }

    fn load_current(&self) -> Result<Option<GovernanceManifest>> {
        if !self.fs.exists(GOVERNANCE_MANIFEST_POINTER_NAME)? {
            return Ok(None);
        }
        let reader = self.fs.open_read(GOVERNANCE_MANIFEST_POINTER_NAME)?;
        let pointer_payload = match MetadataReader::new(reader).read_all() {
            Ok(payload) => payload,
            Err(Error::ChecksumMismatch(_)) => return Ok(None),
            Err(err) => return Err(err),
        };
        let manifest_name = String::from_utf8(pointer_payload.to_vec())
            .map_err(|err| Error::IoError(format!("Invalid manifest pointer: {}", err)))?;
        let manifest_name = manifest_name.trim();
        if manifest_name.is_empty() {
            return Ok(None);
        }
        let reader = self.fs.open_read(manifest_name)?;
        let manifest_payload = match MetadataReader::new(reader).read_all() {
            Ok(payload) => payload,
            Err(Error::ChecksumMismatch(_)) => return Ok(None),
            Err(err) => return Err(err),
        };
        let manifest = serde_json::from_slice(manifest_payload.as_ref()).map_err(|err| {
            Error::IoError(format!("Failed to decode governance manifest: {}", err))
        })?;
        Ok(Some(manifest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FileSystemRegistry;

    fn make_manager(root: &str) -> GovernanceManager {
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", root))
            .unwrap();
        GovernanceManager::with_file_lock(fs, root).unwrap()
    }

    fn cleanup_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn test_governance_insert_and_publish_round_trip() {
        let root = "/tmp/governance_insert_publish";
        cleanup_root(root);
        let manager = make_manager(root);
        let id = "db-a".to_string();
        manager
            .insert_and_publish(&id, vec![0u16..=4u16, 10u16..=11u16], 12)
            .unwrap();
        cleanup_root(root);
    }

    #[test]
    fn test_governance_insert_rejects_overlap() {
        let root = "/tmp/governance_overlap";
        cleanup_root(root);
        let manager = make_manager(root);
        let id_a = "db-a".to_string();
        let id_b = "db-b".to_string();
        manager
            .insert_and_publish(&id_a, vec![0u16..=4u16], 10)
            .unwrap();
        let err = manager
            .insert_and_publish(&id_b, vec![4u16..=5u16], 10)
            .unwrap_err();
        assert!(matches!(err, Error::IoError(_)));
        cleanup_root(root);
    }

    #[test]
    fn test_governance_insert_rejects_total_bucket_mismatch() {
        let root = "/tmp/governance_total_bucket_mismatch";
        cleanup_root(root);
        let manager = make_manager(root);
        let id = "db-a".to_string();
        manager
            .insert_and_publish(&id, vec![0u16..=4u16], 10)
            .unwrap();
        let err = manager
            .insert_and_publish(&id, vec![0u16..=4u16], 12)
            .unwrap_err();
        assert!(matches!(err, Error::IoError(_)));
        cleanup_root(root);
    }
}
