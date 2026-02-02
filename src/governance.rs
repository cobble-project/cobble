//! Governance manifest management for distributed bucket ownership.
//! This module provides structures and functions to manage governance manifests.
use crate::Config;
use crate::error::{Error, Result};
use crate::file::{File, FileSystem, SequentialWriteFile};
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const MANIFEST_POINTER_NAME: &str = "MANIFEST";
const MANIFEST_LOCK_NAME: &str = "MANIFEST.lock";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct GovernanceEntry {
    pub(crate) db_id: String,
    pub(crate) ranges: Vec<Range<u16>>,
}

/// Snapshot of bucket ownership for distributed governance.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct GovernanceManifest {
    pub(crate) total_buckets: u16,
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
    pub(crate) fn new(fs: Arc<dyn FileSystem>, lock_path: String) -> Self {
        Self {
            fs,
            lock_path,
            retries: 50,
            retry_delay: Duration::from_millis(50),
        }
    }

    pub(crate) fn with_retry(
        fs: Arc<dyn FileSystem>,
        lock_path: String,
        retries: usize,
        retry_delay: Duration,
    ) -> Self {
        Self {
            fs,
            lock_path,
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
    config: &Config,
) -> Result<Arc<dyn ManifestLockProvider>> {
    Ok(Arc::new(FileManifestLockProvider::new(
        fs,
        config.path.clone(),
    )))
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

    pub(crate) fn with_file_lock(fs: Arc<dyn FileSystem>, root_dir: String) -> Result<Self> {
        if !fs.exists(&root_dir)? {
            fs.create_dir(&root_dir)?;
        }
        let lock_path = format!("{}/{}", root_dir, MANIFEST_LOCK_NAME);
        let lock_provider = Arc::new(FileManifestLockProvider::new(Arc::clone(&fs), lock_path));
        Ok(Self::new(fs, lock_provider))
    }

    pub(crate) fn insert_and_publish(
        &self,
        id: &String,
        ranges: Vec<Range<u16>>,
        total_buckets: u16,
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
            if range.start >= range.end || range.end > total_buckets {
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
                    if new_range.start < existing_range.end && existing_range.start < new_range.end
                    {
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
            .retain(|entry| &entry.db_id != id);
        current_manifest.assignments.push(GovernanceEntry {
            db_id: id.clone(),
            ranges,
        });
        self.publish(&current_manifest)
    }

    fn publish(&self, manifest: &GovernanceManifest) -> Result<String> {
        let manifest_name = format!("MANIFEST.{}", Uuid::new_v4());
        let mut manifest_writer = self.fs.open_write(&manifest_name)?;
        let manifest_bytes = serde_json::to_vec(manifest).map_err(|err| {
            Error::IoError(format!("Failed to encode governance manifest: {}", err))
        })?;
        manifest_writer.write(&manifest_bytes)?;
        manifest_writer.close()?;

        let mut pointer_writer = self.fs.open_write(MANIFEST_POINTER_NAME)?;
        pointer_writer.write(manifest_name.as_bytes())?;
        pointer_writer.close()?;
        Ok(manifest_name)
    }

    fn load_current(&self) -> Result<Option<GovernanceManifest>> {
        if !self.fs.exists(MANIFEST_POINTER_NAME)? {
            return Ok(None);
        }
        let reader = self.fs.open_read(MANIFEST_POINTER_NAME)?;
        let bytes = reader.read_at(0, reader.size())?;
        let manifest_name = String::from_utf8(bytes.to_vec())
            .map_err(|err| Error::IoError(format!("Invalid manifest pointer: {}", err)))?;
        let manifest_name = manifest_name.trim();
        if manifest_name.is_empty() {
            return Ok(None);
        }
        let reader = self.fs.open_read(manifest_name)?;
        let bytes = reader.read_at(0, reader.size())?;
        let manifest = serde_json::from_slice(&bytes).map_err(|err| {
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
        GovernanceManager::with_file_lock(fs, root.to_string()).unwrap()
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
            .insert_and_publish(&id, vec![0u16..5u16, 10u16..12u16], 12)
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
            .insert_and_publish(&id_a, vec![0u16..5u16], 10)
            .unwrap();
        let err = manager
            .insert_and_publish(&id_b, vec![4u16..6u16], 10)
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
            .insert_and_publish(&id, vec![0u16..5u16], 10)
            .unwrap();
        let err = manager
            .insert_and_publish(&id, vec![0u16..5u16], 12)
            .unwrap_err();
        assert!(matches!(err, Error::IoError(_)));
        cleanup_root(root);
    }
}
