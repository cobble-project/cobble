//! Offline roots and metadata routes for the shared snapshot lease collector.
use super::{FileKey, Shard, Storage};
use crate::file::ReadAllFile;
use crate::file::logical_file::ReplicaOrigin;
use crate::rescale_protocol::{ImportStart, export_lease_name};
use crate::snapshot::lease::{ImportLeases, LeaseCleanupMode, LeaseMetadata};
use crate::snapshot::manifest::manifest_data_file_refs;
use crate::{Error, Result};

pub(super) fn reclaim(storage: &Storage, shards: Vec<(String, FileKey)>) -> Result<()> {
    for (db_id, directory) in shards {
        let root = directory
            .path
            .strip_suffix("/snapshot")
            .expect("snapshot directory");
        let metadata = OfflineMetadata {
            storage,
            root: FileKey::new(directory.volume, root),
        };
        let imports = ImportLeases::load(&metadata, &db_id)?;
        if imports.starts.is_empty() {
            imports.reclaim(&metadata, &Default::default(), LeaseCleanupMode::Offline)?;
            continue;
        }
        // Re-read the actual survivors, not the planned deletions. On retry this
        // also works when all manifests were removed before an upstream I/O error.
        let shard = Shard::load(storage, db_id, directory)?;
        let mut referenced = std::collections::BTreeSet::new();
        for id in shard.snapshots.keys() {
            for (_, _, origin) in manifest_data_file_refs(&shard.resolved(*id)?) {
                if let ReplicaOrigin::ExternalLeased { export_id } = origin {
                    referenced.insert(export_id);
                }
            }
        }
        imports.reclaim(&metadata, &referenced, LeaseCleanupMode::Offline)?;
    }
    Ok(())
}

struct OfflineMetadata<'a> {
    storage: &'a Storage,
    root: FileKey,
}
impl OfflineMetadata<'_> {
    fn key(&self, name: &str) -> FileKey {
        FileKey::new(self.root.volume, format!("{}/{name}", self.root.path))
    }
}
impl LeaseMetadata for OfflineMetadata<'_> {
    fn import_names(&self) -> Result<Vec<String>> {
        let key = self.key("imports");
        self.storage.fs(&key).list(&key.path)
    }
    fn read(&self, name: &str) -> Result<Option<Vec<u8>>> {
        let key = self.key(name);
        if !self.storage.fs(&key).exists(&key.path)? {
            return Ok(None);
        }
        Ok(Some(
            self.storage
                .fs(&key)
                .open_read(&key.path)?
                .read_all()?
                .to_vec(),
        ))
    }
    fn remove(&self, name: &str) -> Result<()> {
        self.storage.delete(&self.key(name)).map(|_| ())
    }
    fn release_source(&self, record: &ImportStart) -> Result<()> {
        let suffix = format!("/snapshot/SNAPSHOT-{}", record.snapshot_id);
        let manifest = self.storage.resolve(&record.source_manifest_path)?;
        let root = manifest
            .path
            .strip_suffix(&suffix)
            .filter(|root| root.rsplit('/').next() == Some(record.source_db_id.as_str()))
            .ok_or_else(|| {
                Error::InvalidState("import source manifest identity mismatch".into())
            })?;
        let lease = FileKey::new(
            manifest.volume,
            format!("{root}/{}", export_lease_name(&record.export_id)),
        );
        self.storage.delete(&lease).map(|_| ())
    }
}
