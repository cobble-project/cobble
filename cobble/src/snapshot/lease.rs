//! Import leases belong to snapshot retention, not the lifetime of the copying task.
//! Online and offline callers supply their complete surviving reference set; both
//! use the same decision and source-first, retryable metadata removal sequence.

use crate::error::{Error, Result};
use crate::rescale_protocol::{ImportEnd, ImportStart, import_end_name, import_start_name};
use std::collections::BTreeSet;

pub(crate) trait LeaseMetadata {
    fn import_names(&self) -> Result<Vec<String>>;
    fn read(&self, name: &str) -> Result<Option<Vec<u8>>>;
    fn remove(&self, name: &str) -> Result<()>;
    fn release_source(&self, record: &ImportStart) -> Result<()>;
}

pub(crate) enum LeaseCleanupMode {
    /// A running DB must first publish an independent recovery point (END).
    Online,
    /// No runtime may use this shard. Remaining snapshots are its recovery roots;
    /// an unused import can be discarded without finishing its data copy.
    Offline,
}

/// Capture the import inventory before examining roots. A concurrently started
/// import must not be reclaimed using an earlier snapshot of those roots.
pub(crate) struct ImportLeases {
    pub(crate) starts: Vec<ImportStart>,
    names: Vec<String>,
}

impl ImportLeases {
    pub(crate) fn load(metadata: &impl LeaseMetadata, db_id: &str) -> Result<Self> {
        let names = metadata.import_names()?;
        let mut starts = Vec::new();
        for name in &names {
            if !name.starts_with("IMPORT-") || !name.ends_with(".START") {
                continue;
            }
            let path = format!("imports/{name}");
            let Some(bytes) = metadata.read(&path)? else {
                continue;
            };
            let record = ImportStart::decode(name, &bytes)?;
            if record.target_db_id != db_id {
                return Err(Error::InvalidState(
                    "import target differs from cleaned shard".into(),
                ));
            }
            starts.push(record);
        }
        Ok(Self { starts, names })
    }

    pub(crate) fn reclaim(
        self,
        metadata: &impl LeaseMetadata,
        referenced: &BTreeSet<String>,
        mode: LeaseCleanupMode,
    ) -> Result<()> {
        for record in self.starts {
            if referenced.contains(&record.export_id) {
                continue;
            }
            if matches!(mode, LeaseCleanupMode::Online) {
                let Some(end) = metadata.read(&import_end_name(&record.export_id))? else {
                    continue;
                };
                ImportEnd::decode(&record.export_id, &end)?;
            }
            // START is the durable retry instruction. Never erase it before the
            // upstream release succeeds. Missing leases/remnant ENDs are idempotent.
            metadata.release_source(&record)?;
            metadata.remove(&import_start_name(&record.export_id))?;
            metadata.remove(&import_end_name(&record.export_id))?;
        }
        for name in self.names {
            let Some(id) = name
                .strip_prefix("IMPORT-")
                .and_then(|s| s.strip_suffix(".END"))
            else {
                continue;
            };
            if metadata.read(&import_start_name(id))?.is_none() {
                metadata.remove(&import_end_name(id))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/snapshot_import_leases.rs"]
mod tests;
