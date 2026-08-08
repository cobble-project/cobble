use crate::error::{Error, Result};
use crate::file::{FileManager, ReadAllFile};
use std::ops::RangeInclusive;

pub(crate) const EXPORT_LEASE_DIR: &str = "exports";
pub(crate) const IMPORT_RECORD_DIR: &str = "imports";

#[derive(serde::Deserialize, serde::Serialize)]
pub(crate) struct ExportLease {
    pub(crate) version: u32,
    pub(crate) export_id: String,
    pub(crate) source_db_id: String,
    pub(crate) snapshot_id: u64,
    pub(crate) target_db_id: String,
    pub(crate) ranges: Vec<RangeInclusive<u16>>,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub(crate) struct ImportRecord {
    pub(crate) version: u32,
    pub(crate) export_id: String,
    pub(crate) source_db_id: String,
    pub(crate) snapshot_id: u64,
    pub(crate) target_db_id: String,
    pub(crate) ranges: Vec<RangeInclusive<u16>>,
    pub(crate) import_snapshot_id: Option<u64>,
    /// The owned-only snapshot created after all leased replicas have been adopted. This id is
    /// written before its flush; its manifest is the commit record.
    pub(crate) adoption_barrier_snapshot_id: Option<u64>,
}

pub(crate) fn export_lease_name(export_id: &str) -> String {
    format!("{EXPORT_LEASE_DIR}/LEASE-{export_id}.json")
}

pub(crate) fn import_record_name(export_id: &str) -> String {
    format!("{IMPORT_RECORD_DIR}/IMPORT-{export_id}.json")
}

pub(crate) fn write_export_lease(file_manager: &FileManager, lease: &ExportLease) -> Result<()> {
    file_manager.ensure_metadata_dir(EXPORT_LEASE_DIR)?;
    let payload = serde_json::to_vec(lease)
        .map_err(|err| Error::IoError(format!("encode export lease: {err}")))?;
    file_manager.write_plain_metadata_file_atomic(&export_lease_name(&lease.export_id), &payload)
}

pub(crate) fn write_import_record(file_manager: &FileManager, record: &ImportRecord) -> Result<()> {
    file_manager.ensure_metadata_dir(IMPORT_RECORD_DIR)?;
    let payload = serde_json::to_vec(record)
        .map_err(|err| Error::IoError(format!("encode import record: {err}")))?;
    file_manager.write_plain_metadata_file_atomic(&import_record_name(&record.export_id), &payload)
}

pub(crate) fn snapshot_has_export_lease(
    file_manager: &FileManager,
    snapshot_id: u64,
) -> Result<bool> {
    for name in file_manager.list_metadata_names(EXPORT_LEASE_DIR)? {
        if !name.starts_with("LEASE-") || !name.ends_with(".json") {
            continue;
        }
        let reader = file_manager
            .open_metadata_file_reader_untracked(&format!("{EXPORT_LEASE_DIR}/{name}"))?;
        let lease: ExportLease = serde_json::from_slice(reader.read_all()?.as_ref())
            .map_err(|err| Error::InvalidState(format!("decode export lease {name}: {err}")))?;
        if lease.version != 1 {
            return Err(Error::InvalidState(format!(
                "unsupported export lease version {} in {name}",
                lease.version
            )));
        }
        if lease.snapshot_id == snapshot_id {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn load_import_records(file_manager: &FileManager) -> Result<Vec<ImportRecord>> {
    file_manager
        .list_metadata_names(IMPORT_RECORD_DIR)?
        .into_iter()
        .filter(|name| name.starts_with("IMPORT-") && name.ends_with(".json"))
        .map(|name| {
            let reader = file_manager
                .open_metadata_file_reader_untracked(&format!("{IMPORT_RECORD_DIR}/{name}"))?;
            let record: ImportRecord = serde_json::from_slice(reader.read_all()?.as_ref())
                .map_err(|err| {
                    Error::InvalidState(format!("decode import record {name}: {err}"))
                })?;
            if record.version != 1 {
                return Err(Error::InvalidState(format!(
                    "unsupported import record version {} in {name}",
                    record.version
                )));
            }
            Ok(record)
        })
        .collect()
}
