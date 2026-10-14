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

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct ImportStart {
    pub(crate) version: u32,
    pub(crate) export_id: String,
    pub(crate) source_db_id: String,
    pub(crate) source_manifest_path: String,
    pub(crate) snapshot_id: u64,
    pub(crate) target_db_id: String,
    pub(crate) ranges: Vec<RangeInclusive<u16>>,
    pub(crate) import_snapshot_id: u64,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub(crate) struct ImportEnd {
    pub(crate) version: u32,
    pub(crate) export_id: String,
    /// Published only after this owned-only snapshot has materialized successfully.
    pub(crate) snapshot_id: u64,
}

pub(crate) fn export_lease_name(export_id: &str) -> String {
    format!("{EXPORT_LEASE_DIR}/LEASE-{export_id}.json")
}

pub(crate) fn import_start_name(export_id: &str) -> String {
    format!("{IMPORT_RECORD_DIR}/IMPORT-{export_id}.START")
}

pub(crate) fn import_end_name(export_id: &str) -> String {
    format!("{IMPORT_RECORD_DIR}/IMPORT-{export_id}.END")
}

pub(crate) fn write_export_lease(file_manager: &FileManager, lease: &ExportLease) -> Result<()> {
    file_manager.ensure_metadata_dir(EXPORT_LEASE_DIR)?;
    let payload = serde_json::to_vec(lease)
        .map_err(|err| Error::IoError(format!("encode export lease: {err}")))?;
    file_manager.write_plain_metadata_file_atomic(&export_lease_name(&lease.export_id), &payload)
}

pub(crate) fn write_import_start(file_manager: &FileManager, record: &ImportStart) -> Result<()> {
    file_manager.ensure_metadata_dir(IMPORT_RECORD_DIR)?;
    let name = import_start_name(&record.export_id);
    let payload = serde_json::to_vec(record)
        .map_err(|err| Error::IoError(format!("encode import START: {err}")))?;
    if file_manager.metadata_file_exists_untracked(&name)? {
        let existing = file_manager
            .open_metadata_file_reader_untracked(&name)?
            .read_all()?;
        return if existing.as_ref() == payload.as_slice() {
            Ok(())
        } else {
            Err(Error::InvalidState(format!(
                "import START already exists with different contents: {}",
                record.export_id
            )))
        };
    }
    file_manager.write_plain_metadata_file_atomic(&name, &payload)
}

pub(crate) fn write_import_end(file_manager: &FileManager, record: &ImportEnd) -> Result<()> {
    if load_import_end(file_manager, &record.export_id)?.is_some() {
        return Ok(());
    }
    file_manager.ensure_metadata_dir(IMPORT_RECORD_DIR)?;
    let payload = serde_json::to_vec(record)
        .map_err(|err| Error::IoError(format!("encode import END: {err}")))?;
    file_manager.write_plain_metadata_file_atomic(&import_end_name(&record.export_id), &payload)
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

pub(crate) fn load_import_starts(file_manager: &FileManager) -> Result<Vec<ImportStart>> {
    file_manager
        .list_metadata_names(IMPORT_RECORD_DIR)?
        .into_iter()
        .filter(|name| name.starts_with("IMPORT-") && name.ends_with(".START"))
        .map(|name| {
            let reader = file_manager
                .open_metadata_file_reader_untracked(&format!("{IMPORT_RECORD_DIR}/{name}"))?;
            let record: ImportStart =
                serde_json::from_slice(reader.read_all()?.as_ref()).map_err(|err| {
                    Error::InvalidState(format!("decode import record {name}: {err}"))
                })?;
            if record.version != 1 || name != format!("IMPORT-{}.START", record.export_id) {
                return Err(Error::InvalidState(format!("invalid import START {name}")));
            }
            Ok(record)
        })
        .collect()
}

pub(crate) fn load_import_end(
    file_manager: &FileManager,
    export_id: &str,
) -> Result<Option<ImportEnd>> {
    let name = import_end_name(export_id);
    if !file_manager.metadata_file_exists_untracked(&name)? {
        return Ok(None);
    }
    let reader = file_manager.open_metadata_file_reader_untracked(&name)?;
    let record: ImportEnd = serde_json::from_slice(reader.read_all()?.as_ref())
        .map_err(|err| Error::InvalidState(format!("decode import END {name}: {err}")))?;
    if record.version != 1 || record.export_id != export_id {
        return Err(Error::InvalidState(format!("invalid import END {name}")));
    }
    Ok(Some(record))
}

pub(crate) fn has_import_starts(file_manager: &FileManager) -> Result<bool> {
    Ok(file_manager
        .list_metadata_names(IMPORT_RECORD_DIR)?
        .iter()
        .any(|name| name.starts_with("IMPORT-") && name.ends_with(".START")))
}
