use crate::error::{Error, Result};
use crate::storage_url::{map_url_path, url_path};

/// An unpublished metadata file beside its destination, outside the destination's name prefix.
pub fn metadata_temporary_path(path: &str) -> String {
    let suffix = uuid::Uuid::new_v4();
    match path.rsplit_once('/') {
        Some((parent, name)) => format!("{parent}/_{name}.tmp-{suffix}"),
        None => format!("_{path}.tmp-{suffix}"),
    }
}

pub(crate) const SNAPSHOT_DIR: &str = "snapshot";
pub(crate) const SCHEMA_DIR: &str = "schema";
pub(crate) const GLOBAL_SNAPSHOT_POINTER_NAME: &str = "CURRENT";
pub(crate) const GOVERNANCE_MANIFEST_POINTER_NAME: &str = "MANIFEST";
pub(crate) const GOVERNANCE_MANIFEST_LOCK_NAME: &str = "MANIFEST.lock";
pub(crate) const DATA_DIR: &str = "data";

pub(crate) fn snapshot_manifest_name(id: u64) -> String {
    format!("SNAPSHOT-{}", id)
}

pub(crate) fn snapshot_active_data_name(id: u64) -> String {
    format!("SNAPSHOT-ACTIVE-DATA-{}", id)
}

pub(crate) fn snapshot_active_data_relative_path(id: u64) -> String {
    format!("{}/{}", SNAPSHOT_DIR, snapshot_active_data_name(id))
}

pub fn bucket_snapshot_manifest_path(db_id: &str, snapshot_id: u64) -> String {
    format!("{}/{}", db_id, snapshot_manifest_relative_path(snapshot_id))
}

pub(crate) fn bucket_snapshot_dir(db_id: &str) -> String {
    format!("{}/{}", db_id, SNAPSHOT_DIR)
}

pub(crate) fn snapshot_manifest_relative_path(id: u64) -> String {
    format!("{}/{}", SNAPSHOT_DIR, snapshot_manifest_name(id))
}

pub(crate) fn sibling_snapshot_manifest_path(
    manifest_path: &str,
    snapshot_id: u64,
) -> Result<String> {
    map_url_path(manifest_path, |path| {
        let (snapshot_dir, _) = path.rsplit_once('/').ok_or_else(|| {
            Error::InvalidState(format!(
                "Snapshot manifest path missing file name: {}",
                manifest_path
            ))
        })?;
        Ok(format!(
            "{}/{}",
            snapshot_dir,
            snapshot_manifest_name(snapshot_id)
        ))
    })
}

pub(crate) fn schema_file_name(id: u64) -> String {
    format!("schema-{}", id)
}

pub(crate) fn schema_file_relative_path(id: u64) -> String {
    format!("{}/{}", SCHEMA_DIR, schema_file_name(id))
}

pub(crate) fn schema_file_path_from_snapshot_manifest_path(
    manifest_path: &str,
    schema_id: u64,
) -> Result<String> {
    map_url_path(manifest_path, |path| {
        let (snapshot_dir, _) = path.rsplit_once('/').ok_or_else(|| {
            Error::InvalidState(format!(
                "Snapshot manifest path missing file name: {}",
                manifest_path
            ))
        })?;
        let (db_root, dir_name) = snapshot_dir.rsplit_once('/').ok_or_else(|| {
            Error::InvalidState(format!(
                "Snapshot manifest path missing parent directory: {}",
                manifest_path
            ))
        })?;
        if dir_name != SNAPSHOT_DIR {
            return Err(Error::InvalidState(format!(
                "Snapshot manifest path {} is not under /{}/",
                manifest_path, SNAPSHOT_DIR
            )));
        }
        Ok(format!(
            "{}/{}/{}",
            db_root,
            SCHEMA_DIR,
            schema_file_name(schema_id)
        ))
    })
}

pub(crate) fn snapshot_source_db_id(manifest_path: &str) -> Result<String> {
    let path = url_path(manifest_path)?;
    path.rsplit('/').nth(2).map(str::to_string).ok_or_else(|| {
        Error::InvalidState("Source manifest path is missing the database ID".into())
    })
}

pub(crate) fn global_snapshot_current_path() -> String {
    format!("{}/{}", SNAPSHOT_DIR, GLOBAL_SNAPSHOT_POINTER_NAME)
}

pub(crate) fn global_snapshot_manifest_path(snapshot_id: u64) -> String {
    format!("{}/{}", SNAPSHOT_DIR, snapshot_manifest_name(snapshot_id))
}

pub(crate) fn global_snapshot_manifest_path_by_pointer(pointer: &str) -> String {
    format!("{}/{}", SNAPSHOT_DIR, pointer)
}

pub(crate) fn governance_manifest_lock_path(root_dir: &str) -> String {
    format!("{}/{}", root_dir, GOVERNANCE_MANIFEST_LOCK_NAME)
}
