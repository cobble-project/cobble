pub(crate) const SNAPSHOT_DIR: &str = "snapshot";
pub(crate) const SCHEMA_DIR: &str = "schema";
pub(crate) const GLOBAL_SNAPSHOT_POINTER_NAME: &str = "CURRENT";
pub(crate) const GOVERNANCE_MANIFEST_POINTER_NAME: &str = "MANIFEST";
pub(crate) const GOVERNANCE_MANIFEST_LOCK_NAME: &str = "MANIFEST.lock";
pub(crate) const DATA_DIR: &str = "data";

pub(crate) fn snapshot_manifest_name(id: u64) -> String {
    format!("SNAPSHOT-{}", id)
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

pub(crate) fn schema_file_name(id: u64) -> String {
    format!("schema-{}", id)
}

pub(crate) fn schema_file_relative_path(id: u64) -> String {
    format!("{}/{}", SCHEMA_DIR, schema_file_name(id))
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
