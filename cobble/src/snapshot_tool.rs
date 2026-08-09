use crate::db_status::DbLifecycle;
use crate::file::FileManager;
use crate::metrics_manager::MetricsManager;
use crate::schema::SchemaManager;
use crate::snapshot::{SnapshotManager, list_snapshot_manifest_ids, load_manifest_entry};
use crate::{Config, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// Prune a shard snapshot as an external maintenance operation.
///
/// This helper always runs with noop governance to avoid affecting runtime
/// ownership coordination when invoked out-of-band by lifecycle managers.
pub fn prune_shard_snapshot(
    config: Config,
    db_id: impl Into<String>,
    snapshot_id: u64,
) -> Result<bool> {
    let mut config = config.normalize_volume_paths()?;
    config.governance_mode = crate::GovernanceMode::Noop;
    let db_id = db_id.into();
    let metrics_manager = Arc::new(MetricsManager::new(&db_id));
    let file_manager = Arc::new(FileManager::from_config(&config, &db_id, metrics_manager)?);
    let snapshot_ids = list_snapshot_manifest_ids(&file_manager)?;
    if snapshot_ids.is_empty() {
        return Ok(false);
    }

    let mut loaded = Vec::with_capacity(snapshot_ids.len());
    let mut loaded_by_id = HashMap::new();
    for id in snapshot_ids {
        let entry = load_manifest_entry(&file_manager, id, &loaded_by_id)?;
        loaded_by_id.insert(id, entry.clone());
        loaded.push(entry);
    }

    let schema_manager = Arc::new(SchemaManager::from_manifests(
        &file_manager,
        loaded.iter().map(|entry| &entry.manifest),
        None,
    )?);
    let snapshot_manager = SnapshotManager::new_for_maintenance(
        Arc::clone(&file_manager),
        schema_manager,
        Arc::new(DbLifecycle::new_open()),
        config.snapshot_retention,
        config.snapshot_only_track,
        config.snapshot_disable_incremental_base_link,
        Vec::new(),
    );
    for entry in &loaded {
        snapshot_manager.import_snapshot_from_manifest(
            entry.snapshot_id,
            entry.base_snapshot_id,
            &entry.manifest,
        )?;
    }
    snapshot_manager.expire_snapshot(snapshot_id)
}

#[cfg(test)]
#[path = "../tests/unit/snapshot_tool.rs"]
mod tests;
