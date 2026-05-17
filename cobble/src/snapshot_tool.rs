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
mod tests {
    use super::prune_shard_snapshot;
    use crate::snapshot::{list_snapshot_manifest_ids, load_manifest_for_snapshot};
    use crate::{
        Config, Db, GovernanceMode, MetricsManager, Result, VolumeDescriptor, WriteOptions,
    };
    use std::collections::HashSet;
    use std::ops::RangeInclusive;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;
    use uuid::Uuid;

    fn test_config(root: &str) -> Config {
        Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            total_buckets: 1,
            memtable_capacity: size::Size::from_kib(16),
            l0_file_limit: 4,
            snapshot_retention: None,
            snapshot_only_track: true,
            snapshot_disable_incremental_base_link: true,
            governance_mode: GovernanceMode::Filesystem,
            ..Config::default()
        }
    }

    fn open_single_bucket_db(config: Config) -> Db {
        Db::open(
            config,
            std::iter::once(0u16..=0u16).collect::<Vec<RangeInclusive<u16>>>(),
        )
        .expect("open db")
    }

    fn create_materialized_snapshot(db: &Db, key: &[u8], value: &[u8]) -> u64 {
        db.put_with_options(0, key, 0, value, &WriteOptions::default())
            .expect("put value");
        let (tx, rx) = mpsc::channel();
        let snapshot_id = db
            .snapshot_with_callback(move |result| {
                tx.send(result).expect("send snapshot callback result");
            })
            .expect("start snapshot");
        let callback_result = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("receive snapshot callback result");
        callback_result.expect("snapshot callback should succeed");
        snapshot_id
    }

    fn referenced_data_paths(manifest: &crate::snapshot::ManifestSnapshot) -> HashSet<String> {
        let mut paths = HashSet::new();
        for levels in &manifest.tree_levels {
            for level in levels {
                for file in &level.files {
                    paths.insert(file.path.clone());
                }
            }
        }
        for vlog in &manifest.vlog_files {
            paths.insert(vlog.path.clone());
        }
        paths
    }

    fn path_exists(file_manager: &Arc<crate::file::FileManager>, path: &str) -> bool {
        if let Some(local_path) = path.strip_prefix("file://") {
            return std::path::Path::new(local_path).exists();
        }
        if file_manager
            .meta_volume
            .fs()
            .exists(path)
            .expect("check path existence on meta volume")
        {
            return true;
        }
        file_manager.data_volumes.iter().any(|volume| {
            volume
                .fs()
                .exists(path)
                .expect("check path existence on data volume")
        })
    }

    fn list_snapshot_ids(config: &Config, db_id: &str) -> Result<Vec<u64>> {
        let normalized = config.normalize_volume_paths()?;
        let metrics_manager = Arc::new(MetricsManager::new(db_id));
        let file_manager = Arc::new(crate::file::FileManager::from_config(
            &normalized,
            db_id,
            metrics_manager,
        )?);
        list_snapshot_manifest_ids(&file_manager)
    }

    #[test]
    fn test_prune_shard_snapshot_removes_snapshot_manifest() {
        let root = format!("/tmp/prune_shard_snapshot_{}", Uuid::new_v4());
        let config = test_config(&root);
        let db = open_single_bucket_db(config.clone());
        let db_id = db.id().to_string();

        let s0 = create_materialized_snapshot(&db, b"k0", &vec![b'a'; 64 * 1024]);
        let s1 = create_materialized_snapshot(&db, b"k1", &vec![b'b'; 64 * 1024]);
        let s2 = create_materialized_snapshot(&db, b"k2", &vec![b'c'; 64 * 1024]);
        db.close().expect("close db");

        let normalized = config.normalize_volume_paths().expect("normalize config");
        let metrics_manager = Arc::new(MetricsManager::new(&db_id));
        let file_manager = Arc::new(
            crate::file::FileManager::from_config(&normalized, &db_id, metrics_manager)
                .expect("create file manager"),
        );

        let s0_manifest = load_manifest_for_snapshot(&file_manager, s0).expect("load snapshot s0");
        let s1_manifest = load_manifest_for_snapshot(&file_manager, s1).expect("load snapshot s1");
        let s2_manifest = load_manifest_for_snapshot(&file_manager, s2).expect("load snapshot s2");
        let target_paths = referenced_data_paths(&s2_manifest);
        let other_paths: HashSet<String> = referenced_data_paths(&s0_manifest)
            .into_iter()
            .chain(referenced_data_paths(&s1_manifest))
            .collect();
        let unique_target_paths: Vec<String> =
            target_paths.difference(&other_paths).cloned().collect();
        let shared_target_paths: Vec<String> =
            target_paths.intersection(&other_paths).cloned().collect();
        assert!(
            !unique_target_paths.is_empty(),
            "expected pruned snapshot to contain uniquely referenced data files"
        );
        assert!(
            !shared_target_paths.is_empty(),
            "expected pruned snapshot to share some data files with surviving snapshots"
        );

        assert!(prune_shard_snapshot(config.clone(), db_id.clone(), s2).expect("prune snapshot"));

        let snapshot_ids = list_snapshot_ids(&config, &db_id).expect("list snapshot ids");
        assert_eq!(snapshot_ids, vec![s0, s1]);
        assert!(
            !path_exists(
                &file_manager,
                &file_manager.metadata_path(&crate::paths::snapshot_manifest_relative_path(s2))
            ),
            "pruned snapshot manifest should be deleted"
        );
        for path in unique_target_paths {
            assert!(
                !path_exists(&file_manager, &path),
                "unique data file should be deleted when pruning snapshot: {}",
                path
            );
        }
        for path in shared_target_paths {
            assert!(
                path_exists(&file_manager, &path),
                "shared data file should remain after pruning snapshot: {}",
                path
            );
        }
        let _ =
            load_manifest_for_snapshot(&file_manager, s0).expect("remaining snapshot s0 exists");
        let _ =
            load_manifest_for_snapshot(&file_manager, s1).expect("remaining snapshot s1 exists");

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_prune_shard_snapshot_returns_false_for_missing_snapshot() {
        let root = format!("/tmp/prune_shard_snapshot_missing_{}", Uuid::new_v4());
        let config = test_config(&root);
        let db = open_single_bucket_db(config.clone());
        let db_id = db.id().to_string();
        let _ = create_materialized_snapshot(&db, b"k0", b"v0");
        db.close().expect("close db");

        assert!(!prune_shard_snapshot(config, db_id, 9_999).expect("prune missing snapshot"));

        let _ = std::fs::remove_dir_all(root);
    }
}
