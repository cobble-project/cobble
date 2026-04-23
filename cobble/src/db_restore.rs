use super::Db;
use crate::Config;
use crate::config::PrimaryVolumeOffloadPolicyKind;
use crate::db_state::{DbStateHandle, MultiLSMTreeVersion};
use crate::db_status::DbLifecycle;
use crate::error::{Error, Result};
use crate::file::{
    FileManager, PrimaryOffloadFileRef, RestoreCopyResourceRegistry, TrackedFileId,
    VLOG_FILE_PRIORITY, compare_primary_offload_file_refs, lsm_file_priority_for_level,
};
use crate::lsm::LSMTreeVersion;
use crate::merge_operator::MergeOperatorResolver;
use crate::metrics_manager::MetricsManager;
use crate::metrics_registry;
use crate::paths::bucket_snapshot_manifest_path;
use crate::snapshot::{
    ManifestSnapshot, build_tree_scopes_from_manifest, build_tree_versions_from_manifest,
    build_vlog_version_from_manifest, list_snapshot_manifest_ids, load_manifest_chain_from_path,
    load_manifest_entry, load_manifest_for_snapshot,
};
use crate::util::{build_commit_short_id, build_version_string, init_logging};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use uuid::Uuid;

struct RestoreTempResourceRegistry {
    file_manager: Arc<FileManager>,
    temp_copied_file_ids: Mutex<Vec<u64>>,
    finalized: AtomicBool,
}

impl RestoreTempResourceRegistry {
    fn new(file_manager: Arc<FileManager>) -> Self {
        Self {
            file_manager,
            temp_copied_file_ids: Mutex::new(Vec::new()),
            finalized: AtomicBool::new(false),
        }
    }

    fn finalize(&self) {
        self.finalized.store(true, Ordering::SeqCst);
    }
}

impl RestoreCopyResourceRegistry for RestoreTempResourceRegistry {
    fn register_temp_restored_copy(&self, file_id: u64) {
        if self.finalized.load(Ordering::SeqCst) {
            return;
        }
        if let Ok(mut guard) = self.temp_copied_file_ids.lock() {
            guard.push(file_id);
        }
    }
}

impl Drop for RestoreTempResourceRegistry {
    fn drop(&mut self) {
        if self.finalized.load(Ordering::SeqCst) {
            return;
        }
        let copied_ids = self
            .temp_copied_file_ids
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        for file_id in copied_ids {
            let _ = self.file_manager.remove_data_file(file_id);
        }
    }
}

fn prepare_manifest_data_files_for_restore(
    file_manager: &Arc<FileManager>,
    manifest: &ManifestSnapshot,
) -> Result<HashMap<u64, Option<u64>>> {
    let refs = ordered_manifest_data_file_refs_for_restore(
        manifest,
        file_manager.options.primary_volume_offload_policy,
    );
    if refs.is_empty() {
        return Ok(HashMap::new());
    }
    if let Some(max_file_id) = refs.iter().map(|item| item.file_id).max() {
        let min_next_file_id = max_file_id.saturating_add(1);
        if file_manager.peek_next_file_id() < min_next_file_id {
            file_manager.set_next_file_id(min_next_file_id);
        }
    }
    let resources = Arc::new(RestoreTempResourceRegistry::new(Arc::clone(file_manager)));
    let registry: Arc<dyn RestoreCopyResourceRegistry + Send + Sync> = resources.clone();
    let file_manager = Arc::clone(file_manager);
    let registry = Arc::clone(&registry);
    let prepared = std::thread::spawn(move || {
        let worker_threads = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(1)
            .min(8);
        let runtime = Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .build()
            .map_err(|err| Error::IoError(format!("Failed to build restore runtime: {}", err)))?;
        runtime.block_on(run_restore_prepare_jobs(
            file_manager,
            refs,
            registry,
            worker_threads.max(1),
        ))
    })
    .join()
    .map_err(|_| Error::IoError("Restore worker thread panicked".to_string()))??;
    resources.finalize();
    Ok(prepared)
}

async fn run_restore_prepare_jobs(
    file_manager: Arc<FileManager>,
    refs: Vec<RestoreFileRef>,
    registry: Arc<dyn RestoreCopyResourceRegistry + Send + Sync>,
    worker_count: usize,
) -> Result<HashMap<u64, Option<u64>>> {
    let worker_count = worker_count.max(1);
    let queue = Arc::new(Mutex::new(VecDeque::from(refs)));
    let mut join_set = JoinSet::new();
    for _ in 0..worker_count {
        let file_manager = Arc::clone(&file_manager);
        let queue = Arc::clone(&queue);
        let registry = Arc::clone(&registry);
        join_set.spawn_blocking(move || -> Result<Vec<(u64, Option<u64>)>> {
            let mut completed = Vec::new();
            loop {
                let next = {
                    let mut guard = queue.lock().unwrap();
                    guard.pop_front()
                };
                let Some(file) = next else {
                    break;
                };
                let restored = file_manager.register_data_file_for_restore(
                    file.file_id,
                    &file.path,
                    Some(file.size_bytes),
                    Some(Arc::clone(&registry)),
                )?;
                completed.push((file.file_id, restored.snapshot_link_file_id));
            }
            Ok(completed)
        });
    }
    let mut links = HashMap::new();
    while let Some(joined) = join_set.join_next().await {
        let completed = joined
            .map_err(|err| Error::IoError(format!("Restore worker join failed: {}", err)))??;
        for (file_id, snapshot_link_file_id) in completed {
            links.insert(file_id, snapshot_link_file_id);
        }
    }
    Ok(links)
}

#[derive(Clone, Debug)]
struct RestoreFileRef {
    file_id: u64,
    path: String,
    size_bytes: u64,
    priority: u8,
}

fn ordered_manifest_data_file_refs_for_restore(
    manifest: &ManifestSnapshot,
    policy: PrimaryVolumeOffloadPolicyKind,
) -> Vec<RestoreFileRef> {
    let mut refs: HashMap<u64, RestoreFileRef> = HashMap::new();
    for tree_levels in &manifest.tree_levels {
        for level in tree_levels {
            let level_priority = lsm_file_priority_for_level(level.ordinal);
            for file in &level.files {
                let entry = refs.entry(file.file_id).or_insert_with(|| RestoreFileRef {
                    file_id: file.file_id,
                    path: file.path.clone(),
                    size_bytes: file.size as u64,
                    priority: level_priority,
                });
                if file.size as u64 > entry.size_bytes {
                    entry.size_bytes = file.size as u64;
                }
                if level_priority < entry.priority {
                    entry.priority = level_priority;
                }
            }
        }
    }
    for file in &manifest.vlog_files {
        let entry = refs.entry(file.file_id).or_insert_with(|| RestoreFileRef {
            file_id: file.file_id,
            path: file.path.clone(),
            size_bytes: 0,
            priority: VLOG_FILE_PRIORITY,
        });
        entry.priority = entry.priority.min(VLOG_FILE_PRIORITY);
    }
    let mut ordered: Vec<RestoreFileRef> = refs.into_values().collect();
    ordered.sort_by(|left, right| {
        let left_ref = PrimaryOffloadFileRef {
            file_id: left.file_id,
            size_bytes: left.size_bytes,
            priority: left.priority,
        };
        let right_ref = PrimaryOffloadFileRef {
            file_id: right.file_id,
            size_bytes: right.size_bytes,
            priority: right.priority,
        };
        compare_primary_offload_file_refs(policy, &left_ref, &right_ref)
    });
    ordered
}

fn apply_restore_snapshot_links(
    tree_versions: &[LSMTreeVersion],
    file_manager: &Arc<FileManager>,
    restored_snapshot_links: &HashMap<u64, Option<u64>>,
) {
    for file in tree_versions
        .iter()
        .flat_map(|version| version.levels.iter())
        .flat_map(|level| level.files.iter())
    {
        if let Some(snapshot_data_file_id) = restored_snapshot_links
            .get(&file.file_id)
            .copied()
            .flatten()
            && snapshot_data_file_id != file.file_id
        {
            file.set_snapshot_data_file(TrackedFileId::new(file_manager, snapshot_data_file_id));
            file_manager.register_snapshot_replica_hint(file.file_id, snapshot_data_file_id);
        }
    }
}

fn can_incremental_snapshot_from_tree_versions(
    tree_versions: &[LSMTreeVersion],
    file_manager: &Arc<FileManager>,
) -> bool {
    tree_versions
        .iter()
        .flat_map(|version| version.levels.iter())
        .flat_map(|level| level.files.iter())
        .all(|file| {
            file.snapshot_data_file_id().is_some()
                || file_manager.is_data_file_on_snapshot_volume(file.file_id)
        })
}

#[allow(clippy::too_many_arguments)]
fn open_restored_db_from_manifest(
    config: Config,
    file_manager: Arc<FileManager>,
    db_id: String,
    hybrid_cache_plan: Option<crate::config::HybridCacheVolumePlan>,
    metrics_manager: Arc<MetricsManager>,
    schema_manager: Arc<crate::schema::SchemaManager>,
    manifest: ManifestSnapshot,
    suggested_base_snapshot_id: Option<u64>,
    advance_next_id_from_existing_manifests: bool,
) -> Result<Db> {
    let restored_snapshot_links =
        prepare_manifest_data_files_for_restore(&file_manager, &manifest)?;
    let max_vlog_file_seq = manifest
        .vlog_files
        .iter()
        .map(|file| file.file_seq as u64)
        .max()
        .unwrap_or(0);
    let restored_seq_id = manifest.seq_id;
    if manifest.bucket_ranges.is_empty() {
        return Err(Error::InvalidState(
            "Snapshot manifest missing bucket_ranges".to_string(),
        ));
    }
    let bucket_ranges = manifest.bucket_ranges.clone();
    let active_memtable_data = manifest.active_memtable_data.clone();
    let tree_versions = build_tree_versions_from_manifest(&file_manager, &manifest, false)?;
    apply_restore_snapshot_links(&tree_versions, &file_manager, &restored_snapshot_links);
    let vlog_version = build_vlog_version_from_manifest(&file_manager, &manifest, false)?;
    let tree_scopes = build_tree_scopes_from_manifest(&manifest);
    let can_incremental_base =
        can_incremental_snapshot_from_tree_versions(&tree_versions, &file_manager);
    let multi_lsm_version = MultiLSMTreeVersion::from_scopes_with_tree_versions(
        config.total_buckets,
        &tree_scopes,
        tree_versions.into_iter().map(Arc::new).collect(),
    )?;

    let db_state = Arc::new(DbStateHandle::new());
    let db_lifecycle = Arc::new(DbLifecycle::new_initializing());
    db_state.store(crate::db_state::DbState {
        seq_id: restored_seq_id,
        bucket_ranges: bucket_ranges.clone(),
        multi_lsm_version,
        vlog_version,
        active: None,
        immutables: VecDeque::new(),
        suggested_base_snapshot_id: suggested_base_snapshot_id.filter(|_| can_incremental_base),
    });
    let db = Db::open_with_state(
        config,
        Arc::clone(&file_manager),
        db_state,
        Arc::clone(&db_lifecycle),
        db_id,
        bucket_ranges,
        max_vlog_file_seq.saturating_add(1).min(u32::MAX as u64) as u32,
        hybrid_cache_plan,
        metrics_manager,
        schema_manager,
    )?;
    if advance_next_id_from_existing_manifests
        && let Some(max_snapshot_id) = list_snapshot_manifest_ids(&file_manager)?.into_iter().max()
    {
        db.snapshot_manager
            .advance_next_id(max_snapshot_id.saturating_add(1));
    }
    db.restore_active_memtable_snapshot_to_l0(&active_memtable_data)?;
    db.memtable_manager.open()?;
    db.db_lifecycle.mark_open()?;
    Ok(db)
}

impl Db {
    /// Open a writable database initialized from a snapshot manifest.
    pub fn open_from_snapshot(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
    ) -> Result<Self> {
        Self::open_from_snapshot_with_resolver(config, snapshot_id, db_id, None)
    }

    pub fn open_from_snapshot_with_resolver(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        let db_id = db_id.into();
        let config = config.normalize_volume_paths()?;
        init_logging(&config);
        log::info!(
            "cobble=db runtime start version={} build_commit={}",
            build_version_string(),
            build_commit_short_id()
        );
        metrics_registry::init_metrics();
        let metrics_manager = Arc::new(MetricsManager::new(&db_id));
        let hybrid_cache_plan =
            config.resolve_hybrid_cache_volume_plan(config.block_cache_size_bytes()?)?;
        let file_manager_config =
            config.apply_hybrid_cache_primary_partition_with_plan(hybrid_cache_plan.as_ref())?;
        let file_manager =
            FileManager::from_config(&file_manager_config, &db_id, Arc::clone(&metrics_manager))?;
        let file_manager = Arc::new(file_manager);
        let manifest = load_manifest_for_snapshot(&file_manager, snapshot_id)?;
        let schema_manager = Arc::new(crate::schema::SchemaManager::from_manifest(
            &file_manager,
            &manifest,
            resolver,
        )?);
        open_restored_db_from_manifest(
            config,
            file_manager,
            db_id,
            hybrid_cache_plan,
            metrics_manager,
            schema_manager,
            manifest,
            Some(snapshot_id),
            true,
        )
    }

    /// Open a fresh writable database from an existing source snapshot.
    ///
    /// Unlike [`Db::open_from_snapshot`], this creates a new runtime db id and starts a new
    /// snapshot chain in the target db directory. The source manifest is used only as restore
    /// input.
    pub fn open_new_with_snapshot(
        config: Config,
        snapshot_id: u64,
        source_db_id: impl AsRef<str>,
    ) -> Result<Self> {
        Self::open_new_with_snapshot_with_resolver(config, snapshot_id, source_db_id, None)
    }

    pub fn open_new_with_snapshot_with_resolver(
        config: Config,
        snapshot_id: u64,
        source_db_id: impl AsRef<str>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        let manifest_path = bucket_snapshot_manifest_path(source_db_id.as_ref(), snapshot_id);
        Self::open_new_with_manifest_path_with_resolver(config, manifest_path, resolver)
    }

    pub fn open_new_with_manifest_path(
        config: Config,
        manifest_path: impl Into<String>,
    ) -> Result<Self> {
        Self::open_new_with_manifest_path_with_resolver(config, manifest_path, None)
    }

    pub fn open_new_with_manifest_path_with_resolver(
        config: Config,
        manifest_path: impl Into<String>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        let manifest_path = manifest_path.into();
        let config = config.normalize_volume_paths()?;
        init_logging(&config);
        log::info!(
            "cobble=db runtime start version={} build_commit={}",
            build_version_string(),
            build_commit_short_id()
        );
        metrics_registry::init_metrics();
        let db_id = Uuid::new_v4().to_string();
        let metrics_manager = Arc::new(MetricsManager::new(&db_id));
        let hybrid_cache_plan =
            config.resolve_hybrid_cache_volume_plan(config.block_cache_size_bytes()?)?;
        let file_manager_config =
            config.apply_hybrid_cache_primary_partition_with_plan(hybrid_cache_plan.as_ref())?;
        let file_manager =
            FileManager::from_config(&file_manager_config, &db_id, Arc::clone(&metrics_manager))?;
        let file_manager = Arc::new(file_manager);
        let manifest_chain = load_manifest_chain_from_path(&file_manager, &manifest_path)?;
        let manifest = manifest_chain
            .last()
            .map(|entry| entry.manifest.clone())
            .ok_or_else(|| {
                Error::IoError(format!("Snapshot manifest not found: {}", manifest_path))
            })?;
        let schema_manager = Arc::new(
            crate::schema::SchemaManager::from_snapshot_source_manifests(
                &file_manager,
                &manifest_path,
                manifest_chain.iter().map(|entry| &entry.manifest),
                resolver,
            )?,
        );
        schema_manager.persist_loaded_schemas(&file_manager)?;
        open_restored_db_from_manifest(
            config,
            file_manager,
            db_id,
            hybrid_cache_plan,
            metrics_manager,
            schema_manager,
            manifest,
            None,
            false,
        )
    }

    /// Resume a writable database from an existing folder by loading all snapshot manifests.
    pub fn resume(config: Config, db_id: impl Into<String>) -> Result<Self> {
        Self::resume_with_resolver(config, db_id, None)
    }

    pub fn resume_with_resolver(
        config: Config,
        db_id: impl Into<String>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        let db_id = db_id.into();
        let config = config.normalize_volume_paths()?;
        init_logging(&config);
        log::info!(
            "Cobble db ({}, Rev:{}) start.",
            build_version_string(),
            build_commit_short_id()
        );
        metrics_registry::init_metrics();
        let metrics_manager = Arc::new(MetricsManager::new(&db_id));
        let hybrid_cache_plan =
            config.resolve_hybrid_cache_volume_plan(config.block_cache_size_bytes()?)?;
        let file_manager_config =
            config.apply_hybrid_cache_primary_partition_with_plan(hybrid_cache_plan.as_ref())?;
        let file_manager =
            FileManager::from_config(&file_manager_config, &db_id, Arc::clone(&metrics_manager))?;
        let file_manager = Arc::new(file_manager);
        let snapshot_ids = list_snapshot_manifest_ids(&file_manager)?;
        if snapshot_ids.is_empty() {
            return Err(Error::IoError(format!(
                "No snapshot manifests found for db {}",
                db_id
            )));
        }

        let mut loaded = Vec::with_capacity(snapshot_ids.len());
        let mut loaded_by_id = HashMap::new();
        for snapshot_id in snapshot_ids {
            let entry = load_manifest_entry(&file_manager, snapshot_id, &loaded_by_id)?;
            loaded_by_id.insert(snapshot_id, entry.clone());
            loaded.push(entry);
        }
        let latest = loaded.last().ok_or_else(|| {
            Error::IoError(format!("No snapshot manifests found for db {}", db_id))
        })?;
        let manifest = latest.manifest.clone();
        if manifest.bucket_ranges.is_empty() {
            return Err(Error::InvalidState(format!(
                "Snapshot {} manifest missing bucket_ranges",
                latest.snapshot_id
            )));
        }
        let restored_snapshot_links =
            prepare_manifest_data_files_for_restore(&file_manager, &manifest)?;
        let bucket_ranges = manifest.bucket_ranges.clone();
        let _lsm_tree_bucket_ranges = if manifest.lsm_tree_bucket_ranges.is_empty() {
            manifest.bucket_ranges.clone()
        } else {
            manifest.lsm_tree_bucket_ranges.clone()
        };
        let active_memtable_data = manifest.active_memtable_data.clone();
        let schema_manager = Arc::new(crate::schema::SchemaManager::from_manifests(
            &file_manager,
            loaded.iter().map(|entry| &entry.manifest),
            resolver,
        )?);
        let max_vlog_file_seq = manifest
            .vlog_files
            .iter()
            .map(|file| file.file_seq as u64)
            .max()
            .unwrap_or(0);
        let restored_seq_id = latest.manifest.seq_id;
        let tree_versions = build_tree_versions_from_manifest(&file_manager, &manifest, false)?;
        apply_restore_snapshot_links(&tree_versions, &file_manager, &restored_snapshot_links);
        let vlog_version = build_vlog_version_from_manifest(&file_manager, &manifest, false)?;
        let can_incremental_base =
            can_incremental_snapshot_from_tree_versions(&tree_versions, &file_manager);
        let tree_scopes = build_tree_scopes_from_manifest(&manifest);
        let multi_lsm_version = MultiLSMTreeVersion::from_scopes_with_tree_versions(
            config.total_buckets,
            &tree_scopes,
            tree_versions.into_iter().map(Arc::new).collect(),
        )?;
        let db_state = Arc::new(DbStateHandle::new());
        let db_lifecycle = Arc::new(DbLifecycle::new_initializing());
        db_state.store(crate::db_state::DbState {
            seq_id: restored_seq_id,
            bucket_ranges: bucket_ranges.clone(),
            multi_lsm_version,
            vlog_version,
            active: None,
            immutables: VecDeque::new(),
            suggested_base_snapshot_id: can_incremental_base.then_some(latest.snapshot_id),
        });
        let db = Self::open_with_state(
            config,
            file_manager,
            db_state,
            Arc::clone(&db_lifecycle),
            db_id,
            bucket_ranges,
            max_vlog_file_seq.saturating_add(1).min(u32::MAX as u64) as u32,
            hybrid_cache_plan,
            metrics_manager,
            schema_manager,
        )?;
        db.take_over_snapshot_chain(&loaded)?;
        db.restore_active_memtable_snapshot_to_l0(&active_memtable_data)?;
        db.memtable_manager.open()?;
        db.db_lifecycle.mark_open()?;
        Ok(db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(refs: Vec<RestoreFileRef>) -> Vec<u64> {
        refs.into_iter().map(|item| item.file_id).collect()
    }

    #[test]
    fn restore_ref_order_priority_policy() {
        let mut refs = vec![
            RestoreFileRef {
                file_id: 3,
                path: "a".to_string(),
                size_bytes: 100,
                priority: 10,
            },
            RestoreFileRef {
                file_id: 1,
                path: "b".to_string(),
                size_bytes: 200,
                priority: 2,
            },
            RestoreFileRef {
                file_id: 2,
                path: "c".to_string(),
                size_bytes: 50,
                priority: 2,
            },
        ];
        refs.sort_by(|left, right| {
            compare_primary_offload_file_refs(
                PrimaryVolumeOffloadPolicyKind::Priority,
                &PrimaryOffloadFileRef {
                    file_id: left.file_id,
                    size_bytes: left.size_bytes,
                    priority: left.priority,
                },
                &PrimaryOffloadFileRef {
                    file_id: right.file_id,
                    size_bytes: right.size_bytes,
                    priority: right.priority,
                },
            )
        });
        assert_eq!(ids(refs), vec![1, 2, 3]);
    }

    #[test]
    fn restore_ref_order_largest_file_policy() {
        let mut refs = vec![
            RestoreFileRef {
                file_id: 3,
                path: "a".to_string(),
                size_bytes: 100,
                priority: 10,
            },
            RestoreFileRef {
                file_id: 1,
                path: "b".to_string(),
                size_bytes: 200,
                priority: 2,
            },
            RestoreFileRef {
                file_id: 2,
                path: "c".to_string(),
                size_bytes: 200,
                priority: 8,
            },
        ];
        refs.sort_by(|left, right| {
            compare_primary_offload_file_refs(
                PrimaryVolumeOffloadPolicyKind::LargestFile,
                &PrimaryOffloadFileRef {
                    file_id: left.file_id,
                    size_bytes: left.size_bytes,
                    priority: left.priority,
                },
                &PrimaryOffloadFileRef {
                    file_id: right.file_id,
                    size_bytes: right.size_bytes,
                    priority: right.priority,
                },
            )
        });
        assert_eq!(ids(refs), vec![1, 2, 3]);
    }
}
