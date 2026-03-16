use super::Db;
use crate::Config;
use crate::db_state::{DbStateHandle, MultiLSMTreeVersion};
use crate::db_status::DbLifecycle;
use crate::error::{Error, Result};
use crate::file::{FileManager, RestoreCopyResourceRegistry, TrackedFileId};
use crate::lsm::LSMTreeVersion;
use crate::merge_operator::MergeOperatorResolver;
use crate::metrics_manager::MetricsManager;
use crate::metrics_registry;
use crate::snapshot::{
    ManifestSnapshot, build_tree_versions_from_manifest, build_vlog_version_from_manifest,
    list_snapshot_manifest_ids, load_manifest_entry, load_manifest_for_snapshot,
    manifest_data_file_refs,
};
use crate::util::init_logging;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::runtime::Builder;
use tokio::task::JoinSet;

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
    let refs: Vec<(u64, String)> = manifest_data_file_refs(manifest).collect();
    if refs.is_empty() {
        return Ok(HashMap::new());
    }
    if let Some(max_file_id) = refs.iter().map(|(file_id, _)| *file_id).max() {
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
        runtime.block_on(run_restore_prepare_jobs(file_manager, refs, registry))
    })
    .join()
    .map_err(|_| Error::IoError("Restore worker thread panicked".to_string()))??;
    resources.finalize();
    Ok(prepared)
}

async fn run_restore_prepare_jobs(
    file_manager: Arc<FileManager>,
    refs: Vec<(u64, String)>,
    registry: Arc<dyn RestoreCopyResourceRegistry + Send + Sync>,
) -> Result<HashMap<u64, Option<u64>>> {
    let mut join_set = JoinSet::new();
    for (file_id, path) in refs {
        let file_manager = Arc::clone(&file_manager);
        let registry = Arc::clone(&registry);
        join_set.spawn_blocking(move || {
            file_manager
                .register_data_file_for_restore(file_id, &path, Some(registry))
                .map(|restored| (file_id, restored.snapshot_link_file_id))
        });
    }
    let mut links = HashMap::new();
    while let Some(joined) = join_set.join_next().await {
        let (file_id, snapshot_link_file_id) = joined
            .map_err(|err| Error::IoError(format!("Restore worker join failed: {}", err)))??;
        links.insert(file_id, snapshot_link_file_id);
    }
    Ok(links)
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

impl Db {
    /// Open a writable database initialized from a snapshot manifest.
    pub fn open_from_snapshot(config: Config, snapshot_id: u64, db_id: String) -> Result<Self> {
        Self::open_from_snapshot_with_resolver(config, snapshot_id, db_id, None)
    }

    pub fn open_from_snapshot_with_resolver(
        config: Config,
        snapshot_id: u64,
        db_id: String,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        let config = config.normalize_volume_paths()?;
        init_logging(&config);
        metrics_registry::init_metrics();
        let metrics_manager = Arc::new(MetricsManager::new(&db_id));
        let hybrid_cache_plan = config.resolve_hybrid_cache_volume_plan(config.block_cache_size)?;
        let file_manager_config =
            config.apply_hybrid_cache_primary_partition_with_plan(hybrid_cache_plan.as_ref())?;
        let file_manager =
            FileManager::from_config(&file_manager_config, &db_id, Arc::clone(&metrics_manager))?;
        let file_manager = Arc::new(file_manager);
        let manifest = load_manifest_for_snapshot(&file_manager, snapshot_id)?;
        let schema_manager = Arc::new(crate::schema::SchemaManager::from_manifest(
            &file_manager,
            &manifest,
            config.num_columns,
            resolver,
        )?);
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
            return Err(Error::InvalidState(format!(
                "Snapshot {} manifest missing bucket_ranges",
                snapshot_id
            )));
        }
        let bucket_ranges = manifest.bucket_ranges.clone();
        let lsm_tree_bucket_ranges = if manifest.lsm_tree_bucket_ranges.is_empty() {
            manifest.bucket_ranges.clone()
        } else {
            manifest.lsm_tree_bucket_ranges.clone()
        };
        let active_memtable_data = manifest.active_memtable_data.clone();
        let tree_versions = build_tree_versions_from_manifest(&file_manager, &manifest, false)?;
        apply_restore_snapshot_links(&tree_versions, &file_manager, &restored_snapshot_links);
        let vlog_version = build_vlog_version_from_manifest(&file_manager, &manifest, false)?;
        let can_incremental_base =
            can_incremental_snapshot_from_tree_versions(&tree_versions, &file_manager);
        let multi_lsm_version = MultiLSMTreeVersion::from_bucket_ranges_with_tree_versions(
            config.total_buckets,
            &lsm_tree_bucket_ranges,
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
            immutables: Vec::new().into(),
            suggested_base_snapshot_id: can_incremental_base.then_some(snapshot_id),
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
        db.restore_active_memtable_snapshot_to_l0(&active_memtable_data)?;
        db.memtable_manager.open()?;
        db.db_lifecycle.mark_open()?;
        Ok(db)
    }

    /// Resume a writable database from an existing folder by loading all snapshot manifests.
    pub fn resume(config: Config, db_id: String) -> Result<Self> {
        Self::resume_with_resolver(config, db_id, None)
    }

    pub fn resume_with_resolver(
        config: Config,
        db_id: String,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        let config = config.normalize_volume_paths()?;
        init_logging(&config);
        metrics_registry::init_metrics();
        let metrics_manager = Arc::new(MetricsManager::new(&db_id));
        let hybrid_cache_plan = config.resolve_hybrid_cache_volume_plan(config.block_cache_size)?;
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
        let lsm_tree_bucket_ranges = if manifest.lsm_tree_bucket_ranges.is_empty() {
            manifest.bucket_ranges.clone()
        } else {
            manifest.lsm_tree_bucket_ranges.clone()
        };
        let active_memtable_data = manifest.active_memtable_data.clone();
        let schema_manager = Arc::new(crate::schema::SchemaManager::from_manifests(
            &file_manager,
            loaded.iter().map(|entry| &entry.manifest),
            config.num_columns,
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
        let multi_lsm_version = MultiLSMTreeVersion::from_bucket_ranges_with_tree_versions(
            config.total_buckets,
            &lsm_tree_bucket_ranges,
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
            immutables: Vec::new().into(),
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
