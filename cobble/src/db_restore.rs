use super::Db;
use crate::Config;
use crate::config::{
    PrimaryVolumeOffloadPolicyKind, VolumeDescriptor, resolve_volume_descriptor_credentials,
};
use crate::db_state::{DbStateHandle, MultiLSMTreeVersion, new_truncation_cursors_with};
use crate::db_status::DbLifecycle;
use crate::error::{Error, Result};
use crate::file::logical_file::ReplicaOrigin;
use crate::file::{
    FileManager, PrimaryDataPlacement, PrimaryOffloadFileRef, RestoreCopyResourceRegistry,
    VLOG_FILE_PRIORITY, compare_primary_offload_file_refs, lsm_file_priority_for_level,
};
use crate::lsm::LSMTreeVersion;
use crate::merge_operator::MergeOperatorResolver;
use crate::metrics_manager::MetricsManager;
use crate::metrics_registry;
use crate::paths::bucket_snapshot_manifest_path;
use crate::snapshot::{
    ManifestSnapshot, build_tree_scopes_from_manifest, build_tree_versions_from_manifest,
    build_truncation_cursors_from_manifest, build_vlog_version_from_manifest,
    list_snapshot_manifest_ids, load_manifest_chain_from_path, load_manifest_entry,
    load_manifest_for_snapshot,
};
use crate::util::{build_commit_short_id, build_version_string, init_logging};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::runtime::Builder;
use tokio::task::JoinSet;
use uuid::Uuid;

/// Selects how a writable database is recovered from a snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RecoveryMode {
    /// Restore exactly the selected snapshot state.
    #[default]
    SnapshotOnly,
    /// Replay durable WAL entries after the selected snapshot only when it is the latest;
    /// otherwise restore the selected snapshot exactly.
    LatestWithWal,
}

#[derive(Clone, Copy, Debug)]
struct ResumeAllocatorFloors {
    next_file_id: u64,
    next_vlog_file_seq: u32,
    next_state_seq_id: u64,
}

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
    retained_owned_source_id: Option<&str>,
) -> Result<Vec<u64>> {
    let refs = ordered_manifest_data_file_refs_for_restore(
        manifest,
        file_manager.options.primary_volume_offload_policy,
        retained_owned_source_id,
    );
    if refs.is_empty() {
        return Ok(Vec::new());
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
    let residual_file_ids = std::thread::spawn(move || {
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
    Ok(residual_file_ids)
}

fn register_scanned_primary_residuals(
    file_manager: &Arc<FileManager>,
    manifest: &ManifestSnapshot,
) {
    let mut candidates_by_name = HashMap::new();
    for candidate in file_manager.scan_primary_residual_files() {
        candidates_by_name
            .entry(candidate.file_name.clone())
            .or_insert_with(Vec::new)
            .push(candidate);
    }
    if candidates_by_name.is_empty() {
        return;
    }

    let refs = ordered_manifest_data_file_refs_for_restore(
        manifest,
        file_manager.options.primary_volume_offload_policy,
        None,
    );
    for file in refs {
        let Some(file_name) = file.path.rsplit('/').next() else {
            continue;
        };
        let Some(candidates) = candidates_by_name.get(file_name) else {
            continue;
        };
        let expected_size = file
            .expected_size_bytes
            .or_else(|| file_manager.data_file_size_at_path(&file.path).ok());
        let Some(expected_size) = expected_size else {
            continue;
        };
        for candidate in candidates {
            if candidate.size_bytes != expected_size {
                continue;
            }
            let _ = file_manager.register_primary_residual_replica(
                file.file_id,
                &candidate.absolute_path,
                candidate.size_bytes,
            );
        }
    }
}

async fn run_restore_prepare_jobs(
    file_manager: Arc<FileManager>,
    refs: Vec<RestoreFileRef>,
    registry: Arc<dyn RestoreCopyResourceRegistry + Send + Sync>,
    worker_count: usize,
) -> Result<Vec<u64>> {
    let worker_count = worker_count.max(1);
    let queue = Arc::new(Mutex::new(VecDeque::from(refs)));
    let mut join_set = JoinSet::new();
    for _ in 0..worker_count {
        let file_manager = Arc::clone(&file_manager);
        let queue = Arc::clone(&queue);
        let registry = Arc::clone(&registry);
        join_set.spawn_blocking(move || -> Result<Vec<u64>> {
            let mut residual_file_ids = Vec::new();
            loop {
                let next = {
                    let mut guard = queue.lock().unwrap();
                    guard.pop_front()
                };
                let Some(file) = next else {
                    break;
                };
                if file_manager.register_data_file_for_restore(
                    file.file_id,
                    &file.path,
                    file.origin,
                    file.placement,
                    file.expected_size_bytes,
                    Some(Arc::clone(&registry)),
                )? {
                    residual_file_ids.push(file.file_id);
                }
            }
            Ok(residual_file_ids)
        });
    }
    let mut residual_file_ids = Vec::new();
    while let Some(joined) = join_set.join_next().await {
        residual_file_ids.extend(
            joined
                .map_err(|err| Error::IoError(format!("Restore worker join failed: {}", err)))??,
        );
    }
    Ok(residual_file_ids)
}

#[derive(Clone, Debug)]
struct RestoreFileRef {
    file_id: u64,
    path: String,
    origin: ReplicaOrigin,
    size_bytes: u64,
    expected_size_bytes: Option<u64>,
    priority: u8,
    placement: PrimaryDataPlacement,
}

fn ordered_manifest_data_file_refs_for_restore(
    manifest: &ManifestSnapshot,
    policy: PrimaryVolumeOffloadPolicyKind,
    retained_owned_source_id: Option<&str>,
) -> Vec<RestoreFileRef> {
    let mut refs: HashMap<u64, RestoreFileRef> = HashMap::new();
    for tree_levels in &manifest.tree_levels {
        for level in tree_levels {
            let level_priority = lsm_file_priority_for_level(level.ordinal);
            for file in &level.files {
                let entry = refs.entry(file.file_id).or_insert_with(|| RestoreFileRef {
                    file_id: file.file_id,
                    path: file.path.clone(),
                    origin: restore_source_origin(&file.origin, retained_owned_source_id),
                    size_bytes: file.size as u64,
                    expected_size_bytes: Some(file.size as u64),
                    priority: level_priority,
                    placement: PrimaryDataPlacement::Standard,
                });
                if file.size as u64 > entry.size_bytes {
                    entry.size_bytes = file.size as u64;
                }
                if entry
                    .expected_size_bytes
                    .is_none_or(|size| file.size as u64 > size)
                {
                    entry.expected_size_bytes = Some(file.size as u64);
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
            origin: restore_source_origin(&file.origin, retained_owned_source_id),
            size_bytes: 0,
            expected_size_bytes: None,
            priority: VLOG_FILE_PRIORITY,
            placement: PrimaryDataPlacement::Vlog,
        });
        entry.priority = entry.priority.min(VLOG_FILE_PRIORITY);
        entry.placement = PrimaryDataPlacement::Vlog;
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

fn restore_source_origin(
    origin: &ReplicaOrigin,
    retained_owned_source_id: Option<&str>,
) -> ReplicaOrigin {
    match (origin, retained_owned_source_id) {
        (ReplicaOrigin::Owned, Some(source_id)) => ReplicaOrigin::ExternalPersistent {
            source_id: source_id.to_string(),
        },
        _ => origin.clone(),
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
        .all(|file| file_manager.is_data_file_on_snapshot_volume(file.file_id))
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
    retained_owned_source_id: Option<String>,
    recovery_wal_volume: Option<VolumeDescriptor>,
) -> Result<Db> {
    let residual_primary_file_ids = prepare_manifest_data_files_for_restore(
        &file_manager,
        &manifest,
        retained_owned_source_id.as_deref(),
    )?;
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
    let truncation_cursors = build_truncation_cursors_from_manifest(&manifest)?;
    let tree_versions = build_tree_versions_from_manifest(&file_manager, &manifest, false)?;
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
        topology_epoch: manifest.topology_epoch,
        bucket_ranges: bucket_ranges.clone(),
        multi_lsm_version,
        vlog_version,
        active: None,
        immutables: VecDeque::new(),
        truncation_cursors: new_truncation_cursors_with(truncation_cursors),
        suggested_base_snapshot_id: suggested_base_snapshot_id.filter(|_| can_incremental_base),
    });
    let mut db = Db::open_with_state(
        config,
        Arc::clone(&file_manager),
        db_state,
        Arc::clone(&db_lifecycle),
        db_id,
        None,
        bucket_ranges,
        max_vlog_file_seq.saturating_add(1).min(u32::MAX as u64) as u32,
        manifest.wal_checkpoint_id,
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
    if let Some(recovery_wal_volume) = recovery_wal_volume.as_ref() {
        db.replay_wal_after_checkpoint(manifest.wal_checkpoint_id, recovery_wal_volume)?;
    }
    file_manager.commit_logical_files(residual_primary_file_ids.iter().copied());
    for file_id in residual_primary_file_ids {
        file_manager.adopt_primary_residual_replicas(file_id);
    }
    db.db_lifecycle.mark_open()?;
    db.start_dedicated_poller();
    Ok(db)
}

impl Db {
    /// Open a writable database at the exact state of a snapshot manifest.
    ///
    /// Unlike [`Db::resume`], this does not replay WAL entries newer than the snapshot.
    pub fn open_from_snapshot(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
    ) -> Result<Self> {
        Self::open_from_snapshot_with_recovery_mode(
            config,
            snapshot_id,
            db_id,
            RecoveryMode::SnapshotOnly,
        )
    }

    /// Open a writable database from a selected snapshot using the requested recovery mode.
    ///
    /// [`RecoveryMode::LatestWithWal`] replays a durable WAL tail only when `snapshot_id` is the
    /// latest snapshot; for historical snapshots it behaves as [`RecoveryMode::SnapshotOnly`].
    pub fn open_from_snapshot_with_recovery_mode(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
        recovery_mode: RecoveryMode,
    ) -> Result<Self> {
        Self::open_from_snapshot_with_recovery_mode_and_resolver(
            config,
            snapshot_id,
            db_id,
            recovery_mode,
            None,
        )
    }

    pub fn open_from_snapshot_with_resolver(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        Self::open_from_snapshot_with_recovery_mode_and_resolver(
            config,
            snapshot_id,
            db_id,
            RecoveryMode::SnapshotOnly,
            resolver,
        )
    }

    /// Like [`Db::open_from_snapshot_with_recovery_mode`], with custom merge operators.
    pub fn open_from_snapshot_with_recovery_mode_and_resolver(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
        recovery_mode: RecoveryMode,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        let db_id = db_id.into();
        let config = config.normalize_volume_paths()?;
        let retained_owned_source_id = Some(format!("snapshot:{db_id}:{snapshot_id}"));
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
        let is_latest_snapshot = list_snapshot_manifest_ids(&file_manager)?
            .last()
            .is_some_and(|latest_id| *latest_id == snapshot_id);
        let manifest = load_manifest_for_snapshot(&file_manager, snapshot_id)?;
        let recovery_wal_volume = (recovery_mode == RecoveryMode::LatestWithWal
            && is_latest_snapshot)
            .then(|| {
                manifest
                    .wal_volume
                    .as_ref()
                    .map(|route| resolve_volume_descriptor_credentials(route, &config))
            })
            .flatten();
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
            retained_owned_source_id,
            recovery_wal_volume,
        )
    }

    /// Open a fresh writable database from the exact state of an existing source snapshot.
    ///
    /// WAL entries from the source database are never replayed.
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
        let source_db_id = source_db_id.as_ref();
        let manifest_path = bucket_snapshot_manifest_path(source_db_id, snapshot_id);
        Self::open_new_with_manifest_path_and_source_id_with_resolver(
            config,
            manifest_path,
            format!("snapshot:{source_db_id}:{snapshot_id}"),
            resolver,
        )
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
        Self::open_new_with_manifest_path_and_source_id_with_resolver(
            config,
            manifest_path.clone(),
            format!("manifest:{manifest_path}"),
            resolver,
        )
    }

    fn open_new_with_manifest_path_and_source_id_with_resolver(
        config: Config,
        manifest_path: String,
        retained_owned_source_id: String,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
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
            Some(retained_owned_source_id),
            None,
        )
    }

    /// Resume a writable database from its latest snapshot and replay newer durable WAL entries.
    pub fn resume(config: Config, db_id: impl Into<String>) -> Result<Self> {
        Self::resume_with_recovery_mode(config, db_id, RecoveryMode::LatestWithWal)
    }

    /// Resume from the latest snapshot using the requested recovery mode.
    pub fn resume_with_recovery_mode(
        config: Config,
        db_id: impl Into<String>,
        recovery_mode: RecoveryMode,
    ) -> Result<Self> {
        Self::resume_with_recovery_mode_and_resolver(config, db_id, recovery_mode, None)
    }

    pub fn resume_with_resolver(
        config: Config,
        db_id: impl Into<String>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        Self::resume_with_recovery_mode_and_resolver(
            config,
            db_id,
            RecoveryMode::LatestWithWal,
            resolver,
        )
    }

    /// Resume a writable database from a selected snapshot in its existing snapshot chain.
    ///
    /// All snapshot manifests for the database are taken over by the resumed runtime, including
    /// snapshots newer than `snapshot_id`, so their existing retention lifecycle continues. New
    /// snapshots are allocated after the largest existing snapshot id. This defaults to exact
    /// snapshot recovery and does not replay a newer WAL tail.
    pub fn resume_from_snapshot(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
    ) -> Result<Self> {
        Self::resume_from_snapshot_with_recovery_mode(
            config,
            snapshot_id,
            db_id,
            RecoveryMode::SnapshotOnly,
        )
    }

    /// Resume from a selected snapshot using the requested recovery mode.
    ///
    /// [`RecoveryMode::LatestWithWal`] replays a durable WAL tail only when `snapshot_id` is the
    /// latest snapshot; historical snapshots are always restored exactly. Use
    /// [`RecoveryMode::SnapshotOnly`] for an exact latest snapshot boundary as well.
    pub fn resume_from_snapshot_with_recovery_mode(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
        recovery_mode: RecoveryMode,
    ) -> Result<Self> {
        Self::resume_from_snapshot_with_recovery_mode_and_resolver(
            config,
            snapshot_id,
            db_id,
            recovery_mode,
            None,
        )
    }

    pub fn resume_from_snapshot_with_resolver(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        Self::resume_from_snapshot_with_recovery_mode_and_resolver(
            config,
            snapshot_id,
            db_id,
            RecoveryMode::SnapshotOnly,
            resolver,
        )
    }

    /// Like [`Db::resume_from_snapshot_with_recovery_mode`], with custom merge operators.
    pub fn resume_from_snapshot_with_recovery_mode_and_resolver(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
        recovery_mode: RecoveryMode,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        Self::resume_internal(
            config,
            db_id,
            Some(snapshot_id),
            recovery_mode,
            resolver,
            None,
            None,
        )
    }

    /// Like [`Db::resume_with_recovery_mode`], with custom merge operators.
    pub fn resume_with_recovery_mode_and_resolver(
        config: Config,
        db_id: impl Into<String>,
        recovery_mode: RecoveryMode,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        Self::resume_internal(config, db_id, None, recovery_mode, resolver, None, None)
    }

    /// Switch this active handle to a historical snapshot by closing and rebuilding its runtime.
    ///
    /// The target is preflighted while the current database is still open. If preflight fails,
    /// this handle remains usable. Once normal close begins, a later restart error leaves the old
    /// handle closed. A successful switch changes only this handle and does not publish a snapshot
    /// or durable active marker. Until a new snapshot is created, restarting at the same historical
    /// state therefore requires [`Db::resume_from_snapshot`] with `snapshot_id`; ordinary
    /// [`Db::resume`] continues to select the greatest available snapshot id.
    ///
    /// When WAL is enabled, this operation does not truncate, fork, or otherwise isolate the
    /// existing WAL sequence. WAL records after the latest snapshot remain available so the
    /// caller can abandon the switch and resume that latest state. Until a new snapshot is
    /// published, however, WAL records written after this switch share the same sequence: an
    /// exact [`Db::resume_from_snapshot`] will not replay them, while ordinary [`Db::resume`] may
    /// replay them on top of the latest snapshot. See the WAL architecture documentation before
    /// allowing writes in this interval.
    ///
    /// This is intentionally a controlled restart, not an in-place `DbState` swap: background
    /// workers, WAL state, file tracking, governance, and schema state are rebuilt together.
    /// Snapshot metadata mutation for the same DB (including switch, prune, and publication from
    /// another process) must be externally serialized. This version does not provide distributed
    /// fencing for cross-process snapshot writers.
    pub fn switch_to_snapshot(&mut self, snapshot_id: u64) -> Result<()> {
        let resolver = self.schema_manager.merge_operator_resolver();
        self.switch_to_snapshot_with_resolver(snapshot_id, resolver)
    }

    /// Like [`Db::switch_to_snapshot`], using an explicit merge-operator resolver.
    pub fn switch_to_snapshot_with_resolver(
        &mut self,
        snapshot_id: u64,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<()> {
        {
            let _access = self.begin_access()?;
            self.preflight_switch_to_snapshot(snapshot_id, resolver.as_ref().map(Arc::clone))?;
        }

        let config = self.config.clone();
        let db_id = self.id.clone();
        let governance = self.db_governance.as_ref().map(Arc::clone);
        self.close()?;

        // Normal close may flush or compact additional files. Capture allocator floors only after
        // those workers have stopped, while the closed runtime still owns the final counters.
        let current_state_seq_floor = self
            .db_state
            .load()
            .seq_id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidState("DbState sequence id exhausted".to_string()))?;
        let allocator_floors = ResumeAllocatorFloors {
            next_file_id: self.file_manager.peek_next_file_id(),
            next_vlog_file_seq: self.vlog_store.next_file_seq(),
            next_state_seq_id: self.db_state.next_seq_id().max(current_state_seq_floor),
        };

        let replacement = Self::resume_from_snapshot_for_switch(
            config,
            snapshot_id,
            db_id,
            resolver,
            governance,
            allocator_floors,
        )?;
        *self = replacement;
        Ok(())
    }

    fn resume_from_snapshot_for_switch(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
        governance: Option<Arc<dyn crate::governance::DbGovernance>>,
        allocator_floors: ResumeAllocatorFloors,
    ) -> Result<Self> {
        Self::resume_internal(
            config,
            db_id,
            Some(snapshot_id),
            RecoveryMode::SnapshotOnly,
            resolver,
            governance,
            Some(allocator_floors),
        )
    }

    pub(super) fn preflight_switch_to_snapshot(
        &self,
        snapshot_id: u64,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<()> {
        if list_snapshot_manifest_ids(&self.file_manager)?
            .binary_search(&snapshot_id)
            .is_err()
        {
            return Err(Error::IoError(format!(
                "Snapshot {} manifest not found for db {}",
                snapshot_id, self.id
            )));
        }
        let manifest = load_manifest_for_snapshot(&self.file_manager, snapshot_id)?;
        if manifest.bucket_ranges.is_empty() {
            return Err(Error::InvalidState(format!(
                "Snapshot {} manifest missing bucket_ranges",
                snapshot_id
            )));
        }
        build_truncation_cursors_from_manifest(&manifest)?;
        let schema_manager =
            crate::schema::SchemaManager::from_manifest(&self.file_manager, &manifest, resolver)?;
        schema_manager.select_latest_schema_for_restore(manifest.latest_schema_id)?;
        // Preflight must not construct tracked handles: dropping a temporary TrackedFileId would
        // unregister the same logical file id that the still-active runtime is using.
        crate::snapshot::manifest::build_tree_versions_from_manifest_untracked(&manifest)?;
        crate::snapshot::manifest::build_vlog_version_from_manifest_untracked(&manifest);
        Ok(())
    }

    fn resume_internal(
        config: Config,
        db_id: impl Into<String>,
        selected_snapshot_id: Option<u64>,
        recovery_mode: RecoveryMode,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
        governance: Option<Arc<dyn crate::governance::DbGovernance>>,
        allocator_floors: Option<ResumeAllocatorFloors>,
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
            if let Some(snapshot_id) = selected_snapshot_id {
                return Err(Error::IoError(format!(
                    "Snapshot {} manifest not found for db {}",
                    snapshot_id, db_id
                )));
            }
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
        let manifest_next_file_id = loaded
            .iter()
            .flat_map(|entry| crate::snapshot::manifest::manifest_data_file_refs(&entry.manifest))
            .map(|(file_id, _, _)| file_id)
            .max()
            .map(|file_id| file_id.saturating_add(1))
            .unwrap_or(1);
        let min_next_file_id = allocator_floors
            .map(|floors| floors.next_file_id)
            .unwrap_or(1)
            .max(manifest_next_file_id);
        if file_manager.peek_next_file_id() < min_next_file_id {
            file_manager.set_next_file_id(min_next_file_id);
        }
        let latest_snapshot_id = loaded
            .last()
            .map(|entry| entry.snapshot_id)
            .ok_or_else(|| {
                Error::IoError(format!("No snapshot manifests found for db {}", db_id))
            })?;
        let selected_snapshot_id = selected_snapshot_id.unwrap_or(latest_snapshot_id);
        let selected = loaded
            .iter()
            .find(|entry| entry.snapshot_id == selected_snapshot_id)
            .ok_or_else(|| {
                Error::IoError(format!(
                    "Snapshot {} manifest not found for db {}",
                    selected_snapshot_id, db_id
                ))
            })?;
        let manifest = selected.manifest.clone();
        let recovery_wal_volume = (recovery_mode == RecoveryMode::LatestWithWal
            && selected_snapshot_id == latest_snapshot_id)
            .then(|| {
                manifest
                    .wal_volume
                    .as_ref()
                    .map(|route| resolve_volume_descriptor_credentials(route, &config))
            })
            .flatten();
        if manifest.bucket_ranges.is_empty() {
            return Err(Error::InvalidState(format!(
                "Snapshot {} manifest missing bucket_ranges",
                selected_snapshot_id
            )));
        }
        if config.resume_primary_residual_scan_enabled {
            register_scanned_primary_residuals(&file_manager, &manifest);
        }
        let residual_primary_file_ids =
            prepare_manifest_data_files_for_restore(&file_manager, &manifest, None)?;
        let bucket_ranges = manifest.bucket_ranges.clone();
        let _lsm_tree_bucket_ranges = if manifest.lsm_tree_bucket_ranges.is_empty() {
            manifest.bucket_ranges.clone()
        } else {
            manifest.lsm_tree_bucket_ranges.clone()
        };
        let active_memtable_data = manifest.active_memtable_data.clone();
        let truncation_cursors = build_truncation_cursors_from_manifest(&manifest)?;
        let schema_manager = Arc::new(crate::schema::SchemaManager::from_manifests(
            &file_manager,
            loaded.iter().map(|entry| &entry.manifest),
            resolver,
        )?);
        schema_manager.select_latest_schema_for_restore(manifest.latest_schema_id)?;
        let manifest_next_vlog_file_seq = loaded
            .iter()
            .flat_map(|entry| entry.manifest.vlog_files.iter())
            .map(|file| file.file_seq as u64)
            .max()
            .unwrap_or(0)
            .saturating_add(1)
            .min(u32::MAX as u64) as u32;
        let min_next_vlog_file_seq = allocator_floors
            .map(|floors| floors.next_vlog_file_seq)
            .unwrap_or(0)
            .max(manifest_next_vlog_file_seq);
        let restored_seq_id = manifest.seq_id;
        let manifest_next_state_seq_id = loaded
            .iter()
            .map(|entry| entry.manifest.seq_id)
            .max()
            .unwrap_or(restored_seq_id)
            .checked_add(1)
            .ok_or_else(|| Error::InvalidState("DbState sequence id exhausted".to_string()))?;
        let min_next_state_seq_id = allocator_floors
            .map(|floors| floors.next_state_seq_id)
            .unwrap_or(1)
            .max(manifest_next_state_seq_id);
        let tree_versions = build_tree_versions_from_manifest(&file_manager, &manifest, false)?;
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
            topology_epoch: manifest.topology_epoch,
            bucket_ranges: bucket_ranges.clone(),
            multi_lsm_version,
            vlog_version,
            active: None,
            immutables: VecDeque::new(),
            truncation_cursors: new_truncation_cursors_with(truncation_cursors),
            suggested_base_snapshot_id: can_incremental_base.then_some(selected_snapshot_id),
        });
        // Historical resume must not reuse sequence ids that appeared in newer snapshots: stale
        // persisted compaction work may still carry those ids as its CAS base.
        db_state.advance_next_seq_id(min_next_state_seq_id);
        if let Some(governance) = governance.as_ref() {
            governance.register_db(&db_id, &bucket_ranges, config.total_buckets)?;
        }
        let mut db = match Self::open_with_state(
            config,
            file_manager,
            db_state,
            Arc::clone(&db_lifecycle),
            db_id.clone(),
            governance.as_ref().map(Arc::clone),
            bucket_ranges,
            min_next_vlog_file_seq,
            manifest.wal_checkpoint_id,
            hybrid_cache_plan,
            metrics_manager,
            schema_manager,
        ) {
            Ok(db) => db,
            Err(err) => {
                if let Some(governance) = governance.as_ref() {
                    let _ = governance.unregister_db(&db_id);
                }
                return Err(err);
            }
        };
        db.take_over_snapshot_chain(&loaded)?;
        db.restore_active_memtable_snapshot_to_l0(&active_memtable_data)?;
        db.memtable_manager.open()?;
        if let Some(recovery_wal_volume) = recovery_wal_volume.as_ref() {
            db.replay_wal_after_checkpoint(manifest.wal_checkpoint_id, recovery_wal_volume)?;
        }
        db.file_manager
            .commit_logical_files(residual_primary_file_ids.iter().copied());
        for file_id in residual_primary_file_ids {
            db.file_manager.adopt_primary_residual_replicas(file_id);
        }
        db.db_lifecycle.mark_open()?;
        db.start_dedicated_poller();
        Ok(db)
    }
}

#[cfg(test)]
#[path = "../tests/unit/db_restore.rs"]
mod tests;
