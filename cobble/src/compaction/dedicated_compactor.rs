//! Standalone dedicated compactor process.
//!
//! The dedicated compactor is a separate process that compacts SST files and publishes the
//! result as a delta file on the shared volume. It does not call `Db::open`, does not create
//! memtables, and does not enter the writer/WAL path. It only reads writer snapshots, schemas,
//! and input data files, and only writes output data files and compaction result files.
//!
//! Communication with the writer is entirely through the shared volume:
//! - The compactor reads `SNAPSHOT-*` manifests to observe the writer's latest LSM state.
//! - The compactor publishes `compaction/results/COMPACTION-<job_id>` result files.
//! - The writer polls for results, applies them, commits a new manifest, and deletes the result.
//! - The result file's disappearance signals the compactor to proceed to the next plan.
use crate::compaction::dedicated::{
    DEDICATED_COMPACTION_RESULT_VERSION, DedicatedCompactionInput, DedicatedCompactionOperation,
    DedicatedCompactionResult, DedicatedDataFile, dedicated_compaction_job_output_prefix,
    publish_dedicated_compaction_result, write_job_lease,
};
use crate::compaction::policy::{
    CompactionPolicy, CompactionPolicyContext, MinOverlapPolicy, RoundRobinPolicy,
    ScorePriorityPolicy, build_runs_for_plan, file_fully_covered_by_truncation_cursor,
};
use crate::compaction::{
    CompactionConfig, CompactionExecutor, CompactionTask, CompactionTaskMetrics,
    build_compaction_config, build_writer_options, make_data_file_builder_factory,
};
use crate::config::Config;
use crate::db_state::LSMTreeScope;
use crate::db_status::DbLifecycle;
use crate::error::{Error, Result};
use crate::file::FileManager;
use crate::lsm::Level;
use crate::metrics_manager::MetricsManager;
use crate::schema::SchemaManager;
use crate::snapshot::manifest::{
    build_tree_versions_from_manifest, build_truncation_cursors_from_manifest,
    list_snapshot_manifest_ids, load_manifest_for_snapshot,
};
use crate::writer_options::WriterOptionsFactory;
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// A standalone dedicated compactor process.
///
/// Created via [`DedicatedCompactor::open`], it runs a main loop that polls for the writer's
/// latest snapshot, selects a compaction plan, executes the compaction, publishes the result,
/// and waits for the writer to consume it.
pub struct DedicatedCompactor {
    db_id: String,
    config: Config,
    file_manager: Arc<FileManager>,
    schema_manager: Arc<SchemaManager>,
    metrics_manager: Arc<MetricsManager>,
    /// Saved merge operator resolver, passed to every schema reload so custom merge
    /// operators remain functional across manifest refreshes.
    resolver: Option<Arc<dyn crate::MergeOperatorResolver>>,
    poll_interval: Duration,
    /// Interval at which the lease heartbeat is refreshed. Independently computed from
    /// `orphan_min_age / 3` (capped at `poll_interval`) so the lease is always refreshed well
    /// before the writer's orphan sweep would consider it stale, even if `poll_interval` is
    /// later changed to an unsafe value.
    heartbeat_interval: Duration,
    executor: CompactionExecutor,
    db_lifecycle: Arc<DbLifecycle>,
    compaction_metrics: Arc<CompactionTaskMetrics>,
    stop: Arc<AtomicBool>,
}

impl DedicatedCompactor {
    /// Opens a dedicated compactor for the given config and db_id.
    ///
    /// The db_id must match the writer's db_id so both processes point at the same shared
    /// volume. This does NOT call `Db::open` - it constructs an independent `FileManager` and
    /// `SchemaManager` that read from the shared volume.
    pub fn open(config: Config, db_id: impl Into<String>) -> Result<Self> {
        Self::open_with_resolver(config, db_id, None)
    }

    /// Opens a dedicated compactor with an optional merge operator resolver.
    #[allow(clippy::too_many_arguments)]
    pub fn open_with_resolver(
        config: Config,
        db_id: impl Into<String>,
        resolver: Option<Arc<dyn crate::MergeOperatorResolver>>,
    ) -> Result<Self> {
        // Re-validate dedicated compaction constraints. This catches unsafe overrides
        // (e.g. CLI --poll-interval) applied after Config::from_path.
        config.validate_dedicated_compaction()?;
        let db_id = db_id.into();
        let metrics_manager = Arc::new(MetricsManager::new(&db_id));
        let file_manager = Arc::new(FileManager::from_config(
            &config,
            &db_id,
            Arc::clone(&metrics_manager),
        )?);
        // Load the latest manifest to bootstrap schemas.
        let schema_manager = Self::load_schema_manager(&file_manager, resolver.clone())?;
        let db_lifecycle = Arc::new(DbLifecycle::new_open());
        let compaction_config = build_compaction_config(&config, config.num_columns)?;
        let executor = CompactionExecutor::new(compaction_config, Arc::clone(&db_lifecycle))?;
        let compaction_metrics = Arc::new(CompactionTaskMetrics::new(&db_id));
        let poll_interval = Duration::from_millis(config.compaction_dedicated_poll_interval_ms);
        // Compute the heartbeat interval as orphan_min_age / 3 (in ms), capped at poll_interval.
        // This ensures the lease is refreshed well before the orphan sweep considers it stale,
        // regardless of the poll interval. Config validation already enforces poll_interval <
        // orphan_min_age / 3, so this normally equals poll_interval, but the cap protects
        // against future config changes.
        let heartbeat_interval = {
            let min_age = config.compaction_orphan_min_age_ms;
            let heartbeat = min_age / 3;
            let heartbeat = heartbeat.max(1); // at least 1ms
            Duration::from_millis(heartbeat.min(config.compaction_dedicated_poll_interval_ms))
        };
        Ok(Self {
            db_id,
            config,
            file_manager,
            schema_manager,
            metrics_manager,
            resolver,
            poll_interval,
            heartbeat_interval,
            executor,
            db_lifecycle,
            compaction_metrics,
            stop: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Loads the schema manager from the latest manifest on the shared volume.
    fn load_schema_manager(
        file_manager: &Arc<FileManager>,
        resolver: Option<Arc<dyn crate::MergeOperatorResolver>>,
    ) -> Result<Arc<SchemaManager>> {
        let snapshot_ids = list_snapshot_manifest_ids(file_manager)?;
        if snapshot_ids.is_empty() {
            return Err(Error::InvalidState(
                "No snapshots found on shared volume; writer must create at least one snapshot before starting the compactor".to_string(),
            ));
        }
        let latest_id = *snapshot_ids.last().unwrap();
        let manifest = load_manifest_for_snapshot(file_manager, latest_id)?;
        let schema_manager = Arc::new(SchemaManager::from_manifest(
            file_manager,
            &manifest,
            resolver,
        )?);
        Ok(schema_manager)
    }

    /// Signals the compactor to stop its main loop.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    /// Runs the compactor main loop until `stop()` is called or a fatal error occurs.
    pub fn run(&self) -> Result<()> {
        info!("dedicated compactor started for db_id={}", self.db_id);
        while !self.stop.load(Ordering::SeqCst) {
            if let Err(err) = self.run_once() {
                if self.stop.load(Ordering::SeqCst) {
                    break;
                }
                error!("dedicated compactor iteration error: {}", err);
                std::thread::sleep(self.poll_interval);
            }
        }
        info!("dedicated compactor stopped");
        Ok(())
    }

    /// Runs one iteration of the main loop.
    ///
    /// Exposed so callers (e.g. tests) can drive the compactor one step at a time instead of
    /// entering the blocking [`run`](Self::run) loop.
    pub fn run_once(&self) -> Result<()> {
        // Step 1: Check if there's an unprocessed result. If so, wait for the writer to delete it.
        let existing_jobs = crate::compaction::dedicated::list_dedicated_compaction_result_job_ids(
            &self.file_manager,
        )?;
        if !existing_jobs.is_empty() {
            debug!(
                "waiting for writer to consume {} existing result(s)",
                existing_jobs.len()
            );
            std::thread::sleep(self.poll_interval);
            return Ok(());
        }

        // Step 2: Read the writer's latest snapshot.
        let snapshot_ids = list_snapshot_manifest_ids(&self.file_manager)?;
        if snapshot_ids.is_empty() {
            debug!("no snapshots found; waiting for writer to flush");
            std::thread::sleep(self.poll_interval);
            return Ok(());
        }
        let latest_id = *snapshot_ids.last().unwrap();
        let manifest = load_manifest_for_snapshot(&self.file_manager, latest_id)?;

        // Step 3: Rebuild read-only LSM state, schema, truncation cursors.
        // Reload schemas in case the writer evolved the schema. Pass the saved resolver so
        // custom merge operators remain functional.
        let schema_manager = Arc::new(SchemaManager::from_manifest(
            &self.file_manager,
            &manifest,
            self.resolver.clone(),
        )?);
        let tree_versions = build_tree_versions_from_manifest(
            &self.file_manager,
            &manifest,
            true, // read_only
        )?;
        let truncation_cursors = build_truncation_cursors_from_manifest(&manifest)?;
        let tree_scopes = manifest.tree_scopes.clone();

        // Advance the compactor's file-id allocator past all file ids in the manifest.
        // The compactor's FileManager is a separate instance from the writer's, so its
        // `next_file_id` starts at 1. Without this, the executor would allocate output file
        // ids (1, 2, ...) that collide with the writer's canonical ids already registered
        // readonly via `build_tree_versions_from_manifest`. A collision causes
        // `register_data_file` to silently keep the old path (via `or_insert_with`), so the
        // compactor would publish input descriptors with the wrong (output) paths, and the
        // writer's fingerprint matching would fail.
        let max_manifest_file_id = manifest
            .tree_levels
            .iter()
            .flat_map(|levels| levels.iter())
            .flat_map(|level| level.files.iter())
            .map(|f| f.file_id)
            .max()
            .unwrap_or(0);
        let next = self.file_manager.peek_next_file_id();
        if next <= max_manifest_file_id {
            self.file_manager.set_next_file_id(max_manifest_file_id + 1);
        }

        // Step 4: For each tree, try to select a compaction plan.
        let compaction_config = build_compaction_config(&self.config, self.config.num_columns)?;
        for (tree_idx, tree_version) in tree_versions.iter().enumerate() {
            let Some(tree_scope) = tree_scopes.get(tree_idx) else {
                continue;
            };
            let mut policy = Self::make_policy(self.config.compaction_policy);
            let policy_context = CompactionPolicyContext {
                truncation_cursors: Some(&truncation_cursors),
                tree_scope: Some(tree_scope),
            };
            let plan =
                policy.pick_with_context(&tree_version.levels, compaction_config, policy_context);
            let Some(plan) = plan else {
                continue;
            };

            // Step 5-11: Execute the compaction and publish the result.
            let job_id = Uuid::new_v4().to_string();
            debug!(
                "selected compaction plan tree={} {} job_id={}",
                tree_idx, plan, job_id
            );

            match self.execute_and_publish(
                tree_idx,
                tree_scope,
                &tree_version.levels,
                &plan,
                &compaction_config,
                &truncation_cursors,
                &schema_manager,
                latest_id,
                &job_id,
            ) {
                Ok(()) => {
                    // Step 12: Wait for the writer to delete the result.
                    self.wait_for_result_consumed(&job_id)?;
                    return Ok(()); // Re-read latest snapshot on next iteration.
                }
                Err(err) => {
                    warn!(
                        "compaction job {} failed: {}; will retry on next iteration",
                        job_id, err
                    );
                    return Ok(());
                }
            }
        }

        // No plan found for any tree; sleep before retrying.
        std::thread::sleep(self.poll_interval);
        Ok(())
    }

    /// Executes a compaction plan and publishes the result.
    #[allow(clippy::too_many_arguments)]
    fn execute_and_publish(
        &self,
        tree_idx: usize,
        tree_scope: &LSMTreeScope,
        levels: &[Level],
        plan: &crate::compaction::policy::CompactionPlan,
        compaction_config: &CompactionConfig,
        truncation_cursors: &crate::db_state::TruncationCursorMap,
        schema_manager: &Arc<SchemaManager>,
        base_snapshot_id: u64,
        job_id: &str,
    ) -> Result<()> {
        // Write a lease file so the writer's orphan sweep doesn't delete our outputs
        // while we're still working. We also start a heartbeat thread to refresh the lease
        // periodically, in case the compaction takes longer than min_age_ms.
        write_job_lease(&self.file_manager, job_id)?;
        let heartbeat_handle = self.start_lease_heartbeat(job_id);

        // Handle trivial move and drop without executing a full compaction task.
        let result = if plan.drop_truncated {
            self.publish_drop_result(
                tree_idx,
                tree_scope,
                levels,
                plan,
                truncation_cursors,
                base_snapshot_id,
                job_id,
            )
        } else if plan.trivial_move {
            self.publish_trivial_move_result(
                tree_idx,
                tree_scope,
                levels,
                plan,
                base_snapshot_id,
                job_id,
            )
        } else {
            self.execute_rewrite_and_publish(
                tree_idx,
                tree_scope,
                levels,
                plan,
                compaction_config,
                truncation_cursors,
                schema_manager,
                base_snapshot_id,
                job_id,
            )
        };

        // Stop the heartbeat thread.
        heartbeat_handle.stop();
        heartbeat_handle.join();

        result
    }

    /// Starts a background thread that periodically refreshes the lease file for a job.
    /// Returns a handle that can be used to stop and join the thread.
    fn start_lease_heartbeat(&self, job_id: &str) -> LeaseHeartbeatHandle {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let file_manager = Arc::clone(&self.file_manager);
        let heartbeat_interval = self.heartbeat_interval;
        let job_id = job_id.to_string();
        let handle = std::thread::Builder::new()
            .name("dedicated-lease-heartbeat".to_string())
            .spawn(move || {
                while !stop_clone.load(Ordering::SeqCst) {
                    std::thread::sleep(heartbeat_interval);
                    if stop_clone.load(Ordering::SeqCst) {
                        break;
                    }
                    if let Err(err) = write_job_lease(&file_manager, &job_id) {
                        debug!("failed to refresh lease for job {}: {}", job_id, err);
                    }
                }
            })
            .expect("failed to spawn lease heartbeat thread");
        LeaseHeartbeatHandle {
            stop,
            handle: Some(handle),
        }
    }

    /// Executes a normal rewrite compaction and publishes the result.
    #[allow(clippy::too_many_arguments)]
    fn execute_rewrite_and_publish(
        &self,
        tree_idx: usize,
        tree_scope: &LSMTreeScope,
        levels: &[Level],
        plan: &crate::compaction::policy::CompactionPlan,
        compaction_config: &CompactionConfig,
        truncation_cursors: &crate::db_state::TruncationCursorMap,
        schema_manager: &Arc<SchemaManager>,
        base_snapshot_id: u64,
        job_id: &str,
    ) -> Result<()> {
        let runs = build_runs_for_plan(levels, plan, compaction_config);
        if runs.is_empty() {
            return Err(Error::InvalidState(format!(
                "compaction plan produced no runs for tree {}",
                tree_idx
            )));
        }

        // Build the writer options for the output level.
        let schema = schema_manager.latest_schema();
        let runtime_num_columns = schema
            .num_columns_in_family(tree_scope.column_family_id)
            .unwrap_or_else(|| schema.num_columns());
        let writer_options = build_writer_options(
            &self.config,
            plan.output_level,
            compaction_config.output_file_type,
            runtime_num_columns,
        )?;
        let file_builder_factory = make_data_file_builder_factory(writer_options.clone());
        let writer_options_factory = WriterOptionsFactory::from(&writer_options);
        let sst_metrics = self.metrics_manager.sst_iterator_metrics();

        // Construct the compaction task with job-namespace output paths and readonly outputs.
        let task = CompactionTask::new(
            Arc::clone(&self.compaction_metrics),
            sst_metrics,
            tree_idx,
            runs,
            plan.output_level,
            Arc::clone(&self.file_manager),
            file_builder_factory,
            compaction_config.output_file_type,
            Arc::new(crate::ttl::TTLProvider::disabled()),
            Arc::clone(schema_manager),
        )
        .with_writer_options_factory(writer_options_factory)
        .with_column_family(tree_scope.column_family_id, runtime_num_columns)
        .with_truncation_cursors(truncation_cursors.clone())
        .with_output_path_prefix(dedicated_compaction_job_output_prefix(job_id))
        .with_readonly_outputs();

        // Execute the compaction synchronously.
        let result = self.executor.execute_blocking(task, None)?;

        // Build the operation and result.
        let inputs = Self::collect_inputs_from_plan(levels, plan, &self.file_manager)?;
        let outputs: Vec<DedicatedDataFile> = result
            .new_files()
            .iter()
            .map(|f| DedicatedDataFile::from_data_file(f, &self.file_manager))
            .collect::<Result<_>>()?;

        let operation = DedicatedCompactionOperation::Rewrite {
            inputs,
            output_level: plan.output_level,
            outputs,
        };

        let vlog_entry_deltas = result
            .vlog_edit()
            .map(|edit| edit.entry_deltas().to_vec())
            .unwrap_or_default();

        let result_struct = DedicatedCompactionResult {
            version: DEDICATED_COMPACTION_RESULT_VERSION,
            job_id: job_id.to_string(),
            base_snapshot_id,
            lsm_tree_idx: tree_idx,
            tree_scope: tree_scope.clone(),
            operation,
            vlog_entry_deltas,
            created_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        };

        publish_dedicated_compaction_result(&self.file_manager, &result_struct)?;
        info!(
            "published dedicated compaction result job={} tree={} output_level={}",
            job_id, tree_idx, plan.output_level
        );
        Ok(())
    }

    /// Publishes a trivial move result (no new physical files).
    fn publish_trivial_move_result(
        &self,
        tree_idx: usize,
        tree_scope: &LSMTreeScope,
        levels: &[Level],
        plan: &crate::compaction::policy::CompactionPlan,
        base_snapshot_id: u64,
        job_id: &str,
    ) -> Result<()> {
        let input_file = Self::find_plan_file(levels, plan)?;
        let input = DedicatedCompactionInput {
            level: plan.input_level,
            file: DedicatedDataFile::from_data_file(&input_file, &self.file_manager)?,
        };
        let operation = DedicatedCompactionOperation::TrivialMove {
            input,
            output_level: plan.output_level,
        };
        let result = DedicatedCompactionResult {
            version: DEDICATED_COMPACTION_RESULT_VERSION,
            job_id: job_id.to_string(),
            base_snapshot_id,
            lsm_tree_idx: tree_idx,
            tree_scope: tree_scope.clone(),
            operation,
            vlog_entry_deltas: Vec::new(),
            created_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        };
        publish_dedicated_compaction_result(&self.file_manager, &result)?;
        info!(
            "published trivial move result job={} tree={} L{}->L{}",
            job_id, tree_idx, plan.input_level, plan.output_level
        );
        Ok(())
    }

    /// Publishes a drop result (removal-only, no outputs).
    #[allow(clippy::too_many_arguments)]
    fn publish_drop_result(
        &self,
        tree_idx: usize,
        tree_scope: &LSMTreeScope,
        levels: &[Level],
        plan: &crate::compaction::policy::CompactionPlan,
        truncation_cursors: &crate::db_state::TruncationCursorMap,
        base_snapshot_id: u64,
        job_id: &str,
    ) -> Result<()> {
        let input_file = Self::find_plan_file(levels, plan)?;
        // Verify the file is fully covered by a truncation cursor.
        let policy_context = CompactionPolicyContext {
            truncation_cursors: Some(truncation_cursors),
            tree_scope: Some(tree_scope),
        };
        if !file_fully_covered_by_truncation_cursor(&input_file, policy_context) {
            return Err(Error::InvalidState(format!(
                "drop plan file {} is not fully covered by truncation cursor",
                input_file.file_id
            )));
        }
        let input = DedicatedCompactionInput {
            level: plan.input_level,
            file: DedicatedDataFile::from_data_file(&input_file, &self.file_manager)?,
        };
        let operation = DedicatedCompactionOperation::Drop {
            inputs: vec![input],
        };
        let result = DedicatedCompactionResult {
            version: DEDICATED_COMPACTION_RESULT_VERSION,
            job_id: job_id.to_string(),
            base_snapshot_id,
            lsm_tree_idx: tree_idx,
            tree_scope: tree_scope.clone(),
            operation,
            vlog_entry_deltas: Vec::new(),
            created_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        };
        publish_dedicated_compaction_result(&self.file_manager, &result)?;
        info!(
            "published drop result job={} tree={} L{} file_id={}",
            job_id, tree_idx, plan.input_level, input_file.file_id
        );
        Ok(())
    }

    /// Waits for the writer to consume (delete) the result file.
    fn wait_for_result_consumed(&self, job_id: &str) -> Result<()> {
        while !self.stop.load(Ordering::SeqCst) {
            if !crate::compaction::dedicated::dedicated_compaction_result_exists(
                &self.file_manager,
                job_id,
            )? {
                debug!("result job={} consumed by writer", job_id);
                return Ok(());
            }
            std::thread::sleep(self.poll_interval);
        }
        Ok(())
    }

    /// Finds the base file for a plan in the tree levels.
    fn find_plan_file(
        levels: &[Level],
        plan: &crate::compaction::policy::CompactionPlan,
    ) -> Result<Arc<crate::data_file::DataFile>> {
        levels
            .iter()
            .find(|l| l.ordinal == plan.input_level)
            .and_then(|l| {
                l.files
                    .iter()
                    .find(|f| f.file_id == plan.base_file_id)
                    .cloned()
            })
            .ok_or_else(|| {
                Error::InvalidState(format!(
                    "plan base file {} not found in level {}",
                    plan.base_file_id, plan.input_level
                ))
            })
    }

    /// Collects input descriptors from a plan by finding the input files in the tree levels.
    fn collect_inputs_from_plan(
        levels: &[Level],
        plan: &crate::compaction::policy::CompactionPlan,
        file_manager: &Arc<FileManager>,
    ) -> Result<Vec<DedicatedCompactionInput>> {
        let mut inputs = Vec::new();
        let Some(input_level) = levels.iter().find(|l| l.ordinal == plan.input_level) else {
            return Ok(inputs);
        };
        if input_level.tiered {
            // For tiered levels, all files with file_id >= base_file_id are inputs.
            for file in &input_level.files {
                if file.file_id >= plan.base_file_id {
                    inputs.push(DedicatedCompactionInput {
                        level: plan.input_level,
                        file: DedicatedDataFile::from_data_file(file, file_manager)?,
                    });
                }
            }
        } else {
            // For non-tiered levels, only the base file (smallest file_id >= base_file_id) is the
            // input, matching `build_runs_for_plan`'s selection logic.
            let base_file = input_level
                .files
                .iter()
                .filter(|f| f.file_id >= plan.base_file_id)
                .min_by_key(|f| f.file_id);
            if let Some(file) = base_file {
                inputs.push(DedicatedCompactionInput {
                    level: plan.input_level,
                    file: DedicatedDataFile::from_data_file(file, file_manager)?,
                });
            }
        }
        Ok(inputs)
    }

    fn make_policy(kind: crate::config::CompactionPolicyKind) -> Box<dyn CompactionPolicy> {
        match kind {
            crate::config::CompactionPolicyKind::RoundRobin => Box::new(RoundRobinPolicy::new()),
            crate::config::CompactionPolicyKind::MinOverlap => Box::new(MinOverlapPolicy::new()),
            crate::config::CompactionPolicyKind::ScorePriority => {
                Box::new(ScorePriorityPolicy::new())
            }
        }
    }
}

/// Handle for the lease heartbeat thread. Stops and joins the thread when dropped.
struct LeaseHeartbeatHandle {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl LeaseHeartbeatHandle {
    fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    fn join(mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
