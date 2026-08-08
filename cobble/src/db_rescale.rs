//! Functionality for expanding owned buckets by importing snapshot data from another db.
use super::{Db, ExpandStorageMode};
use crate::data_file::intersect_bucket_ranges;
use crate::db_state::{
    DbState, LSMTreeScope, MultiLSMTreeVersion, TruncationCursorMap, bucket_range_fits_total,
};
use crate::error::{Error, Result};
use crate::file::logical_file::ReplicaOrigin;
use crate::file::{File, FileManager, MetadataReader, SequentialWriteFile};
use crate::lsm::LSMTree;
use crate::metrics_manager::MetricsManager;
use crate::paths::schema_file_relative_path;
use crate::rescale_protocol::{
    ExportLease, ImportRecord, export_lease_name, has_import_records, import_record_name,
    load_import_records, write_export_lease, write_import_record,
};
use crate::snapshot::{
    build_tree_scopes_from_manifest, build_tree_versions_from_manifest,
    build_truncation_cursors_from_manifest, build_vlog_version_from_manifest,
    list_snapshot_manifest_ids, load_manifest_entry,
};
use crate::util::{
    normalize_bucket_ranges, range_is_covered_by_ranges, ranges_overlap, subtract_range_by_cuts,
    subtract_ranges,
};
use std::collections::{BTreeSet, HashMap};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

const RESCALE_PUBLICATION_OWNER_PREFIX: &str = "rescale";

struct RuntimePublicationSuspension {
    publisher: Option<Arc<crate::runtime_manifest::publisher::RuntimeManifestPublisherHandle>>,
    owner: String,
    resume_on_drop: bool,
}

impl RuntimePublicationSuspension {
    fn acquire(db: &Db, owner: String) -> Result<Self> {
        let publisher = db.runtime_manifest_publisher.clone();
        if let Some(publisher) = &publisher {
            publisher.suspend_for_owner(&owner)?;
        }
        Ok(Self {
            publisher,
            owner,
            resume_on_drop: true,
        })
    }

    fn publish_at_least_and_resume(&mut self, seq_id: u64) -> Result<()> {
        self.resume_on_drop = false;
        if let Some(publisher) = &self.publisher {
            publisher.publish_at_least_and_resume(&self.owner, seq_id)?;
        }
        Ok(())
    }
}

impl Drop for RuntimePublicationSuspension {
    fn drop(&mut self) {
        if self.resume_on_drop
            && let Some(publisher) = &self.publisher
        {
            let _ = publisher.resume_without_publish(&self.owner);
        }
    }
}

fn copy_db_state(state: &DbState) -> DbState {
    DbState {
        seq_id: state.seq_id,
        topology_epoch: state.topology_epoch,
        bucket_ranges: state.bucket_ranges.clone(),
        multi_lsm_version: state.multi_lsm_version.clone(),
        vlog_version: state.vlog_version.clone(),
        active: state.active.clone(),
        immutables: state.immutables.clone(),
        truncation_cursors: crate::db_state::new_truncation_cursors_with(
            state.truncation_cursors_snapshot(),
        ),
        suggested_base_snapshot_id: state.suggested_base_snapshot_id,
    }
}

fn shared_adoption_barrier_id(records: &[ImportRecord]) -> Option<u64> {
    let snapshot_id = records
        .first()
        .and_then(|record| record.adoption_barrier_snapshot_id)?;
    records
        .iter()
        .all(|record| record.adoption_barrier_snapshot_id == Some(snapshot_id))
        .then_some(snapshot_id)
}

pub(crate) struct AdoptionCoordinator {
    db_id: String,
    config: crate::Config,
    file_manager: Arc<FileManager>,
    lsm_tree: Arc<crate::lsm::LSMTree>,
    db_state: Arc<crate::db_state::DbStateHandle>,
    db_lifecycle: Arc<crate::db_status::DbLifecycle>,
    memtable_manager: Arc<crate::memtable::MemtableManager>,
    snapshot_manager: crate::snapshot::SnapshotManager,
    runtime_manifest_publisher:
        Option<Arc<crate::runtime_manifest::publisher::RuntimeManifestPublisherHandle>>,
    finalizing: AtomicBool,
}

impl AdoptionCoordinator {
    pub(crate) fn new(
        target: (String, crate::Config),
        storage: (Arc<FileManager>, Arc<crate::lsm::LSMTree>),
        db_state: Arc<crate::db_state::DbStateHandle>,
        db_lifecycle: Arc<crate::db_status::DbLifecycle>,
        memtable_manager: Arc<crate::memtable::MemtableManager>,
        snapshot_manager: crate::snapshot::SnapshotManager,
        runtime_manifest_publisher: Option<
            Arc<crate::runtime_manifest::publisher::RuntimeManifestPublisherHandle>,
        >,
    ) -> Self {
        let (db_id, config) = target;
        let (file_manager, lsm_tree) = storage;
        Self {
            db_id,
            config,
            file_manager,
            lsm_tree,
            db_state,
            db_lifecycle,
            memtable_manager,
            snapshot_manager,
            runtime_manifest_publisher,
            finalizing: AtomicBool::new(false),
        }
    }

    fn current_leased_file_ids(&self) -> Vec<u64> {
        let state = self.db_state.load();
        state
            .multi_lsm_version
            .tree_versions_cloned()
            .iter()
            .flat_map(|tree| tree.levels.iter())
            .flat_map(|level| level.files.iter())
            .map(|file| file.file_id)
            .chain(
                state
                    .vlog_version
                    .tracked_files()
                    .into_iter()
                    .map(|file| file.file_id()),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|file_id| {
                self.file_manager
                    .preferred_replica_origin(*file_id)
                    .is_some_and(|origin| matches!(origin, ReplicaOrigin::ExternalLeased { .. }))
            })
            .collect()
    }

    pub(crate) fn tick(self: &Arc<Self>) -> Result<()> {
        let records = load_import_records(&self.file_manager)?;
        if records
            .iter()
            .any(|record| record.target_db_id != self.db_id)
        {
            return Err(Error::InvalidState(
                "import record target_db_id does not match this database".to_string(),
            ));
        }
        if records.is_empty() {
            return Ok(());
        }
        self.file_manager
            .schedule_referenced_adoptions(&self.db_state)?;
        if !self.current_leased_file_ids().is_empty()
            || self
                .file_manager
                .has_referenced_adoption_jobs(&self.db_state)
            || self.finalizing.swap(true, Ordering::AcqRel)
        {
            return Ok(());
        }
        let committed_barrier = match self.adoption_barrier_is_committed(&records) {
            Ok(committed) => committed,
            Err(err) => {
                self.finalizing.store(false, Ordering::Release);
                return Err(err);
            }
        };
        if let Some(barrier_id) = committed_barrier {
            let result = self.finish_adoption(barrier_id);
            self.finalizing.store(false, Ordering::Release);
            return result;
        }
        let _access = match self.db_lifecycle.begin_owned_access() {
            Ok(access) => access,
            Err(err) => {
                self.finalizing.store(false, Ordering::Release);
                return Err(err);
            }
        };
        if let Some(base) = self.db_state.load().suggested_base_snapshot_id {
            self.db_state.rebase_suggested_snapshot(base, None);
        }
        let coordinator = Arc::clone(self);
        let callback = Arc::new(
            move |result: Result<crate::snapshot::SnapshotManifestInfo>| {
                if let Ok(info) = result
                    && let Err(err) = coordinator.finish_adoption(info.id)
                {
                    log::warn!("expand adoption finalization failed: {err}");
                }
                coordinator.finalizing.store(false, Ordering::Release);
            },
        );
        if let Err(err) = self.memtable_manager.create_snapshot_with_before_flush(
            self.snapshot_manager.clone(),
            Some(callback),
            |snapshot_id| self.write_adoption_barrier(snapshot_id),
        ) {
            self.finalizing.store(false, Ordering::Release);
            return Err(err);
        }
        Ok(())
    }

    /// A barrier is committed exactly when its manifest exists. A missing manifest represents a
    /// failed or interrupted pre-commit attempt and may be replaced by a new barrier.
    fn adoption_barrier_is_committed(&self, records: &[ImportRecord]) -> Result<Option<u64>> {
        let Some(snapshot_id) = shared_adoption_barrier_id(records) else {
            return Ok(None);
        };
        Ok(list_snapshot_manifest_ids(&self.file_manager)?
            .contains(&snapshot_id)
            .then_some(snapshot_id))
    }

    fn write_adoption_barrier(&self, snapshot_id: u64) -> Result<()> {
        for mut record in load_import_records(&self.file_manager)? {
            record.adoption_barrier_snapshot_id = Some(snapshot_id);
            write_import_record(&self.file_manager, &record)?;
        }
        Ok(())
    }

    fn finish_adoption(&self, barrier_id: u64) -> Result<()> {
        if let Some(publisher) = &self.runtime_manifest_publisher {
            publisher.publish_at_least(self.db_state.load().seq_id)?;
        }
        if self.lsm_tree.has_pending_compactions()
            || crate::compaction::dedicated::has_active_dedicated_compaction(&self.file_manager)?
        {
            return Ok(());
        }
        for record in load_import_records(&self.file_manager)? {
            if record.adoption_barrier_snapshot_id != Some(barrier_id) {
                continue;
            }
            if let Some(snapshot_id) = record.import_snapshot_id
                && !self.snapshot_manager.is_retained(snapshot_id)
            {
                let _ = self.snapshot_manager.expire_snapshot(snapshot_id)?;
            }
            if self
                .snapshot_manager
                .has_live_external_lease(&record.export_id)
            {
                continue;
            }
            let source = FileManager::from_config(
                &self.config,
                &record.source_db_id,
                Arc::new(MetricsManager::new(format!(
                    "{}-adoption-source",
                    record.source_db_id
                ))),
            )?;
            source.remove_metadata_file(&export_lease_name(&record.export_id))?;
            self.file_manager
                .remove_metadata_file(&import_record_name(&record.export_id))?;
        }
        Ok(())
    }

    pub(crate) fn is_complete(&self) -> Result<bool> {
        Ok(!has_import_records(&self.file_manager)?)
    }
}

fn filter_truncation_cursors(
    cursors: &TruncationCursorMap,
    ranges: &[RangeInclusive<u16>],
) -> TruncationCursorMap {
    cursors
        .iter()
        .filter(|(id, _)| ranges.iter().any(|range| range.contains(&id.bucket)))
        .map(|(id, key)| (id.clone(), key.clone()))
        .collect()
}

impl Db {
    /// Wait until all current leased expand files are copied into this DB's owned storage.
    pub fn wait_for_expand_adoption(&self, timeout: std::time::Duration) -> Result<()> {
        let deadline = std::time::Instant::now() + timeout;
        while !self.adoption_coordinator.is_complete()? {
            if std::time::Instant::now() >= deadline {
                return Err(Error::IoError(
                    "Timed out waiting for expand adoption".to_string(),
                ));
            }
            if let Some(worker) = &self.primary_tiering_worker {
                worker.wake();
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        Ok(())
    }

    fn ensure_dedicated_rescale_can_start(&self) -> Result<()> {
        if self.config.compaction_mode != crate::config::CompactionMode::Dedicated {
            return Ok(());
        }
        if let Some(job_id) = self
            .runtime_manifest_publisher
            .as_ref()
            .and_then(|publisher| publisher.suspension_owner())
        {
            return Err(Error::InvalidState(format!(
                "cannot change LSM topology while runtime publication is suspended for {job_id}"
            )));
        }
        if crate::compaction::dedicated::has_active_dedicated_compaction(&self.file_manager)? {
            return Err(Error::InvalidState(
                "cannot change LSM topology while dedicated compaction is active".to_string(),
            ));
        }
        Ok(())
    }

    /// Expands owned bucket ranges by importing LSM tree and VLOG state from a snapshot of another db, while
    /// keeping source files read-only and remapping file IDs to avoid collisions in local tracking. The source
    /// snapshot must have bucket ranges that are fully covered by the requested expand ranges, and the target db
    /// must not have any existing owned ranges that overlap with the requested expand ranges. The source snapshot's
    /// active memtable segments will be replayed into L0 of the target after metadata-level merging,
    /// with a file-level VLOG seq offset to avoid conflicts with existing VLOG files.
    /// The source manifest is resolved before the exclusive cutover. During the cutover normal
    /// reads and writes are paused so no observer can see a partial topology.
    pub fn expand_bucket(
        &self,
        source_db_id: impl Into<String>,
        snapshot_id: Option<u64>,
        ranges: Option<Vec<RangeInclusive<u16>>>,
    ) -> Result<u64> {
        self.expand_bucket_with_storage_mode(
            source_db_id,
            snapshot_id,
            ranges,
            ExpandStorageMode::AdoptAsync,
        )
    }

    /// Expands owned buckets using the selected imported-file storage policy.
    pub fn expand_bucket_with_storage_mode(
        &self,
        source_db_id: impl Into<String>,
        snapshot_id: Option<u64>,
        ranges: Option<Vec<RangeInclusive<u16>>>,
        storage_mode: ExpandStorageMode,
    ) -> Result<u64> {
        let source_db_id = source_db_id.into();
        if source_db_id == self.id {
            return Err(Error::ConfigError(
                "cannot expand bucket from the same db".to_string(),
            ));
        }
        // Step 1: Build a read-only file manager for the source db and resolve snapshot.
        let source_metrics = Arc::new(MetricsManager::new(format!(
            "{}-expand-source",
            source_db_id
        )));
        let source_file_manager = Arc::new(FileManager::from_config(
            &self.config,
            &source_db_id,
            source_metrics,
        )?);
        let source_snapshot_id = match snapshot_id {
            Some(snapshot_id) => snapshot_id,
            None => {
                let snapshot_ids = list_snapshot_manifest_ids(&source_file_manager)?;
                snapshot_ids.last().copied().ok_or_else(|| {
                    Error::IoError(format!(
                        "No snapshot manifests found for db {}",
                        source_db_id
                    ))
                })?
            }
        };
        let source_entry =
            load_manifest_entry(&source_file_manager, source_snapshot_id, &HashMap::new())?;
        let mut source_manifest = source_entry.manifest;
        if source_manifest.bucket_ranges.is_empty() {
            return Err(Error::InvalidState(format!(
                "Snapshot {} manifest missing bucket_ranges",
                source_snapshot_id
            )));
        }
        let expand_ranges = ranges.unwrap_or_else(|| source_manifest.bucket_ranges.clone());
        if expand_ranges.is_empty() {
            return Err(Error::ConfigError(
                "expand ranges must not be empty".to_string(),
            ));
        }
        for range in &expand_ranges {
            if !bucket_range_fits_total(range, self.config.total_buckets) {
                return Err(Error::ConfigError(format!(
                    "Invalid expand range {}..={} for total_buckets {}",
                    range.start(),
                    range.end(),
                    self.config.total_buckets
                )));
            }
            if !range_is_covered_by_ranges(range, &source_manifest.bucket_ranges) {
                return Err(Error::ConfigError(format!(
                    "Expand range {}..={} is outside source snapshot bucket ranges",
                    range.start(),
                    range.end()
                )));
            }
        }
        let source_tree_ranges = if source_manifest.lsm_tree_bucket_ranges.is_empty() {
            source_manifest.bucket_ranges.clone()
        } else {
            source_manifest.lsm_tree_bucket_ranges.clone()
        };
        for range in &expand_ranges {
            if !range_is_covered_by_ranges(range, &source_tree_ranges) {
                return Err(Error::InvalidState(format!(
                    "Expand range {}..={} is not fully covered by source LSM trees",
                    range.start(),
                    range.end()
                )));
            }
        }

        let _access = self.db_lifecycle.begin_exclusive_access()?;
        let _topology_guard = self.lsm_topology_lock.lock().unwrap();
        self.ensure_dedicated_rescale_can_start()?;
        let mut publication = RuntimePublicationSuspension::acquire(
            self,
            format!(
                "{RESCALE_PUBLICATION_OWNER_PREFIX}:expand:{}",
                Uuid::new_v4()
            ),
        )?;
        let export_id =
            (storage_mode == ExpandStorageMode::AdoptAsync).then(|| Uuid::new_v4().to_string());
        if let Some(export_id) = &export_id {
            let lease = ExportLease {
                version: 1,
                export_id: export_id.clone(),
                source_db_id: source_db_id.clone(),
                snapshot_id: source_snapshot_id,
                target_db_id: self.id.clone(),
                ranges: expand_ranges.clone(),
            };
            write_export_lease(&source_file_manager, &lease)?;
            let record = ImportRecord {
                version: 1,
                export_id: export_id.clone(),
                source_db_id: source_db_id.clone(),
                snapshot_id: source_snapshot_id,
                target_db_id: self.id.clone(),
                ranges: expand_ranges.clone(),
                import_snapshot_id: None,
                adoption_barrier_snapshot_id: None,
            };
            if let Err(err) = write_import_record(&self.file_manager, &record) {
                let _ = source_file_manager.remove_metadata_file(&export_lease_name(export_id));
                return Err(err);
            }
        }
        let mut original_state = None;
        let mut original_ranges = None;
        let mut topology_installed = false;
        let mut snapshot_committed = false;
        let result = (|| {
            self.create_snapshot_and_wait("bucket expand export")?;
            // Step 2: Remap file IDs to avoid collisions in local tracking, while keeping source files read-only.
            let source_file_ids: BTreeSet<u64> = source_manifest
                .tree_levels
                .iter()
                .flat_map(|levels| levels.iter())
                .flat_map(|level| level.files.iter().map(|file| file.file_id))
                .chain(source_manifest.vlog_files.iter().map(|file| file.file_id))
                .collect();
            let remapped_ids = self
                .file_manager
                .reserve_data_file_ids(source_file_ids.len());
            let file_id_map: HashMap<u64, u64> =
                source_file_ids.iter().copied().zip(remapped_ids).collect();
            let imported_file_ids = file_id_map.values().copied().collect::<Vec<_>>();
            let source_origin = match &export_id {
                Some(export_id) => ReplicaOrigin::ExternalLeased {
                    export_id: export_id.clone(),
                },
                None => ReplicaOrigin::ExternalPersistent {
                    source_id: format!("snapshot:{source_db_id}:{source_snapshot_id}"),
                },
            };
            for levels in &mut source_manifest.tree_levels {
                for level in levels {
                    for file in &mut level.files {
                        if let Some(mapped) = file_id_map.get(&file.file_id) {
                            file.file_id = *mapped;
                        }
                        file.origin = source_origin.clone();
                    }
                }
            }
            for file in &mut source_manifest.vlog_files {
                if let Some(mapped) = file_id_map.get(&file.file_id) {
                    file.file_id = *mapped;
                }
                file.origin = source_origin.clone();
            }

            // Step 3: Ensure target has all schema files required by source snapshot.
            let current_schema = self.schema_manager.latest_schema();
            if source_manifest.latest_schema_id > current_schema.version() {
                return Err(Error::InvalidState(format!(
                    "Source snapshot schema {} is newer than current schema {}",
                    source_manifest.latest_schema_id,
                    current_schema.version()
                )));
            }
            let mut required_schema_ids = BTreeSet::from([source_manifest.latest_schema_id]);
            for levels in &source_manifest.tree_levels {
                for level in levels {
                    for file in &level.files {
                        if file.schema_id <= source_manifest.latest_schema_id {
                            for schema_id in file.schema_id..=source_manifest.latest_schema_id {
                                required_schema_ids.insert(schema_id);
                            }
                        } else {
                            required_schema_ids.insert(file.schema_id);
                        }
                    }
                }
            }
            for schema_id in required_schema_ids {
                if self.schema_manager.schema(schema_id).is_ok() {
                    continue;
                }
                let schema_path = schema_file_relative_path(schema_id);
                let reader =
                    source_file_manager.open_metadata_file_reader_untracked(&schema_path)?;
                let payload = MetadataReader::new(reader).read_all()?;
                let mut writer = self.file_manager.create_metadata_file(&schema_path)?;
                writer.write(payload.as_ref())?;
                writer.close()?;
                self.schema_manager
                    .register_schema_from_file(&self.file_manager, schema_id)?;
            }

            // Step 4: Validate ownership ranges and reserve a conflict-free VLOG seq window.
            let guard = self.db_state.lock();
            let current = self.db_state.load();
            original_ranges = Some(current.bucket_ranges.clone());
            original_state = Some(Arc::clone(&current));
            for current_range in &current.bucket_ranges {
                if expand_ranges
                    .iter()
                    .any(|incoming| ranges_overlap(current_range, incoming))
                {
                    return Err(Error::ConfigError(format!(
                        "Expand range overlaps existing owned range {}..={}",
                        current_range.start(),
                        current_range.end()
                    )));
                }
            }
            let source_vlog_max_seq = source_manifest
                .vlog_files
                .iter()
                .map(|file| file.file_seq)
                .max();
            let vlog_file_seq_offset = if let Some(max_seq) = source_vlog_max_seq {
                let span = max_seq
                    .checked_add(1)
                    .ok_or_else(|| Error::IoError("source vlog seq span overflow".to_string()))?;
                self.vlog_store.reserve_file_seq_span(span)
            } else {
                0
            };

            // Step 5: Shift source VLOG seqs at metadata level and apply per-file seq offset on imported SSTs.
            for file in &mut source_manifest.vlog_files {
                file.file_seq =
                    file.file_seq
                        .checked_add(vlog_file_seq_offset)
                        .ok_or_else(|| {
                            Error::IoError(format!(
                                "VLOG file seq overflow: {} + {}",
                                file.file_seq, vlog_file_seq_offset
                            ))
                        })?;
            }
            for levels in &mut source_manifest.tree_levels {
                for level in levels {
                    for file in &mut level.files {
                        file.vlog_file_seq_offset = file
                            .vlog_file_seq_offset
                            .checked_add(vlog_file_seq_offset)
                            .ok_or_else(|| {
                                Error::IoError(format!(
                                    "VLOG file seq offset overflow: {} + {}",
                                    file.vlog_file_seq_offset, vlog_file_seq_offset
                                ))
                            })?;
                    }
                }
            }

            // Step 6: Build imported tree/vlog versions as read-only source files.
            let source_tree_versions =
                build_tree_versions_from_manifest(&self.file_manager, &source_manifest, true)?;
            let source_scopes = build_tree_scopes_from_manifest(&source_manifest);
            if source_tree_versions.len() != source_tree_ranges.len()
                || source_tree_versions.len() != source_scopes.len()
            {
                return Err(Error::InvalidState(format!(
                    "Source tree version count {}, range count {}, and scope count {} do not match",
                    source_tree_versions.len(),
                    source_tree_ranges.len(),
                    source_scopes.len()
                )));
            }
            let mut imported_scopes = Vec::new();
            let mut imported_versions = Vec::new();
            for expand_range in &expand_ranges {
                for ((source_version, source_range), source_scope) in source_tree_versions
                    .iter()
                    .zip(source_tree_ranges.iter())
                    .zip(source_scopes.iter())
                {
                    let Some(intersection) = intersect_bucket_ranges(expand_range, source_range)
                    else {
                        continue;
                    };
                    imported_scopes.push(LSMTreeScope::new(
                        intersection.clone(),
                        source_scope.column_family_id,
                    ));
                    imported_versions.push(LSMTree::clone_version_for_range(
                        source_version,
                        &intersection,
                    ));
                }
            }
            if imported_versions.is_empty() {
                return Err(Error::InvalidState(
                    "No source LSM trees matched requested expand ranges".to_string(),
                ));
            }
            let source_vlog =
                build_vlog_version_from_manifest(&self.file_manager, &source_manifest, true)?;

            // Step 7: Merge source tree/vlog versions into target state.
            let mut merged_scopes = current.multi_lsm_version.tree_scopes();
            let mut merged_versions = current.multi_lsm_version.tree_versions_cloned();
            merged_scopes.extend(imported_scopes);
            merged_versions.extend(imported_versions);
            let merged_multi_lsm = MultiLSMTreeVersion::from_scopes_with_tree_versions(
                current.multi_lsm_version.total_buckets(),
                &merged_scopes,
                merged_versions,
            )?;
            let mut merged_vlog_entries = current.vlog_version.files_with_entries();
            let mut existing_vlog_seqs: HashMap<u32, u64> = merged_vlog_entries
                .iter()
                .map(|(seq, tracked_id, _)| (*seq, tracked_id.file_id()))
                .collect();
            for (seq, tracked_id, valid_entries) in source_vlog.files_with_entries() {
                if let Some(existing_file_id) = existing_vlog_seqs.get(&seq) {
                    if *existing_file_id != tracked_id.file_id() {
                        return Err(Error::InvalidState(format!(
                            "VLOG file seq {} conflict while taking over shard",
                            seq
                        )));
                    }
                    continue;
                }
                existing_vlog_seqs.insert(seq, tracked_id.file_id());
                merged_vlog_entries.push((seq, tracked_id, valid_entries));
            }
            let merged_vlog =
                crate::vlog::VlogVersion::from_files_with_entries(merged_vlog_entries);
            let mut updated_ranges = current.bucket_ranges.clone();
            updated_ranges.extend(expand_ranges.clone());
            let updated_ranges = normalize_bucket_ranges(updated_ranges);
            let mut truncation_cursors = current.truncation_cursors_snapshot();
            truncation_cursors.extend(filter_truncation_cursors(
                &build_truncation_cursors_from_manifest(&source_manifest)?,
                &updated_ranges,
            ));
            self.db_state.store(DbState {
                seq_id: current.seq_id,
                topology_epoch: current.topology_epoch.saturating_add(1),
                bucket_ranges: updated_ranges.clone(),
                multi_lsm_version: merged_multi_lsm,
                vlog_version: merged_vlog,
                active: current.active.clone(),
                immutables: current.immutables.clone(),
                truncation_cursors: crate::db_state::new_truncation_cursors_with(
                    truncation_cursors,
                ),
                suggested_base_snapshot_id: None,
            });
            topology_installed = true;
            drop(guard);

            // Step 8: Replay source active memtable snapshot segments into L0.
            self.snapshot_manager.set_bucket_ranges(updated_ranges);
            self.restore_active_memtable_snapshot_to_l0_with_source(
                &source_file_manager,
                &source_manifest.active_memtable_data,
            )?;
            if let Some(export_id) = &export_id {
                self.create_snapshot_and_wait_with_before_flush("bucket expand", |snapshot_id| {
                    write_import_record(
                        &self.file_manager,
                        &ImportRecord {
                            version: 1,
                            export_id: export_id.clone(),
                            source_db_id: source_db_id.clone(),
                            snapshot_id: source_snapshot_id,
                            target_db_id: self.id.clone(),
                            ranges: expand_ranges.clone(),
                            import_snapshot_id: Some(snapshot_id),
                            adoption_barrier_snapshot_id: None,
                        },
                    )
                })?
            } else {
                self.create_snapshot_and_wait("bucket expand")?
            };
            snapshot_committed = true;
            let seq_id = self.db_state.load().seq_id;
            if let Err(err) = publication.publish_at_least_and_resume(seq_id) {
                self.db_lifecycle.mark_error(err.clone());
                return Err(err);
            }
            if storage_mode == ExpandStorageMode::ReferencePersistentWithCache {
                self.file_manager
                    .request_referenced_persistent_caches(imported_file_ids)?;
                self.file_manager
                    .schedule_referenced_persistent_caches(&self.db_state)?;
            }
            Ok(source_snapshot_id)
        })();
        if let Err(err) = result {
            if topology_installed && !snapshot_committed {
                self.db_state.store(copy_db_state(
                    original_state
                        .as_deref()
                        .expect("topology has original state"),
                ));
                self.snapshot_manager
                    .set_bucket_ranges(original_ranges.expect("topology has original ranges"));
            }
            if !snapshot_committed && let Some(export_id) = &export_id {
                let _ = self
                    .file_manager
                    .remove_metadata_file(&import_record_name(export_id));
                let _ = source_file_manager.remove_metadata_file(&export_lease_name(export_id));
            }
            return Err(err);
        }
        result
    }

    /// Shrinks owned bucket ranges by kicking out specified ranges and removing all data in those ranges,
    /// after a synchronous source snapshot. The topology cutover itself excludes normal reads and writes.
    /// The requested shrink ranges must be fully covered by current owned ranges, and the resulting owned ranges after
    /// shrink must not be empty. This operation creates a snapshot to capture the state before shrink,
    /// which will be used as the base for future expand operations on the kicked-out ranges,
    /// and returns the snapshot ID of that snapshot.
    pub fn shrink_bucket(&self, ranges: Vec<RangeInclusive<u16>>) -> Result<u64> {
        if ranges.is_empty() {
            return Err(Error::ConfigError(
                "shrink ranges must not be empty".to_string(),
            ));
        }
        let shrink_ranges = normalize_bucket_ranges(ranges);
        for range in &shrink_ranges {
            if !bucket_range_fits_total(range, self.config.total_buckets) {
                return Err(Error::ConfigError(format!(
                    "Invalid shrink range {}..={} for total_buckets {}",
                    range.start(),
                    range.end(),
                    self.config.total_buckets
                )));
            }
        }

        let precheck = self.db_state.load();
        for range in &shrink_ranges {
            if !range_is_covered_by_ranges(range, &precheck.bucket_ranges) {
                return Err(Error::ConfigError(format!(
                    "Shrink range {}..={} is outside current owned ranges",
                    range.start(),
                    range.end()
                )));
            }
        }
        if subtract_ranges(&precheck.bucket_ranges, &shrink_ranges).is_empty() {
            return Err(Error::ConfigError(
                "cannot shrink all owned bucket ranges".to_string(),
            ));
        }

        let _access = self.db_lifecycle.begin_exclusive_access()?;
        let _topology_guard = self.lsm_topology_lock.lock().unwrap();
        self.ensure_dedicated_rescale_can_start()?;
        let mut publication = RuntimePublicationSuspension::acquire(
            self,
            format!(
                "{RESCALE_PUBLICATION_OWNER_PREFIX}:shrink:{}",
                Uuid::new_v4()
            ),
        )?;
        let mut original_state = None;
        let mut original_ranges = None;
        let mut topology_installed = false;
        let mut snapshot_committed = false;
        let result = (|| {
            let snapshot_id = self.create_snapshot_and_wait("bucket shrink export")?;

            let guard = self.db_state.lock();
            let current = self.db_state.load();
            original_ranges = Some(current.bucket_ranges.clone());
            original_state = Some(Arc::clone(&current));
            for range in &shrink_ranges {
                if !range_is_covered_by_ranges(range, &current.bucket_ranges) {
                    return Err(Error::ConfigError(format!(
                        "Shrink range {}..={} is outside current owned ranges",
                        range.start(),
                        range.end()
                    )));
                }
            }
            let updated_ranges = subtract_ranges(&current.bucket_ranges, &shrink_ranges);
            if updated_ranges.is_empty() {
                return Err(Error::ConfigError(
                    "cannot shrink all owned bucket ranges".to_string(),
                ));
            }
            let existing_scopes = current.multi_lsm_version.tree_scopes();
            let tree_versions = current.multi_lsm_version.tree_versions_cloned();
            if existing_scopes.len() != tree_versions.len() {
                return Err(Error::InvalidState(format!(
                    "LSM tree version count {} does not match scope count {}",
                    tree_versions.len(),
                    existing_scopes.len()
                )));
            }
            let mut updated_scopes = Vec::new();
            let mut updated_tree_versions = Vec::new();
            for (scope, tree_version) in existing_scopes.into_iter().zip(tree_versions) {
                for kept_range in subtract_range_by_cuts(&scope.bucket_range, &shrink_ranges) {
                    let kept_scope = crate::db_state::LSMTreeScope::new(
                        kept_range.clone(),
                        scope.column_family_id,
                    );
                    updated_scopes.push(kept_scope);
                    if kept_range == scope.bucket_range {
                        updated_tree_versions.push(tree_version.clone());
                    } else {
                        updated_tree_versions.push(LSMTree::clone_version_for_range(
                            tree_version.as_ref(),
                            &kept_range,
                        ));
                    }
                }
            }
            if updated_scopes.is_empty() {
                return Err(Error::ConfigError(
                    "cannot shrink all LSM tree ranges".to_string(),
                ));
            }
            let updated_multi_lsm = MultiLSMTreeVersion::from_scopes_with_tree_versions(
                current.multi_lsm_version.total_buckets(),
                &updated_scopes,
                updated_tree_versions,
            )?;
            let truncation_cursors =
                filter_truncation_cursors(&current.truncation_cursors_snapshot(), &updated_ranges);
            self.db_state.store(DbState {
                seq_id: current.seq_id,
                topology_epoch: current.topology_epoch.saturating_add(1),
                bucket_ranges: updated_ranges.clone(),
                multi_lsm_version: updated_multi_lsm,
                vlog_version: current.vlog_version.clone(),
                active: current.active.clone(),
                immutables: current.immutables.clone(),
                truncation_cursors: crate::db_state::new_truncation_cursors_with(
                    truncation_cursors,
                ),
                suggested_base_snapshot_id: None,
            });
            topology_installed = true;
            drop(guard);
            self.snapshot_manager.set_bucket_ranges(updated_ranges);
            self.create_snapshot_and_wait("bucket shrink")?;
            snapshot_committed = true;
            let seq_id = self.db_state.load().seq_id;
            if let Err(err) = publication.publish_at_least_and_resume(seq_id) {
                self.db_lifecycle.mark_error(err.clone());
                return Err(err);
            }
            Ok(snapshot_id)
        })();
        if let Err(err) = result {
            if topology_installed && !snapshot_committed {
                self.db_state.store(copy_db_state(
                    original_state
                        .as_deref()
                        .expect("topology has original state"),
                ));
                self.snapshot_manager
                    .set_bucket_ranges(original_ranges.expect("topology has original ranges"));
            }
            return Err(err);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_state::full_bucket_range;
    use crate::file::FileManager;
    use crate::metrics_manager::MetricsManager;
    use crate::{Config, DbBuilder, RuntimeManifestMode, VolumeDescriptor};
    use serial_test::serial;
    use size::Size;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    fn cleanup_test_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn adoption_barrier_requires_every_live_import() {
        let record = |barrier| ImportRecord {
            version: 1,
            export_id: "export".to_string(),
            source_db_id: "source".to_string(),
            snapshot_id: 1,
            target_db_id: "target".to_string(),
            ranges: vec![0..=0],
            import_snapshot_id: Some(2),
            adoption_barrier_snapshot_id: barrier,
        };
        assert_eq!(
            shared_adoption_barrier_id(&[record(Some(3)), record(Some(3))]),
            Some(3)
        );
        assert_eq!(
            shared_adoption_barrier_id(&[record(Some(3)), record(None)]),
            None
        );
    }

    #[test]
    #[serial(file)]
    fn test_expand_bucket_from_latest_snapshot() {
        let root = "/tmp/db_expand_bucket";
        cleanup_test_root(root);
        let mut config = Config {
            memtable_capacity: Size::from_const(128),
            memtable_buffer_count: 2,
            num_columns: 1,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        config.total_buckets = 8;
        config.file_transfer_concurrency = 1;
        let source = Db::open(config.clone(), vec![2u16..=3u16]).unwrap();
        source.put(2, b"k1", 0, b"v1").unwrap();
        let (tx, rx) = mpsc::channel();
        let source_snapshot = source
            .snapshot_with_callback(move |result| {
                let _ = tx.send(result);
            })
            .unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(10))
                .unwrap()
                .unwrap()
                .snapshot_id,
            source_snapshot
        );
        source.put(2, b"k1-second", 0, b"v1-second").unwrap();
        let (tx, rx) = mpsc::channel();
        let source_snapshot = source
            .snapshot_with_callback(move |result| {
                let _ = tx.send(result);
            })
            .unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(10))
                .unwrap()
                .unwrap()
                .snapshot_id,
            source_snapshot
        );

        let mut target_config = config.clone();
        target_config.runtime_manifest_mode = RuntimeManifestMode::Enabled;
        let target = Db::open(target_config, vec![0u16..=1u16]).unwrap();
        let imported_snapshot = target
            .expand_bucket(source.id().to_string(), Some(source_snapshot), None)
            .unwrap();
        assert_eq!(imported_snapshot, source_snapshot);
        assert!(
            !source.expire_snapshot(source_snapshot).unwrap(),
            "the export lease must keep the source snapshot alive until adoption finishes"
        );

        let value = target.get(2, b"k1").unwrap().unwrap();
        assert_eq!(value[0].as_deref(), Some(&b"v1"[..]));

        target.put(3, b"k2", 0, b"v2").unwrap();
        let value = target.get(3, b"k2").unwrap().unwrap();
        assert_eq!(value[0].as_deref(), Some(&b"v2"[..]));

        let target_id = target.id().to_string();
        let target_metrics = Arc::new(MetricsManager::new("expand-target-manifest"));
        let target_file_manager =
            Arc::new(FileManager::from_config(&config, &target_id, target_metrics).unwrap());
        let import_snapshot = *list_snapshot_manifest_ids(&target_file_manager)
            .unwrap()
            .last()
            .unwrap();
        let import_manifest =
            crate::snapshot::load_manifest_for_snapshot(&target_file_manager, import_snapshot)
                .unwrap();
        assert_eq!(import_manifest.bucket_ranges, vec![0u16..=3u16]);
        let imported_origins = import_manifest
            .tree_levels
            .iter()
            .flatten()
            .flat_map(|level| level.files.iter())
            .map(|file| &file.origin)
            .chain(import_manifest.vlog_files.iter().map(|file| &file.origin))
            .collect::<Vec<_>>();
        assert!(!imported_origins.is_empty());
        assert!(
            imported_origins
                .iter()
                .all(|origin| { matches!(origin, ReplicaOrigin::ExternalLeased { .. }) })
        );
        target
            .wait_for_expand_adoption(Duration::from_secs(10))
            .unwrap();
        let target_snapshot = *list_snapshot_manifest_ids(&target_file_manager)
            .unwrap()
            .last()
            .unwrap();
        let target_manifest =
            crate::snapshot::load_manifest_for_snapshot(&target_file_manager, target_snapshot)
                .unwrap();
        assert!(
            target_manifest
                .tree_levels
                .iter()
                .flatten()
                .flat_map(|level| level.files.iter())
                .all(|file| matches!(file.origin, ReplicaOrigin::Owned))
        );
        assert!(
            target_file_manager
                .list_metadata_names("imports")
                .unwrap()
                .is_empty()
        );
        let runtime_store =
            crate::runtime_manifest::RuntimeManifestStore::new(Arc::clone(&target.file_manager));
        let runtime_manifest = (0..100)
            .find_map(|_| {
                let current = runtime_store.load_current().unwrap();
                if current.as_ref().is_some_and(|manifest| {
                    manifest
                        .manifest
                        .tree_levels
                        .iter()
                        .flatten()
                        .flat_map(|level| level.files.iter())
                        .all(|file| matches!(file.origin, ReplicaOrigin::Owned))
                }) {
                    current
                } else {
                    std::thread::sleep(Duration::from_millis(10));
                    None
                }
            })
            .expect("runtime manifest must publish adopted owned paths");
        assert!(
            runtime_manifest
                .manifest
                .tree_levels
                .iter()
                .flatten()
                .flat_map(|level| level.files.iter())
                .all(|file| matches!(file.origin, ReplicaOrigin::Owned))
        );
        let persistent = Db::open(config.clone(), vec![4u16..=5u16]).unwrap();
        persistent
            .expand_bucket_with_storage_mode(
                source.id().to_string(),
                Some(source_snapshot),
                None,
                ExpandStorageMode::ReferencePersistent,
            )
            .unwrap();
        let persistent_file_manager = Arc::new(
            FileManager::from_config(
                &config,
                persistent.id(),
                Arc::new(MetricsManager::new("expand-persistent-manifest")),
            )
            .unwrap(),
        );
        let persistent_snapshot = *list_snapshot_manifest_ids(&persistent_file_manager)
            .unwrap()
            .last()
            .unwrap();
        let persistent_manifest = crate::snapshot::load_manifest_for_snapshot(
            &persistent_file_manager,
            persistent_snapshot,
        )
        .unwrap();
        assert!(
            persistent_manifest
                .tree_levels
                .iter()
                .flatten()
                .flat_map(|level| level.files.iter())
                .all(|file| matches!(file.origin, ReplicaOrigin::ExternalPersistent { .. }))
        );

        let persistent_file_ids = persistent
            .db_state
            .load()
            .multi_lsm_version
            .tree_versions_cloned()
            .into_iter()
            .flat_map(|tree| tree.levels.clone().into_iter())
            .flat_map(|level| level.files.into_iter())
            .map(|file| file.file_id)
            .collect::<Vec<_>>();
        std::thread::sleep(Duration::from_millis(1100));
        assert!(persistent_file_ids.iter().all(|file_id| {
            matches!(
                persistent.file_manager.preferred_replica_origin(*file_id),
                Some(ReplicaOrigin::ExternalPersistent { .. })
            )
        }));

        let mut cached_config = config.clone();
        cached_config.runtime_manifest_mode = RuntimeManifestMode::Enabled;
        let cached = Db::open(cached_config, vec![6u16..=7u16]).unwrap();
        cached
            .expand_bucket_with_storage_mode(
                source.id().to_string(),
                Some(source_snapshot),
                None,
                ExpandStorageMode::ReferencePersistentWithCache,
            )
            .unwrap();
        let cached_file_ids = cached
            .db_state
            .load()
            .multi_lsm_version
            .tree_versions_cloned()
            .into_iter()
            .flat_map(|tree| tree.levels.clone().into_iter())
            .flat_map(|level| level.files.into_iter())
            .map(|file| file.file_id)
            .collect::<Vec<_>>();
        let deadline = Instant::now() + Duration::from_secs(10);
        while cached_file_ids.iter().any(|file_id| {
            !matches!(
                cached.file_manager.preferred_replica_origin(*file_id),
                Some(ReplicaOrigin::Owned)
            )
        }) && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(cached_file_ids.iter().all(|file_id| {
            matches!(
                cached.file_manager.preferred_replica_origin(*file_id),
                Some(ReplicaOrigin::Owned)
            )
        }));
        assert_eq!(
            cached.get(2, b"k1").unwrap().unwrap()[0].as_deref(),
            Some(&b"v1"[..])
        );
        assert!(cached_file_ids.iter().all(|file_id| {
            matches!(
                cached
                    .file_manager
                    .durable_data_file_path_with_origin(*file_id)
                    .map(|(_, origin)| origin),
                Some(ReplicaOrigin::ExternalPersistent { .. })
            )
        }));
        let cached_runtime_store =
            crate::runtime_manifest::RuntimeManifestStore::new(Arc::clone(&cached.file_manager));
        let cached_runtime_manifest = cached_runtime_store.load_current().unwrap().unwrap();
        assert!(
            cached_runtime_manifest
                .manifest
                .tree_levels
                .iter()
                .flatten()
                .flat_map(|level| level.files.iter())
                .all(|file| matches!(file.origin, ReplicaOrigin::ExternalPersistent { .. }))
        );

        let cached_id = cached.id().to_string();
        let (cache_tx, cache_rx) = mpsc::channel();
        let cached_snapshot = cached
            .snapshot_with_callback(move |result| {
                let _ = cache_tx.send(result);
            })
            .unwrap();
        assert_eq!(
            cache_rx
                .recv_timeout(Duration::from_secs(10))
                .unwrap()
                .unwrap()
                .snapshot_id,
            cached_snapshot
        );
        let cached_manifest_manager = Arc::new(
            FileManager::from_config(
                &config,
                &cached_id,
                Arc::new(MetricsManager::new("expand-persistent-cache-manifest")),
            )
            .unwrap(),
        );
        let cached_manifest =
            crate::snapshot::load_manifest_for_snapshot(&cached_manifest_manager, cached_snapshot)
                .unwrap();
        assert!(
            cached_manifest
                .tree_levels
                .iter()
                .flatten()
                .flat_map(|level| level.files.iter())
                .all(|file| matches!(file.origin, ReplicaOrigin::ExternalPersistent { .. }))
        );
        for file_id in &cached_file_ids {
            assert!(
                cached
                    .file_manager
                    .evict_preferred_persistent_cache(*file_id)
                    .unwrap()
            );
        }
        assert!(cached_file_ids.iter().all(|file_id| {
            matches!(
                cached.file_manager.preferred_replica_origin(*file_id),
                Some(ReplicaOrigin::ExternalPersistent { .. })
            )
        }));
        drop(cached);
        let reopened_cached =
            Db::open_from_snapshot(config.clone(), cached_snapshot, cached_id).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while cached_file_ids.iter().any(|file_id| {
            !matches!(
                reopened_cached
                    .file_manager
                    .preferred_replica_origin(*file_id),
                Some(ReplicaOrigin::Owned)
            )
        }) && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(cached_file_ids.iter().all(|file_id| {
            matches!(
                reopened_cached
                    .file_manager
                    .preferred_replica_origin(*file_id),
                Some(ReplicaOrigin::Owned)
            )
        }));
        assert_eq!(
            reopened_cached.get(2, b"k1").unwrap().unwrap()[0].as_deref(),
            Some(&b"v1"[..])
        );
        drop(reopened_cached);
        drop(persistent);
        assert!(source.get(2, b"k1").unwrap().is_some());

        assert!(source.expire_snapshot(source_snapshot).unwrap());
        drop(source);
        drop(target);
        let reopened = Db::open_from_snapshot(config.clone(), target_snapshot, target_id).unwrap();
        assert_eq!(
            reopened.get(2, b"k1").unwrap().unwrap()[0].as_deref(),
            Some(&b"v1"[..])
        );
        drop(reopened);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_expand_bucket_outside_source_rejected() {
        let root = "/tmp/db_expand_bucket_outside_source";
        cleanup_test_root(root);
        let mut config = Config {
            memtable_capacity: Size::from_const(128),
            memtable_buffer_count: 2,
            num_columns: 1,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        config.total_buckets = 8;
        let source = Db::open(config.clone(), vec![1u16..=2u16]).unwrap();
        source.put(1, b"k1", 0, b"v1").unwrap();
        let target = Db::open(config, vec![3u16..=4u16]).unwrap();
        let (tx, rx) = mpsc::channel();
        let snapshot_id = source
            .snapshot_with_callback(move |result| {
                let _ = tx.send(result);
            })
            .unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(10))
                .unwrap()
                .unwrap()
                .snapshot_id,
            snapshot_id
        );
        let err = target
            .expand_bucket(
                source.id().to_string(),
                Some(snapshot_id),
                Some(vec![0u16..=1u16]),
            )
            .unwrap_err();
        assert!(matches!(err, Error::ConfigError(_)));

        drop(target);
        drop(source);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_expand_bucket_accepts_full_range_with_empty_source() {
        let root = "/tmp/db_expand_bucket_empty_source";
        cleanup_test_root(root);
        let config = Config {
            total_buckets: 4,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let source = Db::open(config.clone(), vec![2u16..=3u16]).unwrap();
        let (tx, rx) = mpsc::channel();
        let snapshot_id = source
            .snapshot_with_callback(move |result| {
                let _ = tx.send(result);
            })
            .unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(10))
                .unwrap()
                .unwrap()
                .snapshot_id,
            snapshot_id
        );
        let target = Db::open(config, std::iter::once(full_bucket_range(2)).collect()).unwrap();
        target
            .expand_bucket(
                source.id().to_string(),
                Some(snapshot_id),
                Some(vec![2u16..=3u16]),
            )
            .unwrap();
        target.put(2, b"k", 0, b"v").unwrap();
        let got = target.get(2, b"k").unwrap().unwrap();
        assert_eq!(got[0].as_deref(), Some(&b"v"[..]));
        drop(target);
        drop(source);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_expand_bucket_restores_active_memtable_segments() {
        let root = "/tmp/db_expand_bucket_active_segments";
        cleanup_test_root(root);
        let config = Config {
            total_buckets: 8,
            memtable_capacity: Size::from_kib(8),
            memtable_buffer_count: 2,
            num_columns: 1,
            value_separation_threshold: Some(Size::from_const(1)),
            active_memtable_incremental_snapshot_ratio: 1.0,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let source = Db::open(config.clone(), vec![4u16..=5u16]).unwrap();
        source.put(4, b"k-sep", 0, b"payload-separated").unwrap();
        let (tx, rx) = mpsc::channel();
        let snapshot_id = source
            .snapshot_with_callback(move |result| {
                let _ = tx.send(result);
            })
            .unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(10))
                .unwrap()
                .unwrap()
                .snapshot_id,
            snapshot_id
        );
        let source_metrics = Arc::new(MetricsManager::new("rescale-source-manifest"));
        let source_file_manager =
            Arc::new(FileManager::from_config(&config, source.id(), source_metrics).unwrap());
        let source_manifest =
            crate::snapshot::load_manifest_for_snapshot(&source_file_manager, snapshot_id).unwrap();
        assert!(!source_manifest.active_memtable_data.is_empty());

        let target = Db::open(config, vec![0u16..=1u16]).unwrap();
        target
            .expand_bucket(source.id().to_string(), Some(snapshot_id), None)
            .unwrap();
        let got = target.get(4, b"k-sep").unwrap().unwrap();
        assert_eq!(got[0].as_deref(), Some(&b"payload-separated"[..]));

        drop(target);
        drop(source);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_shrink_bucket_removes_data_from_kicked_range() {
        let root = "/tmp/db_shrink_bucket";
        cleanup_test_root(root);
        let config = Config {
            total_buckets: 8,
            memtable_capacity: Size::from_const(128),
            memtable_buffer_count: 2,
            num_columns: 1,
            sst_bloom_filter_enabled: true,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let db = Db::open(config.clone(), vec![0u16..=3u16]).unwrap();
        db.put(1, b"k1", 0, b"v1").unwrap();
        db.put(2, b"k2", 0, b"v2").unwrap();

        let shrink_snapshot = db.shrink_bucket(vec![2u16..=3u16]).unwrap();
        let bucket_input = db.shard_snapshot_input(shrink_snapshot).unwrap();
        assert_eq!(bucket_input.ranges, vec![0u16..=1u16]);

        let kept = db.get(1, b"k1").unwrap().unwrap();
        assert_eq!(kept[0].as_deref(), Some(&b"v1"[..]));
        let removed = db.get(2, b"k2").unwrap();
        assert!(removed.is_none());

        let metrics = Arc::new(MetricsManager::new("shrink-manifest"));
        let file_manager = Arc::new(FileManager::from_config(&config, db.id(), metrics).unwrap());
        let manifest =
            crate::snapshot::load_manifest_for_snapshot(&file_manager, shrink_snapshot).unwrap();
        assert_eq!(manifest.bucket_ranges, vec![0u16..=3u16]);

        let post_shrink_snapshot = *list_snapshot_manifest_ids(&file_manager)
            .unwrap()
            .last()
            .unwrap();
        let post_shrink_manifest =
            crate::snapshot::load_manifest_for_snapshot(&file_manager, post_shrink_snapshot)
                .unwrap();
        assert_eq!(post_shrink_manifest.bucket_ranges, vec![0u16..=1u16]);
        let db_id = db.id().to_string();
        drop(db);
        let reopened = Db::open_from_snapshot(config.clone(), post_shrink_snapshot, db_id).unwrap();
        assert_eq!(
            reopened.get(1, b"k1").unwrap().unwrap()[0].as_deref(),
            Some(&b"v1"[..])
        );
        assert!(reopened.get(2, b"k2").unwrap().is_none());
        drop(reopened);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_shrink_bucket_rejects_outside_range() {
        let root = "/tmp/db_shrink_bucket_outside";
        cleanup_test_root(root);
        let config = Config {
            total_buckets: 8,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let db = Db::open(config, vec![0u16..=1u16]).unwrap();
        let err = db.shrink_bucket(vec![2u16..=2u16]).unwrap_err();
        assert!(matches!(err, Error::ConfigError(_)));
        drop(db);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn test_shrink_bucket_rejects_removing_all_ranges() {
        let root = "/tmp/db_shrink_bucket_all";
        cleanup_test_root(root);
        let config = Config {
            total_buckets: 8,
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            ..Config::default()
        };
        let db = Db::open(config, vec![0u16..=1u16]).unwrap();
        let err = db.shrink_bucket(vec![0u16..=1u16]).unwrap_err();
        assert!(matches!(err, Error::ConfigError(_)));
        drop(db);
        cleanup_test_root(root);
    }

    #[test]
    #[serial(file)]
    fn dedicated_rescale_rejects_pending_compaction_result() {
        let root = "/tmp/db_dedicated_rescale_pending_result";
        cleanup_test_root(root);
        let mut config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            compaction_mode: crate::config::CompactionMode::Dedicated,
            compaction_dedicated_poll_interval_ms: 1_000,
            ..Config::default()
        };
        config.total_buckets = 4;
        let db = DbBuilder::new(config)
            .bucket_ranges(vec![0u16..=3u16])
            .db_id("dedicated-rescale-pending")
            .open()
            .unwrap();
        if let Some(poller) = &db.dedicated_poller {
            poller.stop();
            poller.join();
        }
        let result = crate::compaction::dedicated::DedicatedCompactionResult {
            version: crate::compaction::dedicated::DEDICATED_COMPACTION_RESULT_VERSION,
            job_id: "pending-rescale".to_string(),
            source: crate::compaction::dedicated::DedicatedCompactionSource::Runtime {
                generation: 1,
                seq_id: db.db_state.load().seq_id,
            },
            topology_epoch: db.db_state.load().topology_epoch,
            lsm_tree_idx: 0,
            tree_scope: LSMTreeScope::new(0u16..=3u16, 0),
            operation: crate::compaction::dedicated::DedicatedCompactionOperation::Drop {
                inputs: Vec::new(),
            },
            vlog_entry_deltas: Vec::new(),
            created_at_ms: 0,
        };
        crate::compaction::dedicated::publish_dedicated_compaction_result(
            &db.file_manager,
            &result,
        )
        .unwrap();

        let err = db.shrink_bucket(vec![0u16..=0u16]).unwrap_err();
        assert!(err.to_string().contains("dedicated compaction is active"));

        crate::compaction::dedicated::delete_dedicated_compaction_result(
            &db.file_manager,
            &result.job_id,
        )
        .unwrap();
        db.close().unwrap();
        cleanup_test_root(root);
    }
}
