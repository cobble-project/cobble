use crate::cache::{
    BlockCache, BlockCachePreload, BlockCachePreloadWorker, ScanHotBlockRegistry,
    bucket_scoped_cache_namespace, cache_namespace_for_db_id,
};
use crate::compaction::{
    CompactionConfig, CompactionPlan, CompactionPolicy, CompactionPolicyContext, CompactionWorker,
    MinOverlapPolicy, RoundRobinPolicy, ScorePriorityPolicy,
    file_fully_covered_by_truncation_cursor, level_threshold, resolve_compaction_plan,
};
use crate::config::SstReadMetadataCacheMode;
use crate::data_file::{DataFile, DataFileType, intersect_bucket_ranges};
use crate::db_status::DbLifecycle;
use crate::error::Result;
use crate::file::{
    FileManager, RandomAccessFile, ReadAheadBufferedReader, lsm_file_priority_for_level,
    read_ahead_runtime,
};
use crate::iterator::{
    BucketFilterIterator, ColumnMaskingIterator, KvIterator, SchemaEvolvingIterator, SortedRun,
    VlogSeqOffsetIterator,
};
use crate::metrics_manager::MetricsManager;
use crate::parquet::ParquetIterator;
use crate::row_merge::SchemaValue;
use crate::schema::{DEFAULT_COLUMN_FAMILY_ID, Schema, SchemaManager};
use crate::sst::row_codec::{decode_value, decode_value_masked};
use crate::sst::{SSTIterator, SSTIteratorMetrics, SSTIteratorOptions, SSTPointReader};
use crate::r#type::{key_bucket, key_column_family};
use bytes::Bytes;
use log::{debug, error, warn};
use std::collections::{BTreeMap, HashMap};
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use crate::db_state::{DbState, DbStateHandle, LSMTreeScope, bucket_range_len};
use crate::ttl::TTLProvider;
use crate::vlog::{VlogEdit, apply_vlog_offset_to_value};

pub(crate) type DynKvIterator = Box<dyn for<'a> KvIterator<'a>>;

#[derive(Clone)]
pub(crate) struct Level {
    pub(crate) ordinal: u8,
    pub(crate) tiered: bool,
    pub(crate) files: Vec<Arc<DataFile>>,
}

pub(crate) struct LevelOptions {
    pub(crate) tiered: bool,
}

fn file_intersects_scan(file: &DataFile, encoded_start: &[u8], encoded_end: Option<&[u8]>) -> bool {
    file.end_key.as_slice() >= encoded_start
        && encoded_end.is_none_or(|end| file.start_key.as_slice() < end)
}

#[derive(Clone)]
pub(crate) struct LSMTreeVersion {
    pub(crate) levels: Vec<Level>,
}

/// Per-key mutable state for a batched point lookup. The caller initializes
/// values/masks from the memtable snapshot; the LSM path appends older values.
pub(crate) struct BatchGetRequest {
    pub(crate) bucket: u16,
    pub(crate) encoded_key: Bytes,
    pub(crate) values: Vec<SchemaValue>,
    pub(crate) terminal_mask: Option<Vec<u8>>,
    pub(crate) decode_mask: Vec<u8>,
    pub(crate) stopped: bool,
}

/// Small point batches avoid sorting and use per-key binary routing in
/// non-tiered levels. Larger batches amortize one sort with merge routing.
const SMALL_BATCH_POINT_ROUTING_REQUEST_LIMIT: usize = 8;

pub(crate) struct LSMTree {
    db_state: Arc<DbStateHandle>,
    db_lifecycle: Arc<DbLifecycle>,
    block_cache: Option<BlockCache>,
    state: Mutex<LSMTreeState>,
    ttl_provider: Arc<crate::ttl::TTLProvider>,
    sst_metrics: Arc<SSTIteratorMetrics>,
    sst_read_metadata_cache_mode: SstReadMetadataCacheMode,
    sst_pinned_metadata_max_level: Option<u8>,
    sst_pinned_metadata_partitions_enabled: bool,
    cache_namespace: u64,
    scan_hot_blocks: Arc<ScanHotBlockRegistry>,
    block_cache_preload_worker: Arc<BlockCachePreloadWorker>,
}

struct LSMTreeState {
    level_options: Vec<LevelOptions>,
    compaction_config: CompactionConfig,
    compaction_policy: Box<dyn CompactionPolicy>,
    schema_manager: Arc<SchemaManager>,
    pending_compaction: HashMap<usize, PendingCompaction>,
    compaction_worker: Option<Arc<dyn CompactionWorker>>,
}

struct PendingCompaction {
    scope: Option<LSMTreeScope>,
    topology_epoch: u64,
}

#[derive(Clone)]
pub(crate) struct LevelEdit {
    pub(crate) level: u8,
    pub(crate) removed_files: Vec<Arc<DataFile>>,
    pub(crate) new_files: Vec<Arc<DataFile>>,
}

#[derive(Clone)]
pub(crate) struct VersionEdit {
    pub(crate) level_edits: Vec<LevelEdit>,
}

struct VersionSummary<'a>(&'a LSMTreeVersion);

impl std::fmt::Display for VersionSummary<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "current version [")?;
        for (idx, level) in self.0.levels.iter().enumerate() {
            if idx > 0 {
                write!(f, "; ")?;
            }
            write!(f, "L{} files={}", level.ordinal, level.files.len())?;
            if !level.files.is_empty() {
                write!(f, " ids=")?;
                for (file_idx, file) in level.files.iter().enumerate() {
                    if file_idx > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{}", file.file_id)?;
                }
            }
        }
        write!(f, "]")
    }
}

impl Default for LSMTree {
    fn default() -> Self {
        Self::with_state(
            Arc::new(DbStateHandle::new()),
            Arc::new(MetricsManager::new("unknown")),
        )
    }
}

impl LSMTree {
    pub(crate) fn with_state(
        db_state: Arc<DbStateHandle>,
        metrics_manager: Arc<MetricsManager>,
    ) -> Self {
        Self::with_state_and_ttl(
            db_state,
            Arc::new(TTLProvider::disabled()),
            Arc::new(DbLifecycle::new_open()),
            metrics_manager,
        )
    }

    pub(crate) fn with_state_and_ttl(
        db_state: Arc<DbStateHandle>,
        ttl_provider: Arc<TTLProvider>,
        db_lifecycle: Arc<DbLifecycle>,
        metrics_manager: Arc<MetricsManager>,
    ) -> Self {
        let cache_namespace = cache_namespace_for_db_id(metrics_manager.db_id());
        let block_cache_preload_worker =
            Arc::new(BlockCachePreloadWorker::new(Arc::clone(&db_lifecycle)));
        Self {
            db_state,
            db_lifecycle,
            block_cache: None,
            state: Mutex::new(LSMTreeState {
                // at least 2 level option
                level_options: vec![
                    LevelOptions { tiered: true },
                    LevelOptions { tiered: false },
                ],
                compaction_config: CompactionConfig::default(),
                compaction_policy: Box::new(RoundRobinPolicy::new()),
                schema_manager: Arc::new(SchemaManager::new(1)),
                pending_compaction: HashMap::new(),
                compaction_worker: None,
            }),
            ttl_provider,
            sst_metrics: metrics_manager.sst_iterator_metrics(),
            sst_read_metadata_cache_mode: SstReadMetadataCacheMode::Eager,
            sst_pinned_metadata_max_level: None,
            sst_pinned_metadata_partitions_enabled: false,
            cache_namespace,
            scan_hot_blocks: Arc::new(ScanHotBlockRegistry::new()),
            block_cache_preload_worker,
        }
    }

    fn get_level_option(state: &LSMTreeState, level: u8) -> &LevelOptions {
        if let Some(opt) = state.level_options.get(level as usize) {
            opt
        } else {
            state.level_options.last().unwrap()
        }
    }

    pub(crate) fn db_state(&self) -> Arc<DbStateHandle> {
        Arc::clone(&self.db_state)
    }

    pub(crate) fn set_schema_manager(&mut self, schema_manager: Arc<SchemaManager>) {
        self.state.get_mut().unwrap().schema_manager = schema_manager;
    }

    pub(crate) fn apply_edit(
        &self,
        tree_idx: usize,
        edit: VersionEdit,
        vlog_edit: Option<VlogEdit>,
    ) {
        let mut state = self.state.lock().unwrap();
        self.apply_edit_locked(&mut state, vec![(tree_idx, edit)], move |db_state| {
            if let Some(vlog_edit) = vlog_edit {
                db_state.vlog_version = db_state.vlog_version.apply_edit(vlog_edit);
            }
        });
    }

    fn apply_edit_locked(
        &self,
        state: &mut LSMTreeState,
        edits: Vec<(usize, VersionEdit)>,
        fix: impl FnOnce(&mut DbState),
    ) -> Arc<DbState> {
        if edits.is_empty() {
            return self.db_state.load();
        }
        let guard = self.db_state.lock();
        let snapshot = self.db_state.load();
        let mut updated_versions: BTreeMap<usize, Arc<LSMTreeVersion>> = BTreeMap::new();
        let mut inherit_suggested_base_snapshot_id = true;
        for (tree_idx, edit) in &edits {
            inherit_suggested_base_snapshot_id &= edit.level_edits.is_empty()
                || (edit.level_edits.len() == 1
                    && edit.level_edits[0].level == 0
                    && edit.level_edits[0].removed_files.is_empty());
            let mut new_levels = updated_versions
                .get(tree_idx)
                .cloned()
                .unwrap_or_else(|| snapshot.multi_lsm_version.version_of_index(*tree_idx))
                .levels
                .clone();
            for level_edit in &edit.level_edits {
                let file_priority = lsm_file_priority_for_level(level_edit.level);
                for new_file in &level_edit.new_files {
                    if let Err(err) = new_file.tracked_id.set_priority(file_priority) {
                        warn!(
                            "failed to set offload priority for file {} at level {}: {}",
                            new_file.file_id, level_edit.level, err
                        );
                    }
                }
                if let Some(level) = new_levels
                    .iter_mut()
                    .find(|l| l.ordinal == level_edit.level)
                {
                    let mut insert_pos = Option::<usize>::None;
                    for file in &level_edit.removed_files {
                        if let Some(pos) = level.files.iter().position(|f| Arc::ptr_eq(f, file)) {
                            level.files.remove(pos);
                            if !level.tiered {
                                if let Some(previous) = insert_pos {
                                    assert_eq!(pos, previous);
                                } else {
                                    insert_pos = Some(pos);
                                }
                            } else if insert_pos.is_none() {
                                insert_pos = Some(pos);
                            }
                        }
                    }
                    if let Some(pos) = insert_pos {
                        for (i, new_file) in level_edit.new_files.iter().enumerate() {
                            level.files.insert(pos + i, Arc::clone(new_file));
                        }
                    } else if level.tiered {
                        level.files.extend(level_edit.new_files.clone());
                    } else {
                        let mut last_pos = 0;
                        for new_file in &level_edit.new_files {
                            let mut inserted = false;
                            for (i, existing_file) in level.files.iter().enumerate().skip(last_pos)
                            {
                                if new_file.end_key < existing_file.start_key {
                                    level.files.insert(i, Arc::clone(new_file));
                                    inserted = true;
                                    last_pos = i + 1;
                                    break;
                                }
                            }
                            if !inserted {
                                level.files.push(Arc::clone(new_file));
                                last_pos = level.files.len();
                            }
                        }
                    }
                } else {
                    new_levels.push(Level {
                        ordinal: level_edit.level,
                        tiered: Self::get_level_option(state, level_edit.level).tiered,
                        files: level_edit.new_files.clone(),
                    });
                }
            }
            updated_versions.insert(*tree_idx, Arc::new(LSMTreeVersion { levels: new_levels }));
        }

        self.db_state
            .cas_mutate(snapshot.seq_id, |db_state, snapshot| {
                let mut multi_lsm_version = snapshot.multi_lsm_version.clone();
                for (tree_idx, version) in &updated_versions {
                    multi_lsm_version =
                        multi_lsm_version.with_lsm_version_at(*tree_idx, Arc::clone(version));
                }
                let mut new_db_state = DbState {
                    seq_id: db_state.allocate_seq_id(),
                    topology_epoch: snapshot.topology_epoch,
                    bucket_ranges: snapshot.bucket_ranges.clone(),
                    multi_lsm_version,
                    vlog_version: snapshot.vlog_version.clone(),
                    active: snapshot.active.clone(),
                    immutables: snapshot.immutables.clone(),
                    truncation_cursors: snapshot.truncation_cursors.clone(),
                    suggested_base_snapshot_id: if inherit_suggested_base_snapshot_id {
                        snapshot.suggested_base_snapshot_id
                    } else {
                        None
                    },
                };
                fix(&mut new_db_state);
                Some(new_db_state)
            });
        let snapshot = self.db_state.load();
        drop(guard);
        debug!(
            "apply {} version edits. {}",
            edits.len(),
            VersionSummary(
                self.db_state
                    .load()
                    .multi_lsm_version
                    .version_of_index(0)
                    .as_ref()
            )
        );
        for tree_idx in updated_versions.keys().copied() {
            self.maybe_trigger_compaction_locked(state, tree_idx);
        }
        snapshot
    }

    pub(crate) fn add_level0_files(
        &self,
        to_remove_memtable_id: Uuid,
        files_by_scope: Vec<(LSMTreeScope, Arc<DataFile>)>,
        vlog_edit: Option<VlogEdit>,
    ) -> Result<Arc<DbState>> {
        if files_by_scope.is_empty() {
            return Err(crate::error::Error::InvalidState(
                "cannot add empty new files".to_string(),
            ));
        }
        let mut state = self.state.lock().unwrap();
        let snapshot = self.db_state.load();
        let mut grouped: BTreeMap<usize, Vec<Arc<DataFile>>> = BTreeMap::new();
        for (tree_scope, file) in files_by_scope {
            for (tree_idx, scoped_file) in
                Self::remap_flushed_level0_file(&snapshot, &tree_scope, &file)?
            {
                grouped.entry(tree_idx).or_default().push(scoped_file);
            }
        }
        let edits: Vec<(usize, VersionEdit)> = grouped
            .into_iter()
            .map(|(tree_idx, files)| {
                (
                    tree_idx,
                    VersionEdit {
                        level_edits: vec![LevelEdit {
                            level: 0,
                            removed_files: Vec::new(),
                            new_files: files,
                        }],
                    },
                )
            })
            .collect();
        Ok(self.apply_edit_locked(&mut state, edits, move |db_state| {
            db_state
                .immutables
                .retain(|imm| imm.id != to_remove_memtable_id);
            if let Some(edit) = vlog_edit {
                db_state.vlog_version = db_state.vlog_version.apply_edit(edit);
            }
        }))
    }

    fn remap_flushed_level0_file(
        snapshot: &Arc<DbState>,
        source_scope: &LSMTreeScope,
        file: &Arc<DataFile>,
    ) -> Result<Vec<(usize, Arc<DataFile>)>> {
        if let Some(tree_idx) = snapshot
            .multi_lsm_version
            .tree_index_for_exact_scope(source_scope)
        {
            return Ok(vec![(tree_idx, Arc::clone(file))]);
        }

        let remapped: Vec<(usize, Arc<DataFile>)> = snapshot
            .multi_lsm_version
            .tree_scopes()
            .into_iter()
            .enumerate()
            .filter_map(|(tree_idx, scope)| {
                if scope.column_family_id != source_scope.column_family_id {
                    return None;
                }
                let effective_bucket_range =
                    intersect_bucket_ranges(&file.bucket_range, &scope.bucket_range)?;
                let scoped_file = if effective_bucket_range == file.effective_bucket_range
                    || (effective_bucket_range == file.bucket_range
                        && file.effective_bucket_range == file.bucket_range)
                {
                    Arc::clone(file)
                } else {
                    Arc::new(
                        file.as_ref()
                            .with_effective_bucket_range(effective_bucket_range),
                    )
                };
                Some((tree_idx, scoped_file))
            })
            .collect();

        if remapped.is_empty() {
            return Err(crate::error::Error::InvalidState(format!(
                "cannot map flush output file {} from scope {}..={} cf={} to current multi-lsm scopes",
                file.file_id,
                source_scope.bucket_range.start(),
                source_scope.bucket_range.end(),
                source_scope.column_family_id
            )));
        }
        Ok(remapped)
    }

    pub(crate) fn level_files(&self, level: u8) -> Vec<Arc<DataFile>> {
        self.level_files_in_tree(0, level)
    }

    pub(crate) fn level_files_in_tree(&self, tree_idx: usize, level: u8) -> Vec<Arc<DataFile>> {
        self.db_state
            .load()
            .multi_lsm_version
            .version_of_index(tree_idx)
            .levels
            .iter()
            .find(|l| l.ordinal == level)
            .map(|l| l.files.clone())
            .unwrap_or_default()
    }

    pub(crate) fn configure_compaction(
        &self,
        config: CompactionConfig,
        worker: Option<Arc<dyn CompactionWorker>>,
    ) {
        let mut state = self.state.lock().unwrap();
        state.compaction_config = config;
        state.compaction_policy = Self::make_policy(config.policy);
        state.compaction_worker = worker;
    }

    pub(crate) fn set_block_cache(&mut self, block_cache: Option<BlockCache>) {
        self.block_cache = block_cache;
    }

    pub(crate) fn set_sst_read_metadata_cache_mode(&mut self, mode: SstReadMetadataCacheMode) {
        self.sst_read_metadata_cache_mode = mode;
    }

    pub(crate) fn set_sst_pinned_metadata_max_level(&mut self, max_level: Option<u8>) {
        self.sst_pinned_metadata_max_level = max_level;
    }

    pub(crate) fn set_sst_pinned_metadata_partitions_enabled(&mut self, enabled: bool) {
        self.sst_pinned_metadata_partitions_enabled = enabled;
    }

    pub(crate) fn sst_metrics(&self) -> Arc<SSTIteratorMetrics> {
        Arc::clone(&self.sst_metrics)
    }

    pub(crate) fn block_cache(&self) -> Option<BlockCache> {
        self.block_cache.clone()
    }

    pub(crate) fn cache_namespace(&self) -> u64 {
        self.cache_namespace
    }

    pub(crate) fn scan_hot_blocks(&self) -> Arc<ScanHotBlockRegistry> {
        Arc::clone(&self.scan_hot_blocks)
    }

    pub(crate) fn submit_block_cache_preload(
        &self,
        file_manager: Arc<FileManager>,
        preloads: Vec<BlockCachePreload>,
    ) {
        if let Some(block_cache) = self.block_cache() {
            self.block_cache_preload_worker
                .submit(file_manager, block_cache, preloads);
        }
    }

    pub(crate) fn shutdown_compaction(&self) {
        self.block_cache_preload_worker.shutdown();
        let worker = {
            let mut state = self.state.lock().unwrap();
            let worker = state.compaction_worker.take();
            state.pending_compaction.clear();
            worker
        };
        if let Some(worker) = worker {
            worker.shutdown();
        }
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

    fn split_bucket_range(range: &RangeInclusive<u16>, parts: usize) -> Vec<RangeInclusive<u16>> {
        let total = bucket_range_len(range);
        let parts = parts.max(1).min(total.max(1));
        let base = total / parts;
        let extra = total % parts;
        let mut cursor = *range.start();
        let mut ranges = Vec::with_capacity(parts);
        for idx in 0..parts {
            let len = base + usize::from(idx < extra);
            let is_last = idx + 1 == parts;
            let end = if is_last {
                *range.end()
            } else {
                cursor.saturating_add(len.saturating_sub(1) as u16)
            };
            ranges.push(cursor..=end);
            if is_last {
                break;
            }
            cursor = end.saturating_add(1);
        }
        ranges
    }

    pub(crate) fn clone_version_for_range(
        version: &LSMTreeVersion,
        range: &RangeInclusive<u16>,
    ) -> Arc<LSMTreeVersion> {
        let levels = version
            .levels
            .iter()
            .map(|level| Level {
                ordinal: level.ordinal,
                tiered: level.tiered,
                files: level
                    .files
                    .iter()
                    .filter_map(|file| {
                        let effective_range =
                            intersect_bucket_ranges(&file.effective_bucket_range, range)?;
                        if effective_range == file.effective_bucket_range
                            || (effective_range == file.bucket_range
                                && file.effective_bucket_range == file.bucket_range)
                        {
                            Some(Arc::clone(file))
                        } else {
                            let cloned = file.as_ref().with_effective_bucket_range(effective_range);
                            Some(Arc::new(cloned))
                        }
                    })
                    .collect(),
            })
            .collect();
        Arc::new(LSMTreeVersion { levels })
    }

    fn estimate_split_parts(
        level_size: usize,
        level_threshold: usize,
        bucket_count: usize,
    ) -> Option<usize> {
        if bucket_count <= 1 || level_threshold == 0 || level_size <= level_threshold {
            return None;
        }
        let by_size = level_size.div_ceil(level_threshold).max(2);
        Some(by_size.min(bucket_count))
    }

    fn maybe_split_tree_locked(
        &self,
        state: &mut LSMTreeState,
        snapshot: &Arc<DbState>,
        tree_idx: usize,
    ) -> Option<std::ops::Range<usize>> {
        let split_level = state.compaction_config.split_trigger_level?;
        if split_level == 0 {
            return None;
        }
        let tree_scope = snapshot.multi_lsm_version.tree_scope_of_tree(tree_idx)?;
        let tree_range = tree_scope.bucket_range.clone();
        let bucket_count = bucket_range_len(&tree_range);
        if bucket_count <= 1 {
            return None;
        }
        let tree_version = snapshot.multi_lsm_version.version_of_index(tree_idx);
        let split_level_view = tree_version
            .levels
            .iter()
            .find(|level| level.ordinal == split_level)?;
        let level_size = split_level_view
            .files
            .iter()
            .map(|file| file.size)
            .sum::<usize>();
        let has_out_of_range_data = split_level_view
            .files
            .iter()
            .any(|file| file.needs_bucket_filter());
        // if there are out-of-range data files, we cannot accurately estimate the level size for
        // the split-level, so we skip auto split to avoid potential mis-split.
        if has_out_of_range_data {
            debug!(
                "skip auto split tree={} level={} because of out-of-range data files",
                tree_idx, split_level
            );
            return None;
        }
        let threshold = level_threshold(
            state.compaction_config.l1_base_bytes,
            state.compaction_config.level_size_multiplier,
            split_level,
        );
        let parts = Self::estimate_split_parts(level_size, threshold, bucket_count)?;
        let split_ranges = Self::split_bucket_range(&tree_range, parts);
        if split_ranges.len() <= 1 {
            return None;
        }
        let old_scopes = snapshot.multi_lsm_version.tree_scopes();
        if old_scopes.len() != snapshot.multi_lsm_version.tree_count() {
            return None;
        }
        let old_versions = snapshot.multi_lsm_version.tree_versions_cloned();
        let mut new_scopes = Vec::with_capacity(old_scopes.len() + split_ranges.len() - 1);
        let mut new_versions = Vec::with_capacity(old_versions.len() + split_ranges.len() - 1);
        for (idx, (scope, version)) in old_scopes.into_iter().zip(old_versions).enumerate() {
            if idx != tree_idx {
                new_scopes.push(scope);
                new_versions.push(version);
                continue;
            }
            for split_range in &split_ranges {
                new_scopes.push(LSMTreeScope::new(
                    split_range.clone(),
                    tree_scope.column_family_id,
                ));
                new_versions.push(Self::clone_version_for_range(version.as_ref(), split_range));
            }
        }
        let new_multi = match crate::db_state::MultiLSMTreeVersion::from_scopes_with_tree_versions(
            snapshot.multi_lsm_version.total_buckets(),
            &new_scopes,
            new_versions,
        ) {
            Ok(multi) => multi,
            Err(_) => return None,
        };
        if !self
            .db_state
            .cas_mutate(snapshot.seq_id, |db_state, current| {
                Some(DbState {
                    seq_id: db_state.allocate_seq_id(),
                    topology_epoch: current.topology_epoch.saturating_add(1),
                    bucket_ranges: current.bucket_ranges.clone(),
                    multi_lsm_version: new_multi.clone(),
                    vlog_version: current.vlog_version.clone(),
                    active: current.active.clone(),
                    immutables: current.immutables.clone(),
                    truncation_cursors: current.truncation_cursors.clone(),
                    suggested_base_snapshot_id: None,
                })
            })
        {
            return None;
        }
        let split_tree_count = split_ranges.len();
        // Results planned against the old scope layout cannot be remapped safely. Their outputs
        // remain uncommitted and drop once their workers observe the missing pending slot.
        state.pending_compaction.clear();
        debug!(
            "auto split tree={} level={} size={} threshold={} old_range={}..{} parts={}",
            tree_idx,
            split_level,
            level_size,
            threshold,
            tree_range.start(),
            tree_range.end(),
            split_ranges.len()
        );
        Some(tree_idx..tree_idx + split_tree_count)
    }

    /// Releases a pending compaction that produced no edit.
    pub(crate) fn on_compaction_complete(&self, tree_idx: usize) -> Option<usize> {
        let mut state = self.state.lock().unwrap();
        self.resolve_completed_compaction_locked(&mut state, tree_idx)
    }

    /// Returns whether any compaction still owns a pending-slot fence. Callers that release
    /// external file leases use this as a lightweight read-only drain check.
    pub(crate) fn has_pending_compactions(&self) -> bool {
        !self.state.lock().unwrap().pending_compaction.is_empty()
    }

    /// Applies a successful compaction while holding the pending-compaction lock.
    ///
    /// Removing the pending marker and installing the edit must be atomic. Otherwise a flush can
    /// schedule another compaction from the old version in between, and both results can install
    /// overlapping files into a non-tiered level.
    pub(crate) fn apply_compaction_result(
        &self,
        tree_idx: usize,
        edit: VersionEdit,
        vlog_edit: Option<VlogEdit>,
    ) -> Option<usize> {
        let access = self.db_lifecycle.begin_owned_access().ok();
        let mut state = self.state.lock().unwrap();
        if access.is_none() {
            state.pending_compaction.remove(&tree_idx);
            return None;
        }
        let apply_tree_idx = self.resolve_completed_compaction_locked(&mut state, tree_idx)?;
        self.apply_edit_locked(&mut state, vec![(apply_tree_idx, edit)], move |db_state| {
            if let Some(vlog_edit) = vlog_edit {
                db_state.vlog_version = db_state.vlog_version.apply_edit(vlog_edit);
            }
        });
        Some(apply_tree_idx)
    }

    fn resolve_completed_compaction_locked(
        &self,
        state: &mut LSMTreeState,
        tree_idx: usize,
    ) -> Option<usize> {
        let expected = state.pending_compaction.remove(&tree_idx)?;
        if self.db_lifecycle.ensure_open().is_err() {
            return None;
        }
        let snapshot = self.db_state.load();
        if snapshot.topology_epoch != expected.topology_epoch {
            return None;
        }
        let Some(expected_scope) = expected.scope else {
            return Some(tree_idx);
        };
        if snapshot
            .multi_lsm_version
            .tree_scope_of_tree(tree_idx)
            .as_ref()
            == Some(&expected_scope)
        {
            return Some(tree_idx);
        }
        None
    }

    pub(crate) fn ttl_provider(&self) -> Arc<crate::ttl::TTLProvider> {
        Arc::clone(&self.ttl_provider)
    }

    /// Returns the current wall-clock seconds for read-time TTL checks, or 0 when TTL is
    /// disabled (in which case `is_fully_expired` always returns false).
    fn read_now_seconds(&self) -> u32 {
        if self.ttl_provider.is_enabled() {
            self.ttl_provider.now_seconds()
        } else {
            0
        }
    }

    pub(crate) fn tree_scope_of_tree(&self, tree_idx: usize) -> Option<LSMTreeScope> {
        self.db_state
            .load()
            .multi_lsm_version
            .tree_scope_of_tree(tree_idx)
    }

    /// Evaluate whether a compaction task should be scheduled for a tree.
    ///
    /// Compaction trigger: checks the target tree's version for L0 overflow
    /// or level-size pressure, consults the compaction policy for file
    /// selection, and submits the task to the compaction worker. Also checks
    /// for auto-split conditions (contaminated files or size trigger) and
    /// performs tree splitting before scheduling compaction on split trees.
    fn maybe_trigger_compaction_locked(&self, state: &mut LSMTreeState, tree_idx: usize) {
        if self.db_lifecycle.ensure_open().is_err() {
            return;
        }
        let levels_snapshot = self.db_state.load();
        let Some(worker) = state.compaction_worker.clone() else {
            return;
        };
        if state.pending_compaction.contains_key(&tree_idx) {
            return;
        }
        if let Some(split_tree_indices) =
            self.maybe_split_tree_locked(state, &levels_snapshot, tree_idx)
        {
            for split_tree_idx in split_tree_indices {
                self.maybe_trigger_compaction_locked(state, split_tree_idx);
            }
            return;
        }
        let tree_version = levels_snapshot.multi_lsm_version.version_of_index(tree_idx);
        let Some(expected_scope) = levels_snapshot
            .multi_lsm_version
            .tree_scope_of_tree(tree_idx)
        else {
            return;
        };
        let truncation_cursors = levels_snapshot.truncation_cursors.capture();
        let ttl_provider = self.ttl_provider();
        let now_seconds = if ttl_provider.is_enabled() {
            ttl_provider.now_seconds()
        } else {
            0
        };
        let policy_context = CompactionPolicyContext {
            truncation_cursors: Some(truncation_cursors.as_map()),
            tree_scope: Some(&expected_scope),
            now_seconds,
        };
        let plan = state.compaction_policy.pick_with_context(
            &tree_version.levels,
            state.compaction_config,
            policy_context,
        );
        let Some(plan) = plan else {
            return;
        };
        if plan.drop_expired {
            if let Some(file) = Self::find_plan_base_file(&tree_version.levels, &plan)
                && file.is_fully_expired(now_seconds)
            {
                let edit = Self::build_truncated_drop_edit(file, &plan);
                debug!(
                    "compaction drop expired file tree={} L{} file_id={}",
                    tree_idx, plan.input_level, plan.base_file_id
                );
                self.apply_edit_locked(state, vec![(tree_idx, edit)], |_db_state| {});
            }
            return;
        }
        if plan.drop_truncated {
            if let Some(file) = Self::find_plan_base_file(&tree_version.levels, &plan)
                && file_fully_covered_by_truncation_cursor(&file, policy_context)
            {
                let edit = Self::build_truncated_drop_edit(file, &plan);
                debug!(
                    "compaction drop truncated file tree={} L{} file_id={}",
                    tree_idx, plan.input_level, plan.base_file_id
                );
                self.apply_edit_locked(state, vec![(tree_idx, edit)], |_db_state| {});
            }
            return;
        }
        if plan.trivial_move {
            if let Some(file) = Self::find_plan_base_file(&tree_version.levels, &plan) {
                let edit = Self::build_trivial_move_edit(file, &plan);
                debug!(
                    "compaction trivial move tree={} L{}->L{} file_id={}",
                    tree_idx, plan.input_level, plan.output_level, plan.base_file_id
                );
                self.apply_edit_locked(state, vec![(tree_idx, edit)], |_db_state| {});
            }
            return;
        }
        debug!("trigger compaction plan tree={} {}", tree_idx, plan);
        let resolved = match resolve_compaction_plan(
            &tree_version.levels,
            &plan,
            &state.compaction_config,
            state.schema_manager.as_ref(),
            expected_scope.column_family_id,
        ) {
            Ok(resolved) => resolved,
            Err(err) => {
                error!(
                    "failed to resolve compaction plan tree={}: {}",
                    tree_idx, err
                );
                return;
            }
        };
        if let Some(handle) = worker.submit_runs(
            tree_idx,
            resolved.runs,
            resolved.output_level,
            resolved.target_schema_id,
            state.compaction_config.output_file_type,
            self.ttl_provider(),
        ) {
            // Pending is inserted AFTER submit_runs returns. This is safe against the worker's
            // completion racing ahead: `on_compaction_complete` re-acquires this same `state` mutex,
            // which we currently hold, so the worker's async completion task cannot remove pending
            // until we release the lock here — i.e. after this insert. Insert always precedes
            // remove. (A worker that called `on_compaction_complete` synchronously inside
            // `submit_runs` would deadlock on this mutex; all real workers defer completion to a
            // spawned task.) If submit_runs returns None (worker declined), no pending entry is
            // inserted, leaving the slot free for the next trigger.
            state.pending_compaction.insert(
                tree_idx,
                PendingCompaction {
                    scope: Some(expected_scope),
                    topology_epoch: levels_snapshot.topology_epoch,
                },
            );
            std::mem::drop(handle);
        }
    }

    fn find_plan_base_file(levels: &[Level], plan: &CompactionPlan) -> Option<Arc<DataFile>> {
        levels
            .iter()
            .find(|level| level.ordinal == plan.input_level)
            .and_then(|level| {
                level
                    .files
                    .iter()
                    .find(|file| file.file_id == plan.base_file_id)
            })
            .cloned()
    }

    fn build_truncated_drop_edit(file: Arc<DataFile>, plan: &CompactionPlan) -> VersionEdit {
        VersionEdit {
            level_edits: vec![LevelEdit {
                level: plan.input_level,
                removed_files: vec![file],
                new_files: Vec::new(),
            }],
        }
    }

    fn build_trivial_move_edit(file: Arc<DataFile>, plan: &CompactionPlan) -> VersionEdit {
        VersionEdit {
            level_edits: vec![
                LevelEdit {
                    level: plan.input_level,
                    removed_files: vec![Arc::clone(&file)],
                    new_files: Vec::new(),
                },
                LevelEdit {
                    level: plan.output_level,
                    removed_files: Vec::new(),
                    new_files: vec![file],
                },
            ],
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn get(
        &self,
        file_manager: &Arc<FileManager>,
        bucket: u16,
        encoded_key: &[u8],
        target_schema: &Schema,
        schema_manager: &SchemaManager,
        selected_columns: Option<&[usize]>,
        selected_mask: Option<&[u8]>,
        terminal_mask: Option<&mut [u8]>,
    ) -> Result<Vec<SchemaValue>> {
        let snapshot = self.db_state.load();
        self.get_with_snapshot(
            file_manager,
            snapshot,
            bucket,
            encoded_key,
            target_schema,
            schema_manager,
            selected_columns,
            selected_mask,
            terminal_mask,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn get_with_snapshot(
        &self,
        file_manager: &Arc<FileManager>,
        snapshot: Arc<DbState>,
        bucket: u16,
        encoded_key: &[u8],
        target_schema: &Schema,
        schema_manager: &SchemaManager,
        selected_columns: Option<&[usize]>,
        selected_mask: Option<&[u8]>,
        terminal_mask: Option<&mut [u8]>,
    ) -> Result<Vec<SchemaValue>> {
        let column_family_id = key_column_family(encoded_key).unwrap_or(DEFAULT_COLUMN_FAMILY_ID);
        let Some(version) = snapshot
            .multi_lsm_version
            .version_for_bucket_and_column_family(bucket, column_family_id)
        else {
            return Ok(Vec::new());
        };
        self.get_with_levels(
            file_manager,
            version.as_ref().levels.as_slice(),
            encoded_key,
            target_schema,
            schema_manager,
            column_family_id,
            selected_columns,
            selected_mask,
            terminal_mask,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn get_many_with_snapshot(
        &self,
        file_manager: &Arc<FileManager>,
        snapshot: Arc<DbState>,
        requests: &mut [BatchGetRequest],
        target_schema: &Schema,
        schema_manager: &SchemaManager,
        selected_columns: Option<&[usize]>,
        selected_mask: Option<&[u8]>,
        column_family_id: u8,
    ) -> Result<()> {
        let tree_groups = group_request_indices_by_tree(&snapshot, requests, column_family_id);
        for (tree_idx, mut request_indices) in tree_groups {
            self.get_many_with_levels(
                file_manager,
                snapshot
                    .multi_lsm_version
                    .version_of_index(tree_idx)
                    .levels
                    .as_slice(),
                &mut request_indices,
                requests,
                target_schema,
                schema_manager,
                column_family_id,
                selected_columns,
                selected_mask,
            )?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn get_many_with_levels(
        &self,
        file_manager: &Arc<FileManager>,
        levels: &[Level],
        request_indices: &mut Vec<usize>,
        requests: &mut [BatchGetRequest],
        target_schema: &Schema,
        schema_manager: &SchemaManager,
        column_family_id: u8,
        selected_columns: Option<&[usize]>,
        selected_mask: Option<&[u8]>,
    ) -> Result<()> {
        let mut requests_are_sorted = false;
        let now_seconds = self.read_now_seconds();
        for level in levels {
            retain_active_request_indices(request_indices, requests);
            if request_indices.is_empty() {
                break;
            }
            if level.tiered {
                let use_partition_routing =
                    request_indices.len() > SMALL_BATCH_POINT_ROUTING_REQUEST_LIMIT;
                if use_partition_routing && !requests_are_sorted {
                    sort_request_indices(request_indices, requests);
                    requests_are_sorted = true;
                }
                for file in level.files.iter().rev() {
                    if file.is_fully_expired(now_seconds) {
                        continue;
                    }
                    let matches = if use_partition_routing {
                        let first = request_indices.partition_point(|idx| {
                            requests[*idx].encoded_key.as_ref() < file.start_key.as_slice()
                        });
                        let last = request_indices.partition_point(|idx| {
                            requests[*idx].encoded_key.as_ref() <= file.end_key.as_slice()
                        });
                        request_indices[first..last]
                            .iter()
                            .copied()
                            .filter(|idx| !requests[*idx].stopped)
                            .collect::<Vec<_>>()
                    } else {
                        request_indices
                            .iter()
                            .copied()
                            .filter(|idx| {
                                let request = &requests[*idx];
                                !request.stopped
                                    && request.encoded_key.as_ref() >= file.start_key.as_slice()
                                    && request.encoded_key.as_ref() <= file.end_key.as_slice()
                            })
                            .collect::<Vec<_>>()
                    };
                    if matches.is_empty() {
                        continue;
                    }
                    self.get_values_in_one_file_many(
                        file,
                        level.ordinal,
                        file_manager,
                        &matches,
                        requests,
                        target_schema,
                        schema_manager,
                        column_family_id,
                        selected_columns,
                        selected_mask,
                    )?;
                }
            } else {
                let use_binary_routing =
                    should_use_binary_non_tiered_routing(request_indices.len(), level.files.len());
                if !use_binary_routing && !requests_are_sorted {
                    sort_request_indices(request_indices, requests);
                    requests_are_sorted = true;
                }
                let by_file = if use_binary_routing {
                    route_non_tiered_requests_binary(&level.files, request_indices, requests)
                        .into_iter()
                        .collect::<Vec<_>>()
                } else {
                    route_non_tiered_requests(&level.files, request_indices, requests)
                        .into_iter()
                        .enumerate()
                        .filter_map(|(file_idx, matches)| {
                            (!matches.is_empty()).then_some((file_idx, matches))
                        })
                        .collect::<Vec<_>>()
                };
                for (file_idx, matches) in by_file {
                    self.get_values_in_one_file_many(
                        &level.files[file_idx],
                        level.ordinal,
                        file_manager,
                        &matches,
                        requests,
                        target_schema,
                        schema_manager,
                        column_family_id,
                        selected_columns,
                        selected_mask,
                    )?;
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn get_with_levels(
        &self,
        file_manager: &Arc<FileManager>,
        levels: &[Level],
        encoded_key: &[u8],
        target_schema: &Schema,
        schema_manager: &SchemaManager,
        column_family_id: u8,
        selected_columns: Option<&[usize]>,
        selected_mask: Option<&[u8]>,
        mut terminal_mask: Option<&mut [u8]>,
    ) -> Result<Vec<SchemaValue>> {
        let num_columns = target_schema
            .num_columns_in_family(column_family_id)
            .unwrap_or(0);
        let mut values = Vec::new();
        let mask_size = num_columns.div_ceil(8).max(1);
        let last_bits = (num_columns - 1) % 8 + 1;
        let last_mask = (1u8 << last_bits) - 1;
        let mut decode_mask = vec![0xFF; mask_size];
        decode_mask[mask_size - 1] &= last_mask;
        if let Some(ref cols) = terminal_mask {
            for (idx, mask_byte) in cols.iter().enumerate().take(mask_size) {
                decode_mask[idx] &= !*mask_byte;
            }
            decode_mask[mask_size - 1] &= last_mask;
        }
        if let Some(mask) = selected_mask {
            for (idx, mask_byte) in mask.iter().enumerate().take(mask_size) {
                decode_mask[idx] &= *mask_byte;
            }
            decode_mask[mask_size - 1] &= last_mask;
        }
        if num_columns == 1 {
            terminal_mask = None;
        }

        let now_seconds = self.read_now_seconds();
        for level in levels.iter() {
            if level.tiered {
                for file in level.files.iter().rev() {
                    if file.is_fully_expired(now_seconds) {
                        continue;
                    }
                    let should_continue = self.get_values_in_one_file(
                        file,
                        level.ordinal,
                        file_manager,
                        encoded_key,
                        target_schema,
                        schema_manager,
                        column_family_id,
                        selected_columns,
                        selected_mask,
                        terminal_mask.as_deref_mut(),
                        &mut decode_mask,
                        &mut values,
                    )?;
                    if !should_continue {
                        return Ok(values);
                    }
                }
            } else {
                for file in level.files.iter() {
                    if encoded_key < file.start_key.as_slice()
                        || encoded_key > file.end_key.as_slice()
                    {
                        continue;
                    }
                    if file.is_fully_expired(now_seconds) {
                        break;
                    }
                    let should_continue = self.get_values_in_one_file(
                        file,
                        level.ordinal,
                        file_manager,
                        encoded_key,
                        target_schema,
                        schema_manager,
                        column_family_id,
                        selected_columns,
                        selected_mask,
                        terminal_mask.as_deref_mut(),
                        &mut decode_mask,
                        &mut values,
                    )?;
                    if !should_continue {
                        return Ok(values);
                    }
                    break;
                }
            }
        }

        Ok(values)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_with_snapshot(
        &self,
        file_manager: &Arc<FileManager>,
        snapshot: Arc<DbState>,
        target_schema: Arc<Schema>,
        schema_manager: Arc<SchemaManager>,
        read_ahead_bytes: usize,
        selected_columns: Option<&[usize]>,
        bucket: u16,
        column_family_id: u8,
        encoded_start: &[u8],
        encoded_end: Option<&[u8]>,
        preload_scan_cursor_block: bool,
    ) -> Result<Vec<DynKvIterator>> {
        let selected_columns = selected_columns.map(|columns| columns.to_vec());
        let preload_scan_cursor_block = preload_scan_cursor_block && self.block_cache.is_some();
        let read_metadata_cache_mode = self.sst_read_metadata_cache_mode;
        let pinned_metadata_max_level = self.sst_pinned_metadata_max_level;
        let pin_metadata_partitions = self.sst_pinned_metadata_partitions_enabled;
        let mut iterators: Vec<DynKvIterator> = Vec::new();
        let mut runs: Vec<SortedRun> = Vec::new();
        let now_seconds = self.read_now_seconds();
        let target_num_columns = target_schema
            .num_columns_in_family(column_family_id)
            .unwrap_or(0);
        let version = snapshot
            .multi_lsm_version
            .version_for_bucket_and_column_family(bucket, column_family_id);
        if let Some(version) = version {
            for level in &version.levels {
                if level.files.is_empty() {
                    continue;
                }
                if level.tiered {
                    for file in level.files.iter().rev() {
                        if !file.is_fully_expired(now_seconds)
                            && file_intersects_scan(file, encoded_start, encoded_end)
                        {
                            runs.push(SortedRun::new(level.ordinal, vec![Arc::clone(file)]));
                        }
                    }
                } else {
                    let files: Vec<Arc<DataFile>> = level
                        .files
                        .iter()
                        .filter(|file| {
                            !file.is_fully_expired(now_seconds)
                                && file_intersects_scan(file, encoded_start, encoded_end)
                        })
                        .cloned()
                        .collect();
                    if !files.is_empty() {
                        runs.push(SortedRun::new(level.ordinal, files));
                    }
                }
            }
        }
        for run in runs {
            let file_manager = Arc::clone(file_manager);
            let block_cache = self.block_cache.clone();
            let sst_metrics = Arc::clone(&self.sst_metrics);
            let cache_namespace = bucket_scoped_cache_namespace(self.cache_namespace, bucket);
            let target_schema = Arc::clone(&target_schema);
            let schema_manager = Arc::clone(&schema_manager);
            let selected_columns = selected_columns.clone();
            let scan_hot_blocks = Arc::clone(&self.scan_hot_blocks);
            let pin_metadata =
                pinned_metadata_max_level.is_some_and(|max_level| run.level() <= max_level);
            let run_iter = run.iter(move |file| {
                let source_schema = schema_manager.schema(file.schema_id)?;
                let source_num_columns = source_schema
                    .num_columns_in_family(column_family_id)
                    .unwrap_or(0);
                let reader = file_manager.open_data_file_reader(file.file_id)?;
                let reader: Box<dyn crate::file::RandomAccessFile> =
                    if read_ahead_bytes > 0 && reader.prefers_read_ahead() {
                        Box::new(ReadAheadBufferedReader::new(
                            reader,
                            read_ahead_bytes,
                            read_ahead_runtime(),
                        ))
                    } else {
                        Box::new(reader)
                    };
                let iter: DynKvIterator = match file.file_type {
                    DataFileType::SSTable => {
                        let sst_options = SSTIteratorOptions {
                            metrics: Some(Arc::clone(&sst_metrics)),
                            num_columns: source_num_columns,
                            bloom_filter_enabled: true,
                            read_metadata_cache_mode,
                            pin_metadata,
                            pin_metadata_partitions,
                            cache_namespace,
                            preload_next_data_block: preload_scan_cursor_block,
                            hot_block_registry: preload_scan_cursor_block
                                .then(|| Arc::clone(&scan_hot_blocks)),
                            ..SSTIteratorOptions::default()
                        };
                        Box::new(SSTIterator::with_cache_and_file(
                            reader,
                            file,
                            sst_options,
                            block_cache.clone(),
                        )?)
                    }
                    DataFileType::Parquet => {
                        let parquet_read_columns = if file.schema_id == target_schema.version() {
                            selected_columns.as_deref()
                        } else {
                            None
                        };
                        Box::new(ParquetIterator::from_data_file_with_options(
                            reader,
                            file,
                            block_cache.clone(),
                            parquet_read_columns,
                            crate::parquet::ParquetIteratorOptions {
                                cache_namespace,
                                preload_next_row_group: preload_scan_cursor_block,
                                hot_block_registry: preload_scan_cursor_block
                                    .then(|| Arc::clone(&scan_hot_blocks)),
                                ..Default::default()
                            },
                        )?)
                    }
                };
                let base_iter: DynKvIterator = if file.needs_bucket_filter() {
                    Box::new(BucketFilterIterator::new(
                        iter,
                        file.effective_bucket_range.clone(),
                    ))
                } else {
                    Box::new(iter)
                };
                let iter: DynKvIterator = if file.schema_id == target_schema.version() {
                    base_iter
                } else {
                    Box::new(SchemaEvolvingIterator::new(
                        base_iter,
                        Arc::clone(&source_schema),
                        Arc::clone(&target_schema),
                        Arc::clone(&schema_manager),
                        column_family_id,
                    ))
                };
                let iter: DynKvIterator = if file.vlog_file_seq_offset == 0 {
                    iter
                } else {
                    Box::new(VlogSeqOffsetIterator::new(
                        iter,
                        target_num_columns,
                        file.vlog_file_seq_offset,
                    ))
                };
                let iter: DynKvIterator = if let Some(columns) = selected_columns.as_deref() {
                    Box::new(ColumnMaskingIterator::new(
                        iter,
                        target_num_columns,
                        columns,
                    ))
                } else {
                    iter
                };
                Ok(iter)
            });
            iterators.push(Box::new(run_iter));
        }
        Ok(iterators)
    }

    #[allow(clippy::too_many_arguments)]
    fn get_values_in_one_file_many(
        &self,
        file: &Arc<DataFile>,
        level_ordinal: u8,
        file_manager: &Arc<FileManager>,
        request_indices: &[usize],
        requests: &mut [BatchGetRequest],
        target_schema: &Schema,
        schema_manager: &SchemaManager,
        column_family_id: u8,
        selected_columns: Option<&[usize]>,
        selected_mask: Option<&[u8]>,
    ) -> Result<()> {
        if request_indices.is_empty() {
            return Ok(());
        }
        let target_num_columns = target_schema
            .num_columns_in_family(column_family_id)
            .unwrap_or(0);
        if file.file_type == DataFileType::Parquet {
            // Keep Parquet on the established point-read path for now. SSTs are
            // the format whose block I/O is coalesced by this batch API.
            for idx in request_indices {
                let request = &mut requests[*idx];
                let should_continue = self.get_values_in_one_file(
                    file,
                    level_ordinal,
                    file_manager,
                    request.encoded_key.as_ref(),
                    target_schema,
                    schema_manager,
                    column_family_id,
                    selected_columns,
                    selected_mask,
                    request.terminal_mask.as_deref_mut(),
                    &mut request.decode_mask,
                    &mut request.values,
                )?;
                request.stopped = !should_continue
                    || (target_num_columns == 1
                        && request
                            .values
                            .last()
                            .is_some_and(|value| value.value.is_terminal()));
            }
            return Ok(());
        }

        let source_schema = schema_manager.schema(file.schema_id)?;
        let source_num_columns = source_schema
            .num_columns_in_family(column_family_id)
            .unwrap_or(0);
        let indices = request_indices
            .iter()
            .copied()
            .filter(|idx| file.effective_bucket_range.contains(&requests[*idx].bucket))
            .collect::<Vec<_>>();
        if indices.is_empty() {
            return Ok(());
        }
        let keys = indices
            .iter()
            .map(|idx| requests[*idx].encoded_key.as_ref())
            .collect::<Vec<_>>();
        let data_cache_namespaces = indices
            .iter()
            .map(|idx| bucket_scoped_cache_namespace(self.cache_namespace, requests[*idx].bucket))
            .collect::<Vec<_>>();
        let values = SSTPointReader::get_exact_many(
            Box::new(file_manager.open_data_file_reader(file.file_id)?),
            file.as_ref(),
            SSTIteratorOptions {
                num_columns: source_num_columns,
                metrics: Some(Arc::clone(&self.sst_metrics)),
                bloom_filter_enabled: true,
                read_metadata_cache_mode: self.sst_read_metadata_cache_mode,
                pin_metadata: self
                    .sst_pinned_metadata_max_level
                    .is_some_and(|max_level| level_ordinal <= max_level),
                pin_metadata_partitions: self.sst_pinned_metadata_partitions_enabled,
                cache_namespace: self.cache_namespace,
                ..SSTIteratorOptions::default()
            },
            self.block_cache.clone(),
            &keys,
            &data_cache_namespaces,
        )?;
        for (idx, value_bytes) in indices.into_iter().zip(values) {
            let request = &mut requests[idx];
            let should_continue = self.apply_value_from_file(
                file,
                target_schema,
                column_family_id,
                source_num_columns,
                selected_mask,
                request.terminal_mask.as_deref_mut(),
                &mut request.decode_mask,
                &mut request.values,
                value_bytes,
            )?;
            request.stopped = !should_continue
                || (target_num_columns == 1
                    && request
                        .values
                        .last()
                        .is_some_and(|value| value.value.is_terminal()));
        }
        Ok(())
    }

    /// Get values from one data file for the given encoded key.
    /// Returns Ok(true) if the caller should continue to the next file,
    /// or Ok(false) if the caller should stop.
    #[allow(clippy::too_many_arguments)]
    fn get_values_in_one_file(
        &self,
        file: &Arc<DataFile>,
        level_ordinal: u8,
        file_manager: &Arc<FileManager>,
        encoded_key: &[u8],
        target_schema: &Schema,
        schema_manager: &SchemaManager,
        column_family_id: u8,
        selected_columns: Option<&[usize]>,
        selected_mask: Option<&[u8]>,
        terminal_mask: Option<&mut [u8]>,
        decode_mask: &mut [u8],
        out_values: &mut Vec<SchemaValue>,
    ) -> Result<bool> {
        let target_schema_id = target_schema.version();
        let source_schema = schema_manager.schema(file.schema_id)?;
        let source_num_columns = source_schema
            .num_columns_in_family(column_family_id)
            .unwrap_or(0);
        if let Some(bucket) = key_bucket(encoded_key)
            && !file.effective_bucket_range.contains(&bucket)
        {
            return Ok(true);
        }
        let reader = file_manager.open_data_file_reader(file.file_id)?;
        let cache_namespace = key_bucket(encoded_key)
            .map(|bucket| bucket_scoped_cache_namespace(self.cache_namespace, bucket))
            .unwrap_or(self.cache_namespace);
        let value_bytes_opt = match file.file_type {
            DataFileType::SSTable => SSTPointReader::get_exact(
                Box::new(reader),
                file.as_ref(),
                SSTIteratorOptions {
                    num_columns: source_num_columns,
                    metrics: Some(Arc::clone(&self.sst_metrics)),
                    bloom_filter_enabled: true,
                    read_metadata_cache_mode: self.sst_read_metadata_cache_mode,
                    pin_metadata: self
                        .sst_pinned_metadata_max_level
                        .is_some_and(|max_level| level_ordinal <= max_level),
                    pin_metadata_partitions: self.sst_pinned_metadata_partitions_enabled,
                    cache_namespace,
                    ..SSTIteratorOptions::default()
                },
                self.block_cache.clone(),
                encoded_key,
            )?,
            DataFileType::Parquet => {
                let parquet_read_columns = if file.schema_id == target_schema_id {
                    selected_columns
                } else {
                    None
                };
                let mut iter = ParquetIterator::from_data_file_with_columns(
                    Box::new(reader),
                    file.as_ref(),
                    self.block_cache.clone(),
                    parquet_read_columns,
                )?;
                iter.seek(encoded_key)?;
                if iter.valid()
                    && let Some(current_key) = iter.key()?
                    && current_key.as_ref() == encoded_key
                {
                    iter.value()?
                } else {
                    None
                }
            }
        };
        self.apply_value_from_file(
            file,
            target_schema,
            column_family_id,
            source_num_columns,
            selected_mask,
            terminal_mask,
            decode_mask,
            out_values,
            value_bytes_opt,
        )
    }

    /// Applies one file lookup result to a logical point read.
    ///
    /// Both the single-key path and the batched SST file-read path call this
    /// function so schema evolution, TTL, terminal values, and VLOG offsets
    /// have identical behavior. `value_bytes_opt == None` means this file has
    /// no value for the key and the caller should continue searching older
    /// files. Values keep their source schema id, so final merging can use the
    /// merge operators that encoded each schema epoch.
    ///
    /// `terminal_mask` records columns already made terminal by newer files;
    /// `decode_mask` avoids decoding those older columns; and `out_values`
    /// accumulates values in newest-to-oldest file order for the final merge.
    #[allow(clippy::too_many_arguments)]
    fn apply_value_from_file(
        &self,
        file: &DataFile,
        target_schema: &Schema,
        column_family_id: u8,
        source_num_columns: usize,
        selected_mask: Option<&[u8]>,
        mut terminal_mask: Option<&mut [u8]>,
        decode_mask: &mut [u8],
        out_values: &mut Vec<SchemaValue>,
        value_bytes_opt: Option<Bytes>,
    ) -> Result<bool> {
        let num_columns = target_schema
            .num_columns_in_family(column_family_id)
            .unwrap_or(0);
        let target_schema_id = target_schema.version();
        let mask_size = decode_mask.len();
        if let Some(value_bytes) = value_bytes_opt {
            let is_target_schema = file.schema_id == target_schema_id;
            let value = if is_target_schema {
                let mut value_bytes = value_bytes;
                let value = decode_value_masked(
                    &mut value_bytes,
                    num_columns,
                    decode_mask,
                    terminal_mask.as_deref_mut(),
                )?;
                if self.ttl_provider.expired(&value.expired_at) {
                    return Ok(false);
                }
                value
            } else {
                let mut value_bytes = value_bytes;
                let value = decode_value(&mut value_bytes, source_num_columns)?;
                if self.ttl_provider.expired(&value.expired_at) {
                    return Ok(false);
                }
                value
            };
            if is_target_schema {
                if let Some(mask) = terminal_mask.as_deref_mut() {
                    let evolved_mask = value.terminal_mask();
                    for (idx, mask_byte) in mask.iter_mut().enumerate().take(mask_size) {
                        *mask_byte |= evolved_mask.get(idx).copied().unwrap_or(0);
                    }
                }
                if let (Some(mask), Some(selected_mask)) =
                    (terminal_mask.as_deref_mut(), selected_mask)
                {
                    for (idx, mask_byte) in mask.iter_mut().enumerate().take(mask_size) {
                        *mask_byte &= selected_mask[idx];
                    }
                }
            }
            let value = apply_vlog_offset_to_value(value, file.vlog_file_seq_offset)?;
            let should_stop = num_columns > 1 && value.is_terminal();
            if let Some(ref mask) = terminal_mask {
                for (idx, mask_byte) in mask.iter().enumerate().take(mask_size) {
                    decode_mask[idx] &= !*mask_byte;
                }
                if mask_size > 0 {
                    let last_bits = (num_columns - 1) % 8 + 1;
                    let last_mask = (1u8 << last_bits) - 1;
                    decode_mask[mask_size - 1] &= last_mask;
                }
            }
            out_values.push(SchemaValue {
                schema_id: file.schema_id,
                value,
            });
            return Ok(!should_stop);
        }
        Ok(true)
    }
}

/// Routes active, sorted request indices to non-overlapping, start-key-sorted files.
/// A request in a gap is intentionally not assigned to either adjacent file.
fn route_non_tiered_requests(
    files: &[Arc<DataFile>],
    request_indices: &[usize],
    requests: &[BatchGetRequest],
) -> Vec<Vec<usize>> {
    let mut by_file = vec![Vec::new(); files.len()];
    let mut file_idx = 0;
    for idx in request_indices {
        let request = &requests[*idx];
        while file_idx < files.len()
            && files[file_idx].end_key.as_slice() < request.encoded_key.as_ref()
        {
            file_idx += 1;
        }
        if file_idx == files.len() {
            break;
        }
        if request.encoded_key.as_ref() >= files[file_idx].start_key.as_slice() {
            by_file[file_idx].push(*idx);
        }
    }
    by_file
}

fn sort_request_indices(request_indices: &mut [usize], requests: &[BatchGetRequest]) {
    request_indices.sort_unstable_by(|left, right| {
        requests[*left]
            .encoded_key
            .cmp(&requests[*right].encoded_key)
    });
}

fn retain_active_request_indices(request_indices: &mut Vec<usize>, requests: &[BatchGetRequest]) {
    request_indices.retain(|idx| !requests[*idx].stopped);
}

fn should_use_binary_non_tiered_routing(request_count: usize, file_count: usize) -> bool {
    if request_count <= SMALL_BATCH_POINT_ROUTING_REQUEST_LIMIT || file_count <= 1 {
        return true;
    }
    let binary_steps = (usize::BITS - (file_count - 1).leading_zeros()) as usize;
    let binary_cost = request_count.saturating_mul(binary_steps);
    let merge_cost = request_count.saturating_add(file_count);
    binary_cost <= merge_cost
}

fn route_non_tiered_requests_binary(
    files: &[Arc<DataFile>],
    request_indices: &[usize],
    requests: &[BatchGetRequest],
) -> BTreeMap<usize, Vec<usize>> {
    let mut by_file = BTreeMap::new();
    for idx in request_indices {
        let request = &requests[*idx];
        let file_idx =
            files.partition_point(|file| file.end_key.as_slice() < request.encoded_key.as_ref());
        if let Some(file) = files.get(file_idx)
            && request.encoded_key.as_ref() >= file.start_key.as_slice()
        {
            by_file.entry(file_idx).or_insert_with(Vec::new).push(*idx);
        }
    }
    by_file
}

fn group_request_indices_by_tree(
    snapshot: &DbState,
    requests: &[BatchGetRequest],
    column_family_id: u8,
) -> BTreeMap<usize, Vec<usize>> {
    let mut groups = BTreeMap::new();
    for (idx, request) in requests.iter().enumerate() {
        if request.stopped {
            continue;
        }
        if let Some(tree_idx) = snapshot
            .multi_lsm_version
            .tree_index_for_bucket_and_column_family(request.bucket, column_family_id)
        {
            groups.entry(tree_idx).or_insert_with(Vec::new).push(idx);
        }
    }
    groups
}

#[cfg(test)]
#[path = "../tests/unit/lsm.rs"]
mod tests;
