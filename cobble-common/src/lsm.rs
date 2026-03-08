use crate::compaction::{
    CompactionConfig, CompactionPlan, CompactionPolicy, CompactionWorker, MinOverlapPolicy,
    RoundRobinPolicy, build_runs_for_plan, level_threshold,
};
use crate::data_file::{DataFile, intersect_bucket_ranges};
use crate::error::Result;
use crate::file::{FileManager, ReadAheadBufferedReader};
use crate::iterator::{
    BucketFilterIterator, KvIterator, SchemaEvolvingIterator, SortedRun, VlogSeqOffsetIterator,
};
use crate::metrics_manager::MetricsManager;
use crate::schema::{Schema, SchemaManager};
use crate::sst::block_cache::BlockCache;
use crate::sst::row_codec::{decode_value, decode_value_masked};
use crate::sst::{SSTIterator, SSTIteratorMetrics, SSTIteratorOptions};
use crate::r#type::{Value, key_bucket};
use log::debug;
use std::collections::{BTreeMap, HashMap};
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use crate::db_state::{DbState, DbStateHandle, bucket_range_len};
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

#[derive(Clone)]
pub(crate) struct LSMTreeVersion {
    pub(crate) levels: Vec<Level>,
}

pub(crate) struct LSMTree {
    db_state: Arc<DbStateHandle>,
    block_cache: Option<BlockCache>,
    state: Mutex<LSMTreeState>,
    ttl_provider: Arc<crate::ttl::TTLProvider>,
    sst_metrics: Arc<SSTIteratorMetrics>,
}

struct LSMTreeState {
    level_options: Vec<LevelOptions>,
    compaction_config: CompactionConfig,
    compaction_policy: Box<dyn CompactionPolicy>,
    pending_compaction: HashMap<usize, Option<RangeInclusive<u16>>>,
    compaction_worker: Option<Arc<dyn CompactionWorker>>,
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
            Arc::new(MetricsManager::new("unknown".to_string())),
        )
    }
}

impl LSMTree {
    pub(crate) fn with_state(
        db_state: Arc<DbStateHandle>,
        metrics_manager: Arc<MetricsManager>,
    ) -> Self {
        Self::with_state_and_ttl(db_state, Arc::new(TTLProvider::disabled()), metrics_manager)
    }

    pub(crate) fn with_state_and_ttl(
        db_state: Arc<DbStateHandle>,
        ttl_provider: Arc<TTLProvider>,
        metrics_manager: Arc<MetricsManager>,
    ) -> Self {
        Self {
            db_state,
            block_cache: None,
            state: Mutex::new(LSMTreeState {
                // at least 2 level option
                level_options: vec![
                    LevelOptions { tiered: true },
                    LevelOptions { tiered: false },
                ],
                compaction_config: CompactionConfig::default(),
                compaction_policy: Box::new(RoundRobinPolicy::new()),
                pending_compaction: HashMap::new(),
                compaction_worker: None,
            }),
            ttl_provider,
            sst_metrics: metrics_manager.sst_iterator_metrics(),
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
                    bucket_ranges: snapshot.bucket_ranges.clone(),
                    multi_lsm_version,
                    vlog_version: snapshot.vlog_version.clone(),
                    active: snapshot.active.clone(),
                    immutables: snapshot.immutables.clone(),
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
        files_by_tree: Vec<(usize, Arc<DataFile>)>,
        vlog_edit: Option<VlogEdit>,
    ) -> Result<Arc<DbState>> {
        if files_by_tree.is_empty() {
            return Err(crate::error::Error::InvalidState(
                "cannot add empty new files".to_string(),
            ));
        }
        let mut state = self.state.lock().unwrap();
        let snapshot = self.db_state.load();
        let tree_ranges = snapshot.multi_lsm_version.bucket_ranges();
        let mut grouped: BTreeMap<usize, Vec<Arc<DataFile>>> = BTreeMap::new();
        for (tree_idx_hint, file) in files_by_tree {
            if tree_ranges.is_empty() {
                grouped.entry(0).or_default().push(file);
                continue;
            }
            let mut matched = false;
            for (tree_idx, tree_range) in tree_ranges.iter().enumerate() {
                if let Some(effective_bucket_range) =
                    intersect_bucket_ranges(&file.bucket_range, tree_range)
                {
                    let scoped_file = if effective_bucket_range == file.effective_bucket_range
                        || (effective_bucket_range == file.bucket_range
                            && file.effective_bucket_range == file.bucket_range)
                    {
                        Arc::clone(&file)
                    } else {
                        Arc::new(
                            file.as_ref()
                                .with_effective_bucket_range(effective_bucket_range),
                        )
                    };
                    grouped.entry(tree_idx).or_default().push(scoped_file);
                    matched = true;
                }
            }
            if !matched {
                return Err(crate::error::Error::InvalidState(format!(
                    "cannot map flush output file {} from tree {} to current multi-lsm ranges",
                    file.file_id, tree_idx_hint
                )));
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

    pub(crate) fn sst_metrics(&self) -> Arc<SSTIteratorMetrics> {
        Arc::clone(&self.sst_metrics)
    }

    pub(crate) fn shutdown_compaction(&self) {
        let mut state = self.state.lock().unwrap();
        if let Some(worker) = state.compaction_worker.take() {
            worker.shutdown();
        }
        state.compaction_worker = None;
        state.pending_compaction.clear();
    }

    fn make_policy(kind: crate::config::CompactionPolicyKind) -> Box<dyn CompactionPolicy> {
        match kind {
            crate::config::CompactionPolicyKind::RoundRobin => Box::new(RoundRobinPolicy::new()),
            crate::config::CompactionPolicyKind::MinOverlap => Box::new(MinOverlapPolicy::new()),
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

    fn remap_pending_compactions(
        pending: &HashMap<usize, Option<RangeInclusive<u16>>>,
        new_multi: &crate::db_state::MultiLSMTreeVersion,
        split_tree_idx: usize,
        added_tree_count: usize,
    ) -> HashMap<usize, Option<RangeInclusive<u16>>> {
        let mut remapped = HashMap::with_capacity(pending.len());
        for (idx, expected_range) in pending {
            let new_idx = if let Some(expected_range) = expected_range.as_ref() {
                new_multi
                    .tree_index_for_exact_range(expected_range)
                    .or_else(|| new_multi.tree_index_for_bucket(*expected_range.start()))
                    .unwrap_or(*idx)
            } else if *idx > split_tree_idx {
                idx.saturating_add(added_tree_count)
            } else {
                *idx
            };
            remapped.insert(new_idx, expected_range.clone());
        }
        remapped
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
        let tree_range = snapshot.multi_lsm_version.bucket_range_of_tree(tree_idx)?;
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
        let old_ranges = snapshot.multi_lsm_version.bucket_ranges();
        if old_ranges.len() != snapshot.multi_lsm_version.tree_count() {
            return None;
        }
        let old_versions = snapshot.multi_lsm_version.tree_versions_cloned();
        let mut new_ranges = Vec::with_capacity(old_ranges.len() + split_ranges.len() - 1);
        let mut new_versions = Vec::with_capacity(old_versions.len() + split_ranges.len() - 1);
        for (idx, (range, version)) in old_ranges
            .into_iter()
            .zip(old_versions.into_iter())
            .enumerate()
        {
            if idx != tree_idx {
                new_ranges.push(range);
                new_versions.push(version);
                continue;
            }
            for split_range in &split_ranges {
                new_ranges.push(split_range.clone());
                new_versions.push(Self::clone_version_for_range(version.as_ref(), split_range));
            }
        }
        let new_multi =
            match crate::db_state::MultiLSMTreeVersion::from_bucket_ranges_with_tree_versions(
                snapshot.multi_lsm_version.total_buckets(),
                &new_ranges,
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
                    bucket_ranges: current.bucket_ranges.clone(),
                    multi_lsm_version: new_multi.clone(),
                    vlog_version: current.vlog_version.clone(),
                    active: current.active.clone(),
                    immutables: current.immutables.clone(),
                    suggested_base_snapshot_id: None,
                })
            })
        {
            return None;
        }
        let split_tree_count = split_ranges.len();
        let added_tree_count = split_ranges.len().saturating_sub(1);
        state.pending_compaction = Self::remap_pending_compactions(
            &state.pending_compaction,
            &new_multi,
            tree_idx,
            added_tree_count,
        );
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

    /// Returns Some(tree_idx) if the compaction result for the tree index can be applied.
    pub(crate) fn on_compaction_complete(&self, tree_idx: usize) -> Option<usize> {
        let mut state = self.state.lock().unwrap();
        let expected_range = state.pending_compaction.remove(&tree_idx).flatten();
        let snapshot = self.db_state.load();
        let Some(expected_range) = expected_range else {
            return Some(tree_idx);
        };
        if snapshot
            .multi_lsm_version
            .bucket_range_of_tree(tree_idx)
            .as_ref()
            == Some(&expected_range)
        {
            return Some(tree_idx);
        }
        snapshot
            .multi_lsm_version
            .tree_index_for_exact_range(&expected_range)
            .or_else(|| {
                snapshot
                    .multi_lsm_version
                    .tree_index_for_bucket(*expected_range.start())
            })
    }

    #[cfg(test)]
    pub(crate) fn on_compaction_started(&self, tree_idx: usize) {
        let mut state = self.state.lock().unwrap();
        let expected_range = self
            .db_state
            .load()
            .multi_lsm_version
            .bucket_range_of_tree(tree_idx);
        state.pending_compaction.insert(tree_idx, expected_range);
    }

    pub(crate) fn ttl_provider(&self) -> Arc<crate::ttl::TTLProvider> {
        Arc::clone(&self.ttl_provider)
    }

    fn maybe_trigger_compaction_locked(&self, state: &mut LSMTreeState, tree_idx: usize) {
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
        let expected_range = levels_snapshot
            .multi_lsm_version
            .bucket_range_of_tree(tree_idx);
        let plan = state
            .compaction_policy
            .pick(&tree_version.levels, state.compaction_config);
        let Some(plan) = plan else {
            return;
        };
        if plan.trivial_move {
            if let Some(edit) = self.build_trivial_move_edit(&tree_version.levels, &plan) {
                debug!(
                    "compaction trivial move tree={} L{}->L{} file_id={}",
                    tree_idx, plan.input_level, plan.output_level, plan.base_file_id
                );
                self.apply_edit_locked(state, vec![(tree_idx, edit)], |_db_state| {});
            }
            return;
        }
        debug!("trigger compaction plan tree={} {}", tree_idx, plan);
        let runs = build_runs_for_plan(&tree_version.levels, &plan, &state.compaction_config);
        if let Some(handle) = worker.submit_runs(
            tree_idx,
            runs,
            plan.output_level,
            crate::data_file::DataFileType::SSTable,
            self.ttl_provider(),
        ) {
            state.pending_compaction.insert(tree_idx, expected_range);
            std::mem::drop(handle);
        }
    }

    fn build_trivial_move_edit(
        &self,
        levels: &[Level],
        plan: &CompactionPlan,
    ) -> Option<VersionEdit> {
        let file = levels
            .iter()
            .find(|level| level.ordinal == plan.input_level)
            .and_then(|level| {
                level
                    .files
                    .iter()
                    .find(|file| file.file_id == plan.base_file_id)
            })
            .cloned()?;
        Some(VersionEdit {
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
        })
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
    ) -> Result<Vec<Value>> {
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
    ) -> Result<Vec<Value>> {
        let Some(version) = snapshot.multi_lsm_version.version_for_bucket(bucket) else {
            return Ok(Vec::new());
        };
        self.get_with_levels(
            file_manager,
            version.as_ref().levels.as_slice(),
            encoded_key,
            target_schema,
            schema_manager,
            selected_columns,
            selected_mask,
            terminal_mask,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn get_with_levels(
        &self,
        file_manager: &Arc<FileManager>,
        levels: &[Level],
        encoded_key: &[u8],
        target_schema: &Schema,
        schema_manager: &SchemaManager,
        selected_columns: Option<&[usize]>,
        selected_mask: Option<&[u8]>,
        mut terminal_mask: Option<&mut [u8]>,
    ) -> Result<Vec<Value>> {
        let num_columns = target_schema.num_columns();
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

        for level in levels.iter() {
            if level.tiered {
                for file in level.files.iter().rev() {
                    let should_continue = self.get_values_in_one_file(
                        file,
                        file_manager,
                        encoded_key,
                        target_schema,
                        schema_manager,
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
                    let should_continue = self.get_values_in_one_file(
                        file,
                        file_manager,
                        encoded_key,
                        target_schema,
                        schema_manager,
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

    pub(crate) fn scan_with_snapshot(
        &self,
        file_manager: &Arc<FileManager>,
        snapshot: Arc<DbState>,
        target_schema: Arc<Schema>,
        schema_manager: Arc<SchemaManager>,
        read_ahead_bytes: usize,
    ) -> Result<Vec<DynKvIterator>> {
        let mut iterators: Vec<DynKvIterator> = Vec::new();
        let use_read_ahead = read_ahead_bytes > 0 && tokio::runtime::Handle::try_current().is_ok();
        let mut runs: Vec<SortedRun> = Vec::new();
        let primary_version = snapshot.multi_lsm_version.version_of_index(0);
        for level in &primary_version.levels {
            if level.files.is_empty() {
                continue;
            }
            if level.tiered {
                for file in level.files.iter().rev() {
                    runs.push(SortedRun::new(level.ordinal, vec![Arc::clone(file)]));
                }
            } else {
                runs.push(SortedRun::new(level.ordinal, level.files.clone()));
            }
        }
        for run in runs {
            let file_manager = Arc::clone(file_manager);
            let block_cache = self.block_cache.clone();
            let sst_metrics = Arc::clone(&self.sst_metrics);
            let target_schema = Arc::clone(&target_schema);
            let schema_manager = Arc::clone(&schema_manager);
            let run_iter = run.iter(move |file| {
                let source_schema = schema_manager.schema(file.schema_id)?;
                let reader = file_manager.open_data_file_reader(file.file_id)?;
                let reader: Box<dyn crate::file::RandomAccessFile> = if use_read_ahead {
                    Box::new(ReadAheadBufferedReader::new(reader, read_ahead_bytes))
                } else {
                    Box::new(reader)
                };
                let sst_options = SSTIteratorOptions {
                    metrics: Some(Arc::clone(&sst_metrics)),
                    num_columns: source_schema.num_columns(),
                    bloom_filter_enabled: true,
                    ..SSTIteratorOptions::default()
                };
                let iter = SSTIterator::with_cache_and_file(
                    reader,
                    file,
                    sst_options,
                    block_cache.clone(),
                )?;
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
                    ))
                };
                if file.vlog_file_seq_offset == 0 {
                    Ok(iter)
                } else {
                    Ok(Box::new(VlogSeqOffsetIterator::new(
                        iter,
                        target_schema.num_columns(),
                        file.vlog_file_seq_offset,
                    )))
                }
            });
            iterators.push(Box::new(run_iter));
        }
        Ok(iterators)
    }

    /// Get values from one data file for the given encoded key.
    /// Returns Ok(true) if the caller should continue to the next file,
    /// or Ok(false) if the caller should stop.
    #[allow(clippy::too_many_arguments)]
    fn get_values_in_one_file(
        &self,
        file: &Arc<DataFile>,
        file_manager: &Arc<FileManager>,
        encoded_key: &[u8],
        target_schema: &Schema,
        schema_manager: &SchemaManager,
        selected_columns: Option<&[usize]>,
        selected_mask: Option<&[u8]>,
        mut terminal_mask: Option<&mut [u8]>,
        decode_mask: &mut [u8],
        out_values: &mut Vec<Value>,
    ) -> Result<bool> {
        let num_columns = target_schema.num_columns();
        let target_schema_id = target_schema.version();
        let source_schema = schema_manager.schema(file.schema_id)?;
        let source_num_columns = source_schema.num_columns();
        let mask_size = decode_mask.len();
        if let Some(bucket) = key_bucket(encoded_key)
            && !file.effective_bucket_range.contains(&bucket)
        {
            return Ok(true);
        }
        let reader = file_manager.open_data_file_reader(file.file_id)?;
        let mut iter = SSTIterator::with_cache_and_file(
            Box::new(reader),
            file.as_ref(),
            SSTIteratorOptions {
                num_columns: source_num_columns,
                metrics: Some(Arc::clone(&self.sst_metrics)),
                bloom_filter_enabled: true,
                ..SSTIteratorOptions::default()
            },
            self.block_cache.clone(),
        )?;
        if iter.may_contain(encoded_key)? {
            iter.seek(encoded_key)?;
        } else {
            return Ok(true);
        }
        if iter.valid()
            && let Some(current_key) = iter.key()?
            && current_key.as_ref() == encoded_key
            && let Some(value_bytes) = iter.value()?
        {
            let value = if file.schema_id == target_schema_id {
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
                let value = schema_manager.evolve_value(value, file.schema_id, target_schema_id)?;
                if let Some(mask) = terminal_mask.as_deref_mut() {
                    let evolved_mask = value.terminal_mask();
                    for (idx, mask_byte) in mask.iter_mut().enumerate().take(mask_size) {
                        *mask_byte |= evolved_mask.get(idx).copied().unwrap_or(0);
                    }
                }
                value
            };
            if let (Some(mask), Some(selected_mask)) = (terminal_mask.as_deref_mut(), selected_mask)
            {
                for (idx, mask_byte) in mask.iter_mut().enumerate().take(mask_size) {
                    *mask_byte &= selected_mask[idx];
                }
            }
            let value = apply_vlog_offset_to_value(value, file.vlog_file_seq_offset)?;
            let value = if let Some(columns) = selected_columns {
                value.select_columns(columns)
            } else {
                value
            };
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
            out_values.push(value);
            return Ok(!should_stop);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_file::DataFileType;
    use crate::db_state::{DbState, DbStateHandle, MultiLSMTreeVersion};
    use crate::file::{FileId, FileManager, FileSystemRegistry, TrackedFileId};
    use crate::sst::row_codec::{encode_key, encode_value};
    use crate::sst::{SSTWriter, SSTWriterOptions};
    use crate::r#type::{Column, Key, Value, ValueType};
    use crate::vlog::VlogVersion;
    use std::sync::Mutex;

    static mut FILE_ID_COUNTER: FileId = 0;

    fn create_data_file(start: &[u8], end: &[u8]) -> Arc<DataFile> {
        unsafe {
            let id = FILE_ID_COUNTER;
            FILE_ID_COUNTER += 1;
            let bucket_range = DataFile::bucket_range_from_keys(start, end);
            Arc::new(DataFile {
                file_type: DataFileType::SSTable,
                start_key: start.to_vec(),
                end_key: end.to_vec(),
                file_id: id,
                tracked_id: TrackedFileId::detached(id),
                schema_id: 0,
                size: 0, // Test file, size doesn't matter
                bucket_range: bucket_range.clone(),
                effective_bucket_range: bucket_range,
                vlog_file_seq_offset: 0,
                has_separated_values: false,
                meta_bytes: Default::default(),
            })
        }
    }

    fn create_data_file_with_size(start: &[u8], end: &[u8], size: usize) -> Arc<DataFile> {
        unsafe {
            let id = FILE_ID_COUNTER;
            FILE_ID_COUNTER += 1;
            let bucket_range = DataFile::bucket_range_from_keys(start, end);
            Arc::new(DataFile {
                file_type: DataFileType::SSTable,
                start_key: start.to_vec(),
                end_key: end.to_vec(),
                file_id: id,
                tracked_id: TrackedFileId::detached(id),
                schema_id: 0,
                size,
                bucket_range: bucket_range.clone(),
                effective_bucket_range: bucket_range,
                vlog_file_seq_offset: 0,
                has_separated_values: false,
                meta_bytes: Default::default(),
            })
        }
    }

    fn create_data_file_with_bucket(bucket: u16, size: usize) -> Arc<DataFile> {
        let start_key = encode_key(&Key::new(bucket, b"a".to_vec())).to_vec();
        let end_key = encode_key(&Key::new(bucket, b"z".to_vec())).to_vec();
        create_data_file_with_size(start_key.as_slice(), end_key.as_slice(), size)
    }

    fn cleanup_test_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn create_test_sst(
        file_manager: &Arc<FileManager>,
        _seq: u64,
        entries: Vec<(&[u8], &[u8])>,
    ) -> Result<Arc<DataFile>> {
        create_test_sst_in_bucket(file_manager, 0, entries)
    }

    fn create_test_sst_in_bucket(
        file_manager: &Arc<FileManager>,
        bucket: u16,
        entries: Vec<(&[u8], &[u8])>,
    ) -> Result<Arc<DataFile>> {
        let (file_id, writer_file) = file_manager.create_data_file()?;
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                num_columns: 1,
                bloom_filter_enabled: true,
                bloom_bits_per_key: 10,
                partitioned_index: false,
                ..SSTWriterOptions::default()
            },
        );
        for (key, value) in entries {
            let encoded_key = encode_key(&Key::new(bucket, key.to_vec()));
            writer.add(encoded_key.as_ref(), value)?;
        }
        let (first_key, last_key, file_size, footer_bytes) = writer.finish_with_range()?;
        let bucket_range = DataFile::bucket_range_from_keys(&first_key, &last_key);
        let data_file = DataFile {
            file_type: DataFileType::SSTable,
            start_key: first_key,
            end_key: last_key,
            file_id,
            tracked_id: TrackedFileId::new(file_manager, file_id),
            schema_id: 0,
            size: file_size,
            bucket_range: bucket_range.clone(),
            effective_bucket_range: bucket_range,
            vlog_file_seq_offset: 0,
            has_separated_values: false,
            meta_bytes: Default::default(),
        };
        data_file.set_meta_bytes(footer_bytes);
        Ok(Arc::new(data_file))
    }

    fn make_value_bytes(data: &[u8], num_columns: usize) -> Vec<u8> {
        let value = Value::new(vec![Some(Column::new(ValueType::Put, data.to_vec()))]);
        encode_value(&value, num_columns).to_vec()
    }

    #[derive(Default)]
    struct RecordingCompactionWorker {
        submitted_tree_idxs: Mutex<Vec<usize>>,
    }

    impl CompactionWorker for RecordingCompactionWorker {
        fn submit_runs(
            &self,
            lsm_tree_idx: usize,
            _sorted_runs: Vec<SortedRun>,
            _output_level: u8,
            _data_file_type: DataFileType,
            _ttl_provider: Arc<TTLProvider>,
        ) -> Option<tokio::task::JoinHandle<Result<crate::compaction::CompactionResult>>> {
            self.submitted_tree_idxs.lock().unwrap().push(lsm_tree_idx);
            None
        }

        fn shutdown(&self) {}
    }

    #[test]
    fn test_lsm_tree_apply_edit() {
        let db_state = Arc::new(DbStateHandle::new());
        let lsm_version = LSMTreeVersion {
            levels: vec![
                Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![create_data_file(b"a", b"b"), create_data_file(b"c", b"d")],
                },
                Level {
                    ordinal: 1,
                    tiered: false,
                    files: vec![create_data_file(b"e", b"f"), create_data_file(b"g", b"h")],
                },
            ],
        };
        db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version: MultiLSMTreeVersion::new(lsm_version),
            vlog_version: VlogVersion::new(),
            active: None,
            immutables: Vec::new().into(),
            suggested_base_snapshot_id: None,
        });
        let metrics_manager = Arc::new(MetricsManager::new("lsm-test".to_string()));
        let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);

        // Create a version edit to remove one file from level 0 and add two new files
        let current_version = db_state.load().multi_lsm_version.version_of_index(0);
        let edit = VersionEdit {
            level_edits: vec![
                LevelEdit {
                    level: 0,
                    removed_files: vec![current_version.as_ref().levels[0].files[0].clone()],
                    new_files: vec![
                        create_data_file(b"a1", b"a2"),
                        create_data_file(b"b1", b"b2"),
                    ],
                },
                LevelEdit {
                    level: 1,
                    removed_files: vec![],
                    new_files: vec![create_data_file(b"d1", b"d2")],
                },
            ],
        };

        lsm_tree.apply_edit(0, edit, None);

        // Verify the new version
        let version = db_state.load().multi_lsm_version.version_of_index(0);
        assert_eq!(version.as_ref().levels.len(), 2);

        let level0 = &version.as_ref().levels[0];
        assert_eq!(level0.ordinal, 0);
        assert_eq!(level0.files.len(), 3);
        assert_eq!(level0.files[0].start_key, b"a1");
        assert_eq!(level0.files[1].start_key, b"b1");
        assert_eq!(level0.files[2].start_key, b"c");

        let level1 = &version.as_ref().levels[1];
        assert_eq!(level1.ordinal, 1);
        assert_eq!(level1.files.len(), 3);
        assert_eq!(level1.files[0].start_key, b"d1");
        assert_eq!(level1.files[1].start_key, b"e");
        assert_eq!(level1.files[2].start_key, b"g");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_lsm_edit_removes_data_file() {
        let root = "/tmp/lsm_edit_remove_file";
        cleanup_test_root(root);
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", root))
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("lsm-test".to_string()));
        let file_manager =
            Arc::new(FileManager::with_defaults(fs.clone(), Arc::clone(&metrics_manager)).unwrap());
        let num_columns = 1;
        let to_remove = create_test_sst(
            &file_manager,
            1,
            vec![(b"k1", &make_value_bytes(b"value", num_columns))],
        )
        .unwrap();
        let file_id = to_remove.file_id;
        let path = file_manager.get_data_file_path(file_id).unwrap();
        assert!(fs.exists(&path).unwrap());

        let db_state = Arc::new(DbStateHandle::new());
        let lsm_version = LSMTreeVersion {
            levels: vec![Level {
                ordinal: 0,
                tiered: true,
                files: vec![Arc::clone(&to_remove)],
            }],
        };
        db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version: MultiLSMTreeVersion::new(lsm_version),
            vlog_version: VlogVersion::new(),
            active: None,
            immutables: Vec::new().into(),
            suggested_base_snapshot_id: None,
        });
        let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
        lsm_tree.apply_edit(
            0,
            VersionEdit {
                level_edits: vec![LevelEdit {
                    level: 0,
                    removed_files: vec![Arc::clone(&to_remove)],
                    new_files: Vec::new(),
                }],
            },
            None,
        );
        assert!(lsm_tree.level_files(0).is_empty());
        drop(to_remove);

        crate::file::test_utils::wait_for_file_deletion(&fs, &path);
        for _ in 0..50 {
            if !fs.exists(&path).unwrap() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(!fs.exists(&path).unwrap());
        cleanup_test_root(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_lsm_trivial_move_compaction() {
        let root = "/tmp/lsm_trivial_move";
        cleanup_test_root(root);
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", root))
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("lsm-compaction-test".to_string()));
        let file_manager =
            Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
        let config = crate::compaction::CompactionConfig {
            l1_base_bytes: 1,
            level_size_multiplier: 1,
            max_level: 3,
            bloom_filter_enabled: true,
            bloom_bits_per_key: 10,
            partitioned_index: false,
            ..crate::compaction::CompactionConfig::default()
        };
        let db_config = crate::Config::default();
        let db_state = Arc::new(DbStateHandle::new());
        let lsm_version = LSMTreeVersion {
            levels: vec![
                Level {
                    ordinal: 0,
                    tiered: true,
                    files: Vec::new(),
                },
                Level {
                    ordinal: 1,
                    tiered: false,
                    files: vec![
                        create_data_file_with_size(b"a", b"b", 10),
                        create_data_file_with_size(b"c", b"d", 10),
                    ],
                },
                Level {
                    ordinal: 2,
                    tiered: false,
                    files: vec![create_data_file_with_size(b"e", b"f", 1)],
                },
            ],
        };
        db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version: MultiLSMTreeVersion::new(lsm_version),
            vlog_version: VlogVersion::new(),
            active: None,
            immutables: Vec::new().into(),
            suggested_base_snapshot_id: None,
        });
        let lsm_tree = Arc::new(LSMTree::with_state(
            Arc::clone(&db_state),
            Arc::clone(&metrics_manager),
        ));
        let worker: Arc<dyn crate::compaction::CompactionWorker> =
            Arc::new(crate::compaction::LocalCompactionWorker::new(
                crate::compaction::CompactionExecutor::new(config).unwrap(),
                Arc::clone(&file_manager),
                Arc::downgrade(&lsm_tree),
                db_config,
                Arc::clone(&metrics_manager),
                Arc::new(crate::schema::SchemaManager::new(1)),
            ));
        lsm_tree.configure_compaction(config, Some(Arc::clone(&worker)));
        let target = lsm_tree
            .db_state
            .load()
            .multi_lsm_version
            .version_of_index(0)
            .levels
            .iter()
            .find(|level| level.ordinal == 1)
            .and_then(|level| level.files.iter().find(|file| file.start_key == b"a"))
            .cloned()
            .expect("target file");
        lsm_tree.apply_edit(
            0,
            VersionEdit {
                level_edits: vec![LevelEdit {
                    level: 1,
                    removed_files: vec![target],
                    new_files: Vec::new(),
                }],
            },
            None,
        );
        let level1 = lsm_tree.level_files(1);
        let level2 = lsm_tree.level_files(2);
        assert_eq!(level1.len(), 0);
        assert_eq!(level2.len(), 1);
        assert!(level2.iter().any(|file| file.start_key == b"e"));
        cleanup_test_root(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_lsm_get_in_bucket_routes_to_bucket_tree_state() {
        let root = "/tmp/lsm_get_in_bucket_routes";
        cleanup_test_root(root);
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", root))
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("lsm-test".to_string()));
        let file_manager =
            Arc::new(FileManager::with_defaults(fs.clone(), Arc::clone(&metrics_manager)).unwrap());
        let num_columns = 1;
        let file_bucket0 = create_test_sst_in_bucket(
            &file_manager,
            0,
            vec![(b"k", &make_value_bytes(b"v0", num_columns))],
        )
        .unwrap();
        let file_bucket1 = create_test_sst_in_bucket(
            &file_manager,
            1,
            vec![(b"k", &make_value_bytes(b"v1", num_columns))],
        )
        .unwrap();

        let db_state = Arc::new(DbStateHandle::new());
        let multi_lsm_version = MultiLSMTreeVersion::from_parts(
            2,
            vec![0u32, 1u32],
            vec![
                Arc::new(LSMTreeVersion {
                    levels: vec![Level {
                        ordinal: 0,
                        tiered: true,
                        files: vec![Arc::clone(&file_bucket0)],
                    }],
                }),
                Arc::new(LSMTreeVersion {
                    levels: vec![Level {
                        ordinal: 0,
                        tiered: true,
                        files: vec![Arc::clone(&file_bucket1)],
                    }],
                }),
            ],
        );
        db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version,
            vlog_version: VlogVersion::new(),
            active: None,
            immutables: Vec::new().into(),
            suggested_base_snapshot_id: None,
        });
        let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);

        let schema_manager = SchemaManager::new(1);
        let schema = schema_manager.latest_schema();
        let encoded_bucket0 = encode_key(&Key::new(0, b"k".to_vec()));
        let encoded_bucket1 = encode_key(&Key::new(1, b"k".to_vec()));
        let bucket0_values = lsm_tree
            .get(
                &file_manager,
                0,
                encoded_bucket0.as_ref(),
                schema.as_ref(),
                &schema_manager,
                None,
                None,
                None,
            )
            .unwrap();
        let bucket1_values = lsm_tree
            .get(
                &file_manager,
                1,
                encoded_bucket1.as_ref(),
                schema.as_ref(),
                &schema_manager,
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(bucket0_values.len(), 1);
        assert_eq!(bucket1_values.len(), 1);
        assert_eq!(
            bucket0_values[0].columns()[0].as_ref().unwrap().data(),
            b"v0".as_slice()
        );
        assert_eq!(
            bucket1_values[0].columns()[0].as_ref().unwrap().data(),
            b"v1".as_slice()
        );
        let unknown_bucket_values = lsm_tree
            .get(
                &file_manager,
                3,
                encoded_bucket0.as_ref(),
                schema.as_ref(),
                &schema_manager,
                None,
                None,
                None,
            )
            .unwrap();
        assert!(unknown_bucket_values.is_empty());
        cleanup_test_root(root);
    }

    #[test]
    fn test_lsm_compaction_submits_only_changed_tree() {
        let db_state = Arc::new(DbStateHandle::new());
        let multi_lsm_version = MultiLSMTreeVersion::from_parts(
            2,
            vec![0u32, 1u32],
            vec![
                Arc::new(LSMTreeVersion {
                    levels: vec![
                        Level {
                            ordinal: 0,
                            tiered: true,
                            files: vec![create_data_file_with_size(b"a", b"b", 1)],
                        },
                        Level {
                            ordinal: 1,
                            tiered: false,
                            files: Vec::new(),
                        },
                    ],
                }),
                Arc::new(LSMTreeVersion {
                    levels: vec![
                        Level {
                            ordinal: 0,
                            tiered: true,
                            files: vec![create_data_file_with_size(b"c", b"d", 1)],
                        },
                        Level {
                            ordinal: 1,
                            tiered: false,
                            files: Vec::new(),
                        },
                    ],
                }),
            ],
        );
        db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version,
            vlog_version: VlogVersion::new(),
            active: None,
            immutables: Vec::new().into(),
            suggested_base_snapshot_id: None,
        });
        let metrics_manager = Arc::new(MetricsManager::new("lsm-test".to_string()));
        let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
        let worker = Arc::new(RecordingCompactionWorker::default());
        let worker_dyn: Arc<dyn CompactionWorker> = worker.clone();
        let mut config = crate::compaction::CompactionConfig::default();
        config.l0_file_limit = 0;
        lsm_tree.configure_compaction(config, Some(worker_dyn));
        lsm_tree.apply_edit(
            0,
            VersionEdit {
                level_edits: Vec::new(),
            },
            None,
        );
        let mut submitted = worker.submitted_tree_idxs.lock().unwrap().clone();
        submitted.sort_unstable();
        assert_eq!(submitted, vec![0]);
    }

    #[test]
    fn test_lsm_auto_split_rewrites_tree_ranges() {
        let db_state = Arc::new(DbStateHandle::new());
        let initial_version = Arc::new(LSMTreeVersion {
            levels: vec![
                Level {
                    ordinal: 0,
                    tiered: true,
                    files: Vec::new(),
                },
                Level {
                    ordinal: 1,
                    tiered: false,
                    files: vec![
                        create_data_file_with_bucket(0, 10),
                        create_data_file_with_bucket(1, 10),
                        create_data_file_with_bucket(2, 10),
                        create_data_file_with_bucket(3, 10),
                    ],
                },
            ],
        });
        let multi_lsm_version = MultiLSMTreeVersion::from_bucket_ranges_with_tree_versions(
            4,
            &[0u16..=3u16],
            vec![initial_version],
        )
        .unwrap();
        db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version,
            vlog_version: VlogVersion::new(),
            active: None,
            immutables: Vec::new().into(),
            suggested_base_snapshot_id: None,
        });
        let metrics_manager = Arc::new(MetricsManager::new("lsm-test".to_string()));
        let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
        let worker = Arc::new(RecordingCompactionWorker::default());
        let worker_dyn: Arc<dyn CompactionWorker> = worker.clone();
        let mut config = crate::compaction::CompactionConfig::default();
        config.l1_base_bytes = 10;
        config.level_size_multiplier = 1;
        config.split_trigger_level = Some(1);
        lsm_tree.configure_compaction(config, Some(worker_dyn));

        lsm_tree.apply_edit(
            0,
            VersionEdit {
                level_edits: Vec::new(),
            },
            None,
        );

        let snapshot = db_state.load();
        assert_eq!(snapshot.multi_lsm_version.tree_count(), 4);
        for bucket in 0..4u16 {
            let tree_idx = snapshot
                .multi_lsm_version
                .tree_index_for_bucket(bucket)
                .expect("tree idx for bucket");
            let range = snapshot
                .multi_lsm_version
                .bucket_range_of_tree(tree_idx)
                .expect("bucket range for tree");
            assert_eq!(range, bucket..=bucket);
            let level1 = snapshot.multi_lsm_version.version_of_index(tree_idx);
            let level1_files = level1
                .levels
                .iter()
                .find(|level| level.ordinal == 1)
                .map(|level| level.files.clone())
                .unwrap_or_default();
            assert_eq!(level1_files.len(), 1);
            assert_eq!(
                u16::from_le_bytes([level1_files[0].start_key[0], level1_files[0].start_key[1]]),
                bucket
            );
        }
    }

    #[test]
    fn test_lsm_auto_split_skips_l0_trigger_level() {
        let db_state = Arc::new(DbStateHandle::new());
        let initial_version = Arc::new(LSMTreeVersion {
            levels: vec![
                Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![
                        create_data_file_with_bucket(0, 10),
                        create_data_file_with_bucket(1, 10),
                        create_data_file_with_bucket(2, 10),
                        create_data_file_with_bucket(3, 10),
                    ],
                },
                Level {
                    ordinal: 1,
                    tiered: false,
                    files: Vec::new(),
                },
            ],
        });
        let multi_lsm_version = MultiLSMTreeVersion::from_bucket_ranges_with_tree_versions(
            4,
            &[0u16..=3u16],
            vec![initial_version],
        )
        .unwrap();
        db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version,
            vlog_version: VlogVersion::new(),
            active: None,
            immutables: Vec::new().into(),
            suggested_base_snapshot_id: None,
        });
        let metrics_manager = Arc::new(MetricsManager::new("lsm-test".to_string()));
        let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
        let worker = Arc::new(RecordingCompactionWorker::default());
        let worker_dyn: Arc<dyn CompactionWorker> = worker.clone();
        let mut config = crate::compaction::CompactionConfig::default();
        config.split_trigger_level = Some(0);
        lsm_tree.configure_compaction(config, Some(worker_dyn));
        lsm_tree.apply_edit(
            0,
            VersionEdit {
                level_edits: Vec::new(),
            },
            None,
        );
        assert_eq!(db_state.load().multi_lsm_version.tree_count(), 1);
    }

    #[test]
    fn test_lsm_compaction_completion_remaps_tree_index_by_range() {
        let db_state = Arc::new(DbStateHandle::new());
        let base_version = Arc::new(LSMTreeVersion {
            levels: vec![Level {
                ordinal: 0,
                tiered: true,
                files: vec![create_data_file_with_bucket(2, 8)],
            }],
        });
        let initial_multi = MultiLSMTreeVersion::from_bucket_ranges_with_tree_versions(
            4,
            &[0u16..=1u16, 2u16..=3u16],
            vec![Arc::clone(&base_version), Arc::clone(&base_version)],
        )
        .unwrap();
        db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version: initial_multi,
            vlog_version: VlogVersion::new(),
            active: None,
            immutables: Vec::new().into(),
            suggested_base_snapshot_id: None,
        });
        let metrics_manager = Arc::new(MetricsManager::new("lsm-test".to_string()));
        let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
        lsm_tree.on_compaction_started(1);

        let shifted_multi = MultiLSMTreeVersion::from_bucket_ranges_with_tree_versions(
            4,
            &[0u16..=0u16, 1u16..=1u16, 2u16..=3u16],
            vec![
                Arc::clone(&base_version),
                Arc::clone(&base_version),
                Arc::clone(&base_version),
            ],
        )
        .unwrap();
        db_state.store(DbState {
            seq_id: 1,
            bucket_ranges: Vec::new(),
            multi_lsm_version: shifted_multi,
            vlog_version: VlogVersion::new(),
            active: None,
            immutables: Vec::new().into(),
            suggested_base_snapshot_id: None,
        });

        let remapped_idx = lsm_tree.on_compaction_complete(1);
        assert_eq!(remapped_idx, Some(2));
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_lsm_get_tiered_returns_newest_first() {
        let root = "/tmp/lsm_get_tiered_order";
        cleanup_test_root(root);
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(format!("file://{}", root))
            .unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("lsm-test".to_string()));
        let file_manager =
            Arc::new(FileManager::with_defaults(fs, Arc::clone(&metrics_manager)).unwrap());
        let num_columns = 1;
        let older = create_test_sst(
            &file_manager,
            1,
            vec![(b"k1", &make_value_bytes(b"old", num_columns))],
        )
        .unwrap();
        let newer = create_test_sst(
            &file_manager,
            3,
            vec![(b"k1", &make_value_bytes(b"new", num_columns))],
        )
        .unwrap();
        let db_state = Arc::new(DbStateHandle::new());
        let lsm_version = LSMTreeVersion {
            levels: vec![Level {
                ordinal: 0,
                tiered: true,
                files: vec![older, newer],
            }],
        };
        db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version: MultiLSMTreeVersion::new(lsm_version),
            vlog_version: VlogVersion::new(),
            active: None,
            immutables: Vec::new().into(),
            suggested_base_snapshot_id: None,
        });
        let lsm_tree = LSMTree::with_state(Arc::clone(&db_state), metrics_manager);
        let schema_manager = Arc::new(crate::schema::SchemaManager::new(num_columns));
        let schema = schema_manager.latest_schema();
        let encoded_key = encode_key(&crate::r#type::Key::new(0, b"k1".to_vec()));
        let value = lsm_tree
            .get(
                &file_manager,
                0,
                encoded_key.as_ref(),
                schema.as_ref(),
                schema_manager.as_ref(),
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(value.len(), 2);
        assert_eq!(
            value[0].columns()[0].as_ref().unwrap().data().as_ref(),
            b"new"
        );
        assert_eq!(
            value[1].columns()[0].as_ref().unwrap().data().as_ref(),
            b"old"
        );
        cleanup_test_root(root);
    }
}
