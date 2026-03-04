use crate::compaction::{
    CompactionConfig, CompactionPlan, CompactionPolicy, CompactionWorker, MinOverlapPolicy,
    RoundRobinPolicy, build_runs_for_plan,
};
use crate::data_file::DataFile;
use crate::error::Result;
use crate::file::{FileManager, ReadAheadBufferedReader};
use crate::iterator::{KvIterator, SchemaEvolvingIterator, SortedRun};
use crate::metrics_manager::MetricsManager;
use crate::schema::{Schema, SchemaManager};
use crate::sst::block_cache::BlockCache;
use crate::sst::row_codec::{decode_value, decode_value_masked};
use crate::sst::{SSTIterator, SSTIteratorMetrics, SSTIteratorOptions};
use crate::r#type::Value;
use log::debug;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::db_state::{DbState, DbStateHandle};
use crate::ttl::TTLProvider;
use crate::vlog::VlogEdit;

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
    pending_compaction: bool,
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
                pending_compaction: false,
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
        self.maybe_trigger_compaction_locked(state);
        snapshot
    }

    pub(crate) fn add_level0_files(
        &self,
        to_remove_memtable_seq: u64,
        files_by_tree: Vec<(usize, Arc<DataFile>)>,
        vlog_edit: Option<VlogEdit>,
    ) -> Arc<DbState> {
        if files_by_tree.is_empty() {
            panic!("cannot add empty new files");
        }
        let mut grouped: BTreeMap<usize, Vec<Arc<DataFile>>> = BTreeMap::new();
        for (tree_idx, file) in files_by_tree {
            grouped.entry(tree_idx).or_default().push(file);
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
        let mut state = self.state.lock().unwrap();
        self.apply_edit_locked(&mut state, edits, move |db_state| {
            db_state
                .immutables
                .retain(|imm| imm.seq != to_remove_memtable_seq);
            if let Some(edit) = vlog_edit {
                db_state.vlog_version = db_state.vlog_version.apply_edit(edit);
            }
        })
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
        state.pending_compaction = false;
    }

    fn make_policy(kind: crate::config::CompactionPolicyKind) -> Box<dyn CompactionPolicy> {
        match kind {
            crate::config::CompactionPolicyKind::RoundRobin => Box::new(RoundRobinPolicy::new()),
            crate::config::CompactionPolicyKind::MinOverlap => Box::new(MinOverlapPolicy::new()),
        }
    }

    pub(crate) fn on_compaction_complete(&self) {
        let mut state = self.state.lock().unwrap();
        state.pending_compaction = false;
    }

    pub(crate) fn on_compaction_started(&self) {
        let mut state = self.state.lock().unwrap();
        state.pending_compaction = true;
    }

    pub(crate) fn ttl_provider(&self) -> Arc<crate::ttl::TTLProvider> {
        Arc::clone(&self.ttl_provider)
    }

    fn maybe_trigger_compaction_locked(&self, state: &mut LSMTreeState) {
        if state.pending_compaction {
            return;
        }
        let levels_snapshot = self.db_state.load();
        let primary_version = levels_snapshot.multi_lsm_version.version_of_index(0);
        let plan = state
            .compaction_policy
            .pick(&primary_version.levels, state.compaction_config);
        let Some(plan) = plan else {
            return;
        };
        let Some(worker) = state.compaction_worker.clone() else {
            return;
        };
        if plan.trivial_move {
            if let Some(edit) = self.build_trivial_move_edit(&primary_version.levels, &plan) {
                debug!(
                    "compaction trivial move L{}->L{} file_id={}",
                    plan.input_level, plan.output_level, plan.base_file_id
                );
                self.apply_edit_locked(state, vec![(0, edit)], |_db_state| {});
            }
            return;
        }
        debug!("trigger compaction plan {}", plan);
        let runs = build_runs_for_plan(&primary_version.levels, &plan, &state.compaction_config);
        if let Some(handle) = worker.submit_runs(
            runs,
            plan.output_level,
            crate::data_file::DataFileType::SSTable,
            self.ttl_provider(),
        ) {
            state.pending_compaction = true;
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
        max_seq: Option<u64>,
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
            max_seq,
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
        max_seq: Option<u64>,
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
            max_seq,
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
        max_seq: Option<u64>,
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
                        max_seq,
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
                        max_seq,
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
                let iter: DynKvIterator = if file.schema_id == target_schema.version() {
                    Box::new(iter)
                } else {
                    Box::new(SchemaEvolvingIterator::new(
                        iter,
                        Arc::clone(&source_schema),
                        Arc::clone(&target_schema),
                        Arc::clone(&schema_manager),
                    ))
                };
                Ok(iter)
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
        max_seq: Option<u64>,
        out_values: &mut Vec<Value>,
    ) -> Result<bool> {
        let num_columns = target_schema.num_columns();
        let target_schema_id = target_schema.version();
        let source_schema = schema_manager.schema(file.schema_id)?;
        let source_num_columns = source_schema.num_columns();
        let mask_size = decode_mask.len();
        if let Some(max_seq) = max_seq
            && file.seq >= max_seq
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

    static mut FILE_ID_COUNTER: FileId = 0;

    fn create_data_file(start: &[u8], end: &[u8]) -> Arc<DataFile> {
        unsafe {
            let id = FILE_ID_COUNTER;
            FILE_ID_COUNTER += 1;
            Arc::new(DataFile {
                file_type: DataFileType::SSTable,
                start_key: start.to_vec(),
                end_key: end.to_vec(),
                file_id: id,
                tracked_id: TrackedFileId::detached(id),
                seq: 0,
                schema_id: 0,
                size: 0, // Test file, size doesn't matter
                has_separated_values: false,
                meta_bytes: Default::default(),
            })
        }
    }

    fn create_data_file_with_size(start: &[u8], end: &[u8], size: usize) -> Arc<DataFile> {
        unsafe {
            let id = FILE_ID_COUNTER;
            FILE_ID_COUNTER += 1;
            Arc::new(DataFile {
                file_type: DataFileType::SSTable,
                start_key: start.to_vec(),
                end_key: end.to_vec(),
                file_id: id,
                tracked_id: TrackedFileId::detached(id),
                seq: 0,
                schema_id: 0,
                size,
                has_separated_values: false,
                meta_bytes: Default::default(),
            })
        }
    }

    fn cleanup_test_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn create_test_sst(
        file_manager: &Arc<FileManager>,
        seq: u64,
        entries: Vec<(&[u8], &[u8])>,
    ) -> Result<Arc<DataFile>> {
        create_test_sst_in_bucket(file_manager, seq, 0, entries)
    }

    fn create_test_sst_in_bucket(
        file_manager: &Arc<FileManager>,
        seq: u64,
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
        let data_file = DataFile {
            file_type: DataFileType::SSTable,
            start_key: first_key,
            end_key: last_key,
            file_id,
            tracked_id: TrackedFileId::new(file_manager, file_id),
            seq,
            schema_id: 0,
            size: file_size,
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
            1,
            0,
            vec![(b"k", &make_value_bytes(b"v0", num_columns))],
        )
        .unwrap();
        let file_bucket1 = create_test_sst_in_bucket(
            &file_manager,
            2,
            1,
            vec![(b"k", &make_value_bytes(b"v1", num_columns))],
        )
        .unwrap();

        let db_state = Arc::new(DbStateHandle::new());
        let multi_lsm_version = MultiLSMTreeVersion::from_parts(
            vec![0, 1],
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
                None,
            )
            .unwrap();
        assert!(unknown_bucket_values.is_empty());
        cleanup_test_root(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_lsm_get_respects_max_seq() {
        let root = "/tmp/lsm_get_max_seq";
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
                Some(2),
            )
            .unwrap();
        assert_eq!(value.len(), 1);
        assert_eq!(
            value[0].columns()[0].as_ref().unwrap().data().as_ref(),
            b"old"
        );
        cleanup_test_root(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_lsm_get_without_max_seq() {
        let root = "/tmp/lsm_get_no_max_seq";
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
