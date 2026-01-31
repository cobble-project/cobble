use crate::compaction::{
    CompactionConfig, CompactionPlan, CompactionPolicy, CompactionWorker, MinOverlapPolicy,
    RoundRobinPolicy, build_runs_for_plan,
};
use crate::data_file::DataFile;
use crate::error::Result;
use crate::file::FileManager;
use crate::sst::block_cache::BlockCache;
use crate::sst::row_codec::decode_value_masked;
use crate::sst::{SSTIterator, SSTIteratorOptions};
use crate::r#type::Value;
use log::debug;
use std::sync::{Arc, Mutex};

use crate::db_state::{DbState, DbStateHandle};
use crate::ttl::TTLProvider;

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
}

struct LSMTreeState {
    level_options: Vec<LevelOptions>,
    compaction_config: CompactionConfig,
    compaction_policy: Box<dyn CompactionPolicy>,
    pending_compaction: bool,
    compaction_worker: Option<Arc<CompactionWorker>>,
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

struct VersionEditSummary<'a>(&'a VersionEdit);

impl std::fmt::Display for VersionEditSummary<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "apply version edit [")?;
        for (idx, level_edit) in self.0.level_edits.iter().enumerate() {
            if idx > 0 {
                write!(f, "; ")?;
            }
            write!(
                f,
                "L{} -{} +{}",
                level_edit.level,
                level_edit.removed_files.len(),
                level_edit.new_files.len()
            )?;
            if !level_edit.removed_files.is_empty() {
                write!(f, " removed=")?;
                for (file_idx, file) in level_edit.removed_files.iter().enumerate() {
                    if file_idx > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{}", file.file_id)?;
                }
            }
            if !level_edit.new_files.is_empty() {
                write!(f, " new=")?;
                for (file_idx, file) in level_edit.new_files.iter().enumerate() {
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
        Self::with_state(Arc::new(DbStateHandle::new()))
    }
}

impl LSMTree {
    pub(crate) fn with_state(db_state: Arc<DbStateHandle>) -> Self {
        Self::with_state_and_ttl(db_state, Arc::new(TTLProvider::disabled()))
    }

    pub(crate) fn with_state_and_ttl(
        db_state: Arc<DbStateHandle>,
        ttl_provider: Arc<TTLProvider>,
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

    pub(crate) fn apply_edit(&self, edit: VersionEdit) {
        let mut state = self.state.lock().unwrap();
        self.apply_edit_locked(&mut state, edit, |_db_state| {});
    }

    fn apply_edit_locked(
        &self,
        state: &mut LSMTreeState,
        edit: VersionEdit,
        fix: impl Fn(&mut DbState),
    ) -> Arc<DbState> {
        let guard = self.db_state.lock();
        let snapshot = self.db_state.load();
        let mut new_levels = snapshot.lsm_version.levels.clone();

        for level_edit in &edit.level_edits {
            if let Some(level) = new_levels
                .iter_mut()
                .find(|l| l.ordinal == level_edit.level)
            {
                let mut insert_pos = Option::<usize>::None;
                // First, find the position of files to be removed, and remove the files.
                for file in &level_edit.removed_files {
                    if let Some(pos) = level.files.iter().position(|f| Arc::ptr_eq(f, file)) {
                        level.files.remove(pos);
                        if !level.tiered {
                            if let Some(previous) = insert_pos {
                                // Ensure that all removed files are contiguous.
                                assert_eq!(pos, previous);
                            } else {
                                insert_pos = Some(pos);
                            }
                        } else if insert_pos.is_none() {
                            insert_pos = Some(pos);
                        }
                    }
                }
                // Insert new files at the position of the first removed file
                if let Some(pos) = insert_pos {
                    for (i, new_file) in level_edit.new_files.iter().enumerate() {
                        level.files.insert(pos + i, Arc::clone(new_file));
                    }
                } else {
                    // If no files were removed.
                    //  if tiering, at the end if no files were removed.
                    //  else, determine by file key range.
                    if level.tiered {
                        level.files.extend(level_edit.new_files.clone());
                    } else {
                        // Determine position by key range
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
                }
            } else {
                // If the level does not exist, create it
                new_levels.push(Level {
                    ordinal: level_edit.level,
                    tiered: Self::get_level_option(state, level_edit.level).tiered,
                    files: level_edit.new_files.clone(),
                });
            }
        }

        self.db_state
            .cas_mutate(snapshot.seq_id, |db_state, snapshot| {
                let mut new_db_state = DbState::new(
                    db_state,
                    LSMTreeVersion {
                        levels: new_levels.clone(),
                    },
                    snapshot.active.clone(),
                    snapshot.immutables.clone(),
                );
                fix(&mut new_db_state);
                Some(new_db_state)
            });
        let snapshot = self.db_state.load();
        drop(guard);
        debug!(
            "{}. {}",
            VersionEditSummary(&edit),
            VersionSummary(&self.db_state.load().lsm_version)
        );
        self.maybe_trigger_compaction_locked(state);
        snapshot
    }

    pub(crate) fn add_level0_files(
        &self,
        to_remove_memtable_seq: u64,
        new_files: Vec<Arc<DataFile>>,
    ) -> Arc<DbState> {
        if new_files.is_empty() {
            panic!("cannot add empty new files");
        }
        let edit = VersionEdit {
            level_edits: vec![LevelEdit {
                level: 0,
                removed_files: Vec::new(),
                new_files,
            }],
        };
        let mut state = self.state.lock().unwrap();
        self.apply_edit_locked(&mut state, edit, |db_state| {
            db_state
                .immutables
                .retain(|imm| imm.seq != to_remove_memtable_seq);
        })
    }

    pub(crate) fn level_files(&self, level: u8) -> Vec<Arc<DataFile>> {
        self.db_state
            .load()
            .lsm_version
            .levels
            .iter()
            .find(|l| l.ordinal == level)
            .map(|l| l.files.clone())
            .unwrap_or_default()
    }

    pub(crate) fn configure_compaction(
        &self,
        config: CompactionConfig,
        worker: Option<Arc<CompactionWorker>>,
    ) {
        let mut state = self.state.lock().unwrap();
        state.compaction_config = config;
        state.compaction_policy = Self::make_policy(config.policy);
        state.compaction_worker = worker;
    }

    pub(crate) fn set_block_cache(&mut self, block_cache: Option<BlockCache>) {
        self.block_cache = block_cache;
    }

    pub(crate) fn shutdown_compaction(&self) {
        let mut state = self.state.lock().unwrap();
        if let Some(worker) = state.compaction_worker.take()
            && let Ok(worker) = Arc::try_unwrap(worker)
        {
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
        let plan = state
            .compaction_policy
            .pick(&levels_snapshot.lsm_version.levels, state.compaction_config);
        let Some(plan) = plan else {
            return;
        };
        let Some(worker) = state.compaction_worker.clone() else {
            return;
        };
        if plan.trivial_move {
            if let Some(edit) =
                self.build_trivial_move_edit(&levels_snapshot.lsm_version.levels, &plan)
            {
                debug!(
                    "compaction trivial move L{}->L{} file_id={}",
                    plan.input_level, plan.output_level, plan.base_file_id
                );
                self.apply_edit_locked(state, edit, |_db_state| {});
            }
            return;
        }
        debug!("trigger compaction plan {}", plan);
        let runs = build_runs_for_plan(&levels_snapshot.lsm_version.levels, &plan);
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

    pub(crate) fn get(
        &self,
        file_manager: &Arc<FileManager>,
        encoded_key: &[u8],
        num_columns: usize,
        terminal_mask: Option<&mut [u8]>,
        max_seq: Option<u64>,
    ) -> Result<Vec<Value>> {
        let snapshot = self.db_state.load();
        self.get_with_snapshot(
            file_manager,
            snapshot,
            encoded_key,
            num_columns,
            terminal_mask,
            max_seq,
        )
    }

    pub(crate) fn get_with_snapshot(
        &self,
        file_manager: &Arc<FileManager>,
        snapshot: Arc<DbState>,
        encoded_key: &[u8],
        num_columns: usize,
        mut terminal_mask: Option<&mut [u8]>,
        max_seq: Option<u64>,
    ) -> Result<Vec<Value>> {
        let mut values = Vec::new();
        let mask_size = num_columns.div_ceil(8);
        let mut decode_mask = vec![0u8; mask_size.max(1)];
        if num_columns == 1 {
            decode_mask[0] = 0x01;
            terminal_mask = None;
        } else if let Some(ref cols) = terminal_mask {
            for (idx, mask_byte) in cols.iter().enumerate().take(mask_size) {
                decode_mask[idx] = !*mask_byte;
            }
            if mask_size > 0 {
                let last_bits = (num_columns - 1) % 8 + 1;
                let last_mask = (1u8 << last_bits) - 1;
                decode_mask[mask_size - 1] &= last_mask;
            }
        } else {
            for mask in decode_mask.iter_mut().take(mask_size) {
                *mask = 0xFF;
            }
            if mask_size > 0 {
                let last_bits = (num_columns - 1) % 8 + 1;
                let last_mask = (1u8 << last_bits) - 1;
                decode_mask[mask_size - 1] = last_mask;
            }
        }

        for level in snapshot.lsm_version.levels.iter() {
            if level.tiered {
                for file in level.files.iter().rev() {
                    let should_continue = self.get_values_in_one_file(
                        file,
                        file_manager,
                        encoded_key,
                        num_columns,
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
                        num_columns,
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

    /// Get values from one data file for the given encoded key.
    /// Returns Ok(true) if the caller should continue to the next file,
    /// or Ok(false) if the caller should stop.
    #[allow(clippy::too_many_arguments)]
    fn get_values_in_one_file(
        &self,
        file: &Arc<DataFile>,
        file_manager: &Arc<FileManager>,
        encoded_key: &[u8],
        num_columns: usize,
        mut terminal_mask: Option<&mut [u8]>,
        decode_mask: &mut [u8],
        max_seq: Option<u64>,
        out_values: &mut Vec<Value>,
    ) -> Result<bool> {
        let mask_size = decode_mask.len();
        if let Some(max_seq) = max_seq
            && file.seq >= max_seq
        {
            return Ok(true);
        }
        let reader = file_manager.open_data_file_reader(file.file_id)?;
        let mut iter = SSTIterator::with_cache(
            Box::new(reader),
            file.file_id,
            SSTIteratorOptions {
                num_columns,
                ..SSTIteratorOptions::default()
            },
            self.block_cache.clone(),
        )?;
        iter.seek(encoded_key)?;
        if iter.valid()
            && let Some(current_key) = iter.key()?
            && current_key.as_ref() == encoded_key
            && let Some(value_bytes) = iter.value()?
        {
            let value = decode_value_masked(
                &value_bytes,
                num_columns,
                decode_mask,
                terminal_mask.as_deref_mut(),
            )?;
            if self.ttl_provider.expired(&value.expired_at) {
                return Ok(false);
            }
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
    use crate::db_state::{DbState, DbStateHandle};
    use crate::file::{FileId, FileManager, FileSystemRegistry};
    use crate::sst::row_codec::{encode_key, encode_value};
    use crate::sst::{SSTWriter, SSTWriterOptions};
    use crate::r#type::{Column, Key, Value, ValueType};

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
                seq: 0,
                size: 0, // Test file, size doesn't matter
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
                seq: 0,
                size,
            })
        }
    }

    fn cleanup_test_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn create_test_sst(
        file_manager: &FileManager,
        seq: u64,
        entries: Vec<(&[u8], &[u8])>,
    ) -> Result<Arc<DataFile>> {
        let (file_id, writer_file) = file_manager.create_data_file()?;
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                num_columns: 1,
                ..SSTWriterOptions::default()
            },
        );
        for (key, value) in entries {
            let encoded_key = encode_key(&Key::new(0, key.to_vec()));
            writer.add(encoded_key.as_ref(), value)?;
        }
        let (first_key, last_key, file_size) = writer.finish_with_range()?;
        Ok(Arc::new(DataFile {
            file_type: DataFileType::SSTable,
            start_key: first_key,
            end_key: last_key,
            file_id,
            seq,
            size: file_size,
        }))
    }

    fn make_value_bytes(data: &[u8], num_columns: usize) -> Vec<u8> {
        let value = Value::new(vec![Some(Column::new(ValueType::Put, data.to_vec()))]);
        encode_value(&value, num_columns).to_vec()
    }

    #[test]
    fn test_lsm_tree_apply_edit() {
        let db_state = Arc::new(DbStateHandle::new());
        db_state.store(DbState {
            seq_id: 0,
            lsm_version: LSMTreeVersion {
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
            },
            active: None,
            immutables: Vec::new().into(),
        });
        let lsm_tree = LSMTree::with_state(Arc::clone(&db_state));

        // Create a version edit to remove one file from level 0 and add two new files
        let current_version = db_state.load().lsm_version.clone();
        let edit = VersionEdit {
            level_edits: vec![
                LevelEdit {
                    level: 0,
                    removed_files: vec![current_version.levels[0].files[0].clone()],
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

        lsm_tree.apply_edit(edit);

        // Verify the new version
        let version = db_state.load().lsm_version.clone();
        assert_eq!(version.levels.len(), 2);

        let level0 = &version.levels[0];
        assert_eq!(level0.ordinal, 0);
        assert_eq!(level0.files.len(), 3);
        assert_eq!(level0.files[0].start_key, b"a1");
        assert_eq!(level0.files[1].start_key, b"b1");
        assert_eq!(level0.files[2].start_key, b"c");

        let level1 = &version.levels[1];
        assert_eq!(level1.ordinal, 1);
        assert_eq!(level1.files.len(), 3);
        assert_eq!(level1.files[0].start_key, b"d1");
        assert_eq!(level1.files[1].start_key, b"e");
        assert_eq!(level1.files[2].start_key, b"g");
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
        let file_manager = Arc::new(FileManager::with_defaults(fs).unwrap());
        let config = crate::compaction::CompactionConfig {
            l1_base_bytes: 1,
            level_size_multiplier: 1,
            max_level: 3,
            ..crate::compaction::CompactionConfig::default()
        };
        let factory = crate::compaction::make_sst_builder_factory(SSTWriterOptions::default());
        let db_state = Arc::new(DbStateHandle::new());
        db_state.store(DbState {
            seq_id: 0,
            lsm_version: LSMTreeVersion {
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
            },
            active: None,
            immutables: Vec::new().into(),
        });
        let lsm_tree = Arc::new(LSMTree::with_state(Arc::clone(&db_state)));
        let worker = Arc::new(crate::compaction::CompactionWorker::new(
            crate::compaction::CompactionExecutor::new(config).unwrap(),
            factory,
            Arc::clone(&file_manager),
            Arc::downgrade(&lsm_tree),
        ));
        lsm_tree.configure_compaction(config, Some(Arc::clone(&worker)));
        let target = lsm_tree
            .db_state
            .load()
            .lsm_version
            .levels
            .iter()
            .find(|level| level.ordinal == 1)
            .and_then(|level| level.files.iter().find(|file| file.start_key == b"a"))
            .cloned()
            .expect("target file");
        lsm_tree.apply_edit(VersionEdit {
            level_edits: vec![LevelEdit {
                level: 1,
                removed_files: vec![target],
                new_files: Vec::new(),
            }],
        });
        let level1 = lsm_tree.level_files(1);
        let level2 = lsm_tree.level_files(2);
        assert_eq!(level1.len(), 0);
        assert_eq!(level2.len(), 1);
        assert!(level2.iter().any(|file| file.start_key == b"e"));
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
        let file_manager = Arc::new(FileManager::with_defaults(fs).unwrap());
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
        db_state.store(DbState {
            seq_id: 0,
            lsm_version: LSMTreeVersion {
                levels: vec![Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![older, newer],
                }],
            },
            active: None,
            immutables: Vec::new().into(),
        });
        let lsm_tree = LSMTree::with_state(Arc::clone(&db_state));
        let encoded_key = encode_key(&crate::r#type::Key::new(0, b"k1".to_vec()));
        let value = lsm_tree
            .get(
                &file_manager,
                encoded_key.as_ref(),
                num_columns,
                None,
                Some(2),
            )
            .unwrap();
        assert_eq!(value.len(), 1);
        assert_eq!(value[0].columns()[0].as_ref().unwrap().data(), b"old");
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
        let file_manager = Arc::new(FileManager::with_defaults(fs).unwrap());
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
        db_state.store(DbState {
            seq_id: 0,
            lsm_version: LSMTreeVersion {
                levels: vec![Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![older, newer],
                }],
            },
            active: None,
            immutables: Vec::new().into(),
        });
        let lsm_tree = LSMTree::with_state(Arc::clone(&db_state));
        let encoded_key = encode_key(&crate::r#type::Key::new(0, b"k1".to_vec()));
        let value = lsm_tree
            .get(&file_manager, encoded_key.as_ref(), num_columns, None, None)
            .unwrap();
        assert_eq!(value.len(), 2);
        assert_eq!(value[0].columns()[0].as_ref().unwrap().data(), b"new");
        assert_eq!(value[1].columns()[0].as_ref().unwrap().data(), b"old");
        cleanup_test_root(root);
    }
}
