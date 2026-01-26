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
use std::sync::Arc;

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
    levels: Vec<Level>,
}

pub(crate) struct LSMTree {
    current_version: Arc<LSMTreeVersion>,
    level_options: Vec<LevelOptions>,
    compaction_config: CompactionConfig,
    compaction_policy: Box<dyn CompactionPolicy>,
    pending_compaction: bool,
    compaction_worker: Option<Arc<CompactionWorker>>,
    block_cache: Option<BlockCache>,
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
        Self {
            current_version: Arc::new(LSMTreeVersion { levels: vec![] }),
            // at least 2 level option
            level_options: vec![
                LevelOptions { tiered: true },
                LevelOptions { tiered: false },
            ],
            compaction_config: CompactionConfig::default(),
            compaction_policy: Box::new(RoundRobinPolicy::new()),
            pending_compaction: false,
            compaction_worker: None,
            block_cache: None,
        }
    }
}

impl LSMTree {
    fn get_level_option(&self, level: u8) -> &LevelOptions {
        if let Some(opt) = self.level_options.get(level as usize) {
            opt
        } else {
            self.level_options.last().unwrap()
        }
    }

    pub(crate) fn apply_edit(&mut self, edit: VersionEdit) {
        let mut new_levels = self.current_version.levels.clone();

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
                    tiered: self.get_level_option(level_edit.level).tiered,
                    files: level_edit.new_files.clone(),
                });
            }
        }

        self.current_version = Arc::new(LSMTreeVersion { levels: new_levels });
        debug!(
            "{}. {}",
            VersionEditSummary(&edit),
            VersionSummary(&self.current_version)
        );
        self.maybe_trigger_compaction();
    }

    pub(crate) fn add_level0_files(&mut self, new_files: Vec<Arc<DataFile>>) {
        if new_files.is_empty() {
            return;
        }
        let edit = VersionEdit {
            level_edits: vec![LevelEdit {
                level: 0,
                removed_files: Vec::new(),
                new_files,
            }],
        };
        self.apply_edit(edit);
    }

    pub(crate) fn level_files(&self, level: u8) -> Vec<Arc<DataFile>> {
        self.current_version
            .levels
            .iter()
            .find(|l| l.ordinal == level)
            .map(|l| l.files.clone())
            .unwrap_or_default()
    }

    pub(crate) fn configure_compaction(
        &mut self,
        config: CompactionConfig,
        worker: Option<Arc<CompactionWorker>>,
    ) {
        self.compaction_config = config;
        self.compaction_policy = Self::make_policy(config.policy);
        self.compaction_worker = worker;
    }

    pub(crate) fn set_block_cache(&mut self, block_cache: Option<BlockCache>) {
        self.block_cache = block_cache;
    }

    pub(crate) fn shutdown_compaction(&mut self) {
        if let Some(worker) = self.compaction_worker.take()
            && let Ok(worker) = Arc::try_unwrap(worker)
        {
            worker.shutdown();
        }
        self.compaction_worker = None;
        self.pending_compaction = false;
    }

    fn make_policy(kind: crate::config::CompactionPolicyKind) -> Box<dyn CompactionPolicy> {
        match kind {
            crate::config::CompactionPolicyKind::RoundRobin => Box::new(RoundRobinPolicy::new()),
            crate::config::CompactionPolicyKind::MinOverlap => Box::new(MinOverlapPolicy::new()),
        }
    }

    pub(crate) fn on_compaction_complete(&mut self) {
        self.pending_compaction = false;
    }

    pub(crate) fn on_compaction_started(&mut self) {
        self.pending_compaction = true;
    }

    fn maybe_trigger_compaction(&mut self) {
        if self.pending_compaction {
            return;
        }
        let levels = &self.current_version.levels;
        let plan = self.compaction_policy.pick(levels, self.compaction_config);
        let Some(plan) = plan else {
            return;
        };
        let Some(worker) = self.compaction_worker.clone() else {
            return;
        };
        if plan.trivial_move {
            if let Some(edit) = self.build_trivial_move_edit(levels, &plan) {
                debug!(
                    "compaction trivial move L{}->L{} file_id={}",
                    plan.input_level, plan.output_level, plan.base_file_id
                );
                self.apply_edit(edit);
            }
            return;
        }
        debug!("trigger compaction plan {}", plan);
        let runs = build_runs_for_plan(levels, &plan);
        if let Some(handle) = worker.submit_runs(
            runs,
            plan.output_level,
            crate::data_file::DataFileType::SSTable,
        ) {
            self.pending_compaction = true;
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

        for level in self.current_version.levels.iter() {
            if level.tiered {
                for file in level.files.iter().rev() {
                    if let Some(max_seq) = max_seq
                        && file.seq >= max_seq
                    {
                        continue;
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
                            &decode_mask,
                            terminal_mask.as_deref_mut(),
                        )?;
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
                        values.push(value);
                        if should_stop {
                            return Ok(values);
                        }
                    }
                }
            } else {
                for file in level.files.iter() {
                    if encoded_key < file.start_key.as_slice()
                        || encoded_key > file.end_key.as_slice()
                    {
                        continue;
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
                            &decode_mask,
                            terminal_mask.as_deref_mut(),
                        )?;
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
                        values.push(value);
                        if should_stop {
                            return Ok(values);
                        }
                    }
                    break;
                }
            }
        }

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_file::DataFileType;
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
        let mut lsm_tree = LSMTree {
            current_version: Arc::new(LSMTreeVersion {
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
            }),
            ..Default::default()
        };

        // Create a version edit to remove one file from level 0 and add two new files
        let edit = VersionEdit {
            level_edits: vec![
                LevelEdit {
                    level: 0,
                    removed_files: vec![lsm_tree.current_version.levels[0].files[0].clone()],
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
        let version = &lsm_tree.current_version;
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
        let lsm_tree = Arc::new(std::sync::Mutex::new(LSMTree {
            current_version: Arc::new(LSMTreeVersion {
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
            }),
            ..Default::default()
        }));
        let worker = Arc::new(crate::compaction::CompactionWorker::new(
            crate::compaction::CompactionExecutor::new(config).unwrap(),
            factory,
            Arc::clone(&file_manager),
            Arc::downgrade(&lsm_tree),
        ));
        {
            let mut tree = lsm_tree.lock().unwrap();
            tree.configure_compaction(config, Some(Arc::clone(&worker)));
            let target = tree
                .current_version
                .levels
                .iter()
                .find(|level| level.ordinal == 1)
                .and_then(|level| level.files.iter().find(|file| file.start_key == b"a"))
                .cloned()
                .expect("target file");
            tree.apply_edit(VersionEdit {
                level_edits: vec![LevelEdit {
                    level: 1,
                    removed_files: vec![target],
                    new_files: Vec::new(),
                }],
            });
        }
        let tree = lsm_tree.lock().unwrap();
        let level1 = tree.level_files(1);
        let level2 = tree.level_files(2);
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
        let lsm_tree = LSMTree {
            current_version: Arc::new(LSMTreeVersion {
                levels: vec![Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![older, newer],
                }],
            }),
            ..Default::default()
        };
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
        let lsm_tree = LSMTree {
            current_version: Arc::new(LSMTreeVersion {
                levels: vec![Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![older, newer],
                }],
            }),
            ..Default::default()
        };
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
