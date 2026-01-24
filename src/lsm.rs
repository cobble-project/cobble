use crate::data_file::DataFile;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct Level {
    ordinal: u8,
    tiered: bool,
    files: Vec<Arc<DataFile>>,
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
}

#[derive(Clone)]
pub(crate) struct LevelEdit {
    level: u8,
    removed_files: Vec<Arc<DataFile>>,
    new_files: Vec<Arc<DataFile>>,
}

#[derive(Clone)]
pub(crate) struct VersionEdit {
    level_edits: Vec<LevelEdit>,
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

        for level_edit in edit.level_edits {
            if let Some(level) = new_levels
                .iter_mut()
                .find(|l| l.ordinal == level_edit.level)
            {
                let mut insert_pos = Option::<usize>::None;
                // First, find the position of files to be removed, and remove the files.
                for file in &level_edit.removed_files {
                    if let Some(pos) = level.files.iter().position(|f| Arc::ptr_eq(f, file)) {
                        level.files.remove(pos);
                        if let Some(previous) = insert_pos {
                            // Ensure that all removed files are contiguous.
                            assert_eq!(pos, previous);
                        } else {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_file::DataFileType;

    static mut FILE_ID_COUNTER: u64 = 0;

    fn create_data_file(start: &[u8], end: &[u8]) -> Arc<DataFile> {
        unsafe {
            let id = FILE_ID_COUNTER;
            FILE_ID_COUNTER += 1;
            Arc::new(DataFile {
                file_type: DataFileType::SSTable,
                start_key: start.to_vec(),
                end_key: end.to_vec(),
                file_id: id,
                size: 0, // Test file, size doesn't matter
            })
        }
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
}
