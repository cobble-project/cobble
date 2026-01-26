use crate::config::CompactionPolicyKind;
use crate::data_file::DataFile;
use crate::file::FileId;
use crate::iterator::SortedRun;
use crate::lsm::Level;
use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub(crate) struct CompactionConfig {
    pub(crate) policy: CompactionPolicyKind,
    pub(crate) l0_file_limit: usize,
    pub(crate) l1_base_bytes: usize,
    pub(crate) level_size_multiplier: usize,
    pub(crate) max_level: u8,
    pub(crate) block_size: usize,
    pub(crate) buffer_size: usize,
    pub(crate) num_columns: usize,
    pub(crate) target_file_size: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            policy: CompactionPolicyKind::RoundRobin,
            l0_file_limit: 4,
            l1_base_bytes: 64 * 1024 * 1024,
            level_size_multiplier: 10,
            max_level: 6,
            block_size: 4096,
            buffer_size: 8192,
            num_columns: 1,
            target_file_size: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone)]
pub(crate) struct CompactionPlan {
    pub(crate) input_level: u8,
    pub(crate) output_level: u8,
    pub(crate) base_file_id: u64,
    pub(crate) trivial_move: bool,
}

impl fmt::Display for CompactionPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "L{}->L{} base_file_id={} trivial_move={}",
            self.input_level, self.output_level, self.base_file_id, self.trivial_move
        )
    }
}

pub(crate) trait CompactionPolicy: Send {
    fn pick(&mut self, levels: &[Level], config: CompactionConfig) -> Option<CompactionPlan>;
}

/// Picks compaction from level 0 if it exceeds the file limit.
fn pick_first_level(levels: &[Level], config: &CompactionConfig) -> Option<CompactionPlan> {
    let level0 = levels.iter().find(|level| level.ordinal == 0)?;
    if level0.files.len() > config.l0_file_limit {
        let base_file_id = level0
            .files
            .iter()
            .map(|file| file.file_id)
            .min()
            .unwrap_or(0);
        return Some(CompactionPlan {
            input_level: 0,
            output_level: 1,
            base_file_id,
            trivial_move: false,
        });
    }
    None
}

/// A compaction policy that picks files in a round-robin fashion.
/// It keeps track of the last picked file ID for each level to ensure fairness.
pub(crate) struct RoundRobinPolicy {
    last_file_ids: Vec<FileId>,
}

impl RoundRobinPolicy {
    pub(crate) fn new() -> Self {
        Self {
            last_file_ids: Vec::new(),
        }
    }
}

impl CompactionPolicy for RoundRobinPolicy {
    fn pick(&mut self, levels: &[Level], config: CompactionConfig) -> Option<CompactionPlan> {
        if levels.is_empty() {
            return None;
        }
        let first_level = pick_first_level(levels, &config);
        if first_level.is_some() {
            return first_level;
        }

        let mut best_level: Option<u8> = None;
        let mut best_ratio = 1.0;

        for level in levels {
            if level.ordinal == 0 || level.ordinal >= config.max_level {
                continue;
            }
            let threshold = level_threshold(
                config.l1_base_bytes,
                config.level_size_multiplier,
                level.ordinal,
            );
            if threshold == 0 {
                continue;
            }
            let level_size: usize = level.files.iter().map(|file| file.size).sum();
            let ratio = level_size as f64 / threshold as f64;
            if ratio > 1.0 && ratio > best_ratio {
                best_ratio = ratio;
                best_level = Some(level.ordinal);
            }
        }

        let selected = best_level?;
        if selected >= config.max_level {
            return None;
        }
        let level = levels.iter().find(|level| level.ordinal == selected)?;
        if level.files.is_empty() {
            return None;
        }
        let mut sorted = level.files.clone();
        sorted.sort_by_key(|file| file.file_id);
        let last_file_id = self
            .last_file_ids
            .get(selected as usize)
            .copied()
            .unwrap_or(0);
        let base_file_id = sorted
            .iter()
            .find(|file| file.file_id > last_file_id)
            .map_or(sorted[0].as_ref(), |file| file.as_ref())
            .file_id;
        if self.last_file_ids.len() <= selected as usize {
            self.last_file_ids.resize(selected as usize + 1, 0);
        }
        self.last_file_ids[selected as usize] = base_file_id;
        let target_file = sorted
            .iter()
            .find(|file| file.file_id == base_file_id)
            .map_or(sorted[0].as_ref(), |file| file.as_ref());
        let trivial_move = levels
            .iter()
            .find(|level| level.ordinal == selected + 1)
            .iter()
            .flat_map(|level| level.files.iter())
            .all(|file| !file_overlap(target_file, file));
        Some(CompactionPlan {
            input_level: selected,
            output_level: selected + 1,
            base_file_id,
            trivial_move,
        })
    }
}

/// A compaction policy that picks the file with the minimum overlap in the next level.
pub(crate) struct MinOverlapPolicy;

impl MinOverlapPolicy {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl CompactionPolicy for MinOverlapPolicy {
    fn pick(&mut self, levels: &[Level], config: CompactionConfig) -> Option<CompactionPlan> {
        if levels.is_empty() {
            return None;
        }
        let first_level = pick_first_level(levels, &config);
        if first_level.is_some() {
            return first_level;
        }

        let mut best: Option<(usize, u64, usize)> = None;
        let max_level = levels
            .iter()
            .map(|level| level.ordinal as usize)
            .max()
            .unwrap_or(0);
        for level in 1..=max_level.saturating_sub(1) {
            if level as u8 >= config.max_level {
                continue;
            }
            let threshold = level_threshold(
                config.l1_base_bytes,
                config.level_size_multiplier,
                level as u8,
            );
            if threshold == 0 {
                continue;
            }
            let Some(input_level) = levels.iter().find(|item| item.ordinal == level as u8) else {
                continue;
            };
            let level_size: usize = input_level.files.iter().map(|file| file.size).sum();
            if level_size <= threshold {
                continue;
            }
            let output_level = levels.iter().find(|item| item.ordinal == level as u8 + 1);
            if output_level.is_none() {
                continue;
            }
            let output_files = output_level
                .map(|item| item.files.as_slice())
                .unwrap_or_default();
            for file in &input_level.files {
                let overlap_bytes = overlap_size(file, output_files);
                let candidate = (overlap_bytes, file.file_id, level);
                if best
                    .as_ref()
                    .is_none_or(|current| compare_overlap(candidate, *current) == Ordering::Less)
                {
                    best = Some(candidate);
                }
            }
        }

        let (overlap, base_file_id, level) = best?;
        let _ = overlap;
        let input_file = levels
            .iter()
            .find(|item| item.ordinal == level as u8)
            .and_then(|item| item.files.iter().find(|file| file.file_id == base_file_id));
        let trivial_move = input_file.is_some_and(|file| {
            levels
                .iter()
                .find(|item| item.ordinal == level as u8 + 1)
                .iter()
                .flat_map(|item| item.files.iter())
                .all(|other| !file_overlap(file, other))
        });
        Some(CompactionPlan {
            input_level: level as u8,
            output_level: level as u8 + 1,
            base_file_id,
            trivial_move,
        })
    }
}

pub(crate) fn build_runs_for_plan(levels: &[Level], plan: &CompactionPlan) -> Vec<SortedRun> {
    if levels.is_empty() {
        return Vec::new();
    }
    let mut runs = Vec::new();
    let input_level = levels
        .iter()
        .find(|level| level.ordinal == plan.input_level);
    let output_level = levels
        .iter()
        .find(|level| level.ordinal == plan.output_level);
    let Some(input_level) = input_level else {
        return runs;
    };
    let input_files = if input_level.tiered {
        input_level.files.clone()
    } else if let Some(input_file) = input_level
        .files
        .iter()
        .filter(|file| file.file_id >= plan.base_file_id)
        .min_by_key(|file| file.file_id)
        .cloned()
    {
        vec![input_file]
    } else {
        Vec::new()
    };
    if input_files.is_empty() {
        return runs;
    }
    if input_level.tiered {
        for file in input_files.iter().rev() {
            runs.push(SortedRun::new(plan.input_level, vec![Arc::clone(file)]));
        }
    } else {
        runs.push(SortedRun::new(plan.input_level, input_files.clone()));
    }
    let Some(output_level) = output_level else {
        return runs;
    };
    if output_level.ordinal == 0 || plan.input_level == plan.output_level {
        // No output level or same level compaction (e.g., level 0 compaction)
    } else {
        let output_files = if input_level.tiered {
            let (start_key, end_key) = input_files.iter().fold(
                (
                    input_files[0].start_key.as_slice(),
                    input_files[0].end_key.as_slice(),
                ),
                |(start, end), file| {
                    (
                        std::cmp::min(start, file.start_key.as_slice()),
                        std::cmp::max(end, file.end_key.as_slice()),
                    )
                },
            );
            output_level
                .files
                .iter()
                .filter(|file| {
                    !(end_key < file.start_key.as_slice() || file.end_key.as_slice() < start_key)
                })
                .cloned()
                .collect()
        } else {
            let input_file = input_files
                .iter()
                .min_by_key(|file| file.file_id)
                .expect("input_files not empty");
            overlapping_files(input_file, &output_level.files).unwrap_or_default()
        };
        if !output_files.is_empty() {
            runs.push(SortedRun::new(plan.output_level, output_files));
        }
    }
    runs
}

fn overlap_size(file: &DataFile, candidates: &[Arc<DataFile>]) -> usize {
    candidates
        .iter()
        .filter(|candidate| file_overlap(file, candidate))
        .map(|candidate| candidate.size)
        .sum()
}

fn overlapping_files(file: &DataFile, candidates: &[Arc<DataFile>]) -> Option<Vec<Arc<DataFile>>> {
    let mut overlaps = Vec::new();
    for candidate in candidates {
        if file_overlap(file, candidate) {
            overlaps.push(Arc::clone(candidate));
        }
    }
    if overlaps.is_empty() {
        None
    } else {
        Some(overlaps)
    }
}

fn file_overlap(left: &DataFile, right: &DataFile) -> bool {
    if left.end_key < right.start_key || right.end_key < left.start_key {
        return false;
    }
    true
}

fn level_threshold(base: usize, multiplier: usize, level: u8) -> usize {
    if level <= 1 {
        base
    } else {
        let power = level.saturating_sub(1) as u32;
        base.saturating_mul(multiplier.saturating_pow(power))
    }
}

fn compare_overlap(candidate: (usize, u64, usize), current: (usize, u64, usize)) -> Ordering {
    match candidate.0.cmp(&current.0) {
        Ordering::Equal => candidate.1.cmp(&current.1),
        ord => ord,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_file::DataFileType;

    fn make_file(id: FileId, start: &[u8], end: &[u8], size: usize) -> Arc<DataFile> {
        Arc::new(DataFile {
            file_type: DataFileType::SSTable,
            start_key: start.to_vec(),
            end_key: end.to_vec(),
            file_id: id,
            seq: 0,
            size,
        })
    }

    #[test]
    fn test_round_robin_trivial_move() {
        let config = CompactionConfig {
            l0_file_limit: 4,
            l1_base_bytes: 10,
            level_size_multiplier: 10,
            max_level: 3,
            ..CompactionConfig::default()
        };
        let level1 = Level {
            ordinal: 1,
            tiered: false,
            files: vec![make_file(1, b"a", b"b", 100)],
        };
        let level2 = Level {
            ordinal: 2,
            tiered: false,
            files: vec![make_file(2, b"c", b"d", 10)],
        };
        let level0 = Level {
            ordinal: 0,
            tiered: true,
            files: Vec::new(),
        };
        let mut policy = RoundRobinPolicy::new();
        let plan = policy
            .pick(&vec![level0, level1, level2], config)
            .expect("plan");
        assert_eq!(plan.input_level, 1);
        assert!(plan.trivial_move);
        assert_eq!(plan.base_file_id, 1);
    }

    #[test]
    fn test_min_overlap_prefers_smaller_overlap() {
        let config = CompactionConfig {
            l0_file_limit: 4,
            l1_base_bytes: 10,
            level_size_multiplier: 10,
            max_level: 3,
            ..CompactionConfig::default()
        };
        let level1 = Level {
            ordinal: 1,
            tiered: false,
            files: vec![make_file(1, b"a", b"f", 50), make_file(2, b"g", b"h", 50)],
        };
        let level2 = Level {
            ordinal: 2,
            tiered: false,
            files: vec![make_file(3, b"a", b"z", 10), make_file(4, b"g", b"h", 200)],
        };
        let level0 = Level {
            ordinal: 0,
            tiered: true,
            files: Vec::new(),
        };
        let mut policy = MinOverlapPolicy::new();
        let plan = policy
            .pick(&vec![level0, level1, level2], config)
            .expect("plan");
        assert_eq!(plan.input_level, 1);
        assert_eq!(plan.base_file_id, 1);
        assert!(!plan.trivial_move);
    }
}
