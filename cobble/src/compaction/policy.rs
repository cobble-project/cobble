use crate::config::CompactionPolicyKind;
use crate::data_file::DataFile;
use crate::data_file::DataFileType;
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
    pub(crate) read_buffer_size: usize,
    pub(crate) read_ahead_enabled: bool,
    pub(crate) num_columns: usize,
    pub(crate) target_file_size: usize,
    pub(crate) bloom_filter_enabled: bool,
    pub(crate) bloom_bits_per_key: u32,
    pub(crate) partitioned_index: bool,
    pub(crate) max_threads: usize,
    pub(crate) split_trigger_level: Option<u8>,
    pub(crate) output_file_type: DataFileType,
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
            read_buffer_size: 64 * 1024,
            read_ahead_enabled: true,
            num_columns: 1,
            target_file_size: 64 * 1024 * 1024,
            bloom_filter_enabled: false,
            bloom_bits_per_key: 10,
            partitioned_index: false,
            max_threads: 4,
            split_trigger_level: None,
            output_file_type: DataFileType::SSTable,
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
        let input_level = levels
            .iter()
            .rev()
            .filter(|level| level.ordinal > 0 && level.ordinal < config.max_level)
            .find(|level| {
                let threshold = level_threshold(
                    config.l1_base_bytes,
                    config.level_size_multiplier,
                    level.ordinal,
                );
                if threshold == 0 {
                    return false;
                }
                let level_size: usize = level.files.iter().map(|file| file.size).sum();
                level_size > threshold
            });

        let level = if let Some(input_level) = input_level {
            input_level
        } else {
            let first_level = pick_first_level(levels, &config);
            if first_level.is_some() {
                return first_level;
            }
            return None;
        };

        let selected = level.ordinal;
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

        let input_level = levels
            .iter()
            .rev()
            .filter(|level| level.ordinal > 0 && level.ordinal < config.max_level)
            .find(|level| {
                let threshold = level_threshold(
                    config.l1_base_bytes,
                    config.level_size_multiplier,
                    level.ordinal,
                );
                if threshold == 0 {
                    return false;
                }
                let level_size: usize = level.files.iter().map(|file| file.size).sum();
                level_size > threshold
            });

        let input_level = if let Some(input_level) = input_level {
            input_level
        } else {
            let first_level = pick_first_level(levels, &config);
            if first_level.is_some() {
                return first_level;
            }
            return None;
        };

        let selected_level = input_level.ordinal as usize;
        let output_level = levels
            .iter()
            .find(|item| item.ordinal == selected_level as u8 + 1);
        let output_files: &[Arc<DataFile>] = output_level
            .map(|level| level.files.as_slice())
            .unwrap_or(&[]);
        let mut best: Option<(usize, u64, usize)> = None;
        for file in &input_level.files {
            let overlap_bytes = overlap_size(file, output_files);
            let candidate = (overlap_bytes, file.file_id, selected_level);
            if best
                .as_ref()
                .is_none_or(|current| compare_overlap(candidate, *current) == Ordering::Less)
            {
                best = Some(candidate);
            }
        }

        let (_, base_file_id, level) = best?;
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

pub(crate) fn build_runs_for_plan(
    levels: &[Level],
    plan: &CompactionPlan,
    config: &CompactionConfig,
) -> Vec<SortedRun> {
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
    let mut output_range: Option<(usize, usize)>;
    let mut output_files_opt: Option<Vec<Arc<DataFile>>> = None;
    let input_files = if input_level.tiered {
        input_level.files.clone()
    } else {
        let base_file_info = input_level
            .files
            .iter()
            .enumerate()
            .filter(|(_, file)| file.file_id >= plan.base_file_id)
            .min_by_key(|(_, file)| file.file_id);
        let Some((base_idx, base_file)) = base_file_info else {
            return Vec::new();
        };
        let mut min_idx = base_idx;
        let mut max_idx = base_idx;
        let mut selected_bytes = base_file.size;
        let threshold = level_threshold(
            config.l1_base_bytes,
            config.level_size_multiplier,
            input_level.ordinal,
        );
        let output_candidates: &[Arc<DataFile>] = output_level
            .map(|level| level.files.as_slice())
            .unwrap_or(&[]);
        output_range = overlap_range_for_file(base_file, output_candidates);
        if threshold > 0 {
            let level_size: usize = input_level.files.iter().map(|file| file.size).sum();
            let target_bytes = level_size.saturating_sub(threshold);
            if target_bytes > 0 && selected_bytes < target_bytes {
                while selected_bytes < target_bytes {
                    let left_idx = min_idx.checked_sub(1);
                    let right_idx = if max_idx + 1 < input_level.files.len() {
                        Some(max_idx + 1)
                    } else {
                        None
                    };
                    if left_idx.is_none() && right_idx.is_none() {
                        break;
                    }
                    let left_overlaps = left_idx.map(|idx| {
                        output_range
                            .map(|(start, end)| {
                                output_candidates[start..=end].iter().any(|output_file| {
                                    file_overlap(&input_level.files[idx], output_file)
                                })
                            })
                            .unwrap_or(false)
                    });
                    let right_overlaps = right_idx.map(|idx| {
                        output_range
                            .map(|(start, end)| {
                                output_candidates[start..=end].iter().any(|output_file| {
                                    file_overlap(&input_level.files[idx], output_file)
                                })
                            })
                            .unwrap_or(false)
                    });
                    let next_idx = match (left_idx, right_idx, left_overlaps, right_overlaps) {
                        (Some(left), Some(_right), Some(true), Some(false)) => left,
                        (Some(_left), Some(right), Some(false), Some(true)) => right,
                        (Some(left), Some(right), _, _) => {
                            if input_level.files[left].file_id <= input_level.files[right].file_id {
                                left
                            } else {
                                right
                            }
                        }
                        (Some(left), None, _, _) => left,
                        (None, Some(right), _, _) => right,
                        (None, None, _, _) => break,
                    };
                    if next_idx < min_idx {
                        min_idx = next_idx;
                    } else if next_idx > max_idx {
                        max_idx = next_idx;
                    }
                    selected_bytes =
                        selected_bytes.saturating_add(input_level.files[next_idx].size);
                    if let Some(range) =
                        overlap_range_for_file(&input_level.files[next_idx], output_candidates)
                    {
                        output_range = Some(match output_range {
                            Some((start, end)) => (start.min(range.0), end.max(range.1)),
                            None => range,
                        });
                    }
                }
            }
        }
        // We try to expand the selected input files to cover all overlapping files with the output
        // level to maximize compaction efficiency.
        if let Some(output_level) = output_level
            && output_level.ordinal != 0
            && plan.input_level != plan.output_level
            && let Some((start, end)) = output_range
        {
            let output_files = &output_level.files[start..=end];
            if let (Some(first), Some(last)) = (output_files.first(), output_files.last()) {
                let output_start = first.start_key.as_slice();
                let output_end = last.end_key.as_slice();
                loop {
                    let mut extended = false;
                    if min_idx > 0 {
                        let candidate = &input_level.files[min_idx - 1];
                        if candidate.start_key.as_slice() >= output_start {
                            min_idx -= 1;
                            selected_bytes =
                                selected_bytes.saturating_add(input_level.files[min_idx].size);
                            extended = true;
                        }
                    }
                    if max_idx + 1 < input_level.files.len() {
                        let candidate = &input_level.files[max_idx + 1];
                        if candidate.end_key.as_slice() <= output_end {
                            max_idx += 1;
                            selected_bytes =
                                selected_bytes.saturating_add(input_level.files[max_idx].size);
                            extended = true;
                        }
                    }
                    if !extended {
                        break;
                    }
                }
            }
        }
        let selected_files: Vec<Arc<DataFile>> = (min_idx..=max_idx)
            .map(|idx| Arc::clone(&input_level.files[idx]))
            .collect();
        if let Some(output_level) = output_level
            && output_level.ordinal != 0
            && plan.input_level != plan.output_level
        {
            output_files_opt =
                output_range.map(|(start, end)| output_level.files[start..=end].to_vec());
        }
        selected_files
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
        let output_files: Vec<Arc<DataFile>> = if let Some(output_files) = output_files_opt {
            output_files
        } else if input_level.tiered {
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
            Vec::new()
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

fn overlap_range_for_file(file: &DataFile, candidates: &[Arc<DataFile>]) -> Option<(usize, usize)> {
    let mut start: Option<usize> = None;
    let mut end: Option<usize> = None;
    for (idx, candidate) in candidates.iter().enumerate() {
        if file_overlap(file, candidate) {
            if start.is_none() {
                start = Some(idx);
            }
            end = Some(idx);
            continue;
        }
        if candidate.start_key.as_slice() > file.end_key.as_slice() {
            break;
        }
    }
    match (start, end) {
        (Some(start), Some(end)) => Some((start, end)),
        _ => None,
    }
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

pub fn level_threshold(base: usize, multiplier: usize, level: u8) -> usize {
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
        let bucket_range = DataFile::bucket_range_from_keys(start, end);
        Arc::new(DataFile::new_detached(
            DataFileType::SSTable,
            start.to_vec(),
            end.to_vec(),
            id,
            0,
            size,
            bucket_range.clone(),
            bucket_range,
        ))
    }

    #[test]
    fn test_round_robin_trivial_move() {
        let config = CompactionConfig {
            l0_file_limit: 4,
            l1_base_bytes: 10,
            level_size_multiplier: 10,
            max_level: 3,
            bloom_filter_enabled: true,
            bloom_bits_per_key: 10,
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
            .pick(&[level0, level1, level2], config)
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
            max_level: 4,
            bloom_filter_enabled: true,
            bloom_bits_per_key: 10,
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
            files: vec![make_file(3, b"a", b"f", 60), make_file(4, b"g", b"h", 60)],
        };
        let level3 = Level {
            ordinal: 3,
            tiered: false,
            files: vec![make_file(5, b"a", b"z", 10), make_file(6, b"g", b"h", 200)],
        };
        let level0 = Level {
            ordinal: 0,
            tiered: true,
            files: Vec::new(),
        };
        let mut policy = MinOverlapPolicy::new();
        let plan = policy
            .pick(&[level0, level1, level2, level3], config)
            .expect("plan");
        assert_eq!(plan.input_level, 2);
        assert_eq!(plan.base_file_id, 3);
        assert!(!plan.trivial_move);
    }

    #[test]
    fn test_build_runs_for_plan_expands_input() {
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
            files: vec![
                make_file(1, b"a", b"b", 6),
                make_file(2, b"c", b"d", 6),
                make_file(3, b"e", b"f", 6),
            ],
        };
        let level2 = Level {
            ordinal: 2,
            tiered: false,
            files: Vec::new(),
        };
        let plan = CompactionPlan {
            input_level: 1,
            output_level: 2,
            base_file_id: 1,
            trivial_move: false,
        };
        let runs = build_runs_for_plan(&[level1, level2], &plan, &config);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].len(), 2);
        let file_ids: Vec<u64> = runs[0].files().iter().map(|file| file.file_id).collect();
        assert_eq!(file_ids, vec![1, 2]);
    }

    #[test]
    fn test_build_runs_for_plan_prefers_output_overlap() {
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
            files: vec![
                make_file(1, b"a", b"b", 6),
                make_file(2, b"c", b"d", 6),
                make_file(3, b"e", b"f", 6),
            ],
        };
        let level2 = Level {
            ordinal: 2,
            tiered: false,
            files: vec![make_file(4, b"c", b"f", 5)],
        };
        let plan = CompactionPlan {
            input_level: 1,
            output_level: 2,
            base_file_id: 2,
            trivial_move: false,
        };
        let runs = build_runs_for_plan(&[level1, level2], &plan, &config);
        assert_eq!(runs.len(), 2);
        let file_ids: Vec<u64> = runs[0].files().iter().map(|file| file.file_id).collect();
        assert_eq!(file_ids, vec![2, 3]);
    }

    #[test]
    fn test_round_robin_prefers_deeper_level() {
        let config = CompactionConfig {
            l0_file_limit: 4,
            l1_base_bytes: 10,
            level_size_multiplier: 1,
            max_level: 4,
            ..CompactionConfig::default()
        };
        let level1 = Level {
            ordinal: 1,
            tiered: false,
            files: vec![make_file(1, b"a", b"b", 12)],
        };
        let level2 = Level {
            ordinal: 2,
            tiered: false,
            files: vec![make_file(2, b"c", b"d", 12)],
        };
        let level3 = Level {
            ordinal: 3,
            tiered: false,
            files: Vec::new(),
        };
        let level0 = Level {
            ordinal: 0,
            tiered: true,
            files: Vec::new(),
        };
        let mut policy = RoundRobinPolicy::new();
        let plan = policy
            .pick(&[level0, level1, level2, level3], config)
            .expect("plan");
        assert_eq!(plan.input_level, 2);
    }

    #[test]
    fn test_min_overlap_prefers_deeper_level() {
        let config = CompactionConfig {
            l0_file_limit: 4,
            l1_base_bytes: 10,
            level_size_multiplier: 1,
            max_level: 4,
            ..CompactionConfig::default()
        };
        let level1 = Level {
            ordinal: 1,
            tiered: false,
            files: vec![make_file(1, b"a", b"b", 12)],
        };
        let level2 = Level {
            ordinal: 2,
            tiered: false,
            files: vec![make_file(2, b"c", b"e", 12)],
        };
        let level3 = Level {
            ordinal: 3,
            tiered: false,
            files: vec![make_file(3, b"c", b"d", 5)],
        };
        let level0 = Level {
            ordinal: 0,
            tiered: true,
            files: Vec::new(),
        };
        let mut policy = MinOverlapPolicy::new();
        let plan = policy
            .pick(&[level0, level1, level2, level3], config)
            .expect("plan");
        assert_eq!(plan.input_level, 2);
    }

    #[test]
    fn test_min_overlap_prefers_deeper_level_without_output() {
        let config = CompactionConfig {
            l0_file_limit: 4,
            l1_base_bytes: 10,
            level_size_multiplier: 1,
            max_level: 4,
            ..CompactionConfig::default()
        };
        let level1 = Level {
            ordinal: 1,
            tiered: false,
            files: vec![make_file(1, b"a", b"b", 12)],
        };
        let level2 = Level {
            ordinal: 2,
            tiered: false,
            files: vec![make_file(2, b"c", b"d", 12)],
        };
        let level0 = Level {
            ordinal: 0,
            tiered: true,
            files: Vec::new(),
        };
        let mut policy = MinOverlapPolicy::new();
        let plan = policy
            .pick(&[level0, level1, level2], config)
            .expect("plan");
        assert_eq!(plan.input_level, 2);
    }
}
