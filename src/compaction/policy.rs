use crate::config::CompactionPolicyKind;
use crate::data_file::DataFile;
use crate::file::FileId;
use crate::iterator::SortedRun;
use std::cmp::Ordering;
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
    pub(crate) output_files: Vec<Arc<DataFile>>,
    pub(crate) base_file_id: u64,
    pub(crate) trivial_move: bool,
}

pub(crate) trait CompactionPolicy: Send {
    fn pick(
        &mut self,
        levels: &[Vec<Arc<DataFile>>],
        config: CompactionConfig,
    ) -> Option<CompactionPlan>;
}

/// Picks compaction from level 0 if it exceeds the file limit.
fn pick_first_level(
    levels: &[Vec<Arc<DataFile>>],
    config: &CompactionConfig,
) -> Option<CompactionPlan> {
    if levels
        .first()
        .is_some_and(|files| files.len() > config.l0_file_limit)
    {
        let base_file_id = levels
            .first()
            .map(|files| files.iter().map(|file| file.file_id).min().unwrap_or(0))
            .unwrap_or(0);
        return Some(CompactionPlan {
            input_level: 0,
            output_level: 1,
            output_files: Vec::new(),
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
    fn pick(
        &mut self,
        levels: &[Vec<Arc<DataFile>>],
        config: CompactionConfig,
    ) -> Option<CompactionPlan> {
        if levels.is_empty() {
            return None;
        }
        let first_level = pick_first_level(levels, &config);
        if first_level.is_some() {
            return first_level;
        }

        let mut best_level: Option<u8> = None;
        let mut best_ratio = 1.0;

        for (level, files) in levels.iter().enumerate().skip(1) {
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
            let level_size: usize = files.iter().map(|file| file.size).sum();
            let ratio = level_size as f64 / threshold as f64;
            if ratio > 1.0 && ratio > best_ratio {
                best_ratio = ratio;
                best_level = Some(level as u8);
            }
        }

        let selected = best_level?;
        if selected >= config.max_level {
            return None;
        }
        let files = levels.get(selected as usize)?;
        if files.is_empty() {
            return None;
        }
        let output_files = levels
            .get(selected as usize + 1)
            .cloned()
            .unwrap_or_default();
        let mut sorted = files.clone();
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
        let trivial_move = output_files
            .iter()
            .all(|file| !file_overlap(target_file, file));
        Some(CompactionPlan {
            input_level: selected,
            output_level: selected + 1,
            output_files,
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
    fn pick(
        &mut self,
        levels: &[Vec<Arc<DataFile>>],
        config: CompactionConfig,
    ) -> Option<CompactionPlan> {
        if levels.is_empty() {
            return None;
        }
        let first_level = pick_first_level(levels, &config);
        if first_level.is_some() {
            return first_level;
        }

        let mut best: Option<(usize, u64, usize)> = None;
        for level in 1..levels.len().saturating_sub(1) {
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
            let level_size: usize = levels[level].iter().map(|file| file.size).sum();
            if level_size <= threshold {
                continue;
            }
            for file in &levels[level] {
                let overlap_bytes = overlap_size(file, &levels[level + 1]);
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
        let output_files = levels.get(level + 1).cloned().unwrap_or_default();
        let input_file = levels
            .get(level)
            .and_then(|files| files.iter().find(|file| file.file_id == base_file_id));
        let trivial_move = input_file
            .is_some_and(|file| output_files.iter().all(|other| !file_overlap(file, other)));
        Some(CompactionPlan {
            input_level: level as u8,
            output_level: level as u8 + 1,
            output_files,
            base_file_id,
            trivial_move,
        })
    }
}

pub(crate) fn build_runs_for_plan(
    levels: &[Vec<Arc<DataFile>>],
    plan: &CompactionPlan,
) -> Vec<SortedRun> {
    if levels.is_empty() {
        return Vec::new();
    }
    let input_level = plan.input_level as usize;
    let output_level = plan.output_level as usize;
    if input_level >= levels.len() || output_level >= levels.len() {
        return Vec::new();
    }
    let mut runs = Vec::new();
    if let Some(input_file) = levels[input_level]
        .iter()
        .filter(|file| file.file_id >= plan.base_file_id)
        .min_by_key(|file| file.file_id)
    {
        runs.push(SortedRun::new(
            plan.input_level,
            vec![Arc::clone(input_file)],
        ));
        if output_level == 0 || plan.input_level == plan.output_level {
            let output_files = if plan.output_files.is_empty() {
                levels.get(output_level).cloned().unwrap_or_default()
            } else {
                plan.output_files.clone()
            };
            if !output_files.is_empty() {
                runs.push(SortedRun::new(plan.output_level, output_files));
            }
        } else {
            let output_files = if plan.output_files.is_empty() {
                levels.get(output_level).cloned().unwrap_or_default()
            } else {
                plan.output_files.clone()
            };
            if let Some(output_files) = overlapping_files(input_file, &output_files) {
                runs.push(SortedRun::new(plan.output_level, output_files));
            }
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
        let level1 = vec![make_file(1, b"a", b"b", 100)];
        let level2 = vec![make_file(2, b"c", b"d", 10)];
        let mut policy = RoundRobinPolicy::new();
        let plan = policy
            .pick(&vec![Vec::new(), level1.clone(), level2.clone()], config)
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
        let level1 = vec![make_file(1, b"a", b"f", 50), make_file(2, b"g", b"h", 50)];
        let level2 = vec![make_file(3, b"a", b"z", 10), make_file(4, b"g", b"h", 200)];
        let mut policy = MinOverlapPolicy::new();
        let plan = policy
            .pick(&vec![Vec::new(), level1.clone(), level2.clone()], config)
            .expect("plan");
        assert_eq!(plan.input_level, 1);
        assert_eq!(plan.base_file_id, 1);
        assert!(!plan.trivial_move);
    }
}
