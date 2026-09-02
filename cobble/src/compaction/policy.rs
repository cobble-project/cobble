use crate::config::{CompactionPolicyKind, SstReadMetadataCacheMode};
use crate::data_file::DataFile;
use crate::data_file::DataFileType;
use crate::db_state::{LSMTreeScope, TruncationCursorMap};
use crate::error::{Error, Result};
use crate::file::FileId;
use crate::iterator::SortedRun;
use crate::lsm::Level;
use crate::schema::SchemaManager;
use crate::sst::SSTWriterOptions;
use crate::r#type::{
    ENCODED_KEY_PREFIX_BYTES, encode_bucket_prefix, key_bucket, key_column_family,
};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
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
    pub(crate) read_metadata_cache_mode: SstReadMetadataCacheMode,
    pub(crate) pinned_metadata_max_level: Option<u8>,
    pub(crate) pinned_metadata_partitions_enabled: bool,
    pub(crate) max_threads: usize,
    pub(crate) split_trigger_level: Option<u8>,
    pub(crate) output_file_type: DataFileType,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            policy: CompactionPolicyKind::RoundRobin,
            l0_file_limit: 4,
            l1_base_bytes: 256 * 1024 * 1024,
            level_size_multiplier: 10,
            max_level: 6,
            block_size: 4096,
            buffer_size: SSTWriterOptions::default().buffer_size,
            read_buffer_size: 64 * 1024,
            read_ahead_enabled: true,
            num_columns: 1,
            target_file_size: 64 * 1024 * 1024,
            bloom_filter_enabled: false,
            bloom_bits_per_key: 10,
            partitioned_index: false,
            read_metadata_cache_mode: SstReadMetadataCacheMode::Eager,
            pinned_metadata_max_level: None,
            pinned_metadata_partitions_enabled: false,
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
    pub(crate) drop_truncated: bool,
    pub(crate) drop_expired: bool,
}

/// Concrete compaction inputs resolved from a policy candidate.
///
/// Policy selection remains intentionally lightweight: it names a base file and
/// adjacent output level. This resolved form is the only input passed to a
/// compaction executor, including when schema safety expands the candidate into
/// a bidirectional fixed-point overlap closure.
pub(crate) struct ResolvedCompactionPlan {
    pub(crate) output_level: u8,
    pub(crate) runs: Vec<SortedRun>,
    pub(crate) target_schema_id: u64,
}

impl fmt::Display for CompactionPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "L{}->L{} base_file_id={} trivial_move={} drop_truncated={} drop_expired={}",
            self.input_level,
            self.output_level,
            self.base_file_id,
            self.trivial_move,
            self.drop_truncated,
            self.drop_expired
        )
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct CompactionPolicyContext<'a> {
    pub(crate) truncation_cursors: Option<&'a TruncationCursorMap>,
    pub(crate) tree_scope: Option<&'a LSMTreeScope>,
    /// Current wall-clock seconds from the TTL provider. 0 means TTL is disabled
    /// and expired-file detection should be skipped.
    pub(crate) now_seconds: u32,
}

pub(crate) trait CompactionPolicy: Send {
    fn pick(&mut self, levels: &[Level], config: CompactionConfig) -> Option<CompactionPlan> {
        self.pick_with_context(levels, config, CompactionPolicyContext::default())
    }

    fn pick_with_context(
        &mut self,
        levels: &[Level],
        config: CompactionConfig,
        context: CompactionPolicyContext<'_>,
    ) -> Option<CompactionPlan>;
}

/// Picks compaction from level 0 if it exceeds the file limit.
fn pick_first_level(
    levels: &[Level],
    config: &CompactionConfig,
    context: CompactionPolicyContext<'_>,
) -> Option<CompactionPlan> {
    let level0 = levels.iter().find(|level| level.ordinal == 0)?;
    if let Some((level, file_id)) = pick_expired_file(levels, context.now_seconds) {
        return Some(CompactionPlan {
            input_level: level,
            output_level: level.saturating_add(1),
            base_file_id: file_id,
            trivial_move: true,
            drop_truncated: false,
            drop_expired: true,
        });
    }
    if let Some((level, output_level, file_id)) =
        pick_expired_separated_file_for_rewrite(levels, context.now_seconds, config.max_level)
    {
        return Some(CompactionPlan {
            input_level: level,
            output_level,
            base_file_id: file_id,
            trivial_move: false,
            drop_truncated: false,
            drop_expired: false,
        });
    }
    if let Some((level, file_id)) = pick_fully_truncated_file(levels, context) {
        return Some(CompactionPlan {
            input_level: level,
            output_level: level.saturating_add(1),
            base_file_id: file_id,
            trivial_move: true,
            drop_truncated: true,
            drop_expired: false,
        });
    }
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
            drop_truncated: false,
            drop_expired: false,
        });
    }
    None
}

fn pick_fully_truncated_file(
    levels: &[Level],
    context: CompactionPolicyContext<'_>,
) -> Option<(u8, FileId)> {
    levels
        .iter()
        .flat_map(|level| {
            level
                .files
                .iter()
                .map(move |file| (level.ordinal, file.as_ref()))
        })
        .filter(|(_, file)| {
            // Files with separated values cannot be dropped via the removal-only fast path
            // because VLOG entry-count deltas are only computed during rewrite compaction.
            !file.has_separated_values() && file_fully_covered_by_truncation_cursor(file, context)
        })
        .min_by_key(|(level, file)| (*level, file.file_id))
        .map(|(level, file)| (level, file.file_id))
}

/// Pick the lowest-level file whose every value is expired, so compaction can drop it
/// without reading or rewriting. Returns None when TTL is disabled (`now_seconds == 0`).
/// Files with separated values are excluded (they need rewrite to collect VLOG deltas).
fn pick_expired_file(levels: &[Level], now_seconds: u32) -> Option<(u8, FileId)> {
    if now_seconds == 0 {
        return None;
    }
    levels
        .iter()
        .flat_map(|level| {
            level
                .files
                .iter()
                .map(move |file| (level.ordinal, file.as_ref()))
        })
        .filter(|(_, file)| {
            // Files with separated values cannot be dropped via the removal-only fast path
            // because VLOG entry-count deltas are only computed during rewrite compaction.
            !file.has_separated_values() && file.is_fully_expired(now_seconds)
        })
        .min_by_key(|(level, file)| (*level, file.file_id))
        .map(|(level, file)| (level, file.file_id))
}

/// Pick the lowest-level separated-value file whose every value is expired, so compaction can
/// rewrite it (filtering expired entries and collecting VLOG deltas). Returns None when TTL is
/// disabled or no separated-value file is fully expired.
///
/// Files below `max_level` move down one level. Files at or above `max_level` are rewritten in
/// place, which also handles databases reopened with a lower configured maximum.
fn pick_expired_separated_file_for_rewrite(
    levels: &[Level],
    now_seconds: u32,
    max_level: u8,
) -> Option<(u8, u8, FileId)> {
    if now_seconds == 0 {
        return None;
    }
    levels
        .iter()
        .flat_map(|level| {
            level
                .files
                .iter()
                .map(move |file| (level.ordinal, file.as_ref()))
        })
        .filter(|(_, file)| file.has_separated_values() && file.is_fully_expired(now_seconds))
        .min_by_key(|(level, file)| (*level, file.file_id))
        .map(|(level, file)| {
            let output_level = if level >= max_level { level } else { level + 1 };
            (level, output_level, file.file_id)
        })
}

pub(crate) fn file_fully_covered_by_truncation_cursor(
    file: &DataFile,
    context: CompactionPolicyContext<'_>,
) -> bool {
    let Some(cursors) = context
        .truncation_cursors
        .filter(|cursors| !cursors.is_empty())
    else {
        return false;
    };
    let Some(start_bucket) = key_bucket(&file.start_key) else {
        return false;
    };
    let Some(end_bucket) = key_bucket(&file.end_key) else {
        return false;
    };
    let Some(start_cf) = key_column_family(&file.start_key) else {
        return false;
    };
    let Some(end_cf) = key_column_family(&file.end_key) else {
        return false;
    };
    if start_bucket != end_bucket || start_cf != end_cf {
        return false;
    }

    cursors.iter().any(|(cursor_id, cursor)| {
        if cursor_id.bucket != start_bucket || cursor_id.column_family_id != start_cf {
            return false;
        }
        if let Some(scope) = context.tree_scope
            && (scope.column_family_id != cursor_id.column_family_id
                || !scope.bucket_range.contains(&cursor_id.bucket))
        {
            return false;
        }
        if !file.effective_bucket_range.contains(&cursor_id.bucket) {
            return false;
        }
        let mut encoded_cursor = Vec::with_capacity(ENCODED_KEY_PREFIX_BYTES + cursor.len());
        encoded_cursor.extend_from_slice(&encode_bucket_prefix(cursor_id.bucket));
        encoded_cursor.push(cursor_id.column_family_id);
        encoded_cursor.extend_from_slice(cursor);
        encoded_cursor.as_slice() >= file.end_key.as_slice()
    })
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
    fn pick_with_context(
        &mut self,
        levels: &[Level],
        config: CompactionConfig,
        context: CompactionPolicyContext<'_>,
    ) -> Option<CompactionPlan> {
        if levels.is_empty() {
            return None;
        }
        if let Some((level, file_id)) = pick_expired_file(levels, context.now_seconds) {
            return Some(CompactionPlan {
                input_level: level,
                output_level: level.saturating_add(1),
                base_file_id: file_id,
                trivial_move: true,
                drop_truncated: false,
                drop_expired: true,
            });
        }
        if let Some((level, output_level, file_id)) =
            pick_expired_separated_file_for_rewrite(levels, context.now_seconds, config.max_level)
        {
            return Some(CompactionPlan {
                input_level: level,
                output_level,
                base_file_id: file_id,
                trivial_move: false,
                drop_truncated: false,
                drop_expired: false,
            });
        }
        if let Some((level, file_id)) = pick_fully_truncated_file(levels, context) {
            return Some(CompactionPlan {
                input_level: level,
                output_level: level.saturating_add(1),
                base_file_id: file_id,
                trivial_move: true,
                drop_truncated: true,
                drop_expired: false,
            });
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
            let first_level = pick_first_level(levels, &config, context);
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
            drop_truncated: false,
            drop_expired: false,
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

/// A score-priority policy aligned with RocksDB's leveled compaction scoring.
///
/// It computes an L0 score from both file count and aggregate bytes, computes
/// L1+ scores as level_bytes / level_target, sorts levels by descending score,
/// and compacts the highest-priority level whose score is >= 1.0.
pub(crate) struct ScorePriorityPolicy {
    next_indices: Vec<usize>,
}

impl ScorePriorityPolicy {
    pub(crate) fn new() -> Self {
        Self {
            next_indices: Vec::new(),
        }
    }

    fn level_total_size(level: &Level) -> usize {
        level.files.iter().map(|file| file.size).sum()
    }

    fn score_for_level(level: &Level, config: &CompactionConfig) -> f64 {
        if level.ordinal >= config.max_level {
            return 0.0;
        }
        if level.ordinal == 0 {
            let file_score = if config.l0_file_limit == 0 {
                0.0
            } else {
                level.files.len() as f64 / config.l0_file_limit as f64
            };
            let size_score = if config.l1_base_bytes == 0 {
                0.0
            } else {
                Self::level_total_size(level) as f64 / config.l1_base_bytes as f64
            };
            file_score.max(size_score)
        } else {
            let threshold = level_threshold(
                config.l1_base_bytes,
                config.level_size_multiplier,
                level.ordinal,
            );
            if threshold == 0 {
                0.0
            } else {
                Self::level_total_size(level) as f64 / threshold as f64
            }
        }
    }

    fn scored_levels(levels: &[Level], config: &CompactionConfig) -> Vec<(u8, f64)> {
        let mut scored: Vec<(u8, f64)> = levels
            .iter()
            .filter(|level| level.ordinal < config.max_level)
            .map(|level| (level.ordinal, Self::score_for_level(level, config)))
            .collect();
        scored.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.0.cmp(&right.0))
        });
        scored
    }

    fn max_trivial_move_bytes(config: &CompactionConfig) -> usize {
        config.target_file_size.saturating_mul(25)
    }

    fn grandparent_overlap_size(levels: &[Level], selected_level: u8, file: &DataFile) -> usize {
        levels
            .iter()
            .find(|level| level.ordinal == selected_level + 2)
            .map(|level| overlap_size(file, level.files.as_slice()))
            .unwrap_or(0)
    }

    fn rocksdb_trivial_move(
        levels: &[Level],
        selected_level: u8,
        file: &DataFile,
        config: &CompactionConfig,
    ) -> bool {
        let output_files: &[Arc<DataFile>] = levels
            .iter()
            .find(|level| level.ordinal == selected_level + 1)
            .map(|level| level.files.as_slice())
            .unwrap_or(&[]);
        output_files.iter().all(|other| !file_overlap(file, other))
            && file.size.saturating_add(Self::grandparent_overlap_size(
                levels,
                selected_level,
                file,
            )) <= Self::max_trivial_move_bytes(config)
    }

    fn next_index(&mut self, level: u8, len: usize) -> usize {
        if self.next_indices.len() <= level as usize {
            self.next_indices.resize(level as usize + 1, 0);
        }
        if len == 0 {
            return 0;
        }
        self.next_indices[level as usize] % len
    }

    fn advance_index(&mut self, level: u8, next: usize) {
        if self.next_indices.len() <= level as usize {
            self.next_indices.resize(level as usize + 1, 0);
        }
        self.next_indices[level as usize] = next;
    }

    fn pick_level_plan(
        &mut self,
        levels: &[Level],
        selected_level: u8,
        config: &CompactionConfig,
    ) -> Option<CompactionPlan> {
        let input_level = levels
            .iter()
            .find(|level| level.ordinal == selected_level)?;
        let output_files: &[Arc<DataFile>] = levels
            .iter()
            .find(|level| level.ordinal == selected_level + 1)
            .map(|level| level.files.as_slice())
            .unwrap_or(&[]);
        let mut ordered: Vec<&Arc<DataFile>> = input_level.files.iter().collect();
        ordered.sort_by(|left, right| {
            let left_overlap = overlap_size(left, output_files) as u128;
            let right_overlap = overlap_size(right, output_files) as u128;
            let left_size = left.size.max(1) as u128;
            let right_size = right.size.max(1) as u128;
            match (left_overlap * right_size).cmp(&(right_overlap * left_size)) {
                Ordering::Equal => match left_overlap.cmp(&right_overlap) {
                    Ordering::Equal => left.file_id.cmp(&right.file_id),
                    ord => ord,
                },
                ord => ord,
            }
        });
        let idx = self.next_index(selected_level, ordered.len());
        let file = *ordered.get(idx)?;
        self.advance_index(selected_level, idx + 1);
        Some(CompactionPlan {
            input_level: selected_level,
            output_level: selected_level + 1,
            base_file_id: file.file_id,
            trivial_move: Self::rocksdb_trivial_move(levels, selected_level, file, config),
            drop_truncated: false,
            drop_expired: false,
        })
    }
}

impl CompactionPolicy for ScorePriorityPolicy {
    fn pick_with_context(
        &mut self,
        levels: &[Level],
        config: CompactionConfig,
        context: CompactionPolicyContext<'_>,
    ) -> Option<CompactionPlan> {
        if levels.is_empty() {
            return None;
        }
        if let Some((level, file_id)) = pick_expired_file(levels, context.now_seconds) {
            return Some(CompactionPlan {
                input_level: level,
                output_level: level.saturating_add(1),
                base_file_id: file_id,
                trivial_move: true,
                drop_truncated: false,
                drop_expired: true,
            });
        }
        if let Some((level, output_level, file_id)) =
            pick_expired_separated_file_for_rewrite(levels, context.now_seconds, config.max_level)
        {
            return Some(CompactionPlan {
                input_level: level,
                output_level,
                base_file_id: file_id,
                trivial_move: false,
                drop_truncated: false,
                drop_expired: false,
            });
        }
        if let Some((level, file_id)) = pick_fully_truncated_file(levels, context) {
            return Some(CompactionPlan {
                input_level: level,
                output_level: level.saturating_add(1),
                base_file_id: file_id,
                trivial_move: true,
                drop_truncated: true,
                drop_expired: false,
            });
        }
        for (level_ordinal, score) in Self::scored_levels(levels, &config) {
            if score < 1.0 {
                break;
            }
            if level_ordinal == 0 {
                let level0 = levels.iter().find(|level| level.ordinal == 0)?;
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
                    drop_truncated: false,
                    drop_expired: false,
                });
            }
            if let Some(plan) = self.pick_level_plan(levels, level_ordinal, &config) {
                return Some(plan);
            }
        }
        None
    }
}

impl CompactionPolicy for MinOverlapPolicy {
    fn pick_with_context(
        &mut self,
        levels: &[Level],
        config: CompactionConfig,
        context: CompactionPolicyContext<'_>,
    ) -> Option<CompactionPlan> {
        if levels.is_empty() {
            return None;
        }
        if let Some((level, file_id)) = pick_expired_file(levels, context.now_seconds) {
            return Some(CompactionPlan {
                input_level: level,
                output_level: level.saturating_add(1),
                base_file_id: file_id,
                trivial_move: true,
                drop_truncated: false,
                drop_expired: true,
            });
        }
        if let Some((level, output_level, file_id)) =
            pick_expired_separated_file_for_rewrite(levels, context.now_seconds, config.max_level)
        {
            return Some(CompactionPlan {
                input_level: level,
                output_level,
                base_file_id: file_id,
                trivial_move: false,
                drop_truncated: false,
                drop_expired: false,
            });
        }
        if let Some((level, file_id)) = pick_fully_truncated_file(levels, context) {
            return Some(CompactionPlan {
                input_level: level,
                output_level: level.saturating_add(1),
                base_file_id: file_id,
                trivial_move: true,
                drop_truncated: true,
                drop_expired: false,
            });
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
            let first_level = pick_first_level(levels, &config, context);
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
            drop_truncated: false,
            drop_expired: false,
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

/// Resolves policy output to the exact files a rewrite compaction will consume.
///
/// Trivial moves and removal-only drops bypass this function. Normal candidates
/// retain the existing adjacent-level selection unless one selected file would
/// cross an incompatible schema transition while being materialized to the
/// newest selected physical schema. That requires a schema closure which only
/// includes files needed to preserve the affected overlap chain.
pub(crate) fn resolve_compaction_plan(
    levels: &[Level],
    plan: &CompactionPlan,
    config: &CompactionConfig,
    schema_manager: &SchemaManager,
    column_family_id: u8,
) -> Result<ResolvedCompactionPlan> {
    let runs = build_runs_for_plan(levels, plan, config);
    let Some(target_schema_id) = target_schema_id_for_runs(&runs) else {
        return Err(Error::InvalidState(format!(
            "Compaction plan L{}->L{} has no selected input files",
            plan.input_level, plan.output_level
        )));
    };
    schema_manager.schema(target_schema_id)?;
    if !requires_schema_barrier_closure(&runs, schema_manager, target_schema_id, column_family_id)?
    {
        return Ok(ResolvedCompactionPlan {
            output_level: plan.output_level,
            runs,
            target_schema_id,
        });
    }

    resolve_schema_barrier_closure(
        levels,
        &runs,
        schema_manager,
        column_family_id,
        plan.output_level,
    )
}

fn requires_schema_barrier_closure(
    runs: &[SortedRun],
    schema_manager: &SchemaManager,
    target_schema_id: u64,
    column_family_id: u8,
) -> Result<bool> {
    let mut checked_schema_ids = HashSet::new();
    for file in runs.iter().flat_map(|run| run.files()) {
        if file.schema_id != target_schema_id
            && checked_schema_ids.insert(file.schema_id)
            && !schema_manager.is_builtin_compatible_transition(
                file.schema_id,
                target_schema_id,
                column_family_id,
            )?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn target_schema_id_for_runs(runs: &[SortedRun]) -> Option<u64> {
    runs.iter()
        .flat_map(|run| run.files())
        .map(|file| file.schema_id)
        .max()
}

/// Resolves an incompatible schema boundary to the smallest stable overlap
/// closure. The closure grows in both directions: downward to consume every
/// overlapping lower-level file, and upward to remove incompatible older
/// blockers which would otherwise remain above the rewritten output.
fn resolve_schema_barrier_closure(
    levels: &[Level],
    candidate_runs: &[SortedRun],
    schema_manager: &SchemaManager,
    column_family_id: u8,
    destination_floor: u8,
) -> Result<ResolvedCompactionPlan> {
    let mut selected = candidate_runs
        .iter()
        .flat_map(|run| run.files())
        .map(|file| file.file_id)
        .collect::<HashSet<_>>();
    let mut compatibility_cache = HashMap::new();

    loop {
        let mut changed = false;
        let selected_summary = summarize_selected_files(levels, &selected)
            .expect("schema closure must retain at least one candidate file");

        // A rewritten file descends below every selected input level. Include
        // each overlapping deeper file so the result terminates that overlap
        // chain, then let whole-file expansion drive the next iteration.
        for level in levels
            .iter()
            .filter(|level| level.ordinal > selected_summary.shallowest_level)
        {
            for file in level.files.iter().filter(|file| {
                file_overlaps_key_range(
                    file,
                    &selected_summary.start_key,
                    &selected_summary.end_key,
                )
            }) {
                changed |= selected.insert(file.file_id);
            }
        }

        let selected_summary = summarize_selected_files(levels, &selected)
            .expect("schema closure must retain at least one candidate file");
        let target_schema_id = selected_summary.target_schema_id;
        schema_manager.schema(target_schema_id)?;
        let output_level = selected_summary.deepest_level.max(destination_floor);
        let mut start_key = selected_summary.start_key;
        let mut end_key = selected_summary.end_key;

        // Files above the output that are already target-schema, newer, or
        // built-in compatible remain valid above it. An older incompatible
        // overlap must descend with this closure instead.
        for level in levels.iter().filter(|level| level.ordinal < output_level) {
            for file in &level.files {
                if selected.contains(&file.file_id)
                    || file.schema_id >= target_schema_id
                    || !file_overlaps_key_range(file, &start_key, &end_key)
                {
                    continue;
                }
                if !is_transition_compatible(
                    &mut compatibility_cache,
                    schema_manager,
                    file.schema_id,
                    target_schema_id,
                    column_family_id,
                )? && selected.insert(file.file_id)
                {
                    changed = true;
                    extend_key_range(&mut start_key, &mut end_key, file);
                }
            }
        }

        // `Level.files` is oldest-to-newest for tiered levels, while their
        // runs are read newest-first. Once a tiered file descends, every older
        // overlapping file must descend too or its relative merge order flips.
        for level in levels
            .iter()
            .filter(|level| level.tiered && level.ordinal < output_level)
        {
            let Some(newest_selected_index) = level
                .files
                .iter()
                .enumerate()
                .filter_map(|(index, file)| selected.contains(&file.file_id).then_some(index))
                .max()
            else {
                continue;
            };
            for file in &level.files[..newest_selected_index] {
                if selected.contains(&file.file_id)
                    || !file_overlaps_key_range(file, &start_key, &end_key)
                {
                    continue;
                }
                changed |= selected.insert(file.file_id);
            }
        }

        if !changed {
            return Ok(ResolvedCompactionPlan {
                output_level,
                runs: runs_from_selected_files(levels, &selected),
                target_schema_id,
            });
        }
    }
}

fn is_transition_compatible(
    cache: &mut HashMap<(u64, u64), bool>,
    schema_manager: &SchemaManager,
    from_schema_id: u64,
    to_schema_id: u64,
    column_family_id: u8,
) -> Result<bool> {
    let key = (from_schema_id, to_schema_id);
    if let Some(compatible) = cache.get(&key) {
        return Ok(*compatible);
    }
    let compatible = schema_manager.is_builtin_compatible_transition(
        from_schema_id,
        to_schema_id,
        column_family_id,
    )?;
    cache.insert(key, compatible);
    Ok(compatible)
}

struct SelectedFileSummary {
    shallowest_level: u8,
    deepest_level: u8,
    target_schema_id: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
}

fn summarize_selected_files(
    levels: &[Level],
    selected: &HashSet<FileId>,
) -> Option<SelectedFileSummary> {
    let mut summary: Option<SelectedFileSummary> = None;
    for level in levels {
        for file in level
            .files
            .iter()
            .filter(|file| selected.contains(&file.file_id))
        {
            let Some(summary) = summary.as_mut() else {
                summary = Some(SelectedFileSummary {
                    shallowest_level: level.ordinal,
                    deepest_level: level.ordinal,
                    target_schema_id: file.schema_id,
                    start_key: file.start_key.to_vec(),
                    end_key: file.end_key.to_vec(),
                });
                continue;
            };
            summary.shallowest_level = summary.shallowest_level.min(level.ordinal);
            summary.deepest_level = summary.deepest_level.max(level.ordinal);
            summary.target_schema_id = summary.target_schema_id.max(file.schema_id);
            extend_key_range(&mut summary.start_key, &mut summary.end_key, file);
        }
    }
    summary
}

fn extend_key_range(start_key: &mut Vec<u8>, end_key: &mut Vec<u8>, file: &DataFile) {
    if file.start_key.as_slice() < start_key.as_slice() {
        *start_key = file.start_key.to_vec();
    }
    if file.end_key.as_slice() > end_key.as_slice() {
        *end_key = file.end_key.to_vec();
    }
}

fn runs_from_selected_files(levels: &[Level], selected: &HashSet<FileId>) -> Vec<SortedRun> {
    let mut ordered_levels = levels.iter().collect::<Vec<_>>();
    ordered_levels.sort_by_key(|level| level.ordinal);
    let mut runs = Vec::new();
    for level in ordered_levels {
        let files = level
            .files
            .iter()
            .filter(|file| selected.contains(&file.file_id))
            .cloned()
            .collect::<Vec<_>>();
        if level.tiered {
            for file in files.into_iter().rev() {
                runs.push(SortedRun::new(level.ordinal, vec![file]));
            }
        } else if !files.is_empty() {
            runs.push(SortedRun::new(level.ordinal, files));
        }
    }
    runs
}

fn file_overlaps_key_range(file: &DataFile, start_key: &[u8], end_key: &[u8]) -> bool {
    file.start_key.as_slice() <= end_key && start_key <= file.end_key.as_slice()
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
#[path = "../../tests/unit/compaction/policy.rs"]
mod tests;
