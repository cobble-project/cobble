use super::*;
use crate::data_file::DataFileType;
use crate::db_state::TruncationCursorId;
use crate::schema::DEFAULT_COLUMN_FAMILY_ID;
use crate::sst::row_codec::encode_key;
use crate::r#type::Key;
use std::collections::HashMap;

#[test]
fn default_compaction_uses_sst_write_buffer_size() {
    assert_eq!(
        CompactionConfig::default().buffer_size,
        SSTWriterOptions::default().buffer_size
    );
}

fn make_file(id: FileId, start: &[u8], end: &[u8], size: usize) -> Arc<DataFile> {
    let bucket_range = DataFile::bucket_range_from_keys(start, end);
    Arc::new(DataFile::new_untracked(
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

fn make_separated_file(id: FileId, start: &[u8], end: &[u8], size: usize) -> Arc<DataFile> {
    let bucket_range = DataFile::bucket_range_from_keys(start, end);
    Arc::new(
        DataFile::new_untracked(
            DataFileType::SSTable,
            start.to_vec(),
            end.to_vec(),
            id,
            0,
            size,
            bucket_range.clone(),
            bucket_range,
        )
        .with_separated_values(true),
    )
}

fn make_encoded_file(id: FileId, start: &[u8], end: &[u8], size: usize) -> Arc<DataFile> {
    let start_key = encode_key(&Key::new(0, start.to_vec()));
    let end_key = encode_key(&Key::new(0, end.to_vec()));
    make_file(id, start_key.as_ref(), end_key.as_ref(), size)
}

fn cursor_context(cursor: &[u8]) -> (TruncationCursorMap, LSMTreeScope) {
    let mut cursors = HashMap::new();
    cursors.insert(
        TruncationCursorId::new(0, DEFAULT_COLUMN_FAMILY_ID),
        cursor.to_vec(),
    );
    (
        cursors,
        LSMTreeScope::new(0u16..=0u16, DEFAULT_COLUMN_FAMILY_ID),
    )
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
        drop_truncated: false,
        drop_expired: false,
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
        drop_truncated: false,
        drop_expired: false,
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

#[test]
fn test_score_priority_prefers_l0_when_l0_score_is_higher() {
    let config = CompactionConfig {
        l0_file_limit: 2,
        l1_base_bytes: 10,
        level_size_multiplier: 10,
        max_level: 4,
        ..CompactionConfig::default()
    };
    let level0 = Level {
        ordinal: 0,
        tiered: true,
        files: vec![
            make_file(1, b"a", b"b", 1),
            make_file(2, b"c", b"d", 1),
            make_file(3, b"e", b"f", 1),
            make_file(4, b"g", b"h", 1),
            make_file(5, b"i", b"j", 1),
        ],
    };
    let level1 = Level {
        ordinal: 1,
        tiered: false,
        files: vec![make_file(6, b"a", b"z", 15)],
    };
    let level2 = Level {
        ordinal: 2,
        tiered: false,
        files: vec![make_file(7, b"a", b"z", 150)],
    };
    let level3 = Level {
        ordinal: 3,
        tiered: false,
        files: Vec::new(),
    };
    let mut policy = ScorePriorityPolicy::new();
    let plan = policy
        .pick(&[level0, level1, level2, level3], config)
        .expect("plan");
    assert_eq!(plan.input_level, 0);
    assert_eq!(plan.base_file_id, 1);
}

#[test]
fn test_score_priority_prefers_highest_scored_non_l0_level() {
    let config = CompactionConfig {
        l0_file_limit: 4,
        l1_base_bytes: 10,
        level_size_multiplier: 10,
        max_level: 4,
        ..CompactionConfig::default()
    };
    let level0 = Level {
        ordinal: 0,
        tiered: true,
        files: vec![
            make_file(1, b"a", b"b", 1),
            make_file(2, b"c", b"d", 1),
            make_file(3, b"e", b"f", 1),
            make_file(4, b"g", b"h", 1),
        ],
    };
    let level1 = Level {
        ordinal: 1,
        tiered: false,
        files: vec![make_file(5, b"a", b"m", 25)],
    };
    let level2 = Level {
        ordinal: 2,
        tiered: false,
        files: vec![make_file(6, b"a", b"m", 90)],
    };
    let level3 = Level {
        ordinal: 3,
        tiered: false,
        files: Vec::new(),
    };
    let mut policy = ScorePriorityPolicy::new();
    let plan = policy
        .pick(&[level0, level1, level2, level3], config)
        .expect("plan");
    assert_eq!(plan.input_level, 1);
    assert_eq!(plan.base_file_id, 5);
}

#[test]
fn test_score_priority_uses_l0_size_guard() {
    let config = CompactionConfig {
        l0_file_limit: 10,
        l1_base_bytes: 10,
        level_size_multiplier: 10,
        max_level: 3,
        ..CompactionConfig::default()
    };
    let level0 = Level {
        ordinal: 0,
        tiered: true,
        files: vec![make_file(1, b"a", b"b", 8), make_file(2, b"c", b"d", 8)],
    };
    let level1 = Level {
        ordinal: 1,
        tiered: false,
        files: Vec::new(),
    };
    let level2 = Level {
        ordinal: 2,
        tiered: false,
        files: Vec::new(),
    };
    let mut policy = ScorePriorityPolicy::new();
    let plan = policy
        .pick(&[level0, level1, level2], config)
        .expect("plan");
    assert_eq!(plan.input_level, 0);
}

#[test]
fn test_score_priority_prefers_min_overlap_ratio_within_level() {
    let config = CompactionConfig {
        l0_file_limit: 4,
        l1_base_bytes: 10,
        level_size_multiplier: 10,
        max_level: 4,
        ..CompactionConfig::default()
    };
    let level0 = Level {
        ordinal: 0,
        tiered: true,
        files: Vec::new(),
    };
    let level1 = Level {
        ordinal: 1,
        tiered: false,
        files: vec![make_file(1, b"a", b"f", 60), make_file(2, b"g", b"h", 20)],
    };
    let level2 = Level {
        ordinal: 2,
        tiered: false,
        files: vec![make_file(3, b"a", b"f", 50), make_file(4, b"g", b"h", 1)],
    };
    let level3 = Level {
        ordinal: 3,
        tiered: false,
        files: Vec::new(),
    };
    let mut policy = ScorePriorityPolicy::new();
    let plan = policy
        .pick(&[level0, level1, level2, level3], config)
        .expect("plan");
    assert_eq!(plan.input_level, 1);
    assert_eq!(plan.base_file_id, 2);
}

#[test]
fn test_score_priority_drops_fully_truncated_file_first() {
    let config = CompactionConfig {
        l0_file_limit: 4,
        l1_base_bytes: 100,
        level_size_multiplier: 10,
        max_level: 4,
        ..CompactionConfig::default()
    };
    let level0 = Level {
        ordinal: 0,
        tiered: true,
        files: Vec::new(),
    };
    let level1 = Level {
        ordinal: 1,
        tiered: false,
        files: vec![
            make_encoded_file(1, b"a", b"m", 10),
            make_encoded_file(2, b"n", b"z", 10),
        ],
    };
    let level2 = Level {
        ordinal: 2,
        tiered: false,
        files: Vec::new(),
    };
    let (cursors, scope) = cursor_context(b"m");
    let context = CompactionPolicyContext {
        truncation_cursors: Some(&cursors),
        tree_scope: Some(&scope),
        now_seconds: 0,
    };
    let mut policy = ScorePriorityPolicy::new();
    let plan = policy
        .pick_with_context(&[level0, level1, level2], config, context)
        .expect("plan");

    assert_eq!(plan.input_level, 1);
    assert_eq!(plan.base_file_id, 1);
    assert!(plan.drop_truncated);
}

#[test]
fn test_score_priority_trivial_move_uses_single_file_when_growing_new_level() {
    let config = CompactionConfig {
        l0_file_limit: 4,
        l1_base_bytes: 100,
        level_size_multiplier: 10,
        max_level: 4,
        ..CompactionConfig::default()
    };
    let level0 = Level {
        ordinal: 0,
        tiered: true,
        files: Vec::new(),
    };
    let level1 = Level {
        ordinal: 1,
        tiered: false,
        files: vec![
            make_file(1, b"a", b"b", 40),
            make_file(2, b"c", b"d", 40),
            make_file(3, b"e", b"f", 40),
            make_file(4, b"g", b"h", 40),
            make_file(5, b"i", b"j", 40),
        ],
    };
    let level2 = Level {
        ordinal: 2,
        tiered: false,
        files: Vec::new(),
    };
    let level3 = Level {
        ordinal: 3,
        tiered: false,
        files: Vec::new(),
    };
    let mut policy = ScorePriorityPolicy::new();
    let plan = policy
        .pick(&[level0, level1, level2, level3], config)
        .expect("plan");
    assert!(plan.trivial_move);
    assert_eq!(plan.input_level, 1);
    assert_eq!(plan.output_level, 2);
    assert_eq!(plan.base_file_id, 1);
}

#[test]
fn test_score_priority_uses_cursor_to_advance_overlap_sorted_files() {
    let config = CompactionConfig {
        l0_file_limit: 4,
        l1_base_bytes: 100,
        level_size_multiplier: 10,
        max_level: 4,
        ..CompactionConfig::default()
    };
    let level0 = Level {
        ordinal: 0,
        tiered: true,
        files: Vec::new(),
    };
    let level1 = Level {
        ordinal: 1,
        tiered: false,
        files: vec![
            make_file(1, b"a", b"b", 40),
            make_file(2, b"c", b"d", 40),
            make_file(3, b"e", b"f", 40),
            make_file(4, b"g", b"h", 40),
            make_file(5, b"i", b"j", 40),
        ],
    };
    let level2 = Level {
        ordinal: 2,
        tiered: false,
        files: vec![make_file(6, b"e", b"f", 10)],
    };
    let level3 = Level {
        ordinal: 3,
        tiered: false,
        files: Vec::new(),
    };
    let mut policy = ScorePriorityPolicy::new();
    let first = policy
        .pick(
            &[
                level0.clone(),
                level1.clone(),
                level2.clone(),
                level3.clone(),
            ],
            config,
        )
        .expect("plan");
    let second = policy
        .pick(&[level0, level1, level2, level3], config)
        .expect("plan");
    assert!(first.trivial_move);
    assert_eq!(first.base_file_id, 1);
    assert_eq!(second.base_file_id, 2);
}

#[test]
fn test_score_priority_trivial_move_respects_grandparent_budget() {
    let config = CompactionConfig {
        l0_file_limit: 4,
        l1_base_bytes: 50,
        level_size_multiplier: 10,
        max_level: 4,
        target_file_size: 10,
        ..CompactionConfig::default()
    };
    let level0 = Level {
        ordinal: 0,
        tiered: true,
        files: Vec::new(),
    };
    let level1 = Level {
        ordinal: 1,
        tiered: false,
        files: vec![make_file(1, b"a", b"b", 100)],
    };
    let level2 = Level {
        ordinal: 2,
        tiered: false,
        files: Vec::new(),
    };
    let level3 = Level {
        ordinal: 3,
        tiered: false,
        files: vec![make_file(2, b"a", b"b", 200)],
    };
    let mut policy = ScorePriorityPolicy::new();
    let plan = policy
        .pick(&[level0, level1, level2, level3], config)
        .expect("plan");
    assert!(!plan.trivial_move);
}

fn make_expired_file(id: FileId, size: usize, max_expired_at: u32) -> Arc<DataFile> {
    let file = make_file(id, b"a", b"z", size);
    file.set_max_expired_at(max_expired_at);
    file
}

#[test]
fn test_score_priority_drops_expired_file() {
    let config = CompactionConfig {
        l0_file_limit: 4,
        l1_base_bytes: 100,
        level_size_multiplier: 10,
        max_level: 4,
        ..CompactionConfig::default()
    };
    let level0 = Level {
        ordinal: 0,
        tiered: true,
        files: Vec::new(),
    };
    let level1 = Level {
        ordinal: 1,
        tiered: false,
        files: vec![make_expired_file(1, 10, 500), make_file(2, b"n", b"z", 10)],
    };
    let level2 = Level {
        ordinal: 2,
        tiered: false,
        files: Vec::new(),
    };
    let context = CompactionPolicyContext {
        truncation_cursors: None,
        tree_scope: None,
        now_seconds: 600,
    };
    let mut policy = ScorePriorityPolicy::new();
    let plan = policy
        .pick_with_context(&[level0, level1, level2], config, context)
        .expect("plan");
    assert_eq!(plan.input_level, 1);
    assert_eq!(plan.base_file_id, 1);
    assert!(plan.drop_expired);
    assert!(!plan.drop_truncated);
}

#[test]
fn test_score_priority_skips_expired_when_ttl_disabled() {
    let config = CompactionConfig {
        l0_file_limit: 4,
        l1_base_bytes: 100,
        level_size_multiplier: 10,
        max_level: 4,
        ..CompactionConfig::default()
    };
    let level0 = Level {
        ordinal: 0,
        tiered: true,
        files: Vec::new(),
    };
    let level1 = Level {
        ordinal: 1,
        tiered: false,
        files: vec![make_expired_file(1, 10, 500)],
    };
    let level2 = Level {
        ordinal: 2,
        tiered: false,
        files: Vec::new(),
    };
    // now_seconds = 0 means TTL disabled, so no expired drop should be picked.
    let context = CompactionPolicyContext {
        truncation_cursors: None,
        tree_scope: None,
        now_seconds: 0,
    };
    let mut policy = ScorePriorityPolicy::new();
    let plan = policy.pick_with_context(&[level0, level1, level2], config, context);
    assert!(
        plan.is_none(),
        "no compaction expected when TTL is disabled"
    );
}

#[test]
fn test_expired_file_with_separated_values_picked_for_rewrite() {
    // Files with separated values must not use the removal-only drop path;
    // instead they should be picked for rewrite compaction to collect VLOG deltas.
    let config = CompactionConfig {
        l0_file_limit: 4,
        l1_base_bytes: 100,
        level_size_multiplier: 10,
        max_level: 4,
        ..CompactionConfig::default()
    };
    let level0 = Level {
        ordinal: 0,
        tiered: true,
        files: Vec::new(),
    };
    let expired_sep_file = make_separated_file(1, b"a", b"z", 10);
    expired_sep_file.set_max_expired_at(500);
    let level1 = Level {
        ordinal: 1,
        tiered: false,
        files: vec![expired_sep_file],
    };
    let level2 = Level {
        ordinal: 2,
        tiered: false,
        files: Vec::new(),
    };
    let context = CompactionPolicyContext {
        truncation_cursors: None,
        tree_scope: None,
        now_seconds: 600,
    };
    let mut policy = ScorePriorityPolicy::new();
    let plan = policy
        .pick_with_context(&[level0, level1, level2], config, context)
        .expect("rewrite plan for expired separated-value file");
    assert!(!plan.drop_expired, "must not use removal-only drop");
    assert!(!plan.drop_truncated);
    assert!(!plan.trivial_move);
    assert_eq!(plan.base_file_id, 1);
}

#[test]
fn test_expired_separated_file_at_max_level_rewrites_in_place() {
    // An expired separated-value file at the maximum level must not produce
    // output_level = max_level + 1. It should rewrite in place (output_level == max_level).
    let config = CompactionConfig {
        l0_file_limit: 4,
        l1_base_bytes: 100,
        level_size_multiplier: 10,
        max_level: 4,
        ..CompactionConfig::default()
    };
    let level0 = Level {
        ordinal: 0,
        tiered: true,
        files: Vec::new(),
    };
    let expired_sep_file = make_separated_file(1, b"a", b"z", 10);
    expired_sep_file.set_max_expired_at(500);
    // Place the file at max_level (4).
    let level4 = Level {
        ordinal: 4,
        tiered: false,
        files: vec![expired_sep_file],
    };
    let context = CompactionPolicyContext {
        truncation_cursors: None,
        tree_scope: None,
        now_seconds: 600,
    };
    let mut policy = ScorePriorityPolicy::new();
    let plan = policy
        .pick_with_context(&[level0, level4], config, context)
        .expect("rewrite plan for expired separated-value file at max level");
    assert_eq!(plan.input_level, 4);
    assert_eq!(
        plan.output_level, 4,
        "output_level must be capped at max_level, not max_level + 1"
    );
    assert!(!plan.drop_expired);
    assert!(!plan.trivial_move);
}

#[test]
fn test_expired_separated_file_above_configured_max_level_rewrites_in_place() {
    let config = CompactionConfig {
        l0_file_limit: 4,
        l1_base_bytes: 100,
        level_size_multiplier: 10,
        max_level: 4,
        ..CompactionConfig::default()
    };
    let expired_sep_file = make_separated_file(1, b"a", b"z", 10);
    expired_sep_file.set_max_expired_at(500);
    let level5 = Level {
        ordinal: 5,
        tiered: false,
        files: vec![expired_sep_file],
    };
    let context = CompactionPolicyContext {
        truncation_cursors: None,
        tree_scope: None,
        now_seconds: 600,
    };
    let mut policy = ScorePriorityPolicy::new();
    let plan = policy
        .pick_with_context(&[level5], config, context)
        .expect("rewrite plan for expired separated-value file above configured max level");
    assert_eq!(plan.input_level, 5);
    assert_eq!(plan.output_level, 5);
    assert!(!plan.drop_expired);
    assert!(!plan.trivial_move);
}

#[test]
fn test_truncated_file_with_separated_values_not_dropped() {
    let config = CompactionConfig {
        l0_file_limit: 4,
        l1_base_bytes: 100,
        level_size_multiplier: 10,
        max_level: 4,
        ..CompactionConfig::default()
    };
    let level0 = Level {
        ordinal: 0,
        tiered: true,
        files: Vec::new(),
    };
    let start_key = encode_key(&Key::new(0, b"a".to_vec()));
    let end_key = encode_key(&Key::new(0, b"m".to_vec()));
    let truncated_sep_file = make_separated_file(1, start_key.as_ref(), end_key.as_ref(), 10);
    let level1 = Level {
        ordinal: 1,
        tiered: false,
        files: vec![truncated_sep_file],
    };
    let level2 = Level {
        ordinal: 2,
        tiered: false,
        files: Vec::new(),
    };
    let (cursors, scope) = cursor_context(b"m");
    let context = CompactionPolicyContext {
        truncation_cursors: Some(&cursors),
        tree_scope: Some(&scope),
        now_seconds: 0,
    };
    let mut policy = ScorePriorityPolicy::new();
    let plan = policy.pick_with_context(&[level0, level1, level2], config, context);
    assert!(
        plan.is_none() || !plan.as_ref().is_some_and(|p| p.drop_truncated),
        "separated-value file must not be picked for removal-only truncated drop"
    );
}
