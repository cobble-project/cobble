use super::*;
use crate::data_file::DataFileType;
use crate::db_state::TruncationCursorId;
use crate::schema::DEFAULT_COLUMN_FAMILY_ID;
use crate::schema::SchemaManager;
use crate::sst::row_codec::encode_key;
use crate::r#type::Key;
use bytes::Bytes;
use std::collections::HashMap;

#[test]
fn default_compaction_uses_sst_write_buffer_size() {
    assert_eq!(
        CompactionConfig::default().buffer_size,
        SSTWriterOptions::default().buffer_size
    );
}

fn make_file(id: FileId, start: &[u8], end: &[u8], size: usize) -> Arc<DataFile> {
    make_file_with_schema(id, start, end, size, 0)
}

fn make_file_with_schema(
    id: FileId,
    start: &[u8],
    end: &[u8],
    size: usize,
    schema_id: u64,
) -> Arc<DataFile> {
    let bucket_range = DataFile::bucket_range_from_keys(start, end);
    Arc::new(DataFile::new_untracked(
        DataFileType::SSTable,
        start.to_vec(),
        end.to_vec(),
        id,
        schema_id,
        size,
        bucket_range.clone(),
        bucket_range,
    ))
}

#[test]
fn test_schema_barrier_resolves_bidirectional_fixed_point_closure() {
    let config = CompactionConfig {
        max_level: 4,
        l1_base_bytes: 100,
        ..CompactionConfig::default()
    };
    let schema_manager = Arc::new(SchemaManager::new(1));
    let mut builder = schema_manager.builder();
    builder
        .add_column(1, None, Some(Bytes::from_static(b"default")), None)
        .unwrap();
    builder.commit();
    schema_manager.builder().commit();
    schema_manager.builder().commit();

    // Schema 3 is globally latest, but an ordinary rewrite with only
    // schema-1/schema-2 builtin-compatible inputs retains target schema 2.
    let ordinary_levels = vec![
        Level {
            ordinal: 1,
            tiered: false,
            files: vec![make_file_with_schema(51, b"m", b"n", 1, 2)],
        },
        Level {
            ordinal: 2,
            tiered: false,
            files: vec![make_file_with_schema(52, b"n", b"o", 1, 1)],
        },
    ];
    assert_valid_schema_layering(&ordinary_levels);
    let ordinary = resolve_compaction_plan(
        &ordinary_levels,
        &CompactionPlan {
            input_level: 1,
            output_level: 2,
            base_file_id: 51,
            trivial_move: false,
            drop_truncated: false,
            drop_expired: false,
        },
        &config,
        schema_manager.as_ref(),
        DEFAULT_COLUMN_FAMILY_ID,
    )
    .unwrap();
    assert_eq!(ordinary.output_level, 2);
    assert_eq!(ordinary.target_schema_id, 2);
    assert_eq!(
        run_file_ids(&ordinary.runs),
        vec![(1, vec![51]), (2, vec![52])]
    );

    // The initial L1 schema-3 input overlaps only older schemas below it.
    // Downward closure reaches L3 schema 0, whose expanded range then finds an
    // older L0 blocker that did not overlap L1. Tiered ordering pulls the next
    // older L0 file down, and that range expansion reaches L4 on the following
    // pass. Newer compatible L0 files and disjoint files stay outside.
    let levels = vec![
        Level {
            ordinal: 0,
            tiered: true,
            files: vec![
                make_file_with_schema(5, b"x", b"z", 1, 0),
                make_file_with_schema(1, b"a", b"b", 1, 0),
                make_file_with_schema(2, b"b", b"c", 1, 0),
                make_file_with_schema(3, b"b", b"c", 1, 1),
                make_file_with_schema(4, b"b", b"c", 1, 2),
            ],
        },
        Level {
            ordinal: 1,
            tiered: false,
            files: vec![make_file_with_schema(11, b"d", b"e", 1, 3)],
        },
        Level {
            ordinal: 2,
            tiered: false,
            files: vec![make_file_with_schema(21, b"e", b"f", 1, 0)],
        },
        Level {
            ordinal: 3,
            tiered: false,
            files: vec![
                make_file_with_schema(31, b"c", b"d", 1, 0),
                make_file_with_schema(32, b"u", b"v", 1, 2),
            ],
        },
        Level {
            ordinal: 4,
            tiered: false,
            files: vec![make_file_with_schema(41, b"a", b"b", 1, 0)],
        },
    ];
    assert_valid_schema_layering(&levels);
    let candidate = CompactionPlan {
        input_level: 1,
        output_level: 2,
        base_file_id: 11,
        trivial_move: false,
        drop_truncated: false,
        drop_expired: false,
    };

    let closure = resolve_compaction_plan(
        &levels,
        &candidate,
        &config,
        schema_manager.as_ref(),
        DEFAULT_COLUMN_FAMILY_ID,
    )
    .unwrap();
    assert_eq!(closure.output_level, 4);
    assert_eq!(closure.target_schema_id, 3);
    assert_eq!(
        run_file_ids(&closure.runs),
        vec![
            (0, vec![2]),
            (0, vec![1]),
            (1, vec![11]),
            (2, vec![21]),
            (3, vec![31]),
            (4, vec![41]),
        ]
    );

    // L4 is already selected, so this closure has no lower level to add. The
    // incompatible L0 file still has to join from above.
    let bottom_levels = vec![
        Level {
            ordinal: 0,
            tiered: true,
            files: vec![make_file_with_schema(101, b"f", b"g", 1, 0)],
        },
        Level {
            ordinal: 3,
            tiered: false,
            files: vec![make_file_with_schema(131, b"d", b"e", 1, 1)],
        },
        Level {
            ordinal: 4,
            tiered: false,
            files: vec![make_file_with_schema(141, b"e", b"f", 1, 0)],
        },
    ];
    assert_valid_schema_layering(&bottom_levels);
    let bottom = resolve_compaction_plan(
        &bottom_levels,
        &CompactionPlan {
            input_level: 3,
            output_level: 4,
            base_file_id: 131,
            trivial_move: false,
            drop_truncated: false,
            drop_expired: false,
        },
        &config,
        schema_manager.as_ref(),
        DEFAULT_COLUMN_FAMILY_ID,
    )
    .unwrap();
    assert_eq!(bottom.output_level, 4);
    assert_eq!(bottom.target_schema_id, 1);
    assert_eq!(
        run_file_ids(&bottom.runs),
        vec![(0, vec![101]), (3, vec![131]), (4, vec![141])]
    );

    // An empty L1 output has no selected file to establish the destination,
    // but an incompatible L0 rewrite must still preserve the policy's L1
    // output floor.
    let empty_output = resolve_compaction_plan(
        &[Level {
            ordinal: 0,
            tiered: true,
            files: vec![
                make_file_with_schema(201, b"p", b"q", 1, 0),
                make_file_with_schema(202, b"r", b"s", 1, 1),
            ],
        }],
        &CompactionPlan {
            input_level: 0,
            output_level: 1,
            base_file_id: 201,
            trivial_move: false,
            drop_truncated: false,
            drop_expired: false,
        },
        &config,
        schema_manager.as_ref(),
        DEFAULT_COLUMN_FAMILY_ID,
    )
    .unwrap();
    assert_eq!(empty_output.output_level, 1);
    assert_eq!(empty_output.target_schema_id, 1);
    assert_eq!(
        run_file_ids(&empty_output.runs),
        vec![(0, vec![202]), (0, vec![201])]
    );
}

fn run_file_ids(runs: &[SortedRun]) -> Vec<(u8, Vec<FileId>)> {
    runs.iter()
        .map(|run| {
            (
                run.level(),
                run.files().iter().map(|file| file.file_id).collect(),
            )
        })
        .collect()
}

fn assert_valid_schema_layering(levels: &[Level]) {
    for level in levels.iter().filter(|level| level.tiered) {
        assert!(
            level
                .files
                .windows(2)
                .all(|files| files[0].schema_id <= files[1].schema_id),
            "tiered files must be ordered from older to newer schemas"
        );
    }
    for shallow in levels {
        for deep in levels.iter().filter(|deep| deep.ordinal > shallow.ordinal) {
            for shallow_file in &shallow.files {
                for deep_file in &deep.files {
                    if file_overlaps_key_range(
                        deep_file,
                        &shallow_file.start_key,
                        &shallow_file.end_key,
                    ) {
                        assert!(
                            deep_file.schema_id <= shallow_file.schema_id,
                            "overlapping deeper file {} has newer schema {} than shallow file {} schema {}",
                            deep_file.file_id,
                            deep_file.schema_id,
                            shallow_file.file_id,
                            shallow_file.schema_id
                        );
                    }
                }
            }
        }
    }
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
