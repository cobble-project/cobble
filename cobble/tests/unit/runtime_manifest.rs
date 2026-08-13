use super::*;
use crate::file::FileSystemRegistry;
use crate::metrics_manager::MetricsManager;
use tempfile::TempDir;

fn file(id: u64) -> ManifestFile {
    ManifestFile {
        file_id: id,
        file_type: "sst".to_string(),
        schema_id: 1,
        size: 1,
        start_key: format!("{id:02x}"),
        end_key: format!("{:02x}", id + 1),
        path: format!("data/{id}.sst"),
        has_separated_values: false,
        bucket_range_start: 0,
        bucket_range_end: 0,
        effective_bucket_range_start: 0,
        effective_bucket_range_end: 0,
        vlog_file_seq_offset: 0,
        max_expired_at: 0,
        origin: crate::file::logical_file::ReplicaOrigin::Owned,
    }
}

fn manifest(generation: u64, levels: Vec<Vec<ManifestLevel>>) -> RuntimeManifest {
    let tree_count = levels.len();
    RuntimeManifest {
        generation,
        seq_id: generation,
        timestamp_seconds: generation as u32,
        compaction_mode: CompactionMode::Embedded,
        topology_epoch: 0,
        latest_schema_id: 1,
        bucket_ranges: vec![0..=0],
        lsm_tree_bucket_ranges: vec![0..=0; tree_count],
        tree_scopes: (0..tree_count)
            .map(|_| LSMTreeScope::new(0..=0, 0))
            .collect(),
        tree_levels: levels,
        vlog_files: Vec::new(),
        truncation_cursors: Vec::new(),
    }
}

fn levels(l0: &[u64], l1: &[u64]) -> Vec<ManifestLevel> {
    vec![
        ManifestLevel {
            ordinal: 0,
            tiered: true,
            files: l0.iter().copied().map(file).collect(),
        },
        ManifestLevel {
            ordinal: 1,
            tiered: false,
            files: l1.iter().copied().map(file).collect(),
        },
    ]
}

fn loaded(manifest: RuntimeManifest, chain_depth: usize) -> LoadedRuntimeManifest {
    LoadedRuntimeManifest {
        generation: manifest.generation,
        base_generation: None,
        chain_depth,
        manifest,
    }
}

fn test_store() -> (TempDir, RuntimeManifestStore) {
    let dir = tempfile::tempdir().unwrap();
    let url = url::Url::from_directory_path(dir.path())
        .unwrap()
        .to_string();
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register(&url).unwrap();
    let file_manager = Arc::new(
        FileManager::with_defaults(fs, Arc::new(MetricsManager::new("runtime-manifest-test")))
            .unwrap(),
    );
    (dir, RuntimeManifestStore::new(file_manager))
}

fn write_raw(store: &RuntimeManifestStore, generation: u64, envelope: &RuntimeManifestEnvelope) {
    write_metadata_file(
        &store.file_manager,
        &runtime_manifest_name(generation),
        &encode_runtime_manifest(envelope).unwrap(),
    )
    .unwrap();
}

#[test]
fn full_round_trip_and_atomic_current_publication() {
    let (_dir, store) = test_store();
    let current = manifest(1, vec![levels(&[1], &[])]);
    store
        .publish(&RuntimeManifestEnvelope::full(current.clone()))
        .unwrap();

    let loaded = store.load_current().unwrap().unwrap();
    assert_eq!(loaded.chain_depth, 1);
    assert_eq!(loaded.manifest, current);
}

#[test]
fn optional_runtime_fields_default_for_full_and_incremental_manifests() {
    let envelope = RuntimeManifestEnvelope::full(manifest(1, vec![levels(&[1], &[])]));
    let mut value = serde_json::to_value(envelope).unwrap();
    value["manifest"]["payload"]
        .as_object_mut()
        .unwrap()
        .remove("compaction_mode");
    value["manifest"]["payload"]
        .as_object_mut()
        .unwrap()
        .remove("timestamp_seconds");

    let decoded = decode_runtime_manifest(&serde_json::to_vec(&value).unwrap()).unwrap();
    let RuntimeManifestPayload::Full(decoded) = decoded.manifest else {
        panic!("expected full manifest");
    };
    assert_eq!(decoded.compaction_mode, CompactionMode::Embedded);
    assert_eq!(decoded.timestamp_seconds, 0);

    let base = manifest(1, vec![levels(&[1], &[])]);
    let current = manifest(2, vec![levels(&[1, 2], &[])]);
    let envelope = build_runtime_manifest(current, Some(&loaded(base, 1))).unwrap();
    let mut value = serde_json::to_value(envelope).unwrap();
    value["manifest"]["payload"]
        .as_object_mut()
        .unwrap()
        .remove("compaction_mode");
    value["manifest"]["payload"]
        .as_object_mut()
        .unwrap()
        .remove("timestamp_seconds");
    let decoded = decode_runtime_manifest(&serde_json::to_vec(&value).unwrap()).unwrap();
    let RuntimeManifestPayload::Incremental(decoded) = decoded.manifest else {
        panic!("expected incremental manifest");
    };
    assert_eq!(decoded.compaction_mode, CompactionMode::Embedded);
    assert_eq!(decoded.timestamp_seconds, 0);
}

#[test]
fn incremental_runtime_manifest_uses_the_new_observation_timestamp() {
    let base = manifest(1, vec![levels(&[1], &[])]);
    let mut current = manifest(2, vec![levels(&[1, 2], &[])]);
    current.timestamp_seconds = 4_321;
    let envelope = build_runtime_manifest(current.clone(), Some(&loaded(base.clone(), 1))).unwrap();
    let RuntimeManifestPayload::Incremental(incremental) = envelope.manifest else {
        panic!("expected incremental manifest");
    };

    assert_eq!(incremental.timestamp_seconds, 4_321);
    assert_eq!(
        apply_runtime_incremental(&base, &incremental)
            .unwrap()
            .timestamp_seconds,
        4_321
    );
}

#[test]
fn compaction_mode_change_forces_a_full_manifest() {
    let base = manifest(1, vec![levels(&[1], &[])]);
    let mut current = manifest(2, vec![levels(&[1, 2], &[])]);
    current.compaction_mode = CompactionMode::Dedicated;

    assert!(matches!(
        build_runtime_manifest(current, Some(&loaded(base, 1)))
            .unwrap()
            .manifest,
        RuntimeManifestPayload::Full(_)
    ));
}

#[test]
fn dedicated_compaction_mode_survives_an_incremental_manifest() {
    let mut base = manifest(1, vec![levels(&[1], &[])]);
    base.compaction_mode = CompactionMode::Dedicated;
    let mut current = manifest(2, vec![levels(&[1, 2], &[])]);
    current.compaction_mode = CompactionMode::Dedicated;

    let envelope = build_runtime_manifest(current.clone(), Some(&loaded(base.clone(), 1))).unwrap();
    let RuntimeManifestPayload::Incremental(incremental) = envelope.manifest else {
        panic!("expected incremental manifest");
    };
    assert_eq!(incremental.compaction_mode, CompactionMode::Dedicated);
    assert_eq!(
        apply_runtime_incremental(&base, &incremental).unwrap(),
        current
    );
}

#[test]
fn decode_rejects_previous_physical_key_format() {
    let previous = RuntimeManifestEnvelope {
        version: 1,
        manifest: RuntimeManifestPayload::Full(manifest(1, vec![levels(&[], &[])])),
    };
    let raw = serde_json::to_vec(&previous).unwrap();
    let err = decode_runtime_manifest(&raw).expect_err("version 1 must be rejected");
    assert!(
        err.to_string()
            .contains("Unsupported runtime manifest version: 1 (expected 2..=3)")
    );
}

#[test]
fn interrupted_publish_keeps_current_and_retry_skips_orphan_generation() {
    let (_dir, store) = test_store();
    let base = manifest(1, vec![levels(&[1], &[])]);
    store
        .publish(&RuntimeManifestEnvelope::full(base.clone()))
        .unwrap();
    let loaded_base = store.load_current().unwrap().unwrap();

    // Simulate a crash after MANIFEST-2 is durable but before CURRENT is replaced.
    let orphan = manifest(2, vec![levels(&[1, 2], &[])]);
    let orphan_envelope = build_runtime_manifest(orphan, Some(&loaded_base)).expect("build orphan");
    write_raw(&store, 2, &orphan_envelope);
    assert_eq!(
        store.load_current().unwrap().unwrap().manifest,
        base,
        "an unpublished generation must not change the authoritative state"
    );

    let retry_generation = store.allocate_next_generation().unwrap();
    assert_eq!(retry_generation, 3);
    let retry = manifest(retry_generation, vec![levels(&[1, 3], &[])]);
    let retry_envelope =
        build_runtime_manifest(retry.clone(), Some(&loaded_base)).expect("build retry");
    store.publish(&retry_envelope).unwrap();

    let loaded = store.load_current().unwrap().unwrap();
    assert_eq!(loaded.generation, 3);
    assert_eq!(loaded.manifest, retry);
    assert_eq!(
        store
            .load_chain(3)
            .unwrap()
            .iter()
            .map(|entry| entry.generation)
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    assert!(
        store
            .file_manager
            .metadata_file_exists_untracked(&runtime_manifest_name(2))
            .unwrap(),
        "unreachable generations remain until reader-safe GC exists"
    );
}

#[test]
fn l0_append_uses_incremental_and_preserves_order() {
    let (_dir, store) = test_store();
    let base = manifest(1, vec![levels(&[1], &[])]);
    let current = manifest(2, vec![levels(&[1, 2], &[])]);
    store
        .publish(&RuntimeManifestEnvelope::full(base.clone()))
        .unwrap();
    let envelope = build_runtime_manifest(current.clone(), Some(&loaded(base.clone(), 1))).unwrap();
    let RuntimeManifestPayload::Incremental(incremental) = &envelope.manifest else {
        panic!("expected incremental manifest");
    };
    assert_eq!(
        incremental.tree_level_edits[0].level_edits[0].resulting_file_ids,
        [1, 2]
    );
    assert_eq!(
        apply_runtime_incremental(&manifest(1, vec![levels(&[1], &[])]), incremental).unwrap(),
        current
    );
    store.publish(&envelope).unwrap();
    assert_eq!(store.load_current().unwrap().unwrap().manifest, current);
}

#[test]
fn full_manifest_allows_the_same_file_in_multiple_trees() {
    let (_dir, store) = test_store();
    let shared = manifest(1, vec![levels(&[7], &[7]), levels(&[7], &[])]);
    store
        .publish(&RuntimeManifestEnvelope::full(shared.clone()))
        .unwrap();

    assert_eq!(store.load_current().unwrap().unwrap().manifest, shared);
}

#[test]
fn full_manifest_rejects_conflicting_descriptors_for_a_shared_file() {
    let (_dir, store) = test_store();
    let mut conflicting_file = file(7);
    conflicting_file.path = "data/conflicting-7.sst".to_string();
    let mut conflicting = manifest(1, vec![levels(&[7], &[]), levels(&[7], &[])]);
    conflicting.tree_levels[1][0].files = vec![conflicting_file];
    write_raw(&store, 1, &RuntimeManifestEnvelope::full(conflicting));

    assert!(
        store
            .load_generation(1)
            .unwrap_err()
            .to_string()
            .contains("conflicting descriptors for file id 7")
    );
}

#[test]
fn descriptor_change_falls_back_to_full_and_incremental_rejects_wrong_base_and_rewound_sequence() {
    let base = manifest(1, vec![levels(&[1], &[])]);
    let mut changed = manifest(2, vec![levels(&[1], &[])]);
    changed.tree_levels[0][0].files[0].path = "data/changed-1.sst".to_string();
    assert!(matches!(
        build_runtime_manifest(changed, Some(&loaded(base.clone(), 1)))
            .unwrap()
            .manifest,
        RuntimeManifestPayload::Full(_)
    ));

    let wrong_base = RuntimeIncrementalManifest {
        generation: 2,
        base_generation: 99,
        seq_id: 2,
        timestamp_seconds: 2,
        compaction_mode: CompactionMode::Embedded,
        topology_epoch: 0,
        latest_schema_id: 1,
        tree_level_edits: Vec::new(),
        vlog_files: Vec::new(),
        truncation_cursors: Vec::new(),
    };
    assert!(
        apply_runtime_incremental(&base, &wrong_base)
            .unwrap_err()
            .to_string()
            .contains("expects base generation 99, but received 1")
    );

    let rewound_sequence = RuntimeIncrementalManifest {
        generation: 2,
        base_generation: 1,
        seq_id: 0,
        timestamp_seconds: 2,
        compaction_mode: CompactionMode::Embedded,
        topology_epoch: 0,
        latest_schema_id: 1,
        tree_level_edits: Vec::new(),
        vlog_files: Vec::new(),
        truncation_cursors: Vec::new(),
    };
    assert!(
        apply_runtime_incremental(&base, &rewound_sequence)
            .unwrap_err()
            .to_string()
            .contains("cannot precede base sequence 1")
    );
}

#[test]
fn rewrite_preserves_the_exact_resulting_file_order() {
    let base = manifest(1, vec![levels(&[1, 2], &[3, 4])]);
    let current = manifest(2, vec![levels(&[], &[5, 4])]);
    let envelope = build_runtime_manifest(current.clone(), Some(&loaded(base.clone(), 1))).unwrap();
    let RuntimeManifestPayload::Incremental(incremental) = envelope.manifest else {
        panic!("expected incremental manifest");
    };
    assert_eq!(
        apply_runtime_incremental(&base, &incremental).unwrap(),
        current
    );
}

#[test]
fn trivial_move_is_an_incremental_edit() {
    let base = manifest(1, vec![levels(&[1], &[2])]);
    let current = manifest(2, vec![levels(&[], &[1, 2])]);
    let envelope = build_runtime_manifest(current.clone(), Some(&loaded(base.clone(), 1))).unwrap();
    let RuntimeManifestPayload::Incremental(incremental) = envelope.manifest else {
        panic!("expected incremental manifest");
    };
    assert_eq!(
        apply_runtime_incremental(&base, &incremental).unwrap(),
        current
    );
}

#[test]
fn drop_is_an_incremental_edit() {
    let base = manifest(1, vec![levels(&[1], &[2, 3])]);
    let current = manifest(2, vec![levels(&[1], &[3])]);
    let envelope = build_runtime_manifest(current.clone(), Some(&loaded(base.clone(), 1))).unwrap();
    let RuntimeManifestPayload::Incremental(incremental) = envelope.manifest else {
        panic!("expected incremental manifest");
    };
    assert_eq!(
        apply_runtime_incremental(&base, &incremental).unwrap(),
        current
    );
}

#[test]
fn topology_change_or_chain_limit_falls_back_to_full() {
    let base = manifest(1, vec![levels(&[1], &[])]);
    let changed_topology = manifest(2, vec![levels(&[1], &[]), levels(&[], &[])]);
    assert!(matches!(
        build_runtime_manifest(changed_topology, Some(&loaded(base.clone(), 1)))
            .unwrap()
            .manifest,
        RuntimeManifestPayload::Full(_)
    ));
    let current = manifest(2, vec![levels(&[1, 2], &[])]);
    assert!(matches!(
        build_runtime_manifest(
            current,
            Some(&loaded(base, MAX_RUNTIME_MANIFEST_CHAIN_DEPTH))
        )
        .unwrap()
        .manifest,
        RuntimeManifestPayload::Full(_)
    ));
}

#[test]
fn sequence_rewind_starts_a_new_full_chain() {
    let mut base = manifest(1, vec![levels(&[1], &[])]);
    base.seq_id = 10;
    let mut restored = manifest(2, vec![levels(&[1], &[])]);
    restored.seq_id = 9;

    assert!(matches!(
        build_runtime_manifest(restored, Some(&loaded(base, 1)))
            .unwrap()
            .manifest,
        RuntimeManifestPayload::Full(_)
    ));
}

#[test]
fn large_incremental_diff_falls_back_to_full() {
    let base_files: Vec<u64> = (1..=128).collect();
    let base = manifest(1, vec![levels(&base_files, &[])]);
    let current = manifest(2, vec![levels(&[129], &[])]);

    assert!(matches!(
        build_runtime_manifest(current, Some(&loaded(base, 1)))
            .unwrap()
            .manifest,
        RuntimeManifestPayload::Full(_)
    ));
}

#[test]
fn load_rejects_missing_base_cycle_duplicate_ids_and_corrupt_current() {
    let (_dir, store) = test_store();
    let missing_base = RuntimeManifestEnvelope {
        version: RUNTIME_MANIFEST_VERSION_CURRENT,
        manifest: RuntimeManifestPayload::Incremental(RuntimeIncrementalManifest {
            generation: 2,
            base_generation: 1,
            seq_id: 2,
            timestamp_seconds: 2,
            compaction_mode: CompactionMode::Embedded,
            topology_epoch: 0,
            latest_schema_id: 1,
            tree_level_edits: Vec::new(),
            vlog_files: Vec::new(),
            truncation_cursors: Vec::new(),
        }),
    };
    write_raw(&store, 2, &missing_base);
    assert!(
        store
            .load_generation(2)
            .unwrap_err()
            .to_string()
            .contains("Missing runtime manifest generation 1")
    );

    let cycle_a = RuntimeManifestEnvelope {
        version: RUNTIME_MANIFEST_VERSION_CURRENT,
        manifest: RuntimeManifestPayload::Incremental(RuntimeIncrementalManifest {
            generation: 3,
            base_generation: 4,
            seq_id: 3,
            timestamp_seconds: 3,
            compaction_mode: CompactionMode::Embedded,
            topology_epoch: 0,
            latest_schema_id: 1,
            tree_level_edits: Vec::new(),
            vlog_files: Vec::new(),
            truncation_cursors: Vec::new(),
        }),
    };
    let cycle_b = RuntimeManifestEnvelope {
        version: RUNTIME_MANIFEST_VERSION_CURRENT,
        manifest: RuntimeManifestPayload::Incremental(RuntimeIncrementalManifest {
            generation: 4,
            base_generation: 3,
            seq_id: 4,
            timestamp_seconds: 4,
            compaction_mode: CompactionMode::Embedded,
            topology_epoch: 0,
            latest_schema_id: 1,
            tree_level_edits: Vec::new(),
            vlog_files: Vec::new(),
            truncation_cursors: Vec::new(),
        }),
    };
    write_raw(&store, 3, &cycle_a);
    write_raw(&store, 4, &cycle_b);
    assert!(
        store
            .load_generation(3)
            .unwrap_err()
            .to_string()
            .contains("dependency cycle")
    );

    let duplicate = manifest(5, vec![levels(&[9, 9], &[])]);
    write_raw(&store, 5, &RuntimeManifestEnvelope::full(duplicate));
    assert!(
        store
            .load_generation(5)
            .unwrap_err()
            .to_string()
            .contains("duplicate file id 9")
    );

    write_metadata_file(
        &store.file_manager,
        RUNTIME_CURRENT_NAME,
        b"not-a-generation",
    )
    .unwrap();
    assert!(
        store
            .load_current()
            .unwrap_err()
            .to_string()
            .contains("CURRENT is not a generation")
    );
}

#[test]
fn load_rejects_an_incremental_with_an_incomplete_resulting_order() {
    let (_dir, store) = test_store();
    let base = manifest(1, vec![levels(&[1], &[])]);
    write_raw(&store, 1, &RuntimeManifestEnvelope::full(base));
    let invalid = RuntimeManifestEnvelope {
        version: RUNTIME_MANIFEST_VERSION_CURRENT,
        manifest: RuntimeManifestPayload::Incremental(RuntimeIncrementalManifest {
            generation: 2,
            base_generation: 1,
            seq_id: 2,
            timestamp_seconds: 2,
            compaction_mode: CompactionMode::Embedded,
            topology_epoch: 0,
            latest_schema_id: 1,
            tree_level_edits: vec![RuntimeTreeLevelEdit {
                tree_idx: 0,
                level_edits: vec![RuntimeLevelEdit {
                    level: 0,
                    tiered: true,
                    removed_file_ids: Vec::new(),
                    added_files: vec![file(2)],
                    resulting_file_ids: vec![1],
                }],
            }],
            vlog_files: Vec::new(),
            truncation_cursors: Vec::new(),
        }),
    };
    write_raw(&store, 2, &invalid);

    assert!(
        store
            .load_generation(2)
            .unwrap_err()
            .to_string()
            .contains("invalid resulting file order")
    );
}
